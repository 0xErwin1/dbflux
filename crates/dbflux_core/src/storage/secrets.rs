use crate::DbError;
use crate::core::blocking::run_with_timeout;
use secrecy::SecretString;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub trait SecretStore: Send + Sync {
    fn is_available(&self) -> bool;
    fn get(&self, secret_ref: &str) -> Result<Option<SecretString>, DbError>;
    fn set(&self, secret_ref: &str, value: &SecretString) -> Result<(), DbError>;
    fn delete(&self, secret_ref: &str) -> Result<(), DbError>;
}

pub struct NoopSecretStore;

impl SecretStore for NoopSecretStore {
    fn is_available(&self) -> bool {
        false
    }

    fn get(&self, _secret_ref: &str) -> Result<Option<SecretString>, DbError> {
        Ok(None)
    }

    fn set(&self, _secret_ref: &str, _value: &SecretString) -> Result<(), DbError> {
        Ok(())
    }

    fn delete(&self, _secret_ref: &str) -> Result<(), DbError> {
        Ok(())
    }
}

const SERVICE_NAME: &str = "dbflux";

/// How long a single keyring call may take before DBFlux gives up on it.
///
/// A healthy secret service answers in milliseconds, so this bound is never
/// reached in practice. It exists because the `keyring` crate is synchronous and
/// has no timeout of its own: an unresponsive `org.freedesktop.secrets` (a
/// locked keyring with no prompter on the desktop, competing providers on the
/// same bus name, a stuck D-Bus session) would otherwise block the calling
/// thread — on the connection-manager save path, the GPUI main thread — forever.
pub const SECRET_STORE_OP_TIMEOUT: Duration = Duration::from_secs(5);

/// How long DBFlux stops writing to the keyring after one write timed out.
///
/// Long enough to swallow a burst of clicks on Save, short enough that saving
/// again right after unlocking the keyring goes through instead of failing.
const WRITE_RETRY_COOLDOWN: Duration = Duration::from_secs(5);

pub struct KeyringSecretStore {
    available: bool,
    op_timeout: Duration,
    /// While this deadline is in the future, writes are skipped: the last one
    /// timed out, and a blocking call cannot be killed, so its worker stays for
    /// the life of the process. The hold keeps that at one worker per cooldown
    /// instead of one per attempt, and expires on its own so a keyring that gets
    /// unlocked recovers without a restart. Reads are not covered: they answered
    /// within a millisecond in the failure mode this guards against, and the
    /// session stays useful only while stored passwords can still be read.
    write_hold: Mutex<Option<Instant>>,
}

impl KeyringSecretStore {
    pub fn new() -> Self {
        Self::with_timeout(SECRET_STORE_OP_TIMEOUT)
    }

    /// Builds the store with an explicit bound on a single keyring call.
    pub fn with_timeout(op_timeout: Duration) -> Self {
        let mut store = Self {
            available: false,
            op_timeout,
            write_hold: Mutex::new(None),
        };
        let available = store.check_availability();
        store.available = available;
        store
    }

    /// Runs one keyring call under this store's timeout.
    #[allow(clippy::result_large_err)]
    fn bounded<T, Call>(&self, label: &str, call: Call) -> Result<T, DbError>
    where
        T: Send + 'static,
        Call: FnOnce() -> Result<T, DbError> + Send + 'static,
    {
        run_with_timeout(label, self.op_timeout, call)
    }

    /// Like [`Self::bounded`], for calls that change what the keyring stores.
    ///
    /// Once one of them times out, further writes wait out
    /// [`WRITE_RETRY_COOLDOWN`]: each attempt would cost the caller another
    /// timeout and leave another abandoned worker behind.
    #[allow(clippy::result_large_err)]
    fn bounded_write<T, Call>(&self, label: &str, call: Call) -> Result<T, DbError>
    where
        T: Send + 'static,
        Call: FnOnce() -> Result<T, DbError> + Send + 'static,
    {
        if self.write_is_on_hold() {
            log::debug!("Skipping keyring write '{label}': a recent write timed out");
            return Err(DbError::Timeout);
        }

        let result = self.bounded(label, call);
        let mut hold = self.lock_write_hold();
        let was_on_hold = matches!(*hold, Some(deadline) if Instant::now() < deadline);

        match &result {
            Err(DbError::Timeout) => {
                *hold = Some(Instant::now() + WRITE_RETRY_COOLDOWN);
                if !was_on_hold {
                    log::warn!(
                        "Secret service did not answer '{label}'; DBFlux stops writing to the \
                         keyring for {WRITE_RETRY_COOLDOWN:?} and tries again after that. Unlock \
                         the keyring, or save the connection without storing its password."
                    );
                }
            }
            _ => *hold = None,
        }

        result
    }

    fn write_is_on_hold(&self) -> bool {
        matches!(*self.lock_write_hold(), Some(deadline) if Instant::now() < deadline)
    }

    fn lock_write_hold(&self) -> std::sync::MutexGuard<'_, Option<Instant>> {
        match self.write_hold.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                log::warn!("Keyring write hold lock poisoned, recovering...");
                poisoned.into_inner()
            }
        }
    }

    /// Probes the platform secret store and classifies the outcome so a locked
    /// keyring is not mistaken for a missing one.
    ///
    /// - `Ok` / `NoEntry`: backend reachable -> available.
    /// - `NoStorageAccess`: backend present but locked or access-denied (e.g. a
    ///   locked login keyring). Reported available so we do NOT downgrade to the
    ///   no-op store and silently drop secrets; individual writes will surface
    ///   their own errors until it is unlocked.
    /// - `PlatformFailure` / other: no working secure storage -> unavailable.
    /// - No answer within the timeout: available, as with `NoStorageAccess`. The
    ///   service is present but not answering, and downgrading to the no-op store
    ///   would drop secrets already stored in it for the rest of the process,
    ///   while a timed-out *write* recovers by itself once the cooldown passes.
    ///   Reads are still attempted and report their own timeout.
    ///
    /// Each case logs a distinct message so a locked keyring can be told apart
    /// from an absent one when diagnosing.
    fn check_availability(&self) -> bool {
        self.check_availability_with(Self::probe)
    }

    /// [`Self::check_availability`] with an injectable probe, so the timeout
    /// case can be exercised without a hung desktop keyring.
    fn check_availability_with<Call>(&self, probe: Call) -> bool
    where
        Call: FnOnce() -> Result<bool, DbError> + Send + 'static,
    {
        match self.bounded("availability probe", probe) {
            Ok(available) => available,
            // A probe that accepts the call and never answers is the locked
            // keyring `NoStorageAccess` also describes, not a missing backend:
            // latching `available = false` here would silently drop SSH, proxy
            // and auth-profile secrets until the application restarts.
            Err(DbError::Timeout) => {
                log::warn!(
                    "Secret service did not answer the availability probe; treating it as \
                     present but locked, so stored secrets stay readable and each call \
                     reports its own timeout."
                );
                true
            }
            Err(e) => {
                log::warn!("Keyring probe failed; secrets disabled: {e}");
                false
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn probe() -> Result<bool, DbError> {
        let entry = match keyring::Entry::new(SERVICE_NAME, "__dbflux_test__") {
            Ok(entry) => entry,
            Err(e) => {
                log::warn!("Keyring backend not constructible; secrets disabled: {e}");
                return Ok(false);
            }
        };

        Ok(match entry.get_password() {
            Ok(_) | Err(keyring::Error::NoEntry) => true,
            Err(keyring::Error::NoStorageAccess(e)) => {
                log::warn!(
                    "Keyring present but locked or access-denied; \
                     secret writes may fail until it is unlocked: {e}"
                );
                true
            }
            Err(keyring::Error::PlatformFailure(e)) => {
                log::warn!("Keyring platform unavailable; secrets disabled: {e}");
                false
            }
            Err(e) => {
                log::warn!("Keyring probe failed; secrets disabled: {e}");
                false
            }
        })
    }
}

impl Default for KeyringSecretStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SecretStore for KeyringSecretStore {
    fn is_available(&self) -> bool {
        self.available
    }

    #[allow(clippy::result_large_err)]
    fn get(&self, secret_ref: &str) -> Result<Option<SecretString>, DbError> {
        if !self.available {
            return Ok(None);
        }

        let secret_ref = secret_ref.to_string();
        self.bounded("get", move || {
            let entry =
                keyring::Entry::new(SERVICE_NAME, &secret_ref).map_err(DbError::io_message)?;

            match entry.get_password() {
                Ok(password) => Ok(Some(SecretString::from(password))),
                Err(keyring::Error::NoEntry) => Ok(None),
                Err(e) => Err(DbError::io_message(e)),
            }
        })
    }

    #[allow(clippy::result_large_err)]
    fn set(&self, secret_ref: &str, value: &SecretString) -> Result<(), DbError> {
        use secrecy::ExposeSecret;

        if !self.available {
            return Ok(());
        }

        let secret_ref = secret_ref.to_string();
        let value = value.clone();
        self.bounded_write("set", move || {
            let entry =
                keyring::Entry::new(SERVICE_NAME, &secret_ref).map_err(DbError::io_message)?;

            entry
                .set_password(value.expose_secret())
                .map_err(DbError::io_message)
        })
    }

    #[allow(clippy::result_large_err)]
    fn delete(&self, secret_ref: &str) -> Result<(), DbError> {
        if !self.available {
            return Ok(());
        }

        let secret_ref = secret_ref.to_string();
        self.bounded_write("delete", move || {
            let entry =
                keyring::Entry::new(SERVICE_NAME, &secret_ref).map_err(DbError::io_message)?;

            match entry.delete_credential() {
                Ok(()) => Ok(()),
                Err(keyring::Error::NoEntry) => Ok(()),
                Err(e) => Err(DbError::io_message(e)),
            }
        })
    }
}

pub fn connection_secret_ref(profile_id: &uuid::Uuid) -> String {
    format!("dbflux:conn:{}", profile_id)
}

pub fn ssh_secret_ref(profile_id: &uuid::Uuid) -> String {
    format!("dbflux:ssh:{}", profile_id)
}

pub fn ssh_tunnel_secret_ref(tunnel_id: &uuid::Uuid) -> String {
    format!("dbflux:ssh_tunnel:{}", tunnel_id)
}

pub fn proxy_secret_ref(proxy_id: &uuid::Uuid) -> String {
    format!("dbflux:proxy:{}", proxy_id)
}

/// Keyring reference for a single secret-kind auth profile field
/// (`Password` / `WriteOnly`). One entry per (profile, field) so a profile can
/// hold several independent secrets (e.g. `secret_access_key` + `session_token`).
pub fn auth_field_secret_ref(profile_id: &uuid::Uuid, field_id: &str) -> String {
    format!("dbflux:auth:{}:{}", profile_id, field_id)
}

pub fn create_secret_store() -> Box<dyn SecretStore> {
    let keyring_store = KeyringSecretStore::new();
    if keyring_store.is_available() {
        Box::new(keyring_store)
    } else {
        Box::new(NoopSecretStore)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn proxy_secret_ref_format() {
        let id = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            proxy_secret_ref(&id),
            "dbflux:proxy:550e8400-e29b-41d4-a716-446655440000"
        );
    }

    /// The store must route its calls through its own bound, so a keyring that
    /// never answers fails instead of blocking whoever asked for the secret.
    #[test]
    fn keyring_store_bounds_its_own_calls() {
        let store = silent_keyring_store();

        let result: Result<(), DbError> = store.bounded_write("set", hung_call);
        assert!(matches!(result, Err(DbError::Timeout)));
    }

    /// A service that let one write time out is not asked to write again while
    /// the hold lasts: the worker the timeout left behind is one per cooldown,
    /// not one per attempt, and the user does not pay the bound on every later
    /// save.
    #[test]
    fn a_silent_service_is_not_asked_to_write_again() {
        let store = silent_keyring_store();
        let first: Result<(), DbError> = store.bounded_write("set", hung_call);
        assert!(matches!(first, Err(DbError::Timeout)));
        assert!(store.write_is_on_hold(), "the timeout holds writes back");

        let started = Instant::now();
        let second: Result<(), DbError> = store.bounded_write("set", || {
            panic!("a silent service must not be asked to write a second time")
        });
        assert!(matches!(second, Err(DbError::Timeout)));
        assert!(
            started.elapsed() < Duration::from_millis(20),
            "the later write fails immediately, not after the timeout"
        );
    }

    /// The hold expires on its own, so a keyring that gets unlocked recovers
    /// without restarting the application — which is what the save failure tells
    /// the user to do.
    #[test]
    fn writes_resume_after_the_hold_expires() {
        let store = silent_keyring_store();
        let first: Result<(), DbError> = store.bounded_write("set", hung_call);
        assert!(matches!(first, Err(DbError::Timeout)));

        *store.lock_write_hold() = Some(Instant::now() - Duration::from_millis(1));

        let result: Result<(), DbError> = store.bounded_write("set", || Ok(()));
        assert!(
            result.is_ok(),
            "the keyring is written to again once the hold expires"
        );
        assert!(
            !store.write_is_on_hold(),
            "a successful write clears the hold"
        );
    }

    /// Reads keep their bound but are still attempted: the keyring that hangs on
    /// writes in this failure mode still answers reads, and the session needs the
    /// passwords it already stored.
    #[test]
    fn a_read_is_still_attempted_after_a_write_timed_out() {
        use secrecy::ExposeSecret;

        let store = silent_keyring_store();
        let first: Result<(), DbError> = store.bounded_write("set", hung_call);
        assert!(matches!(first, Err(DbError::Timeout)));

        let read: Result<Option<SecretString>, DbError> =
            store.bounded("get", || Ok(Some(SecretString::from("stored"))));
        assert!(
            matches!(&read, Ok(Some(secret)) if secret.expose_secret() == "stored"),
            "reads still run after a write timed out, got {read:?}"
        );
    }

    /// A probe that never answers must not disable the store: the service is
    /// present but locked, and "unavailable" would drop the secrets already
    /// stored in it for the rest of the process.
    #[test]
    fn a_probe_that_times_out_keeps_the_store_available() {
        let store = silent_keyring_store();

        assert!(
            store.check_availability_with(|| {
                std::thread::sleep(Duration::from_secs(30));
                Ok(false)
            }),
            "a silent probe is the locked case, so secrets stay enabled"
        );

        assert!(
            !store.check_availability_with(|| Err(DbError::io_message("no backend"))),
            "a probe that reports a real failure still disables secrets"
        );
    }

    fn silent_keyring_store() -> KeyringSecretStore {
        KeyringSecretStore {
            available: true,
            op_timeout: Duration::from_millis(50),
            write_hold: Mutex::new(None),
        }
    }

    fn hung_call() -> Result<(), DbError> {
        std::thread::sleep(Duration::from_secs(30));
        Ok(())
    }
}
