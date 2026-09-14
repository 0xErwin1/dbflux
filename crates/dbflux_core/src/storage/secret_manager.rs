use crate::{ConnectionProfile, DbConfig, ProxyProfile, SecretStore, SshTunnelProfile};
use log::error;
use secrecy::SecretString;
use std::sync::Arc;
use std::sync::RwLock;

/// Unifies types that have a keyring secret reference (`secret_ref()`).
pub trait HasSecretRef {
    fn secret_ref(&self) -> String;
}

impl HasSecretRef for SshTunnelProfile {
    fn secret_ref(&self) -> String {
        self.secret_ref()
    }
}

impl HasSecretRef for ProxyProfile {
    fn secret_ref(&self) -> String {
        self.secret_ref()
    }
}

pub struct SecretManager {
    secret_store: Arc<RwLock<Box<dyn SecretStore>>>,
}

impl SecretManager {
    pub fn new(secret_store: Box<dyn SecretStore>) -> Self {
        Self {
            secret_store: Arc::new(RwLock::new(secret_store)),
        }
    }

    fn store_read(&self) -> std::sync::RwLockReadGuard<'_, Box<dyn SecretStore>> {
        match self.secret_store.read() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned, recovering...");
                poison_err.into_inner()
            }
        }
    }

    pub fn is_available(&self) -> bool {
        self.store_read().is_available()
    }

    pub fn secret_store_arc(&self) -> Arc<RwLock<Box<dyn SecretStore>>> {
        self.secret_store.clone()
    }

    /// Reads a secret stored under an explicit keyring reference.
    ///
    /// Used for per-field auth-profile secrets, whose reference is computed as
    /// `dbflux:auth:{profile_id}:{field_id}` rather than derived from a single
    /// `HasSecretRef` value (a profile can hold several independent secrets).
    pub fn get_by_ref(&self, secret_ref: &str) -> Option<SecretString> {
        let store = self.store_read();

        if !store.is_available() {
            return None;
        }

        match store.get(secret_ref) {
            Ok(secret) => secret,
            Err(e) => {
                error!("Failed to get secret '{}': {:?}", secret_ref, e);
                None
            }
        }
    }

    /// Writes a secret under an explicit keyring reference.
    ///
    /// Returns `true` only when the value was actually persisted. A `false`
    /// result (store unavailable, or the backend rejected the write — e.g. a
    /// locked keyring) is logged, and callers that must not lose data (the
    /// plaintext->keyring migration) or that need to tell the user (the auth
    /// profile editor) MUST act on it rather than assume success.
    #[must_use]
    pub fn set_by_ref(&self, secret_ref: &str, secret: &SecretString) -> bool {
        let store = self.store_read();

        if !store.is_available() {
            log::warn!("Secret store unavailable; secret '{secret_ref}' was NOT persisted");
            return false;
        }

        match store.set(secret_ref, secret) {
            Ok(()) => true,
            Err(e) => {
                error!("Failed to save secret '{}': {:?}", secret_ref, e);
                false
            }
        }
    }

    /// Deletes a secret stored under an explicit keyring reference.
    pub fn delete_by_ref(&self, secret_ref: &str) {
        let store = self.store_read();

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.delete(secret_ref) {
            log::warn!("Failed to delete secret '{}': {:?}", secret_ref, e);
        }
    }

    pub fn get_secret<T: HasSecretRef>(&self, item: &T, label: &str) -> Option<SecretString> {
        match self.store_read().get(&item.secret_ref()) {
            Ok(secret) => secret,
            Err(e) => {
                error!("Failed to get {} secret: {:?}", label, e);
                None
            }
        }
    }

    pub fn save_secret<T: HasSecretRef>(&self, item: &T, secret: &SecretString, label: &str) {
        let store = self.store_read();

        if !store.is_available() {
            log::warn!("Secret store unavailable; {label} secret was NOT persisted");
            return;
        }

        if let Err(e) = store.set(&item.secret_ref(), secret) {
            error!("Failed to save {} secret: {:?}", label, e);
        }
    }

    pub fn delete_secret<T: HasSecretRef>(&self, item: &T, label: &str) {
        let store = self.store_read();

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.delete(&item.secret_ref()) {
            log::warn!("Failed to delete {} secret: {:?}", label, e);
        }
    }

    pub fn save_password(
        &self,
        profile: &ConnectionProfile,
        password: &SecretString,
    ) -> Result<(), crate::DbError> {
        if !profile.save_password {
            return Ok(());
        }

        let store = self.store_read();

        if !store.is_available() {
            return Err(crate::DbError::NotSupported(
                "System keyring unavailable".to_string(),
            ));
        }

        store.set(&profile.secret_ref(), password)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrecy::ExposeSecret;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct FakeSecretStore {
        available: bool,
        fail_set: bool,
        values: Mutex<HashMap<String, SecretString>>,
    }

    impl SecretStore for FakeSecretStore {
        fn is_available(&self) -> bool {
            self.available
        }

        fn get(&self, secret_ref: &str) -> Result<Option<SecretString>, crate::DbError> {
            Ok(self
                .values
                .lock()
                .expect("fake secret store lock poisoned")
                .get(secret_ref)
                .cloned())
        }

        fn set(&self, secret_ref: &str, value: &SecretString) -> Result<(), crate::DbError> {
            if self.fail_set {
                return Err(crate::DbError::IoError(std::io::Error::other(
                    "fake keyring write failure",
                )));
            }

            self.values
                .lock()
                .expect("fake secret store lock poisoned")
                .insert(secret_ref.to_string(), value.clone());
            Ok(())
        }

        fn delete(&self, _secret_ref: &str) -> Result<(), crate::DbError> {
            Ok(())
        }
    }

    fn profile() -> ConnectionProfile {
        ConnectionProfile::new("test", DbConfig::default_sqlite())
    }

    #[test]
    fn save_password_persists_password_when_store_accepts_writes() {
        let profile = profile();
        let manager = SecretManager::new(Box::new(FakeSecretStore {
            available: true,
            fail_set: false,
            values: Mutex::new(HashMap::new()),
        }));

        manager
            .save_password(&profile, &SecretString::from("new password"))
            .expect("password save succeeds");

        let saved = manager
            .get_password(&profile)
            .expect("password is available after successful save");
        assert_eq!(saved.expose_secret(), "new password");
    }

    #[test]
    fn save_password_returns_error_and_preserves_existing_password_on_write_failure() {
        let profile = profile();
        let mut values = HashMap::new();
        values.insert(
            profile.secret_ref(),
            SecretString::from("previous password"),
        );
        let manager = SecretManager::new(Box::new(FakeSecretStore {
            available: true,
            fail_set: true,
            values: Mutex::new(values),
        }));

        assert!(
            manager
                .save_password(&profile, &SecretString::from("replacement password"))
                .is_err()
        );

        let saved = manager
            .get_password(&profile)
            .expect("failed save preserves the previous password");
        assert_eq!(saved.expose_secret(), "previous password");
    }

    #[test]
    fn turso_profiles_do_not_request_ssh_secrets() {
        let profile = ConnectionProfile::new(
            "turso",
            DbConfig::Turso {
                url: "https://example.turso.io".to_string(),
            },
        );
        let manager = SecretManager::new(Box::new(FakeSecretStore {
            available: true,
            fail_set: false,
            values: Mutex::new(HashMap::new()),
        }));

        assert!(manager.get_ssh_secret_for_profile(&profile, &[]).is_none());
    }
}

impl SecretManager {
    pub fn delete_password(&self, profile: &ConnectionProfile) {
        let store = self.store_read();

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.delete(&profile.secret_ref()) {
            error!("Failed to delete password: {:?}", e);
        }
    }

    pub fn get_password(&self, profile: &ConnectionProfile) -> Option<SecretString> {
        let store = self.store_read();

        if !store.is_available() {
            return None;
        }

        match store.get(&profile.secret_ref()) {
            Ok(secret) => secret,
            Err(e) => {
                error!("Failed to get password: {:?}", e);
                None
            }
        }
    }

    pub fn get_ssh_password(&self, profile: &ConnectionProfile) -> Option<SecretString> {
        let store = self.store_read();

        if !store.is_available() {
            return None;
        }

        match store.get(&profile.ssh_secret_ref()) {
            Ok(secret) => secret,
            Err(e) => {
                error!("Failed to get SSH secret: {:?}", e);
                None
            }
        }
    }

    pub fn save_ssh_password(&self, profile: &ConnectionProfile, secret: &SecretString) {
        let store = self.store_read();

        if !store.is_available() {
            log::warn!("Secret store unavailable; SSH password was NOT persisted");
            return;
        }

        if let Err(e) = store.set(&profile.ssh_secret_ref(), secret) {
            error!("Failed to save SSH secret: {:?}", e);
        }
    }

    pub fn delete_ssh_password(&self, profile: &ConnectionProfile) {
        let store = self.store_read();

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.delete(&profile.ssh_secret_ref()) {
            error!("Failed to delete SSH secret: {:?}", e);
        }
    }

    pub fn get_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile) -> Option<SecretString> {
        self.get_secret(tunnel, "SSH tunnel")
    }

    pub fn save_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile, secret: &SecretString) {
        self.save_secret(tunnel, secret, "SSH tunnel");
    }

    pub fn delete_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile) {
        self.delete_secret(tunnel, "SSH tunnel");
    }

    pub fn get_proxy_secret(&self, proxy: &ProxyProfile) -> Option<SecretString> {
        self.get_secret(proxy, "proxy")
    }

    pub fn save_proxy_secret(&self, proxy: &ProxyProfile, secret: &SecretString) {
        self.save_secret(proxy, secret, "proxy");
    }

    pub fn delete_proxy_secret(&self, proxy: &ProxyProfile) {
        self.delete_secret(proxy, "proxy");
    }

    pub fn get_proxy_secret_for_profile(
        &self,
        profile: &ConnectionProfile,
        proxies: &[ProxyProfile],
    ) -> Option<SecretString> {
        let proxy_id = profile.proxy_profile_id?;
        let proxy = proxies.iter().find(|p| p.id == proxy_id)?;

        if !proxy.enabled {
            return None;
        }

        if !proxy.save_secret {
            return None;
        }

        self.get_proxy_secret(proxy)
    }

    pub fn get_ssh_secret_for_profile(
        &self,
        profile: &ConnectionProfile,
        ssh_tunnels: &[SshTunnelProfile],
    ) -> Option<SecretString> {
        let (ssh_tunnel, ssh_tunnel_profile_id) = match &profile.config {
            DbConfig::Postgres {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => (ssh_tunnel.as_ref(), *ssh_tunnel_profile_id),
            DbConfig::MySQL {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => (ssh_tunnel.as_ref(), *ssh_tunnel_profile_id),
            DbConfig::MongoDB {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => (ssh_tunnel.as_ref(), *ssh_tunnel_profile_id),
            DbConfig::Redis {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => (ssh_tunnel.as_ref(), *ssh_tunnel_profile_id),
            DbConfig::SqlServer {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => (ssh_tunnel.as_ref(), *ssh_tunnel_profile_id),
            DbConfig::Redshift {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => (ssh_tunnel.as_ref(), *ssh_tunnel_profile_id),
            DbConfig::SQLite { .. }
            | DbConfig::DynamoDB { .. }
            | DbConfig::CloudWatchLogs { .. }
            | DbConfig::InfluxDB { .. }
            | DbConfig::S3 { .. }
            | DbConfig::ClickHouse { .. }
            | DbConfig::Turso { .. }
            | DbConfig::External { .. } => {
                return None;
            }
        };

        if let Some(tunnel_profile_id) = ssh_tunnel_profile_id {
            let tunnel = ssh_tunnels.iter().find(|t| t.id == tunnel_profile_id)?;

            if !tunnel.save_secret {
                return None;
            }

            return self.get_ssh_tunnel_secret(tunnel);
        }

        if ssh_tunnel.is_some() {
            return self.get_ssh_password(profile);
        }

        None
    }
}
