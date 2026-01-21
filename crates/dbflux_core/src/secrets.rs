use crate::DbError;

pub trait SecretStore: Send + Sync {
    fn is_available(&self) -> bool;
    fn get(&self, secret_ref: &str) -> Result<Option<String>, DbError>;
    fn set(&self, secret_ref: &str, value: &str) -> Result<(), DbError>;
    fn delete(&self, secret_ref: &str) -> Result<(), DbError>;
}

pub struct NoopSecretStore;

impl SecretStore for NoopSecretStore {
    fn is_available(&self) -> bool {
        false
    }

    fn get(&self, _secret_ref: &str) -> Result<Option<String>, DbError> {
        Ok(None)
    }

    fn set(&self, _secret_ref: &str, _value: &str) -> Result<(), DbError> {
        Ok(())
    }

    fn delete(&self, _secret_ref: &str) -> Result<(), DbError> {
        Ok(())
    }
}

const SERVICE_NAME: &str = "dbflux";

pub struct KeyringSecretStore {
    available: bool,
}

impl KeyringSecretStore {
    pub fn new() -> Self {
        let available = Self::check_availability();
        Self { available }
    }

    fn check_availability() -> bool {
        let test_entry = keyring::Entry::new(SERVICE_NAME, "__dbflux_test__");
        match test_entry {
            Ok(entry) => {
                let _ = entry.get_password();
                true
            }
            Err(_) => false,
        }
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

    fn get(&self, secret_ref: &str) -> Result<Option<String>, DbError> {
        if !self.available {
            return Ok(None);
        }

        let entry = keyring::Entry::new(SERVICE_NAME, secret_ref)
            .map_err(|e| DbError::IoError(std::io::Error::other(e.to_string())))?;

        match entry.get_password() {
            Ok(password) => Ok(Some(password)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(DbError::IoError(std::io::Error::other(e.to_string()))),
        }
    }

    fn set(&self, secret_ref: &str, value: &str) -> Result<(), DbError> {
        if !self.available {
            return Ok(());
        }

        let entry = keyring::Entry::new(SERVICE_NAME, secret_ref)
            .map_err(|e| DbError::IoError(std::io::Error::other(e.to_string())))?;

        entry
            .set_password(value)
            .map_err(|e| DbError::IoError(std::io::Error::other(e.to_string())))
    }

    fn delete(&self, secret_ref: &str) -> Result<(), DbError> {
        if !self.available {
            return Ok(());
        }

        let entry = keyring::Entry::new(SERVICE_NAME, secret_ref)
            .map_err(|e| DbError::IoError(std::io::Error::other(e.to_string())))?;

        match entry.delete_credential() {
            Ok(()) => Ok(()),
            Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(DbError::IoError(std::io::Error::other(e.to_string()))),
        }
    }
}

pub fn connection_secret_ref(profile_id: &uuid::Uuid) -> String {
    format!("dbflux:conn:{}", profile_id)
}

pub fn ssh_secret_ref(profile_id: &uuid::Uuid) -> String {
    format!("dbflux:ssh:{}", profile_id)
}

pub fn create_secret_store() -> Box<dyn SecretStore> {
    let keyring_store = KeyringSecretStore::new();
    if keyring_store.is_available() {
        Box::new(keyring_store)
    } else {
        Box::new(NoopSecretStore)
    }
}
