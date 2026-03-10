use crate::auth::AuthProfile;
use crate::{ConnectionProfile, DbError, ProxyProfile, SshTunnelProfile};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs;
use std::path::PathBuf;

/// JSON-file backed store for a `Vec<T>`.
pub struct JsonStore<T> {
    path: PathBuf,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> JsonStore<T> {
    pub fn new(filename: &str) -> Result<Self, DbError> {
        let config_dir = dirs::config_dir().ok_or_else(|| {
            DbError::IoError(std::io::Error::other("Could not find config directory"))
        })?;

        let app_dir = config_dir.join("dbflux");
        fs::create_dir_all(&app_dir).map_err(DbError::IoError)?;

        Ok(Self {
            path: app_dir.join(filename),
            _marker: std::marker::PhantomData,
        })
    }

    #[cfg(test)]
    pub fn from_path(path: PathBuf) -> Self {
        Self {
            path,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn load(&self) -> Result<Vec<T>, DbError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(&self.path).map_err(DbError::IoError)?;
        let items: Vec<T> =
            serde_json::from_str(&content).map_err(|e| DbError::InvalidProfile(e.to_string()))?;

        Ok(items)
    }

    pub fn save(&self, items: &[T]) -> Result<(), DbError> {
        let content = serde_json::to_string_pretty(items)
            .map_err(|e| DbError::InvalidProfile(e.to_string()))?;

        fs::write(&self.path, content).map_err(DbError::IoError)?;

        Ok(())
    }
}

pub type ProfileStore = JsonStore<ConnectionProfile>;
pub type SshTunnelStore = JsonStore<SshTunnelProfile>;
pub type ProxyStore = JsonStore<ProxyProfile>;

impl ProfileStore {
    pub fn profiles() -> Result<Self, DbError> {
        Self::new("profiles.json")
    }
}

impl SshTunnelStore {
    pub fn ssh_tunnels() -> Result<Self, DbError> {
        Self::new("ssh_tunnels.json")
    }
}

impl ProxyStore {
    pub fn proxies() -> Result<Self, DbError> {
        Self::new("proxies.json")
    }
}

pub type AuthProfileStore = JsonStore<AuthProfile>;

impl AuthProfileStore {
    pub fn auth_profiles() -> Result<Self, DbError> {
        Self::new("auth_profiles.json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProxyAuth, ProxyKind};

    fn temp_proxy_store() -> (tempfile::TempDir, ProxyStore) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proxies.json");
        (dir, ProxyStore::from_path(path))
    }

    fn sample_proxy(name: &str) -> ProxyProfile {
        ProxyProfile {
            id: uuid::Uuid::new_v4(),
            name: name.to_string(),
            kind: ProxyKind::Http,
            host: "proxy.local".to_string(),
            port: 8080,
            auth: ProxyAuth::None,
            no_proxy: None,
            enabled: true,
            save_secret: false,
        }
    }

    #[test]
    fn proxy_store_load_empty() {
        let (_dir, store) = temp_proxy_store();
        let proxies = store.load().unwrap();
        assert!(proxies.is_empty());
    }

    #[test]
    fn proxy_store_save_load_roundtrip() {
        let (_dir, store) = temp_proxy_store();

        let p1 = sample_proxy("Proxy A");
        let p2 = ProxyProfile {
            auth: ProxyAuth::Basic {
                username: "admin".to_string(),
            },
            no_proxy: Some("localhost".to_string()),
            ..sample_proxy("Proxy B")
        };

        store.save(&[p1.clone(), p2.clone()]).unwrap();
        let loaded = store.load().unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, p1.id);
        assert_eq!(loaded[0].name, "Proxy A");
        assert_eq!(loaded[1].id, p2.id);
        assert_eq!(loaded[1].name, "Proxy B");
        assert_eq!(loaded[1].auth, p2.auth);
        assert_eq!(loaded[1].no_proxy, p2.no_proxy);
    }

    #[test]
    fn proxy_store_load_invalid_json() {
        let (_dir, store) = temp_proxy_store();
        fs::write(&store.path, "not valid json!!!").unwrap();

        let result = store.load();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DbError::InvalidProfile(_)));
    }
}
