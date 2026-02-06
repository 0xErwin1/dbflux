use crate::{ConnectionProfile, DbConfig, SecretStore, SshTunnelProfile};
use log::error;
use std::sync::Arc;
use std::sync::RwLock;

pub struct SecretManager {
    secret_store: Arc<RwLock<Box<dyn SecretStore>>>,
}

impl SecretManager {
    pub fn new(secret_store: Box<dyn SecretStore>) -> Self {
        Self {
            secret_store: Arc::new(RwLock::new(secret_store)),
        }
    }

    /// Get read lock on secret store, recovering from poison errors.
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

    pub fn save_password(&self, profile: &ConnectionProfile, password: &str) {
        if !profile.save_password {
            return;
        }

        let store = self.store_read();

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.set(&profile.secret_ref(), password) {
            error!("Failed to save password: {:?}", e);
        }
    }

    pub fn delete_password(&self, profile: &ConnectionProfile) {
        let store = self.store_read();

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.delete(&profile.secret_ref()) {
            error!("Failed to delete password: {:?}", e);
        }
    }

    pub fn get_password(&self, profile: &ConnectionProfile) -> Option<String> {
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

    pub fn get_ssh_password(&self, profile: &ConnectionProfile) -> Option<String> {
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

    pub fn save_ssh_password(&self, profile: &ConnectionProfile, secret: &str) {
        let store = self.store_read();

        if !store.is_available() {
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

    pub fn get_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile) -> Option<String> {
        match self.store_read().get(&tunnel.secret_ref()) {
            Ok(secret) => secret,
            Err(e) => {
                error!("Failed to get SSH tunnel secret: {:?}", e);
                None
            }
        }
    }

    pub fn save_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile, secret: &str) {
        let store = self.store_read();

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.set(&tunnel.secret_ref(), secret) {
            error!("Failed to save SSH tunnel secret: {:?}", e);
        }
    }

    pub fn delete_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile) {
        let store = self.store_read();

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.delete(&tunnel.secret_ref()) {
            log::warn!("Failed to delete SSH tunnel secret: {:?}", e);
        }
    }

    /// Resolves the SSH secret for a profile, checking both inline and saved tunnel profiles.
    pub fn get_ssh_secret_for_profile(
        &self,
        profile: &ConnectionProfile,
        ssh_tunnels: &[SshTunnelProfile],
    ) -> Option<String> {
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
            DbConfig::SQLite { .. } => return None,
        };

        // If using a saved tunnel profile, get secret from there
        if let Some(tunnel_profile_id) = ssh_tunnel_profile_id {
            let tunnel = ssh_tunnels
                .iter()
                .find(|t| t.id == tunnel_profile_id)?;

            if !tunnel.save_secret {
                return None;
            }

            return self.get_ssh_tunnel_secret(tunnel);
        }

        // If using inline SSH config, get secret from profile's SSH secret store
        if ssh_tunnel.is_some() {
            return self.get_ssh_password(profile);
        }

        None
    }
}
