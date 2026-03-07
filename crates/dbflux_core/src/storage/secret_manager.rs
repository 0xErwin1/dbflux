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

    pub fn save_password(&self, profile: &ConnectionProfile, password: &SecretString) {
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
            DbConfig::SQLite { .. } | DbConfig::External { .. } => return None,
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
