use crate::{SshTunnelProfile, SshTunnelStore};
use log::{error, info};

pub struct SshTunnelManager {
    pub tunnels: Vec<SshTunnelProfile>,
    store: Option<SshTunnelStore>,
}

impl SshTunnelManager {
    pub fn new() -> Self {
        let (store, tunnels) = match SshTunnelStore::new() {
            Ok(store) => {
                let tunnels = store.load().unwrap_or_else(|e| {
                    error!("Failed to load SSH tunnels: {:?}", e);
                    Vec::new()
                });
                info!("Loaded {} SSH tunnel profiles from disk", tunnels.len());
                (Some(store), tunnels)
            }
            Err(e) => {
                error!("Failed to create SSH tunnel store: {:?}", e);
                (None, Vec::new())
            }
        };

        Self { tunnels, store }
    }

    pub fn save(&self) {
        let Some(ref store) = self.store else {
            log::warn!("Cannot save SSH tunnels: store not available");
            return;
        };

        if let Err(e) = store.save(&self.tunnels) {
            error!("Failed to save SSH tunnels: {:?}", e);
        } else {
            info!("Saved {} SSH tunnels to disk", self.tunnels.len());
        }
    }

    pub fn add(&mut self, tunnel: SshTunnelProfile) {
        self.tunnels.push(tunnel);
        self.save();
    }

    #[allow(dead_code)]
    pub fn remove(&mut self, idx: usize) -> Option<SshTunnelProfile> {
        if idx < self.tunnels.len() {
            let removed = self.tunnels.remove(idx);
            self.save();
            Some(removed)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn update(&mut self, tunnel: SshTunnelProfile) {
        if let Some(existing) = self.tunnels.iter_mut().find(|t| t.id == tunnel.id) {
            *existing = tunnel;
            self.save();
        }
    }
}

impl Default for SshTunnelManager {
    fn default() -> Self {
        Self::new()
    }
}
