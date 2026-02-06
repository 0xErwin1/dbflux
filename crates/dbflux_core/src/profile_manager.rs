use crate::{ConnectionProfile, ProfileStore};
use log::{error, info};
use uuid::Uuid;

pub struct ProfileManager {
    pub profiles: Vec<ConnectionProfile>,
    store: Option<ProfileStore>,
}

impl ProfileManager {
    pub fn new() -> Self {
        let (store, profiles) = match ProfileStore::new() {
            Ok(store) => {
                let profiles = store.load().unwrap_or_else(|e| {
                    error!("Failed to load profiles: {:?}", e);
                    Vec::new()
                });
                info!("Loaded {} profiles from disk", profiles.len());
                (Some(store), profiles)
            }
            Err(e) => {
                error!("Failed to create profile store: {:?}", e);
                error!("Application will run without persistent profile storage");
                (None, Vec::new())
            }
        };

        Self { profiles, store }
    }

    pub fn save(&self) {
        let Some(ref store) = self.store else {
            log::warn!("Cannot save profiles: profile store not available");
            return;
        };

        if let Err(e) = store.save(&self.profiles) {
            error!("Failed to save profiles: {:?}", e);
        } else {
            info!("Saved {} profiles to disk", self.profiles.len());
        }
    }

    pub fn update(&mut self, profile: ConnectionProfile) {
        if let Some(existing) = self.profiles.iter_mut().find(|p| p.id == profile.id) {
            *existing = profile;
            self.save();
        }
    }

    pub fn find_by_id(&self, id: Uuid) -> Option<&ConnectionProfile> {
        self.profiles.iter().find(|p| p.id == id)
    }

    pub fn add(&mut self, profile: ConnectionProfile) {
        self.profiles.push(profile);
        self.save();
    }

    pub fn remove(&mut self, idx: usize) -> Option<ConnectionProfile> {
        if idx < self.profiles.len() {
            let removed = self.profiles.remove(idx);
            self.save();
            Some(removed)
        } else {
            None
        }
    }

    pub fn profile_ids(&self) -> Vec<Uuid> {
        self.profiles.iter().map(|p| p.id).collect()
    }
}

impl Default for ProfileManager {
    fn default() -> Self {
        Self::new()
    }
}
