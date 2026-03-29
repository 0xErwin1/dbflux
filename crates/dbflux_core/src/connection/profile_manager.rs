use crate::{ConnectionProfile, ProfileStore};
use log::{error, info};
use uuid::Uuid;

pub struct ProfileManager {
    pub profiles: Vec<ConnectionProfile>,
    store: Option<ProfileStore>,
}

impl ProfileManager {
    /// Creates a manager with pre-loaded profiles and an optional store.
    pub fn with_profiles(profiles: Vec<ConnectionProfile>, store: Option<ProfileStore>) -> Self {
        Self { profiles, store }
    }

    /// Loads profiles from the JSON store (legacy path). Returns an empty manager
    /// if the store cannot be created or loaded.
    pub fn new() -> Self {
        Self::with_profiles(Vec::new(), None)
    }

    /// Creates a new in-memory ProfileManager that does not persist to disk.
    /// Use this for tests that should not pollute ~/.config/dbflux/.
    pub fn new_in_memory() -> Self {
        Self {
            profiles: Vec::new(),
            store: None,
        }
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
