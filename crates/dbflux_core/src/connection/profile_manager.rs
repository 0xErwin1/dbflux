use crate::ConnectionProfile;
use std::collections::HashMap;
use uuid::Uuid;

pub struct ProfileManager {
    pub profiles: Vec<ConnectionProfile>,
    profile_load_errors: HashMap<Uuid, String>,
}

impl ProfileManager {
    /// Creates a manager with pre-loaded profiles.
    pub fn with_profiles(profiles: Vec<ConnectionProfile>, _store: Option<()>) -> Self {
        Self::with_profiles_and_errors(profiles, HashMap::new())
    }

    /// Creates a manager with pre-loaded profiles and profile-specific load errors.
    pub fn with_profiles_and_errors(
        profiles: Vec<ConnectionProfile>,
        profile_load_errors: HashMap<Uuid, String>,
    ) -> Self {
        Self {
            profiles,
            profile_load_errors,
        }
    }

    /// Creates an empty in-memory manager.
    pub fn new() -> Self {
        Self::with_profiles(Vec::new(), None)
    }

    /// Creates a new in-memory ProfileManager that does not persist to disk.
    pub fn new_in_memory() -> Self {
        Self {
            profiles: Vec::new(),
            profile_load_errors: HashMap::new(),
        }
    }

    pub fn save(&self) {
        log::warn!("Cannot save profiles: no profile store attached");
    }

    pub fn update(&mut self, profile: ConnectionProfile) {
        self.profile_load_errors.remove(&profile.id);
        if let Some(existing) = self.profiles.iter_mut().find(|p| p.id == profile.id) {
            *existing = profile;
        }
    }

    pub fn find_by_id(&self, id: Uuid) -> Option<&ConnectionProfile> {
        self.profiles.iter().find(|p| p.id == id)
    }

    /// Returns the hydration error recorded for a profile, if any.
    pub fn profile_load_error(&self, id: Uuid) -> Option<&str> {
        self.profile_load_errors.get(&id).map(String::as_str)
    }

    pub fn add(&mut self, profile: ConnectionProfile) {
        self.profile_load_errors.remove(&profile.id);
        self.profiles.push(profile);
    }

    pub fn remove(&mut self, idx: usize) -> Option<ConnectionProfile> {
        if idx < self.profiles.len() {
            let profile = self.profiles.remove(idx);
            self.profile_load_errors.remove(&profile.id);
            Some(profile)
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
