use crate::ConnectionProfile;
use uuid::Uuid;

pub struct ProfileManager {
    pub profiles: Vec<ConnectionProfile>,
}

impl ProfileManager {
    /// Creates a manager with pre-loaded profiles.
    pub fn with_profiles(profiles: Vec<ConnectionProfile>, _store: Option<()>) -> Self {
        Self { profiles }
    }

    /// Creates an empty in-memory manager.
    pub fn new() -> Self {
        Self::with_profiles(Vec::new(), None)
    }

    /// Creates a new in-memory ProfileManager that does not persist to disk.
    pub fn new_in_memory() -> Self {
        Self {
            profiles: Vec::new(),
        }
    }

    pub fn save(&self) {
        log::warn!("Cannot save profiles: no profile store attached");
    }

    pub fn update(&mut self, profile: ConnectionProfile) {
        if let Some(existing) = self.profiles.iter_mut().find(|p| p.id == profile.id) {
            *existing = profile;
        }
    }

    pub fn find_by_id(&self, id: Uuid) -> Option<&ConnectionProfile> {
        self.profiles.iter().find(|p| p.id == id)
    }

    pub fn add(&mut self, profile: ConnectionProfile) {
        self.profiles.push(profile);
    }

    pub fn remove(&mut self, idx: usize) -> Option<ConnectionProfile> {
        if idx < self.profiles.len() {
            Some(self.profiles.remove(idx))
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
