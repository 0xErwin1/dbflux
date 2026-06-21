use crate::auth::AuthProfile;
use crate::{ConnectionProfile, ProxyProfile, SshTunnelProfile};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

pub trait Identifiable {
    fn id(&self) -> Uuid;
}

/// In-memory CRUD manager. Items are loaded externally (from SQLite) and
/// mutated in memory; callers persist changes via their own repositories.
pub struct ItemManager<T> {
    pub items: Vec<T>,
    label: &'static str,
}

impl<T: Identifiable + Serialize + DeserializeOwned> ItemManager<T> {
    pub fn new(_filename: &str, label: &'static str) -> Self {
        Self {
            items: Vec::new(),
            label,
        }
    }

    /// Creates a manager with pre-loaded items.
    pub fn with_items(items: Vec<T>, _store: Option<()>, label: &'static str) -> Self {
        Self { items, label }
    }

    pub fn save(&self) {
        log::warn!("Cannot save {}: no store attached", self.label);
    }

    pub fn add(&mut self, item: T) {
        self.items.push(item);
    }

    pub fn remove(&mut self, idx: usize) -> Option<T> {
        if idx < self.items.len() {
            Some(self.items.remove(idx))
        } else {
            None
        }
    }

    pub fn update(&mut self, item: T) {
        let target_id = item.id();
        if let Some(existing) = self.items.iter_mut().find(|i| i.id() == target_id) {
            *existing = item;
        }
    }
}

impl<T: Identifiable + Serialize + DeserializeOwned> Default for ItemManager<T>
where
    Self: DefaultFilename,
{
    fn default() -> Self {
        let meta = Self::meta();
        Self::new(meta.0, meta.1)
    }
}

/// Filename/label metadata so `Default` works on `ItemManager` type aliases.
pub trait DefaultFilename {
    fn meta() -> (&'static str, &'static str);
}

impl Identifiable for ProxyProfile {
    fn id(&self) -> Uuid {
        self.id
    }
}

impl Identifiable for SshTunnelProfile {
    fn id(&self) -> Uuid {
        self.id
    }
}

impl Identifiable for ConnectionProfile {
    fn id(&self) -> Uuid {
        self.id
    }
}

impl Identifiable for AuthProfile {
    fn id(&self) -> Uuid {
        self.id
    }
}

pub type AuthProfileManager = ItemManager<AuthProfile>;

impl DefaultFilename for AuthProfileManager {
    fn meta() -> (&'static str, &'static str) {
        ("auth_profiles.json", "auth profiles")
    }
}
