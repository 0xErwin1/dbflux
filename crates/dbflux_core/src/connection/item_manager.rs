use crate::auth::AuthProfile;
use crate::{ConnectionProfile, JsonStore, ProxyProfile, SshTunnelProfile};
use log::{error, info};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

pub trait Identifiable {
    fn id(&self) -> Uuid;
}

/// CRUD manager backed by a `JsonStore<T>`, with auto-save on mutation.
pub struct ItemManager<T> {
    pub items: Vec<T>,
    store: Option<JsonStore<T>>,
    label: &'static str,
}

impl<T: Identifiable + Serialize + DeserializeOwned> ItemManager<T> {
    pub fn new(filename: &str, label: &'static str) -> Self {
        let (store, items) = match JsonStore::new(filename) {
            Ok(store) => {
                let items = store.load().unwrap_or_else(|e| {
                    error!("Failed to load {}: {:?}", label, e);
                    Vec::new()
                });
                info!("Loaded {} {} from disk", items.len(), label);
                (Some(store), items)
            }
            Err(e) => {
                error!("Failed to create {} store: {:?}", label, e);
                (None, Vec::new())
            }
        };

        Self {
            items,
            store,
            label,
        }
    }

    pub fn save(&self) {
        let Some(ref store) = self.store else {
            log::warn!("Cannot save {}: store not available", self.label);
            return;
        };

        if let Err(e) = store.save(&self.items) {
            error!("Failed to save {}: {:?}", self.label, e);
        } else {
            info!("Saved {} {} to disk", self.items.len(), self.label);
        }
    }

    pub fn add(&mut self, item: T) {
        self.items.push(item);
        self.save();
    }

    pub fn remove(&mut self, idx: usize) -> Option<T> {
        if idx < self.items.len() {
            let removed = self.items.remove(idx);
            self.save();
            Some(removed)
        } else {
            None
        }
    }

    pub fn update(&mut self, item: T) {
        let target_id = item.id();
        if let Some(existing) = self.items.iter_mut().find(|i| i.id() == target_id) {
            *existing = item;
            self.save();
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

#[cfg(test)]
impl<T: Identifiable + Serialize + DeserializeOwned> ItemManager<T> {
    pub fn with_store(store: JsonStore<T>, label: &'static str) -> Result<Self, crate::DbError> {
        let items = store.load()?;
        Ok(Self {
            items,
            store: Some(store),
            label,
        })
    }
}
