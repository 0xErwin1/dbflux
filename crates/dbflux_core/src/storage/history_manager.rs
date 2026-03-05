use crate::{HistoryEntry, HistoryStore};
use log::{error, info};
use uuid::Uuid;

pub struct HistoryManager {
    store: Option<HistoryStore>,
}

impl HistoryManager {
    pub fn new() -> Self {
        let store = match HistoryStore::new() {
            Ok(store) => {
                info!("Loaded {} history entries", store.entries().len());
                Some(store)
            }
            Err(e) => {
                error!("Failed to create history store: {:?}", e);
                None
            }
        };

        Self { store }
    }

    pub fn set_max_entries(&mut self, max: usize) {
        if let Some(ref mut store) = self.store {
            store.set_max_entries(max);
        }
    }

    pub fn entries(&self) -> &[HistoryEntry] {
        self.store.as_ref().map(|s| s.entries()).unwrap_or(&[])
    }

    pub fn add(&mut self, entry: HistoryEntry) {
        if let Some(ref mut store) = self.store {
            store.add(entry);
            if let Err(e) = store.save() {
                error!("Failed to save history: {:?}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub fn toggle_favorite(&mut self, id: Uuid) -> bool {
        if let Some(ref mut store) = self.store {
            let result = store.toggle_favorite(id);
            if let Err(e) = store.save() {
                error!("Failed to save history: {:?}", e);
            }
            return result;
        }
        false
    }

    #[allow(dead_code)]
    pub fn remove(&mut self, id: Uuid) {
        if let Some(ref mut store) = self.store {
            store.remove(id);
            if let Err(e) = store.save() {
                error!("Failed to save history: {:?}", e);
            }
        }
    }
}

impl Default for HistoryManager {
    fn default() -> Self {
        Self::new()
    }
}
