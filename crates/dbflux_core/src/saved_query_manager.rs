use crate::{SavedQuery, SavedQueryStore};
use log::{error, info};
use uuid::Uuid;

pub struct SavedQueryManager {
    store: Option<SavedQueryStore>,
    #[allow(dead_code)]
    pending_warning: Option<String>,
}

impl SavedQueryManager {
    pub fn new() -> Self {
        let (store, pending_warning) = match SavedQueryStore::new() {
            Ok(mut store) => {
                let warning = store.take_load_warning();
                info!("Loaded {} saved queries", store.get_all().len());
                (Some(store), warning)
            }
            Err(e) => {
                error!("Failed to create saved query store: {:?}", e);
                (None, None)
            }
        };

        Self {
            store,
            pending_warning,
        }
    }

    #[allow(dead_code)]
    pub fn take_warning(&mut self) -> Option<String> {
        self.pending_warning.take()
    }

    pub fn queries(&self) -> &[SavedQuery] {
        self.store.as_ref().map(|s| s.get_all()).unwrap_or(&[])
    }

    pub fn add(&mut self, query: SavedQuery) {
        if let Some(ref mut store) = self.store {
            store.add(query);
            if let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
        }
    }

    pub fn update(&mut self, id: Uuid, name: String, sql: String) -> bool {
        if let Some(ref mut store) = self.store {
            let updated = store.update(id, name, sql);
            if updated && let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
            return updated;
        }
        false
    }

    pub fn remove(&mut self, id: Uuid) -> bool {
        if let Some(ref mut store) = self.store {
            let removed = store.remove(id);
            if removed && let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
            return removed;
        }
        false
    }

    pub fn toggle_favorite(&mut self, id: Uuid) -> bool {
        if let Some(ref mut store) = self.store {
            let result = store.toggle_favorite(id);
            if let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
            return result;
        }
        false
    }

    pub fn update_last_used(&mut self, id: Uuid) -> bool {
        if let Some(ref mut store) = self.store {
            let result = store.update_last_used(id);
            if result && let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
            return result;
        }
        false
    }

    #[allow(dead_code)]
    pub fn update_sql(&mut self, id: Uuid, sql: &str) -> bool {
        if let Some(ref mut store) = self.store {
            let result = store.update_sql(id, sql);
            if result && let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
            return result;
        }
        false
    }

    #[allow(dead_code)]
    pub fn update_name(&mut self, id: Uuid, name: &str) -> bool {
        if let Some(ref mut store) = self.store {
            let result = store.update_name(id, name);
            if result && let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
            return result;
        }
        false
    }

    #[allow(dead_code)]
    pub fn get(&self, id: Uuid) -> Option<&SavedQuery> {
        self.store.as_ref().and_then(|s| s.get(id))
    }
}

impl Default for SavedQueryManager {
    fn default() -> Self {
        Self::new()
    }
}
