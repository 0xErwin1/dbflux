//! Saved filter management for the audit view.
//!
//! Provides UI components for saving, loading, and managing saved filter presets.

use dbflux_core::observability::EventSource;
use dbflux_storage::repositories::saved_filters::{SavedFilterDto, SavedFiltersRepository};

use crate::ui::document::audit::filters::AuditFilters;

/// Item for displaying a saved filter in a list.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SavedFilterItem {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
}

/// Manages saved filter operations.
pub struct SavedFilterManager {
    repo: SavedFiltersRepository,
}

impl SavedFilterManager {
    /// Creates a new manager with the given repository.
    pub fn new(repo: SavedFiltersRepository) -> Self {
        Self { repo }
    }

    /// Lists all saved filters.
    pub fn list(&self) -> Result<Vec<SavedFilterItem>, String> {
        self.repo
            .list()
            .map(|filters| {
                filters
                    .into_iter()
                    .map(|f| SavedFilterItem {
                        id: f.id.unwrap_or(0),
                        name: f.name,
                        description: f.description,
                    })
                    .collect()
            })
            .map_err(|e| format!("Failed to list saved filters: {}", e))
    }

    /// Saves a filter with the given name.
    pub fn save(
        &self,
        name: &str,
        description: Option<&str>,
        filters: &AuditFilters,
    ) -> Result<(), String> {
        let filter_json = filters.to_json();
        let dto = SavedFilterDto {
            id: None,
            name: name.to_string(),
            description: description.map(String::from),
            filter_json,
            created_at: String::new(),
            updated_at: String::new(),
        };
        self.repo
            .upsert(&dto)
            .map_err(|e| format!("Failed to save filter: {}", e))
    }

    /// Loads a filter by ID.
    #[allow(dead_code)]
    pub fn load(&self, id: i64) -> Result<Option<AuditFilters>, String> {
        self.repo
            .get_by_id(id)
            .map(|opt| opt.map(|f| AuditFilters::from_json(&f.filter_json)))
            .map_err(|e| format!("Failed to load filter: {}", e))
    }

    /// Deletes a filter by ID.
    #[allow(dead_code)]
    pub fn delete(&self, id: i64) -> Result<(), String> {
        self.repo
            .delete(id)
            .map_err(|e| format!("Failed to delete filter: {}", e))
    }
}
