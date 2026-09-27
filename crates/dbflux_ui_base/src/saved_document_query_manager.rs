//! `SavedDocumentQueryManager` — SQLite-backed manager for queries saved from
//! the document query builder.
//!
//! Wraps `DocumentQueryRepo` from `dbflux_storage` and caches the list of each
//! collection scope for synchronous reads. Writes go to the repository first;
//! the cache of the scope is rebuilt only after a successful write.

use std::collections::HashMap;
use std::sync::Arc;

use dbflux_core::DocumentQuerySpec;
use dbflux_storage::error::StorageError;
use dbflux_storage::{
    DocumentQueryRepo, DocumentQueryScope, SavedDocumentQuery, SavedDocumentQuerySummary,
};

/// Saved document queries, listed per collection.
pub struct SavedDocumentQueryManager {
    repo: Arc<DocumentQueryRepo>,
    cache: HashMap<DocumentQueryScope, Vec<SavedDocumentQuerySummary>>,
}

impl SavedDocumentQueryManager {
    pub fn new(repo: Arc<DocumentQueryRepo>) -> Self {
        Self {
            repo,
            cache: HashMap::new(),
        }
    }

    /// Saved queries of `scope`, most recently saved first. Read from the
    /// repository the first time, from the cache afterwards.
    pub fn list(
        &mut self,
        scope: &DocumentQueryScope,
    ) -> Result<Vec<SavedDocumentQuerySummary>, StorageError> {
        if let Some(cached) = self.cache.get(scope) {
            return Ok(cached.clone());
        }

        let rows = self.repo.list_for_scope(scope)?;
        self.cache.insert(scope.clone(), rows.clone());

        Ok(rows)
    }

    /// The saved query `id` with its spec, or `None` when it no longer exists.
    pub fn load(&self, id: &str) -> Result<Option<SavedDocumentQuery>, StorageError> {
        self.repo.get(id)
    }

    /// Saves `spec` as `name` in `scope`, replacing a query of that name.
    /// The name is trimmed and must not be empty.
    pub fn save(
        &mut self,
        scope: &DocumentQueryScope,
        name: &str,
        spec: &DocumentQuerySpec,
    ) -> Result<SavedDocumentQuerySummary, StorageError> {
        let name = name.trim();
        if name.is_empty() {
            return Err(StorageError::Data(
                "a saved document query needs a name".to_string(),
            ));
        }

        let summary = self.repo.upsert_by_name(scope, name, spec)?;
        self.reload(scope)?;

        Ok(summary)
    }

    /// Deletes the saved query `id` of `scope`. Returns whether it existed.
    pub fn delete(&mut self, id: &str, scope: &DocumentQueryScope) -> Result<bool, StorageError> {
        let deleted = self.repo.delete(id)?;
        self.reload(scope)?;

        Ok(deleted)
    }

    fn reload(&mut self, scope: &DocumentQueryScope) -> Result<(), StorageError> {
        self.cache.remove(scope);

        let rows = self.repo.list_for_scope(scope)?;
        self.cache.insert(scope.clone(), rows);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

    use super::*;
    use dbflux_core::DocumentQueryMode;
    use dbflux_storage::bootstrap::StorageRuntime;

    fn manager() -> SavedDocumentQueryManager {
        let runtime = StorageRuntime::in_memory().expect("runtime");
        let conn = runtime.viz_connection().expect("connection");
        SavedDocumentQueryManager::new(Arc::new(DocumentQueryRepo::new(conn)))
    }

    fn scope() -> DocumentQueryScope {
        DocumentQueryScope::new("profile-1", "shop", "orders")
    }

    fn spec(limit: u64) -> DocumentQuerySpec {
        DocumentQuerySpec {
            limit: Some(limit),
            ..DocumentQuerySpec::default()
        }
    }

    #[test]
    fn a_save_shows_in_the_list_of_its_scope_only() {
        let mut manager = manager();
        assert!(manager.list(&scope()).unwrap().is_empty());

        manager.save(&scope(), "  recent  ", &spec(10)).unwrap();

        let list = manager.list(&scope()).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "recent", "the name is trimmed");
        assert_eq!(list[0].mode, DocumentQueryMode::Find);

        let elsewhere = DocumentQueryScope::new("profile-1", "shop", "customers");
        assert!(manager.list(&elsewhere).unwrap().is_empty());
    }

    #[test]
    fn a_blank_name_is_refused_and_nothing_is_written() {
        let mut manager = manager();

        assert!(matches!(
            manager.save(&scope(), "   ", &spec(10)),
            Err(StorageError::Data(_))
        ));
        assert!(manager.list(&scope()).unwrap().is_empty());
    }

    #[test]
    fn load_returns_the_saved_spec() {
        let mut manager = manager();
        let summary = manager.save(&scope(), "recent", &spec(10)).unwrap();

        let saved = manager.load(&summary.id).unwrap().expect("exists");
        assert_eq!(saved.spec, spec(10));
        assert_eq!(manager.load("missing").unwrap(), None);
    }

    #[test]
    fn delete_updates_the_cached_list() {
        let mut manager = manager();
        let summary = manager.save(&scope(), "recent", &spec(10)).unwrap();
        manager.save(&scope(), "older", &spec(5)).unwrap();
        assert_eq!(manager.list(&scope()).unwrap().len(), 2);

        assert!(manager.delete(&summary.id, &scope()).unwrap());

        let names: Vec<String> = manager
            .list(&scope())
            .unwrap()
            .into_iter()
            .map(|summary| summary.name)
            .collect();
        assert_eq!(names, vec!["older".to_string()]);
    }
}
