//! `SavedDocumentQueryManager` — SQLite-backed manager for queries saved from
//! the document query builder.
//!
//! Wraps `DocumentQueryRepo` from `dbflux_storage` and caches the list of each
//! collection scope for synchronous reads. Writes go to the repository first;
//! the cache of the scope is rebuilt only after a successful write. A write
//! that committed succeeds even when that rebuild fails: the failure is
//! logged and the scope is read again on the next `list`, which reports it.

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

    /// The saved query `id` of `scope` with its spec, or `None` when it no
    /// longer exists there.
    pub fn load(
        &self,
        id: &str,
        scope: &DocumentQueryScope,
    ) -> Result<Option<SavedDocumentQuery>, StorageError> {
        self.repo.get(id, scope)
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
        self.reload_after_write(scope);

        Ok(summary)
    }

    /// Deletes the saved query `id` of `scope`. Returns whether it existed.
    pub fn delete(&mut self, id: &str, scope: &DocumentQueryScope) -> Result<bool, StorageError> {
        let deleted = self.repo.delete(id, scope)?;
        self.reload_after_write(scope);

        Ok(deleted)
    }

    /// Rebuilds the cache of `scope` after a committed write. On failure the
    /// scope stays uncached, so the next `list` reads the repository again.
    fn reload_after_write(&mut self, scope: &DocumentQueryScope) {
        self.cache.remove(scope);

        match self.repo.list_for_scope(scope) {
            Ok(rows) => {
                self.cache.insert(scope.clone(), rows);
            }
            Err(error) => log::warn!(
                "saved document queries of {}.{} could not be reloaded after a write: {error}",
                scope.database,
                scope.collection
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

    use super::*;
    use dbflux_core::DocumentQueryMode;
    use dbflux_storage::bootstrap::StorageRuntime;

    fn manager() -> SavedDocumentQueryManager {
        manager_and_connection().0
    }

    fn manager_and_connection() -> (
        SavedDocumentQueryManager,
        Arc<std::sync::Mutex<rusqlite::Connection>>,
    ) {
        let runtime = StorageRuntime::in_memory().expect("runtime");
        let conn = runtime.viz_connection().expect("connection");
        let manager =
            SavedDocumentQueryManager::new(Arc::new(DocumentQueryRepo::new(Arc::clone(&conn))));
        (manager, conn)
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

        let saved = manager
            .load(&summary.id, &scope())
            .unwrap()
            .expect("exists");
        assert_eq!(saved.spec, spec(10));
        assert_eq!(manager.load("missing", &scope()).unwrap(), None);
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

    #[test]
    fn a_committed_write_succeeds_when_the_list_cannot_be_reloaded() {
        let (mut manager, conn) = manager_and_connection();
        let kept = manager.save(&scope(), "kept", &spec(1)).unwrap();

        conn.lock()
            .unwrap()
            .execute(
                "INSERT INTO qry_saved_document_queries
                     (id, profile_id, database_name, collection_name, name, mode,
                      format_version, spec_json, created_at, updated_at)
                 VALUES ('unlistable', 'profile-1', 'shop', 'orders', 'unlistable', 'find', 1,
                         '{}', 1, 'not a timestamp')",
                [],
            )
            .unwrap();
        assert!(manager.repo.list_for_scope(&scope()).is_err());

        let saved = manager.save(&scope(), "recent", &spec(10));
        assert_eq!(
            saved.map(|summary| summary.name).ok(),
            Some("recent".to_string())
        );

        assert!(matches!(manager.delete(&kept.id, &scope()), Ok(true)));
        assert!(
            manager.list(&scope()).is_err(),
            "the next list reads the repository again"
        );
    }
}
