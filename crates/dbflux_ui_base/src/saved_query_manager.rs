//! `SavedQueryManager` — SQLite-backed manager for visual saved queries.
//!
//! Wraps `SavedQueryRepo` from `dbflux_storage` and keeps a per-profile
//! in-memory cache of `SavedQuerySummary` items for synchronous reads.
//!
//! Writes go through the repository first; the cache is updated only on
//! success. Cross-connection imports verify table existence on the target
//! connection before writing any row.

use std::sync::Arc;

use dbflux_core::{Connection, VisualQuerySpec};
use dbflux_storage::{SavedQueryRepo, SavedQuerySummary, error::StorageError};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// TableProbe trait — seam for cross-connection table-existence check
// ---------------------------------------------------------------------------

/// Minimal seam used by `import_to` to verify the source table exists on the
/// target connection before writing a new saved-query row.
///
/// Implemented by a thin wrapper around `Box<dyn Connection>` in production
/// and by `MockTableProbe` in tests.
pub trait TableProbe {
    /// Returns `Ok(())` when the table exists on this connection, `Err` when
    /// it does not or when the probe cannot be executed (connection offline,
    /// driver error, etc.).
    fn check_table_exists(&self, schema: Option<&str>, table: &str) -> Result<(), StorageError>;
}

/// Production adapter: wraps a live `Connection` reference and delegates to
/// `table_details`, which fetches schema metadata from the driver.
pub struct ConnectionTableProbe<'a> {
    connection: &'a dyn Connection,
    database: &'a str,
}

impl<'a> ConnectionTableProbe<'a> {
    /// Creates a probe bound to the given connection and active database.
    pub fn new(connection: &'a dyn Connection, database: &'a str) -> Self {
        Self {
            connection,
            database,
        }
    }
}

impl<'a> TableProbe for ConnectionTableProbe<'a> {
    fn check_table_exists(&self, schema: Option<&str>, table: &str) -> Result<(), StorageError> {
        self.connection
            .table_details(self.database, schema, table)
            .map(|_| ())
            .map_err(|e| StorageError::Data(format!("table not found: {e}")))
    }
}

// ---------------------------------------------------------------------------
// SavedQueryManager
// ---------------------------------------------------------------------------

/// In-memory manager for saved visual queries, backed by `SavedQueryRepo`.
///
/// Per-profile caches are loaded lazily on first `list` call and kept up to
/// date on every successful write. Reads are synchronous; writes go through
/// the repository first.
pub struct SavedQueryManager {
    repo: Arc<SavedQueryRepo>,
    cache: std::collections::HashMap<String, Vec<SavedQuerySummary>>,
}

impl SavedQueryManager {
    /// Creates a new manager wrapping the given repository.
    ///
    /// The in-memory cache starts empty; it is populated lazily on the first
    /// `list` call for each profile.
    pub fn new(repo: Arc<SavedQueryRepo>) -> Self {
        Self {
            repo,
            cache: std::collections::HashMap::new(),
        }
    }

    /// Lists saved queries for `profile_id`, ordered by `updated_at DESC`.
    ///
    /// Fetches from the repository on the first call for a profile and caches
    /// the result. Subsequent calls return the in-memory list.
    pub fn list(&mut self, profile_id: &str) -> Vec<SavedQuerySummary> {
        if let Some(cached) = self.cache.get(profile_id) {
            return cached.clone();
        }

        let rows = match self.repo.list_for_profile(profile_id) {
            Ok(rows) => rows,
            Err(e) => {
                log::warn!("SavedQueryManager: failed to load list for profile {profile_id}: {e}");
                Vec::new()
            }
        };

        self.cache.insert(profile_id.to_string(), rows.clone());

        rows
    }

    /// Loads the full `VisualQuerySpec` for a saved query by its UUID string.
    ///
    /// Returns `None` when no row with that id exists.
    pub fn load(&self, id: &str) -> Result<Option<VisualQuerySpec>, StorageError> {
        self.repo.get(id)
    }

    /// Saves a named spec for `profile_id`.
    ///
    /// Uses upsert-by-name semantics: if a row with `(profile_id, name)` already
    /// exists it is updated in place; otherwise a new UUID v7 row is created.
    /// The cache for `profile_id` is invalidated and re-built from the updated
    /// summary on success.
    pub fn save(
        &mut self,
        profile_id: &str,
        name: &str,
        spec: &VisualQuerySpec,
    ) -> Result<SavedQuerySummary, StorageError> {
        let summary = self.repo.upsert_by_name(profile_id, name, spec)?;

        self.invalidate_and_reload(profile_id);

        Ok(summary)
    }

    /// Saves a spec under an explicit `id` (used when updating an existing row
    /// by the row's stable UUID).
    ///
    /// Returns `Err` when the underlying write fails; the cache is not touched
    /// on failure.
    pub fn save_by_id(
        &mut self,
        id: &str,
        profile_id: &str,
        name: &str,
        spec: &VisualQuerySpec,
    ) -> Result<SavedQuerySummary, StorageError> {
        let summary = self.repo.upsert_by_id(id, profile_id, name, spec)?;

        self.invalidate_and_reload(profile_id);

        Ok(summary)
    }

    /// Renames a saved query by its id.
    ///
    /// Returns `Err` when the new name conflicts with an existing row in the
    /// same profile (UNIQUE constraint). The cache is invalidated and rebuilt
    /// from the repository on success only.
    pub fn rename(
        &mut self,
        id: &str,
        profile_id: &str,
        new_name: &str,
    ) -> Result<(), StorageError> {
        self.repo.rename(id, new_name)?;

        self.invalidate_and_reload(profile_id);

        Ok(())
    }

    /// Forks a saved query into a new row with a new UUID v7 id.
    ///
    /// The new row is created under `target_profile_id` with the name
    /// `"<original_name> (copy)"`. Returns the summary of the new row.
    ///
    /// Returns `Err` when the source id is not found or the write fails.
    pub fn fork(
        &mut self,
        source_id: &str,
        target_profile_id: &str,
    ) -> Result<SavedQuerySummary, StorageError> {
        let source = self
            .repo
            .get(source_id)?
            .ok_or_else(|| StorageError::Data(format!("saved query not found: {source_id}")))?;

        let source_summary = self
            .repo
            .list_for_profile(target_profile_id)?
            .into_iter()
            .chain(self.repo.list_all()?)
            .find(|s| s.id == source_id)
            .ok_or_else(|| StorageError::Data(format!("saved query not found: {source_id}")))?;

        let fork_name = format!("{} (copy)", source_summary.name);
        let new_id = Uuid::now_v7().to_string();

        let summary = self
            .repo
            .upsert_by_id(&new_id, target_profile_id, &fork_name, &source)?;

        self.invalidate_and_reload(target_profile_id);

        Ok(summary)
    }

    /// Deletes a saved query by its id.
    ///
    /// The cache entry for `profile_id` is rebuilt from the repository on
    /// success. On failure the cache is not modified.
    pub fn delete(&mut self, id: &str, profile_id: &str) -> Result<(), StorageError> {
        self.repo.delete(id)?;

        self.invalidate_and_reload(profile_id);

        Ok(())
    }

    /// Imports a saved query from `source_id` into `target_profile_id`.
    ///
    /// The import verifies that the source table exists on the target connection
    /// via `probe.check_table_exists` before writing. On failure no row is
    /// written. On success a new row is created with a new UUID v7 id, the
    /// source spec, and the target profile.
    ///
    /// Per design decision 7: hard-fail when offline or table absent; there is
    /// no deferred / retry path in v1.
    pub fn import_to(
        &mut self,
        source_id: &str,
        target_profile_id: &str,
        probe: &dyn TableProbe,
    ) -> Result<SavedQuerySummary, StorageError> {
        let source_spec = self
            .repo
            .get(source_id)?
            .ok_or_else(|| StorageError::Data(format!("saved query not found: {source_id}")))?;

        probe.check_table_exists(
            source_spec.source.schema.as_deref(),
            &source_spec.source.table,
        )?;

        let source_name = self
            .repo
            .list_all()?
            .into_iter()
            .find(|s| s.id == source_id)
            .map(|s| s.name)
            .unwrap_or_else(|| source_spec.source.table.clone());

        let new_id = Uuid::now_v7().to_string();
        let summary =
            self.repo
                .upsert_by_id(&new_id, target_profile_id, &source_name, &source_spec)?;

        self.invalidate_and_reload(target_profile_id);

        Ok(summary)
    }

    fn invalidate_and_reload(&mut self, profile_id: &str) {
        self.cache.remove(profile_id);

        match self.repo.list_for_profile(profile_id) {
            Ok(rows) => {
                self.cache.insert(profile_id.to_string(), rows);
            }
            Err(e) => {
                log::warn!(
                    "SavedQueryManager: failed to reload cache for profile {profile_id}: {e}"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{Projection, SourceTable, VisualQuerySpec};
    use dbflux_storage::bootstrap::StorageRuntime;
    use rusqlite::Connection as RusqliteConn;
    use std::sync::{Arc, Mutex};

    // ---- helpers -----------------------------------------------------------

    fn make_manager() -> SavedQueryManager {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");
        SavedQueryManager::new(Arc::new(SavedQueryRepo::new(conn)))
    }

    fn make_manager_from_conn(conn: Arc<Mutex<RusqliteConn>>) -> SavedQueryManager {
        SavedQueryManager::new(Arc::new(SavedQueryRepo::new(conn)))
    }

    fn sample_spec(table: &str) -> VisualQuerySpec {
        VisualQuerySpec {
            source: SourceTable {
                schema: None,
                table: table.to_string(),
                alias: table.to_string(),
            },
            projection: Projection::All,
            joins: vec![],
            filter: None,
            group_by: vec![],
            aggregates: vec![],
            having: None,
            sort: vec![],
            limit: Some(100),
            offset: 0,
        }
    }

    fn insert_profile(conn: &Arc<Mutex<RusqliteConn>>, profile_id: &str) {
        conn.lock()
            .unwrap()
            .execute(
                "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, ?2)",
                rusqlite::params![profile_id, "test-profile"],
            )
            .expect("insert test profile");
    }

    // ---- MockTableProbe for import tests -----------------------------------

    struct MockTableProbe {
        succeeds: bool,
    }

    impl TableProbe for MockTableProbe {
        fn check_table_exists(
            &self,
            _schema: Option<&str>,
            _table: &str,
        ) -> Result<(), StorageError> {
            if self.succeeds {
                Ok(())
            } else {
                Err(StorageError::Data("table not found".to_string()))
            }
        }
    }

    // ---- list / cache invariants -------------------------------------------

    #[test]
    fn test_list_empty_before_any_save() {
        let mut mgr = make_manager();
        let result = mgr.list("profile-a");
        assert!(result.is_empty(), "fresh manager must return empty list");
    }

    #[test]
    fn test_list_returns_saved_query_after_save() {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");
        insert_profile(&conn, "p1");
        let mut mgr = make_manager_from_conn(conn);

        mgr.save("p1", "My Query", &sample_spec("users")).unwrap();

        let list = mgr.list("p1");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "My Query");
        assert_eq!(list[0].table_name, "users");
    }

    #[test]
    fn test_save_updates_cache_on_success() {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");
        insert_profile(&conn, "p1");
        let mut mgr = make_manager_from_conn(conn);

        mgr.save("p1", "Q1", &sample_spec("orders")).unwrap();
        mgr.save("p1", "Q2", &sample_spec("products")).unwrap();

        let list = mgr.list("p1");
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_save_same_name_overwrites_existing_row() {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");
        insert_profile(&conn, "p1");
        let mut mgr = make_manager_from_conn(Arc::clone(&conn));

        let first = mgr
            .save("p1", "Active users", &sample_spec("users"))
            .unwrap();
        let second = mgr
            .save("p1", "Active users", &sample_spec("users_v2"))
            .unwrap();

        assert_eq!(
            first.id, second.id,
            "same name within profile must reuse the same id"
        );

        let list = mgr.list("p1");
        assert_eq!(list.len(), 1, "must not create a second row");
    }

    #[test]
    fn test_list_order_stable_by_updated_at_desc() {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");
        insert_profile(&conn, "p1");

        let repo = Arc::new(SavedQueryRepo::new(Arc::clone(&conn)));

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        repo.upsert_by_id(
            &Uuid::now_v7().to_string(),
            "p1",
            "First",
            &sample_spec("t1"),
        )
        .unwrap();

        conn.lock()
            .unwrap()
            .execute(
                "UPDATE qry_saved_queries SET updated_at = ?1 WHERE name = 'First'",
                rusqlite::params![now_ms - 1000],
            )
            .unwrap();

        repo.upsert_by_id(
            &Uuid::now_v7().to_string(),
            "p1",
            "Second",
            &sample_spec("t2"),
        )
        .unwrap();

        let mut mgr = SavedQueryManager::new(repo);
        let list = mgr.list("p1");
        assert_eq!(list[0].name, "Second", "most recently updated first");
    }

    // ---- delete -----------------------------------------------------------

    #[test]
    fn test_delete_removes_from_cache() {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");
        insert_profile(&conn, "p1");
        let mut mgr = make_manager_from_conn(conn);

        let summary = mgr.save("p1", "ToDelete", &sample_spec("t")).unwrap();
        mgr.delete(&summary.id, "p1").unwrap();

        assert!(mgr.list("p1").is_empty());
    }

    // ---- rename -----------------------------------------------------------

    #[test]
    fn test_rename_updates_cache() {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");
        insert_profile(&conn, "p1");
        let mut mgr = make_manager_from_conn(conn);

        let summary = mgr.save("p1", "OldName", &sample_spec("t")).unwrap();
        mgr.rename(&summary.id, "p1", "NewName").unwrap();

        let list = mgr.list("p1");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "NewName");
    }

    #[test]
    fn test_rename_conflict_returns_err_and_cache_unchanged() {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");
        insert_profile(&conn, "p1");
        let mut mgr = make_manager_from_conn(conn);

        mgr.save("p1", "Alpha", &sample_spec("t1")).unwrap();
        let beta = mgr.save("p1", "Beta", &sample_spec("t2")).unwrap();

        let result = mgr.rename(&beta.id, "p1", "Alpha");
        assert!(result.is_err(), "rename to existing name must return Err");

        let list = mgr.list("p1");
        assert!(
            list.iter().any(|s| s.name == "Beta"),
            "cache must retain original name after failed rename"
        );
    }

    // ---- fork -------------------------------------------------------------

    #[test]
    fn test_fork_creates_new_row_with_copy_suffix() {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");
        insert_profile(&conn, "p1");
        let mut mgr = make_manager_from_conn(conn);

        let original = mgr.save("p1", "MyQuery", &sample_spec("t")).unwrap();
        let forked = mgr.fork(&original.id, "p1").unwrap();

        assert_ne!(original.id, forked.id, "fork must have a new id");
        assert_eq!(forked.name, "MyQuery (copy)");

        let list = mgr.list("p1");
        assert_eq!(list.len(), 2);
    }

    // ---- import_to --------------------------------------------------------

    #[test]
    fn test_import_to_succeeds_when_table_exists() {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");
        insert_profile(&conn, "source-profile");
        insert_profile(&conn, "target-profile");
        let mut mgr = make_manager_from_conn(conn);

        let original = mgr
            .save("source-profile", "ImportMe", &sample_spec("users"))
            .unwrap();

        let probe = MockTableProbe { succeeds: true };
        let imported = mgr
            .import_to(&original.id, "target-profile", &probe)
            .unwrap();

        assert_ne!(original.id, imported.id);
        assert_eq!(imported.profile_id, "target-profile");

        let list = mgr.list("target-profile");
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_import_to_fails_when_table_absent() {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");
        insert_profile(&conn, "source-profile");
        insert_profile(&conn, "target-profile");
        let mut mgr = make_manager_from_conn(conn);

        let original = mgr
            .save("source-profile", "Query", &sample_spec("missing_table"))
            .unwrap();

        let probe = MockTableProbe { succeeds: false };
        let result = mgr.import_to(&original.id, "target-profile", &probe);

        assert!(result.is_err(), "import must fail when table is absent");
        assert!(
            mgr.list("target-profile").is_empty(),
            "no row must be written on failure"
        );
    }

    #[test]
    fn test_import_to_fails_when_connection_offline() {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");
        insert_profile(&conn, "source-profile");
        insert_profile(&conn, "target-profile");
        let mut mgr = make_manager_from_conn(conn);

        let original = mgr
            .save("source-profile", "Query", &sample_spec("t"))
            .unwrap();

        struct OfflineProbe;
        impl TableProbe for OfflineProbe {
            fn check_table_exists(
                &self,
                _schema: Option<&str>,
                _table: &str,
            ) -> Result<(), StorageError> {
                Err(StorageError::Data("connection offline".to_string()))
            }
        }

        let result = mgr.import_to(&original.id, "target-profile", &OfflineProbe);
        assert!(result.is_err());
    }
}
