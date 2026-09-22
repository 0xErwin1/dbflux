//! `SchemaSnapshotManager` — SQLite-backed manager for persisted schema snapshots.
//!
//! Wraps `SchemaSnapshotRepo` from `dbflux_storage` and keeps a per-profile/
//! database in-memory cache of `SchemaSnapshotSummary` items for synchronous
//! reads.
//!
//! Writes go through the repository first; the cache is invalidated only on
//! success. `capture` handles the on-connect auto-capture path (dedup against
//! the latest stored fingerprint, then prune to a retention bound); `capture_deep`
//! back-fills full `table_details` for every table before persisting, used by
//! the explicit "Capture snapshot" action and lazy deep back-fill.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dbflux_core::{
    Connection, SchemaFingerprint, SchemaSnapshotRecord, SnapshotDepth, TableCreationMetadata,
    TableInfo,
};
use dbflux_storage::error::StorageError;
use dbflux_storage::repositories::sch_schema_snapshots::{
    SchemaSnapshotRepo, SchemaSnapshotSummary,
};
use uuid::Uuid;

/// Result of a `capture`/`capture_deep` call: either a new snapshot was
/// persisted, or the live structure matched the latest stored fingerprint
/// and the existing snapshot was reused.
#[derive(Debug, Clone, PartialEq)]
pub enum CaptureOutcome {
    Inserted { id: Uuid },
    Deduped { existing_id: String },
}

type CacheKey = (String, Option<String>);

/// In-memory manager for persisted schema snapshots, backed by `SchemaSnapshotRepo`.
pub struct SchemaSnapshotManager {
    repo: Arc<SchemaSnapshotRepo>,
    cache: HashMap<CacheKey, Vec<SchemaSnapshotSummary>>,
}

impl SchemaSnapshotManager {
    /// Creates a new manager wrapping the given repository.
    pub fn new(repo: Arc<SchemaSnapshotRepo>) -> Self {
        Self {
            repo,
            cache: HashMap::new(),
        }
    }

    /// Lists snapshot summaries for `(profile_id, database)`, ordered by
    /// `captured_at DESC`.
    ///
    /// Fetches from the repository on the first call for a key and caches
    /// the result. Subsequent calls return the in-memory list.
    pub fn list(&mut self, profile_id: &str, database: Option<&str>) -> Vec<SchemaSnapshotSummary> {
        let key = cache_key(profile_id, database);

        if let Some(cached) = self.cache.get(&key) {
            return cached.clone();
        }

        let rows = match self.repo.list(profile_id, database) {
            Ok(rows) => rows,
            Err(e) => {
                log::warn!(
                    "SchemaSnapshotManager: failed to load list for {profile_id}/{database:?}: {e}"
                );
                Vec::new()
            }
        };

        self.cache.insert(key, rows.clone());

        rows
    }

    /// Loads the full `SchemaSnapshotRecord` (including every table) by id.
    pub fn get(&self, id: &str) -> Result<Option<SchemaSnapshotRecord>, StorageError> {
        self.repo.get(id)
    }

    /// Captures a snapshot from an already-known table list.
    ///
    /// Computes a combined stable fingerprint over `tables`, compares it
    /// against the most recently stored snapshot for `(profile_id,
    /// database)`, and skips the insert when unchanged. On insert, prunes
    /// down to `retention` (most recent kept).
    pub fn capture(
        &mut self,
        profile_id: &str,
        database: Option<&str>,
        tables: &[TableInfo],
        depth: SnapshotDepth,
        retention: usize,
    ) -> Result<CaptureOutcome, StorageError> {
        self.capture_with_creation_metadata(profile_id, database, tables, &[], depth, retention)
    }

    /// Captures a snapshot together with structured creation metadata.
    ///
    /// Deduplication is metadata-aware: when the legacy schema fingerprint is
    /// unchanged but the new capture carries metadata the latest stored
    /// snapshot lacks (or differs from), a new row is inserted so the freshly
    /// captured metadata is never discarded. A capture without metadata
    /// always dedups against an identical fingerprint, preserving existing
    /// shallow-capture behavior.
    pub fn capture_with_creation_metadata(
        &mut self,
        profile_id: &str,
        database: Option<&str>,
        tables: &[TableInfo],
        creation_metadata: &[TableCreationMetadata],
        depth: SnapshotDepth,
        retention: usize,
    ) -> Result<CaptureOutcome, StorageError> {
        // A retention of 0 would prune the row just inserted, silently
        // disabling persistence; always keep at least the latest snapshot.
        let retention = retention.max(1);

        let fingerprint = SchemaFingerprint::stable_hex_many(tables);

        let latest = self.repo.list(profile_id, database)?.into_iter().next();

        if let Some(latest) = &latest
            && latest.fingerprint == fingerprint
            && !self.metadata_changed_since(&latest.id, creation_metadata)?
        {
            return Ok(CaptureOutcome::Deduped {
                existing_id: latest.id.clone(),
            });
        }

        let profile_uuid = Uuid::parse_str(profile_id)
            .map_err(|e| StorageError::Data(format!("invalid profile_id uuid: {e}")))?;

        let record = SchemaSnapshotRecord {
            id: Uuid::now_v7(),
            profile_id: profile_uuid,
            database: database.map(str::to_string),
            captured_at: now_millis(),
            fingerprint,
            depth,
            tables: tables.to_vec(),
            creation_metadata: creation_metadata.to_vec(),
        };

        self.repo.insert(&record)?;
        self.repo.prune(profile_id, database, retention)?;

        self.cache.remove(&cache_key(profile_id, database));

        Ok(CaptureOutcome::Inserted { id: record.id })
    }

    /// Returns `true` when `new_metadata` differs from the creation metadata
    /// stored with the given snapshot — including the case where the stored
    /// snapshot has none but the new capture does. Only captures that carry
    /// metadata can report a change.
    fn metadata_changed_since(
        &self,
        latest_id: &str,
        new_metadata: &[TableCreationMetadata],
    ) -> Result<bool, StorageError> {
        if new_metadata.is_empty() {
            return Ok(false);
        }

        let Some(latest_record) = self.repo.get(latest_id)? else {
            return Ok(true);
        };

        let stored_by_table: std::collections::HashMap<
            (Option<&str>, &str),
            &TableCreationMetadata,
        > = latest_record
            .creation_metadata
            .iter()
            .map(|metadata| {
                (
                    (metadata.schema.as_deref(), metadata.table.as_str()),
                    metadata,
                )
            })
            .collect();

        for metadata in new_metadata {
            let key = (metadata.schema.as_deref(), metadata.table.as_str());
            match stored_by_table.get(&key) {
                Some(stored) if stored == &metadata => {}
                _ => return Ok(true),
            }
        }

        Ok(false)
    }

    /// Captures a `Deep` snapshot by back-filling full `table_details` for
    /// every entry in `shallow_tables` before persisting.
    ///
    /// Per-table failures abort the capture with an error naming the table
    /// and the source failure; nothing is persisted and the cache is left
    /// untouched. Structured creation metadata is collected through the same
    /// generic connection per table and propagates on failure the same way.
    pub fn capture_deep(
        &mut self,
        connection: &dyn Connection,
        profile_id: &str,
        database: Option<&str>,
        shallow_tables: &[TableInfo],
        retention: usize,
    ) -> Result<CaptureOutcome, StorageError> {
        let db = database.unwrap_or_default();

        let mut deep_tables = Vec::with_capacity(shallow_tables.len());
        let mut creation_metadata = Vec::new();
        for table in shallow_tables {
            let detail = connection
                .table_details(db, table.schema.as_deref(), &table.name)
                .map_err(|error| {
                    StorageError::Data(format!(
                        "deep capture failed for table '{}' in schema {:?}: {error}",
                        table.name, table.schema
                    ))
                })?;

            if let Some(metadata) = connection
                .table_creation_metadata(db, table.schema.as_deref(), &table.name)
                .map_err(|error| {
                    StorageError::Data(format!(
                        "creation metadata introspection failed for table '{}' in schema {:?}: {error}",
                        table.name, table.schema
                    ))
                })?
            {
                creation_metadata.push(metadata);
            }

            deep_tables.push(detail);
        }

        self.capture_with_creation_metadata(
            profile_id,
            database,
            &deep_tables,
            &creation_metadata,
            SnapshotDepth::Deep,
            retention,
        )
    }

    /// Table details from the most recent `Deep` snapshot for `(profile_id,
    /// database)`, restricted to tables that still exist in `live_tables`.
    ///
    /// Used to seed the session's `table_details` cache on connect so
    /// completion and detail views start warm without driver round trips.
    /// Column-level staleness is acceptable here: the drift check refreshes
    /// entries at query time.
    pub fn latest_deep_details(
        &mut self,
        profile_id: &str,
        database: Option<&str>,
        live_tables: &[TableInfo],
    ) -> Vec<TableInfo> {
        let summaries = self.list(profile_id, database);
        let Some(deep) = summaries
            .iter()
            .find(|summary| summary.depth == SnapshotDepth::Deep)
        else {
            return Vec::new();
        };

        let record = match self.get(&deep.id) {
            Ok(Some(record)) => record,
            Ok(None) => return Vec::new(),
            Err(error) => {
                log::warn!(
                    "SchemaSnapshotManager: failed to load deep snapshot {}: {error}",
                    deep.id
                );
                return Vec::new();
            }
        };

        let live: std::collections::HashSet<(Option<&str>, &str)> = live_tables
            .iter()
            .map(|table| (table.schema.as_deref(), table.name.as_str()))
            .collect();

        record
            .tables
            .into_iter()
            .filter(|table| {
                (table.columns.is_some() || table.sample_fields.is_some())
                    && live.contains(&(table.schema.as_deref(), table.name.as_str()))
            })
            .collect()
    }
}

fn cache_key(profile_id: &str, database: Option<&str>) -> CacheKey {
    (profile_id.to_string(), database.map(str::to_string))
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        ColumnInfo, DatabaseInfo, DbError, DbKind, DriverCapabilities, DriverMetadata, QueryHandle,
        QueryRequest, QueryResult, SchemaLoadingStrategy, SchemaSnapshot,
    };
    use dbflux_storage::bootstrap::StorageRuntime;

    fn make_manager() -> (SchemaSnapshotManager, String) {
        let rt = StorageRuntime::in_memory().unwrap();
        let conn = rt
            .viz_connection()
            .expect("viz connection should open in test");

        let profile_id = Uuid::now_v7();
        conn.lock()
            .unwrap()
            .execute(
                "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'test-profile')",
                rusqlite::params![profile_id.to_string()],
            )
            .expect("insert test profile");

        let manager = SchemaSnapshotManager::new(Arc::new(SchemaSnapshotRepo::new(conn)));
        (manager, profile_id.to_string())
    }

    fn table(name: &str) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            schema: Some("public".to_string()),
            columns: Some(vec![ColumnInfo {
                name: "id".to_string(),
                type_name: "integer".to_string(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                enum_values: None,
            }]),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
            storage_hints: None,
        }
    }

    fn shallow_table(name: &str) -> TableInfo {
        TableInfo {
            columns: None,
            ..table(name)
        }
    }

    // --- latest_deep_details ---

    #[test]
    fn latest_deep_details_returns_detailed_tables_still_alive() {
        let (mut mgr, profile_id) = make_manager();

        // Deep snapshot with columns for two tables, then a newer shallow
        // one on top (the on-connect auto-capture).
        mgr.capture(
            &profile_id,
            Some("db1"),
            &[table("users"), table("orders")],
            SnapshotDepth::Deep,
            10,
        )
        .expect("deep capture");
        mgr.capture(
            &profile_id,
            Some("db1"),
            &[shallow_table("users")],
            SnapshotDepth::Shallow,
            10,
        )
        .expect("shallow capture");

        // `orders` no longer exists in the live listing and must be dropped.
        let details = mgr.latest_deep_details(&profile_id, Some("db1"), &[shallow_table("users")]);

        assert_eq!(details.len(), 1);
        assert_eq!(details[0].name, "users");
        assert!(details[0].columns.is_some());
    }

    #[test]
    fn latest_deep_details_empty_without_deep_snapshot() {
        let (mut mgr, profile_id) = make_manager();

        mgr.capture(
            &profile_id,
            Some("db1"),
            &[shallow_table("users")],
            SnapshotDepth::Shallow,
            10,
        )
        .expect("shallow capture");

        assert!(
            mgr.latest_deep_details(&profile_id, Some("db1"), &[shallow_table("users")])
                .is_empty()
        );
    }

    // --- capture ---

    #[test]
    fn capture_inserts_first_snapshot() {
        let (mut mgr, profile_id) = make_manager();

        let outcome = mgr
            .capture(
                &profile_id,
                Some("db1"),
                &[table("users")],
                SnapshotDepth::Shallow,
                10,
            )
            .expect("capture");

        assert!(matches!(outcome, CaptureOutcome::Inserted { .. }));
        assert_eq!(mgr.list(&profile_id, Some("db1")).len(), 1);
    }

    #[test]
    fn capture_dedups_unchanged_structure() {
        let (mut mgr, profile_id) = make_manager();

        let first = mgr
            .capture(
                &profile_id,
                Some("db1"),
                &[table("users")],
                SnapshotDepth::Shallow,
                10,
            )
            .expect("first capture");
        let first_id = match first {
            CaptureOutcome::Inserted { id } => id,
            _ => panic!("expected Inserted"),
        };

        let second = mgr
            .capture(
                &profile_id,
                Some("db1"),
                &[table("users")],
                SnapshotDepth::Shallow,
                10,
            )
            .expect("second capture");

        assert_eq!(
            second,
            CaptureOutcome::Deduped {
                existing_id: first_id.to_string()
            }
        );
        assert_eq!(
            mgr.list(&profile_id, Some("db1")).len(),
            1,
            "dedup must not insert a second row"
        );
    }

    #[test]
    fn capture_inserts_new_row_on_structure_change() {
        let (mut mgr, profile_id) = make_manager();

        mgr.capture(
            &profile_id,
            Some("db1"),
            &[table("users")],
            SnapshotDepth::Shallow,
            10,
        )
        .expect("first capture");

        let mut changed_users = table("users");
        if let Some(cols) = &mut changed_users.columns {
            cols.push(ColumnInfo {
                name: "email".to_string(),
                type_name: "text".to_string(),
                nullable: true,
                is_primary_key: false,
                default_value: None,
                enum_values: None,
            });
        }

        let outcome = mgr
            .capture(
                &profile_id,
                Some("db1"),
                &[changed_users],
                SnapshotDepth::Shallow,
                10,
            )
            .expect("second capture");

        assert!(matches!(outcome, CaptureOutcome::Inserted { .. }));
        assert_eq!(mgr.list(&profile_id, Some("db1")).len(), 2);
    }

    #[test]
    fn capture_prunes_to_retention() {
        let (mut mgr, profile_id) = make_manager();

        for i in 0..5 {
            let mut t = table("users");
            // Force a distinct fingerprint per iteration so every capture inserts.
            t.columns.as_mut().unwrap()[0].type_name = format!("integer_{i}");
            mgr.capture(&profile_id, Some("db1"), &[t], SnapshotDepth::Shallow, 2)
                .expect("capture");
        }

        assert_eq!(
            mgr.list(&profile_id, Some("db1")).len(),
            2,
            "must prune down to retention=2 after each insert"
        );
    }

    #[test]
    fn capture_with_zero_retention_still_keeps_the_latest_snapshot() {
        let (mut mgr, profile_id) = make_manager();

        for i in 0..3 {
            let mut t = table("users");
            // Force a distinct fingerprint per iteration so every capture inserts.
            t.columns.as_mut().unwrap()[0].type_name = format!("integer_{i}");
            mgr.capture(&profile_id, Some("db1"), &[t], SnapshotDepth::Shallow, 0)
                .expect("capture");
        }

        assert_eq!(
            mgr.list(&profile_id, Some("db1")).len(),
            1,
            "a retention of 0 must not prune the just-inserted snapshot away"
        );
    }

    // --- capture_deep ---

    struct MockConnection {
        metadata: DriverMetadata,
        detail: TableInfo,
        fail_lookup: bool,
        /// Table name whose `table_details` lookup fails, to exercise
        /// failure after earlier tables succeeded.
        fail_lookup_table: Option<String>,
        /// Structured creation metadata the mock returns from
        /// `table_creation_metadata`; `None` mimics a driver without
        /// introspection.
        creation_metadata: Option<TableCreationMetadata>,
        fail_metadata_lookup: bool,
    }

    impl Connection for MockConnection {
        fn metadata(&self) -> &DriverMetadata {
            &self.metadata
        }
        fn ping(&self) -> Result<(), DbError> {
            Ok(())
        }
        fn close(&mut self) -> Result<(), DbError> {
            Ok(())
        }
        fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
            Err(DbError::NotSupported("mock".into()))
        }
        fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
            Ok(())
        }
        fn schema(&self) -> Result<SchemaSnapshot, DbError> {
            Ok(SchemaSnapshot::default())
        }
        fn kind(&self) -> DbKind {
            DbKind::Postgres
        }
        fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
            SchemaLoadingStrategy::LazyPerDatabase
        }
        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &dbflux_core::DefaultSqlDialect
        }
        fn table_details(
            &self,
            _database: &str,
            _schema: Option<&str>,
            _table: &str,
        ) -> Result<TableInfo, DbError> {
            if self.fail_lookup || self.fail_lookup_table.as_deref() == Some(_table) {
                Err(DbError::NotSupported("boom".into()))
            } else {
                Ok(self.detail.clone())
            }
        }
        fn table_creation_metadata(
            &self,
            _database: &str,
            _schema: Option<&str>,
            _table: &str,
        ) -> Result<Option<TableCreationMetadata>, DbError> {
            if self.fail_metadata_lookup {
                Err(DbError::NotSupported("metadata boom".into()))
            } else {
                Ok(self.creation_metadata.clone())
            }
        }
        fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
            Ok(Vec::new())
        }
    }

    fn minimal_metadata() -> DriverMetadata {
        DriverMetadata {
            id: "mock".to_string(),
            display_name: "Mock".to_string(),
            description: "test".to_string(),
            category: dbflux_core::DatabaseCategory::Relational,
            transfer_family: dbflux_core::TransferFamily::Sql,
            deployment_class: None,
            query_language: dbflux_core::QueryLanguage::Sql,
            capabilities: DriverCapabilities::empty(),
            default_port: None,
            uri_scheme: "mock".to_string(),
            icon: dbflux_core::Icon::Database,
            syntax: None,
            query: None,
            mutation: None,
            ddl: None,
            transactions: None,
            limits: None,
            ssl_modes: None,
            ssl_cert_fields: None,
            classification_override: None,
            default_chunk_size: None,
            supports_lock_timeout: false,
            editor_profile: None,
        }
    }

    #[test]
    fn capture_deep_backfills_full_table_detail() {
        let (mut mgr, profile_id) = make_manager();

        let mut deep_users = table("users");
        deep_users.columns = Some(vec![
            ColumnInfo {
                name: "id".to_string(),
                type_name: "integer".to_string(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                enum_values: None,
            },
            ColumnInfo {
                name: "email".to_string(),
                type_name: "text".to_string(),
                nullable: false,
                is_primary_key: false,
                default_value: None,
                enum_values: None,
            },
        ]);

        let connection = MockConnection {
            metadata: minimal_metadata(),
            detail: deep_users,
            fail_lookup: false,
            fail_lookup_table: None,
            creation_metadata: None,
            fail_metadata_lookup: false,
        };

        let outcome = mgr
            .capture_deep(&connection, &profile_id, Some("db1"), &[table("users")], 10)
            .expect("capture_deep");

        let id = match outcome {
            CaptureOutcome::Inserted { id } => id,
            _ => panic!("expected Inserted"),
        };

        let loaded = mgr.get(&id.to_string()).unwrap().unwrap();
        assert_eq!(loaded.depth, SnapshotDepth::Deep);
        assert_eq!(loaded.tables[0].columns.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn capture_deep_propagates_table_details_errors() {
        let (mut mgr, profile_id) = make_manager();

        let connection = MockConnection {
            metadata: minimal_metadata(),
            detail: table("unused"),
            fail_lookup: true,
            fail_lookup_table: None,
            creation_metadata: None,
            fail_metadata_lookup: false,
        };

        let outcome =
            mgr.capture_deep(&connection, &profile_id, Some("db1"), &[table("users")], 10);

        assert!(
            outcome.is_err(),
            "a table_details failure must propagate, got: {:?}",
            outcome
        );
        let error = outcome.err().unwrap().to_string();
        assert!(
            error.contains("users"),
            "the error must name the failing table, got: {error}"
        );
        assert!(
            error.contains("boom"),
            "the error must retain the source error, got: {error}"
        );
        assert_eq!(
            mgr.list(&profile_id, Some("db1")).len(),
            0,
            "a failed capture must persist no snapshot row"
        );
    }

    #[test]
    fn capture_deep_error_after_prior_table_success_persists_nothing() {
        let (mut mgr, profile_id) = make_manager();

        let connection = MockConnection {
            metadata: minimal_metadata(),
            detail: table("unused"),
            fail_lookup: false,
            fail_lookup_table: Some("orders".to_string()),
            creation_metadata: None,
            fail_metadata_lookup: false,
        };

        let outcome = mgr.capture_deep(
            &connection,
            &profile_id,
            Some("db1"),
            &[table("users"), table("orders")],
            10,
        );

        assert!(
            outcome.is_err(),
            "a failure on any table must propagate even after earlier tables succeeded, got: {:?}",
            outcome
        );
        assert_eq!(
            mgr.list(&profile_id, Some("db1")).len(),
            0,
            "no partial snapshot may be persisted when a later table fails"
        );
    }

    fn sample_creation_metadata(table_name: &str) -> TableCreationMetadata {
        TableCreationMetadata {
            schema: Some("public".to_string()),
            table: table_name.to_string(),
            completeness: dbflux_core::MetadataCompleteness::Complete,
            identity: Some(dbflux_core::IdentitySpec {
                column: "id".to_string(),
                seed: "99999999999999999999999999999999999999".to_string(),
                increment: "1".to_string(),
            }),
            primary_key: Some(dbflux_core::PrimaryKeySpec {
                columns: vec!["id".to_string()],
            }),
            blockers: Vec::new(),
        }
    }

    #[test]
    fn capture_deep_persists_creation_metadata() {
        let (mut mgr, profile_id) = make_manager();

        let connection = MockConnection {
            metadata: minimal_metadata(),
            detail: table("users"),
            fail_lookup: false,
            fail_lookup_table: None,
            creation_metadata: Some(sample_creation_metadata("users")),
            fail_metadata_lookup: false,
        };

        let outcome = mgr
            .capture_deep(&connection, &profile_id, Some("db1"), &[table("users")], 10)
            .expect("capture_deep");

        let id = match outcome {
            CaptureOutcome::Inserted { id } => id,
            _ => panic!("expected Inserted"),
        };

        let loaded = mgr.get(&id.to_string()).unwrap().unwrap();
        assert_eq!(loaded.creation_metadata.len(), 1);
        assert_eq!(loaded.creation_metadata[0].table, "users");
        assert_eq!(
            loaded.creation_metadata[0].schema.as_deref(),
            Some("public")
        );
        let identity = loaded.creation_metadata[0]
            .identity
            .as_ref()
            .expect("identity captured");
        assert_eq!(
            identity.seed, "99999999999999999999999999999999999999",
            "38-digit seed must be persisted as the exact decimal string"
        );
    }

    #[test]
    fn capture_deep_propagates_metadata_introspection_errors() {
        let (mut mgr, profile_id) = make_manager();

        let connection = MockConnection {
            metadata: minimal_metadata(),
            detail: table("users"),
            fail_lookup: false,
            fail_lookup_table: None,
            creation_metadata: None,
            fail_metadata_lookup: true,
        };

        let outcome =
            mgr.capture_deep(&connection, &profile_id, Some("db1"), &[table("users")], 10);

        assert!(
            outcome.is_err(),
            "a creation-metadata introspection failure must propagate, got: {:?}",
            outcome
        );
    }

    #[test]
    fn capture_with_metadata_inserts_new_row_despite_identical_fingerprint() {
        let (mut mgr, profile_id) = make_manager();

        // First capture: same tables, no metadata (e.g. a legacy snapshot).
        mgr.capture(
            &profile_id,
            Some("db1"),
            &[table("users")],
            SnapshotDepth::Deep,
            10,
        )
        .expect("first capture");

        // Second capture: identical structure fingerprint, but the driver now
        // reports creation metadata. It must not be silently discarded.
        let metadata = vec![sample_creation_metadata("users")];
        let outcome = mgr
            .capture_with_creation_metadata(
                &profile_id,
                Some("db1"),
                &[table("users")],
                &metadata,
                SnapshotDepth::Deep,
                10,
            )
            .expect("second capture");

        assert!(
            matches!(outcome, CaptureOutcome::Inserted { .. }),
            "newly captured metadata must not be deduped away, got: {:?}",
            outcome
        );

        assert_eq!(mgr.list(&profile_id, Some("db1")).len(), 2);
        let inserted_id = match outcome {
            CaptureOutcome::Inserted { id } => id,
            _ => unreachable!("asserted above"),
        };
        let inserted = mgr.get(&inserted_id.to_string()).unwrap().unwrap();
        assert_eq!(inserted.creation_metadata.len(), 1);
    }

    #[test]
    fn capture_with_identical_metadata_dedups() {
        let (mut mgr, profile_id) = make_manager();

        let metadata = vec![sample_creation_metadata("users")];
        let first = mgr
            .capture_with_creation_metadata(
                &profile_id,
                Some("db1"),
                &[table("users")],
                &metadata,
                SnapshotDepth::Deep,
                10,
            )
            .expect("first capture");
        let first_id = match first {
            CaptureOutcome::Inserted { id } => id,
            _ => panic!("expected Inserted"),
        };

        let second = mgr
            .capture_with_creation_metadata(
                &profile_id,
                Some("db1"),
                &[table("users")],
                &metadata,
                SnapshotDepth::Deep,
                10,
            )
            .expect("second capture");

        assert_eq!(
            second,
            CaptureOutcome::Deduped {
                existing_id: first_id.to_string()
            }
        );
    }

    #[test]
    fn capture_without_metadata_dedups_against_metadata_bearing_snapshot() {
        // A shallow on-connect capture of an unchanged structure must still
        // dedup even when the stored snapshot carries creation metadata; only
        // changed or newly present metadata forces an insert.
        let (mut mgr, profile_id) = make_manager();

        let metadata = vec![sample_creation_metadata("users")];
        mgr.capture_with_creation_metadata(
            &profile_id,
            Some("db1"),
            &[table("users")],
            &metadata,
            SnapshotDepth::Deep,
            10,
        )
        .expect("metadata-bearing capture");

        let shallow = mgr
            .capture(
                &profile_id,
                Some("db1"),
                &[table("users")],
                SnapshotDepth::Shallow,
                10,
            )
            .expect("shallow capture");

        assert!(
            matches!(shallow, CaptureOutcome::Deduped { .. }),
            "shallow capture without metadata must dedup, got: {:?}",
            shallow
        );
    }
}
