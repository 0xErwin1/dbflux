//! Repository for `sch_schema_snapshots` and its `sch_snapshot_tables` child rows.
//!
//! Persists `SchemaSnapshotRecord` from `dbflux_core` across two SQLite tables:
//! - `sch_schema_snapshots` — root row with native scalar columns (identity,
//!   fingerprint, depth) used for list/prune queries without touching JSON.
//! - `sch_snapshot_tables` — one row per captured `TableInfo`; `schema_name`/
//!   `name` are native columns for identity, `detail_json` holds the full
//!   per-driver structure (genuinely dynamic, bounded by `serde` derives).
//!
//! `insert` wraps the parent + child writes in a single transaction. `prune`
//! deletes the oldest rows beyond a per-profile/database retention bound;
//! child rows cascade via `ON DELETE CASCADE`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use uuid::Uuid;

use dbflux_core::{SchemaSnapshotRecord, SnapshotDepth, TableCreationMetadata, TableInfo};

use crate::error::StorageError;

const DB_PATH: &str = "dbflux.db";

/// A lightweight summary row returned by `list` — avoids deserializing every
/// table's `detail_json` when only the snapshot identity is needed (e.g. for
/// a source picker).
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaSnapshotSummary {
    pub id: String,
    pub profile_id: String,
    pub database: Option<String>,
    pub captured_at: i64,
    pub fingerprint: String,
    pub depth: SnapshotDepth,
}

/// Repository for the `sch_schema_snapshots` table family.
#[derive(Clone)]
pub struct SchemaSnapshotRepo {
    conn: Arc<Mutex<Connection>>,
    connect_gates: Arc<Mutex<HashMap<Uuid, Arc<ConnectCaptureGate>>>>,
}

struct ConnectCaptureGate {
    current_generation: AtomicU64,
    operation_lock: Mutex<()>,
}

#[derive(Clone)]
pub struct ConnectCaptureToken {
    gate: Arc<ConnectCaptureGate>,
    generation: u64,
}

impl SchemaSnapshotRepo {
    /// Creates a new repository wrapping the given shared connection.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            conn,
            connect_gates: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn begin_connect_capture(
        &self,
        profile_id: Uuid,
    ) -> Result<ConnectCaptureToken, StorageError> {
        let mut gates = self.connect_gates.lock().map_err(lock_err)?;
        let gate = Arc::clone(gates.entry(profile_id).or_insert_with(|| {
            Arc::new(ConnectCaptureGate {
                current_generation: AtomicU64::new(0),
                operation_lock: Mutex::new(()),
            })
        }));
        let generation = gate.current_generation.fetch_add(1, Ordering::AcqRel) + 1;
        Ok(ConnectCaptureToken { gate, generation })
    }

    pub fn with_current_connect_capture<T>(
        &self,
        token: &ConnectCaptureToken,
        operation: impl FnOnce() -> Result<T, StorageError>,
    ) -> Result<Option<T>, StorageError> {
        let _guard = token.gate.operation_lock.lock().map_err(lock_err)?;
        if token.generation != token.gate.current_generation.load(Ordering::Acquire) {
            return Ok(None);
        }
        operation().map(Some)
    }

    /// Inserts a new snapshot with its table rows in a single transaction.
    pub fn insert(&self, record: &SchemaSnapshotRecord) -> Result<(), StorageError> {
        let mut conn = self.conn.lock().map_err(lock_err)?;
        let tx = conn.transaction().map_err(sqlite_err)?;

        let id = record.id.to_string();

        tx.execute(
            "INSERT INTO sch_schema_snapshots
                 (id, profile_id, database, captured_at, fingerprint, depth)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                id,
                record.profile_id.to_string(),
                record.database,
                record.captured_at,
                record.fingerprint,
                depth_to_storage(record.depth),
            ],
        )
        .map_err(sqlite_err)?;

        let metadata_by_table: std::collections::HashMap<
            (Option<&str>, &str),
            &TableCreationMetadata,
        > = record
            .creation_metadata
            .iter()
            .map(|metadata| {
                (
                    (metadata.schema.as_deref(), metadata.table.as_str()),
                    metadata,
                )
            })
            .collect();

        for table in &record.tables {
            let detail_json = serde_json::to_string(table)
                .map_err(|e| StorageError::Data(format!("serialize table detail: {e}")))?;

            let creation_metadata_json =
                match metadata_by_table.get(&(table.schema.as_deref(), table.name.as_str())) {
                    Some(metadata) => {
                        let json = serde_json::to_string(metadata).map_err(|e| {
                            StorageError::Data(format!("serialize creation metadata: {e}"))
                        })?;
                        Some(json)
                    }
                    None => None,
                };

            tx.execute(
                "INSERT INTO sch_snapshot_tables (snapshot_id, schema_name, name, detail_json, creation_metadata_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    id,
                    table.schema,
                    table.name,
                    detail_json,
                    creation_metadata_json,
                ],
            )
            .map_err(sqlite_err)?;
        }

        tx.commit().map_err(sqlite_err)?;

        Ok(())
    }

    /// Checks whether the profile still exists in the shared configuration database.
    pub fn profile_exists(&self, profile_id: &str) -> Result<bool, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM cfg_connection_profiles WHERE id = ?1)",
            [profile_id],
            |row| row.get(0),
        )
        .map_err(sqlite_err)
    }

    /// Lists snapshot summaries for `(profile_id, database)`, ordered by
    /// `captured_at DESC, id DESC` (most recent first, UUIDv7 tie-break).
    pub fn list(
        &self,
        profile_id: &str,
        database: Option<&str>,
    ) -> Result<Vec<SchemaSnapshotSummary>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut stmt = conn
            .prepare(
                "SELECT id, profile_id, database, captured_at, fingerprint, depth
                 FROM sch_schema_snapshots
                 WHERE profile_id = ?1 AND database IS ?2
                 ORDER BY captured_at DESC, id DESC",
            )
            .map_err(sqlite_err)?;

        let rows = stmt
            .query_map(rusqlite::params![profile_id, database], map_summary_row)
            .map_err(sqlite_err)?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Returns the full snapshot (including every captured `TableInfo`) for
    /// `id`, or `None` if the id is not found.
    pub fn get(&self, id: &str) -> Result<Option<SchemaSnapshotRecord>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        load_record(&conn, id)
    }

    /// Deletes snapshots beyond the `keep` most recent for `(profile_id,
    /// database)`. Returns the number of snapshots pruned. Child rows are
    /// removed by `ON DELETE CASCADE`.
    pub fn prune(
        &self,
        profile_id: &str,
        database: Option<&str>,
        keep: usize,
    ) -> Result<usize, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let deleted = conn
            .execute(
                "DELETE FROM sch_schema_snapshots
                 WHERE profile_id = ?1 AND database IS ?2
                 AND id NOT IN (
                     SELECT id FROM sch_schema_snapshots
                     WHERE profile_id = ?1 AND database IS ?2
                     ORDER BY captured_at DESC, id DESC
                     LIMIT ?3
                 )",
                rusqlite::params![profile_id, database, keep as i64],
            )
            .map_err(sqlite_err)?;

        Ok(deleted)
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn load_record(conn: &Connection, id: &str) -> Result<Option<SchemaSnapshotRecord>, StorageError> {
    struct RootRow {
        profile_id: String,
        database: Option<String>,
        captured_at: i64,
        fingerprint: String,
        depth: String,
    }

    let root: Option<RootRow> = conn
        .query_row(
            "SELECT profile_id, database, captured_at, fingerprint, depth
             FROM sch_schema_snapshots WHERE id = ?1",
            [id],
            |row| {
                Ok(RootRow {
                    profile_id: row.get(0)?,
                    database: row.get(1)?,
                    captured_at: row.get(2)?,
                    fingerprint: row.get(3)?,
                    depth: row.get(4)?,
                })
            },
        )
        .ok();

    let root = match root {
        Some(r) => r,
        None => return Ok(None),
    };

    let mut stmt = conn
        .prepare(
            "SELECT schema_name, name, detail_json, creation_metadata_json
             FROM sch_snapshot_tables WHERE snapshot_id = ?1",
        )
        .map_err(sqlite_err)?;

    struct TableRow {
        detail_json: String,
        creation_metadata_json: Option<String>,
    }

    let rows = stmt
        .query_map([id], |row| {
            Ok(TableRow {
                detail_json: row.get(2)?,
                creation_metadata_json: row.get(3)?,
            })
        })
        .map_err(sqlite_err)?
        .filter_map(|r| r.ok())
        .collect::<Vec<_>>();

    let mut tables = Vec::with_capacity(rows.len());
    let mut creation_metadata = Vec::new();
    for row in rows {
        let table: TableInfo = serde_json::from_str(&row.detail_json)
            .map_err(|e| StorageError::Data(format!("deserialize table detail: {e}")))?;
        if let Some(json) = row.creation_metadata_json {
            let metadata: TableCreationMetadata = serde_json::from_str(&json)
                .map_err(|e| StorageError::Data(format!("deserialize creation metadata: {e}")))?;
            creation_metadata.push(metadata);
        }
        tables.push(table);
    }

    let profile_id = Uuid::parse_str(&root.profile_id)
        .map_err(|e| StorageError::Data(format!("invalid profile_id uuid: {e}")))?;

    Ok(Some(SchemaSnapshotRecord {
        id: Uuid::parse_str(id).map_err(|e| StorageError::Data(format!("invalid id uuid: {e}")))?,
        profile_id,
        database: root.database,
        captured_at: root.captured_at,
        fingerprint: root.fingerprint,
        depth: depth_from_storage(&root.depth),
        tables,
        creation_metadata,
    }))
}

fn map_summary_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SchemaSnapshotSummary> {
    let depth_raw: String = row.get(5)?;
    Ok(SchemaSnapshotSummary {
        id: row.get(0)?,
        profile_id: row.get(1)?,
        database: row.get(2)?,
        captured_at: row.get(3)?,
        fingerprint: row.get(4)?,
        depth: depth_from_storage(&depth_raw),
    })
}

fn depth_to_storage(depth: SnapshotDepth) -> &'static str {
    match depth {
        SnapshotDepth::Shallow => "shallow",
        SnapshotDepth::Deep => "deep",
    }
}

fn depth_from_storage(raw: &str) -> SnapshotDepth {
    match raw {
        "deep" => SnapshotDepth::Deep,
        _ => SnapshotDepth::Shallow,
    }
}

fn sqlite_err(source: rusqlite::Error) -> StorageError {
    StorageError::Sqlite {
        path: DB_PATH.into(),
        source,
    }
}

fn lock_err<T>(e: std::sync::PoisonError<T>) -> StorageError {
    StorageError::Sqlite {
        path: DB_PATH.into(),
        source: rusqlite::Error::InvalidParameterName(e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;

    fn temp_db(suffix: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_sch_repo_{}_{}.db",
            suffix,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn setup(suffix: &str) -> (Arc<Mutex<Connection>>, SchemaSnapshotRepo, Uuid) {
        let path = temp_db(suffix);
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = Uuid::now_v7();
        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'P')",
            [&profile_id.to_string()],
        )
        .unwrap();

        let conn = Arc::new(Mutex::new(conn));
        let repo = SchemaSnapshotRepo::new(Arc::clone(&conn));
        (conn, repo, profile_id)
    }

    fn sample_table(name: &str) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            schema: Some("public".to_string()),
            columns: Some(vec![dbflux_core::ColumnInfo {
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

    fn sample_record(
        profile_id: Uuid,
        database: Option<&str>,
        captured_at: i64,
    ) -> SchemaSnapshotRecord {
        SchemaSnapshotRecord {
            id: Uuid::now_v7(),
            profile_id,
            database: database.map(str::to_string),
            captured_at,
            fingerprint: "fp-1".to_string(),
            depth: SnapshotDepth::Shallow,
            tables: vec![sample_table("users"), sample_table("orders")],
            creation_metadata: Vec::new(),
        }
    }

    #[test]
    fn stale_connect_capture_cannot_prune_newer_snapshot() {
        let (conn, repo, profile_id) = setup("stale_connect_capture");
        let older = repo.begin_connect_capture(profile_id).expect("begin older");
        let newer = repo
            .clone()
            .begin_connect_capture(profile_id)
            .expect("begin newer");
        let newer_record = sample_record(profile_id, Some("db1"), 1000);
        let older_record = sample_record(profile_id, Some("db1"), 2000);

        let connection_guard = conn.lock().expect("lock connection");
        let third = repo
            .begin_connect_capture(profile_id)
            .expect("begin without DB lock");
        drop(connection_guard);
        // Restore a current token after proving begin never needs the SQLite lock.
        let newest = third;
        assert!(
            repo.with_current_connect_capture(&newer, || Ok(()))
                .expect("stale newer")
                .is_none()
        );
        repo.with_current_connect_capture(&newest, || {
            repo.insert(&newer_record)?;
            repo.prune(&profile_id.to_string(), Some("db1"), 1)?;
            Ok(())
        })
        .expect("persist newest")
        .expect("current");
        let ran = std::cell::Cell::new(false);
        assert!(
            repo.with_current_connect_capture(&older, || {
                ran.set(true);
                repo.insert(&older_record)?;
                repo.prune(&profile_id.to_string(), Some("db1"), 1)?;
                Ok(())
            })
            .expect("skip older")
            .is_none()
        );
        assert!(!ran.get());
        let remaining = repo
            .list(&profile_id.to_string(), Some("db1"))
            .expect("list");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, newer_record.id.to_string());
    }

    #[test]
    fn running_capture_finishes_before_newer_capture_persists() {
        use std::sync::mpsc;
        use std::time::Duration;

        let (_, repo, profile_id) = setup("running_connect_capture");
        let older = repo.begin_connect_capture(profile_id).expect("begin older");
        let older_record = sample_record(profile_id, Some("db1"), 2000);
        let newer_record = sample_record(profile_id, Some("db1"), 3000);
        let newer_id = newer_record.id;
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let (begun_tx, begun_rx) = mpsc::sync_channel(0);
        let (finished_tx, finished_rx) = mpsc::sync_channel(1);
        std::thread::scope(|scope| {
            let first_repo = repo.clone();
            let first = scope.spawn(move || {
                first_repo.with_current_connect_capture(&older, || {
                    entered_tx.send(()).expect("signal entered");
                    release_rx
                        .recv_timeout(Duration::from_secs(10))
                        .expect("release older");
                    first_repo.insert(&older_record)?;
                    first_repo.prune(&profile_id.to_string(), Some("db1"), 1)?;
                    Ok(())
                })
            });
            entered_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("older entered");
            let newer = repo
                .begin_connect_capture(profile_id)
                .expect("begin while older runs");
            let second_repo = repo.clone();
            let second = scope.spawn(move || {
                begun_tx.send(()).expect("signal attempted");
                let result = second_repo.with_current_connect_capture(&newer, || {
                    second_repo.insert(&newer_record)?;
                    second_repo.prune(&profile_id.to_string(), Some("db1"), 1)?;
                    Ok(())
                });
                finished_tx.send(()).expect("signal finished");
                result
            });
            begun_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("newer attempted");
            assert!(
                finished_rx.try_recv().is_err(),
                "newer cannot finish while older holds gate"
            );
            release_tx.send(()).expect("release");
            first
                .join()
                .expect("older thread")
                .expect("older persistence")
                .expect("older current at entry");
            second
                .join()
                .expect("newer thread")
                .expect("newer persistence")
                .expect("newer current");
        });
        let remaining = repo
            .list(&profile_id.to_string(), Some("db1"))
            .expect("list");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, newer_id.to_string());
    }

    #[test]
    fn profile_exists_tracks_deletion_and_propagates_lookup_failure() {
        let (conn, repo, profile_id) = setup("profile_exists");
        assert!(
            repo.profile_exists(&profile_id.to_string())
                .expect("lookup")
        );
        conn.lock()
            .expect("lock")
            .execute(
                "DELETE FROM cfg_connection_profiles WHERE id = ?1",
                [&profile_id.to_string()],
            )
            .expect("delete profile");
        assert!(
            !repo
                .profile_exists(&profile_id.to_string())
                .expect("lookup after deletion")
        );
        conn.lock()
            .expect("lock")
            .execute("DROP TABLE cfg_connection_profiles", [])
            .expect("drop fixture table");
        assert!(repo.profile_exists(&profile_id.to_string()).is_err());
    }

    // --- insert + get round-trip ---

    #[test]
    fn insert_and_get_roundtrip() {
        let (_, repo, profile_id) = setup("insert_get");
        let record = sample_record(profile_id, Some("app_db"), 1000);

        repo.insert(&record).expect("insert");

        let loaded = repo
            .get(&record.id.to_string())
            .expect("get")
            .expect("exists");

        assert_eq!(loaded.id, record.id);
        assert_eq!(loaded.profile_id, profile_id);
        assert_eq!(loaded.database, Some("app_db".to_string()));
        assert_eq!(loaded.captured_at, 1000);
        assert_eq!(loaded.fingerprint, "fp-1");
        assert_eq!(loaded.depth, SnapshotDepth::Shallow);
        assert_eq!(loaded.tables.len(), 2);
        assert!(loaded.tables.iter().any(|t| t.name == "users"));
        assert!(loaded.tables.iter().any(|t| t.name == "orders"));
    }

    #[test]
    fn get_missing_id_returns_none() {
        let (_, repo, _) = setup("get_missing");
        let result = repo.get(&Uuid::now_v7().to_string()).expect("get");
        assert!(result.is_none());
    }

    #[test]
    fn insert_with_none_database_roundtrips() {
        let (_, repo, profile_id) = setup("none_database");
        let record = sample_record(profile_id, None, 1000);

        repo.insert(&record).expect("insert");

        let loaded = repo
            .get(&record.id.to_string())
            .expect("get")
            .expect("exists");
        assert_eq!(loaded.database, None);
    }

    // --- list ---

    #[test]
    fn list_orders_by_captured_at_desc() {
        let (_, repo, profile_id) = setup("list_order");

        let older = sample_record(profile_id, Some("db1"), 1000);
        let newer = sample_record(profile_id, Some("db1"), 2000);

        repo.insert(&older).expect("insert older");
        repo.insert(&newer).expect("insert newer");

        let list = repo
            .list(&profile_id.to_string(), Some("db1"))
            .expect("list");

        assert_eq!(list.len(), 2);
        assert_eq!(list[0].id, newer.id.to_string(), "most recent first");
        assert_eq!(list[1].id, older.id.to_string());
    }

    #[test]
    fn list_filters_by_database() {
        let (_, repo, profile_id) = setup("list_filter_db");

        let db1_record = sample_record(profile_id, Some("db1"), 1000);
        let db2_record = sample_record(profile_id, Some("db2"), 1000);

        repo.insert(&db1_record).expect("insert db1");
        repo.insert(&db2_record).expect("insert db2");

        let list = repo
            .list(&profile_id.to_string(), Some("db1"))
            .expect("list");

        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, db1_record.id.to_string());
    }

    #[test]
    fn list_with_none_database_does_not_match_some_database() {
        let (_, repo, profile_id) = setup("list_none_db");

        let none_record = sample_record(profile_id, None, 1000);
        let some_record = sample_record(profile_id, Some("db1"), 1000);

        repo.insert(&none_record).expect("insert none-db");
        repo.insert(&some_record).expect("insert some-db");

        let list = repo.list(&profile_id.to_string(), None).expect("list");

        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, none_record.id.to_string());
    }

    // --- prune ---

    #[test]
    fn prune_keeps_only_most_recent_n() {
        let (_, repo, profile_id) = setup("prune_keep_n");

        let ids: Vec<Uuid> = (0..5)
            .map(|i| {
                let record = sample_record(profile_id, Some("db1"), 1000 + i);
                repo.insert(&record).expect("insert");
                record.id
            })
            .collect();

        let deleted = repo
            .prune(&profile_id.to_string(), Some("db1"), 2)
            .expect("prune");

        assert_eq!(deleted, 3, "must delete all but the 2 most recent");

        let remaining = repo
            .list(&profile_id.to_string(), Some("db1"))
            .expect("list");
        assert_eq!(remaining.len(), 2);

        let remaining_ids: Vec<String> = remaining.iter().map(|s| s.id.clone()).collect();
        assert!(
            remaining_ids.contains(&ids[4].to_string()),
            "newest must survive"
        );
        assert!(
            remaining_ids.contains(&ids[3].to_string()),
            "2nd newest must survive"
        );
    }

    #[test]
    fn prune_cascades_child_table_rows() {
        let (conn, repo, profile_id) = setup("prune_cascade");

        let ids: Vec<Uuid> = (0..3)
            .map(|i| {
                let record = sample_record(profile_id, Some("db1"), 1000 + i);
                repo.insert(&record).expect("insert");
                record.id
            })
            .collect();

        repo.prune(&profile_id.to_string(), Some("db1"), 1)
            .expect("prune");

        let locked = conn.lock().unwrap();
        let pruned_children: i64 = locked
            .query_row(
                "SELECT COUNT(*) FROM sch_snapshot_tables WHERE snapshot_id = ?1",
                [ids[0].to_string()],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(
            pruned_children, 0,
            "children of pruned snapshot must cascade"
        );
    }

    #[test]
    fn prune_tie_break_on_equal_captured_at_is_deterministic() {
        let (_, repo, profile_id) = setup("prune_tie_break");

        let older = sample_record(profile_id, Some("db1"), 1000);
        let tied_first = sample_record(profile_id, Some("db1"), 2000);
        let tied_second = sample_record(profile_id, Some("db1"), 2000);

        repo.insert(&older).expect("insert older");
        repo.insert(&tied_first).expect("insert tied_first");
        repo.insert(&tied_second).expect("insert tied_second");

        let deleted = repo
            .prune(&profile_id.to_string(), Some("db1"), 1)
            .expect("prune");

        assert_eq!(deleted, 2);

        let remaining = repo
            .list(&profile_id.to_string(), Some("db1"))
            .expect("list");

        assert_eq!(remaining.len(), 1);
        assert_eq!(
            remaining[0].id,
            tied_second.id.to_string(),
            "the later-inserted row of a captured_at tie must survive deterministically"
        );
    }

    #[test]
    fn sch_schema_snapshots_equal_timestamp_list_and_prune_use_id_tie_break() {
        let (_, repo, profile_id) = setup("equal_timestamp_id_tie");
        let mut higher_id = sample_record(profile_id, Some("db1"), 2000);
        higher_id.id = Uuid::parse_str("01900000-0000-7000-8000-000000000002").expect("uuid");
        higher_id.depth = SnapshotDepth::Deep;
        let mut lower_id = sample_record(profile_id, Some("db1"), 2000);
        lower_id.id = Uuid::parse_str("01900000-0000-7000-8000-000000000001").expect("uuid");
        repo.insert(&higher_id).expect("insert higher id first");
        repo.insert(&lower_id).expect("insert lower id second");

        let listed = repo
            .list(&profile_id.to_string(), Some("db1"))
            .expect("list");
        assert_eq!(
            listed[0].id,
            higher_id.id.to_string(),
            "id determines latest equal-ms row"
        );
        repo.prune(&profile_id.to_string(), Some("db1"), 1)
            .expect("prune");
        assert!(repo.get(&higher_id.id.to_string()).expect("get").is_some());
        assert!(repo.get(&lower_id.id.to_string()).expect("get").is_none());
    }

    #[test]
    fn prune_does_not_touch_other_profile_or_database() {
        let (conn, repo, profile_id) = setup("prune_scope");

        let other_profile_id = Uuid::now_v7();
        {
            let locked = conn.lock().unwrap();
            locked
                .execute(
                    "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'Other')",
                    [&other_profile_id.to_string()],
                )
                .unwrap();
        }

        let record_a = sample_record(profile_id, Some("db1"), 1000);
        let record_b = sample_record(other_profile_id, Some("db1"), 1000);

        repo.insert(&record_a).expect("insert a");
        repo.insert(&record_b).expect("insert b");

        repo.prune(&profile_id.to_string(), Some("db1"), 0)
            .expect("prune profile_a to zero");

        let remaining_b = repo
            .list(&other_profile_id.to_string(), Some("db1"))
            .expect("list b");
        assert_eq!(remaining_b.len(), 1, "other profile must be untouched");
    }

    // --- creation metadata (migration 029) ---

    fn sample_creation_metadata(table_name: &str) -> dbflux_core::TableCreationMetadata {
        dbflux_core::TableCreationMetadata {
            schema: Some("public".to_string()),
            table: table_name.to_string(),
            completeness: dbflux_core::MetadataCompleteness::Complete,
            identity: Some(dbflux_core::IdentitySpec {
                column: "id".to_string(),
                seed: "99999999999999999999999999999999999999".to_string(),
                increment: "-1".to_string(),
            }),
            primary_key: Some(dbflux_core::PrimaryKeySpec {
                columns: vec!["id".to_string()],
            }),
            blockers: Vec::new(),
        }
    }

    /// Raw-SQL regression: migration 029 must add a nullable
    /// `creation_metadata_json` column to `sch_snapshot_tables`. Written
    /// against raw schema inspection so it can fail before any new Rust
    /// persistence API exists.
    #[test]
    fn migration_029_adds_nullable_creation_metadata_json_column() {
        let path = temp_db("m029_column");
        let conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let column: Option<(String, String, i64)> = conn
            .query_row(
                "SELECT name, type, \"notnull\" FROM pragma_table_info('sch_snapshot_tables')
                 WHERE name = 'creation_metadata_json'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .ok();

        let (name, column_type, notnull) = column.expect(
            "creation_metadata_json column must exist on sch_snapshot_tables after migrations",
        );
        assert_eq!(name, "creation_metadata_json");
        assert_eq!(column_type.to_uppercase(), "TEXT");
        assert_eq!(notnull, 0, "the column must be nullable for legacy rows");
    }

    /// Simulates upgrading a database created before migration 029: legacy
    /// rows are present, the column and its bookkeeping entry are removed,
    /// and the migration registry runs again.
    #[test]
    fn migration_029_upgrades_existing_snapshot_database_and_preserves_rows() {
        let (conn, repo, profile_id) = setup("m029_upgrade");

        // Legacy snapshot written before creation metadata existed.
        let legacy = sample_record(profile_id, Some("db1"), 1000);
        repo.insert(&legacy).expect("insert legacy record");

        {
            let locked = conn.lock().unwrap();
            locked
                .execute(
                    "ALTER TABLE sch_snapshot_tables DROP COLUMN creation_metadata_json",
                    [],
                )
                .expect("simulate pre-029 schema");
            locked
                .execute(
                    "DELETE FROM sys_migrations WHERE name = '029_sch_snapshot_creation_metadata'",
                    [],
                )
                .expect("clear 029 bookkeeping");
        }

        MigrationRegistry::new()
            .run_all(&conn.lock().unwrap())
            .expect("re-run migrations on the simulated old database");

        let column_exists: i64 = conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('sch_snapshot_tables')
                 WHERE name = 'creation_metadata_json'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(column_exists, 1, "re-run must re-create the column");

        // The legacy row survives the upgrade and stays readable.
        let loaded = repo
            .get(&legacy.id.to_string())
            .expect("get after upgrade")
            .expect("legacy row must survive");
        assert!(loaded.creation_metadata.is_empty());
        assert_eq!(loaded.tables.len(), 2);
    }

    #[test]
    fn insert_and_get_roundtrip_preserves_creation_metadata() {
        let (_, repo, profile_id) = setup("m029_roundtrip");

        let mut record = sample_record(profile_id, Some("app_db"), 1000);
        record.depth = SnapshotDepth::Deep;
        // Give the orders table its own schema so the association test proves
        // metadata lands on the right (schema, name) component pair.
        record.tables[1].schema = Some("sales".to_string());
        record.creation_metadata = vec![
            sample_creation_metadata("users"),
            sample_creation_metadata("orders"),
        ];
        record.creation_metadata[1].schema = Some("sales".to_string());

        repo.insert(&record).expect("insert");

        let loaded = repo
            .get(&record.id.to_string())
            .expect("get")
            .expect("exists");

        assert_eq!(loaded.creation_metadata.len(), 2);
        let users = loaded
            .creation_metadata
            .iter()
            .find(|m| m.table == "users")
            .expect("users metadata");
        assert_eq!(users.schema.as_deref(), Some("public"));
        assert_eq!(
            users.identity.as_ref().expect("identity").seed,
            "99999999999999999999999999999999999999",
            "38-digit seed must survive persistence as the exact decimal string"
        );
        let orders = loaded
            .creation_metadata
            .iter()
            .find(|m| m.table == "orders")
            .expect("orders metadata");
        assert_eq!(orders.schema.as_deref(), Some("sales"));
    }

    #[test]
    fn legacy_rows_without_metadata_remain_readable() {
        let (conn, repo, profile_id) = setup("m029_legacy_readable");

        let record = sample_record(profile_id, Some("db1"), 1000);
        repo.insert(&record).expect("insert");

        // Force the exact state a pre-029 database presents after upgrading:
        // NULL metadata on every table row.
        conn.lock()
            .unwrap()
            .execute(
                "UPDATE sch_snapshot_tables SET creation_metadata_json = NULL
                 WHERE snapshot_id = ?1",
                [record.id.to_string()],
            )
            .expect("null out metadata");

        let loaded = repo
            .get(&record.id.to_string())
            .expect("get")
            .expect("exists");
        assert!(loaded.creation_metadata.is_empty());
        assert_eq!(loaded.tables.len(), 2);
    }
}
