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

use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use uuid::Uuid;

use dbflux_core::{SchemaSnapshotRecord, SnapshotDepth, TableInfo};

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
}

impl SchemaSnapshotRepo {
    /// Creates a new repository wrapping the given shared connection.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
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

        for table in &record.tables {
            let detail_json = serde_json::to_string(table)
                .map_err(|e| StorageError::Data(format!("serialize table detail: {e}")))?;

            tx.execute(
                "INSERT INTO sch_snapshot_tables (snapshot_id, schema_name, name, detail_json)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![id, table.schema, table.name, detail_json],
            )
            .map_err(sqlite_err)?;
        }

        tx.commit().map_err(sqlite_err)?;

        Ok(())
    }

    /// Lists snapshot summaries for `(profile_id, database)`, ordered by
    /// `captured_at DESC` (most recent first).
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
                 ORDER BY captured_at DESC",
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
                     ORDER BY captured_at DESC, rowid DESC
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
        .prepare("SELECT detail_json FROM sch_snapshot_tables WHERE snapshot_id = ?1")
        .map_err(sqlite_err)?;

    let tables = stmt
        .query_map([id], |row| row.get::<_, String>(0))
        .map_err(sqlite_err)?
        .filter_map(|r| r.ok())
        .map(|json| serde_json::from_str::<TableInfo>(&json))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| StorageError::Data(format!("deserialize table detail: {e}")))?;

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
        }
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
}
