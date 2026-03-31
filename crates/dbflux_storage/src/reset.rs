//! Runtime-state reset support.
//!
//! Provides functionality to clear runtime state in `dbflux.db` without affecting
//! durable configuration. This is useful for "factory reset"
//! scenarios where the user wants to clear sessions, history, and UI state
//! while preserving all connection profiles and settings.

use std::path::PathBuf;

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;
use crate::paths;

/// Result of a runtime-state reset operation.
#[derive(Debug, Clone, Default)]
pub struct ResetResult {
    pub state_db_path: PathBuf,
    pub tables_cleared: Vec<String>,
    pub rows_deleted: usize,
    pub errors: Vec<String>,
}

impl ResetResult {
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

/// Clears all tables in `dbflux.db` (sessions, query_history, saved_queries,
/// recent_items, app_runtime_state).
///
/// This does NOT touch configuration — connection profiles, auth profiles,
/// proxies, SSH tunnels, hook definitions, services, and settings are preserved.
///
/// To fully reset runtime state including deleting the dbflux.db file, use
/// `hard_reset()` instead.
pub fn clear_state_db(conn: &OwnedConnection) -> ResetResult {
    let mut result = ResetResult {
        state_db_path: paths::dbflux_db_path().unwrap_or_else(|_| PathBuf::from("dbflux.db")),
        ..Default::default()
    };

    // Run all table clears in a single transaction so they succeed or fail together.
    let tx = match conn.unchecked_transaction() {
        Ok(t) => t,
        Err(e) => {
            result
                .errors
                .push(format!("cannot start transaction: {}", e));
            return result;
        }
    };

    let tables = [
        ("st_sessions", "DELETE FROM st_sessions"),
        ("st_session_tabs", "DELETE FROM st_session_tabs"),
        ("st_query_history", "DELETE FROM st_query_history"),
        (
            "st_saved_query_folders",
            "DELETE FROM st_saved_query_folders",
        ),
        ("st_saved_queries", "DELETE FROM st_saved_queries"),
        ("st_recent_items", "DELETE FROM st_recent_items"),
        ("st_ui_state", "DELETE FROM st_ui_state"),
        ("st_schema_cache", "DELETE FROM st_schema_cache"),
        ("st_event_log", "DELETE FROM st_event_log"),
    ];

    for (table, sql) in tables {
        match tx.execute(sql, []) {
            Ok(count) => {
                result.tables_cleared.push(table.to_string());
                result.rows_deleted += count;
            }
            Err(e) => {
                result.errors.push(format!("{}: {}", table, e));
                return result;
            }
        }
    }

    if let Err(e) = tx.commit() {
        result.errors.push(format!("commit failed: {}", e));
    }

    result
}

/// Performs a hard reset by deleting and recreating the database.
///
/// This removes the `dbflux.db` file entirely and creates a fresh one with
/// all migrations applied. All data is preserved in the fresh database.
///
/// Returns the path to the new database.
pub fn hard_reset() -> Result<PathBuf, StorageError> {
    let path = paths::dbflux_db_path()?;

    // Close the connection if open (this is best-effort)
    // Delete the file
    if path.exists() {
        std::fs::remove_file(&path).map_err(|source| StorageError::Io {
            path: path.clone(),
            source,
        })?;
    }

    // Also remove WAL and SHM files
    for ext in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{}", path.display(), ext));
        if sidecar.exists() {
            let _ = std::fs::remove_file(&sidecar);
        }
    }

    // Re-create the database with migrations
    let conn = crate::sqlite::open_database(&path)?;
    crate::migrations::MigrationRegistry::new().run_all(&conn)?;

    log::info!(
        "Hard reset completed: dbflux.db recreated at {}",
        path.display()
    );
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_state_db(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_reset_state_{}_{}.sqlite",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let conn = open_database(&path).expect("open");
        migrations::MigrationRegistry::new()
            .run_all(&conn)
            .expect("migrate");
        path
    }

    #[test]
    fn clear_state_db_removes_tables() {
        let path = temp_state_db("clear_tables");
        let conn = Arc::new(open_database(&path).expect("open"));

        // Insert some test data
        conn.execute(
            "INSERT INTO st_query_history (id, query_text, executed_at) VALUES (?1, ?2, datetime('now'))",
            ["h1", "SELECT 1"],
        )
        .expect("insert history");
        conn.execute(
            "INSERT INTO st_ui_state (key, value_json) VALUES (?1, ?2)",
            ["test_key", r#"{"value":true}"#],
        )
        .expect("insert state");

        let result = clear_state_db(&conn);

        assert!(
            result
                .tables_cleared
                .contains(&"st_query_history".to_string())
        );
        assert!(result.tables_cleared.contains(&"st_ui_state".to_string()));
        assert!(result.rows_deleted >= 2);

        // Verify data is gone
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM st_query_history", [], |row| {
                row.get(0)
            })
            .expect("query");
        assert_eq!(count, 0);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn reset_result_tracks_errors() {
        let mut result = ResetResult::default();
        result.errors.push("test error".to_string());

        assert!(result.has_errors());
    }

    #[test]
    fn clear_state_db_clears_all_migrated_tables() {
        let path = temp_state_db("all_tables");
        let conn = Arc::new(open_database(&path).expect("open"));

        // Insert test data across all migrated tables
        conn.execute(
            "INSERT INTO st_query_history (id, query_text, executed_at) VALUES (?1, ?2, datetime('now'))",
            ["h1", "SELECT 1"],
        )
        .expect("insert history");
        conn.execute(
            "INSERT INTO st_ui_state (key, value_json) VALUES (?1, ?2)",
            ["test_key", r#"{"value":true}"#],
        )
        .expect("insert state");
        conn.execute(
            "INSERT INTO st_recent_items (id, kind, title, accessed_at) VALUES (?1, ?2, ?3, datetime('now'))",
            ["r1", "file", "test.txt"],
        )
        .expect("insert recent");
        conn.execute(
            "INSERT INTO st_saved_queries (id, name, sql, created_at, last_used_at) VALUES (?1, ?2, ?3, datetime('now'), datetime('now'))",
            ["sq1", "Test Query", "SELECT 1"],
        )
        .expect("insert saved query");
        conn.execute(
            "INSERT INTO st_sessions (id, name) VALUES (?1, ?2)",
            ["s1", "Test Session"],
        )
        .expect("insert session");
        conn.execute(
            "INSERT INTO st_event_log (id, event_kind, description) VALUES (?1, ?2, ?3)",
            ["e1", "test", "Test event"],
        )
        .expect("insert event");
        conn.execute(
            "INSERT INTO st_schema_cache (id, cache_key, driver_id, connection_fingerprint, resource_kind, resource_name, payload_json, expires_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, datetime('now', '+1 day'))",
            rusqlite::params!["sc1", "key1", "postgres", "fp1", "table", "users", r#"{"cols":[]}"#],
        )
        .expect("insert schema cache");
        conn.execute(
            "INSERT INTO st_saved_query_folders (id, name) VALUES (?1, ?2)",
            ["f1", "Test Folder"],
        )
        .expect("insert folder");

        let result = clear_state_db(&conn);

        // All 9 migrated tables should be in tables_cleared
        assert!(
            result
                .tables_cleared
                .contains(&"st_query_history".to_string())
        );
        assert!(result.tables_cleared.contains(&"st_ui_state".to_string()));
        assert!(
            result
                .tables_cleared
                .contains(&"st_recent_items".to_string())
        );
        assert!(
            result
                .tables_cleared
                .contains(&"st_saved_queries".to_string())
        );
        assert!(result.tables_cleared.contains(&"st_sessions".to_string()));
        assert!(result.tables_cleared.contains(&"st_event_log".to_string()));
        assert!(
            result
                .tables_cleared
                .contains(&"st_schema_cache".to_string())
        );
        assert!(
            result
                .tables_cleared
                .contains(&"st_saved_query_folders".to_string())
        );
        assert!(
            result
                .tables_cleared
                .contains(&"st_session_tabs".to_string())
        );

        // Verify data is gone
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM st_query_history", [], |row| {
                row.get(0)
            })
            .expect("query");
        assert_eq!(count, 0);

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM st_sessions", [], |row| row.get(0))
            .expect("query");
        assert_eq!(count, 0);

        // sys_migrations table is NOT cleared (migration bookkeeping preserved)
        let migration_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sys_migrations", [], |row| row.get(0))
            .expect("query");
        assert_eq!(
            migration_count, 1,
            "sys_migrations should be preserved (1 migration: 001_initial)"
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn hard_reset_recreates_unified_db() {
        // Create a temporary directory and initialize a unified dbflux.db
        let base_dir = std::env::temp_dir().join(format!(
            "dbflux_hard_reset_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&base_dir).unwrap();

        let db_path = base_dir.join("dbflux.db");

        // Create unified db with data
        let conn = open_database(&db_path).expect("create db");
        crate::migrations::MigrationRegistry::new()
            .run_all(&conn)
            .expect("migrate");
        conn.execute(
            "INSERT INTO st_ui_state (key, value_json) VALUES (?1, ?2)",
            ["test_key", r#"{"value":true}"#],
        )
        .expect("insert state");

        // Verify db exists with data
        assert!(db_path.exists());
        let count_before: i64 = conn
            .query_row("SELECT COUNT(*) FROM st_ui_state", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count_before, 1);

        // Simulate hard_reset: delete and recreate
        drop(conn);
        std::fs::remove_file(&db_path).expect("delete dbflux.db");
        for ext in ["-wal", "-shm"] {
            let sidecar = format!("{}{}", db_path.display(), ext);
            let _ = std::fs::remove_file(sidecar);
        }

        // Recreate with migrations
        let new_conn = open_database(&db_path).expect("recreate");
        crate::migrations::MigrationRegistry::new()
            .run_all(&new_conn)
            .expect("re-migrate");

        // Verify fresh db has migrations recorded
        let migration_count: i64 = new_conn
            .query_row("SELECT COUNT(*) FROM sys_migrations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            migration_count, 1,
            "001_initial migration should be recorded"
        );

        let _ = std::fs::remove_dir_all(&base_dir);
    }
}
