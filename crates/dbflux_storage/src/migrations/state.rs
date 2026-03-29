//! Database migration infrastructure for the state database.
//!
//! State database holds runtime state: UI layout, recent items, query history,
//! saved queries, sessions, schema cache, and event log.

use log::info;
use rusqlite::Connection;

use crate::error::StorageError;

/// Current state database schema version.
pub const INITIAL_VERSION: u32 = 1;

/// Runs all pending state database migrations.
pub fn run_state_migrations(conn: &Connection) -> Result<(), StorageError> {
    let current_version = current_schema_version(conn)?;

    info!("State database current schema version: {}", current_version);

    if current_version < INITIAL_VERSION {
        let tx = conn
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        run_initial_migration_in(&tx)?;

        tx.pragma_update(None, "user_version", INITIAL_VERSION)
            .map_err(|source| StorageError::Sqlite {
                path: "state.db".into(),
                source,
            })?;

        tx.execute(
            "INSERT INTO schema_migrations (version, name) VALUES (?1, ?2)",
            rusqlite::params![INITIAL_VERSION, "0001_initial"],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "state.db".into(),
            source,
        })?;

        tx.commit().map_err(|source| StorageError::Sqlite {
            path: "state.db".into(),
            source,
        })?;

        info!(
            "State initial migration {} applied successfully",
            INITIAL_VERSION
        );
    }

    Ok(())
}

fn current_schema_version(conn: &Connection) -> Result<u32, StorageError> {
    match conn.pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0)) {
        Ok(v) => Ok(v),
        Err(rusqlite::Error::InvalidQuery) => Ok(0),
        Err(e) => Err(StorageError::Sqlite {
            path: "state.db".into(),
            source: e,
        }),
    }
}

/// Creates the baseline state tables: app_runtime_state, recent_items, query_history,
/// saved_query_folders, saved_queries, sessions, session_tabs, schema_cache, event_log.
fn run_initial_migration_in(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- app_runtime_state: persisted UI layout/collapse preferences
        CREATE TABLE IF NOT EXISTS app_runtime_state (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- recent_items: recently opened files / connections
        CREATE TABLE IF NOT EXISTS recent_items (
            id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            profile_id TEXT,
            path TEXT,
            title TEXT,
            accessed_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- query_history: individual query executions
        CREATE TABLE IF NOT EXISTS query_history (
            id TEXT PRIMARY KEY,
            connection_profile_id TEXT,
            driver_id TEXT,
            database_name TEXT,
            query_text TEXT NOT NULL,
            query_kind TEXT NOT NULL DEFAULT 'select',
            executed_at TEXT NOT NULL DEFAULT (datetime('now')),
            duration_ms INTEGER,
            succeeded INTEGER NOT NULL DEFAULT 1,
            error_summary TEXT,
            row_count INTEGER,
            is_favorite INTEGER NOT NULL DEFAULT 0
        );

        -- saved_query_folders: folder structure for saved queries
        CREATE TABLE IF NOT EXISTS saved_query_folders (
            id TEXT PRIMARY KEY,
            parent_id TEXT,
            name TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (parent_id) REFERENCES saved_query_folders(id) ON DELETE CASCADE
        );

        -- saved_queries: named, reusable query definitions
        CREATE TABLE IF NOT EXISTS saved_queries (
            id TEXT PRIMARY KEY,
            folder_id TEXT,
            name TEXT NOT NULL,
            sql TEXT NOT NULL,
            is_favorite INTEGER NOT NULL DEFAULT 0,
            connection_id TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_used_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (folder_id) REFERENCES saved_query_folders(id) ON DELETE SET NULL
        );

        -- sessions: workspace session metadata
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            kind TEXT NOT NULL DEFAULT 'workspace',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_opened_at TEXT NOT NULL DEFAULT (datetime('now')),
            is_last_active INTEGER NOT NULL DEFAULT 1
        );

        -- session_tabs: per-session tab restore data
        CREATE TABLE IF NOT EXISTS session_tabs (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            tab_kind TEXT NOT NULL,
            title TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            is_pinned INTEGER NOT NULL DEFAULT 0,
            restore_payload_json TEXT,
            scratch_file_path TEXT,
            shadow_file_path TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
        );

        -- schema_cache: cached schema metadata keyed by connection fingerprint
        CREATE TABLE IF NOT EXISTS schema_cache (
            id TEXT PRIMARY KEY,
            cache_key TEXT NOT NULL,
            driver_id TEXT NOT NULL,
            connection_fingerprint TEXT NOT NULL,
            resource_kind TEXT NOT NULL,
            resource_name TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- event_log: background event/task history
        CREATE TABLE IF NOT EXISTS event_log (
            id TEXT PRIMARY KEY,
            event_kind TEXT NOT NULL,
            description TEXT NOT NULL,
            target_kind TEXT,
            target_id TEXT,
            details_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        "#,
    )
    .map_err(|source| StorageError::Sqlite {
        path: "state.db".into(),
        source,
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::open_database;
    use std::path::PathBuf;

    fn temp_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux_state_migrations_{}_{}",
            name,
            std::process::id()
        ))
    }

    fn cleanup(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn initial_migration_creates_tables() {
        let path = temp_db("state_initial");
        cleanup(&path);

        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("migration should run");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 1);

        // Verify key tables exist
        conn.execute("SELECT 1 FROM app_runtime_state", [])
            .expect("app_runtime_state should exist");
        conn.execute("SELECT 1 FROM recent_items", [])
            .expect("recent_items should exist");
        conn.execute("SELECT 1 FROM query_history", [])
            .expect("query_history should exist");
        conn.execute("SELECT 1 FROM saved_query_folders", [])
            .expect("saved_query_folders should exist");
        conn.execute("SELECT 1 FROM saved_queries", [])
            .expect("saved_queries should exist");
        conn.execute("SELECT 1 FROM sessions", [])
            .expect("sessions should exist");
        conn.execute("SELECT 1 FROM session_tabs", [])
            .expect("session_tabs should exist");
        conn.execute("SELECT 1 FROM schema_cache", [])
            .expect("schema_cache should exist");
        conn.execute("SELECT 1 FROM event_log", [])
            .expect("event_log should exist");

        cleanup(&path);
    }

    #[test]
    fn migration_is_idempotent() {
        let path = temp_db("state_idempotent");
        cleanup(&path);

        let conn = open_database(&path).expect("should open");

        run_state_migrations(&conn).expect("first migration should run");
        run_state_migrations(&conn).expect("second migration should be idempotent");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 1);

        cleanup(&path);
    }

    #[test]
    fn migration_isolated_from_config_db() {
        // Verify state and config don't share tables
        let path = temp_db("state_isolation");
        cleanup(&path);

        let conn = open_database(&path).expect("should open");
        run_state_migrations(&conn).expect("state migration should run");

        // Config tables should not exist in state DB
        let result = conn.query_row(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='connection_profiles'",
            [],
            |_| Ok(()),
        );
        assert!(
            result.is_err(),
            "config tables should not exist in state DB"
        );

        cleanup(&path);
    }
}
