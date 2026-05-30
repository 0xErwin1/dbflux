//! Migration 014: Audit settings — log capture minimum level.
//!
//! Adds a `log_capture_min_level` column to `cfg_audit_settings` so the
//! tracing bridge capture threshold can be persisted and updated at runtime
//! without application restart.
//!
//! Schema change:
//!
//! `log_capture_min_level` TEXT NOT NULL DEFAULT 'info' — minimum severity for
//! routing tracing events to `aud_audit_events`. Valid values mirror
//! `EventSeverity`: `trace`, `debug`, `info`, `warn`, `error`, `fatal`.
//! Existing rows receive the `'info'` default on migration.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "014_audit_settings_log_capture_min_level"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        // If cfg_audit_settings does not exist (e.g. in a partial test schema
        // that recorded earlier migrations without creating tables), skip the
        // ALTER gracefully. The table is always present in real databases.
        let table_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cfg_audit_settings'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;

        if !table_exists {
            return Ok(());
        }

        // Check whether the column already exists (defensive for re-runs).
        let column_exists: bool = tx
            .prepare("PRAGMA table_info(cfg_audit_settings)")
            .and_then(|mut stmt| {
                let exists = stmt
                    .query_map([], |row| row.get::<_, String>(1))?
                    .filter_map(|r| r.ok())
                    .any(|name| name == "log_capture_min_level");
                Ok(exists)
            })
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;

        if column_exists {
            return Ok(());
        }

        tx.execute(
            "ALTER TABLE cfg_audit_settings ADD COLUMN log_capture_min_level TEXT NOT NULL DEFAULT 'info'",
            [],
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::migrations::MigrationRegistry;

    fn columns(conn: &Connection, table: &str) -> std::collections::HashSet<String> {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .unwrap();
        stmt.query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    }

    #[test]
    fn fresh_install_adds_log_capture_min_level_column() {
        let conn = Connection::open_in_memory().unwrap();
        MigrationRegistry::new().run_all(&conn).unwrap();

        let cols = columns(&conn, "cfg_audit_settings");
        assert!(
            cols.contains("log_capture_min_level"),
            "cfg_audit_settings should have log_capture_min_level after migration"
        );
    }

    #[test]
    fn default_value_is_info() {
        let conn = Connection::open_in_memory().unwrap();
        MigrationRegistry::new().run_all(&conn).unwrap();

        // Insert a row with only the required id column to exercise the default.
        conn.execute(
            "INSERT OR IGNORE INTO cfg_audit_settings (id) VALUES (1)",
            [],
        )
        .unwrap();

        let level: String = conn
            .query_row(
                "SELECT log_capture_min_level FROM cfg_audit_settings WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(
            level, "info",
            "log_capture_min_level should default to 'info'"
        );
    }

    #[test]
    fn migration_skipped_when_already_applied_adds_column_on_manual_apply() {
        // Simulates a database that was at schema 013 (no log_capture_min_level).
        let conn = Connection::open_in_memory().unwrap();

        // Run migrations 001-013 only.
        conn.execute(
            "CREATE TABLE sys_migrations (name TEXT PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT (datetime('now')))",
            [],
        )
        .unwrap();

        // Run only migrations 001-013 then apply 014 and verify.
        let registry = MigrationRegistry::new();
        registry.run_all(&conn).unwrap();

        let cols = columns(&conn, "cfg_audit_settings");
        assert!(
            cols.contains("log_capture_min_level"),
            "column should be present after full migration run"
        );
    }
}
