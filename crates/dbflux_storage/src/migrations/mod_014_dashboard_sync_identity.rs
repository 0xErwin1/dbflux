//! Migration 014: Dashboard sync identity columns.
//!
//! Adds identity columns to `viz_dashboards` and `viz_dashboard_panels` so
//! dashboards previously imported from an upstream source (e.g. CloudWatch)
//! can be detected, diffed, and refreshed against the live upstream.
//!
//! Schema changes on `viz_dashboards` (all additive ALTER TABLE):
//! - `source_kind`             TEXT NOT NULL DEFAULT 'local'  CHECK in ('local','cloudwatch')
//! - `source_account_id`       TEXT
//! - `source_home_region`      TEXT
//! - `source_dashboard_name`   TEXT
//! - `source_content_hash`     TEXT
//! - `source_last_modified`    TEXT (ISO8601)
//! - `source_last_synced_at`   TEXT (ISO8601)
//!
//! A unique partial index enforces `(source_account_id, source_dashboard_name)`
//! uniqueness WHERE `source_kind = 'cloudwatch'`.
//!
//! Schema changes on `viz_dashboard_panels`:
//! - `source_widget_index`     INTEGER
//! - `source_widget_hash`      TEXT
//!
//! Pre-existing dashboards default to `source_kind = 'local'`; all other
//! identity fields default to NULL. Such rows are treated as detached and
//! never trigger drift checks.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "014_dashboard_sync_identity"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        tx.pragma_update(None, "legacy_alter_table", "ON")
            .map_err(sqlite_err)?;

        let result = tx.execute_batch(SCHEMA);

        let restore = tx.pragma_update(None, "legacy_alter_table", "OFF");

        result.map_err(sqlite_err)?;
        restore.map_err(sqlite_err)?;
        Ok(())
    }
}

fn sqlite_err(source: rusqlite::Error) -> MigrationError {
    MigrationError::Sqlite {
        path: std::path::PathBuf::from("<unknown>"),
        source,
    }
}

const SCHEMA: &str = r#"
ALTER TABLE viz_dashboards ADD COLUMN source_kind TEXT NOT NULL DEFAULT 'local'
    CHECK (source_kind IN ('local', 'cloudwatch'));
ALTER TABLE viz_dashboards ADD COLUMN source_account_id     TEXT;
ALTER TABLE viz_dashboards ADD COLUMN source_home_region    TEXT;
ALTER TABLE viz_dashboards ADD COLUMN source_dashboard_name TEXT;
ALTER TABLE viz_dashboards ADD COLUMN source_content_hash   TEXT;
ALTER TABLE viz_dashboards ADD COLUMN source_last_modified  TEXT;
ALTER TABLE viz_dashboards ADD COLUMN source_last_synced_at TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS viz_dashboards_cw_identity
    ON viz_dashboards (source_account_id, source_dashboard_name)
    WHERE source_kind = 'cloudwatch';

ALTER TABLE viz_dashboard_panels ADD COLUMN source_widget_index INTEGER;
ALTER TABLE viz_dashboard_panels ADD COLUMN source_widget_hash  TEXT;
"#;

#[cfg(test)]
mod tests {
    use crate::migrations::MigrationRegistry;
    use rusqlite::Connection;
    use std::collections::HashSet;

    fn fresh_conn() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    fn columns(conn: &Connection, table: &str) -> HashSet<String> {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .unwrap();
        stmt.query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    }

    #[test]
    fn fresh_install_adds_all_identity_columns() {
        let conn = fresh_conn();
        MigrationRegistry::new().run_all(&conn).unwrap();

        let dashboard_cols = columns(&conn, "viz_dashboards");
        for expected in &[
            "source_kind",
            "source_account_id",
            "source_home_region",
            "source_dashboard_name",
            "source_content_hash",
            "source_last_modified",
            "source_last_synced_at",
        ] {
            assert!(
                dashboard_cols.contains(*expected),
                "viz_dashboards missing column '{expected}'"
            );
        }

        let panel_cols = columns(&conn, "viz_dashboard_panels");
        for expected in &["source_widget_index", "source_widget_hash"] {
            assert!(
                panel_cols.contains(*expected),
                "viz_dashboard_panels missing column '{expected}'"
            );
        }
    }

    #[test]
    fn second_run_all_is_idempotent() {
        let conn = fresh_conn();
        let registry = MigrationRegistry::new();
        registry.run_all(&conn).unwrap();
        registry.run_all(&conn).unwrap();

        let cols = columns(&conn, "viz_dashboards");
        assert!(cols.contains("source_kind"));
    }

    #[test]
    fn existing_dashboards_default_to_local_after_migration() {
        let conn = fresh_conn();
        MigrationRegistry::new().run_all(&conn).unwrap();

        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES ('p1', 'P1')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO viz_dashboards
                 (id, name, profile_id, shared_refresh_policy_kind, grid_columns,
                  created_at, updated_at)
             VALUES ('d1', 'D', 'p1', 'off', 12, 0, 0)",
            [],
        )
        .unwrap();

        let (kind, account, hash): (String, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT source_kind, source_account_id, source_content_hash
                 FROM viz_dashboards WHERE id = 'd1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(kind, "local");
        assert!(account.is_none());
        assert!(hash.is_none());
    }

    #[test]
    fn cw_identity_index_rejects_duplicate_account_and_name() {
        let conn = fresh_conn();
        MigrationRegistry::new().run_all(&conn).unwrap();

        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES ('p1', 'P1')",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO viz_dashboards
                 (id, name, profile_id, shared_refresh_policy_kind, grid_columns,
                  created_at, updated_at, source_kind, source_account_id,
                  source_dashboard_name)
             VALUES ('d1', 'D', 'p1', 'off', 12, 0, 0, 'cloudwatch',
                     '123456789012', 'prod-overview')",
            [],
        )
        .unwrap();

        let dup = conn.execute(
            "INSERT INTO viz_dashboards
                 (id, name, profile_id, shared_refresh_policy_kind, grid_columns,
                  created_at, updated_at, source_kind, source_account_id,
                  source_dashboard_name)
             VALUES ('d2', 'D2', 'p1', 'off', 12, 0, 0, 'cloudwatch',
                     '123456789012', 'prod-overview')",
            [],
        );
        assert!(
            dup.is_err(),
            "duplicate (account_id, dashboard_name) for cloudwatch must be rejected"
        );
    }

    #[test]
    fn cw_identity_index_allows_duplicate_names_for_local_rows() {
        let conn = fresh_conn();
        MigrationRegistry::new().run_all(&conn).unwrap();

        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES ('p1', 'P1')",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO viz_dashboards
                 (id, name, profile_id, shared_refresh_policy_kind, grid_columns,
                  created_at, updated_at)
             VALUES ('d1', 'D', 'p1', 'off', 12, 0, 0)",
            [],
        )
        .unwrap();

        // Same default kind=local, same name implicit -- partial index ignores 'local'.
        conn.execute(
            "INSERT INTO viz_dashboards
                 (id, name, profile_id, shared_refresh_policy_kind, grid_columns,
                  created_at, updated_at)
             VALUES ('d2', 'D', 'p1', 'off', 12, 0, 0)",
            [],
        )
        .expect("local rows with identical names must be allowed");
    }

    #[test]
    fn source_kind_check_rejects_unknown_values() {
        let conn = fresh_conn();
        MigrationRegistry::new().run_all(&conn).unwrap();

        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES ('p1', 'P1')",
            [],
        )
        .unwrap();

        let bad = conn.execute(
            "INSERT INTO viz_dashboards
                 (id, name, profile_id, shared_refresh_policy_kind, grid_columns,
                  created_at, updated_at, source_kind)
             VALUES ('d1', 'D', 'p1', 'off', 12, 0, 0, 'datadog')",
            [],
        );
        assert!(
            bad.is_err(),
            "source_kind must be restricted to ('local','cloudwatch')"
        );
    }
}
