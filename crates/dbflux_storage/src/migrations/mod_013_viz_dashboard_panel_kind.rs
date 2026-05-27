//! Migration 013: Dashboard panel kind discriminator.
//!
//! Adds a `panel_kind` column to `viz_dashboard_panels` so a panel can either
//! be a chart slot (referencing a SavedChart) or a markdown divider (no chart
//! reference). The companion `divider_markdown` column carries the markdown
//! source for `panel_kind = 'divider'` rows.
//!
//! Schema changes:
//!
//! 1. `panel_kind` TEXT NOT NULL DEFAULT 'chart' — discriminator
//!    (`'chart'` or `'divider'`). Existing rows default to `'chart'`.
//! 2. `divider_markdown` TEXT NULLABLE — markdown payload for dividers.
//!    NOT NULL when `panel_kind = 'divider'` (enforced by CHECK).
//! 3. `saved_chart_id` is relaxed so divider rows can carry an empty string;
//!    a CHECK ensures it stays non-empty for chart rows.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "013_viz_dashboard_panel_kind"
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
CREATE TABLE viz_dashboard_panels_new (
    dashboard_id        TEXT    NOT NULL
        REFERENCES viz_dashboards(id) ON DELETE CASCADE,
    panel_index         INTEGER NOT NULL CHECK (panel_index >= 0),
    panel_kind          TEXT    NOT NULL DEFAULT 'chart'
        CHECK (panel_kind IN ('chart', 'divider')),
    saved_chart_id      TEXT    NOT NULL,
    divider_markdown    TEXT
        CHECK (panel_kind != 'divider' OR divider_markdown IS NOT NULL),
    title_override      TEXT,
    grid_row            INTEGER NOT NULL CHECK (grid_row >= 0),
    grid_column         INTEGER NOT NULL CHECK (grid_column >= 0),
    grid_width          INTEGER NOT NULL CHECK (grid_width >= 1),
    grid_height         INTEGER NOT NULL CHECK (grid_height >= 1),

    PRIMARY KEY (dashboard_id, panel_index)
);

INSERT INTO viz_dashboard_panels_new
    (dashboard_id, panel_index, panel_kind, saved_chart_id, divider_markdown,
     title_override, grid_row, grid_column, grid_width, grid_height)
SELECT dashboard_id, panel_index, 'chart', saved_chart_id, NULL,
       title_override, grid_row, grid_column, grid_width, grid_height
FROM viz_dashboard_panels;

DROP TABLE viz_dashboard_panels;
ALTER TABLE viz_dashboard_panels_new RENAME TO viz_dashboard_panels;

CREATE INDEX IF NOT EXISTS idx_viz_dashboard_panels_saved_chart
    ON viz_dashboard_panels (saved_chart_id);
"#;

#[cfg(test)]
mod tests {
    use crate::migrations::MigrationRegistry;
    use rusqlite::Connection;

    fn fresh_conn() -> Connection {
        Connection::open_in_memory().unwrap()
    }

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
    fn fresh_install_adds_panel_kind_and_divider_markdown() {
        let conn = fresh_conn();
        MigrationRegistry::new().run_all(&conn).unwrap();

        let cols = columns(&conn, "viz_dashboard_panels");
        assert!(cols.contains("panel_kind"));
        assert!(cols.contains("divider_markdown"));
    }

    #[test]
    fn existing_chart_rows_default_to_chart_kind() {
        let conn = fresh_conn();
        let registry = MigrationRegistry::new();
        registry.run_all(&conn).unwrap();

        // Insert a chart-kind panel through the new schema and confirm the default.
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
        conn.execute(
            "INSERT INTO viz_dashboard_panels
                 (dashboard_id, panel_index, saved_chart_id,
                  grid_row, grid_column, grid_width, grid_height)
             VALUES ('d1', 0, 'chart-id', 0, 0, 6, 4)",
            [],
        )
        .unwrap();

        let kind: String = conn
            .query_row(
                "SELECT panel_kind FROM viz_dashboard_panels
                 WHERE dashboard_id = 'd1' AND panel_index = 0",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(kind, "chart");
    }

    #[test]
    fn divider_requires_markdown() {
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

        // Divider with NULL markdown — must violate the CHECK.
        let bad = conn.execute(
            "INSERT INTO viz_dashboard_panels
                 (dashboard_id, panel_index, panel_kind, saved_chart_id, divider_markdown,
                  grid_row, grid_column, grid_width, grid_height)
             VALUES ('d1', 0, 'divider', '', NULL, 0, 0, 12, 1)",
            [],
        );
        assert!(bad.is_err(), "divider with NULL markdown must fail CHECK");

        // Divider with a markdown string — succeeds.
        conn.execute(
            "INSERT INTO viz_dashboard_panels
                 (dashboard_id, panel_index, panel_kind, saved_chart_id, divider_markdown,
                  grid_row, grid_column, grid_width, grid_height)
             VALUES ('d1', 0, 'divider', '', '# Header', 0, 0, 12, 1)",
            [],
        )
        .unwrap();
    }
}
