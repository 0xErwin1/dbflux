//! Migration 015: Tighten the inspector-panel CHECK constraint.
//!
//! The migration 014 CHECK for `panel_kind = 'inspector'` did not enforce that
//! `saved_chart_id` must be empty. This migration rebuilds `viz_dashboard_panels`
//! with the corrected constraint so that inspector rows are rejected if they
//! accidentally carry a non-empty `saved_chart_id`.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "015_viz_inspector_saved_chart_id_constraint"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        tx.execute_batch(SCHEMA).map_err(sqlite_err)?;
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
PRAGMA foreign_keys = OFF;

CREATE TABLE viz_dashboard_panels__015 (
    dashboard_id        TEXT    NOT NULL
        REFERENCES viz_dashboards(id) ON DELETE CASCADE,
    panel_index         INTEGER NOT NULL CHECK (panel_index >= 0),
    panel_kind          TEXT    NOT NULL DEFAULT 'chart'
        CHECK (panel_kind IN ('chart', 'divider', 'inspector')),
    saved_chart_id      TEXT    NOT NULL,
    divider_markdown    TEXT,
    inspector_metric_id TEXT,
    title_override      TEXT,
    grid_row            INTEGER NOT NULL CHECK (grid_row >= 0),
    grid_column         INTEGER NOT NULL CHECK (grid_column >= 0),
    grid_width          INTEGER NOT NULL CHECK (grid_width >= 1),
    grid_height         INTEGER NOT NULL CHECK (grid_height >= 1),

    PRIMARY KEY (dashboard_id, panel_index),

    CHECK (
        (panel_kind = 'chart'     AND saved_chart_id != ''                                           AND divider_markdown IS NULL     AND inspector_metric_id IS NULL) OR
        (panel_kind = 'divider'   AND divider_markdown IS NOT NULL                                   AND inspector_metric_id IS NULL) OR
        (panel_kind = 'inspector' AND inspector_metric_id IS NOT NULL AND divider_markdown IS NULL    AND (saved_chart_id = '' OR saved_chart_id IS NULL))
    )
);

INSERT INTO viz_dashboard_panels__015 (
    dashboard_id, panel_index, panel_kind, saved_chart_id, divider_markdown,
    inspector_metric_id, title_override,
    grid_row, grid_column, grid_width, grid_height
)
SELECT
    dashboard_id, panel_index, panel_kind, saved_chart_id, divider_markdown,
    inspector_metric_id, title_override,
    grid_row, grid_column, grid_width, grid_height
FROM viz_dashboard_panels;

DROP TABLE viz_dashboard_panels;
ALTER TABLE viz_dashboard_panels__015 RENAME TO viz_dashboard_panels;

CREATE INDEX IF NOT EXISTS idx_viz_dashboard_panels_saved_chart
    ON viz_dashboard_panels (saved_chart_id);

PRAGMA foreign_keys = ON;
"#;

#[cfg(test)]
mod tests {
    use crate::migrations::MigrationRegistry;
    use rusqlite::Connection;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        MigrationRegistry::new().run_all(&conn).unwrap();
        conn
    }

    fn insert_profile(conn: &Connection) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, 'P')",
            [&id],
        )
        .unwrap();
        id
    }

    fn insert_dashboard(conn: &Connection, profile_id: &str) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO viz_dashboards \
             (id, name, profile_id, shared_refresh_policy_kind, grid_columns, created_at, updated_at) \
             VALUES (?1, 'D', ?2, 'off', 12, 0, 0)",
            [&id, profile_id],
        )
        .unwrap();
        id
    }

    /// K14: Inspector rows with a non-empty saved_chart_id must be rejected
    /// by the CHECK constraint.
    #[test]
    fn inspector_panel_with_saved_chart_id_rejected() {
        let conn = setup();
        let profile_id = insert_profile(&conn);
        let dashboard_id = insert_dashboard(&conn, &profile_id);

        let result = conn.execute(
            "INSERT INTO viz_dashboard_panels \
             (dashboard_id, panel_index, panel_kind, saved_chart_id, inspector_metric_id, \
              grid_row, grid_column, grid_width, grid_height) \
             VALUES (?1, 0, 'inspector', 'non-empty-chart-id', 'some.metric', 0, 0, 2, 2)",
            rusqlite::params![dashboard_id],
        );

        assert!(
            result.is_err(),
            "inspector row with non-empty saved_chart_id must be rejected by CHECK constraint"
        );
    }

    /// K14: Inspector rows with an empty saved_chart_id must be accepted.
    #[test]
    fn inspector_panel_with_empty_saved_chart_id_accepted() {
        let conn = setup();
        let profile_id = insert_profile(&conn);
        let dashboard_id = insert_dashboard(&conn, &profile_id);

        conn.execute(
            "INSERT INTO viz_dashboard_panels \
             (dashboard_id, panel_index, panel_kind, saved_chart_id, inspector_metric_id, \
              grid_row, grid_column, grid_width, grid_height) \
             VALUES (?1, 0, 'inspector', '', 'some.metric', 0, 0, 2, 2)",
            rusqlite::params![dashboard_id],
        )
        .expect("inspector row with empty saved_chart_id must be accepted");
    }
}
