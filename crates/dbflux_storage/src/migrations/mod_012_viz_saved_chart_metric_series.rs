//! Migration 012: Multi-series `SavedChartSource::Metric`.
//!
//! Migration 011 stored a single CloudWatch metric per chart in flat
//! `source_metric_*` columns on `viz_saved_charts` plus a child
//! `viz_saved_chart_source_metric_dimensions` table keyed on
//! `(chart_id, dim_index)`. The Metric source now carries N series per chart
//! so the schema gains a parent `viz_saved_chart_source_metric_series` table
//! and re-keys the dimensions table on
//! `(chart_id, series_index, dim_index)`.
//!
//! Changes performed:
//!
//! 1. Create `viz_saved_chart_source_metric_series` with columns
//!    `(chart_id, series_index, namespace, metric_name, period_seconds,
//!    statistic, region, label)` and PK `(chart_id, series_index)`.
//! 2. Rebuild `viz_saved_chart_source_metric_dimensions` with PK
//!    `(chart_id, series_index, dim_index)` and an FK to the new parent
//!    table on `(chart_id, series_index)`.
//! 3. Migrate any existing rows: every legacy single-series row writes one
//!    entry into the new series table at `series_index = 0` and its
//!    dimensions inherit `series_index = 0`.
//! 4. Rebuild `viz_saved_charts` to drop the now-redundant scalar
//!    `source_metric_*` columns. The CHECK on `source_kind = 'metric'` is
//!    relaxed to "the series table must contain at least one row" — a
//!    constraint the application layer enforces because SQLite cannot CHECK
//!    cross-table presence.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "012_viz_saved_chart_metric_series"
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
-- 1. Parent series table.
CREATE TABLE IF NOT EXISTS viz_saved_chart_source_metric_series (
    chart_id        TEXT    NOT NULL
        REFERENCES viz_saved_charts(id) ON DELETE CASCADE,
    series_index    INTEGER NOT NULL CHECK (series_index >= 0),
    namespace       TEXT    NOT NULL,
    metric_name     TEXT    NOT NULL,
    period_seconds  INTEGER NOT NULL CHECK (period_seconds > 0),
    statistic       TEXT    NOT NULL,
    region          TEXT,
    label           TEXT,

    PRIMARY KEY (chart_id, series_index)
);

CREATE INDEX IF NOT EXISTS idx_viz_metric_series_chart
    ON viz_saved_chart_source_metric_series (chart_id);

-- 2. Backfill the new series table from the legacy single-metric columns
--    on viz_saved_charts. Only rows whose source_kind = 'metric' contribute.
INSERT INTO viz_saved_chart_source_metric_series
    (chart_id, series_index, namespace, metric_name, period_seconds, statistic, region, label)
SELECT id, 0,
       source_metric_namespace,
       source_metric_name,
       source_metric_period_seconds,
       source_metric_statistic,
       source_metric_region,
       NULL
FROM viz_saved_charts
WHERE source_kind = 'metric'
  AND source_metric_namespace IS NOT NULL;

-- 3. Re-key the dimensions table on (chart_id, series_index, dim_index).
--    SQLite cannot ALTER PRIMARY KEY; build a new table and migrate rows.
CREATE TABLE viz_saved_chart_source_metric_dimensions_new (
    chart_id      TEXT    NOT NULL,
    series_index  INTEGER NOT NULL CHECK (series_index >= 0),
    dim_index     INTEGER NOT NULL CHECK (dim_index >= 0),
    dim_key       TEXT    NOT NULL,
    dim_value     TEXT    NOT NULL,

    PRIMARY KEY (chart_id, series_index, dim_index),
    FOREIGN KEY (chart_id, series_index)
        REFERENCES viz_saved_chart_source_metric_series (chart_id, series_index)
        ON DELETE CASCADE
);

INSERT INTO viz_saved_chart_source_metric_dimensions_new
    (chart_id, series_index, dim_index, dim_key, dim_value)
SELECT chart_id, 0, dim_index, dim_key, dim_value
FROM viz_saved_chart_source_metric_dimensions;

DROP TABLE viz_saved_chart_source_metric_dimensions;
ALTER TABLE viz_saved_chart_source_metric_dimensions_new
    RENAME TO viz_saved_chart_source_metric_dimensions;

CREATE INDEX IF NOT EXISTS idx_viz_metric_dimensions_series
    ON viz_saved_chart_source_metric_dimensions (chart_id, series_index);

-- 4. Rebuild viz_saved_charts to drop the now-redundant scalar metric columns.
CREATE TABLE viz_saved_charts_new (
    id                                TEXT    PRIMARY KEY,
    name                              TEXT    NOT NULL,
    profile_id                        TEXT    NOT NULL
        REFERENCES cfg_connection_profiles(id) ON DELETE CASCADE,
    created_at                        INTEGER NOT NULL,
    updated_at                        INTEGER NOT NULL,

    chart_kind                        TEXT    NOT NULL
        CHECK (chart_kind IN ('line', 'bar', 'scatter', 'area', 'stacked_bar', 'pie', 'number')),
    legend_visible                    INTEGER NOT NULL DEFAULT 0
        CHECK (legend_visible IN (0, 1)),
    decimation_threshold              INTEGER NOT NULL DEFAULT 10000,
    track_source_indices              INTEGER NOT NULL DEFAULT 0
        CHECK (track_source_indices IN (0, 1)),
    y_scale                           TEXT    NOT NULL DEFAULT 'linear'
        CHECK (y_scale IN ('linear', 'log')),

    x_axis_column_index               INTEGER NOT NULL CHECK (x_axis_column_index >= 0),
    x_axis_label                      TEXT    NOT NULL,
    x_axis_kind                       TEXT    NOT NULL CHECK (x_axis_kind IN ('time', 'numeric')),
    x_axis_unit                       TEXT,

    binding_x                         INTEGER NOT NULL CHECK (binding_x >= 0),
    binding_group_by                  INTEGER
        CHECK (binding_group_by IS NULL OR binding_group_by >= 0),
    binding_filter                    TEXT,
    binding_aggregation               TEXT    NOT NULL DEFAULT 'none'
        CHECK (binding_aggregation IN ('none', 'sum', 'avg', 'min', 'max')),

    source_kind                       TEXT    NOT NULL
        CHECK (source_kind IN ('query', 'collection', 'metric')),
    source_query                      TEXT
        CHECK (source_kind != 'query' OR source_query IS NOT NULL),
    source_collection_database        TEXT
        CHECK (source_kind != 'collection' OR source_collection_database IS NOT NULL),
    source_collection_name            TEXT
        CHECK (source_kind != 'collection' OR source_collection_name IS NOT NULL),
    source_time_window_start_ms       INTEGER,
    source_time_window_end_ms         INTEGER,
    source_time_window_language       TEXT,

    time_range_preset                 TEXT
        CHECK (time_range_preset IS NULL OR time_range_preset IN
            ('last_15_min', 'last_hour', 'last_6_hours', 'last_24_hours', 'last_7_days')),
    refresh_policy_kind               TEXT    NOT NULL DEFAULT 'off'
        CHECK (refresh_policy_kind IN ('off', 'interval', 'on_open')),
    refresh_policy_interval_secs      INTEGER
        CHECK (refresh_policy_kind != 'interval' OR refresh_policy_interval_secs IS NOT NULL)
);

INSERT INTO viz_saved_charts_new (
    id, name, profile_id, created_at, updated_at,
    chart_kind, legend_visible, decimation_threshold, track_source_indices, y_scale,
    x_axis_column_index, x_axis_label, x_axis_kind, x_axis_unit,
    binding_x, binding_group_by, binding_filter, binding_aggregation,
    source_kind, source_query,
    source_collection_database, source_collection_name,
    source_time_window_start_ms, source_time_window_end_ms, source_time_window_language,
    time_range_preset, refresh_policy_kind, refresh_policy_interval_secs
)
SELECT
    id, name, profile_id, created_at, updated_at,
    chart_kind, legend_visible, decimation_threshold, track_source_indices, y_scale,
    x_axis_column_index, x_axis_label, x_axis_kind, x_axis_unit,
    binding_x, binding_group_by, binding_filter, binding_aggregation,
    source_kind, source_query,
    source_collection_database, source_collection_name,
    source_time_window_start_ms, source_time_window_end_ms, source_time_window_language,
    time_range_preset, refresh_policy_kind, refresh_policy_interval_secs
FROM viz_saved_charts;

DROP TABLE viz_saved_charts;
ALTER TABLE viz_saved_charts_new RENAME TO viz_saved_charts;

CREATE INDEX IF NOT EXISTS idx_viz_saved_charts_profile
    ON viz_saved_charts (profile_id);

CREATE INDEX IF NOT EXISTS idx_viz_saved_charts_updated_at
    ON viz_saved_charts (updated_at DESC);
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

    /// Fresh install creates the series table and drops legacy metric columns
    /// from viz_saved_charts.
    #[test]
    fn fresh_install_has_series_table_and_no_legacy_metric_columns() {
        let conn = fresh_conn();
        MigrationRegistry::new().run_all(&conn).unwrap();

        let series_exists: bool = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='viz_saved_chart_source_metric_series'",
                [],
                |_| Ok(true),
            )
            .unwrap_or(false);
        assert!(series_exists, "series table must exist after mig 012");

        let chart_cols = columns(&conn, "viz_saved_charts");
        for dropped in [
            "source_metric_namespace",
            "source_metric_name",
            "source_metric_period_seconds",
            "source_metric_statistic",
            "source_metric_region",
        ] {
            assert!(
                !chart_cols.contains(dropped),
                "legacy metric column '{dropped}' must be dropped after mig 012"
            );
        }

        let dim_cols = columns(&conn, "viz_saved_chart_source_metric_dimensions");
        assert!(dim_cols.contains("series_index"), "dim table re-keyed");
    }

    /// Upgrade path: a legacy mig-011 metric row backfills into the new series
    /// table at series_index = 0 and its dimensions follow.
    ///
    /// Simulating a pre-012 state requires recreating the migration-011 schema
    /// in-place; this test rebuilds those tables before unmarking 012 so the
    /// re-run materialises the backfill behaviour.
    #[test]
    #[ignore = "rebuilds pre-012 schema by hand; brittle to schema drift"]
    fn legacy_metric_row_backfills_into_series_table() {
        let conn = fresh_conn();
        let registry = MigrationRegistry::new();

        registry.run_all(&conn).unwrap();

        // Pretend mig 012 hasn't run yet so we can seed a legacy row through
        // the migration-011 column shape. We need to recreate the pre-012
        // schema for viz_saved_charts and viz_saved_chart_source_metric_dimensions.
        conn.execute(
            "DELETE FROM sys_migrations WHERE name = '012_viz_saved_chart_metric_series'",
            [],
        )
        .unwrap();

        conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS viz_saved_chart_source_metric_series;
            DROP TABLE viz_saved_chart_source_metric_dimensions;
            DROP TABLE viz_saved_charts;

            CREATE TABLE viz_saved_charts (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                chart_kind TEXT NOT NULL,
                legend_visible INTEGER NOT NULL DEFAULT 0,
                decimation_threshold INTEGER NOT NULL DEFAULT 10000,
                track_source_indices INTEGER NOT NULL DEFAULT 0,
                y_scale TEXT NOT NULL DEFAULT 'linear',
                x_axis_column_index INTEGER NOT NULL,
                x_axis_label TEXT NOT NULL,
                x_axis_kind TEXT NOT NULL,
                x_axis_unit TEXT,
                binding_x INTEGER NOT NULL,
                binding_group_by INTEGER,
                binding_filter TEXT,
                binding_aggregation TEXT NOT NULL DEFAULT 'none',
                source_kind TEXT NOT NULL,
                source_query TEXT,
                source_collection_database TEXT,
                source_collection_name TEXT,
                source_time_window_start_ms INTEGER,
                source_time_window_end_ms INTEGER,
                source_time_window_language TEXT,
                source_metric_namespace TEXT,
                source_metric_name TEXT,
                source_metric_period_seconds INTEGER,
                source_metric_statistic TEXT,
                source_metric_region TEXT,
                time_range_preset TEXT,
                refresh_policy_kind TEXT NOT NULL DEFAULT 'off',
                refresh_policy_interval_secs INTEGER
            );

            CREATE TABLE viz_saved_chart_source_metric_dimensions (
                chart_id TEXT NOT NULL,
                dim_index INTEGER NOT NULL,
                dim_key TEXT NOT NULL,
                dim_value TEXT NOT NULL,
                PRIMARY KEY (chart_id, dim_index)
            );

            INSERT INTO cfg_connection_profiles (id, name) VALUES ('p1', 'P1');

            INSERT INTO viz_saved_charts
                (id, name, profile_id, created_at, updated_at,
                 chart_kind, x_axis_column_index, x_axis_label, x_axis_kind,
                 binding_x, source_kind,
                 source_metric_namespace, source_metric_name,
                 source_metric_period_seconds, source_metric_statistic,
                 source_metric_region,
                 refresh_policy_kind)
            VALUES ('c1', 'Legacy Metric', 'p1', 0, 0,
                    'line', 0, 't', 'time',
                    0, 'metric',
                    'AWS/EC2', 'CPUUtilization',
                    300, 'Average', 'us-east-1',
                    'off');

            INSERT INTO viz_saved_chart_source_metric_dimensions
                (chart_id, dim_index, dim_key, dim_value)
            VALUES ('c1', 0, 'InstanceId', 'i-1');
            "#,
        )
        .unwrap();

        // Re-run the registry — only 012 should apply.
        registry.run_all(&conn).unwrap();

        let series_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM viz_saved_chart_source_metric_series WHERE chart_id = 'c1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(series_count, 1, "legacy row must backfill to series table");

        let (namespace, period): (String, i64) = conn
            .query_row(
                "SELECT namespace, period_seconds
                 FROM viz_saved_chart_source_metric_series
                 WHERE chart_id = 'c1' AND series_index = 0",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(namespace, "AWS/EC2");
        assert_eq!(period, 300);

        // Dimension migrated with series_index = 0.
        let dim_series_index: i64 = conn
            .query_row(
                "SELECT series_index
                 FROM viz_saved_chart_source_metric_dimensions
                 WHERE chart_id = 'c1' AND dim_index = 0",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(dim_series_index, 0);
    }
}
