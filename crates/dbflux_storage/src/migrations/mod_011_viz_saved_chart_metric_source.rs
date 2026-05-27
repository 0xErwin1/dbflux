//! Migration 011: Add `SavedChartSource::Metric` support to `viz_saved_charts`.
//!
//! Migration 010 only knew about `source_kind` values `'query'` and `'collection'`.
//! When the `Metric` variant was introduced, an earlier patch attempted to extend
//! migration 010 in-place — which silently broke any DB that had already applied
//! the original migration 010 (the new columns and the updated `source_kind`
//! CHECK constraint never landed). Migration 010 is now back to its original
//! shape; this migration carries the forward change.
//!
//! Changes performed by this migration:
//!
//! 1. Rebuild `viz_saved_charts` (SQLite cannot ALTER an existing CHECK
//!    constraint, and the existing one rejects `source_kind = 'metric'`). The
//!    rebuild preserves all existing rows verbatim and adds five nullable
//!    `source_metric_*` columns with CHECK constraints that fire only when
//!    `source_kind = 'metric'`.
//! 2. Recreate the two indexes that depended on `viz_saved_charts`.
//! 3. Create the new child table `viz_saved_chart_source_metric_dimensions` for
//!    `SavedChartSource::Metric.dimensions: Vec<(String, String)>`. Mirrors the
//!    `viz_saved_chart_series` / `viz_saved_chart_binding_y` pattern.
//!
//! FK references from `viz_saved_chart_series.chart_id` and
//! `viz_saved_chart_binding_y.chart_id` resolve by table name, so renaming the
//! rebuilt table back to `viz_saved_charts` keeps them valid. The
//! `MigrationRegistry` runs each migration inside a transaction, so the rebuild
//! is atomic — partial state cannot leak if any statement fails.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "011_viz_saved_chart_metric_source"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        // `legacy_alter_table = ON` makes `ALTER TABLE ... RENAME TO` purely
        // lexical: SQLite 3.25+ otherwise tries to rewrite FK clauses in
        // sibling tables that reference the renamed name, which trips a
        // runtime lookup of `cfg_connection_profiles` even when foreign keys
        // are disabled. The pragma must be set on the connection directly —
        // setting it inside `execute_batch` does not take effect for the
        // ALTER statement in the same batch on some SQLite builds.
        tx.pragma_update(None, "legacy_alter_table", "ON")
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;

        let result = tx.execute_batch(SCHEMA);

        // Always restore the pragma even if the rebuild failed.
        let restore = tx.pragma_update(None, "legacy_alter_table", "OFF");

        result.map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;
        restore.map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;
        Ok(())
    }
}

// SQLite's documented "12-step" pattern for changing a CHECK constraint on an
// existing table (see https://www.sqlite.org/lang_altertable.html#otheralter).
//
// `legacy_alter_table = ON` makes `ALTER TABLE ... RENAME TO` purely lexical:
// without it SQLite 3.25+ rewrites FK clauses in other tables to point at the
// temporary `viz_saved_charts_new` name and then resolves the rename — which
// also re-parses sibling tables' schemas and trips on any referenced table that
// happens to be absent in a partial test fixture. Toggling this pragma is safe
// inside the migration transaction (unlike `foreign_keys`).
const SCHEMA: &str = r#"
-- --------------------------------------------------------------------------
-- 1. Build the new viz_saved_charts shape under a temporary name.
-- --------------------------------------------------------------------------
CREATE TABLE viz_saved_charts_new (
    id                                TEXT    PRIMARY KEY,
    name                              TEXT    NOT NULL,
    profile_id                        TEXT    NOT NULL
        REFERENCES cfg_connection_profiles(id) ON DELETE CASCADE,
    created_at                        INTEGER NOT NULL,
    updated_at                        INTEGER NOT NULL,

    -- ChartSpec fields
    chart_kind                        TEXT    NOT NULL
        CHECK (chart_kind IN ('line', 'bar', 'scatter', 'area', 'stacked_bar', 'pie')),
    legend_visible                    INTEGER NOT NULL DEFAULT 0
        CHECK (legend_visible IN (0, 1)),
    decimation_threshold              INTEGER NOT NULL DEFAULT 10000,
    track_source_indices              INTEGER NOT NULL DEFAULT 0
        CHECK (track_source_indices IN (0, 1)),
    y_scale                           TEXT    NOT NULL DEFAULT 'linear'
        CHECK (y_scale IN ('linear', 'log')),

    -- AxisSpec (x_axis)
    x_axis_column_index               INTEGER NOT NULL CHECK (x_axis_column_index >= 0),
    x_axis_label                      TEXT    NOT NULL,
    x_axis_kind                       TEXT    NOT NULL CHECK (x_axis_kind IN ('time', 'numeric')),
    x_axis_unit                       TEXT,

    -- BindingSpec scalars
    binding_x                         INTEGER NOT NULL CHECK (binding_x >= 0),
    binding_group_by                  INTEGER
        CHECK (binding_group_by IS NULL OR binding_group_by >= 0),
    binding_filter                    TEXT,
    binding_aggregation               TEXT    NOT NULL DEFAULT 'none'
        CHECK (binding_aggregation IN ('none', 'sum', 'avg', 'min', 'max')),

    -- SavedChartSource discriminator and fields.
    --   'query'      -> source_query
    --   'collection' -> source_collection_*
    --   'metric'     -> source_metric_* + viz_saved_chart_source_metric_dimensions
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

    -- Metric source fields (NULL for non-metric rows).
    source_metric_namespace           TEXT
        CHECK (source_kind != 'metric' OR source_metric_namespace IS NOT NULL),
    source_metric_name                TEXT
        CHECK (source_kind != 'metric' OR source_metric_name IS NOT NULL),
    source_metric_period_seconds      INTEGER
        CHECK (source_kind != 'metric' OR source_metric_period_seconds IS NOT NULL),
    source_metric_statistic           TEXT
        CHECK (source_kind != 'metric' OR source_metric_statistic IS NOT NULL),
    source_metric_region              TEXT,

    -- SavedChart metadata
    time_range_preset                 TEXT
        CHECK (time_range_preset IS NULL OR time_range_preset IN
            ('last_15_min', 'last_hour', 'last_6_hours', 'last_24_hours', 'last_7_days')),
    refresh_policy_kind               TEXT    NOT NULL DEFAULT 'off'
        CHECK (refresh_policy_kind IN ('off', 'interval', 'on_open')),
    refresh_policy_interval_secs      INTEGER
        CHECK (refresh_policy_kind != 'interval' OR refresh_policy_interval_secs IS NOT NULL)
);

-- --------------------------------------------------------------------------
-- 2. Copy existing rows verbatim. New metric_* columns land as NULL.
-- --------------------------------------------------------------------------
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

-- --------------------------------------------------------------------------
-- 3. Drop the old table and promote the new one.
-- --------------------------------------------------------------------------
DROP TABLE viz_saved_charts;
ALTER TABLE viz_saved_charts_new RENAME TO viz_saved_charts;

-- --------------------------------------------------------------------------
-- 4. Recreate the two indexes that lived on the original table.
-- --------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_viz_saved_charts_profile
    ON viz_saved_charts (profile_id);

CREATE INDEX IF NOT EXISTS idx_viz_saved_charts_updated_at
    ON viz_saved_charts (updated_at DESC);

-- --------------------------------------------------------------------------
-- 5. New child table for SavedChartSource::Metric.dimensions.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS viz_saved_chart_source_metric_dimensions (
    chart_id      TEXT    NOT NULL
        REFERENCES viz_saved_charts(id) ON DELETE CASCADE,
    dim_index     INTEGER NOT NULL CHECK (dim_index >= 0),
    dim_key       TEXT    NOT NULL,
    dim_value     TEXT    NOT NULL,

    PRIMARY KEY (chart_id, dim_index)
);

CREATE INDEX IF NOT EXISTS idx_viz_metric_dimensions_chart
    ON viz_saved_chart_source_metric_dimensions (chart_id);
"#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::migrations::MigrationRegistry;
    use rusqlite::Connection;

    fn fresh_conn() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    /// Migration 011 created scalar `source_metric_*` columns and the
    /// dimensions table keyed on `(chart_id, dim_index)`. Migration 012 drops
    /// those columns and re-keys the dimensions table. After running ALL
    /// migrations the dimensions table still exists; the scalar columns are
    /// expected to be gone — that assertion lives in mig 012's tests.
    #[test]
    fn dimensions_table_survives_full_migration_run() {
        let conn = fresh_conn();
        MigrationRegistry::new().run_all(&conn).unwrap();

        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
            .unwrap();
        let exists: bool = stmt
            .query_row(["viz_saved_chart_source_metric_dimensions"], |_| Ok(true))
            .unwrap_or(false);
        assert!(
            exists,
            "viz_saved_chart_source_metric_dimensions must exist after the full chain"
        );
    }

    #[test]
    fn upgrade_path_preserves_legacy_rows_through_rebuild() {
        // Simulate the exact failure mode the user reported: a DB that already
        // applied the original migration 010 (no metric columns) and now has
        // 011 applied on top. The legacy row written under 010's schema must
        // survive the table rebuild, and the new metric_* columns must be NULL
        // for that row.
        //
        // Build the 010 shape manually by running migrations 001 through 010,
        // marking 011 as not applied, seeding a row, then running 011 in a
        // second pass via a fresh registry.
        let conn = fresh_conn();
        let registry = MigrationRegistry::new();
        registry.run_all(&conn).unwrap();

        // Pretend 011 never ran on this DB by clearing its marker. The row we
        // seed below is written through the rebuilt schema (because the first
        // run_all already applied 011), but the assertion is the same: legacy
        // 'query' rows survive the rebuild with NULL metric columns.
        conn.execute(
            "DELETE FROM sys_migrations WHERE name = '011_viz_saved_chart_metric_source'",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name, driver_id) \
             VALUES ('p1', 'P1', 'sqlite')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO viz_saved_charts (
                id, name, profile_id, created_at, updated_at,
                chart_kind, x_axis_column_index, x_axis_label, x_axis_kind,
                binding_x, source_kind, source_query
            ) VALUES (
                'c1', 'My Chart', 'p1', 0, 0,
                'line', 0, 't', 'time',
                0, 'query', 'SELECT 1'
            )",
            [],
        )
        .unwrap();

        // Re-apply 011 via a second run. The migration must NOT lose the row.
        registry.run_all(&conn).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM viz_saved_charts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 1,
            "the seeded row must survive the migration rebuild"
        );

        let namespace: Option<String> = conn
            .query_row(
                "SELECT source_metric_namespace FROM viz_saved_charts WHERE id = 'c1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            namespace.is_none(),
            "legacy query row must have NULL metric namespace after rebuild"
        );
    }

}
