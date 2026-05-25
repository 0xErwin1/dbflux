//! Migration 010: Visualization domain tables for saved charts and dashboards.
//!
//! Creates six tables under the `viz_*` prefix:
//!
//! - `viz_saved_charts` — one row per saved chart (parent of series and binding_y)
//! - `viz_saved_chart_series` — child table for `ChartSpec.series: Vec<SeriesSpec>`
//! - `viz_saved_chart_binding_y` — child table for `BindingSpec.y: Vec<usize>`
//! - `viz_saved_chart_source_metric_dimensions` — child table for `SavedChartSource::Metric.dimensions`
//! - `viz_dashboards` — one row per dashboard
//! - `viz_dashboard_panels` — child table for dashboard panel slots
//!
//! All tables use native columns only. The only free-form TEXT field is
//! `binding_filter` in `viz_saved_charts`, which stores a user-typed expression
//! with no structured shape. No JSON columns exist in this schema.
//!
//! FK policy:
//! - `viz_saved_charts.profile_id → cfg_connection_profiles.id` ON DELETE CASCADE
//! - `viz_saved_chart_series.chart_id → viz_saved_charts.id` ON DELETE CASCADE
//! - `viz_saved_chart_binding_y.chart_id → viz_saved_charts.id` ON DELETE CASCADE
//! - `viz_saved_chart_source_metric_dimensions.chart_id → viz_saved_charts.id` ON DELETE CASCADE
//! - `viz_dashboard_panels.dashboard_id → viz_dashboards.id` ON DELETE CASCADE
//! - `viz_dashboards.profile_id → cfg_connection_profiles.id` ON DELETE SET NULL
//! - `viz_dashboard_panels.saved_chart_id` is a soft reference (NO FK) so deleting
//!   a SavedChart does not cascade to panels; they become orphans instead.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "010_viz_charts_and_dashboards"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        tx.execute_batch(SCHEMA)
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;
        Ok(())
    }
}

const SCHEMA: &str = r#"
-- ============================================================================
-- VIZ DOMAIN (viz_*) - Saved charts and dashboards
-- ============================================================================

-- --------------------------------------------------------------------------
-- viz_saved_charts — one row per saved chart
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS viz_saved_charts (
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

    -- SavedChartSource discriminator and fields
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

    -- Metric source fields (NULL for non-metric rows)
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

CREATE INDEX IF NOT EXISTS idx_viz_saved_charts_profile
    ON viz_saved_charts (profile_id);

CREATE INDEX IF NOT EXISTS idx_viz_saved_charts_updated_at
    ON viz_saved_charts (updated_at DESC);

-- --------------------------------------------------------------------------
-- viz_saved_chart_series — child table for ChartSpec.series: Vec<SeriesSpec>
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS viz_saved_chart_series (
    chart_id      TEXT    NOT NULL
        REFERENCES viz_saved_charts(id) ON DELETE CASCADE,
    series_index  INTEGER NOT NULL CHECK (series_index >= 0),
    column_index  INTEGER NOT NULL CHECK (column_index >= 0),
    label         TEXT    NOT NULL,
    color_slot    INTEGER NOT NULL CHECK (color_slot BETWEEN 0 AND 255),

    PRIMARY KEY (chart_id, series_index)
);

-- --------------------------------------------------------------------------
-- viz_saved_chart_binding_y — child table for BindingSpec.y: Vec<usize>
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS viz_saved_chart_binding_y (
    chart_id      TEXT    NOT NULL
        REFERENCES viz_saved_charts(id) ON DELETE CASCADE,
    slot_index    INTEGER NOT NULL CHECK (slot_index >= 0),
    column_index  INTEGER NOT NULL CHECK (column_index >= 0),

    PRIMARY KEY (chart_id, slot_index)
);

-- --------------------------------------------------------------------------
-- viz_saved_chart_source_metric_dimensions
--   Child table for SavedChartSource::Metric.dimensions: Vec<(String, String)>
--   Mirrors the viz_saved_chart_series / viz_saved_chart_binding_y pattern.
--   Empty for non-metric charts; implicitly all rows deleted when chart deleted.
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

-- --------------------------------------------------------------------------
-- viz_dashboards — one row per dashboard
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS viz_dashboards (
    id                                    TEXT    PRIMARY KEY,
    name                                  TEXT    NOT NULL,
    description                           TEXT,
    profile_id                            TEXT
        REFERENCES cfg_connection_profiles(id) ON DELETE SET NULL,
    shared_time_range_preset              TEXT
        CHECK (shared_time_range_preset IS NULL OR shared_time_range_preset IN
            ('last_15_min', 'last_hour', 'last_6_hours', 'last_24_hours', 'last_7_days')),
    shared_refresh_policy_kind            TEXT    NOT NULL DEFAULT 'off'
        CHECK (shared_refresh_policy_kind IN ('off', 'interval', 'on_open')),
    shared_refresh_policy_interval_secs   INTEGER
        CHECK (shared_refresh_policy_kind != 'interval' OR shared_refresh_policy_interval_secs IS NOT NULL),
    grid_columns                          INTEGER NOT NULL DEFAULT 2
        CHECK (grid_columns BETWEEN 1 AND 12),
    created_at                            INTEGER NOT NULL,
    updated_at                            INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_viz_dashboards_profile
    ON viz_dashboards (profile_id);

-- --------------------------------------------------------------------------
-- viz_dashboard_panels — child table for dashboard panel slots
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS viz_dashboard_panels (
    dashboard_id    TEXT    NOT NULL
        REFERENCES viz_dashboards(id) ON DELETE CASCADE,
    panel_index     INTEGER NOT NULL CHECK (panel_index >= 0),
    -- Soft reference: no FK so deleting a SavedChart makes this panel an orphan
    -- rather than cascading the delete. The UI renders a broken-placeholder element.
    saved_chart_id  TEXT    NOT NULL,
    title_override  TEXT,
    grid_row        INTEGER NOT NULL CHECK (grid_row >= 0),
    grid_column     INTEGER NOT NULL CHECK (grid_column >= 0),
    grid_width      INTEGER NOT NULL CHECK (grid_width >= 1),
    grid_height     INTEGER NOT NULL CHECK (grid_height >= 1),

    PRIMARY KEY (dashboard_id, panel_index)
);

CREATE INDEX IF NOT EXISTS idx_viz_dashboard_panels_saved_chart
    ON viz_dashboard_panels (saved_chart_id);
"#;
