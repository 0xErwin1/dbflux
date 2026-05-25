//! Repository for `viz_saved_charts` — aggregate root for saved chart persistence.
//!
//! This repository coordinates writes across three tables:
//! - `viz_saved_charts` (parent row)
//! - `viz_saved_chart_series` (via `SavedChartSeriesRepository`)
//! - `viz_saved_chart_binding_y` (via `SavedChartBindingYRepository`)
//!
//! All multi-table operations run inside a single SQLite transaction. The
//! `SavedChartDto` struct carries `series` and `binding_y` vecs that are
//! populated on read by assembling child rows in Rust — there are no JOIN
//! queries that would produce duplicate parent columns per child row.
//!
//! ## Query pattern for `list` / `list_by_profile`
//!
//! Three separate SELECTs are issued (one for parent rows, one for all series,
//! one for all binding_y) and assembled in Rust. This avoids a `3 * N`-query
//! pattern when loading dashboards.
//!
//! ## `list_full_for_dashboard`
//!
//! Fetches all `saved_chart_id` values for the given dashboard from
//! `viz_dashboard_panels`, then issues exactly three keyed IN-list SELECTs
//! (parent rows, series, binding_y) regardless of panel count.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use uuid::Uuid;

use crate::error::StorageError;
use crate::repositories::viz_saved_chart_binding_y::BindingYDto;
use crate::repositories::viz_saved_chart_series::SeriesDto;
use crate::repositories::viz_saved_chart_source_metric_dimensions::MetricDimensionDto;

const DB_PATH: &str = "dbflux.db";

/// Data transfer object for a saved chart plus its associated child rows.
///
/// `series` and `binding_y` are populated on read; they are not columns on
/// `viz_saved_charts` itself.
#[derive(Debug, Clone, PartialEq)]
pub struct SavedChartDto {
    // Primary key
    pub id: String,
    pub name: String,
    pub profile_id: String,
    pub created_at: i64,
    pub updated_at: i64,

    // ChartSpec
    pub chart_kind: String,
    pub legend_visible: i64,
    pub decimation_threshold: i64,
    pub track_source_indices: i64,
    pub y_scale: String,

    // AxisSpec (x_axis)
    pub x_axis_column_index: i64,
    pub x_axis_label: String,
    pub x_axis_kind: String,
    pub x_axis_unit: Option<String>,

    // BindingSpec scalars
    pub binding_x: i64,
    pub binding_group_by: Option<i64>,
    pub binding_filter: Option<String>,
    pub binding_aggregation: String,

    // SavedChartSource
    pub source_kind: String,
    pub source_query: Option<String>,
    pub source_collection_database: Option<String>,
    pub source_collection_name: Option<String>,
    pub source_time_window_start_ms: Option<i64>,
    pub source_time_window_end_ms: Option<i64>,
    pub source_time_window_language: Option<String>,

    // Metric source fields (NULL for non-metric rows)
    pub source_metric_namespace: Option<String>,
    pub source_metric_name: Option<String>,
    pub source_metric_period_seconds: Option<i64>,
    pub source_metric_statistic: Option<String>,
    pub source_metric_region: Option<String>,

    // SavedChart metadata
    pub time_range_preset: Option<String>,
    pub refresh_policy_kind: String,
    pub refresh_policy_interval_secs: Option<i64>,

    // Assembled child rows (not columns on the parent table).
    pub series: Vec<SeriesDto>,
    pub binding_y: Vec<BindingYDto>,
    /// Ordered dimension pairs for `SavedChartSource::Metric`. Empty for other kinds.
    pub metric_dimensions: Vec<MetricDimensionDto>,
}

/// Repository for `viz_saved_charts`.
#[derive(Clone)]
pub struct SavedChartsRepository {
    conn: Arc<Mutex<Connection>>,
}

impl SavedChartsRepository {
    /// Creates a new repository wrapping the given shared connection.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Lists all saved charts with their series and binding_y rows.
    ///
    /// Issues three keyed SELECTs (parents, series, binding_y) and assembles
    /// in Rust. Ordered by `updated_at DESC`.
    pub fn list(&self) -> Result<Vec<SavedChartDto>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        let charts = query_parent_rows(&conn, "ORDER BY updated_at DESC", [])?;
        drop(conn);

        self.attach_children(charts)
    }

    /// Lists all saved charts for a given profile with children attached.
    pub fn list_by_profile(&self, profile_id: Uuid) -> Result<Vec<SavedChartDto>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        let mut stmt = conn
            .prepare(
                "SELECT id, name, profile_id, created_at, updated_at,
                        chart_kind, legend_visible, decimation_threshold,
                        track_source_indices, y_scale,
                        x_axis_column_index, x_axis_label, x_axis_kind, x_axis_unit,
                        binding_x, binding_group_by, binding_filter, binding_aggregation,
                        source_kind, source_query,
                        source_collection_database, source_collection_name,
                        source_time_window_start_ms, source_time_window_end_ms,
                        source_time_window_language,
                        source_metric_namespace, source_metric_name,
                        source_metric_period_seconds, source_metric_statistic,
                        source_metric_region,
                        time_range_preset, refresh_policy_kind, refresh_policy_interval_secs
                 FROM viz_saved_charts
                 WHERE profile_id = ?1
                 ORDER BY updated_at DESC",
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        let charts: Vec<SavedChartDto> = stmt
            .query_map([profile_id.to_string()], map_parent_row)
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .collect();

        drop(stmt);
        drop(conn);

        self.attach_children(charts)
    }

    /// Fetches a single chart with all its children, or `None` if not found.
    pub fn get_full_chart(&self, id: Uuid) -> Result<Option<SavedChartDto>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut stmt = conn
            .prepare(
                "SELECT id, name, profile_id, created_at, updated_at,
                        chart_kind, legend_visible, decimation_threshold,
                        track_source_indices, y_scale,
                        x_axis_column_index, x_axis_label, x_axis_kind, x_axis_unit,
                        binding_x, binding_group_by, binding_filter, binding_aggregation,
                        source_kind, source_query,
                        source_collection_database, source_collection_name,
                        source_time_window_start_ms, source_time_window_end_ms,
                        source_time_window_language,
                        source_metric_namespace, source_metric_name,
                        source_metric_period_seconds, source_metric_statistic,
                        source_metric_region,
                        time_range_preset, refresh_policy_kind, refresh_policy_interval_secs
                 FROM viz_saved_charts
                 WHERE id = ?1",
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        let charts: Vec<SavedChartDto> = stmt
            .query_map([id.to_string()], map_parent_row)
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .collect();

        drop(stmt);
        drop(conn);

        if charts.is_empty() {
            return Ok(None);
        }

        let mut charts = self.attach_children(charts)?;
        Ok(charts.pop())
    }

    /// Loads all saved charts referenced by panels in the given dashboard.
    ///
    /// Issues exactly three keyed IN-list SELECTs regardless of panel count:
    /// one for parent rows, one for series, one for binding_y.
    pub fn list_full_for_dashboard(
        &self,
        dashboard_id: Uuid,
    ) -> Result<Vec<SavedChartDto>, StorageError> {
        // Fetch the saved_chart_id values from the panels table.
        let chart_ids: Vec<String> = {
            let conn = self.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare(
                    "SELECT DISTINCT saved_chart_id
                     FROM viz_dashboard_panels
                     WHERE dashboard_id = ?1",
                )
                .map_err(|source| StorageError::Sqlite {
                    path: DB_PATH.into(),
                    source,
                })?;

            stmt.query_map([dashboard_id.to_string()], |row| row.get(0))
                .map_err(|source| StorageError::Sqlite {
                    path: DB_PATH.into(),
                    source,
                })?
                .filter_map(|r| r.ok())
                .collect()
        };

        if chart_ids.is_empty() {
            return Ok(vec![]);
        }

        // Build an IN-list placeholder string like "?1, ?2, ?3".
        let placeholders: String = (1..=chart_ids.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");

        // SELECT 1: parent rows
        let charts: Vec<SavedChartDto> = {
            let conn = self.conn.lock().map_err(lock_err)?;
            let sql = format!(
                "SELECT id, name, profile_id, created_at, updated_at,
                        chart_kind, legend_visible, decimation_threshold,
                        track_source_indices, y_scale,
                        x_axis_column_index, x_axis_label, x_axis_kind, x_axis_unit,
                        binding_x, binding_group_by, binding_filter, binding_aggregation,
                        source_kind, source_query,
                        source_collection_database, source_collection_name,
                        source_time_window_start_ms, source_time_window_end_ms,
                        source_time_window_language,
                        source_metric_namespace, source_metric_name,
                        source_metric_period_seconds, source_metric_statistic,
                        source_metric_region,
                        time_range_preset, refresh_policy_kind, refresh_policy_interval_secs
                 FROM viz_saved_charts
                 WHERE id IN ({placeholders})"
            );

            let mut stmt = conn.prepare(&sql).map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

            stmt.query_map(rusqlite::params_from_iter(chart_ids.iter()), map_parent_row)
                .map_err(|source| StorageError::Sqlite {
                    path: DB_PATH.into(),
                    source,
                })?
                .filter_map(|r| r.ok())
                .collect()
        };

        // SELECT 2 + SELECT 3: children for all charts in the IN list.
        // attach_children issues exactly these two SELECTs.
        self.attach_children(charts)
    }

    /// Inserts or replaces a chart and its children in a single atomic transaction.
    ///
    /// All three tables (`viz_saved_charts`, `viz_saved_chart_series`,
    /// `viz_saved_chart_binding_y`) are written within one transaction. On any
    /// failure the entire operation is rolled back and the previous state is
    /// preserved.
    pub fn upsert(&self, chart: &SavedChartDto) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        let now_ms = now_millis();

        let tx = conn
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;

        // 1. Write / replace the parent row.
        tx.execute(
            "INSERT OR REPLACE INTO viz_saved_charts
                 (id, name, profile_id, created_at, updated_at,
                  chart_kind, legend_visible, decimation_threshold,
                  track_source_indices, y_scale,
                  x_axis_column_index, x_axis_label, x_axis_kind, x_axis_unit,
                  binding_x, binding_group_by, binding_filter, binding_aggregation,
                  source_kind, source_query,
                  source_collection_database, source_collection_name,
                  source_time_window_start_ms, source_time_window_end_ms,
                  source_time_window_language,
                  source_metric_namespace, source_metric_name,
                  source_metric_period_seconds, source_metric_statistic,
                  source_metric_region,
                  time_range_preset, refresh_policy_kind, refresh_policy_interval_secs)
             VALUES
                 (?1, ?2, ?3, ?4, ?5,
                  ?6, ?7, ?8, ?9, ?10,
                  ?11, ?12, ?13, ?14,
                  ?15, ?16, ?17, ?18,
                  ?19, ?20,
                  ?21, ?22, ?23, ?24, ?25,
                  ?26, ?27, ?28, ?29, ?30,
                  ?31, ?32, ?33)",
            rusqlite::params![
                chart.id,
                chart.name,
                chart.profile_id,
                chart.created_at,
                now_ms,
                chart.chart_kind,
                chart.legend_visible,
                chart.decimation_threshold,
                chart.track_source_indices,
                chart.y_scale,
                chart.x_axis_column_index,
                chart.x_axis_label,
                chart.x_axis_kind,
                chart.x_axis_unit,
                chart.binding_x,
                chart.binding_group_by,
                chart.binding_filter,
                chart.binding_aggregation,
                chart.source_kind,
                chart.source_query,
                chart.source_collection_database,
                chart.source_collection_name,
                chart.source_time_window_start_ms,
                chart.source_time_window_end_ms,
                chart.source_time_window_language,
                chart.source_metric_namespace,
                chart.source_metric_name,
                chart.source_metric_period_seconds,
                chart.source_metric_statistic,
                chart.source_metric_region,
                chart.time_range_preset,
                chart.refresh_policy_kind,
                chart.refresh_policy_interval_secs,
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;

        // 2. Atomically replace series child rows within the same transaction.
        tx.execute(
            "DELETE FROM viz_saved_chart_series WHERE chart_id = ?1",
            [&chart.id],
        )
        .map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;

        for s in &chart.series {
            tx.execute(
                "INSERT INTO viz_saved_chart_series
                     (chart_id, series_index, column_index, label, color_slot)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    s.chart_id,
                    s.series_index,
                    s.column_index,
                    s.label,
                    s.color_slot,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;
        }

        // 3. Atomically replace binding_y child rows within the same transaction.
        tx.execute(
            "DELETE FROM viz_saved_chart_binding_y WHERE chart_id = ?1",
            [&chart.id],
        )
        .map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;

        for b in &chart.binding_y {
            tx.execute(
                "INSERT INTO viz_saved_chart_binding_y
                     (chart_id, slot_index, column_index)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![b.chart_id, b.slot_index, b.column_index],
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;
        }

        // 4. Atomically replace metric dimension child rows within the same transaction.
        tx.execute(
            "DELETE FROM viz_saved_chart_source_metric_dimensions WHERE chart_id = ?1",
            [&chart.id],
        )
        .map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;

        for d in &chart.metric_dimensions {
            tx.execute(
                "INSERT INTO viz_saved_chart_source_metric_dimensions
                     (chart_id, dim_index, dim_key, dim_value)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![d.chart_id, d.dim_index, d.dim_key, d.dim_value],
            )
            .map_err(|source| StorageError::Sqlite {
                path: DB_PATH.into(),
                source,
            })?;
        }

        tx.commit().map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;

        Ok(())
    }

    /// Deletes a chart by UUID. FK CASCADE removes series and binding_y rows.
    pub fn delete(&self, id: Uuid) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        conn.execute(
            "DELETE FROM viz_saved_charts WHERE id = ?1",
            [id.to_string()],
        )
        .map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?;
        Ok(())
    }

    // Attaches series and binding_y rows to a list of charts using two bulk SELECTs.
    fn attach_children(
        &self,
        mut charts: Vec<SavedChartDto>,
    ) -> Result<Vec<SavedChartDto>, StorageError> {
        if charts.is_empty() {
            return Ok(charts);
        }

        // Build a map from chart_id → index for efficient child assignment.
        let mut index_map: HashMap<String, usize> = HashMap::new();
        for (i, c) in charts.iter().enumerate() {
            index_map.insert(c.id.clone(), i);
        }

        // Fetch all series for the chart set.
        let chart_ids: Vec<String> = charts.iter().map(|c| c.id.clone()).collect();
        let placeholders: String = (1..=chart_ids.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");

        {
            let conn = self.conn.lock().map_err(lock_err)?;

            let series_sql = format!(
                "SELECT chart_id, series_index, column_index, label, color_slot
                 FROM viz_saved_chart_series
                 WHERE chart_id IN ({placeholders})
                 ORDER BY chart_id, series_index ASC"
            );
            let mut stmt = conn
                .prepare(&series_sql)
                .map_err(|source| StorageError::Sqlite {
                    path: DB_PATH.into(),
                    source,
                })?;

            let series_rows: Vec<SeriesDto> = stmt
                .query_map(rusqlite::params_from_iter(chart_ids.iter()), |row| {
                    Ok(SeriesDto {
                        chart_id: row.get(0)?,
                        series_index: row.get(1)?,
                        column_index: row.get(2)?,
                        label: row.get(3)?,
                        color_slot: row.get(4)?,
                    })
                })
                .map_err(|source| StorageError::Sqlite {
                    path: DB_PATH.into(),
                    source,
                })?
                .filter_map(|r| r.ok())
                .collect();

            for s in series_rows {
                if let Some(&idx) = index_map.get(&s.chart_id) {
                    charts[idx].series.push(s);
                }
            }

            let binding_y_sql = format!(
                "SELECT chart_id, slot_index, column_index
                 FROM viz_saved_chart_binding_y
                 WHERE chart_id IN ({placeholders})
                 ORDER BY chart_id, slot_index ASC"
            );
            let mut stmt = conn
                .prepare(&binding_y_sql)
                .map_err(|source| StorageError::Sqlite {
                    path: DB_PATH.into(),
                    source,
                })?;

            let binding_y_rows: Vec<BindingYDto> = stmt
                .query_map(rusqlite::params_from_iter(chart_ids.iter()), |row| {
                    Ok(BindingYDto {
                        chart_id: row.get(0)?,
                        slot_index: row.get(1)?,
                        column_index: row.get(2)?,
                    })
                })
                .map_err(|source| StorageError::Sqlite {
                    path: DB_PATH.into(),
                    source,
                })?
                .filter_map(|r| r.ok())
                .collect();

            for b in binding_y_rows {
                if let Some(&idx) = index_map.get(&b.chart_id) {
                    charts[idx].binding_y.push(b);
                }
            }

            // Fetch metric dimensions (empty for non-metric charts).
            let dim_sql = format!(
                "SELECT chart_id, dim_index, dim_key, dim_value
                 FROM viz_saved_chart_source_metric_dimensions
                 WHERE chart_id IN ({placeholders})
                 ORDER BY chart_id, dim_index ASC"
            );
            let mut stmt = conn
                .prepare(&dim_sql)
                .map_err(|source| StorageError::Sqlite {
                    path: DB_PATH.into(),
                    source,
                })?;

            let dim_rows: Vec<MetricDimensionDto> = stmt
                .query_map(rusqlite::params_from_iter(chart_ids.iter()), |row| {
                    Ok(MetricDimensionDto {
                        chart_id: row.get(0)?,
                        dim_index: row.get(1)?,
                        dim_key: row.get(2)?,
                        dim_value: row.get(3)?,
                    })
                })
                .map_err(|source| StorageError::Sqlite {
                    path: DB_PATH.into(),
                    source,
                })?
                .filter_map(|r| r.ok())
                .collect();

            for d in dim_rows {
                if let Some(&idx) = index_map.get(&d.chart_id) {
                    charts[idx].metric_dimensions.push(d);
                }
            }
        }

        Ok(charts)
    }
}

fn query_parent_rows<P: rusqlite::Params>(
    conn: &Connection,
    order: &str,
    params: P,
) -> Result<Vec<SavedChartDto>, StorageError> {
    let sql = format!(
        "SELECT id, name, profile_id, created_at, updated_at,
                chart_kind, legend_visible, decimation_threshold,
                track_source_indices, y_scale,
                x_axis_column_index, x_axis_label, x_axis_kind, x_axis_unit,
                binding_x, binding_group_by, binding_filter, binding_aggregation,
                source_kind, source_query,
                source_collection_database, source_collection_name,
                source_time_window_start_ms, source_time_window_end_ms,
                source_time_window_language,
                source_metric_namespace, source_metric_name,
                source_metric_period_seconds, source_metric_statistic,
                source_metric_region,
                time_range_preset, refresh_policy_kind, refresh_policy_interval_secs
         FROM viz_saved_charts
         {order}"
    );

    let mut stmt = conn.prepare(&sql).map_err(|source| StorageError::Sqlite {
        path: DB_PATH.into(),
        source,
    })?;

    let rows = stmt
        .query_map(params, map_parent_row)
        .map_err(|source| StorageError::Sqlite {
            path: DB_PATH.into(),
            source,
        })?
        .filter_map(|r| r.ok())
        .collect();

    Ok(rows)
}

fn map_parent_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SavedChartDto> {
    Ok(SavedChartDto {
        id: row.get(0)?,
        name: row.get(1)?,
        profile_id: row.get(2)?,
        created_at: row.get(3)?,
        updated_at: row.get(4)?,
        chart_kind: row.get(5)?,
        legend_visible: row.get(6)?,
        decimation_threshold: row.get(7)?,
        track_source_indices: row.get(8)?,
        y_scale: row.get(9)?,
        x_axis_column_index: row.get(10)?,
        x_axis_label: row.get(11)?,
        x_axis_kind: row.get(12)?,
        x_axis_unit: row.get(13)?,
        binding_x: row.get(14)?,
        binding_group_by: row.get(15)?,
        binding_filter: row.get(16)?,
        binding_aggregation: row.get(17)?,
        source_kind: row.get(18)?,
        source_query: row.get(19)?,
        source_collection_database: row.get(20)?,
        source_collection_name: row.get(21)?,
        source_time_window_start_ms: row.get(22)?,
        source_time_window_end_ms: row.get(23)?,
        source_time_window_language: row.get(24)?,
        source_metric_namespace: row.get(25)?,
        source_metric_name: row.get(26)?,
        source_metric_period_seconds: row.get(27)?,
        source_metric_statistic: row.get(28)?,
        source_metric_region: row.get(29)?,
        time_range_preset: row.get(30)?,
        refresh_policy_kind: row.get(31)?,
        refresh_policy_interval_secs: row.get(32)?,
        series: vec![],
        binding_y: vec![],
        metric_dimensions: vec![],
    })
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn lock_err<T>(e: std::sync::PoisonError<T>) -> StorageError {
    StorageError::Sqlite {
        path: DB_PATH.into(),
        source: rusqlite::Error::InvalidParameterName(e.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Helper: build a minimal valid SavedChartDto for tests
// ---------------------------------------------------------------------------
#[cfg(test)]
fn make_chart(id: Uuid, profile_id: Uuid, source_kind: &str) -> SavedChartDto {
    let query = if source_kind == "query" {
        Some("SELECT 1".to_string())
    } else {
        None
    };
    let coll_db = if source_kind == "collection" {
        Some("mydb".to_string())
    } else {
        None
    };
    let coll_name = if source_kind == "collection" {
        Some("mycoll".to_string())
    } else {
        None
    };

    SavedChartDto {
        id: id.to_string(),
        name: "Test Chart".to_string(),
        profile_id: profile_id.to_string(),
        created_at: 1_000_000,
        updated_at: 1_000_000,
        chart_kind: "line".to_string(),
        legend_visible: 0,
        decimation_threshold: 10000,
        track_source_indices: 0,
        y_scale: "linear".to_string(),
        x_axis_column_index: 0,
        x_axis_label: "Time".to_string(),
        x_axis_kind: "time".to_string(),
        x_axis_unit: None,
        binding_x: 0,
        binding_group_by: None,
        binding_filter: None,
        binding_aggregation: "none".to_string(),
        source_kind: source_kind.to_string(),
        source_query: query,
        source_collection_database: coll_db,
        source_collection_name: coll_name,
        source_time_window_start_ms: None,
        source_time_window_end_ms: None,
        source_time_window_language: None,
        source_metric_namespace: None,
        source_metric_name: None,
        source_metric_period_seconds: None,
        source_metric_statistic: None,
        source_metric_region: None,
        time_range_preset: None,
        refresh_policy_kind: "off".to_string(),
        refresh_policy_interval_secs: None,
        series: vec![],
        binding_y: vec![],
        metric_dimensions: vec![],
    }
}

#[cfg(test)]
fn make_metric_chart(id: Uuid, profile_id: Uuid) -> SavedChartDto {
    use crate::repositories::viz_saved_chart_source_metric_dimensions::MetricDimensionDto;

    SavedChartDto {
        id: id.to_string(),
        name: "Metric Chart".to_string(),
        profile_id: profile_id.to_string(),
        created_at: 1_000_000,
        updated_at: 1_000_000,
        chart_kind: "line".to_string(),
        legend_visible: 0,
        decimation_threshold: 10000,
        track_source_indices: 0,
        y_scale: "linear".to_string(),
        x_axis_column_index: 0,
        x_axis_label: "Time".to_string(),
        x_axis_kind: "time".to_string(),
        x_axis_unit: None,
        binding_x: 0,
        binding_group_by: None,
        binding_filter: None,
        binding_aggregation: "none".to_string(),
        source_kind: "metric".to_string(),
        source_query: None,
        source_collection_database: None,
        source_collection_name: None,
        source_time_window_start_ms: None,
        source_time_window_end_ms: None,
        source_time_window_language: None,
        source_metric_namespace: Some("AWS/EC2".to_string()),
        source_metric_name: Some("CPUUtilization".to_string()),
        source_metric_period_seconds: Some(300),
        source_metric_statistic: Some("Average".to_string()),
        source_metric_region: Some("us-east-1".to_string()),
        time_range_preset: None,
        refresh_policy_kind: "off".to_string(),
        refresh_policy_interval_secs: None,
        series: vec![],
        binding_y: vec![],
        metric_dimensions: vec![
            MetricDimensionDto {
                chart_id: id.to_string(),
                dim_index: 0,
                dim_key: "InstanceId".to_string(),
                dim_value: "i-12345".to_string(),
            },
            MetricDimensionDto {
                chart_id: id.to_string(),
                dim_index: 1,
                dim_key: "Region".to_string(),
                dim_value: "us-east-1".to_string(),
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::MigrationRegistry;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_db(suffix: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dbflux_charts_{}_{}.db",
            suffix,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn setup(suffix: &str) -> (Arc<Mutex<Connection>>, SavedChartsRepository, Uuid) {
        let path = temp_db(suffix);
        let conn = open_database(&path).expect("open db");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = Uuid::new_v4();
        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, ?2)",
            rusqlite::params![profile_id.to_string(), "test-profile"],
        )
        .expect("insert profile");

        let conn = Arc::new(Mutex::new(conn));
        let repo = SavedChartsRepository::new(Arc::clone(&conn));

        (conn, repo, profile_id)
    }

    #[test]
    fn test_saved_chart_query_source_roundtrip() {
        let (_conn, repo, profile_id) = setup("query_roundtrip");
        let id = Uuid::new_v4();

        let mut dto = make_chart(id, profile_id, "query");
        dto.x_axis_kind = "numeric".to_string();
        dto.y_scale = "log".to_string();
        dto.legend_visible = 1;
        dto.binding_aggregation = "sum".to_string();
        dto.refresh_policy_kind = "interval".to_string();
        dto.refresh_policy_interval_secs = Some(30);
        dto.time_range_preset = Some("last_hour".to_string());
        dto.series = vec![SeriesDto {
            chart_id: id.to_string(),
            series_index: 0,
            column_index: 1,
            label: "Metric".to_string(),
            color_slot: 5,
        }];
        dto.binding_y = vec![BindingYDto {
            chart_id: id.to_string(),
            slot_index: 0,
            column_index: 1,
        }];

        repo.upsert(&dto).expect("upsert");

        let loaded = repo.get_full_chart(id).expect("get").expect("should exist");
        assert_eq!(loaded.id, dto.id);
        assert_eq!(loaded.name, dto.name);
        assert_eq!(loaded.profile_id, dto.profile_id);
        assert_eq!(loaded.chart_kind, dto.chart_kind);
        assert_eq!(loaded.y_scale, dto.y_scale);
        assert_eq!(loaded.legend_visible, dto.legend_visible);
        assert_eq!(loaded.x_axis_kind, dto.x_axis_kind);
        assert_eq!(loaded.binding_aggregation, dto.binding_aggregation);
        assert_eq!(loaded.refresh_policy_kind, dto.refresh_policy_kind);
        assert_eq!(
            loaded.refresh_policy_interval_secs,
            dto.refresh_policy_interval_secs
        );
        assert_eq!(loaded.time_range_preset, dto.time_range_preset);
        assert_eq!(loaded.source_kind, "query");
        assert_eq!(loaded.source_query, dto.source_query);
        assert_eq!(loaded.series.len(), 1);
        assert_eq!(loaded.series[0].label, "Metric");
        assert_eq!(loaded.binding_y.len(), 1);
        assert_eq!(loaded.binding_y[0].column_index, 1);
    }

    #[test]
    fn test_saved_chart_collection_source_roundtrip() {
        let (_conn, repo, profile_id) = setup("collection_roundtrip");
        let id = Uuid::new_v4();

        let mut dto = make_chart(id, profile_id, "collection");
        dto.source_time_window_start_ms = Some(1_700_000_000_000);
        dto.source_time_window_end_ms = Some(1_700_003_600_000);
        dto.source_time_window_language = Some("flux".to_string());

        repo.upsert(&dto).expect("upsert");

        let loaded = repo.get_full_chart(id).expect("get").expect("should exist");
        assert_eq!(loaded.source_kind, "collection");
        assert_eq!(
            loaded.source_collection_database,
            dto.source_collection_database
        );
        assert_eq!(loaded.source_collection_name, dto.source_collection_name);
        assert_eq!(
            loaded.source_time_window_start_ms,
            dto.source_time_window_start_ms
        );
        assert_eq!(
            loaded.source_time_window_end_ms,
            dto.source_time_window_end_ms
        );
        assert_eq!(
            loaded.source_time_window_language,
            dto.source_time_window_language
        );
    }

    #[test]
    fn test_series_and_binding_y_order_preserved() {
        let (_conn, repo, profile_id) = setup("order_preserved");
        let id = Uuid::new_v4();

        let mut dto = make_chart(id, profile_id, "query");
        dto.series = vec![
            SeriesDto {
                chart_id: id.to_string(),
                series_index: 0,
                column_index: 1,
                label: "A".to_string(),
                color_slot: 0,
            },
            SeriesDto {
                chart_id: id.to_string(),
                series_index: 1,
                column_index: 2,
                label: "B".to_string(),
                color_slot: 1,
            },
            SeriesDto {
                chart_id: id.to_string(),
                series_index: 2,
                column_index: 3,
                label: "C".to_string(),
                color_slot: 2,
            },
        ];
        dto.binding_y = vec![
            BindingYDto {
                chart_id: id.to_string(),
                slot_index: 0,
                column_index: 10,
            },
            BindingYDto {
                chart_id: id.to_string(),
                slot_index: 1,
                column_index: 20,
            },
        ];

        repo.upsert(&dto).expect("upsert");

        let loaded = repo.get_full_chart(id).expect("get").expect("exists");
        assert_eq!(loaded.series.len(), 3);
        assert_eq!(loaded.series[0].series_index, 0);
        assert_eq!(loaded.series[1].series_index, 1);
        assert_eq!(loaded.series[2].series_index, 2);
        assert_eq!(loaded.binding_y.len(), 2);
        assert_eq!(loaded.binding_y[0].slot_index, 0);
        assert_eq!(loaded.binding_y[1].slot_index, 1);
    }

    #[test]
    fn test_upsert_atomicity_on_series_violation() {
        let (_conn, repo, profile_id) = setup("upsert_atomicity");
        let id = Uuid::new_v4();

        let mut dto = make_chart(id, profile_id, "query");
        dto.series = vec![SeriesDto {
            chart_id: id.to_string(),
            series_index: 0,
            column_index: 1,
            label: "Initial".to_string(),
            color_slot: 0,
        }];
        repo.upsert(&dto).expect("initial upsert");

        // Attempt an upsert with a color_slot that violates CHECK (BETWEEN 0 AND 255).
        let mut bad = dto.clone();
        bad.series = vec![SeriesDto {
            chart_id: id.to_string(),
            series_index: 0,
            column_index: 1,
            label: "Bad".to_string(),
            color_slot: 300,
        }];
        let result = repo.upsert(&bad);
        assert!(result.is_err(), "should fail on CHECK violation");

        // Original series must be intact.
        let loaded = repo.get_full_chart(id).expect("get").expect("exists");
        assert_eq!(loaded.series.len(), 1);
        assert_eq!(loaded.series[0].label, "Initial");
    }

    #[test]
    fn test_delete_cascades_series_and_binding_y() {
        let (conn, repo, profile_id) = setup("delete_cascades");
        let id = Uuid::new_v4();

        let mut dto = make_chart(id, profile_id, "query");
        dto.series = vec![
            SeriesDto {
                chart_id: id.to_string(),
                series_index: 0,
                column_index: 1,
                label: "A".to_string(),
                color_slot: 0,
            },
            SeriesDto {
                chart_id: id.to_string(),
                series_index: 1,
                column_index: 2,
                label: "B".to_string(),
                color_slot: 1,
            },
        ];
        dto.binding_y = vec![
            BindingYDto {
                chart_id: id.to_string(),
                slot_index: 0,
                column_index: 5,
            },
            BindingYDto {
                chart_id: id.to_string(),
                slot_index: 1,
                column_index: 6,
            },
        ];
        repo.upsert(&dto).expect("upsert");

        repo.delete(id).expect("delete");

        let locked = conn.lock().unwrap();
        let series_count: i64 = locked
            .query_row(
                "SELECT COUNT(*) FROM viz_saved_chart_series WHERE chart_id = ?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .expect("count series");
        let binding_count: i64 = locked
            .query_row(
                "SELECT COUNT(*) FROM viz_saved_chart_binding_y WHERE chart_id = ?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .expect("count binding_y");

        assert_eq!(series_count, 0, "series must cascade on chart delete");
        assert_eq!(binding_count, 0, "binding_y must cascade on chart delete");
    }

    #[test]
    fn test_delete_does_not_cascade_to_panels() {
        use crate::repositories::viz_dashboard_panels::{
            DashboardPanelDto, DashboardPanelsRepository,
        };

        let (conn, repo, profile_id) = setup("no_cascade_panels");
        let id = Uuid::new_v4();
        let dashboard_id = Uuid::new_v4();

        // Insert a dashboard.
        {
            let locked = conn.lock().unwrap();
            locked
                .execute(
                    "INSERT INTO viz_dashboards
                     (id, name, profile_id, shared_refresh_policy_kind, grid_columns, created_at, updated_at)
                     VALUES (?1, 'D', ?2, 'off', 2, 0, 0)",
                    rusqlite::params![dashboard_id.to_string(), profile_id.to_string()],
                )
                .expect("insert dashboard");
        }

        let panels_repo = DashboardPanelsRepository::new(Arc::clone(&conn));

        let dto = make_chart(id, profile_id, "query");
        repo.upsert(&dto).expect("upsert");

        panels_repo
            .replace_panels_for_dashboard(
                dashboard_id,
                &[DashboardPanelDto {
                    dashboard_id: dashboard_id.to_string(),
                    panel_index: 0,
                    saved_chart_id: id.to_string(),
                    title_override: None,
                    grid_row: 0,
                    grid_column: 0,
                    grid_width: 1,
                    grid_height: 1,
                }],
            )
            .expect("insert panel");

        repo.delete(id).expect("delete chart");

        let panels = panels_repo.list_for_dashboard(dashboard_id).expect("list");
        assert_eq!(
            panels.len(),
            1,
            "panel must survive chart deletion (soft ref)"
        );

        let orphans = panels_repo.count_orphans().expect("count");
        assert_eq!(orphans, 1, "panel is now an orphan");
    }

    #[test]
    fn test_profile_cascade_deletes_chart() {
        let (conn, repo, profile_id) = setup("profile_cascade");
        let id = Uuid::new_v4();

        let dto = make_chart(id, profile_id, "query");
        repo.upsert(&dto).expect("upsert");

        // Delete the profile.
        {
            let locked = conn.lock().unwrap();
            locked
                .execute(
                    "DELETE FROM cfg_connection_profiles WHERE id = ?1",
                    [profile_id.to_string()],
                )
                .expect("delete profile");
        }

        let loaded = repo.get_full_chart(id).expect("get");
        assert!(
            loaded.is_none(),
            "chart must be cascade-deleted when its profile is deleted"
        );
    }

    #[test]
    fn test_check_enum_violation() {
        let (conn, _repo, profile_id) = setup("check_enum");
        let locked = conn.lock().unwrap();
        let result = locked.execute(
            "INSERT INTO viz_saved_charts
             (id, name, profile_id, created_at, updated_at,
              chart_kind, legend_visible, decimation_threshold, track_source_indices,
              y_scale, x_axis_column_index, x_axis_label, x_axis_kind,
              binding_x, binding_aggregation, source_kind, source_query,
              refresh_policy_kind)
             VALUES (?1, 'Test', ?2, 0, 0,
                     'invalid_kind', 0, 10000, 0,
                     'linear', 0, 'X', 'time',
                     0, 'none', 'query', 'SELECT 1',
                     'off')",
            rusqlite::params![Uuid::new_v4().to_string(), profile_id.to_string()],
        );
        assert!(
            result.is_err(),
            "invalid chart_kind should violate CHECK constraint"
        );
    }

    #[test]
    fn test_check_source_kind_query_without_query() {
        let (conn, _repo, profile_id) = setup("check_query_null");
        let locked = conn.lock().unwrap();
        let result = locked.execute(
            "INSERT INTO viz_saved_charts
             (id, name, profile_id, created_at, updated_at,
              chart_kind, legend_visible, decimation_threshold, track_source_indices,
              y_scale, x_axis_column_index, x_axis_label, x_axis_kind,
              binding_x, binding_aggregation, source_kind, source_query,
              refresh_policy_kind)
             VALUES (?1, 'Test', ?2, 0, 0,
                     'line', 0, 10000, 0,
                     'linear', 0, 'X', 'time',
                     0, 'none', 'query', NULL,
                     'off')",
            rusqlite::params![Uuid::new_v4().to_string(), profile_id.to_string()],
        );
        assert!(
            result.is_err(),
            "source_kind='query' with NULL source_query should violate CHECK"
        );
    }

    #[test]
    fn test_check_source_kind_collection_without_database() {
        let (conn, _repo, profile_id) = setup("check_collection_null");
        let locked = conn.lock().unwrap();
        let result = locked.execute(
            "INSERT INTO viz_saved_charts
             (id, name, profile_id, created_at, updated_at,
              chart_kind, legend_visible, decimation_threshold, track_source_indices,
              y_scale, x_axis_column_index, x_axis_label, x_axis_kind,
              binding_x, binding_aggregation,
              source_kind, source_collection_database, source_collection_name,
              refresh_policy_kind)
             VALUES (?1, 'Test', ?2, 0, 0,
                     'line', 0, 10000, 0,
                     'linear', 0, 'X', 'time',
                     0, 'none',
                     'collection', NULL, 'mycoll',
                     'off')",
            rusqlite::params![Uuid::new_v4().to_string(), profile_id.to_string()],
        );
        assert!(
            result.is_err(),
            "source_kind='collection' with NULL source_collection_database should violate CHECK"
        );
    }

    #[test]
    fn test_list_full_for_dashboard_uses_three_keyed_selects() {
        // Verifies the "3 SELECTs regardless of panel count" contract from design §6.4.
        //
        // rusqlite's trace hook takes `fn(&str)` (not a closure), so we use a thread-local
        // counter that the trace function increments. The counter is reset before
        // list_full_for_dashboard is called and read after it returns.
        use std::cell::Cell;

        thread_local! {
            static VIZ_SELECT_COUNT: Cell<usize> = const { Cell::new(0) };
        }

        fn trace_hook(stmt: &str) {
            // Count the three chart-assembly SELECTs issued by list_full_for_dashboard:
            //   1. parent rows: SELECT ... FROM viz_saved_charts WHERE id IN (...)
            //   2. series: SELECT ... FROM viz_saved_chart_series WHERE chart_id IN (...)
            //   3. binding_y: SELECT ... FROM viz_saved_chart_binding_y WHERE chart_id IN (...)
            // The initial panels lookup (FROM viz_dashboard_panels) is NOT counted here;
            // the design's "3 SELECTs" refers only to the chart-assembly phase.
            // Note: multi-line SQL has table names followed by whitespace or newlines.
            let is_chart_assembly_select = stmt.contains("SELECT")
                && (stmt.contains("viz_saved_charts")
                    || stmt.contains("viz_saved_chart_series")
                    || stmt.contains("viz_saved_chart_binding_y"))
                && !stmt.contains("viz_dashboard_panels");
            if is_chart_assembly_select {
                VIZ_SELECT_COUNT.with(|c| c.set(c.get() + 1));
            }
        }

        let path = temp_db("three_selects");
        let mut conn = open_database(&path).expect("open");
        MigrationRegistry::new().run_all(&conn).expect("migrate");

        let profile_id = Uuid::new_v4();
        conn.execute(
            "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, ?2)",
            rusqlite::params![profile_id.to_string(), "p"],
        )
        .unwrap();

        let dashboard_id = Uuid::new_v4();
        conn.execute(
            "INSERT INTO viz_dashboards (id, name, shared_refresh_policy_kind, grid_columns, created_at, updated_at)
             VALUES (?1, 'D', 'off', 2, 0, 0)",
            rusqlite::params![dashboard_id.to_string()],
        )
        .unwrap();

        // Insert 3 charts and 3 panel rows.
        for i in 0..3u32 {
            let chart_id = Uuid::new_v4();
            conn.execute(
                "INSERT INTO viz_saved_charts
                 (id, name, profile_id, created_at, updated_at,
                  chart_kind, legend_visible, decimation_threshold, track_source_indices,
                  y_scale, x_axis_column_index, x_axis_label, x_axis_kind,
                  binding_x, binding_aggregation, source_kind, source_query,
                  refresh_policy_kind)
                 VALUES (?1, 'C', ?2, 0, 0,
                         'line', 0, 10000, 0,
                         'linear', 0, 'X', 'time',
                         0, 'none', 'query', 'SELECT 1',
                         'off')",
                rusqlite::params![chart_id.to_string(), profile_id.to_string()],
            )
            .unwrap();

            conn.execute(
                "INSERT INTO viz_dashboard_panels
                 (dashboard_id, panel_index, saved_chart_id, grid_row, grid_column, grid_width, grid_height)
                 VALUES (?1, ?2, ?3, 0, 0, 1, 1)",
                rusqlite::params![dashboard_id.to_string(), i as i64, chart_id.to_string()],
            )
            .unwrap();
        }

        // Attach trace hook AFTER setup so we only count queries from list_full_for_dashboard.
        conn.trace(Some(trace_hook as fn(&str)));
        VIZ_SELECT_COUNT.with(|c| c.set(0));

        let conn = Arc::new(Mutex::new(conn));
        let repo = SavedChartsRepository::new(Arc::clone(&conn));

        let charts = repo.list_full_for_dashboard(dashboard_id).expect("list");
        assert_eq!(charts.len(), 3, "should load 3 charts");

        let count = VIZ_SELECT_COUNT.with(|c| c.get());
        // Expect exactly 3 viz_* SELECTs: panels IN-list, series IN-list, binding_y IN-list.
        assert_eq!(
            count, 3,
            "list_full_for_dashboard must issue exactly 3 SELECTs against viz_ tables, got {count}"
        );
    }

    /// Metric source round-trip: all scalar columns + ordered dimensions child table.
    #[test]
    fn test_saved_chart_metric_source_roundtrip() {
        let (_conn, repo, profile_id) = setup("metric_roundtrip");
        let id = Uuid::new_v4();
        let dto = make_metric_chart(id, profile_id);

        repo.upsert(&dto).expect("upsert");

        let loaded = repo.get_full_chart(id).expect("get").expect("should exist");
        assert_eq!(loaded.source_kind, "metric");
        assert_eq!(loaded.source_metric_namespace.as_deref(), Some("AWS/EC2"));
        assert_eq!(loaded.source_metric_name.as_deref(), Some("CPUUtilization"));
        assert_eq!(loaded.source_metric_period_seconds, Some(300));
        assert_eq!(loaded.source_metric_statistic.as_deref(), Some("Average"));
        assert_eq!(loaded.source_metric_region.as_deref(), Some("us-east-1"));

        // Dimensions must be loaded and ordered.
        assert_eq!(loaded.metric_dimensions.len(), 2);
        assert_eq!(loaded.metric_dimensions[0].dim_index, 0);
        assert_eq!(loaded.metric_dimensions[0].dim_key, "InstanceId");
        assert_eq!(loaded.metric_dimensions[0].dim_value, "i-12345");
        assert_eq!(loaded.metric_dimensions[1].dim_index, 1);
        assert_eq!(loaded.metric_dimensions[1].dim_key, "Region");
        assert_eq!(loaded.metric_dimensions[1].dim_value, "us-east-1");

        // Non-metric columns must be NULL.
        assert!(loaded.source_query.is_none());
        assert!(loaded.source_collection_database.is_none());
    }

    /// CHECK: `source_kind = 'metric'` with NULL `source_metric_namespace` is rejected.
    #[test]
    fn test_check_metric_source_kind_without_namespace() {
        let (conn, _repo, profile_id) = setup("check_metric_null_ns");
        let locked = conn.lock().unwrap();
        let result = locked.execute(
            "INSERT INTO viz_saved_charts
             (id, name, profile_id, created_at, updated_at,
              chart_kind, legend_visible, decimation_threshold, track_source_indices,
              y_scale, x_axis_column_index, x_axis_label, x_axis_kind,
              binding_x, binding_aggregation,
              source_kind,
              source_metric_namespace, source_metric_name,
              source_metric_period_seconds, source_metric_statistic,
              refresh_policy_kind)
             VALUES (?1, 'Test', ?2, 0, 0,
                     'line', 0, 10000, 0,
                     'linear', 0, 'X', 'time',
                     0, 'none',
                     'metric',
                     NULL, 'CPUUtilization', 300, 'Average',
                     'off')",
            rusqlite::params![Uuid::new_v4().to_string(), profile_id.to_string()],
        );
        assert!(
            result.is_err(),
            "source_kind='metric' with NULL source_metric_namespace should violate CHECK"
        );
    }

    /// Metric dimensions are deleted with the chart (FK CASCADE).
    #[test]
    fn test_metric_dimensions_cascade_on_chart_delete() {
        let (conn, repo, profile_id) = setup("metric_dim_cascade");
        let id = Uuid::new_v4();
        let dto = make_metric_chart(id, profile_id);

        repo.upsert(&dto).expect("upsert");
        repo.delete(id).expect("delete");

        let locked = conn.lock().unwrap();
        let count: i64 = locked
            .query_row(
                "SELECT COUNT(*) FROM viz_saved_chart_source_metric_dimensions WHERE chart_id = ?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(count, 0, "metric dimensions must cascade on chart delete");
    }

    /// Metric chart upsert preserves dimension order through a re-upsert.
    #[test]
    fn test_metric_dimension_order_preserved_on_upsert() {
        let (_conn, repo, profile_id) = setup("metric_dim_order");
        let id = Uuid::new_v4();
        let dto = make_metric_chart(id, profile_id);

        // First upsert.
        repo.upsert(&dto).expect("first upsert");

        // Second upsert with reversed dimensions to confirm atomic replace.
        let mut dto2 = dto.clone();
        dto2.metric_dimensions = vec![
            crate::repositories::viz_saved_chart_source_metric_dimensions::MetricDimensionDto {
                chart_id: id.to_string(),
                dim_index: 0,
                dim_key: "Z-key".to_string(),
                dim_value: "z-val".to_string(),
            },
        ];
        repo.upsert(&dto2).expect("second upsert");

        let loaded = repo.get_full_chart(id).expect("get").expect("exists");
        assert_eq!(
            loaded.metric_dimensions.len(),
            1,
            "re-upsert must replace old dimensions"
        );
        assert_eq!(loaded.metric_dimensions[0].dim_key, "Z-key");
    }
}
