//! `SavedChartManager` — SQLite-backed manager for `SavedChart` records.
//!
//! This module replaces the JSON-backed `SavedChartManager` that previously
//! lived in `dbflux_components`. The new implementation wraps
//! `SavedChartsRepository` from `dbflux_storage` and keeps an in-memory
//! cache of the loaded charts for synchronous reads.
//!
//! ## DTO-to-domain conversion
//!
//! `SavedChartDto` from storage uses flat scalar columns (strings for enums,
//! integers for usize, etc.). The conversion from DTO to `SavedChart` is
//! implemented as `TryFrom<SavedChartDto>` in this file. An unrecognized
//! string value in an enum column is treated as a `Data` error rather than
//! panicking — this matches the robustness posture of the old JSON path.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use dbflux_components::{
    SavedChart, SavedChartRefreshPolicy, TimeRangePreset,
    chart::{AggKind, AxisKind, AxisSpec, BindingSpec, ChartKind, ChartSpec, SeriesSpec, YScale},
    saved_chart::SavedChartSource,
};
use dbflux_core::{CollectionRef, QueryLanguage, ResolvedWindow};
use dbflux_storage::{
    error::StorageError,
    repositories::viz_saved_chart_binding_y::BindingYDto,
    repositories::viz_saved_chart_series::SeriesDto,
    repositories::viz_saved_charts::{SavedChartDto, SavedChartsRepository},
};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DTO → domain conversion
// ---------------------------------------------------------------------------

/// Convert a flat `SavedChartDto` (storage layer) into a `SavedChart`
/// (domain layer). Returns `Err(StorageError::Data)` for unrecognized enum
/// values rather than panicking.
fn dto_to_chart(dto: SavedChartDto) -> Result<SavedChart, StorageError> {
    let id = Uuid::parse_str(&dto.id)
        .map_err(|e| StorageError::Data(format!("invalid chart id '{}': {e}", dto.id)))?;

    let profile_id = Uuid::parse_str(&dto.profile_id)
        .map_err(|e| StorageError::Data(format!("invalid profile_id '{}': {e}", dto.profile_id)))?;

    let created_at = Utc
        .timestamp_millis_opt(dto.created_at)
        .single()
        .ok_or_else(|| StorageError::Data(format!("invalid created_at: {}", dto.created_at)))?;

    let updated_at = Utc
        .timestamp_millis_opt(dto.updated_at)
        .single()
        .ok_or_else(|| StorageError::Data(format!("invalid updated_at: {}", dto.updated_at)))?;

    let chart_kind = parse_chart_kind(&dto.chart_kind)?;
    let axis_kind = parse_axis_kind(&dto.x_axis_kind)?;
    let y_scale = parse_y_scale(&dto.y_scale)?;
    let aggregation = parse_agg_kind(&dto.binding_aggregation)?;

    let x_axis = AxisSpec {
        column_index: dto.x_axis_column_index as usize,
        label: dto.x_axis_label,
        kind: axis_kind,
        unit: dto.x_axis_unit,
    };

    let series: Vec<SeriesSpec> = dto.series.into_iter().map(series_dto_to_spec).collect();

    let binding_y: Vec<usize> = dto
        .binding_y
        .iter()
        .map(|b| b.column_index as usize)
        .collect();

    let binding = BindingSpec {
        x: dto.binding_x as usize,
        y: binding_y,
        group_by: dto.binding_group_by.map(|v| v as usize),
        filter: dto.binding_filter,
        aggregation,
    };

    let chart_spec = ChartSpec {
        kind: chart_kind,
        x_axis,
        series,
        legend_visible: dto.legend_visible != 0,
        decimation_threshold: dto.decimation_threshold as usize,
        binding: binding.clone(),
        track_source_indices: dto.track_source_indices != 0,
        y_scale,
    };

    let source = match dto.source_kind.as_str() {
        "query" => {
            let query = dto.source_query.unwrap_or_default();
            SavedChartSource::Query { query }
        }
        "collection" => {
            let database = dto.source_collection_database.ok_or_else(|| {
                StorageError::Data("collection source missing database".to_string())
            })?;
            let name = dto
                .source_collection_name
                .ok_or_else(|| StorageError::Data("collection source missing name".to_string()))?;

            let time_window = match (
                dto.source_time_window_start_ms,
                dto.source_time_window_end_ms,
                dto.source_time_window_language,
            ) {
                (Some(start), Some(end), Some(lang)) => {
                    let language = parse_query_language(&lang)?;
                    Some(ResolvedWindow {
                        start_ms: start,
                        end_ms: end,
                        language,
                    })
                }
                _ => None,
            };

            SavedChartSource::Collection {
                collection_ref: CollectionRef::new(database, name),
                time_window,
            }
        }
        other => {
            return Err(StorageError::Data(format!(
                "unknown source_kind: '{other}'"
            )));
        }
    };

    let time_range_preset = dto
        .time_range_preset
        .as_deref()
        .map(parse_time_range_preset)
        .transpose()?;

    let refresh_policy =
        parse_refresh_policy(&dto.refresh_policy_kind, dto.refresh_policy_interval_secs)?;

    Ok(SavedChart {
        id,
        name: dto.name,
        profile_id,
        source,
        chart_spec,
        bindings: binding,
        time_range_preset,
        refresh_policy,
        created_at,
        updated_at,
    })
}

/// Convert a `SavedChart` domain value to a `SavedChartDto` for storage.
fn chart_to_dto(
    chart: &SavedChart,
    series: Vec<SeriesDto>,
    binding_y: Vec<BindingYDto>,
) -> SavedChartDto {
    let (
        source_kind,
        source_query,
        source_collection_database,
        source_collection_name,
        source_time_window_start_ms,
        source_time_window_end_ms,
        source_time_window_language,
    ) = match &chart.source {
        SavedChartSource::Query { query } => (
            "query".to_string(),
            Some(query.clone()),
            None,
            None,
            None,
            None,
            None,
        ),
        SavedChartSource::Collection {
            collection_ref,
            time_window,
        } => {
            let (start, end, lang) = match time_window {
                Some(w) => (
                    Some(w.start_ms),
                    Some(w.end_ms),
                    Some(query_language_to_str(w.language.clone())),
                ),
                None => (None, None, None),
            };
            (
                "collection".to_string(),
                None,
                Some(collection_ref.database.clone()),
                Some(collection_ref.name.clone()),
                start,
                end,
                lang,
            )
        }
    };

    SavedChartDto {
        id: chart.id.to_string(),
        name: chart.name.clone(),
        profile_id: chart.profile_id.to_string(),
        created_at: chart.created_at.timestamp_millis(),
        updated_at: chart.updated_at.timestamp_millis(),

        chart_kind: chart_kind_to_str(chart.chart_spec.kind),
        legend_visible: chart.chart_spec.legend_visible as i64,
        decimation_threshold: chart.chart_spec.decimation_threshold as i64,
        track_source_indices: chart.chart_spec.track_source_indices as i64,
        y_scale: y_scale_to_str(chart.chart_spec.y_scale),

        x_axis_column_index: chart.chart_spec.x_axis.column_index as i64,
        x_axis_label: chart.chart_spec.x_axis.label.clone(),
        x_axis_kind: axis_kind_to_str(chart.chart_spec.x_axis.kind),
        x_axis_unit: chart.chart_spec.x_axis.unit.clone(),

        binding_x: chart.bindings.x as i64,
        binding_group_by: chart.bindings.group_by.map(|v| v as i64),
        binding_filter: chart.bindings.filter.clone(),
        binding_aggregation: agg_kind_to_str(chart.bindings.aggregation),

        source_kind,
        source_query,
        source_collection_database,
        source_collection_name,
        source_time_window_start_ms,
        source_time_window_end_ms,
        source_time_window_language,

        time_range_preset: chart.time_range_preset.map(time_range_preset_to_str),
        refresh_policy_kind: refresh_policy_kind_to_str(chart.refresh_policy),
        refresh_policy_interval_secs: match chart.refresh_policy {
            SavedChartRefreshPolicy::Interval { every_secs } => Some(every_secs as i64),
            _ => None,
        },

        series,
        binding_y,
    }
}

// ---------------------------------------------------------------------------
// Enum string serializers/parsers
// ---------------------------------------------------------------------------

fn parse_chart_kind(s: &str) -> Result<ChartKind, StorageError> {
    match s {
        "line" => Ok(ChartKind::Line),
        "bar" => Ok(ChartKind::Bar),
        "scatter" => Ok(ChartKind::Scatter),
        "area" => Ok(ChartKind::Area),
        "stacked_bar" => Ok(ChartKind::StackedBar),
        "pie" => Ok(ChartKind::Pie),
        other => Err(StorageError::Data(format!("unknown chart_kind: '{other}'"))),
    }
}

fn chart_kind_to_str(k: ChartKind) -> String {
    match k {
        ChartKind::Line => "line",
        ChartKind::Bar => "bar",
        ChartKind::Scatter => "scatter",
        ChartKind::Area => "area",
        ChartKind::StackedBar => "stacked_bar",
        ChartKind::Pie => "pie",
    }
    .to_string()
}

fn parse_axis_kind(s: &str) -> Result<AxisKind, StorageError> {
    match s {
        "time" => Ok(AxisKind::Time),
        "numeric" => Ok(AxisKind::Numeric),
        other => Err(StorageError::Data(format!("unknown axis_kind: '{other}'"))),
    }
}

fn axis_kind_to_str(k: AxisKind) -> String {
    match k {
        AxisKind::Time => "time",
        AxisKind::Numeric => "numeric",
    }
    .to_string()
}

fn parse_y_scale(s: &str) -> Result<YScale, StorageError> {
    match s {
        "linear" => Ok(YScale::Linear),
        "log" => Ok(YScale::Log),
        other => Err(StorageError::Data(format!("unknown y_scale: '{other}'"))),
    }
}

fn y_scale_to_str(y: YScale) -> String {
    match y {
        YScale::Linear => "linear",
        YScale::Log => "log",
    }
    .to_string()
}

fn parse_agg_kind(s: &str) -> Result<AggKind, StorageError> {
    match s {
        "none" => Ok(AggKind::None),
        "sum" => Ok(AggKind::Sum),
        "avg" => Ok(AggKind::Avg),
        "min" => Ok(AggKind::Min),
        "max" => Ok(AggKind::Max),
        other => Err(StorageError::Data(format!(
            "unknown aggregation: '{other}'"
        ))),
    }
}

fn agg_kind_to_str(a: AggKind) -> String {
    match a {
        AggKind::None => "none",
        AggKind::Sum => "sum",
        AggKind::Avg => "avg",
        AggKind::Min => "min",
        AggKind::Max => "max",
    }
    .to_string()
}

fn parse_time_range_preset(s: &str) -> Result<TimeRangePreset, StorageError> {
    match s {
        "last_15_min" => Ok(TimeRangePreset::Last15min),
        "last_hour" => Ok(TimeRangePreset::LastHour),
        "last_6_hours" => Ok(TimeRangePreset::Last6Hours),
        "last_24_hours" => Ok(TimeRangePreset::Last24Hours),
        "last_7_days" => Ok(TimeRangePreset::Last7Days),
        other => Err(StorageError::Data(format!(
            "unknown time_range_preset: '{other}'"
        ))),
    }
}

fn time_range_preset_to_str(p: TimeRangePreset) -> String {
    match p {
        TimeRangePreset::Last15min => "last_15_min",
        TimeRangePreset::LastHour => "last_hour",
        TimeRangePreset::Last6Hours => "last_6_hours",
        TimeRangePreset::Last24Hours => "last_24_hours",
        TimeRangePreset::Last7Days => "last_7_days",
    }
    .to_string()
}

fn parse_refresh_policy(
    kind: &str,
    interval_secs: Option<i64>,
) -> Result<SavedChartRefreshPolicy, StorageError> {
    match kind {
        "off" => Ok(SavedChartRefreshPolicy::Off),
        "interval" => {
            let secs = interval_secs.ok_or_else(|| {
                StorageError::Data(
                    "refresh_policy_kind = 'interval' but interval_secs is NULL".to_string(),
                )
            })?;
            Ok(SavedChartRefreshPolicy::Interval {
                every_secs: secs as u32,
            })
        }
        "on_open" => Ok(SavedChartRefreshPolicy::OnOpen),
        other => Err(StorageError::Data(format!(
            "unknown refresh_policy_kind: '{other}'"
        ))),
    }
}

fn refresh_policy_kind_to_str(p: SavedChartRefreshPolicy) -> String {
    match p {
        SavedChartRefreshPolicy::Off => "off",
        SavedChartRefreshPolicy::Interval { .. } => "interval",
        SavedChartRefreshPolicy::OnOpen => "on_open",
    }
    .to_string()
}

fn parse_query_language(s: &str) -> Result<QueryLanguage, StorageError> {
    match s {
        "Sql" => Ok(QueryLanguage::Sql),
        "CloudWatchLogsInsightsQl" => Ok(QueryLanguage::CloudWatchLogsInsightsQl),
        "OpenSearchPpl" => Ok(QueryLanguage::OpenSearchPpl),
        "OpenSearchSql" => Ok(QueryLanguage::OpenSearchSql),
        "MongoQuery" => Ok(QueryLanguage::MongoQuery),
        "RedisCommands" => Ok(QueryLanguage::RedisCommands),
        "Cypher" => Ok(QueryLanguage::Cypher),
        "InfluxQuery" => Ok(QueryLanguage::InfluxQuery),
        "Flux" => Ok(QueryLanguage::Flux),
        "Cql" => Ok(QueryLanguage::Cql),
        "Lua" => Ok(QueryLanguage::Lua),
        "Python" => Ok(QueryLanguage::Python),
        "Bash" => Ok(QueryLanguage::Bash),
        other if other.starts_with("Custom:") => {
            Ok(QueryLanguage::Custom(other["Custom:".len()..].to_string()))
        }
        other => Err(StorageError::Data(format!(
            "unknown query language: '{other}'"
        ))),
    }
}

fn query_language_to_str(l: QueryLanguage) -> String {
    match l {
        QueryLanguage::Sql => "Sql".to_string(),
        QueryLanguage::CloudWatchLogsInsightsQl => "CloudWatchLogsInsightsQl".to_string(),
        QueryLanguage::OpenSearchPpl => "OpenSearchPpl".to_string(),
        QueryLanguage::OpenSearchSql => "OpenSearchSql".to_string(),
        QueryLanguage::MongoQuery => "MongoQuery".to_string(),
        QueryLanguage::RedisCommands => "RedisCommands".to_string(),
        QueryLanguage::Cypher => "Cypher".to_string(),
        QueryLanguage::InfluxQuery => "InfluxQuery".to_string(),
        QueryLanguage::Flux => "Flux".to_string(),
        QueryLanguage::Cql => "Cql".to_string(),
        // Scripting languages are not stored as chart collection sources;
        // these branches are unreachable in practice but must be exhaustive.
        QueryLanguage::Lua => "Lua".to_string(),
        QueryLanguage::Python => "Python".to_string(),
        QueryLanguage::Bash => "Bash".to_string(),
        QueryLanguage::Custom(name) => format!("Custom:{name}"),
    }
}

fn series_dto_to_spec(dto: SeriesDto) -> SeriesSpec {
    SeriesSpec {
        column_index: dto.column_index as usize,
        label: dto.label,
        color_slot: dto.color_slot as u8,
    }
}

fn chart_to_series_dtos(chart: &SavedChart) -> Vec<SeriesDto> {
    chart
        .chart_spec
        .series
        .iter()
        .enumerate()
        .map(|(i, s)| SeriesDto {
            chart_id: chart.id.to_string(),
            series_index: i as i64,
            column_index: s.column_index as i64,
            label: s.label.clone(),
            color_slot: s.color_slot as i64,
        })
        .collect()
}

fn chart_to_binding_y_dtos(chart: &SavedChart) -> Vec<BindingYDto> {
    chart
        .bindings
        .y
        .iter()
        .enumerate()
        .map(|(i, &col)| BindingYDto {
            chart_id: chart.id.to_string(),
            slot_index: i as i64,
            column_index: col as i64,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// SavedChartManager
// ---------------------------------------------------------------------------

/// In-memory manager for `SavedChart` records backed by `SavedChartsRepository`.
///
/// Charts are loaded eagerly on construction and kept in a cache. Writes
/// (upsert, remove) go through the repository first; the cache is updated only
/// on success.
pub struct SavedChartManager {
    items: Vec<SavedChart>,
    repo: Arc<SavedChartsRepository>,
}

impl SavedChartManager {
    /// Load all charts from the repository and build the cache.
    ///
    /// DTOs that fail domain conversion are logged as warnings and skipped;
    /// the manager starts with whatever valid rows exist.
    pub fn new(repo: Arc<SavedChartsRepository>) -> Self {
        let items = match repo.list() {
            Ok(dtos) => dtos
                .into_iter()
                .filter_map(|dto| match dto_to_chart(dto) {
                    Ok(chart) => Some(chart),
                    Err(e) => {
                        log::warn!("SavedChartManager: skipping chart with conversion error: {e}");
                        None
                    }
                })
                .collect(),
            Err(e) => {
                log::warn!("SavedChartManager: failed to load charts: {e}; starting empty");
                Vec::new()
            }
        };

        Self { items, repo }
    }

    /// Create an empty in-memory manager with no-op writes.
    ///
    /// Used in `AppStateEntity::new()` (the no-storage variant) where no
    /// `StorageRuntime` is available. Writes on an empty manager return
    /// `false`/`true` as appropriate but do NOT persist.
    pub fn empty_with_repo(repo: Arc<SavedChartsRepository>) -> Self {
        Self {
            items: Vec::new(),
            repo,
        }
    }

    /// Insert or replace a chart by `id`.
    ///
    /// Returns `true` when an existing record was replaced, `false` when a
    /// new record was inserted. The cache is updated only when the repository
    /// write succeeds.
    pub fn upsert(&mut self, chart: SavedChart) -> bool {
        let series = chart_to_series_dtos(&chart);
        let binding_y = chart_to_binding_y_dtos(&chart);
        let dto = chart_to_dto(&chart, series, binding_y);

        let is_update = self.items.iter().any(|c| c.id == chart.id);

        match self.repo.upsert(&dto) {
            Ok(()) => {
                if let Some(existing) = self.items.iter_mut().find(|c| c.id == chart.id) {
                    *existing = chart;
                } else {
                    self.items.push(chart);
                }
                is_update
            }
            Err(e) => {
                log::error!("SavedChartManager: upsert failed: {e}");
                is_update
            }
        }
    }

    /// All charts whose `profile_id` matches the given id.
    pub fn charts_for_profile(&self, profile_id: Uuid) -> Vec<&SavedChart> {
        self.items
            .iter()
            .filter(|c| c.profile_id == profile_id)
            .collect()
    }

    /// All charts, regardless of profile.
    pub fn all_charts(&self) -> &[SavedChart] {
        &self.items
    }

    /// Remove a chart by id. Returns `true` if a record was removed.
    ///
    /// The cache is updated only when the repository delete succeeds.
    pub fn remove(&mut self, id: Uuid) -> bool {
        let was_present = self.items.iter().any(|c| c.id == id);
        if !was_present {
            return false;
        }

        match self.repo.delete(id) {
            Ok(()) => {
                self.items.retain(|c| c.id != id);
                true
            }
            Err(e) => {
                log::error!("SavedChartManager: delete failed: {e}");
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_components::chart::{
        AggKind, AxisKind, AxisSpec, BindingSpec, ChartKind, ChartSpec, SeriesSpec, YScale,
    };
    use dbflux_storage::bootstrap::StorageRuntime;
    use std::sync::Arc;

    fn temp_storage() -> StorageRuntime {
        StorageRuntime::in_memory().unwrap()
    }

    /// Insert a row into `cfg_connection_profiles` so FK constraints on
    /// `viz_saved_charts.profile_id` are satisfied during tests.
    fn insert_test_profile(
        conn: &std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
        profile_id: Uuid,
    ) {
        conn.lock()
            .unwrap()
            .execute(
                "INSERT INTO cfg_connection_profiles (id, name) VALUES (?1, ?2)",
                rusqlite::params![profile_id.to_string(), "test-profile"],
            )
            .expect("insert test profile");
    }

    fn sample_chart(name: &str, profile_id: Uuid) -> SavedChart {
        let now = Utc::now();
        SavedChart {
            id: Uuid::new_v4(),
            name: name.to_string(),
            profile_id,
            source: SavedChartSource::Query {
                query: "SELECT 1".to_string(),
            },
            chart_spec: ChartSpec {
                kind: ChartKind::Line,
                x_axis: AxisSpec {
                    column_index: 0,
                    label: "ts".to_string(),
                    kind: AxisKind::Time,
                    unit: None,
                },
                series: vec![SeriesSpec {
                    column_index: 1,
                    label: "value".to_string(),
                    color_slot: 0,
                }],
                legend_visible: false,
                decimation_threshold: 10_000,
                binding: BindingSpec {
                    x: 0,
                    y: vec![1],
                    group_by: None,
                    filter: None,
                    aggregation: AggKind::None,
                },
                track_source_indices: false,
                y_scale: YScale::Linear,
            },
            bindings: BindingSpec {
                x: 0,
                y: vec![1],
                group_by: None,
                filter: None,
                aggregation: AggKind::None,
            },
            time_range_preset: None,
            refresh_policy: SavedChartRefreshPolicy::Off,
            created_at: now,
            updated_at: now,
        }
    }

    /// Design test #29: upsert writes through and updates the in-memory cache.
    /// Reconstructing the manager from the same DB connection confirms
    /// persistence.
    #[test]
    fn test_upsert_writes_through_and_updates_cache() {
        let rt = temp_storage();
        let conn = rt.viz_connection();
        let repo = Arc::new(SavedChartsRepository::new(Arc::clone(&conn)));

        let profile_id = Uuid::new_v4();
        insert_test_profile(&conn, profile_id);
        let chart = sample_chart("my chart", profile_id);
        let chart_id = chart.id;

        let mut manager = SavedChartManager::new(Arc::clone(&repo));
        manager.upsert(chart);

        // In-memory cache contains the chart.
        assert_eq!(manager.all_charts().len(), 1);
        assert_eq!(manager.all_charts()[0].id, chart_id);

        // Reconstruct from same repo — should also see the chart.
        let manager2 = SavedChartManager::new(Arc::clone(&repo));
        assert_eq!(
            manager2.all_charts().len(),
            1,
            "write-through: reload sees chart"
        );
        assert_eq!(manager2.all_charts()[0].id, chart_id);
    }

    /// Design test #30: empty database yields empty cache; no JSON fallback.
    #[test]
    fn test_empty_database_yields_empty_cache() {
        let rt = temp_storage();
        let conn = rt.viz_connection();
        let repo = Arc::new(SavedChartsRepository::new(conn));
        let manager = SavedChartManager::new(repo);
        assert!(
            manager.all_charts().is_empty(),
            "fresh DB must yield empty cache"
        );
    }
}
