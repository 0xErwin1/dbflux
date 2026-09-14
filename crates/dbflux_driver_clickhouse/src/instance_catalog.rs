use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dbflux_core::{
    ColumnKind, ColumnMeta, DbError, DefaultDashboardPanel, DefaultInstanceDashboard,
    DriverCapabilities, InspectorRowAction, InstanceCatalog, InstanceInspectorDef,
    InstanceMetricDef, InstanceMetricUnit, QueryResult, QueryResultShape, Row, Value,
};

use crate::connection::parse_response;
use crate::error_formatter::ClickHouseErrorFormatter;
use crate::http::ClickHouseHttpClient;

/// System table backing a curated ClickHouse instance metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricSource {
    /// `system.metrics` — a current gauge value, keyed by the `metric` column.
    Metrics,
    /// `system.events` — a cumulative counter, keyed by the `event` column.
    ///
    /// The table lists only events that have fired at least once since the
    /// server started, so a counter with no row yet reports zero, which is
    /// its true value rather than a missing reading.
    Events,
    /// `system.asynchronous_metrics` — a periodically sampled gauge, keyed by
    /// the `metric` column.
    AsynchronousMetrics,
}

impl MetricSource {
    /// Builds the read-only `SELECT` that fetches this metric's current value.
    ///
    /// `raw_name` is always sourced from [`METRIC_DEFS`], never from caller
    /// input, so string interpolation here carries no injection surface.
    fn select_sql(self, raw_name: &str) -> String {
        match self {
            MetricSource::Metrics => {
                format!("SELECT toFloat64(value) FROM system.metrics WHERE metric = '{raw_name}'")
            }
            MetricSource::Events => {
                format!("SELECT toFloat64(value) FROM system.events WHERE event = '{raw_name}'")
            }
            MetricSource::AsynchronousMetrics => format!(
                "SELECT toFloat64(value) FROM system.asynchronous_metrics WHERE metric = '{raw_name}'"
            ),
        }
    }
}

/// Curated list of ClickHouse system-table metrics mapped to chartable instance metrics.
///
/// Each entry: `(raw_name, metric_id, display_name, group, unit, source)`.
/// `raw_name` matches the `metric`/`event` column value in the source system table.
pub const METRIC_DEFS: &[(&str, &str, &str, &str, InstanceMetricUnit, MetricSource)] = &[
    (
        "Query",
        "clickhouse.query",
        "Active queries",
        "Activity",
        InstanceMetricUnit::Count,
        MetricSource::Metrics,
    ),
    (
        "MemoryTracking",
        "clickhouse.memory_tracking",
        "Tracked memory",
        "Memory",
        InstanceMetricUnit::Bytes,
        MetricSource::Metrics,
    ),
    (
        "TCPConnection",
        "clickhouse.tcp_connection",
        "TCP connections",
        "Connections",
        InstanceMetricUnit::Count,
        MetricSource::Metrics,
    ),
    (
        "HTTPConnection",
        "clickhouse.http_connection",
        "HTTP connections",
        "Connections",
        InstanceMetricUnit::Count,
        MetricSource::Metrics,
    ),
    (
        "SelectedRows",
        "clickhouse.selected_rows",
        "Selected rows",
        "Throughput",
        InstanceMetricUnit::Count,
        MetricSource::Events,
    ),
    (
        "InsertedRows",
        "clickhouse.inserted_rows",
        "Inserted rows",
        "Throughput",
        InstanceMetricUnit::Count,
        MetricSource::Events,
    ),
    (
        "OSMemoryAvailable",
        "clickhouse.os_memory_available",
        "OS memory available",
        "Memory",
        InstanceMetricUnit::Bytes,
        MetricSource::AsynchronousMetrics,
    ),
];

/// Read-only projection of `system.processes` used by the `clickhouse.processes` inspector.
///
/// Numeric columns are cast to `Float64` so every non-text column resolves to
/// `ColumnKind::Float` uniformly, following the same cast-in-SQL pattern used
/// by the PostgreSQL activity inspector.
const PROCESSES_SQL: &str = "\
    SELECT query_id, user, toString(address) AS address, elapsed AS elapsed_secs, \
           toFloat64(read_rows) AS read_rows, toFloat64(memory_usage) AS memory_usage_bytes, \
           substring(query, 1, 200) AS query_preview \
    FROM system.processes \
    ORDER BY elapsed DESC";

/// Placeholder query id used to probe `KILL QUERY` privilege without side effects.
///
/// ClickHouse `query_id` values are user-supplied strings, not necessarily
/// UUIDs, so this only needs to be a value unlikely to match a real running
/// query; ClickHouse allows `KILL QUERY` against zero matching processes.
const KILL_QUERY_PROBE_ID: &str = "dbflux-instance-catalog-kill-probe";

/// Maximum recognized `system.grants` `ACCESS_DENIED` error code, used to
/// detect a missing `KILL QUERY` privilege during the probe.
const ACCESS_DENIED_CODE: &str = "497";

/// ClickHouse instance metrics and inspector catalog.
///
/// Holds a shared reference to the connection's HTTP client so it can issue
/// queries on demand. ClickHouse's HTTP interface is stateless, so no lock is
/// required to share the client across the connection and the catalog.
pub struct ClickHouseInstanceCatalog {
    client: Arc<ClickHouseHttpClient>,
    kill_query_allowed: bool,
}

impl ClickHouseInstanceCatalog {
    /// Constructs a catalog with a probed `kill_query_allowed` flag.
    ///
    /// Issues a harmless `KILL QUERY WHERE query_id = '<placeholder>'` probe:
    /// success (including zero matching rows) means the privilege is granted;
    /// an `ACCESS_DENIED` error (code `497` or a "not enough privileges"
    /// message) means it is not. Any other failure defaults to `false`.
    pub(crate) fn new_probed(client: Arc<ClickHouseHttpClient>) -> Self {
        let kill_query_allowed = probe_kill_query_allowed(&client);
        Self {
            client,
            kill_query_allowed,
        }
    }

    pub fn static_metrics() -> Vec<InstanceMetricDef> {
        METRIC_DEFS
            .iter()
            .map(|(_, id, display_name, group, unit, _)| InstanceMetricDef {
                id: id.to_string(),
                display_name: display_name.to_string(),
                group: group.to_string(),
                unit: *unit,
                description: None,
                default_refresh_secs: 15,
            })
            .collect()
    }

    pub fn static_inspectors() -> Vec<InstanceInspectorDef> {
        vec![InstanceInspectorDef {
            id: "clickhouse.processes".to_string(),
            display_name: "Running queries".to_string(),
            description: Some(
                "Live snapshot of system.processes — one row per running query.".to_string(),
            ),
            default_refresh_secs: 10,
        }]
    }

    /// Curated "Instance Overview" dashboard layout for ClickHouse.
    ///
    /// Row 0: active queries (cols 0-5) | tracked memory (cols 6-11)
    /// Row 3: TCP connections (cols 0-5) | selected rows (cols 6-11)
    /// Row 6: running queries inspector (full width)
    pub fn static_default_dashboard() -> Option<DefaultInstanceDashboard> {
        Some(DefaultInstanceDashboard {
            title: "ClickHouse Instance Overview".to_string(),
            description: Some(
                "Curated ClickHouse system-table metrics and running-queries inspector."
                    .to_string(),
            ),
            panels: vec![
                DefaultDashboardPanel {
                    metric_id: "clickhouse.query".to_string(),
                    is_inspector: false,
                    grid_column: 0,
                    grid_row: 0,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "clickhouse.memory_tracking".to_string(),
                    is_inspector: false,
                    grid_column: 6,
                    grid_row: 0,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "clickhouse.tcp_connection".to_string(),
                    is_inspector: false,
                    grid_column: 0,
                    grid_row: 3,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "clickhouse.selected_rows".to_string(),
                    is_inspector: false,
                    grid_column: 6,
                    grid_row: 3,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "clickhouse.processes".to_string(),
                    is_inspector: true,
                    grid_column: 0,
                    grid_row: 6,
                    grid_width: 12,
                    grid_height: 4,
                },
            ],
        })
    }

    /// Static list of row-level actions for the given inspector metric.
    pub fn static_row_actions(metric_id: &str) -> Vec<InspectorRowAction> {
        match metric_id {
            "clickhouse.processes" => vec![InspectorRowAction {
                id: "kill".to_string(),
                label: "Kill query".to_string(),
                description: Some(
                    "Sends KILL QUERY WHERE query_id = '<id>' to stop the selected query."
                        .to_string(),
                ),
                is_destructive: true,
            }],
            _ => Vec::new(),
        }
    }
}

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn timestamp_col(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        kind: ColumnKind::Timestamp,
        type_name: "DateTime64".to_string(),
        nullable: false,
        is_primary_key: false,
    }
}

fn float_col(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        kind: ColumnKind::Float,
        type_name: "Float64".to_string(),
        nullable: false,
        is_primary_key: false,
    }
}

fn extract_float(value: &Value) -> f64 {
    match value {
        Value::Float(value) => *value,
        Value::Int(value) => *value as f64,
        Value::Decimal(value) => value.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// Runs a read-only instance-catalog query against the current database.
///
/// Shared by metric fetches, the inspector snapshot, the row-action kill
/// command, and the privilege probe; every caller reuses the same HTTP
/// error-formatting and response-parsing path as `ClickHouseConnection::execute_sql`.
fn run_instance_query(client: &ClickHouseHttpClient, sql: &str) -> Result<QueryResult, DbError> {
    let started = Instant::now();
    let response = client
        .execute(sql, None, None, None, None)
        .map_err(|error| ClickHouseErrorFormatter::format_http_error(&error).into_query_error())?;
    parse_response(response, started.elapsed())
}

/// Probes whether the current user can run `KILL QUERY`.
///
/// `KILL QUERY` against zero matching processes succeeds, so only a
/// successful probe grants the privilege. Every failure — `ACCESS_DENIED`
/// (error code `497` or a "not enough privileges" message), a transport
/// error, or anything else — leaves the action hidden, matching the
/// conservative default used by the Redis and PostgreSQL catalogs. Hiding an
/// action the user could have run is recoverable; offering one they cannot is
/// a dead button on a destructive control.
fn probe_kill_query_allowed(client: &ClickHouseHttpClient) -> bool {
    let probe_sql = format!("KILL QUERY WHERE query_id = '{KILL_QUERY_PROBE_ID}'");

    match run_instance_query(client, &probe_sql) {
        Ok(_) => true,
        Err(DbError::QueryFailed(formatted)) => {
            let denied_by_code = formatted.code.as_deref() == Some(ACCESS_DENIED_CODE);
            let denied_by_message = formatted
                .message
                .to_ascii_lowercase()
                .contains("not enough privileges");

            log::debug!(
                "ClickHouse KILL QUERY privilege probe failed (access denied: {}): {}",
                denied_by_code || denied_by_message,
                formatted.message
            );

            false
        }
        Err(_) => false,
    }
}

/// Rejects a `query_id` that is not a plain identifier before it is
/// interpolated into a `KILL QUERY` statement.
///
/// ClickHouse `query_id` values are client-supplied strings, so this allows
/// only ASCII letters, digits, `-`, and `_` — covering both the server's
/// default UUID-shaped ids and any custom id a client may have set.
fn validate_query_id(raw: &str) -> Result<&str, DbError> {
    let trimmed = raw.trim();
    let valid = !trimmed.is_empty()
        && trimmed.len() <= 128
        && trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');

    if valid {
        Ok(trimmed)
    } else {
        Err(DbError::QueryFailed(
            format!("clickhouse.processes kill: invalid query_id format: '{trimmed}'").into(),
        ))
    }
}

#[async_trait]
impl InstanceCatalog for ClickHouseInstanceCatalog {
    async fn list_metrics(&self) -> Result<Vec<InstanceMetricDef>, DbError> {
        Ok(Self::static_metrics())
    }

    async fn list_inspectors(&self) -> Result<Vec<InstanceInspectorDef>, DbError> {
        Ok(Self::static_inspectors())
    }

    fn default_dashboard(&self) -> Option<DefaultInstanceDashboard> {
        Self::static_default_dashboard()
    }

    async fn fetch_metric_series(
        &self,
        metric_id: &str,
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<QueryResult, DbError> {
        dispatch_metric_series(&self.client, metric_id)
    }

    async fn fetch_inspector_snapshot(&self, metric_id: &str) -> Result<QueryResult, DbError> {
        dispatch_inspector_snapshot(&self.client, metric_id)
    }

    fn row_actions(&self, metric_id: &str) -> Vec<InspectorRowAction> {
        if !self.kill_query_allowed {
            return Vec::new();
        }
        Self::static_row_actions(metric_id)
    }

    async fn execute_row_action(
        &self,
        metric_id: &str,
        action_id: &str,
        row_values: &[Value],
    ) -> Result<(), DbError> {
        if metric_id == "clickhouse.processes" && action_id == "kill" {
            let raw_id = match row_values.first() {
                Some(Value::Text(text)) => text.clone(),
                _ => {
                    return Err(DbError::QueryFailed(
                        "clickhouse.processes kill: could not read query_id from row"
                            .to_string()
                            .into(),
                    ));
                }
            };

            let query_id = validate_query_id(&raw_id)?;
            run_instance_query(
                &self.client,
                &format!("KILL QUERY WHERE query_id = '{query_id}'"),
            )
            .map(|_| ())
        } else {
            Err(DbError::NotSupported(format!(
                "row action '{action_id}' not supported for inspector '{metric_id}'"
            )))
        }
    }
}

pub(crate) fn dispatch_metric_series(
    client: &ClickHouseHttpClient,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    let entry = METRIC_DEFS.iter().find(|(_, id, ..)| *id == metric_id);

    match entry {
        Some((raw_name, _, display_name, _, _, source)) => {
            let result = run_instance_query(client, &source.select_sql(raw_name))?;
            let value = result
                .rows
                .first()
                .and_then(|row| row.first())
                .map(extract_float)
                .unwrap_or(0.0);

            let row: Row = vec![Value::Int(now_epoch_ms()), Value::Float(value)];

            Ok(QueryResult {
                shape: QueryResultShape::Table,
                columns: vec![timestamp_col("timestamp_ms"), float_col(display_name)],
                rows: vec![row],
                affected_rows: None,
                execution_time: Duration::ZERO,
                text_body: None,
                raw_bytes: None,
                next_page_token: None,
                resolved_window: None,
                metadata_extra: None,
                additional_results: Vec::new(),
            })
        }
        None => Err(DbError::NotSupported(format!(
            "unknown instance metric: {metric_id}"
        ))),
    }
}

pub(crate) fn dispatch_inspector_snapshot(
    client: &ClickHouseHttpClient,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    match metric_id {
        "clickhouse.processes" => run_instance_query(client, PROCESSES_SQL),
        other => Err(DbError::NotSupported(format!("unknown inspector: {other}"))),
    }
}

/// Returns `true` if the ClickHouse driver metadata advertises both
/// instance-metrics capability bits.
pub fn clickhouse_advertises_instance_capabilities() -> bool {
    let caps = crate::driver::METADATA.capabilities;
    caps.contains(DriverCapabilities::INSTANCE_METRICS)
        && caps.contains(DriverCapabilities::INSTANCE_INSPECTOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_defs_list_is_non_empty_with_distinct_ids() {
        assert!(!METRIC_DEFS.is_empty(), "METRIC_DEFS must have entries");

        let mut ids: Vec<&str> = METRIC_DEFS.iter().map(|(_, id, ..)| *id).collect();
        let inspector_ids: Vec<String> = ClickHouseInstanceCatalog::static_inspectors()
            .into_iter()
            .map(|inspector| inspector.id)
            .collect();
        ids.extend(inspector_ids.iter().map(String::as_str));

        let mut deduped = ids.clone();
        deduped.sort_unstable();
        deduped.dedup();
        assert_eq!(
            ids.len(),
            deduped.len(),
            "metric and inspector ids must be distinct across the shared namespace"
        );
    }

    #[test]
    fn static_metric_default_refresh_secs_at_or_above_floor() {
        for metric in ClickHouseInstanceCatalog::static_metrics() {
            assert!(
                metric.default_refresh_secs >= 10,
                "metric {:?} default_refresh_secs {} is below the 10s floor",
                metric.id,
                metric.default_refresh_secs
            );
        }
    }

    #[test]
    fn static_inspector_default_refresh_secs_at_or_above_floor() {
        for inspector in ClickHouseInstanceCatalog::static_inspectors() {
            assert!(
                inspector.default_refresh_secs >= 10,
                "inspector {:?} default_refresh_secs {} is below the 10s floor",
                inspector.id,
                inspector.default_refresh_secs
            );
        }
    }

    #[test]
    fn clickhouse_advertises_both_instance_capability_bits() {
        assert!(
            clickhouse_advertises_instance_capabilities(),
            "ClickHouse METADATA must include INSTANCE_METRICS and INSTANCE_INSPECTOR bits"
        );
    }

    #[test]
    fn clickhouse_default_dashboard_is_non_none_and_valid() {
        let dashboard = ClickHouseInstanceCatalog::static_default_dashboard()
            .expect("ClickHouseInstanceCatalog must return Some(default_dashboard)");

        assert!(
            !dashboard.panels.is_empty(),
            "default dashboard must have at least one panel"
        );
        assert!(
            !dashboard.title.is_empty(),
            "default dashboard must have a non-empty title"
        );

        let metric_ids: Vec<String> = ClickHouseInstanceCatalog::static_metrics()
            .into_iter()
            .map(|metric| metric.id)
            .collect();
        let inspector_ids: Vec<String> = ClickHouseInstanceCatalog::static_inspectors()
            .into_iter()
            .map(|inspector| inspector.id)
            .collect();

        for panel in &dashboard.panels {
            let valid =
                metric_ids.contains(&panel.metric_id) || inspector_ids.contains(&panel.metric_id);
            assert!(
                valid,
                "panel metric_id {:?} is not in static metrics or inspectors",
                panel.metric_id
            );
            assert!(panel.grid_column <= 11, "grid_column must be within 0..=11");
            assert!(
                panel.grid_width >= 1 && panel.grid_width <= 12,
                "grid_width must be within 1..=12"
            );
        }
    }

    #[test]
    fn row_actions_processes_returns_kill() {
        let actions = ClickHouseInstanceCatalog::static_row_actions("clickhouse.processes");
        assert_eq!(
            actions.len(),
            1,
            "clickhouse.processes must have exactly one row action"
        );
        assert_eq!(actions[0].id, "kill");
        assert!(actions[0].is_destructive);
    }

    #[test]
    fn row_actions_unknown_inspector_returns_empty() {
        let actions = ClickHouseInstanceCatalog::static_row_actions("clickhouse.does_not_exist");
        assert!(actions.is_empty());
    }

    #[test]
    fn validate_query_id_accepts_uuid_shaped_id() {
        let result = validate_query_id("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        assert_eq!(result.ok(), Some("a1b2c3d4-e5f6-7890-abcd-ef1234567890"));
    }

    #[test]
    fn validate_query_id_rejects_quote_injection() {
        assert!(validate_query_id("abc'; DROP TABLE x; --").is_err());
    }

    #[test]
    fn validate_query_id_rejects_empty() {
        assert!(validate_query_id("").is_err());
    }

    #[test]
    fn extract_float_reads_float_int_and_decimal() {
        assert_eq!(extract_float(&Value::Float(1.5)), 1.5);
        assert_eq!(extract_float(&Value::Int(3)), 3.0);
        assert_eq!(extract_float(&Value::Decimal("2.25".to_string())), 2.25);
        assert_eq!(extract_float(&Value::Null), 0.0);
    }
}
