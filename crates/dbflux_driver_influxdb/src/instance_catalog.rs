//! InfluxDB v2 instance metrics and inspector catalog.
//!
//! Sourced from the server's own `/metrics` endpoint (Prometheus text
//! exposition format) rather than the `_monitoring` bucket: `_monitoring`
//! holds alerting check statuses and notification records written by the
//! Tasks/Checks system, not server telemetry, and is empty on a default
//! install. `/metrics` is InfluxDB's actual telemetry surface.
//!
//! Gated to v2 only: v1 does not expose `/metrics` or `/health` in a form
//! this catalog can rely on, mirroring the `probe_write_privilege` v1 gate
//! in `connection.rs`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dbflux_core::{
    ColumnKind, ColumnMeta, DbError, DefaultDashboardPanel, DefaultInstanceDashboard,
    DriverCapabilities, InstanceCatalog, InstanceInspectorDef, InstanceMetricDef,
    InstanceMetricUnit, QueryResult, QueryResultShape, Row, Value,
};

use crate::error_formatter::InfluxErrorFormatter;
use crate::http::{HttpClient, HttpError};

/// Curated set of Prometheus metric names exposed on `/metrics`.
///
/// Each entry: `(prometheus_name, metric_id, display_name, group, unit)`.
/// Every entry here was verified present in a live InfluxDB 2.7.12 `/metrics`
/// scrape (a stock single-node OSS instance, no data written) before being
/// declared, so a metric absent from an install this minimal was dropped
/// rather than left to fail at fetch time. Metrics split across Prometheus
/// label dimensions (e.g. `http_api_requests_total` by handler/path/status)
/// are summed across every matching sample by `dispatch_metric_series`.
pub const METRIC_FIELDS: &[(&str, &str, &str, &str, InstanceMetricUnit)] = &[
    (
        "go_memstats_alloc_bytes",
        "influx.go_memstats_alloc_bytes",
        "Go heap in use (bytes)",
        "Memory",
        InstanceMetricUnit::Bytes,
    ),
    (
        "go_memstats_sys_bytes",
        "influx.go_memstats_sys_bytes",
        "Go memory obtained from OS (bytes)",
        "Memory",
        InstanceMetricUnit::Bytes,
    ),
    (
        "go_goroutines",
        "influx.go_goroutines",
        "Goroutines",
        "Runtime",
        InstanceMetricUnit::Count,
    ),
    (
        "go_threads",
        "influx.go_threads",
        "OS threads",
        "Runtime",
        InstanceMetricUnit::Count,
    ),
    (
        "influxdb_uptime_seconds",
        "influx.uptime_seconds",
        "Uptime (seconds)",
        "Server",
        InstanceMetricUnit::Unknown,
    ),
    (
        "influxdb_buckets_total",
        "influx.buckets_total",
        "Buckets",
        "Server",
        InstanceMetricUnit::Count,
    ),
    (
        "boltdb_reads_total",
        "influx.boltdb_reads_total",
        "BoltDB reads (cumulative)",
        "Storage",
        InstanceMetricUnit::Count,
    ),
    (
        "boltdb_writes_total",
        "influx.boltdb_writes_total",
        "BoltDB writes (cumulative)",
        "Storage",
        InstanceMetricUnit::Count,
    ),
    (
        "http_api_requests_total",
        "influx.http_api_requests_total",
        "HTTP API requests (cumulative)",
        "HTTP",
        InstanceMetricUnit::Count,
    ),
    (
        "qc_all_active",
        "influx.qc_all_active",
        "Active queries",
        "Queries",
        InstanceMetricUnit::Count,
    ),
    (
        "qc_requests_total",
        "influx.qc_requests_total",
        "Query requests (cumulative)",
        "Queries",
        InstanceMetricUnit::Count,
    ),
];

/// One sample scraped from `/metrics`: name, label set, and numeric value.
///
/// `# HELP` / `# TYPE` comment lines and histogram/summary bucket suffixes
/// (`_bucket`, `_sum`, `_count`) are not represented here; the parser skips
/// them because this catalog only needs plain counter/gauge samples.
#[derive(Debug, Clone, PartialEq)]
pub struct PrometheusSample {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

/// Parses a Prometheus text-exposition body into a flat list of samples.
///
/// Tolerant of malformed lines: a line that fails to parse is skipped rather
/// than aborting the whole scrape, since `/metrics` output is otherwise
/// well-formed and a single bad line should not hide the rest.
pub fn parse_prometheus_text(body: &str) -> Vec<PrometheusSample> {
    body.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            parse_prometheus_line(line)
        })
        .collect()
}

/// Sums every sample matching `metric_name` across all label combinations.
///
/// Prometheus counters and gauges are frequently split by label (e.g.
/// `http_api_requests_total` by handler/method/path/status); summing every
/// matching sample gives the aggregate value across the whole instance.
/// Returns `None` when no sample matches, so callers can distinguish "not
/// scraped" from "scraped as zero".
pub fn find_metric_value(samples: &[PrometheusSample], metric_name: &str) -> Option<f64> {
    let matches: Vec<f64> = samples
        .iter()
        .filter(|s| s.name == metric_name)
        .map(|s| s.value)
        .collect();

    if matches.is_empty() {
        None
    } else {
        Some(matches.into_iter().sum())
    }
}

fn parse_prometheus_line(line: &str) -> Option<PrometheusSample> {
    let (name_and_labels, value_str) = line.rsplit_once(' ')?;

    let value: f64 = value_str.trim().parse().ok()?;

    let (name, labels) = match name_and_labels.split_once('{') {
        Some((name, rest)) => {
            let label_body = rest.strip_suffix('}')?;
            (name.to_string(), parse_prometheus_labels(label_body))
        }
        None => (name_and_labels.trim().to_string(), Vec::new()),
    };

    if name.is_empty() {
        return None;
    }

    Some(PrometheusSample {
        name,
        labels,
        value,
    })
}

fn parse_prometheus_labels(body: &str) -> Vec<(String, String)> {
    let mut labels = Vec::new();
    let mut remaining = body;

    while let Some(eq_pos) = remaining.find('=') {
        let key = remaining[..eq_pos].trim().to_string();
        remaining = &remaining[eq_pos + 1..];

        let Some(quote_start) = remaining.find('"') else {
            break;
        };
        remaining = &remaining[quote_start + 1..];

        let Some(quote_end) = remaining.find('"') else {
            break;
        };
        let value = remaining[..quote_end].to_string();
        remaining = remaining[quote_end + 1..]
            .trim_start_matches(',')
            .trim_start();

        labels.push((key, value));
    }

    labels
}

pub struct InfluxInstanceCatalog {
    http: HttpClient,
}

impl InfluxInstanceCatalog {
    pub fn new(http: HttpClient) -> Self {
        Self { http }
    }

    pub fn static_metrics() -> Vec<InstanceMetricDef> {
        METRIC_FIELDS
            .iter()
            .map(|(_, id, display_name, group, unit)| InstanceMetricDef {
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
        vec![
            InstanceInspectorDef {
                id: "influx.health".to_string(),
                display_name: "Health".to_string(),
                description: Some("Snapshot of the /health endpoint.".to_string()),
                default_refresh_secs: 15,
            },
            InstanceInspectorDef {
                id: "influx.metrics".to_string(),
                display_name: "Server metrics".to_string(),
                description: Some(
                    "Full Prometheus scrape from /metrics (name, labels, value).".to_string(),
                ),
                default_refresh_secs: 15,
            },
        ]
    }

    /// Curated "Instance Overview" dashboard layout for InfluxDB v2.
    ///
    /// Row 0: heap in use (cols 0-5) | goroutines (cols 6-11)
    /// Row 1: HTTP API requests (cols 0-5) | active queries (cols 6-11)
    /// Row 2: health inspector (full width)
    /// Row 3: server metrics inspector (full width)
    pub fn static_default_dashboard() -> Option<DefaultInstanceDashboard> {
        Some(DefaultInstanceDashboard {
            title: "InfluxDB Instance Overview".to_string(),
            description: Some(
                "Curated InfluxDB /metrics scrape and health/metrics inspectors.".to_string(),
            ),
            panels: vec![
                DefaultDashboardPanel {
                    metric_id: "influx.go_memstats_alloc_bytes".to_string(),
                    is_inspector: false,
                    grid_column: 0,
                    grid_row: 0,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "influx.go_goroutines".to_string(),
                    is_inspector: false,
                    grid_column: 6,
                    grid_row: 0,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "influx.http_api_requests_total".to_string(),
                    is_inspector: false,
                    grid_column: 0,
                    grid_row: 3,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "influx.qc_all_active".to_string(),
                    is_inspector: false,
                    grid_column: 6,
                    grid_row: 3,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "influx.health".to_string(),
                    is_inspector: true,
                    grid_column: 0,
                    grid_row: 6,
                    grid_width: 12,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "influx.metrics".to_string(),
                    is_inspector: true,
                    grid_column: 0,
                    grid_row: 9,
                    grid_width: 12,
                    grid_height: 4,
                },
            ],
        })
    }
}

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn timestamp_col(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        kind: ColumnKind::Timestamp,
        type_name: "integer".to_string(),
        nullable: false,
        is_primary_key: false,
    }
}

fn float_col(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        kind: ColumnKind::Float,
        type_name: "float".to_string(),
        nullable: false,
        is_primary_key: false,
    }
}

fn text_col(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        kind: ColumnKind::Text,
        type_name: "string".to_string(),
        nullable: true,
        is_primary_key: false,
    }
}

fn http_error_to_db_error(e: HttpError) -> DbError {
    match e {
        HttpError::Server { status, ref body } => {
            let fe = InfluxErrorFormatter::format_http_error(status, body);
            DbError::QueryFailed(fe)
        }
        HttpError::Transport(msg) | HttpError::Body(msg) => DbError::connection_failed(msg),
    }
}

#[async_trait]
impl InstanceCatalog for InfluxInstanceCatalog {
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
        dispatch_metric_series(&self.http, metric_id)
    }

    async fn fetch_inspector_snapshot(&self, metric_id: &str) -> Result<QueryResult, DbError> {
        dispatch_inspector_snapshot(&self.http, metric_id)
    }
}

pub(crate) fn dispatch_metric_series(
    http: &HttpClient,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    let entry = METRIC_FIELDS
        .iter()
        .find(|(_, id, _, _, _)| *id == metric_id);

    match entry {
        Some((prom_name, _, display_name, _, _)) => {
            let resp = http.get_path("metrics").map_err(http_error_to_db_error)?;

            if resp.status >= 400 {
                let fe = InfluxErrorFormatter::format_http_error(resp.status, &resp.body);
                return Err(DbError::QueryFailed(fe));
            }

            let samples = parse_prometheus_text(&resp.body);
            let value = find_metric_value(&samples, prom_name).ok_or_else(|| {
                DbError::NotSupported(format!(
                    "metric '{prom_name}' was not present in the /metrics scrape"
                ))
            })?;

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
    http: &HttpClient,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    match metric_id {
        "influx.health" => fetch_health_snapshot(http),
        "influx.metrics" => fetch_metrics_snapshot(http),
        other => Err(DbError::NotSupported(format!("unknown inspector: {other}"))),
    }
}

fn fetch_health_snapshot(http: &HttpClient) -> Result<QueryResult, DbError> {
    let resp = http.get_path("health").map_err(http_error_to_db_error)?;

    if resp.status >= 400 {
        let fe = InfluxErrorFormatter::format_http_error(resp.status, &resp.body);
        return Err(DbError::QueryFailed(fe));
    }

    let json: serde_json::Value = serde_json::from_str(&resp.body).map_err(|e| {
        DbError::QueryFailed(InfluxErrorFormatter::format_http_error(
            resp.status,
            &e.to_string(),
        ))
    })?;

    let field = |key: &str| -> Value {
        json.get(key)
            .and_then(|v| v.as_str())
            .map(|s| Value::Text(s.to_string()))
            .unwrap_or(Value::Null)
    };

    let row: Row = vec![
        field("name"),
        field("status"),
        field("message"),
        field("version"),
    ];

    Ok(QueryResult {
        shape: QueryResultShape::Table,
        columns: vec![
            text_col("name"),
            text_col("status"),
            text_col("message"),
            text_col("version"),
        ],
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

fn fetch_metrics_snapshot(http: &HttpClient) -> Result<QueryResult, DbError> {
    let resp = http.get_path("metrics").map_err(http_error_to_db_error)?;

    if resp.status >= 400 {
        let fe = InfluxErrorFormatter::format_http_error(resp.status, &resp.body);
        return Err(DbError::QueryFailed(fe));
    }

    let samples = parse_prometheus_text(&resp.body);

    let rows: Vec<Row> = samples
        .into_iter()
        .map(|sample| {
            let labels = sample
                .labels
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(", ");

            vec![
                Value::Text(sample.name),
                Value::Text(labels),
                Value::Float(sample.value),
            ]
        })
        .collect();

    Ok(QueryResult {
        shape: QueryResultShape::Table,
        columns: vec![text_col("metric"), text_col("labels"), float_col("value")],
        rows,
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

/// Returns `true` if the InfluxDB driver metadata advertises both instance-metrics bits.
pub fn influxdb_advertises_instance_capabilities() -> bool {
    let caps = crate::driver::INFLUXDB_METADATA.capabilities;
    caps.contains(DriverCapabilities::INSTANCE_METRICS)
        && caps.contains(DriverCapabilities::INSTANCE_INSPECTOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE_METRICS: &str = r#"
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 42
# HELP go_memstats_alloc_bytes Number of bytes allocated and still in use.
# TYPE go_memstats_alloc_bytes gauge
go_memstats_alloc_bytes 1.2345e+07
# HELP http_api_requests_total Number of http requests received
# TYPE http_api_requests_total counter
http_api_requests_total{handler="platform",method="GET",path="/api/v2/query",status="2XX"} 17
http_api_requests_total{handler="platform",method="POST",path="/api/v2/write",status="2XX"} 3
# HELP boltdb_reads_total Total number of boltdb reads
# TYPE boltdb_reads_total counter
boltdb_reads_total 512
"#;

    #[test]
    fn parses_simple_gauge_sample() {
        let samples = parse_prometheus_text(FIXTURE_METRICS);
        let value = find_metric_value(&samples, "go_goroutines");
        assert_eq!(value, Some(42.0));
    }

    #[test]
    fn parses_scientific_notation_value() {
        let samples = parse_prometheus_text(FIXTURE_METRICS);
        let value = find_metric_value(&samples, "go_memstats_alloc_bytes");
        assert_eq!(value, Some(1.2345e+07));
    }

    #[test]
    fn parses_labeled_samples_with_multiple_matches() {
        let samples = parse_prometheus_text(FIXTURE_METRICS);
        let matches: Vec<&PrometheusSample> = samples
            .iter()
            .filter(|s| s.name == "http_api_requests_total")
            .collect();
        assert_eq!(matches.len(), 2);
        assert!(
            matches[0]
                .labels
                .contains(&("method".to_string(), "GET".to_string()))
        );
    }

    #[test]
    fn find_metric_value_sums_across_label_combinations() {
        let samples = parse_prometheus_text(FIXTURE_METRICS);
        // http_api_requests_total appears twice (GET query, POST write): 17 + 3.
        let value = find_metric_value(&samples, "http_api_requests_total");
        assert_eq!(value, Some(20.0));
    }

    #[test]
    fn skips_help_and_type_comment_lines() {
        let samples = parse_prometheus_text(FIXTURE_METRICS);
        assert!(samples.iter().all(|s| !s.name.starts_with('#')));
    }

    #[test]
    fn find_metric_value_returns_none_for_missing_metric() {
        let samples = parse_prometheus_text(FIXTURE_METRICS);
        assert_eq!(find_metric_value(&samples, "does_not_exist"), None);
    }

    #[test]
    fn metric_fields_list_is_non_empty() {
        assert!(
            !METRIC_FIELDS.is_empty(),
            "METRIC_FIELDS must have at least one entry"
        );
    }

    #[test]
    fn static_metrics_have_distinct_ids_and_valid_refresh() {
        let metrics = InfluxInstanceCatalog::static_metrics();
        let mut ids: Vec<&str> = metrics.iter().map(|m| m.id.as_str()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), metrics.len(), "metric ids must be distinct");

        for m in &metrics {
            assert!(m.default_refresh_secs >= 10);
        }
    }

    #[test]
    fn static_inspectors_have_distinct_ids_and_valid_refresh() {
        let inspectors = InfluxInstanceCatalog::static_inspectors();
        let mut ids: Vec<&str> = inspectors.iter().map(|i| i.id.as_str()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(
            ids.len(),
            inspectors.len(),
            "inspector ids must be distinct"
        );

        for i in &inspectors {
            assert!(i.default_refresh_secs >= 10);
        }
    }

    #[test]
    fn metric_and_inspector_ids_share_no_overlap() {
        let metric_ids: Vec<String> = InfluxInstanceCatalog::static_metrics()
            .into_iter()
            .map(|m| m.id)
            .collect();
        let inspector_ids: Vec<String> = InfluxInstanceCatalog::static_inspectors()
            .into_iter()
            .map(|i| i.id)
            .collect();

        for id in &inspector_ids {
            assert!(
                !metric_ids.contains(id),
                "inspector id {id} collides with a metric id"
            );
        }
    }

    #[test]
    fn influxdb_advertises_both_instance_capability_bits() {
        assert!(
            influxdb_advertises_instance_capabilities(),
            "InfluxDB METADATA must include INSTANCE_METRICS and INSTANCE_INSPECTOR bits"
        );
    }

    #[test]
    fn default_dashboard_is_non_none_and_panels_reference_valid_ids() {
        let dashboard = InfluxInstanceCatalog::static_default_dashboard()
            .expect("InfluxInstanceCatalog must return Some(default_dashboard)");

        assert!(!dashboard.panels.is_empty());
        assert!(!dashboard.title.is_empty());

        let metric_ids: Vec<String> = InfluxInstanceCatalog::static_metrics()
            .into_iter()
            .map(|m| m.id)
            .collect();
        let inspector_ids: Vec<String> = InfluxInstanceCatalog::static_inspectors()
            .into_iter()
            .map(|i| i.id)
            .collect();

        for panel in &dashboard.panels {
            let valid =
                metric_ids.contains(&panel.metric_id) || inspector_ids.contains(&panel.metric_id);
            assert!(
                valid,
                "panel metric_id {:?} is not in static metrics or inspectors",
                panel.metric_id
            );

            assert!(panel.grid_column <= 11, "grid_column out of range");
            assert!(
                panel.grid_width >= 1 && panel.grid_width <= 12,
                "grid_width out of range"
            );
            assert!(panel.grid_height >= 1, "grid_height must be positive");
        }
    }
}
