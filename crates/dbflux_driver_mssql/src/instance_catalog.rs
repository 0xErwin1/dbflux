use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dbflux_core::{
    ColumnKind, ColumnMeta, DbError, DefaultDashboardPanel, DefaultInstanceDashboard,
    DriverCapabilities, InstanceCatalog, InstanceInspectorDef, InstanceMetricDef,
    InstanceMetricUnit, QueryResult, QueryResultShape, Row, Value,
};

use crate::driver::MssqlConnectionInner;

/// Curated list of `sys.dm_os_performance_counters` entries that map to chartable metrics.
///
/// Each entry: `(counter_name, instance_name, metric_id, display_name, group, unit)`.
/// `instance_name` is often `""` (empty) for instance-wide counters or a database name.
/// Counter names in `PERFORMANCE_COUNTERS` that are ratio types in
/// `sys.dm_os_performance_counters`. For these, SQL Server stores the numerator
/// in the main row and the denominator in a paired `<name> base` row; the
/// charted value must be `(numerator / denominator) * 100.0`.
pub const RATIO_COUNTER_NAMES: &[&str] = &["Buffer cache hit ratio"];

pub const PERFORMANCE_COUNTERS: &[(&str, &str, &str, &str, &str, InstanceMetricUnit)] = &[
    (
        "Batch Requests/sec",
        "",
        "mssql.batch_requests_per_sec",
        "Batch requests / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "SQL Compilations/sec",
        "",
        "mssql.compilations_per_sec",
        "SQL compilations / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "SQL Re-Compilations/sec",
        "",
        "mssql.recompilations_per_sec",
        "SQL re-compilations / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "User Connections",
        "",
        "mssql.user_connections",
        "User connections",
        "Connections",
        InstanceMetricUnit::Count,
    ),
    (
        "Lock Waits/sec",
        "_Total",
        "mssql.lock_waits_per_sec",
        "Lock waits / sec",
        "Locks",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "Page reads/sec",
        "",
        "mssql.page_reads_per_sec",
        "Page reads / sec",
        "I/O",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "Page writes/sec",
        "",
        "mssql.page_writes_per_sec",
        "Page writes / sec",
        "I/O",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "Buffer cache hit ratio",
        "",
        "mssql.buffer_cache_hit_ratio",
        "Buffer cache hit ratio",
        "Cache",
        InstanceMetricUnit::Percent,
    ),
    (
        "Total Server Memory (KB)",
        "",
        "mssql.server_memory_kb",
        "Total server memory (KB)",
        "Memory",
        InstanceMetricUnit::Bytes,
    ),
];

pub struct MssqlInstanceCatalog {
    inner: Arc<Mutex<MssqlConnectionInner>>,
    view_server_state_available: bool,
}

impl MssqlInstanceCatalog {
    pub(crate) fn new(
        inner: Arc<Mutex<MssqlConnectionInner>>,
        view_server_state_available: bool,
    ) -> Self {
        Self {
            inner,
            view_server_state_available,
        }
    }

    pub fn static_metrics() -> Vec<InstanceMetricDef> {
        PERFORMANCE_COUNTERS
            .iter()
            .map(|(_, _, id, display_name, group, unit)| InstanceMetricDef {
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
            id: "mssql.active_sessions".to_string(),
            display_name: "Active sessions".to_string(),
            description: Some(
                "Current sessions and requests from sys.dm_exec_sessions \
                 joined with sys.dm_exec_requests."
                    .to_string(),
            ),
            default_refresh_secs: 10,
        }]
    }

    /// Curated "Instance Overview" dashboard layout for SQL Server.
    ///
    /// Row 0: batch requests/sec (cols 0-5) | user connections (cols 6-11)
    /// Row 1: lock waits/sec (cols 0-5) | buffer cache hit ratio (cols 6-11)
    /// Row 2: active sessions inspector (full width)
    pub fn static_default_dashboard() -> Option<DefaultInstanceDashboard> {
        Some(DefaultInstanceDashboard {
            title: "SQL Server Instance Overview".to_string(),
            description: Some(
                "Curated SQL Server performance counters and active-sessions inspector."
                    .to_string(),
            ),
            panels: vec![
                DefaultDashboardPanel {
                    metric_id: "mssql.batch_requests_per_sec".to_string(),
                    is_inspector: false,
                    grid_column: 0,
                    grid_row: 0,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "mssql.user_connections".to_string(),
                    is_inspector: false,
                    grid_column: 6,
                    grid_row: 0,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "mssql.lock_waits_per_sec".to_string(),
                    is_inspector: false,
                    grid_column: 0,
                    grid_row: 3,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "mssql.buffer_cache_hit_ratio".to_string(),
                    is_inspector: false,
                    grid_column: 6,
                    grid_row: 3,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "mssql.active_sessions".to_string(),
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
    pub fn static_row_actions(metric_id: &str) -> Vec<dbflux_core::InspectorRowAction> {
        match metric_id {
            "mssql.active_sessions" => vec![dbflux_core::InspectorRowAction {
                id: "kill".to_string(),
                label: "Kill session".to_string(),
                description: Some(
                    "Executes KILL <session_id> to terminate the selected SQL Server session."
                        .to_string(),
                ),
                is_destructive: true,
            }],
            _ => Vec::new(),
        }
    }

    /// Returns `true` if the connection has `VIEW SERVER STATE` permission.
    ///
    /// Called once at catalog construction time. When permission is absent, the
    /// catalog returns empty metric and inspector lists rather than failing.
    pub(crate) fn probe_view_server_state(inner: &mut MssqlConnectionInner) -> bool {
        let sql = "SELECT HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE')";

        matches!(
            inner.runtime.block_on(async {
                let client = inner.client.as_mut()?;
                let result = client.simple_query(sql).await.ok()?;
                let row = result.into_row().await.ok().flatten()?;
                let val: Option<i32> = row.get(0);
                val
            }),
            Some(1)
        )
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
        type_name: "bigint".to_string(),
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

fn text_col_nullable(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        kind: ColumnKind::Text,
        type_name: "nvarchar".to_string(),
        nullable: true,
        is_primary_key: false,
    }
}

fn tiberius_error(e: tiberius::error::Error) -> DbError {
    DbError::QueryFailed(e.to_string().into())
}

fn kill_session(inner: &mut MssqlConnectionInner, sql: &str) -> Result<(), DbError> {
    let sql = sql.to_string();
    inner.runtime.block_on(async {
        let client = inner
            .client
            .as_mut()
            .ok_or_else(|| DbError::QueryFailed("no active client".to_string().into()))?;
        client
            .simple_query(sql.as_str())
            .await
            .map_err(tiberius_error)?;
        Ok::<(), DbError>(())
    })
}

#[async_trait]
impl InstanceCatalog for MssqlInstanceCatalog {
    async fn list_metrics(&self) -> Result<Vec<InstanceMetricDef>, DbError> {
        if !self.view_server_state_available {
            log::warn!("[MSSQL] VIEW SERVER STATE permission absent; instance metrics unavailable");
            return Ok(Vec::new());
        }

        Ok(Self::static_metrics())
    }

    async fn list_inspectors(&self) -> Result<Vec<InstanceInspectorDef>, DbError> {
        if !self.view_server_state_available {
            return Ok(Vec::new());
        }

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
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| DbError::QueryFailed("mssql inner mutex poisoned".to_string().into()))?;

        dispatch_metric_series(&mut inner, metric_id)
    }

    async fn fetch_inspector_snapshot(&self, metric_id: &str) -> Result<QueryResult, DbError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| DbError::QueryFailed("mssql inner mutex poisoned".to_string().into()))?;

        dispatch_inspector_snapshot(&mut inner, metric_id)
    }

    fn row_actions(&self, metric_id: &str) -> Vec<dbflux_core::InspectorRowAction> {
        if !self.view_server_state_available {
            return Vec::new();
        }
        Self::static_row_actions(metric_id)
    }

    async fn execute_row_action(
        &self,
        metric_id: &str,
        action_id: &str,
        row_values: &[dbflux_core::Value],
    ) -> Result<(), DbError> {
        if metric_id == "mssql.active_sessions" && action_id == "kill" {
            let session_id: i64 = match row_values.first() {
                Some(dbflux_core::Value::Int(n)) => *n,
                Some(dbflux_core::Value::Text(s)) => s.trim().parse().map_err(|_| {
                    DbError::QueryFailed(
                        format!(
                            "mssql.active_sessions kill: session_id '{s}' is not a valid integer"
                        )
                        .into(),
                    )
                })?,
                _ => {
                    return Err(DbError::QueryFailed(
                        "mssql.active_sessions kill: could not read session_id from row"
                            .to_string()
                            .into(),
                    ));
                }
            };

            if !(1..=32767).contains(&session_id) {
                return Err(DbError::QueryFailed(
                    format!(
                        "mssql.active_sessions kill: session_id {session_id} out of valid range [1..32767]"
                    )
                    .into(),
                ));
            }

            let sql = format!("KILL {session_id}");

            let mut inner = self.inner.lock().map_err(|_| {
                DbError::QueryFailed("mssql inner mutex poisoned".to_string().into())
            })?;

            kill_session(&mut inner, &sql)?;

            return Ok(());
        }

        Err(DbError::NotSupported(format!(
            "row action '{action_id}' not supported for inspector '{metric_id}'"
        )))
    }
}

pub(crate) fn dispatch_metric_series(
    inner: &mut MssqlConnectionInner,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    let entry = PERFORMANCE_COUNTERS
        .iter()
        .find(|(_, _, id, _, _, _)| *id == metric_id);

    match entry {
        Some((counter_name, instance_name, _, display_name, _, _)) => {
            fetch_performance_counter(inner, counter_name, instance_name, display_name)
        }
        None => Err(DbError::NotSupported(format!(
            "unknown instance metric: {metric_id}"
        ))),
    }
}

pub(crate) fn dispatch_inspector_snapshot(
    inner: &mut MssqlConnectionInner,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    match metric_id {
        "mssql.active_sessions" => fetch_active_sessions(inner),
        other => Err(DbError::NotSupported(format!("unknown inspector: {other}"))),
    }
}

/// Doubles any single-quote in `s` for safe interpolation into an MSSQL
/// N'...' literal. Tiberius `simple_query` does not support parameter binding,
/// so values must be escaped before interpolation.
fn escape_mssql_literal(s: &str) -> String {
    s.replace('\'', "''")
}

fn fetch_performance_counter(
    inner: &mut MssqlConnectionInner,
    counter_name: &str,
    instance_name: &str,
    display_name: &str,
) -> Result<QueryResult, DbError> {
    debug_assert!(
        PERFORMANCE_COUNTERS
            .iter()
            .any(|(cn, inst, _, _, _, _)| *cn == counter_name && *inst == instance_name),
        "fetch_performance_counter called with values not in PERFORMANCE_COUNTERS — \
         ensure callers are not passing external input"
    );

    let is_ratio = RATIO_COUNTER_NAMES.contains(&counter_name);
    let ec = escape_mssql_literal(counter_name);
    let ei = escape_mssql_literal(instance_name);

    let value = if is_ratio {
        fetch_ratio_counter(inner, &ec, &ei, counter_name)?
    } else {
        let sql = format!(
            "SELECT CAST(cntr_value AS float) \
             FROM sys.dm_os_performance_counters \
             WHERE counter_name = N'{ec}' \
               AND (instance_name = N'{ei}' OR N'{ei}' = '' AND instance_name = '')"
        );
        inner.runtime.block_on(async {
            let client = inner
                .client
                .as_mut()
                .ok_or_else(|| DbError::QueryFailed("no active client".to_string().into()))?;
            let stream = client.simple_query(&sql).await.map_err(tiberius_error)?;
            let row_opt = stream.into_row().await.map_err(tiberius_error)?;
            Ok::<f64, DbError>(row_opt.and_then(|r| r.get::<f64, _>(0)).unwrap_or(0.0))
        })?
    };

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

/// Fetches a ratio-type performance counter as a percentage.
///
/// Queries both the numerator row and the `<name> base` row in a single
/// statement, then returns `(numerator / base) * 100.0`. Returns `0.0` and
/// logs a warning when the base row is missing or zero.
fn fetch_ratio_counter(
    inner: &mut MssqlConnectionInner,
    escaped_counter: &str,
    escaped_instance: &str,
    raw_counter_name: &str,
) -> Result<f64, DbError> {
    let base_name = format!("{raw_counter_name} base");
    let eb = escape_mssql_literal(&base_name);

    let sql = format!(
        "SELECT counter_name, CAST(cntr_value AS float) \
         FROM sys.dm_os_performance_counters \
         WHERE counter_name IN (N'{escaped_counter}', N'{eb}') \
           AND (instance_name = N'{escaped_instance}' OR N'{escaped_instance}' = '' AND instance_name = '')"
    );

    let (numerator, base) = inner.runtime.block_on(async {
        let client = inner
            .client
            .as_mut()
            .ok_or_else(|| DbError::QueryFailed("no active client".to_string().into()))?;

        let results = client
            .simple_query(&sql)
            .await
            .map_err(tiberius_error)?
            .into_results()
            .await
            .map_err(tiberius_error)?;

        let mut num: Option<f64> = None;
        let mut den: Option<f64> = None;

        for row in results.into_iter().flatten() {
            let name: Option<&str> = row.get(0);
            let val: Option<f64> = row.get(1);
            if name == Some(raw_counter_name) {
                num = val;
            } else {
                den = val;
            }
        }

        Ok::<(Option<f64>, Option<f64>), DbError>((num, den))
    })?;

    match (numerator, base) {
        (Some(n), Some(b)) if b != 0.0 => Ok((n / b) * 100.0),
        (Some(_n), Some(_b)) => {
            log::warn!("mssql ratio counter '{raw_counter_name}': base is zero, returning 0.0");
            Ok(0.0)
        }
        _ => {
            log::warn!("mssql ratio counter '{raw_counter_name}': base row missing, returning 0.0");
            Ok(0.0)
        }
    }
}

fn fetch_active_sessions(inner: &mut MssqlConnectionInner) -> Result<QueryResult, DbError> {
    let sql = "SELECT s.session_id, s.login_name, s.host_name, s.program_name, \
                      s.status, s.cpu_time, s.memory_usage, \
                      r.command, LEFT(r.status, 50) AS req_status, \
                      r.wait_type, r.wait_time, r.blocking_session_id \
               FROM sys.dm_exec_sessions s \
               LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id \
               WHERE s.is_user_process = 1 \
               ORDER BY s.cpu_time DESC";

    let columns = vec![
        ColumnMeta {
            name: "session_id".to_string(),
            kind: ColumnKind::Integer,
            type_name: "smallint".to_string(),
            nullable: false,
            is_primary_key: false,
        },
        text_col_nullable("login_name"),
        text_col_nullable("host_name"),
        text_col_nullable("program_name"),
        text_col_nullable("status"),
        ColumnMeta {
            name: "cpu_time".to_string(),
            kind: ColumnKind::Integer,
            type_name: "int".to_string(),
            nullable: false,
            is_primary_key: false,
        },
        ColumnMeta {
            name: "memory_usage".to_string(),
            kind: ColumnKind::Integer,
            type_name: "int".to_string(),
            nullable: false,
            is_primary_key: false,
        },
        text_col_nullable("command"),
        text_col_nullable("req_status"),
        text_col_nullable("wait_type"),
        ColumnMeta {
            name: "wait_time".to_string(),
            kind: ColumnKind::Integer,
            type_name: "int".to_string(),
            nullable: true,
            is_primary_key: false,
        },
        ColumnMeta {
            name: "blocking_session_id".to_string(),
            kind: ColumnKind::Integer,
            type_name: "smallint".to_string(),
            nullable: true,
            is_primary_key: false,
        },
    ];

    let rows = inner.runtime.block_on(async {
        let client = inner
            .client
            .as_mut()
            .ok_or_else(|| DbError::QueryFailed("no active client".to_string().into()))?;

        let stream = client.simple_query(sql).await.map_err(tiberius_error)?;
        let result_set = stream.into_results().await.map_err(tiberius_error)?;

        let mut rows: Vec<Row> = Vec::new();
        for tib_row in result_set.into_iter().flatten() {
            let row: Row = vec![
                tib_row
                    .get::<i16, _>(0)
                    .map(|v| Value::Int(v as i64))
                    .unwrap_or(Value::Null),
                tib_row
                    .get::<&str, _>(1)
                    .map(|v| Value::Text(v.to_string()))
                    .unwrap_or(Value::Null),
                tib_row
                    .get::<&str, _>(2)
                    .map(|v| Value::Text(v.to_string()))
                    .unwrap_or(Value::Null),
                tib_row
                    .get::<&str, _>(3)
                    .map(|v| Value::Text(v.to_string()))
                    .unwrap_or(Value::Null),
                tib_row
                    .get::<&str, _>(4)
                    .map(|v| Value::Text(v.to_string()))
                    .unwrap_or(Value::Null),
                tib_row
                    .get::<i32, _>(5)
                    .map(|v| Value::Int(v as i64))
                    .unwrap_or(Value::Null),
                tib_row
                    .get::<i32, _>(6)
                    .map(|v| Value::Int(v as i64))
                    .unwrap_or(Value::Null),
                tib_row
                    .get::<&str, _>(7)
                    .map(|v| Value::Text(v.to_string()))
                    .unwrap_or(Value::Null),
                tib_row
                    .get::<&str, _>(8)
                    .map(|v| Value::Text(v.to_string()))
                    .unwrap_or(Value::Null),
                tib_row
                    .get::<&str, _>(9)
                    .map(|v| Value::Text(v.to_string()))
                    .unwrap_or(Value::Null),
                tib_row
                    .get::<i32, _>(10)
                    .map(|v| Value::Int(v as i64))
                    .unwrap_or(Value::Null),
                tib_row
                    .get::<i16, _>(11)
                    .map(|v| Value::Int(v as i64))
                    .unwrap_or(Value::Null),
            ];
            rows.push(row);
        }

        Ok::<Vec<Row>, DbError>(rows)
    })?;

    Ok(QueryResult {
        shape: QueryResultShape::Table,
        columns,
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

/// Returns `true` if SQL Server METADATA advertises both instance-metrics bits.
pub fn mssql_advertises_instance_capabilities() -> bool {
    let caps = crate::METADATA.capabilities;
    caps.contains(DriverCapabilities::INSTANCE_METRICS)
        && caps.contains(DriverCapabilities::INSTANCE_INSPECTOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// K2: single-quote characters must be doubled to prevent SQL injection via
    /// interpolation into Tiberius `simple_query` literals.
    #[test]
    fn escape_mssql_literal_doubles_single_quotes() {
        assert_eq!(escape_mssql_literal("it's"), "it''s");
        assert_eq!(escape_mssql_literal("no quotes"), "no quotes");
        assert_eq!(escape_mssql_literal("a'b'c"), "a''b''c");
        assert_eq!(escape_mssql_literal(""), "");
    }

    #[test]
    fn performance_counters_list_is_non_empty() {
        assert!(
            !PERFORMANCE_COUNTERS.is_empty(),
            "PERFORMANCE_COUNTERS must have at least one entry"
        );
    }

    #[test]
    fn static_metrics_ids_are_lowercase_dot_separated() {
        let metrics = MssqlInstanceCatalog::static_metrics();
        for m in &metrics {
            let valid = !m.id.is_empty()
                && m.id
                    .chars()
                    .next()
                    .map(|c| c.is_ascii_lowercase())
                    .unwrap_or(false)
                && m.id
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_');
            assert!(valid, "metric id {:?} must match [a-z][a-z0-9_.]*", m.id);
        }
    }

    #[test]
    fn static_metric_default_refresh_secs_at_or_above_floor() {
        let metrics = MssqlInstanceCatalog::static_metrics();
        for m in &metrics {
            assert!(m.default_refresh_secs >= 10);
        }
    }

    #[test]
    fn static_inspectors_list_is_non_empty() {
        let inspectors = MssqlInstanceCatalog::static_inspectors();
        assert!(!inspectors.is_empty());
    }

    #[test]
    fn mssql_advertises_both_instance_capability_bits() {
        assert!(
            mssql_advertises_instance_capabilities(),
            "SQL Server METADATA must include INSTANCE_METRICS and INSTANCE_INSPECTOR bits"
        );
    }

    /// REQ-DRIVER-MSSQL-1 / WARN-2: when `view_server_state_available` is `false`,
    /// both `list_metrics` and `list_inspectors` return empty vectors rather than
    /// erroring. This exercises the permission-denied guard path without a live
    /// SQL Server connection.
    ///
    /// Uses a dedicated tokio runtime so the test is synchronous and does not
    /// conflict with any outer async executor. The `Runtime` is moved into
    /// `MssqlConnectionInner` (as the driver normally does), and the catalog is
    /// invoked via a separate single-thread runtime so dropping the inner runtime
    /// does not occur inside its own async context.
    #[test]
    fn list_metrics_and_inspectors_return_empty_when_probe_fails() {
        use crate::driver::MssqlConnectionInner;
        use std::sync::{Arc, Mutex};
        use tokio::runtime::Runtime;

        let inner_rt = Runtime::new().expect("tokio runtime for dummy inner");
        let inner = Arc::new(Mutex::new(MssqlConnectionInner {
            client: None,
            runtime: inner_rt,
        }));
        let catalog = MssqlInstanceCatalog::new(inner, false);

        let exec_rt = Runtime::new().expect("tokio runtime for test execution");
        let (metrics, inspectors) = exec_rt.block_on(async {
            let metrics = catalog.list_metrics().await.expect("must not error");
            let inspectors = catalog.list_inspectors().await.expect("must not error");
            (metrics, inspectors)
        });

        assert!(
            metrics.is_empty(),
            "metrics must be empty when VIEW SERVER STATE permission is absent"
        );
        assert!(
            inspectors.is_empty(),
            "inspectors must be empty when VIEW SERVER STATE permission is absent"
        );
    }

    /// BF8: mssql.active_sessions inspector must advertise exactly one kill action.
    #[test]
    fn mssql_row_actions_active_sessions_returns_kill() {
        let actions = MssqlInstanceCatalog::static_row_actions("mssql.active_sessions");
        assert_eq!(
            actions.len(),
            1,
            "mssql.active_sessions must have exactly one row action"
        );
        assert_eq!(actions[0].id, "kill");
        assert!(actions[0].is_destructive);
    }

    /// BF8: unknown inspector must return no row actions.
    #[test]
    fn mssql_row_actions_unknown_returns_empty() {
        let actions = MssqlInstanceCatalog::static_row_actions("mssql.does_not_exist");
        assert!(
            actions.is_empty(),
            "unknown inspector must return no row actions"
        );
    }

    /// BF7: MssqlInstanceCatalog must return a non-None default dashboard with
    /// panels that reference valid metric or inspector IDs.
    #[test]
    fn mssql_default_dashboard_is_non_none_and_valid() {
        use dbflux_core::DefaultInstanceDashboard;

        let dashboard: Option<DefaultInstanceDashboard> =
            MssqlInstanceCatalog::static_default_dashboard();

        let dashboard =
            dashboard.expect("MssqlInstanceCatalog must return Some(default_dashboard)");
        assert!(
            !dashboard.panels.is_empty(),
            "default dashboard must have at least one panel"
        );
        assert!(
            !dashboard.title.is_empty(),
            "default dashboard must have a non-empty title"
        );

        let metric_ids: Vec<String> = MssqlInstanceCatalog::static_metrics()
            .into_iter()
            .map(|m| m.id)
            .collect();
        let inspector_ids: Vec<String> = MssqlInstanceCatalog::static_inspectors()
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
        }
    }
}
