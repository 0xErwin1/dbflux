use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dbflux_core::{
    ColumnKind, ColumnMeta, DbError, DefaultDashboardPanel, DefaultInstanceDashboard,
    DriverCapabilities, InstanceCatalog, InstanceInspectorDef, InstanceMetricDef,
    InstanceMetricUnit, QueryResult, QueryResultShape, Row, Value,
};
use mysql::{Conn, prelude::Queryable};

/// Curated list of `SHOW GLOBAL STATUS` variable names that map to chartable metrics.
///
/// Each entry is `(status_var_name, metric_id, display_name, group, unit)`.
pub const PIVOTED_COUNTERS: &[(&str, &str, &str, &str, InstanceMetricUnit)] = &[
    (
        "Queries",
        "mysql.queries_per_sec",
        "Queries / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "Com_select",
        "mysql.selects_per_sec",
        "SELECT / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "Com_insert",
        "mysql.inserts_per_sec",
        "INSERT / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "Com_update",
        "mysql.updates_per_sec",
        "UPDATE / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "Com_delete",
        "mysql.deletes_per_sec",
        "DELETE / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "Innodb_buffer_pool_read_requests",
        "mysql.buffer_pool_reads",
        "InnoDB buffer pool reads",
        "Cache",
        InstanceMetricUnit::Count,
    ),
    (
        "Innodb_buffer_pool_reads",
        "mysql.buffer_pool_disk_reads",
        "InnoDB disk reads",
        "Cache",
        InstanceMetricUnit::Count,
    ),
    (
        "Threads_connected",
        "mysql.threads_connected",
        "Connected threads",
        "Connections",
        InstanceMetricUnit::Count,
    ),
    (
        "Threads_running",
        "mysql.threads_running",
        "Running threads",
        "Connections",
        InstanceMetricUnit::Count,
    ),
    (
        "Bytes_received",
        "mysql.bytes_received",
        "Bytes received",
        "Network",
        InstanceMetricUnit::Bytes,
    ),
    (
        "Bytes_sent",
        "mysql.bytes_sent",
        "Bytes sent",
        "Network",
        InstanceMetricUnit::Bytes,
    ),
];

pub struct MysqlInstanceCatalog {
    conn: Arc<Mutex<Conn>>,
    #[allow(dead_code)]
    performance_schema_available: bool,
    process_privilege: bool,
    connection_admin: bool,
}

impl MysqlInstanceCatalog {
    pub fn new(conn: Arc<Mutex<Conn>>, performance_schema_available: bool) -> Self {
        Self {
            conn,
            performance_schema_available,
            process_privilege: false,
            connection_admin: false,
        }
    }

    /// Constructs a catalog with pre-probed privilege flags.
    pub fn new_probed(
        conn: Arc<Mutex<Conn>>,
        performance_schema_available: bool,
        process_privilege: bool,
        connection_admin: bool,
    ) -> Self {
        Self {
            conn,
            performance_schema_available,
            process_privilege,
            connection_admin,
        }
    }

    pub fn static_metrics() -> Vec<InstanceMetricDef> {
        PIVOTED_COUNTERS
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
        vec![InstanceInspectorDef {
            id: "mysql.processlist".to_string(),
            display_name: "Process list".to_string(),
            description: Some(
                "Current connections and their query state from information_schema.PROCESSLIST."
                    .to_string(),
            ),
            default_refresh_secs: 10,
        }]
    }

    /// Curated "Instance Overview" dashboard layout for MySQL.
    ///
    /// Row 0: queries/sec (cols 0-5) | threads connected (cols 6-11)
    /// Row 1: threads running (cols 0-5) | buffer pool reads (cols 6-11)
    /// Row 2: process list inspector (full width)
    pub fn static_default_dashboard() -> Option<DefaultInstanceDashboard> {
        Some(DefaultInstanceDashboard {
            title: "MySQL Instance Overview".to_string(),
            description: Some(
                "Curated MySQL instance metrics and process-list inspector.".to_string(),
            ),
            panels: vec![
                DefaultDashboardPanel {
                    metric_id: "mysql.queries_per_sec".to_string(),
                    is_inspector: false,
                    grid_column: 0,
                    grid_row: 0,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "mysql.threads_connected".to_string(),
                    is_inspector: false,
                    grid_column: 6,
                    grid_row: 0,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "mysql.threads_running".to_string(),
                    is_inspector: false,
                    grid_column: 0,
                    grid_row: 3,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "mysql.buffer_pool_reads".to_string(),
                    is_inspector: false,
                    grid_column: 6,
                    grid_row: 3,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "mysql.processlist".to_string(),
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
            "mysql.processlist" => vec![dbflux_core::InspectorRowAction {
                id: "kill".to_string(),
                label: "Kill process".to_string(),
                description: Some(
                    "Executes KILL <id> to terminate the selected connection.".to_string(),
                ),
                is_destructive: true,
            }],
            _ => Vec::new(),
        }
    }

    pub fn probe_performance_schema(conn: &mut Conn) -> bool {
        conn.query_first::<String, _>(
            "SELECT 'ok' FROM information_schema.SCHEMATA WHERE schema_name = 'performance_schema'",
        )
        .unwrap_or(None)
        .is_some()
    }

    /// Returns `true` if the current user has the `PROCESS` privilege or equivalent.
    ///
    /// Without it, `INFORMATION_SCHEMA.PROCESSLIST` only shows the user's own
    /// threads, making the inspector misleading for monitoring. We hide it entirely.
    pub(crate) fn probe_process_privilege(conn: &mut Conn) -> bool {
        let grants: Vec<String> = conn
            .query("SHOW GRANTS FOR CURRENT_USER()")
            .unwrap_or_default();

        grants.iter().any(|g| {
            let upper = g.to_uppercase();
            upper.contains("ALL PRIVILEGES") || upper.contains("PROCESS")
        })
    }

    /// Returns `true` if the current user has `CONNECTION_ADMIN`, `SUPER`, or
    /// equivalent privileges required to execute `KILL`.
    pub(crate) fn probe_connection_admin(conn: &mut Conn) -> bool {
        let grants: Vec<String> = conn
            .query("SHOW GRANTS FOR CURRENT_USER()")
            .unwrap_or_default();

        grants.iter().any(|g| {
            let upper = g.to_uppercase();
            upper.contains("ALL PRIVILEGES")
                || upper.contains("CONNECTION_ADMIN")
                || upper.contains("SUPER")
        })
    }

    /// Returns inspectors filtered by the process_privilege probe result.
    pub fn inspectors_with_probes(process_privilege: bool) -> Vec<InstanceInspectorDef> {
        if process_privilege {
            Self::static_inspectors()
        } else {
            Vec::new()
        }
    }

    /// Returns row actions for the given inspector, gated by the
    /// connection_admin privilege probe.
    pub fn row_actions_with_probes(
        metric_id: &str,
        connection_admin: bool,
    ) -> Vec<dbflux_core::InspectorRowAction> {
        if metric_id == "mysql.processlist" && connection_admin {
            Self::static_row_actions(metric_id)
        } else {
            Vec::new()
        }
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
        type_name: "double".to_string(),
        nullable: false,
        is_primary_key: false,
    }
}

fn text_col_nullable(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        kind: ColumnKind::Text,
        type_name: "varchar".to_string(),
        nullable: true,
        is_primary_key: false,
    }
}

fn single_sample_result(columns: Vec<ColumnMeta>, values: Vec<Value>) -> QueryResult {
    let mut row: Row = vec![Value::Int(now_epoch_ms())];
    row.extend(values);

    QueryResult {
        shape: QueryResultShape::Table,
        columns,
        rows: vec![row],
        affected_rows: None,
        execution_time: Duration::ZERO,
        text_body: None,
        raw_bytes: None,
        next_page_token: None,
        resolved_window: None,
        metadata_extra: None,
        additional_results: Vec::new(),
    }
}

fn mysql_error(e: mysql::Error) -> DbError {
    DbError::QueryFailed(e.to_string().into())
}

#[async_trait]
impl InstanceCatalog for MysqlInstanceCatalog {
    async fn list_metrics(&self) -> Result<Vec<InstanceMetricDef>, DbError> {
        Ok(Self::static_metrics())
    }

    async fn list_inspectors(&self) -> Result<Vec<InstanceInspectorDef>, DbError> {
        Ok(Self::inspectors_with_probes(self.process_privilege))
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
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| DbError::QueryFailed("mysql conn mutex poisoned".to_string().into()))?;

        dispatch_metric_series(&mut conn, metric_id)
    }

    async fn fetch_inspector_snapshot(&self, metric_id: &str) -> Result<QueryResult, DbError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| DbError::QueryFailed("mysql conn mutex poisoned".to_string().into()))?;

        dispatch_inspector_snapshot(&mut conn, metric_id)
    }

    fn row_actions(&self, metric_id: &str) -> Vec<dbflux_core::InspectorRowAction> {
        Self::row_actions_with_probes(metric_id, self.connection_admin)
    }

    async fn execute_row_action(
        &self,
        metric_id: &str,
        action_id: &str,
        row_values: &[dbflux_core::Value],
    ) -> Result<(), DbError> {
        if metric_id == "mysql.processlist" && action_id == "kill" {
            let id: u64 = match row_values.first() {
                Some(dbflux_core::Value::Int(n)) if *n > 0 => *n as u64,
                Some(dbflux_core::Value::Int(n)) => {
                    return Err(DbError::QueryFailed(
                        format!("mysql.processlist kill: id {n} is not a positive integer").into(),
                    ));
                }
                Some(dbflux_core::Value::Text(s)) => s.trim().parse().map_err(|_| {
                    DbError::QueryFailed(
                        format!("mysql.processlist kill: id '{s}' is not a valid integer").into(),
                    )
                })?,
                _ => {
                    return Err(DbError::QueryFailed(
                        "mysql.processlist kill: could not read id from row"
                            .to_string()
                            .into(),
                    ));
                }
            };

            let mut conn = self.conn.lock().map_err(|_| {
                DbError::QueryFailed("mysql conn mutex poisoned".to_string().into())
            })?;

            use mysql::prelude::Queryable;
            conn.exec_drop(format!("KILL {id}"), ())
                .map_err(mysql_error)?;

            return Ok(());
        }

        Err(DbError::NotSupported(format!(
            "row action '{action_id}' not supported for inspector '{metric_id}'"
        )))
    }
}

pub(crate) fn dispatch_metric_series(
    conn: &mut Conn,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    let counter = PIVOTED_COUNTERS
        .iter()
        .find(|(_, id, _, _, _)| *id == metric_id);

    match counter {
        Some((var_name, _, display_name, _, _)) => {
            fetch_global_status_value(conn, var_name, display_name)
        }
        None => Err(DbError::NotSupported(format!(
            "unknown instance metric: {metric_id}"
        ))),
    }
}

pub(crate) fn dispatch_inspector_snapshot(
    conn: &mut Conn,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    match metric_id {
        "mysql.processlist" => fetch_processlist(conn),
        other => Err(DbError::NotSupported(format!("unknown inspector: {other}"))),
    }
}

fn fetch_global_status_value(
    conn: &mut Conn,
    var_name: &str,
    display_name: &str,
) -> Result<QueryResult, DbError> {
    let row: Option<(String, String)> = conn
        .exec_first("SHOW GLOBAL STATUS LIKE ?", (var_name,))
        .map_err(mysql_error)?;

    let value = match row {
        Some((_, v)) => v.parse::<f64>().unwrap_or(0.0),
        None => 0.0,
    };

    Ok(single_sample_result(
        vec![timestamp_col("timestamp_ms"), float_col(display_name)],
        vec![Value::Float(value)],
    ))
}

fn fetch_processlist(conn: &mut Conn) -> Result<QueryResult, DbError> {
    let sql = "SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 200) AS INFO \
               FROM information_schema.PROCESSLIST \
               ORDER BY TIME DESC";

    let columns = vec![
        ColumnMeta {
            name: "id".to_string(),
            kind: ColumnKind::Integer,
            type_name: "bigint".to_string(),
            nullable: false,
            is_primary_key: false,
        },
        text_col_nullable("user"),
        text_col_nullable("host"),
        text_col_nullable("db"),
        text_col_nullable("command"),
        ColumnMeta {
            name: "time".to_string(),
            kind: ColumnKind::Integer,
            type_name: "bigint".to_string(),
            nullable: false,
            is_primary_key: false,
        },
        text_col_nullable("state"),
        text_col_nullable("info"),
    ];

    let rows: Vec<mysql::Row> = conn.query(sql).map_err(mysql_error)?;

    let result_rows: Vec<Row> = rows
        .iter()
        .map(|row| {
            vec![
                row.get::<Option<u64>, _>(0)
                    .flatten()
                    .map(|v| Value::Int(v as i64))
                    .unwrap_or(Value::Null),
                row.get::<Option<String>, _>(1)
                    .flatten()
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
                row.get::<Option<String>, _>(2)
                    .flatten()
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
                row.get::<Option<String>, _>(3)
                    .flatten()
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
                row.get::<Option<String>, _>(4)
                    .flatten()
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
                row.get::<Option<u64>, _>(5)
                    .flatten()
                    .map(|v| Value::Int(v as i64))
                    .unwrap_or(Value::Null),
                row.get::<Option<String>, _>(6)
                    .flatten()
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
                row.get::<Option<String>, _>(7)
                    .flatten()
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
            ]
        })
        .collect();

    Ok(QueryResult {
        shape: QueryResultShape::Table,
        columns,
        rows: result_rows,
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

/// Returns `true` if the MySQL driver metadata advertises both instance-metrics bits.
pub fn mysql_advertises_instance_capabilities() -> bool {
    let caps = crate::MYSQL_METADATA.capabilities;
    caps.contains(DriverCapabilities::INSTANCE_METRICS)
        && caps.contains(DriverCapabilities::INSTANCE_INSPECTOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pivoted_counters_list_is_non_empty() {
        assert!(
            !PIVOTED_COUNTERS.is_empty(),
            "PIVOTED_COUNTERS must have at least one entry"
        );
    }

    #[test]
    fn static_metrics_ids_match_pivoted_counters() {
        let metrics = MysqlInstanceCatalog::static_metrics();
        assert_eq!(metrics.len(), PIVOTED_COUNTERS.len());

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
    fn pivot_produces_float_kind_for_each_counter() {
        let metrics = MysqlInstanceCatalog::static_metrics();
        for m in &metrics {
            assert_eq!(m.default_refresh_secs, 15, "default refresh must be 15s");
        }
    }

    #[test]
    fn static_metric_default_refresh_secs_at_or_above_floor() {
        let metrics = MysqlInstanceCatalog::static_metrics();
        for m in &metrics {
            assert!(
                m.default_refresh_secs >= 10,
                "metric {:?} default_refresh_secs {} below floor",
                m.id,
                m.default_refresh_secs
            );
        }
    }

    #[test]
    fn static_inspectors_list_is_non_empty() {
        let inspectors = MysqlInstanceCatalog::static_inspectors();
        assert!(!inspectors.is_empty(), "must expose at least one inspector");
    }

    #[test]
    fn mysql_advertises_both_instance_capability_bits() {
        assert!(
            mysql_advertises_instance_capabilities(),
            "MySQL METADATA must include INSTANCE_METRICS and INSTANCE_INSPECTOR bits"
        );
    }

    /// BF10: when process_privilege probe returns false, processlist inspector
    /// must be absent from the list.
    #[test]
    fn inspectors_without_process_privilege_exclude_processlist() {
        let inspectors = MysqlInstanceCatalog::inspectors_with_probes(false);
        assert!(
            !inspectors.iter().any(|i| i.id == "mysql.processlist"),
            "mysql.processlist must be absent when PROCESS privilege probe fails"
        );
    }

    /// BF10: when process_privilege probe returns true, processlist is present.
    #[test]
    fn inspectors_with_process_privilege_include_processlist() {
        let inspectors = MysqlInstanceCatalog::inspectors_with_probes(true);
        assert!(
            inspectors.iter().any(|i| i.id == "mysql.processlist"),
            "mysql.processlist must be present when PROCESS privilege probe succeeds"
        );
    }

    /// BF10: when connection_admin probe returns false, kill row action is absent.
    #[test]
    fn row_actions_without_connection_admin_omit_kill() {
        let actions = MysqlInstanceCatalog::row_actions_with_probes("mysql.processlist", false);
        assert!(
            actions.is_empty(),
            "mysql.processlist kill must be absent when CONNECTION_ADMIN probe fails"
        );
    }

    /// BF10: when connection_admin probe returns true, kill row action is present.
    #[test]
    fn row_actions_with_connection_admin_include_kill() {
        let actions = MysqlInstanceCatalog::row_actions_with_probes("mysql.processlist", true);
        assert_eq!(actions.len(), 1, "must have one kill action");
        assert_eq!(actions[0].id, "kill");
    }

    /// BF8: MysqlInstanceCatalog must return a kill action for mysql.processlist.
    #[test]
    fn mysql_row_actions_processlist_returns_kill() {
        let actions = MysqlInstanceCatalog::static_row_actions("mysql.processlist");
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].id, "kill");
        assert!(actions[0].is_destructive);
    }

    #[test]
    fn mysql_row_actions_unknown_returns_empty() {
        let actions = MysqlInstanceCatalog::static_row_actions("unknown");
        assert!(actions.is_empty());
    }

    /// BF7: MysqlInstanceCatalog must return a non-None default dashboard with
    /// panels that reference valid metric or inspector IDs.
    #[test]
    fn mysql_default_dashboard_is_non_none_and_valid() {
        use dbflux_core::DefaultInstanceDashboard;

        let dashboard: Option<DefaultInstanceDashboard> =
            MysqlInstanceCatalog::static_default_dashboard();

        let dashboard =
            dashboard.expect("MysqlInstanceCatalog must return Some(default_dashboard)");
        assert!(
            !dashboard.panels.is_empty(),
            "default dashboard must have at least one panel"
        );
        assert!(
            !dashboard.title.is_empty(),
            "default dashboard must have a non-empty title"
        );

        let metric_ids: Vec<String> = MysqlInstanceCatalog::static_metrics()
            .into_iter()
            .map(|m| m.id)
            .collect();
        let inspector_ids: Vec<String> = MysqlInstanceCatalog::static_inspectors()
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
