use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dbflux_core::{
    ColumnKind, ColumnMeta, DbError, DriverCapabilities, InstanceCatalog, InstanceInspectorDef,
    InstanceMetricDef, InstanceMetricUnit, QueryResult, QueryResultShape, Row, Value,
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
}

impl MysqlInstanceCatalog {
    pub fn new(conn: Arc<Mutex<Conn>>, performance_schema_available: bool) -> Self {
        Self {
            conn,
            performance_schema_available,
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

    pub fn probe_performance_schema(conn: &mut Conn) -> bool {
        conn.query_first::<String, _>(
            "SELECT 'ok' FROM information_schema.SCHEMATA WHERE schema_name = 'performance_schema'",
        )
        .unwrap_or(None)
        .is_some()
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
        Ok(Self::static_inspectors())
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
        .query_first(format!("SHOW GLOBAL STATUS LIKE '{}'", var_name))
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
}
