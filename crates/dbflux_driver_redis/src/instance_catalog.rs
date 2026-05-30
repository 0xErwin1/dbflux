use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dbflux_core::{
    ColumnKind, ColumnMeta, DbError, DriverCapabilities, InstanceCatalog, InstanceInspectorDef,
    InstanceMetricDef, InstanceMetricUnit, QueryResult, QueryResultShape, Row, Value,
};
use redis::Commands;

/// Curated list of Redis INFO section field names mapped to chartable metrics.
///
/// Each entry: `(section_field, metric_id, display_name, group, unit)`.
/// `section_field` matches the key in the flat `INFO` output.
pub const INFO_FIELDS: &[(&str, &str, &str, &str, InstanceMetricUnit)] = &[
    (
        "connected_clients",
        "redis.connected_clients",
        "Connected clients",
        "Connections",
        InstanceMetricUnit::Count,
    ),
    (
        "blocked_clients",
        "redis.blocked_clients",
        "Blocked clients",
        "Connections",
        InstanceMetricUnit::Count,
    ),
    (
        "used_memory",
        "redis.used_memory",
        "Used memory (bytes)",
        "Memory",
        InstanceMetricUnit::Bytes,
    ),
    (
        "used_memory_rss",
        "redis.used_memory_rss",
        "Resident memory (bytes)",
        "Memory",
        InstanceMetricUnit::Bytes,
    ),
    (
        "mem_fragmentation_ratio",
        "redis.mem_fragmentation_ratio",
        "Memory fragmentation ratio",
        "Memory",
        InstanceMetricUnit::Unknown,
    ),
    (
        "instantaneous_ops_per_sec",
        "redis.ops_per_sec",
        "Operations / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "instantaneous_input_kbps",
        "redis.input_kbps",
        "Input (kbps)",
        "Network",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "instantaneous_output_kbps",
        "redis.output_kbps",
        "Output (kbps)",
        "Network",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "total_commands_processed",
        "redis.total_commands",
        "Total commands processed",
        "Throughput",
        InstanceMetricUnit::Count,
    ),
    (
        "keyspace_hits",
        "redis.keyspace_hits",
        "Keyspace hits",
        "Cache",
        InstanceMetricUnit::Count,
    ),
    (
        "keyspace_misses",
        "redis.keyspace_misses",
        "Keyspace misses",
        "Cache",
        InstanceMetricUnit::Count,
    ),
    (
        "repl_backlog_size",
        "redis.repl_backlog_size",
        "Replication backlog size",
        "Replication",
        InstanceMetricUnit::Bytes,
    ),
    (
        "connected_slaves",
        "redis.connected_replicas",
        "Connected replicas",
        "Replication",
        InstanceMetricUnit::Count,
    ),
];

/// Sensitive CLIENT LIST fields to redact per REQ-NF-2.
///
/// These field names appear in `CLIENT LIST` output and contain potentially
/// sensitive information (client addresses, authentication tokens, etc.).
/// They are replaced with a redacted placeholder before returning to the UI.
pub const SENSITIVE_CLIENT_FIELDS: &[&str] = &["addr", "laddr", "name"];

pub struct RedisInstanceCatalog {
    connection: Arc<Mutex<redis::Connection>>,
}

impl RedisInstanceCatalog {
    pub fn new(connection: Arc<Mutex<redis::Connection>>) -> Self {
        Self { connection }
    }

    pub fn static_metrics() -> Vec<InstanceMetricDef> {
        INFO_FIELDS
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
            id: "redis.client_list".to_string(),
            display_name: "Client list".to_string(),
            description: Some(
                "Connected clients from CLIENT LIST (sensitive fields redacted).".to_string(),
            ),
            default_refresh_secs: 10,
        }]
    }

    /// Parses the flat `INFO` output into a `HashMap<field_name, value_string>`.
    ///
    /// Lines starting with `#` are section headers and are skipped. Empty lines are skipped.
    pub fn parse_info_output(info: &str) -> HashMap<String, String> {
        let mut map = HashMap::new();

        for line in info.lines() {
            let line = line.trim();
            if line.starts_with('#') || line.is_empty() {
                continue;
            }

            if let Some((key, value)) = line.split_once(':') {
                map.insert(key.trim().to_string(), value.trim().to_string());
            }
        }

        map
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

fn text_col_nullable(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        kind: ColumnKind::Text,
        type_name: "string".to_string(),
        nullable: true,
        is_primary_key: false,
    }
}

fn redis_error(e: redis::RedisError) -> DbError {
    DbError::QueryFailed(e.to_string().into())
}

#[async_trait]
impl InstanceCatalog for RedisInstanceCatalog {
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
        let mut conn = self.connection.lock().map_err(|_| {
            DbError::QueryFailed("redis connection mutex poisoned".to_string().into())
        })?;

        dispatch_metric_series(&mut conn, metric_id)
    }

    async fn fetch_inspector_snapshot(
        &self,
        metric_id: &str,
    ) -> Result<QueryResult, DbError> {
        let mut conn = self.connection.lock().map_err(|_| {
            DbError::QueryFailed("redis connection mutex poisoned".to_string().into())
        })?;

        dispatch_inspector_snapshot(&mut conn, metric_id)
    }
}

pub(crate) fn dispatch_metric_series(
    conn: &mut redis::Connection,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    let entry = INFO_FIELDS.iter().find(|(_, id, _, _, _)| *id == metric_id);

    match entry {
        Some((field_name, _, display_name, _, _)) => {
            let info: String = redis::cmd("INFO")
                .query(conn)
                .map_err(redis_error)?;

            let fields = RedisInstanceCatalog::parse_info_output(&info);
            let value: f64 = fields
                .get(*field_name)
                .and_then(|v| v.parse().ok())
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
        None => Err(DbError::NotSupported(
            format!("unknown instance metric: {metric_id}").into(),
        )),
    }
}

pub(crate) fn dispatch_inspector_snapshot(
    conn: &mut redis::Connection,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    match metric_id {
        "redis.client_list" => fetch_client_list(conn),
        other => Err(DbError::NotSupported(
            format!("unknown inspector: {other}").into(),
        )),
    }
}

fn fetch_client_list(conn: &mut redis::Connection) -> Result<QueryResult, DbError> {
    let raw: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query(conn)
        .map_err(redis_error)?;

    let columns = vec![
        text_col_nullable("id"),
        text_col_nullable("cmd"),
        text_col_nullable("age"),
        text_col_nullable("idle"),
        text_col_nullable("flags"),
        text_col_nullable("db"),
        text_col_nullable("sub"),
        text_col_nullable("multi"),
    ];

    let mut rows: Vec<Row> = Vec::new();

    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let mut fields: HashMap<String, String> = HashMap::new();
        for part in line.split_whitespace() {
            if let Some((k, v)) = part.split_once('=') {
                let value = if SENSITIVE_CLIENT_FIELDS.contains(&k) {
                    "[redacted]".to_string()
                } else {
                    v.to_string()
                };
                fields.insert(k.to_string(), value);
            }
        }

        let row: Row = vec![
            fields.get("id").map(|v| Value::Text(v.clone())).unwrap_or(Value::Null),
            fields.get("cmd").map(|v| Value::Text(v.clone())).unwrap_or(Value::Null),
            fields.get("age").map(|v| Value::Text(v.clone())).unwrap_or(Value::Null),
            fields.get("idle").map(|v| Value::Text(v.clone())).unwrap_or(Value::Null),
            fields.get("flags").map(|v| Value::Text(v.clone())).unwrap_or(Value::Null),
            fields.get("db").map(|v| Value::Text(v.clone())).unwrap_or(Value::Null),
            fields.get("sub").map(|v| Value::Text(v.clone())).unwrap_or(Value::Null),
            fields.get("multi").map(|v| Value::Text(v.clone())).unwrap_or(Value::Null),
        ];

        rows.push(row);
    }

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

/// Returns `true` if the Redis driver metadata advertises both instance-metrics bits.
pub fn redis_advertises_instance_capabilities() -> bool {
    let caps = crate::REDIS_METADATA.capabilities;
    caps.contains(DriverCapabilities::INSTANCE_METRICS)
        && caps.contains(DriverCapabilities::INSTANCE_INSPECTOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE_INFO: &str = r#"
# Server
redis_version:7.0.0
redis_git_sha1:0
os:Linux 5.15.0

# Clients
connected_clients:5
blocked_clients:0
tracking_clients:0

# Memory
used_memory:1234567
used_memory_rss:2345678
mem_fragmentation_ratio:1.89

# Stats
instantaneous_ops_per_sec:42
total_commands_processed:99999
keyspace_hits:1000
keyspace_misses:50

# Replication
role:master
connected_slaves:1
repl_backlog_size:1048576

# Network
instantaneous_input_kbps:10.5
instantaneous_output_kbps:20.3
"#;

    #[test]
    fn info_fields_list_is_non_empty() {
        assert!(
            !INFO_FIELDS.is_empty(),
            "INFO_FIELDS must have at least one entry"
        );
    }

    #[test]
    fn parse_info_fixture_contains_connected_clients() {
        let fields = RedisInstanceCatalog::parse_info_output(FIXTURE_INFO);
        assert!(
            fields.contains_key("connected_clients"),
            "must have connected_clients"
        );
        assert_eq!(fields["connected_clients"], "5");
    }

    #[test]
    fn parse_info_fixture_contains_used_memory() {
        let fields = RedisInstanceCatalog::parse_info_output(FIXTURE_INFO);
        assert!(fields.contains_key("used_memory"), "must have used_memory");
        assert_eq!(fields["used_memory"], "1234567");
    }

    #[test]
    fn parse_info_fixture_contains_replication_metric() {
        let fields = RedisInstanceCatalog::parse_info_output(FIXTURE_INFO);
        let has_replication = fields.contains_key("connected_slaves")
            || fields.contains_key("repl_backlog_size");
        assert!(has_replication, "must have at least one replication metric");
    }

    #[test]
    fn info_fields_all_covered_in_static_metrics() {
        let metrics = RedisInstanceCatalog::static_metrics();
        assert_eq!(metrics.len(), INFO_FIELDS.len());
        for m in &metrics {
            assert!(m.default_refresh_secs >= 10);
        }
    }

    #[test]
    fn redis_advertises_both_instance_capability_bits() {
        assert!(
            redis_advertises_instance_capabilities(),
            "Redis METADATA must include INSTANCE_METRICS and INSTANCE_INSPECTOR bits"
        );
    }

    #[test]
    fn sensitive_client_fields_list_is_non_empty() {
        assert!(!SENSITIVE_CLIENT_FIELDS.is_empty());
        assert!(SENSITIVE_CLIENT_FIELDS.contains(&"addr"));
    }
}
