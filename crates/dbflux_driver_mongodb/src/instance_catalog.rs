use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bson::{Bson, Document};
use dbflux_core::{
    ColumnKind, ColumnMeta, DbError, DriverCapabilities, InstanceCatalog, InstanceInspectorDef,
    InstanceMetricDef, InstanceMetricUnit, QueryResult, QueryResultShape, Row, Value,
};
use mongodb::sync::Client;

/// Dotted BSON path within the `serverStatus` document mapped to a chartable metric.
///
/// Each entry: `(dotted_path, metric_id, display_name, group, unit)`.
pub const SERVER_STATUS_PATHS: &[(&str, &str, &str, &str, InstanceMetricUnit)] = &[
    (
        "opcounters.query",
        "mongo.opcounters.query",
        "Queries / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "opcounters.insert",
        "mongo.opcounters.insert",
        "Inserts / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "opcounters.update",
        "mongo.opcounters.update",
        "Updates / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "opcounters.delete",
        "mongo.opcounters.delete",
        "Deletes / sec",
        "Throughput",
        InstanceMetricUnit::PerSecond,
    ),
    (
        "connections.current",
        "mongo.connections.current",
        "Current connections",
        "Connections",
        InstanceMetricUnit::Count,
    ),
    (
        "connections.available",
        "mongo.connections.available",
        "Available connections",
        "Connections",
        InstanceMetricUnit::Count,
    ),
    (
        "mem.resident",
        "mongo.mem.resident",
        "Resident memory (MB)",
        "Memory",
        InstanceMetricUnit::Bytes,
    ),
    (
        "globalLock.currentQueue.total",
        "mongo.global_lock.queue",
        "Global lock queue",
        "Locks",
        InstanceMetricUnit::Count,
    ),
    (
        "wiredTiger.cache.bytes currently in the cache",
        "mongo.wt.cache_bytes",
        "WiredTiger cache bytes",
        "Cache",
        InstanceMetricUnit::Bytes,
    ),
    (
        "wiredTiger.cache.unmodified pages evicted",
        "mongo.wt.pages_evicted",
        "WiredTiger pages evicted",
        "Cache",
        InstanceMetricUnit::Count,
    ),
];

pub struct MongoInstanceCatalog {
    client: Arc<Mutex<Client>>,
}

impl MongoInstanceCatalog {
    pub fn new(client: Arc<Mutex<Client>>) -> Self {
        Self { client }
    }

    pub fn static_metrics() -> Vec<InstanceMetricDef> {
        SERVER_STATUS_PATHS
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
            id: "mongo.current_op".to_string(),
            display_name: "Current operations".to_string(),
            description: Some(
                "Active and pending operations from $currentOp aggregation.".to_string(),
            ),
            default_refresh_secs: 10,
        }]
    }

    /// Extracts a numeric value from a BSON document at a dotted path.
    ///
    /// Traverses nested documents using `.`-separated path segments.
    /// Returns `None` if any segment is missing or the leaf is not numeric.
    pub fn extract_path(doc: &Document, path: &str) -> Option<f64> {
        let segments: Vec<&str> = path.splitn(2, '.').collect();

        let key = segments[0];
        let value = doc.get(key)?;

        if segments.len() == 1 {
            bson_to_f64(value)
        } else {
            match value {
                Bson::Document(nested) => Self::extract_path(nested, segments[1]),
                _ => None,
            }
        }
    }
}

fn bson_to_f64(bson: &Bson) -> Option<f64> {
    match bson {
        Bson::Double(v) => Some(*v),
        Bson::Int32(v) => Some(*v as f64),
        Bson::Int64(v) => Some(*v as f64),
        _ => None,
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
        type_name: "int64".to_string(),
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

fn mongo_error(e: mongodb::error::Error) -> DbError {
    DbError::QueryFailed(e.to_string().into())
}

fn get_admin_db(client: &Client) -> mongodb::sync::Database {
    client.database("admin")
}

fn run_server_status(client: &Client) -> Result<Document, DbError> {
    get_admin_db(client)
        .run_command(bson::doc! { "serverStatus": 1 })
        .run()
        .map_err(mongo_error)
}

#[async_trait]
impl InstanceCatalog for MongoInstanceCatalog {
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
        let client = self.client.lock().map_err(|_| {
            DbError::QueryFailed("mongo client mutex poisoned".to_string().into())
        })?;

        dispatch_metric_series(&client, metric_id)
    }

    async fn fetch_inspector_snapshot(
        &self,
        metric_id: &str,
    ) -> Result<QueryResult, DbError> {
        let client = self.client.lock().map_err(|_| {
            DbError::QueryFailed("mongo client mutex poisoned".to_string().into())
        })?;

        dispatch_inspector_snapshot(&client, metric_id)
    }
}

pub(crate) fn dispatch_metric_series(
    client: &Client,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    let entry = SERVER_STATUS_PATHS
        .iter()
        .find(|(_, id, _, _, _)| *id == metric_id);

    match entry {
        Some((path, _, display_name, _, _)) => {
            let status = run_server_status(client)?;
            let value = MongoInstanceCatalog::extract_path(&status, path).unwrap_or(0.0);

            let columns = vec![
                timestamp_col("timestamp_ms"),
                float_col(display_name),
            ];

            let row: Row = vec![Value::Int(now_epoch_ms()), Value::Float(value)];

            Ok(QueryResult {
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
            })
        }
        None => Err(DbError::NotSupported(
            format!("unknown instance metric: {metric_id}").into(),
        )),
    }
}

pub(crate) fn dispatch_inspector_snapshot(
    client: &Client,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    match metric_id {
        "mongo.current_op" => fetch_current_op(client),
        other => Err(DbError::NotSupported(
            format!("unknown inspector: {other}").into(),
        )),
    }
}

fn fetch_current_op(client: &Client) -> Result<QueryResult, DbError> {
    let db = get_admin_db(client);

    let pipeline = vec![
        bson::doc! { "$currentOp": { "allUsers": true, "idleConnections": false } },
        bson::doc! {
            "$project": {
                "opid": 1,
                "type": 1,
                "ns": 1,
                "op": 1,
                "secs_running": 1,
                "desc": 1,
                "client": 1,
            }
        },
        bson::doc! { "$limit": 100 },
    ];

    let cursor = db
        .aggregate(pipeline)
        .run()
        .map_err(mongo_error)?;

    let columns = vec![
        ColumnMeta { name: "opid".to_string(), kind: ColumnKind::Text, type_name: "string".to_string(), nullable: true, is_primary_key: false },
        ColumnMeta { name: "type".to_string(), kind: ColumnKind::Text, type_name: "string".to_string(), nullable: true, is_primary_key: false },
        ColumnMeta { name: "ns".to_string(), kind: ColumnKind::Text, type_name: "string".to_string(), nullable: true, is_primary_key: false },
        ColumnMeta { name: "op".to_string(), kind: ColumnKind::Text, type_name: "string".to_string(), nullable: true, is_primary_key: false },
        ColumnMeta { name: "secs_running".to_string(), kind: ColumnKind::Float, type_name: "double".to_string(), nullable: true, is_primary_key: false },
        ColumnMeta { name: "desc".to_string(), kind: ColumnKind::Text, type_name: "string".to_string(), nullable: true, is_primary_key: false },
        ColumnMeta { name: "client".to_string(), kind: ColumnKind::Text, type_name: "string".to_string(), nullable: true, is_primary_key: false },
    ];

    let mut rows: Vec<Row> = Vec::new();

    for doc_result in cursor {
        let doc = doc_result.map_err(mongo_error)?;

        let row: Row = vec![
            doc.get("opid")
                .and_then(|v| v.as_i64().map(|i| Value::Text(i.to_string())))
                .or_else(|| doc.get("opid").and_then(|v| v.as_str().map(|s| Value::Text(s.to_string()))))
                .unwrap_or(Value::Null),
            doc.get("type").and_then(|v| v.as_str().map(|s| Value::Text(s.to_string()))).unwrap_or(Value::Null),
            doc.get("ns").and_then(|v| v.as_str().map(|s| Value::Text(s.to_string()))).unwrap_or(Value::Null),
            doc.get("op").and_then(|v| v.as_str().map(|s| Value::Text(s.to_string()))).unwrap_or(Value::Null),
            doc.get("secs_running").and_then(bson_to_f64).map(Value::Float).unwrap_or(Value::Null),
            doc.get("desc").and_then(|v| v.as_str().map(|s| Value::Text(s.to_string()))).unwrap_or(Value::Null),
            doc.get("client").and_then(|v| v.as_str().map(|s| Value::Text(s.to_string()))).unwrap_or(Value::Null),
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

/// Returns `true` if MongoDB METADATA advertises both instance-metrics bits.
pub fn mongo_advertises_instance_capabilities() -> bool {
    let caps = crate::MONGODB_METADATA.capabilities;
    caps.contains(DriverCapabilities::INSTANCE_METRICS)
        && caps.contains(DriverCapabilities::INSTANCE_INSPECTOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_status_paths_list_is_non_empty() {
        assert!(
            !SERVER_STATUS_PATHS.is_empty(),
            "SERVER_STATUS_PATHS must have at least one entry"
        );
    }

    #[test]
    fn static_metrics_ids_are_lowercase_dot_separated() {
        let metrics = MongoInstanceCatalog::static_metrics();
        for m in &metrics {
            let valid = !m.id.is_empty()
                && m.id.chars().next().map(|c| c.is_ascii_lowercase()).unwrap_or(false)
                && m.id
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_');
            assert!(valid, "metric id {:?} must match [a-z][a-z0-9_.]*", m.id);
        }
    }

    #[test]
    fn bson_path_extraction_from_fixture_produces_float_value() {
        let mut inner = Document::new();
        inner.insert("query", Bson::Int64(42));
        inner.insert("insert", Bson::Int64(7));

        let mut status = Document::new();
        status.insert("opcounters", Bson::Document(inner));
        status.insert("mem", Bson::Document({
            let mut d = Document::new();
            d.insert("resident", Bson::Int32(256));
            d
        }));

        assert_eq!(
            MongoInstanceCatalog::extract_path(&status, "opcounters.query"),
            Some(42.0)
        );
        assert_eq!(
            MongoInstanceCatalog::extract_path(&status, "opcounters.insert"),
            Some(7.0)
        );
        assert_eq!(
            MongoInstanceCatalog::extract_path(&status, "mem.resident"),
            Some(256.0)
        );
        assert_eq!(
            MongoInstanceCatalog::extract_path(&status, "opcounters.missing"),
            None
        );
        assert_eq!(
            MongoInstanceCatalog::extract_path(&status, "nonexistent.path"),
            None
        );
    }

    #[test]
    fn static_metric_default_refresh_secs_at_or_above_floor() {
        let metrics = MongoInstanceCatalog::static_metrics();
        for m in &metrics {
            assert!(m.default_refresh_secs >= 10);
        }
    }

    #[test]
    fn static_inspectors_list_is_non_empty() {
        let inspectors = MongoInstanceCatalog::static_inspectors();
        assert!(!inspectors.is_empty());
    }

    #[test]
    fn mongo_advertises_both_instance_capability_bits() {
        assert!(
            mongo_advertises_instance_capabilities(),
            "MongoDB METADATA must include INSTANCE_METRICS and INSTANCE_INSPECTOR bits"
        );
    }
}
