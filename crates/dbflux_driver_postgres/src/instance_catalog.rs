use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dbflux_core::{
    ColumnKind, ColumnMeta, DbError, DriverCapabilities, InstanceCatalog, InstanceInspectorDef,
    InstanceMetricDef, InstanceMetricUnit, QueryResult, QueryResultShape, Row, Value,
};
use postgres::Client;

/// Postgres instance metrics and inspector catalog.
///
/// Holds a shared reference to the connection's postgres client so it can
/// issue queries on demand. All methods are internally synchronous (the
/// underlying `postgres` crate is blocking); the `async` wrapper satisfies
/// the `InstanceCatalog` trait boundary.
pub struct PgInstanceCatalog {
    client: Arc<Mutex<Client>>,
}

impl PgInstanceCatalog {
    pub fn new(client: Arc<Mutex<Client>>) -> Self {
        Self { client }
    }

    /// Baseline metrics always available on any PostgreSQL connection.
    pub fn static_metrics() -> Vec<InstanceMetricDef> {
        vec![
            InstanceMetricDef {
                id: "pg.tps".to_string(),
                display_name: "Transactions / sec".to_string(),
                group: "Throughput".to_string(),
                unit: InstanceMetricUnit::PerSecond,
                description: Some(
                    "Total transaction commits and rollbacks per second (all databases)."
                        .to_string(),
                ),
                default_refresh_secs: 30,
            },
            InstanceMetricDef {
                id: "pg.cache_hit_ratio".to_string(),
                display_name: "Buffer cache hit ratio".to_string(),
                group: "Cache".to_string(),
                unit: InstanceMetricUnit::Percent,
                description: Some(
                    "Fraction of block reads served from the shared buffer cache (0–100)."
                        .to_string(),
                ),
                default_refresh_secs: 30,
            },
            InstanceMetricDef {
                id: "pg.active_connections".to_string(),
                display_name: "Active connections".to_string(),
                group: "Connections".to_string(),
                unit: InstanceMetricUnit::Count,
                description: Some(
                    "Number of backends currently in an active query state.".to_string(),
                ),
                default_refresh_secs: 15,
            },
            InstanceMetricDef {
                id: "pg.idle_connections".to_string(),
                display_name: "Idle connections".to_string(),
                group: "Connections".to_string(),
                unit: InstanceMetricUnit::Count,
                description: Some(
                    "Number of backends connected but not executing a query.".to_string(),
                ),
                default_refresh_secs: 30,
            },
            InstanceMetricDef {
                id: "pg.blocks_read".to_string(),
                display_name: "Disk block reads".to_string(),
                group: "I/O".to_string(),
                unit: InstanceMetricUnit::Count,
                description: Some(
                    "Cumulative blocks read from disk (not cache) across all user tables."
                        .to_string(),
                ),
                default_refresh_secs: 30,
            },
        ]
    }

    /// Returns metrics available given the probe result for `pg_stat_statements`.
    pub fn metrics_with_probe(pg_stat_statements_available: bool) -> Vec<InstanceMetricDef> {
        let mut metrics = Self::static_metrics();

        if pg_stat_statements_available {
            metrics.push(InstanceMetricDef {
                id: "pg.stat_statements.mean_exec_ms".to_string(),
                display_name: "Mean query exec time (ms)".to_string(),
                group: "Queries".to_string(),
                unit: InstanceMetricUnit::Milliseconds,
                description: Some(
                    "Average execution time across all tracked queries (pg_stat_statements)."
                        .to_string(),
                ),
                default_refresh_secs: 30,
            });
        }

        metrics
    }

    /// Inspector definitions always available on any PostgreSQL connection.
    pub fn static_inspectors() -> Vec<InstanceInspectorDef> {
        vec![
            InstanceInspectorDef {
                id: "pg.activity".to_string(),
                display_name: "Active sessions".to_string(),
                description: Some(
                    "Live snapshot of pg_stat_activity — one row per backend process.".to_string(),
                ),
                default_refresh_secs: 10,
            },
            InstanceInspectorDef {
                id: "pg.locks".to_string(),
                display_name: "Locks".to_string(),
                description: Some(
                    "Currently held and awaited locks from pg_locks joined with pg_stat_activity."
                        .to_string(),
                ),
                default_refresh_secs: 10,
            },
        ]
    }

    fn probe_pg_stat_statements(client: &mut Client) -> bool {
        client
            .query_one(
                "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'",
                &[],
            )
            .is_ok()
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
        type_name: "int8".to_string(),
        nullable: false,
        is_primary_key: false,
    }
}

fn float_col(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        kind: ColumnKind::Float,
        type_name: "float8".to_string(),
        nullable: false,
        is_primary_key: false,
    }
}

fn text_col_nullable(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        kind: ColumnKind::Text,
        type_name: "text".to_string(),
        nullable: true,
        is_primary_key: false,
    }
}

fn text_col(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        kind: ColumnKind::Text,
        type_name: "text".to_string(),
        nullable: false,
        is_primary_key: false,
    }
}

fn float_col_nullable(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        kind: ColumnKind::Float,
        type_name: "float8".to_string(),
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

fn pg_error(e: postgres::Error) -> DbError {
    DbError::QueryFailed(e.to_string().into())
}

#[async_trait]
impl InstanceCatalog for PgInstanceCatalog {
    async fn list_metrics(&self) -> Result<Vec<InstanceMetricDef>, DbError> {
        let mut client = self.client.lock().map_err(|_| {
            DbError::QueryFailed("postgres client mutex poisoned".to_string().into())
        })?;

        let has_stat_statements = Self::probe_pg_stat_statements(&mut client);

        Ok(Self::metrics_with_probe(has_stat_statements))
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
        let mut client = self.client.lock().map_err(|_| {
            DbError::QueryFailed("postgres client mutex poisoned".to_string().into())
        })?;

        match metric_id {
            "pg.tps" => fetch_tps(&mut client),
            "pg.cache_hit_ratio" => fetch_cache_hit_ratio(&mut client),
            "pg.active_connections" => fetch_connection_count(&mut client, "active"),
            "pg.idle_connections" => fetch_connection_count(&mut client, "idle"),
            "pg.blocks_read" => fetch_blocks_read(&mut client),
            "pg.stat_statements.mean_exec_ms" => fetch_stat_statements_mean_exec(&mut client),
            other => Err(DbError::NotSupported(format!(
                "unknown instance metric: {other}"
            ))),
        }
    }

    async fn fetch_inspector_snapshot(&self, metric_id: &str) -> Result<QueryResult, DbError> {
        let mut client = self.client.lock().map_err(|_| {
            DbError::QueryFailed("postgres client mutex poisoned".to_string().into())
        })?;

        match metric_id {
            "pg.activity" => fetch_activity_snapshot(&mut client),
            "pg.locks" => fetch_locks_snapshot(&mut client),
            other => Err(DbError::NotSupported(format!("unknown inspector: {other}"))),
        }
    }
}

fn fetch_tps(client: &mut Client) -> Result<QueryResult, DbError> {
    let row = client
        .query_one(
            "SELECT COALESCE(SUM(xact_commit) + SUM(xact_rollback), 0)::float8 AS tps \
             FROM pg_stat_database",
            &[],
        )
        .map_err(pg_error)?;

    let tps: f64 = row.get(0);

    Ok(single_sample_result(
        vec![timestamp_col("timestamp_ms"), float_col("tps")],
        vec![Value::Float(tps)],
    ))
}

fn fetch_cache_hit_ratio(client: &mut Client) -> Result<QueryResult, DbError> {
    let row = client
        .query_one(
            "SELECT CASE WHEN (heap_blks_hit + heap_blks_read) = 0 THEN 100.0 \
                         ELSE ROUND(100.0 * heap_blks_hit::numeric \
                              / (heap_blks_hit + heap_blks_read), 2) \
                    END::float8 AS hit_ratio \
             FROM ( \
               SELECT SUM(heap_blks_hit) AS heap_blks_hit, \
                      SUM(heap_blks_read) AS heap_blks_read \
               FROM pg_statio_user_tables \
             ) t",
            &[],
        )
        .map_err(pg_error)?;

    let ratio: f64 = row.get(0);

    Ok(single_sample_result(
        vec![
            timestamp_col("timestamp_ms"),
            float_col("cache_hit_ratio_pct"),
        ],
        vec![Value::Float(ratio)],
    ))
}

fn fetch_connection_count(client: &mut Client, state: &str) -> Result<QueryResult, DbError> {
    let row = client
        .query_one(
            "SELECT COUNT(*)::float8 FROM pg_stat_activity WHERE state = $1",
            &[&state],
        )
        .map_err(pg_error)?;

    let count: f64 = row.get(0);

    Ok(single_sample_result(
        vec![timestamp_col("timestamp_ms"), float_col("connection_count")],
        vec![Value::Float(count)],
    ))
}

fn fetch_blocks_read(client: &mut Client) -> Result<QueryResult, DbError> {
    let row = client
        .query_one(
            "SELECT COALESCE(SUM(heap_blks_read), 0)::float8 AS blocks_read \
             FROM pg_statio_user_tables",
            &[],
        )
        .map_err(pg_error)?;

    let blocks: f64 = row.get(0);

    Ok(single_sample_result(
        vec![timestamp_col("timestamp_ms"), float_col("blocks_read")],
        vec![Value::Float(blocks)],
    ))
}

fn fetch_stat_statements_mean_exec(client: &mut Client) -> Result<QueryResult, DbError> {
    let row = client
        .query_one(
            "SELECT COALESCE(AVG(mean_exec_time), 0.0)::float8 AS mean_exec_ms \
             FROM pg_stat_statements",
            &[],
        )
        .map_err(pg_error)?;

    let mean_ms: f64 = row.get(0);

    Ok(single_sample_result(
        vec![timestamp_col("timestamp_ms"), float_col("mean_exec_ms")],
        vec![Value::Float(mean_ms)],
    ))
}

fn fetch_activity_snapshot(client: &mut Client) -> Result<QueryResult, DbError> {
    let sql = "SELECT pid::text, usename, application_name, client_addr::text, \
                      state, wait_event_type, wait_event, \
                      EXTRACT(EPOCH FROM (now() - query_start))::float8 AS query_age_secs, \
                      LEFT(query, 200) AS query_preview \
               FROM pg_stat_activity \
               WHERE state IS NOT NULL \
               ORDER BY query_start NULLS LAST";

    let rows = client.query(sql, &[]).map_err(pg_error)?;

    let columns = vec![
        text_col("pid"),
        text_col_nullable("usename"),
        text_col_nullable("application_name"),
        text_col_nullable("client_addr"),
        text_col_nullable("state"),
        text_col_nullable("wait_event_type"),
        text_col_nullable("wait_event"),
        float_col_nullable("query_age_secs"),
        text_col_nullable("query_preview"),
    ];

    let result_rows: Vec<Row> = rows
        .iter()
        .map(|row| {
            vec![
                pg_text_opt(row, 0),
                pg_text_opt(row, 1),
                pg_text_opt(row, 2),
                pg_text_opt(row, 3),
                pg_text_opt(row, 4),
                pg_text_opt(row, 5),
                pg_text_opt(row, 6),
                pg_f64_opt(row, 7),
                pg_text_opt(row, 8),
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

fn fetch_locks_snapshot(client: &mut Client) -> Result<QueryResult, DbError> {
    let sql = "SELECT l.pid::text, a.usename, l.locktype, \
                      l.relation::regclass::text AS relation, \
                      l.mode, l.granted::text, \
                      LEFT(a.query, 100) AS query_preview \
               FROM pg_locks l \
               LEFT JOIN pg_stat_activity a ON l.pid = a.pid \
               ORDER BY l.granted, l.pid";

    let rows = client.query(sql, &[]).map_err(pg_error)?;

    let columns = vec![
        text_col("pid"),
        text_col_nullable("usename"),
        text_col_nullable("locktype"),
        text_col_nullable("relation"),
        text_col_nullable("mode"),
        text_col("granted"),
        text_col_nullable("query_preview"),
    ];

    let result_rows: Vec<Row> = rows
        .iter()
        .map(|row| {
            vec![
                pg_text_opt(row, 0),
                pg_text_opt(row, 1),
                pg_text_opt(row, 2),
                pg_text_opt(row, 3),
                pg_text_opt(row, 4),
                pg_text_opt(row, 5),
                pg_text_opt(row, 6),
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

fn pg_text_opt(row: &postgres::Row, idx: usize) -> Value {
    row.get::<_, Option<String>>(idx)
        .map(Value::Text)
        .unwrap_or(Value::Null)
}

fn pg_f64_opt(row: &postgres::Row, idx: usize) -> Value {
    row.get::<_, Option<f64>>(idx)
        .map(Value::Float)
        .unwrap_or(Value::Null)
}

/// Dispatches an `InstanceMetricQuery` synchronously using an already-locked client.
///
/// Called from `PostgresConnection::execute()` to avoid going through the async
/// catalog layer; all metric fetches use the same underlying sync postgres client.
pub(crate) fn dispatch_metric_series(
    client: &mut Client,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    match metric_id {
        "pg.tps" => fetch_tps(client),
        "pg.cache_hit_ratio" => fetch_cache_hit_ratio(client),
        "pg.active_connections" => fetch_connection_count(client, "active"),
        "pg.idle_connections" => fetch_connection_count(client, "idle"),
        "pg.blocks_read" => fetch_blocks_read(client),
        "pg.stat_statements.mean_exec_ms" => fetch_stat_statements_mean_exec(client),
        other => Err(DbError::NotSupported(format!(
            "unknown instance metric: {other}"
        ))),
    }
}

/// Dispatches an `InstanceInspectorQuery` synchronously using an already-locked client.
pub(crate) fn dispatch_inspector_snapshot(
    client: &mut Client,
    metric_id: &str,
) -> Result<QueryResult, DbError> {
    match metric_id {
        "pg.activity" => fetch_activity_snapshot(client),
        "pg.locks" => fetch_locks_snapshot(client),
        other => Err(DbError::NotSupported(format!("unknown inspector: {other}"))),
    }
}

/// Returns `true` if the PostgreSQL driver advertises both instance-metrics
/// capability bits, confirming the metadata declaration is correct.
pub fn postgres_advertises_instance_capabilities() -> bool {
    let caps = crate::METADATA.capabilities;
    caps.contains(DriverCapabilities::INSTANCE_METRICS)
        && caps.contains(DriverCapabilities::INSTANCE_INSPECTOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_metrics_list_is_non_empty() {
        let metrics = PgInstanceCatalog::static_metrics();
        assert!(
            !metrics.is_empty(),
            "PgInstanceCatalog must expose at least one static metric"
        );
    }

    #[test]
    fn static_metric_ids_are_lowercase_dot_separated() {
        let metrics = PgInstanceCatalog::static_metrics();

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

            assert!(
                valid,
                "metric id {:?} does not match [a-z][a-z0-9_.]*",
                m.id
            );
        }
    }

    #[test]
    fn static_inspectors_list_is_non_empty() {
        let inspectors = PgInstanceCatalog::static_inspectors();
        assert!(
            !inspectors.is_empty(),
            "PgInstanceCatalog must expose at least one static inspector"
        );
    }

    #[test]
    fn pg_stat_statements_absent_when_probe_fails() {
        let metrics_without = PgInstanceCatalog::metrics_with_probe(false);
        let metrics_with = PgInstanceCatalog::metrics_with_probe(true);

        let has_stat_statements = |metrics: &[InstanceMetricDef]| {
            metrics.iter().any(|m| m.id.contains("stat_statements"))
        };

        assert!(
            !has_stat_statements(&metrics_without),
            "pg_stat_statements metrics must be absent when probe returns false"
        );
        assert!(
            has_stat_statements(&metrics_with),
            "pg_stat_statements metrics must be present when probe returns true"
        );
    }

    #[test]
    fn postgres_advertises_both_instance_capability_bits() {
        assert!(
            postgres_advertises_instance_capabilities(),
            "Postgres METADATA must include INSTANCE_METRICS and INSTANCE_INSPECTOR bits"
        );
    }

    #[test]
    fn static_metric_default_refresh_secs_at_or_above_floor() {
        let metrics = PgInstanceCatalog::static_metrics();
        for m in &metrics {
            assert!(
                m.default_refresh_secs >= 10,
                "metric {:?} default_refresh_secs {} is below the 10s floor",
                m.id,
                m.default_refresh_secs
            );
        }
    }

    #[test]
    fn static_inspector_default_refresh_secs_at_or_above_floor() {
        let inspectors = PgInstanceCatalog::static_inspectors();
        for i in &inspectors {
            assert!(
                i.default_refresh_secs >= 10,
                "inspector {:?} default_refresh_secs {} is below the 10s floor",
                i.id,
                i.default_refresh_secs
            );
        }
    }
}
