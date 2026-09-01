#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

//! Docker-backed ClickHouse tests. Each test starts an isolated server through
//! testcontainers. Run them with:
//!
//! ```text
//! cargo test --manifest-path crates/dbflux_driver_clickhouse/Cargo.toml --test live_integration -- --ignored
//! ```

use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    ColumnKind, Connection, ConnectionProfile, DbConfig, DbDriver, DbError, ExecutionContext,
    ExecutionSourceContext,
};
use dbflux_core::{QueryRequest, Value};
use dbflux_driver_clickhouse::ClickHouseDriver;
use dbflux_driver_clickhouse::instance_catalog::{METRIC_DEFS, MetricSource};
use dbflux_test_support::containers::{self, ClickHouseConfig};
use std::time::Duration;

fn connect(config: &ClickHouseConfig) -> Result<Box<dyn Connection>, DbError> {
    let profile = ConnectionProfile::new(
        "live-clickhouse",
        DbConfig::ClickHouse {
            url: config.endpoint.clone(),
            user: config.user.clone(),
            database: config.database.clone(),
            request_timeout_seconds: Some(30),
        },
    );
    let password = SecretString::from(config.password.clone());

    containers::retry_db_operation(Duration::from_secs(30), || {
        let connection =
            ClickHouseDriver::new().connect_with_secrets(&profile, Some(&password), None)?;
        connection.ping()?;
        Ok(connection)
    })
}

struct TableCleanup<'a> {
    connection: &'a dyn Connection,
    table: &'static str,
}

impl Drop for TableCleanup<'_> {
    fn drop(&mut self) {
        if let Err(error) = self.connection.execute(&QueryRequest::new(format!(
            "DROP TABLE IF EXISTS {}",
            self.table
        ))) {
            eprintln!(
                "failed to clean up ClickHouse table {}: {error}",
                self.table
            );
        }
    }
}

#[test]
#[ignore = "requires Docker daemon"]
fn clickhouse_connects_and_decodes_types() -> Result<(), DbError> {
    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;
        let result = connection.execute(&QueryRequest::new(
            "SELECT toUInt64(42) AS id, toDateTime64('2026-08-17 12:34:56.789', 3, 'UTC') AS ts, [toInt32(1), 2, 3] AS values, CAST(NULL, 'Nullable(String)') AS note",
        ))?;

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns.len(), 4);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].type_name, "UInt64");
        assert_eq!(result.columns[0].kind, ColumnKind::Integer);
        assert_eq!(result.columns[1].kind, ColumnKind::Timestamp);
        assert_eq!(result.columns[2].kind, ColumnKind::Unknown);
        assert_eq!(result.columns[3].kind, ColumnKind::Text);
        assert!(result.columns[3].nullable);
        assert_eq!(result.rows[0][0], Value::Int(42));
        assert_eq!(
            result.rows[0][1],
            Value::DateTime("2026-08-17T12:34:56.789Z".parse().expect("valid timestamp"))
        );
        assert_eq!(
            result.rows[0][2],
            Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        );
        assert_eq!(result.rows[0][3], Value::Null);

        let page = connection.execute(
            &QueryRequest::new("SELECT number FROM numbers(6) ORDER BY number")
                .with_limit(2)
                .with_offset(2),
        )?;
        assert_eq!(page.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
        assert_eq!(page.columns[0].name, "number");
        assert_eq!(page.columns[0].kind, ColumnKind::Integer);

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn clickhouse_paginates_ordered_views() -> Result<(), DbError> {
    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;
        connection.execute(&QueryRequest::new(
            "CREATE VIEW dbflux_live_pagination_view AS SELECT intDiv(number, 10) AS tens, max(number) AS maximum FROM numbers(500) GROUP BY tens",
        ))?;

        let page = connection.execute(
            &QueryRequest::new(
                "SELECT * FROM dbflux_live_pagination_view ORDER BY tens ASC LIMIT 100",
            )
            .with_limit(6)
            .with_offset(5),
        )?;

        assert_eq!(page.rows.len(), 6);
        assert_eq!(
            page.rows
                .iter()
                .map(|row| row[0].clone())
                .collect::<Vec<_>>(),
            (5..=10).map(Value::Int).collect::<Vec<_>>()
        );
        connection.execute(&QueryRequest::new("DROP VIEW dbflux_live_pagination_view"))?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn clickhouse_introspects_table_details_and_storage_hints() -> Result<(), DbError> {
    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE dbflux_live_test (id UInt64, ts DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY (ts, id)",
        ))?;
        let _cleanup = TableCleanup {
            connection: &*connection,
            table: "dbflux_live_test",
        };

        let details = connection.table_details(&config.database, None, "dbflux_live_test")?;
        let columns = details.columns.expect("columns should be loaded");
        assert_eq!(details.name, "dbflux_live_test");
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].type_name, "UInt64");
        assert_eq!(columns[1].name, "ts");
        assert_eq!(columns[1].type_name, "DateTime64(3, 'UTC')");
        assert!(
            details
                .storage_hints
                .expect("storage hints should be loaded")
                .iter()
                .any(|hint| hint.label == "Engine" && hint.detail.as_deref() == Some("MergeTree"))
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn clickhouse_rejects_query_parameters_and_multiple_statements() -> Result<(), DbError> {
    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;
        let mut parameterized = QueryRequest::new("SELECT ?");
        parameterized.params.push(Value::Int(1));
        let parameter_error = connection
            .execute(&parameterized)
            .expect_err("QueryRequest parameters must be rejected");
        assert!(matches!(
            parameter_error,
            DbError::NotSupported(ref message)
                if message == "ClickHouse HTTP queries do not support QueryRequest parameters"
        ));

        let statement_error = connection
            .execute(&QueryRequest::new("SELECT 1; SELECT 2"))
            .expect_err("multiple statements must be rejected");
        assert!(matches!(
            statement_error,
            DbError::QueryFailed(ref formatted)
                if formatted.message.to_ascii_lowercase().contains("multi-statements")
        ));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn clickhouse_probe_write_privilege_default_user_is_writable() -> Result<(), DbError> {
    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;

        // `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` (set by `with_clickhouse`)
        // makes the container's default user an implicit admin with full
        // grants, so `system.grants` (direct + `system.current_roles` join)
        // and `getSetting('readonly')` should resolve to `Writable`.
        assert_eq!(
            connection.probe_write_privilege(),
            dbflux_core::WritePrivilege::Writable
        );

        Ok(())
    })
}

fn metric_request(metric_id: &str) -> QueryRequest {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as i64;

    QueryRequest {
        execution_context: Some(ExecutionContext {
            source: Some(ExecutionSourceContext::InstanceMetricQuery {
                metric_id: metric_id.to_string(),
                start_ms: now_ms - 60_000,
                end_ms: now_ms,
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn inspector_request(metric_id: &str) -> QueryRequest {
    QueryRequest {
        execution_context: Some(ExecutionContext {
            source: Some(ExecutionSourceContext::InstanceInspectorQuery {
                metric_id: metric_id.to_string(),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
#[ignore = "requires Docker daemon"]
fn clickhouse_instance_metrics_have_timestamp_and_float_columns() -> Result<(), DbError> {
    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;

        for metric in
            dbflux_driver_clickhouse::instance_catalog::ClickHouseInstanceCatalog::static_metrics()
        {
            let result = connection.execute(&metric_request(&metric.id))?;

            assert_eq!(
                result.columns.len(),
                2,
                "metric {} must return exactly one timestamp and one value column",
                metric.id
            );
            assert_eq!(
                result.columns[0].kind,
                ColumnKind::Timestamp,
                "metric {} column 0 must be Timestamp",
                metric.id
            );
            assert_eq!(
                result.columns[1].kind,
                ColumnKind::Float,
                "metric {} column 1 must be Float",
                metric.id
            );
            assert_eq!(
                result.rows.len(),
                1,
                "metric {} must return exactly one sample row",
                metric.id
            );
        }

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn clickhouse_processes_inspector_returns_typed_columns() -> Result<(), DbError> {
    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;
        let result = connection.execute(&inspector_request("clickhouse.processes"))?;

        assert_eq!(result.columns.len(), 7);
        assert_eq!(result.columns[0].name, "query_id");
        assert_eq!(result.columns[0].kind, ColumnKind::Text);
        assert_eq!(result.columns[1].name, "user");
        assert_eq!(result.columns[1].kind, ColumnKind::Text);
        assert_eq!(result.columns[2].name, "address");
        assert_eq!(result.columns[2].kind, ColumnKind::Text);
        assert_eq!(result.columns[3].name, "elapsed_secs");
        assert_eq!(result.columns[3].kind, ColumnKind::Float);
        assert_eq!(result.columns[4].name, "read_rows");
        assert_eq!(result.columns[4].kind, ColumnKind::Float);
        assert_eq!(result.columns[5].name, "memory_usage_bytes");
        assert_eq!(result.columns[5].kind, ColumnKind::Float);
        assert_eq!(result.columns[6].name, "query_preview");
        assert_eq!(result.columns[6].kind, ColumnKind::Text);

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn clickhouse_instance_metric_query_rejects_unknown_metric_id() -> Result<(), DbError> {
    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;
        let error = connection
            .execute(&metric_request("clickhouse.does_not_exist"))
            .expect_err("unknown metric id must be rejected");

        assert!(matches!(error, DbError::NotSupported(_)));

        Ok(())
    })
}

/// Guards the assumption every declared metric rests on: that its raw name
/// exists in the system table it is read from.
///
/// A metric whose row is missing reports `0.0` rather than failing, so a
/// misspelled name would chart a flat zero forever and every shape assertion
/// above would still pass. This asserts the name itself.
#[test]
#[ignore = "requires Docker daemon"]
fn clickhouse_metric_raw_names_exist_in_their_system_tables() -> Result<(), DbError> {
    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;

        for (raw_name, metric_id, .., source) in METRIC_DEFS {
            let (table, key_column) = match source {
                MetricSource::Metrics => ("system.metrics", "metric"),
                MetricSource::Events => ("system.events", "event"),
                MetricSource::AsynchronousMetrics => ("system.asynchronous_metrics", "metric"),
            };

            let result = connection.execute(&QueryRequest::new(format!(
                "SELECT count() FROM {table} WHERE {key_column} = '{raw_name}'"
            )))?;

            let count = match result.rows.first().and_then(|row| row.first()) {
                Some(Value::Int(count)) => *count,
                Some(Value::Decimal(text)) => text.parse().unwrap_or(0),
                other => panic!("unexpected count value for {metric_id}: {other:?}"),
            };

            assert_eq!(
                count, 1,
                "metric {metric_id} declares raw name '{raw_name}', absent from {table}"
            );
        }

        Ok(())
    })
}
