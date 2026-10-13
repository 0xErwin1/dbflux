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
use dbflux_core::{QueryRequest, ReadOnlyEnforcement, Value};
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
fn clickhouse_query_safety_rejects_explicit_controls_before_effects() -> Result<(), DbError> {
    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE dbflux_live_query_safety (id UInt64) ENGINE = MergeTree ORDER BY id",
        ))?;
        let _cleanup = TableCleanup {
            connection: connection.as_ref(),
            table: "dbflux_live_query_safety",
        };

        for request in [
            {
                let mut request =
                    QueryRequest::new("INSERT INTO dbflux_live_query_safety VALUES (3)");
                request.statement_timeout = Some(Duration::from_secs(1));
                request
            },
            {
                let mut request = metric_request("clickhouse.inserted_rows").with_limit(0);
                request.sql = "INSERT INTO dbflux_live_query_safety VALUES (4)".to_string();
                request
            },
            {
                let mut request = inspector_request("clickhouse.processes").with_limit(0);
                request.sql = "INSERT INTO dbflux_live_query_safety VALUES (5)".to_string();
                request
            },
        ] {
            assert!(
                matches!(connection.execute(&request), Err(DbError::NotSupported(_))),
                "protected query must be rejected before execution: {:?}",
                request.sql
            );
        }

        let count = connection.execute(&QueryRequest::new(
            "SELECT count() FROM dbflux_live_query_safety",
        ))?;
        assert_eq!(count.rows, vec![vec![Value::Int(0)]]);

        let insert = connection.execute(
            &QueryRequest::new("INSERT INTO dbflux_live_query_safety VALUES (1), (2)")
                .with_limit(0),
        )?;
        assert!(insert.rows.is_empty());
        assert!(!insert.rows_truncated());
        let count = connection.execute(&QueryRequest::new(
            "SELECT count() FROM dbflux_live_query_safety",
        ))?;
        assert_eq!(count.rows, vec![vec![Value::Int(2)]]);
        Ok(())
    })
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

        let bounded =
            QueryRequest::new("SELECT number FROM numbers(6) ORDER BY number").with_limit(2);
        let bounded = connection.execute(&bounded)?;
        assert_eq!(bounded.rows, vec![vec![Value::Int(0)], vec![Value::Int(1)]]);
        assert!(bounded.rows_truncated());

        let within_budget =
            QueryRequest::new("SELECT number FROM numbers(6) ORDER BY number LIMIT 3")
                .with_limit(5);
        let within_budget = connection.execute(&within_budget)?;
        assert_eq!(within_budget.rows.len(), 3);
        assert!(!within_budget.rows_truncated());

        let zero = connection
            .execute(&QueryRequest::new("SELECT number FROM numbers(6)").with_limit(0))?;
        assert!(zero.rows.is_empty());
        assert_eq!(zero.columns.len(), 1);
        assert!(zero.rows_truncated());

        // Far larger than the 128 MiB response cap that applies to unlimited
        // requests; a row-limited request reads only past the limit.
        let oversized = connection.execute(
            &QueryRequest::new("SELECT number, repeat('x', 1000) FROM numbers(2000000)")
                .with_limit(5),
        )?;
        assert_eq!(oversized.rows.len(), 5);
        assert!(oversized.rows_truncated());

        let read_only_oversized = connection.execute(
            &QueryRequest::new("SELECT number, repeat('x', 1000) FROM numbers(2000000)")
                .with_limit(5)
                .with_read_only(ReadOnlyEnforcement::Required),
        )?;
        assert_eq!(read_only_oversized.rows.len(), 5);
        assert!(read_only_oversized.rows_truncated());

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

        let bounded = QueryRequest::new(
            "SELECT * FROM dbflux_live_pagination_view ORDER BY tens ASC LIMIT 100",
        )
        .with_limit(6);
        let bounded = connection.execute(&bounded)?;
        assert_eq!(bounded.rows.len(), 6);
        assert!(bounded.rows_truncated());
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
///
/// `system.events` only lists events that have already fired at least once on
/// this server, so the counters are exercised first: a fresh container has
/// never inserted a row, and `InsertedRows` would be genuinely absent through
/// no fault of the metric name.
#[test]
#[ignore = "requires Docker daemon"]
fn clickhouse_metric_raw_names_exist_in_their_system_tables() -> Result<(), DbError> {
    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE dbflux_live_events_probe (id UInt64) ENGINE = MergeTree ORDER BY id",
        ))?;
        let _cleanup = TableCleanup {
            connection: connection.as_ref(),
            table: "dbflux_live_events_probe",
        };
        connection.execute(&QueryRequest::new(
            "INSERT INTO dbflux_live_events_probe VALUES (1)",
        ))?;
        connection.execute(&QueryRequest::new(
            "SELECT count() FROM dbflux_live_events_probe",
        ))?;

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

fn read_only_request(sql: &str) -> QueryRequest {
    QueryRequest::new(sql).with_read_only(ReadOnlyEnforcement::Required)
}

fn read_only_item_count(connection: &dyn Connection) -> Result<usize, DbError> {
    Ok(connection
        .execute(&QueryRequest::new("SELECT id FROM dbflux_live_read_only"))?
        .rows
        .len())
}

#[test]
#[ignore = "requires Docker daemon"]
fn clickhouse_read_only_requests_cannot_write() -> Result<(), DbError> {
    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE dbflux_live_read_only (id UInt64) ENGINE = MergeTree ORDER BY id",
        ))?;
        let _cleanup = TableCleanup {
            connection: connection.as_ref(),
            table: "dbflux_live_read_only",
        };
        connection.execute(&QueryRequest::new(
            "INSERT INTO dbflux_live_read_only VALUES (1)",
        ))?;

        for sql in [
            "DELETE FROM dbflux_live_read_only WHERE 1 = 1",
            "INSERT INTO dbflux_live_read_only SELECT id + 1 FROM dbflux_live_read_only",
            "ALTER TABLE dbflux_live_read_only DELETE WHERE 1 = 1",
        ] {
            let error = connection
                .execute(&read_only_request(sql))
                .expect_err("a write must fail under readonly = 2");
            assert!(
                error.to_string().contains("readonly"),
                "{sql}: expected a read-only error, got {error}"
            );
            assert_eq!(read_only_item_count(connection.as_ref())?, 1, "{sql}");
        }

        for sql in [
            "SELECT 1 SETTINGS readonly = 0",
            "SET readonly = 0",
            "INSERT INTO dbflux_live_read_only SELECT 2 SETTINGS readonly = 0",
        ] {
            let error = connection
                .execute(&read_only_request(sql))
                .expect_err("a read-only request cannot lift readonly");
            assert!(
                error.to_string().contains("readonly"),
                "{sql}: expected a read-only error, got {error}"
            );
            assert_eq!(read_only_item_count(connection.as_ref())?, 1, "{sql}");
        }

        let read =
            connection.execute(&read_only_request("SELECT id FROM dbflux_live_read_only"))?;
        assert_eq!(read.rows.len(), 1);

        connection.execute(&QueryRequest::new(
            "CREATE USER IF NOT EXISTS dbflux_reader IDENTIFIED BY 'reader' SETTINGS readonly = 2",
        ))?;
        connection.execute(&QueryRequest::new(format!(
            "GRANT SELECT ON {}.* TO dbflux_reader",
            config.database
        )))?;
        let reader = connect(&ClickHouseConfig {
            endpoint: config.endpoint.clone(),
            user: "dbflux_reader".to_string(),
            password: "reader".to_string(),
            database: config.database.clone(),
        })?;
        let read = reader.execute(&read_only_request("SELECT id FROM dbflux_live_read_only"))?;
        assert_eq!(read.rows.len(), 1, "a readonly = 2 user can still read");

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn clickhouse_table_views_read_metadata_partitions_estimates_and_samples() -> Result<(), DbError> {
    use dbflux_core::{
        ColumnBadge, EstimateScope, PartUnit, ProfileSource, TableBrowseRequest, TableFactKind,
        TableRef, TableViewFeatures,
    };

    containers::with_clickhouse(|config| {
        let connection = connect(&config)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE dbflux_live_page_views (\
             site_id UInt32, ts DateTime, user_id UInt64, path String CODEC(ZSTD(3)), \
             duration_ms Nullable(UInt32)) \
             ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (site_id, toDate(ts), user_id) \
             TTL ts + INTERVAL 50 YEAR",
        ))?;
        let _cleanup = TableCleanup {
            connection: connection.as_ref(),
            table: "dbflux_live_page_views",
        };
        connection.execute(&QueryRequest::new(
            "INSERT INTO dbflux_live_page_views \
             SELECT number % 7, toDateTime('2026-08-01 00:00:00') + number * 60, number, \
             concat('/p/', toString(number % 50)), if(number % 10 = 0, NULL, number % 1000) \
             FROM numbers(100000)",
        ))?;

        let features = connection.table_view_features();
        assert!(
            features.contains(TableViewFeatures::COLUMN_PROFILE | TableViewFeatures::PARTITIONS)
        );

        let table = TableRef::with_schema(config.database.clone(), "dbflux_live_page_views");
        let profile = connection.table_profile(&table)?;

        assert_eq!(
            profile.source_label,
            ProfileSource::Catalog("system.columns")
        );
        assert_eq!(profile.row_count, Some(100_000));
        assert!(profile.total_compressed_bytes.unwrap_or(0) > 0);
        assert_eq!(profile.engine.as_deref(), Some("MergeTree"));
        assert!(
            profile
                .facts
                .iter()
                .any(|fact| fact.kind == TableFactKind::Ttl)
        );

        let names: Vec<&str> = profile
            .columns
            .iter()
            .map(|column| column.name.as_ref())
            .collect();
        assert_eq!(
            names,
            vec!["site_id", "ts", "user_id", "path", "duration_ms"]
        );
        assert_eq!(
            profile.columns[0].badges,
            vec![ColumnBadge::SortKey { position: Some(1) }]
        );
        assert_eq!(
            profile.columns[1].badges,
            vec![
                ColumnBadge::SortKey { position: Some(2) },
                ColumnBadge::PartitionKey
            ]
        );
        assert_eq!(profile.columns[3].codec.as_deref(), Some("ZSTD(3)"));
        assert!(
            profile
                .columns
                .iter()
                .all(|column| column.compressed_bytes.is_some())
        );

        let partitions = connection.table_partitions(&table)?;
        assert_eq!(partitions.key.as_deref(), Some("toYYYYMM(ts)"));
        let partition_names: Vec<&str> = partitions
            .partitions
            .iter()
            .map(|partition| partition.name.as_ref())
            .collect();
        assert_eq!(partition_names, vec!["202610", "202609", "202608"]);
        assert_eq!(
            partitions
                .partitions
                .iter()
                .map(|partition| partition.rows.unwrap_or(0))
                .sum::<u64>(),
            100_000
        );
        assert!(partitions.partitions[2].range.is_some());

        let request = TableBrowseRequest::new(table.clone())
            .with_filter("site_id = 3")
            .with_columns(vec!["ts".to_string(), "path".to_string()]);
        let estimate = connection.estimate_table_read(&request, &profile)?;
        let EstimateScope::Parts {
            touched,
            total,
            unit,
        } = estimate.scopes[1];
        assert_eq!(unit, PartUnit::Granules);
        assert!(touched > 0 && touched <= total, "{touched} of {total}");
        assert_eq!(
            estimate
                .per_column
                .iter()
                .map(|(index, _)| *index)
                .collect::<Vec<_>>(),
            vec![1, 3],
            "only the projected columns cost bytes"
        );

        let page = connection.browse_table(&request)?;
        assert_eq!(
            page.columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["ts", "path"]
        );

        let columns: Vec<String> = names.iter().map(|name| name.to_string()).collect();
        let capped = connection.sample_table_profile(&table, &columns, 1_000)?;
        assert_eq!(capped.source_label, ProfileSource::Sample { rows: 1_000 });

        let sample = connection.sample_table_profile(&table, &columns, 1_000_000)?;
        assert_eq!(sample.source_label, ProfileSource::Sample { rows: 100_000 });
        assert_eq!(sample.columns[4].null_count, Some(10_000));
        assert_eq!(
            sample.columns[0]
                .range
                .as_ref()
                .map(|range| range.max.as_ref()),
            Some("6")
        );
        assert!(
            sample.columns[3]
                .distinct_count
                .is_some_and(|count| (45..=55).contains(&count))
        );

        Ok(())
    })
}
