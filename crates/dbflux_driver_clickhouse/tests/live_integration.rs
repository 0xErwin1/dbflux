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
use dbflux_core::{ColumnKind, Connection, ConnectionProfile, DbConfig, DbDriver, DbError};
use dbflux_core::{QueryRequest, Value};
use dbflux_driver_clickhouse::ClickHouseDriver;
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
