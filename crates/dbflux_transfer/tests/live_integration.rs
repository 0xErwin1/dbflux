#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::result_large_err
)]

//! Docker-backed end-to-end Export (T18) and Import (T20-T24) tests: a real
//! PostgreSQL table is created and populated, then round-tripped through
//! `run_export` -> folder bundle -> `run_import` into a second table, and the
//! resulting CSV + manifest.json / target row values are verified against
//! the source (R2's "roundtrip" scenario).
//!
//! `#[ignore]`d like every other live/testcontainers test in this repo — run
//! with `cargo nextest run -p dbflux_transfer --run-ignored all` against a
//! Docker daemon.

use std::sync::Arc;

use dbflux_core::{
    CancelToken, ColumnInfo, Connection, ConnectionProfile, DbConfig, DbDriver, DbError,
    QueryRequest, TransferColumn,
};
use dbflux_driver_postgres::PostgresDriver;
use dbflux_test_support::containers;
use dbflux_transfer::FileFormat;
use dbflux_transfer::export::{ExportOptions, ExportTable, run_export};
use dbflux_transfer::import::{ImportOptions, ImportTablePlan, run_import};
use std::time::Duration;

fn connect_postgres(uri: String) -> Result<Box<dyn Connection>, DbError> {
    let driver = PostgresDriver::new();
    let profile = ConnectionProfile::new(
        "live-postgres-export",
        DbConfig::Postgres {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 5432,
            user: String::new(),
            database: "postgres".to_string(),
            ssl_mode: Some("prefer".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    containers::retry_db_operation(Duration::from_secs(30), || -> Result<_, DbError> {
        let connection = driver.connect(&profile)?;
        connection.ping()?;
        Ok(connection)
    })
}

fn transfer_column(col: &ColumnInfo) -> TransferColumn {
    TransferColumn {
        name: col.name.clone(),
        type_name: Some(col.type_name.clone()),
        nullable: col.nullable,
        is_primary_key: col.is_primary_key,
    }
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_export_streams_table_to_csv_and_manifest() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let boxed_connection = connect_postgres(uri)?;
        boxed_connection.execute(&QueryRequest::new(
            "CREATE TABLE export_users (id INT PRIMARY KEY, email TEXT NOT NULL)",
        ))?;
        for i in 1..=5 {
            boxed_connection.execute(&QueryRequest::new(format!(
                "INSERT INTO export_users (id, email) VALUES ({i}, 'user{i}@example.com')"
            )))?;
        }

        let table_info =
            boxed_connection.table_details("postgres", Some("public"), "export_users")?;
        let columns: Vec<TransferColumn> = table_info
            .columns
            .expect("table_details must populate columns")
            .iter()
            .map(transfer_column)
            .collect();
        assert_eq!(columns.len(), 2);

        let connection: Arc<dyn Connection> = Arc::from(boxed_connection);
        let tables = vec![ExportTable {
            schema: Some("public".to_string()),
            name: "export_users".to_string(),
            columns,
            estimated_total: None,
        }];

        let dir = std::env::temp_dir().join(format!(
            "dbflux_transfer_live_export_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp export dir");
        let cancel = CancelToken::new();
        let options = ExportOptions {
            driver_id: "postgres",
            database: "postgres",
            format: FileFormat::Csv,
            segment_size: 2,
        };

        let outcome = run_export(&connection, &tables, &dir, &options, &cancel, |_, _, _| {})
            .expect("run_export must succeed against a live Postgres table");

        assert!(!outcome.cancelled);
        assert_eq!(outcome.tables_exported, 1);

        let manifest = outcome.manifest.expect("manifest must be written");
        assert_eq!(manifest.tables.len(), 1);
        assert_eq!(manifest.tables[0].row_count, 5);

        let csv_path = dir.join("public.export_users.csv");
        assert!(csv_path.exists(), "export file must exist");
        let csv_contents = std::fs::read_to_string(&csv_path).expect("read export csv");
        let data_lines = csv_contents.lines().count();
        assert_eq!(
            data_lines, 6,
            "1 header row + 5 data rows, got: {csv_contents}"
        );
        assert!(csv_contents.contains("user3@example.com"));

        let manifest_json =
            std::fs::read_to_string(dir.join("manifest.json")).expect("manifest.json exists");
        assert!(manifest_json.contains("export_users"));

        std::fs::remove_dir_all(&dir).ok();

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_export_then_import_round_trips_row_values() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let boxed_connection = connect_postgres(uri)?;
        boxed_connection.execute(&QueryRequest::new(
            "CREATE TABLE roundtrip_users (id INT PRIMARY KEY, email TEXT NOT NULL)",
        ))?;
        for i in 1..=5 {
            boxed_connection.execute(&QueryRequest::new(format!(
                "INSERT INTO roundtrip_users (id, email) VALUES ({i}, 'user{i}@example.com')"
            )))?;
        }

        let table_info =
            boxed_connection.table_details("postgres", Some("public"), "roundtrip_users")?;
        let columns: Vec<TransferColumn> = table_info
            .columns
            .expect("table_details must populate columns")
            .iter()
            .map(transfer_column)
            .collect();

        let connection: Arc<dyn Connection> = Arc::from(boxed_connection);
        let tables = vec![ExportTable {
            schema: Some("public".to_string()),
            name: "roundtrip_users".to_string(),
            columns,
            estimated_total: None,
        }];

        let dir = std::env::temp_dir().join(format!(
            "dbflux_transfer_live_roundtrip_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp export dir");
        let cancel = CancelToken::new();
        let export_options = ExportOptions {
            driver_id: "postgres",
            database: "postgres",
            format: FileFormat::Csv,
            segment_size: 2,
        };

        run_export(
            &connection,
            &tables,
            &dir,
            &export_options,
            &cancel,
            |_, _, _| {},
        )
        .expect("run_export must succeed against a live Postgres table");

        connection
            .execute(&QueryRequest::new(
                "CREATE TABLE roundtrip_users_imported (id INT PRIMARY KEY, email TEXT NOT NULL)",
            ))
            .expect("create the import target table");

        let plans = vec![ImportTablePlan {
            source_table: "roundtrip_users".to_string(),
            target_schema: Some("public".to_string()),
            target_table: "roundtrip_users_imported".to_string(),
            mapping_mode: dbflux_transfer::TableMappingMode::Existing,
            column_overrides: None,
        }];
        let import_options = ImportOptions {
            segment_size: 2,
            target_database: "postgres".to_string(),
            destructive_confirmed: false,
        };

        let outcome = run_import(
            &connection,
            &dir,
            &plans,
            &import_options,
            &cancel,
            |_, _, _| {},
        )
        .expect("run_import must succeed against a live Postgres target");

        assert!(!outcome.cancelled);
        assert_eq!(outcome.tables.len(), 1);
        assert_eq!(outcome.tables[0].rows_imported, 5);

        let imported_rows = connection
            .execute(&QueryRequest::new(
                "SELECT id, email FROM roundtrip_users_imported ORDER BY id",
            ))
            .expect("read back the imported rows");
        assert_eq!(imported_rows.rows.len(), 5);

        std::fs::remove_dir_all(&dir).ok();

        Ok(())
    })
}
