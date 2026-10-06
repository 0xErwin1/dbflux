#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err,
    clippy::unwrap_in_result
)]

use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, IndexData, OwnedDefaultSpec, QueryRequest,
    TableAlterExpectedColumn, TableAlterOperation, TableAlterRequest, TableAlterRoute, TableRef,
    Value,
};
use dbflux_driver_sqlite::SqliteDriver;
use dbflux_test_support::ddl_fixtures::SqliteFixtures;
use std::path::PathBuf;

mod alter_table;
mod create_index;
mod create_table;
mod create_view;
mod delete;
mod drop;
mod error_scenarios;
mod rebuild_catalog;
mod rebuild_execution;

fn connect_sqlite() -> Result<(Box<dyn dbflux_core::Connection>, SqliteDriver, PathBuf), DbError> {
    let driver = SqliteDriver::new();

    // Exclusive creation guarantees each test its own file; a timestamp name
    // can repeat across threads on a coarse clock.
    let db_path = tempfile::Builder::new()
        .prefix("test_ddl_")
        .suffix(".db")
        .tempfile()
        .and_then(|file| file.into_temp_path().keep().map_err(|error| error.error))
        .map_err(|error| {
            DbError::query_failed(format!("failed to create test database file: {error}"))
        })?;

    let profile = ConnectionProfile::new(
        "ddl-sqlite",
        DbConfig::SQLite {
            path: db_path.clone(),
            connection_id: None,
        },
    );

    let connection = driver.connect(&profile)?;
    connection.ping()?;

    Ok((connection, driver, db_path))
}

fn cleanup_test_tables(conn: &dyn dbflux_core::Connection) {
    conn.execute(&QueryRequest::new("PRAGMA foreign_keys = OFF"))
        .ok();

    let tables = vec![
        "orders",
        "order_items",
        "users",
        "products",
        "accounts",
        "alter_test",
        "fk_parent",
        "fk_child",
        "truncate_test",
    ];

    for table in tables {
        let _ = conn.execute(&QueryRequest::new(format!(
            "DROP TABLE IF EXISTS {}",
            table
        )));
    }

    let views = vec!["active_users", "test_view"];
    for view in views {
        let _ = conn.execute(&QueryRequest::new(format!("DROP VIEW IF EXISTS {}", view)));
    }

    conn.execute(&QueryRequest::new("PRAGMA foreign_keys = ON"))
        .ok();
}

#[derive(Debug, PartialEq)]
struct RebuildBoundarySnapshot {
    databases: Vec<Vec<Value>>,
    main_catalog: Vec<Vec<Value>>,
    temp_catalog: Vec<Vec<Value>>,
    target_schema: Vec<Vec<Value>>,
    target_rows: Vec<Vec<Value>>,
    foreign_keys: Vec<Vec<Value>>,
    deferred_foreign_keys: Vec<Vec<Value>>,
    main_schema_version: Vec<Vec<Value>>,
    temp_schema_version: Vec<Vec<Value>>,
}

fn rebuild_boundary_snapshot(
    connection: &dyn dbflux_core::Connection,
    table: &str,
) -> Result<RebuildBoundarySnapshot, DbError> {
    // SQLite initializes the temp catalog lazily, so do that before schema baselines.
    connection.execute(&QueryRequest::new(
        "SELECT name FROM temp.sqlite_temp_master LIMIT 0",
    ))?;
    let query_rows = |sql: String| -> Result<Vec<Vec<Value>>, DbError> {
        Ok(connection.execute(&QueryRequest::new(sql))?.rows)
    };
    Ok(RebuildBoundarySnapshot {
        databases: query_rows("PRAGMA database_list".to_string())?,
        main_catalog: query_rows(
            "SELECT type, name, tbl_name, rootpage, sql \
             FROM main.sqlite_master \
             ORDER BY type, name, tbl_name, rootpage, sql"
                .to_string(),
        )?,
        temp_catalog: query_rows(
            "SELECT type, name, tbl_name, rootpage, sql \
             FROM temp.sqlite_temp_master \
             ORDER BY type, name, tbl_name, rootpage, sql"
                .to_string(),
        )?,
        target_schema: query_rows(format!(
            "SELECT type, name, tbl_name, rootpage, sql \
             FROM main.sqlite_master \
             WHERE type = 'table' AND name = '{table}'"
        ))?,
        target_rows: query_rows(format!("SELECT rowid, * FROM main.{table} ORDER BY rowid"))?,
        foreign_keys: query_rows("PRAGMA foreign_keys".to_string())?,
        deferred_foreign_keys: query_rows("PRAGMA defer_foreign_keys".to_string())?,
        main_schema_version: query_rows("PRAGMA main.schema_version".to_string())?,
        temp_schema_version: query_rows("PRAGMA temp.schema_version".to_string())?,
    })
}
