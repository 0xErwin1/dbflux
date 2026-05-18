#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{
    CollectionRef, Connection, ConnectionProfile, DbConfig, DbDriver, DbError, DescribeRequest,
    ExplainRequest, OrderByColumn, Pagination, QueryRequest, RecordIdentity, RowDelete, RowInsert,
    RowPatch, SchemaLoadingStrategy, TableBrowseRequest, TableCountRequest, TableRef, Value,
};
use dbflux_driver_mssql::MssqlDriver;
use dbflux_test_support::containers;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const TEST_DATABASE: &str = "dbflux_test";

fn connect_mssql(uri: String) -> Result<(Box<dyn Connection>, MssqlDriver), DbError> {
    let driver = MssqlDriver::new();
    let profile = ConnectionProfile::new(
        "live-mssql",
        DbConfig::SqlServer {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 1433,
            user: String::new(),
            database: None,
            instance: None,
            ssl_mode: Some("on".to_string()),
            trust_server_certificate: true,
            ssl_root_cert_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    // SQL Server takes a few extra seconds to be ready for client logins
    // even after the container's "ready for client connections" log line.
    let connection =
        containers::retry_db_operation(Duration::from_secs(60), || -> Result<_, DbError> {
            let connection = driver.connect(&profile)?;
            connection.ping()?;
            Ok(connection)
        })?;

    // Create a clean per-test database so the implicit `list_databases()`
    // filter (which hides master/tempdb/model/msdb) returns something
    // non-empty, and so test objects don't pile up in `master`.
    connection.execute(&QueryRequest::new(format!(
        "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = '{TEST_DATABASE}') \
         CREATE DATABASE [{TEST_DATABASE}]"
    )))?;
    connection.set_active_database(Some(TEST_DATABASE))?;

    Ok((connection, driver))
}

fn cleanup_test_tables(conn: &dyn Connection) {
    // Drop in FK order — children first.
    for table in [
        "orders",
        "order_items",
        "products",
        "accounts",
        "users",
        "crud_test",
        "browse_test",
        "explain_test",
        "describe_test",
        "codegen_test",
        "cancel_test",
    ] {
        let _ = conn.execute(&QueryRequest::new(format!(
            "DROP TABLE IF EXISTS dbo.[{}]",
            table
        )));
    }
}

// ---------------------------------------------------------------------------
// Basic connectivity
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_live_connect_ping_query_and_schema() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;

        let result = connection.execute(&QueryRequest::new("SELECT 1 AS one"))?;
        assert_eq!(result.rows.len(), 1);

        assert_eq!(
            connection.schema_loading_strategy(),
            SchemaLoadingStrategy::LazyPerDatabase
        );

        let databases = connection.list_databases()?;
        assert!(
            databases.iter().any(|d| d.name == TEST_DATABASE),
            "test database should be visible in list_databases"
        );

        let schema = connection.schema()?;
        assert!(schema.is_relational());

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Schema introspection
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_schema_introspection() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        connection.execute(&QueryRequest::new(
            "CREATE TABLE users (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(100) NOT NULL,
                email NVARCHAR(255) UNIQUE,
                age INT DEFAULT 0
            )",
        ))?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE orders (
                id INT IDENTITY(1,1) PRIMARY KEY,
                user_id INT NOT NULL,
                amount DECIMAL(10, 2) NOT NULL,
                CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )",
        ))?;

        connection.execute(&QueryRequest::new(
            "CREATE INDEX idx_orders_user_id ON orders(user_id)",
        ))?;

        let table = connection.table_details(TEST_DATABASE, Some("dbo"), "users")?;
        assert_eq!(table.name, "users");

        let columns = table.columns.as_ref().expect("columns should be loaded");
        assert!(columns.len() >= 4);

        let id_col = columns.iter().find(|c| c.name == "id").expect("id column");
        assert!(id_col.is_primary_key);
        assert!(!id_col.nullable);

        let name_col = columns
            .iter()
            .find(|c| c.name == "name")
            .expect("name column");
        assert!(!name_col.nullable);

        let email_col = columns
            .iter()
            .find(|c| c.name == "email")
            .expect("email column");
        assert!(email_col.nullable);

        let orders_table = connection.table_details(TEST_DATABASE, Some("dbo"), "orders")?;
        let fks = orders_table
            .foreign_keys
            .as_ref()
            .expect("foreign keys should be loaded");
        assert!(!fks.is_empty());
        let fk = &fks[0];
        assert_eq!(fk.referenced_table, "users");
        assert_eq!(fk.columns, vec!["user_id"]);
        assert_eq!(fk.referenced_columns, vec!["id"]);

        let schema_features = connection.schema_features();
        assert!(!schema_features.is_empty());

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// CRUD operations with OUTPUT clauses
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_crud_operations_with_output() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        connection.execute(&QueryRequest::new(
            "CREATE TABLE crud_test (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(100) NOT NULL,
                value INT DEFAULT 0
            )",
        ))?;

        // INSERT — OUTPUT INSERTED.* should round-trip the row back.
        let insert_result = connection.insert_row(&RowInsert::new(
            "crud_test".to_string(),
            Some("dbo".to_string()),
            vec!["name".to_string(), "value".to_string()],
            vec![Value::Text("alice".to_string()), Value::Int(42)],
        ))?;
        assert_eq!(insert_result.affected_rows, 1);
        assert!(
            insert_result.returning_row.is_some(),
            "INSERT with OUTPUT should return the inserted row"
        );

        // Verify it's actually there.
        let rows = connection
            .execute(&QueryRequest::new(
                "SELECT id, name, value FROM crud_test WHERE name = N'alice'",
            ))?
            .rows;
        assert_eq!(rows.len(), 1);
        let inserted_id = match &rows[0][0] {
            Value::Int(n) => *n,
            other => panic!("expected Int id, got {:?}", other),
        };

        // UPDATE by PK — OUTPUT INSERTED.* should return the post-update row.
        let update_result = connection.update_row(&RowPatch::new(
            RecordIdentity::composite(vec!["id".to_string()], vec![Value::Int(inserted_id)]),
            "crud_test".to_string(),
            Some("dbo".to_string()),
            vec![("value".to_string(), Value::Int(99))],
        ))?;
        assert_eq!(update_result.affected_rows, 1);
        assert!(update_result.returning_row.is_some());

        let rows = connection
            .execute(&QueryRequest::new(
                "SELECT value FROM crud_test WHERE name = N'alice'",
            ))?
            .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int(99));

        // DELETE — OUTPUT DELETED.* should return the removed row.
        let delete_result = connection.delete_row(&RowDelete::new(
            RecordIdentity::composite(vec!["id".to_string()], vec![Value::Int(inserted_id)]),
            "crud_test".to_string(),
            Some("dbo".to_string()),
        ))?;
        assert_eq!(delete_result.affected_rows, 1);
        assert!(delete_result.returning_row.is_some());

        let rows = connection
            .execute(&QueryRequest::new("SELECT * FROM crud_test"))?
            .rows;
        assert!(rows.is_empty());

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Browse and count via OFFSET/FETCH NEXT
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_browse_and_count() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        connection.execute(&QueryRequest::new(
            "CREATE TABLE browse_test (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(50) NOT NULL
            )",
        ))?;

        for i in 1..=25 {
            connection.execute(&QueryRequest::new(format!(
                "INSERT INTO browse_test (name) VALUES (N'item_{}')",
                i
            )))?;
        }

        let table_ref = TableRef::with_schema("dbo", "browse_test");

        let count = connection.count_table(&TableCountRequest::new(table_ref.clone()))?;
        assert_eq!(count, 25);

        let filtered_count = connection.count_table(
            &TableCountRequest::new(table_ref.clone()).with_filter("name LIKE N'item_1%'"),
        )?;
        assert!(filtered_count > 0);
        assert!(filtered_count < 25);

        let page1 = connection.browse_table(
            &TableBrowseRequest::new(table_ref.clone())
                .with_pagination(Pagination::Offset {
                    limit: 10,
                    offset: 0,
                })
                .with_order_by(vec![OrderByColumn::asc("id")]),
        )?;
        assert_eq!(page1.rows.len(), 10);

        let page2 = connection.browse_table(
            &TableBrowseRequest::new(table_ref.clone())
                .with_pagination(Pagination::Offset {
                    limit: 10,
                    offset: 10,
                })
                .with_order_by(vec![OrderByColumn::asc("id")]),
        )?;
        assert_eq!(page2.rows.len(), 10);
        assert_ne!(page1.rows[0], page2.rows[0]);

        let filtered = connection.browse_table(
            &TableBrowseRequest::new(table_ref)
                .with_filter("name = N'item_5'")
                .with_pagination(Pagination::Offset {
                    limit: 100,
                    offset: 0,
                }),
        )?;
        assert_eq!(filtered.rows.len(), 1);

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// EXPLAIN via SET SHOWPLAN_XML
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_explain_returns_xml_plan() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        connection.execute(&QueryRequest::new(
            "CREATE TABLE explain_test (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(50))",
        ))?;

        let table_ref = TableRef::with_schema("dbo", "explain_test");
        let result = connection.explain(&ExplainRequest::new(table_ref))?;
        // SHOWPLAN_XML returns a single-column nvarchar(max) result containing
        // the XML plan. We just assert we got at least one row back.
        assert!(!result.rows.is_empty());

        // After explain the session must still execute normally — i.e. the
        // SET SHOWPLAN_XML OFF reset really happened.
        let normal = connection.execute(&QueryRequest::new("SELECT 1 AS one"))?;
        assert_eq!(normal.rows.len(), 1);

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Describe
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_describe_table() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        connection.execute(&QueryRequest::new(
            "CREATE TABLE describe_test (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(100) NOT NULL,
                active BIT DEFAULT 1
            )",
        ))?;

        let table_ref = TableRef::with_schema("dbo", "describe_test");
        let result = connection.describe_table(&DescribeRequest::new(table_ref))?;
        assert!(result.rows.len() >= 3);

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Code generators
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_code_generators() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        connection.execute(&QueryRequest::new(
            "CREATE TABLE codegen_test (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(100) NOT NULL
            )",
        ))?;

        let table = connection.table_details(TEST_DATABASE, Some("dbo"), "codegen_test")?;

        // The mssql driver supports a fixed set of generators by ID, even if
        // it doesn't enumerate them via `code_generators()`. Verify the
        // canonical IDs round-trip without error.
        for generator_id in [
            "select_star",
            "insert",
            "update",
            "delete",
            "truncate",
            "drop_table",
        ] {
            let code = connection.generate_code(generator_id, &table)?;
            assert!(
                !code.is_empty(),
                "generator '{}' returned empty code",
                generator_id
            );
            assert!(
                code.contains("codegen_test"),
                "generator '{}' should reference the target table",
                generator_id
            );
        }

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Cancellation: KILL + transparent reconnect
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_cancel_query_kills_session_and_reconnects() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;

        // Share the connection with the worker thread that runs the long
        // query. Connection is `Send + Sync` so an `Arc` is enough.
        let conn: Arc<dyn Connection> = Arc::from(connection);
        let worker_conn = Arc::clone(&conn);

        let worker = thread::spawn(move || {
            // WAITFOR DELAY blocks the session for the given duration.
            // The cancel issued below should interrupt it well before this
            // returns naturally.
            worker_conn.execute(&QueryRequest::new("WAITFOR DELAY '00:00:30'"))
        });

        // Give the worker a moment to actually start the query before we
        // KILL the session.
        thread::sleep(Duration::from_millis(500));

        conn.cancel_active()?;

        let worker_result = worker.join().expect("worker thread panicked");
        assert!(
            matches!(worker_result, Err(DbError::Cancelled)),
            "cancelled query should return DbError::Cancelled, got: {:?}",
            worker_result
        );

        // cleanup_after_cancel should rebuild the underlying tiberius
        // client and restore the active database. The connection should be
        // usable again afterward.
        conn.cleanup_after_cancel()?;
        conn.ping()?;

        let result = conn.execute(&QueryRequest::new("SELECT 1 AS one"))?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Int(1));

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Document operations (should return NotSupported)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_document_ops_not_supported() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;

        let browse_result = connection.browse_collection(
            &dbflux_core::CollectionBrowseRequest::new(CollectionRef::new("db", "col")),
        );
        assert!(matches!(browse_result, Err(DbError::NotSupported(_))));

        assert!(connection.key_value_api().is_none());

        Ok(())
    })
}
