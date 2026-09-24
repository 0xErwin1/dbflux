#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{
    CollectionRef, Connection, ConnectionProfile, DbConfig, DbDriver, DbError, DescribeRequest,
    ExecutionContext, ExecutionSourceContext, ExplainRequest, OrderByColumn, Pagination,
    QueryRequest, RecordIdentity, RowDelete, RowInsert, RowPatch, SchemaLoadingStrategy,
    TableBrowseRequest, TableCountRequest, TableRef, TransactionStateNote, Value,
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

/// Parse host and port from a `sqlserver://sa:pw@host:port/db` test URI.
///
/// The container helper hands us a URI; the form-mode and ssl-mode tests need
/// the host and port as separate values to populate `DbConfig::SqlServer`.
fn parse_host_port_from_uri(uri: &str) -> (String, u16) {
    let after_at = uri.split_once('@').map(|(_, rest)| rest).unwrap_or(uri);
    let host_port = after_at
        .split_once('/')
        .map(|(hp, _)| hp)
        .unwrap_or(after_at);
    match host_port.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.parse().expect("port should parse")),
        None => (host_port.to_string(), 1433),
    }
}

/// Connect using form mode (`use_uri: false`) with the given SSL mode override.
///
/// Used by tests that need to exercise specific `DbConfig::SqlServer` field
/// combinations the URI-based `connect_mssql` cannot express directly. The
/// password is delivered through `connect_with_secrets` since form mode does
/// not parse credentials out of the URI.
fn connect_form_mode(
    uri: &str,
    ssl_mode: &str,
    trust_server_certificate: bool,
) -> Result<Box<dyn Connection>, DbError> {
    use dbflux_core::secrecy::SecretString;

    let (host, port) = parse_host_port_from_uri(uri);
    let driver = MssqlDriver::new();
    let profile = ConnectionProfile::new(
        "live-mssql-form",
        DbConfig::SqlServer {
            use_uri: false,
            uri: None,
            host,
            port,
            user: "sa".to_string(),
            database: None,
            instance: None,
            ssl_mode: Some(ssl_mode.to_string()),
            trust_server_certificate,
            ssl_root_cert_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );
    let password: SecretString = containers::MSSQL_TEST_PASSWORD.to_string().into();

    containers::retry_db_operation(Duration::from_secs(60), || -> Result<_, DbError> {
        let connection = driver.connect_with_secrets(&profile, Some(&password), None)?;
        connection.ping()?;
        Ok(connection)
    })
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
        "transaction_test",
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
// Failed batches and the session's transaction
// ---------------------------------------------------------------------------

fn transaction_note(error: &DbError) -> Option<&str> {
    error
        .formatted()
        .and_then(|formatted| formatted.hint.as_deref())
}

fn single_value(connection: &dyn Connection, sql: &str) -> Result<Value, DbError> {
    let result = connection.execute(&QueryRequest::new(sql))?;
    Ok(result.rows[0][0].clone())
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_failed_batch_rolls_back_the_transaction_it_opened() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        connection.execute(&QueryRequest::new(
            "CREATE TABLE transaction_test (id INT PRIMARY KEY)",
        ))?;

        let error = connection
            .execute(&QueryRequest::new(
                "BEGIN TRAN; INSERT INTO transaction_test (id) VALUES (1); \
                 SELECT * FROM missing_table; COMMIT;",
            ))
            .expect_err("batch referencing a missing table should fail");

        let hint = transaction_note(&error).unwrap_or_default();
        assert!(
            hint.contains(TransactionStateNote::RolledBack.message()),
            "error should report the rollback, got hint: {hint:?}"
        );

        assert_eq!(
            single_value(&*connection, "SELECT @@TRANCOUNT AS open_transactions")?,
            Value::Int(0)
        );
        assert_eq!(
            single_value(
                &*connection,
                "SELECT COUNT(*) AS row_count FROM transaction_test"
            )?,
            Value::Int(0),
            "the row inserted inside the rolled-back transaction must not be visible"
        );

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_failed_batch_leaves_an_earlier_transaction_open() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;

        connection.execute(&QueryRequest::new("BEGIN TRAN"))?;

        let error = connection
            .execute(&QueryRequest::new("SELECT * FROM missing_table"))
            .expect_err("query against a missing table should fail");

        let hint = transaction_note(&error).unwrap_or_default();
        assert!(
            hint.contains(TransactionStateNote::StillOpen.message()),
            "error should report the open transaction, got hint: {hint:?}"
        );

        assert_eq!(
            single_value(&*connection, "SELECT @@TRANCOUNT AS open_transactions")?,
            Value::Int(1)
        );

        connection.execute(&QueryRequest::new("ROLLBACK TRANSACTION"))?;
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

// ---------------------------------------------------------------------------
// Form mode (use_uri = false) — exercises connect_direct + form-fed password
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_form_mode_connect_query_and_select_db() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let connection = connect_form_mode(&uri, "on", true)?;

        connection.ping()?;

        // Run a trivial query so we know the session is actually usable, not
        // just that the TCP handshake succeeded.
        let result = connection.execute(&QueryRequest::new("SELECT 1 AS form_mode_ok"))?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Int(1));

        // The form-mode profile leaves `database` unset, so the session should
        // land in `master`. `set_active_database` must work in this mode too.
        connection.execute(&QueryRequest::new(
            "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'dbflux_form_test') \
             CREATE DATABASE [dbflux_form_test]",
        ))?;
        connection.set_active_database(Some("dbflux_form_test"))?;
        assert_eq!(
            connection.active_database().as_deref(),
            Some("dbflux_form_test")
        );

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// SSL mode coverage — `off` and `on` (both with trust_server_certificate=true,
// since the test container uses a self-signed cert)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ssl_mode_off_connects() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        // `ssl_mode = "off"` maps to `EncryptionLevel::Off`. The SQL Server
        // test image accepts unencrypted connections by default.
        let connection = connect_form_mode(&uri, "off", true)?;
        connection.ping()?;
        let result = connection.execute(&QueryRequest::new("SELECT 1"))?;
        assert_eq!(result.rows.len(), 1);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ssl_mode_on_trusts_self_signed() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        // `ssl_mode = "on"` with `trust_server_certificate = true` is the
        // "encrypt but accept self-signed" path tiberius takes when
        // `EncryptionLevel::On` and `trust_cert()` are both set.
        let connection = connect_form_mode(&uri, "on", true)?;
        connection.ping()?;
        let result = connection.execute(&QueryRequest::new("SELECT 1"))?;
        assert_eq!(result.rows.len(), 1);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Instance routing
// ---------------------------------------------------------------------------
//
// The `mcr.microsoft.com/mssql/server` test image only ships the default
// `MSSQLSERVER` instance and does not expose the SQL Browser UDP service, so
// `instance_name` cannot be exercised end-to-end here. The unit tests in
// `driver.rs::tests` cover instance-name plumbing through `parse_mssql_url`
// and `build_uri` (round-trip + URI-vs-form precedence). A real live test
// would need a custom image / sidecar SQL Browser.

// ---------------------------------------------------------------------------
// Execution safety (#671/#672): total row budget across result sets
// ---------------------------------------------------------------------------
// `QueryRequest::limit` is a TOTAL retained-row budget shared across every
// server result set of the batch in actual stream order — not a per-set cap.
// Rows past the budget are observed and discarded while each set drains to
// completion, so mutations finish fully, later statements still run, and late
// errors surface. The primary-selection convention is unchanged: the LAST set
// still wins.

fn drop_safety_tables(conn: &dyn Connection, names: &[&str]) -> Result<(), DbError> {
    for name in names {
        conn.execute(&QueryRequest::new(format!(
            "DROP TABLE IF EXISTS dbo.[{}]",
            name
        )))?;
    }
    Ok(())
}

fn create_safety_table(conn: &dyn Connection, name: &str) -> Result<(), DbError> {
    conn.execute(&QueryRequest::new(format!(
        "CREATE TABLE dbo.[{}] (id INT PRIMARY KEY, label NVARCHAR(20) NOT NULL)",
        name
    )))?;
    Ok(())
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_query_safety_limit_is_total_budget_across_result_sets() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;

        // Two result sets of 5 rows each; a budget of 7 must retain all of
        // the first set plus the first 2 rows of the second set. The second
        // query carries an explicit ORDER BY because the assertions below
        // check the exact retained VALUES in order; unordered tiny
        // VALUES-derived sets do not guarantee row order.
        let sql = "SELECT id FROM (VALUES (1),(2),(3),(4),(5)) AS a(id); \
                   SELECT id FROM (VALUES (6),(7),(8),(9),(10)) AS b(id) ORDER BY id";
        let mut request = QueryRequest::new(sql.to_string());
        request.limit = Some(7);

        let result = connection.execute(&request)?;

        // Total budget across sets in stream order.
        let total_retained: usize = result.iter_result_sets().map(|set| set.rows.len()).sum();
        assert_eq!(
            total_retained, 7,
            "the limit is a total budget across all result sets, not per set"
        );

        // Primary convention: the LAST set still wins, holding only the rows
        // left in the budget (5 were consumed by the first set).
        assert_eq!(
            result.rows.len(),
            2,
            "the last set retains the remaining budget"
        );
        assert_eq!(result.rows[0][0], Value::Int(6));
        assert_eq!(result.rows[1][0], Value::Int(7));
        assert!(
            result.rows_truncated(),
            "the last set observed rows past the cap"
        );

        // The first set (attached as an additional result in batch order) fit
        // entirely inside the budget, so it is complete and untruncated.
        assert_eq!(result.additional_results.len(), 1);
        let first_set = &result.additional_results[0];
        assert_eq!(first_set.rows.len(), 5);
        assert!(!first_set.rows_truncated());

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_query_safety_limit_below_exact_and_over_cap() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;

        let sql = "SELECT id FROM (VALUES (1),(2),(3),(4),(5)) AS a(id)";
        let cases = [
            (
                3u32,
                3usize,
                true,
                "below cap: rows are retained and the omission is reported",
            ),
            (
                5u32,
                5usize,
                false,
                "exact cap: every row retained, nothing omitted",
            ),
            (
                8u32,
                5usize,
                false,
                "over cap: all rows retained and no truncation reported",
            ),
        ];
        for (limit, expected_rows, expected_truncated, message) in cases {
            let mut request = QueryRequest::new(sql.to_string());
            request.limit = Some(limit);
            let result = connection.execute(&request)?;
            assert_eq!(result.rows.len(), expected_rows, "{}", message);
            assert_eq!(result.rows_truncated(), expected_truncated, "{}", message);
        }

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_query_safety_zero_limit_retains_nothing_but_completes_all_effects() -> Result<(), DbError>
{
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        drop_safety_tables(&*connection, &["mssql_safety_zero"])?;
        create_safety_table(&*connection, "mssql_safety_zero")?;

        // limit 0 must retain 0 rows yet still drain every token: the INSERT
        // and the UPDATE between the two SELECTs must run to completion, and
        // the trailing SELECT must still execute (later statements are never
        // skipped when the budget is already 0). The two SELECTs carry
        // distinct column aliases so the retained empty sets' column
        // identities can be asserted, not just counts and flags.
        let sql = "SELECT id AS first_set_id FROM (VALUES (1),(2),(3)) AS s(id); \
                   INSERT INTO dbo.[mssql_safety_zero] (id, label) \
                     SELECT id, N'x' FROM (VALUES (11),(12),(13),(14)) AS v(id); \
                   UPDATE dbo.[mssql_safety_zero] SET label = N'y' WHERE id > 12; \
                   SELECT id AS last_set_id FROM (VALUES (21),(22)) AS t(id)";
        let mut request = QueryRequest::new(sql.to_string());
        request.limit = Some(0);

        let result = connection.execute(&request)?;

        // Nothing is retained, and every set that streamed rows reports the
        // omission. The INSERT/UPDATE produce no result sets of their own.
        // Column identities prove WHICH sets these are: the primary is the
        // LAST set by its declared column, and the earlier set keeps its own
        // identity as an additional result, both with zero retained rows.
        assert_eq!(result.rows.len(), 0, "a zero budget retains no rows");
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name, "last_set_id");
        assert!(result.rows_truncated());
        assert_eq!(result.additional_results.len(), 1);
        let first_set = &result.additional_results[0];
        assert_eq!(first_set.rows.len(), 0);
        assert_eq!(first_set.columns.len(), 1);
        assert_eq!(first_set.columns[0].name, "first_set_id");
        assert!(first_set.rows_truncated());

        // All mutation effects completed despite the exhausted budget.
        let aftermath = connection.execute(&QueryRequest::new(
            "SELECT id, label FROM dbo.[mssql_safety_zero] ORDER BY id",
        ))?;
        assert_eq!(
            aftermath.rows.len(),
            4,
            "the INSERT must have completed fully"
        );
        assert_eq!(aftermath.rows[0][1], Value::Text("x".to_string()));
        assert_eq!(aftermath.rows[2][1], Value::Text("y".to_string()));
        assert_eq!(aftermath.rows[3][1], Value::Text("y".to_string()));

        // The connection is still usable.
        let ping = connection.execute(&QueryRequest::new("SELECT 1 AS one"))?;
        assert_eq!(ping.rows[0][0], Value::Int(1));

        drop_safety_tables(&*connection, &["mssql_safety_zero"])?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_query_safety_none_limit_keeps_unbounded_behavior() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;

        let sql = "SELECT id FROM (VALUES (1),(2),(3)) AS s(id); \
                   SELECT id FROM (VALUES (21),(22)) AS t(id)";
        let result = connection.execute(&QueryRequest::new(sql.to_string()))?;

        assert_eq!(
            result.rows.len(),
            2,
            "uncapped: the last set keeps every row"
        );
        assert!(!result.rows_truncated());
        assert_eq!(result.additional_results.len(), 1);
        assert_eq!(result.additional_results[0].rows.len(), 3);
        assert!(!result.additional_results[0].rows_truncated());

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_query_safety_late_error_after_budget_exhaustion_propagates() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;

        // Ten rows stream before the deliberate server error; with a budget
        // of 1, nine rows are observed past the cap BEFORE the error arrives.
        // The error must still propagate — draining past the cap never
        // swallows a late server error.
        let sql = "SELECT id FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) AS s(id); \
                   SELECT CAST(N'dbflux-late-deliberate' AS INT) AS boom";
        let mut request = QueryRequest::new(sql.to_string());
        request.limit = Some(1);

        let outcome = connection.execute(&request);
        match outcome {
            Err(DbError::Cancelled) => {
                panic!("a late server error after budget exhaustion must not surface as Cancelled")
            }
            Err(err) => {
                let message = format!("{}", err);
                assert!(
                    message.contains("dbflux-late-deliberate"),
                    "the propagated error must carry the server's deliberate late failure, got: {}",
                    message
                );
            }
            Ok(result) => panic!(
                "the late error must propagate after the budget is exhausted instead of returning {} retained rows",
                result.rows.len()
            ),
        }

        // The connection is still usable after the aborted batch.
        let reuse = connection.execute(&QueryRequest::new("SELECT 1 AS one"))?;
        assert_eq!(reuse.rows[0][0], Value::Int(1));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_query_safety_statement_timeout_rejected_before_execution() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        drop_safety_tables(&*connection, &["mssql_safety_timeout"])?;
        create_safety_table(&*connection, "mssql_safety_timeout")?;

        // A requested statement timeout is unsupported by this driver's safe
        // arbitrary-SQL lifecycle and must be refused before ANY execution:
        // the INSERT must carry zero effects.
        let mut request = QueryRequest::new(
            "INSERT INTO dbo.[mssql_safety_timeout] (id, label) VALUES (1, N'never')".to_string(),
        );
        request.statement_timeout = Some(Duration::from_secs(5));

        match connection.execute(&request) {
            Err(DbError::NotSupported(reason)) => {
                let lowered = reason.to_lowercase();
                assert!(
                    lowered.contains("timeout"),
                    "expected the timeout preflight rejection, got: {}",
                    reason
                );
            }
            other => panic!(
                "a requested statement timeout must be rejected before execution, got {:?}",
                other.map(|r| r.rows.len())
            ),
        }

        // Zero effects: the INSERT never reached the server.
        let count = connection.execute(&QueryRequest::new(
            "SELECT COUNT(*) FROM dbo.[mssql_safety_timeout]".to_string(),
        ))?;
        assert_eq!(
            count.rows[0][0],
            Value::Int(0),
            "the refused INSERT must have zero effects"
        );

        // Uncapped execution is untouched.
        let ping = connection.execute(&QueryRequest::new("SELECT 1 AS one"))?;
        assert_eq!(ping.rows[0][0], Value::Int(1));

        drop_safety_tables(&*connection, &["mssql_safety_timeout"])?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_query_safety_bounded_contexts_rejected_and_uncapped_dispatch_works() -> Result<(), DbError>
{
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;

        // A bounded request cannot reach the instance-catalog dispatch paths
        // (they run whole buffered catalog queries with no cap seam), so it
        // must be rejected before the lock or any dispatch.
        for limit in [0u32, 1] {
            let mut request = QueryRequest::new("SELECT 1".to_string());
            request.limit = Some(limit);
            request.execution_context = Some(ExecutionContext {
                source: Some(ExecutionSourceContext::InstanceMetricQuery {
                    metric_id: "mssql.batch_requests_per_sec".to_string(),
                    start_ms: 0,
                    end_ms: 1,
                }),
                ..Default::default()
            });
            match connection.execute(&request) {
                Err(DbError::NotSupported(reason)) => {
                    let lowered = reason.to_lowercase();
                    assert!(
                        lowered.contains("instance metric") && lowered.contains("row limit"),
                        "expected the bounded metric-context rejection, got: {}",
                        reason
                    );
                }
                other => panic!(
                    "bounded metric context must be rejected before dispatch, got {:?}",
                    other.map(|r| r.rows.len())
                ),
            }

            let mut request = QueryRequest::new("SELECT 1".to_string());
            request.limit = Some(limit);
            request.execution_context = Some(ExecutionContext {
                source: Some(ExecutionSourceContext::InstanceInspectorQuery {
                    metric_id: "mssql.active_sessions".to_string(),
                }),
                ..Default::default()
            });
            match connection.execute(&request) {
                Err(DbError::NotSupported(reason)) => {
                    let lowered = reason.to_lowercase();
                    assert!(
                        lowered.contains("instance inspector") && lowered.contains("row limit"),
                        "expected the bounded inspector-context rejection, got: {}",
                        reason
                    );
                }
                other => panic!(
                    "bounded inspector context must be rejected before dispatch, got {:?}",
                    other.map(|r| r.rows.len())
                ),
            }
        }

        // Healthy controls: the same valid contexts WITHOUT a limit keep
        // working through the existing dispatch paths.
        let mut metric = QueryRequest::new("SELECT 1".to_string());
        metric.execution_context = Some(ExecutionContext {
            source: Some(ExecutionSourceContext::InstanceMetricQuery {
                metric_id: "mssql.batch_requests_per_sec".to_string(),
                start_ms: 0,
                end_ms: 1,
            }),
            ..Default::default()
        });
        let metric_result = connection.execute(&metric)?;
        assert_eq!(metric_result.rows.len(), 1);

        let mut inspector = QueryRequest::new("SELECT 1".to_string());
        inspector.execution_context = Some(ExecutionContext {
            source: Some(ExecutionSourceContext::InstanceInspectorQuery {
                metric_id: "mssql.active_sessions".to_string(),
            }),
            ..Default::default()
        });
        let inspector_result = connection.execute(&inspector)?;
        assert!(!inspector_result.rows.is_empty());

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_query_safety_refusal_preserves_existing_cancel_signal() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        let cancel_handle = connection.cancel_handle();

        // Cancelling while idle KILLs the session and leaves both the
        // cancellation signal and the reconnect-on-next-use state armed.
        cancel_handle.cancel()?;
        assert!(cancel_handle.is_cancelled());

        // A refused request must not run the post-cancel recovery, which
        // would clear the pending cancellation signal as a side effect.
        let mut timeout_request = QueryRequest::new("SELECT 1 AS one");
        timeout_request.statement_timeout = Some(Duration::from_secs(1));
        match connection.execute(&timeout_request) {
            Err(DbError::NotSupported(_)) => {}
            other => panic!(
                "a requested statement timeout must be refused, got {:?}",
                other.map(|result| result.rows.len())
            ),
        }
        assert!(
            cancel_handle.is_cancelled(),
            "the timeout refusal erased an existing cancellation"
        );

        let mut metric_request = QueryRequest::new("SELECT 1").with_limit(0);
        metric_request.execution_context = Some(ExecutionContext {
            source: Some(ExecutionSourceContext::InstanceMetricQuery {
                metric_id: "mssql.batch_requests_per_sec".to_string(),
                start_ms: 0,
                end_ms: 1,
            }),
            ..Default::default()
        });
        match connection.execute(&metric_request) {
            Err(DbError::NotSupported(_)) => {}
            other => panic!(
                "a bounded instance metric request must be refused, got {:?}",
                other.map(|result| result.rows.len())
            ),
        }
        assert!(
            cancel_handle.is_cancelled(),
            "the bounded metric refusal erased an existing cancellation"
        );

        // The next supported request still recovers the killed session.
        let recovered = connection.execute(&QueryRequest::new("SELECT 1 AS one"))?;
        assert_eq!(recovered.rows[0][0], Value::Int(1));

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Write-privilege probe
// ---------------------------------------------------------------------------
//
// The test container only provisions the `sa` login (sysadmin), so only the
// writable path is exercised here. There is no low-privilege-user fixture
// pattern in this suite to spin up a SELECT-only login and assert
// `WritePrivilege::ReadOnly` against a real server; adding one would need a
// second connection profile plus a dedicated login/db-user setup this file
// does not otherwise have.

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_probe_write_privilege_sa_is_writable() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;

        // The probe resolves an empty database to Unknown by design (nothing
        // to write to is not the same as forbidden), so a visible base table
        // must exist before the writable verdict can be asserted.
        connection.execute(&QueryRequest::new(
            "CREATE TABLE probe_privilege_test (id INT PRIMARY KEY)",
        ))?;

        // `sa` is sysadmin, so `DATABASEPROPERTYEX(..., 'Updateability')`
        // reports `READ_WRITE` and `HAS_PERMS_BY_NAME` grants INSERT/UPDATE/
        // DELETE on every visible base table — this exercises both real
        // wire queries end to end.
        assert_eq!(
            connection.probe_write_privilege(),
            dbflux_core::WritePrivilege::Writable
        );

        Ok(())
    })
}
