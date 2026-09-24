#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, DbKind, DescribeRequest, ExplainRequest,
    OrderByColumn, Pagination, QueryRequest, RecordIdentity, RowDelete, RowInsert, RowPatch,
    SchemaLoadingStrategy, TableBrowseRequest, TableCountRequest, TableRef, TransactionStateNote,
    Value,
};
use dbflux_driver_mysql::MysqlDriver;
use dbflux_test_support::containers;
use mysql::prelude::Queryable;
use std::time::Duration;

fn connect_mysql(uri: String) -> Result<(Box<dyn dbflux_core::Connection>, MysqlDriver), DbError> {
    let driver = MysqlDriver::new(DbKind::MySQL);
    let profile = ConnectionProfile::new(
        "live-mysql",
        DbConfig::MySQL {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 3306,
            user: String::new(),
            database: None,
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    let connection =
        containers::retry_db_operation(Duration::from_secs(30), || -> Result<_, DbError> {
            let connection = driver.connect(&profile)?;
            connection.ping()?;
            Ok(connection)
        })?;

    Ok((connection, driver))
}

// ---------------------------------------------------------------------------
// Basic connectivity
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_live_connect_ping_query_and_schema() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        let result = connection.execute(&QueryRequest::new("SELECT 1 AS one"))?;
        assert_eq!(result.rows.len(), 1);

        assert_eq!(
            connection.schema_loading_strategy(),
            SchemaLoadingStrategy::LazyPerDatabase
        );

        let databases = connection.list_databases()?;
        assert!(!databases.is_empty());

        let schema = connection.schema()?;
        assert!(schema.is_relational());
        let _ = schema.databases();

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Schema introspection
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_schema_introspection() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        connection.set_active_database(Some("testdb"))?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE test_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE,
                age INT DEFAULT 0
            )",
        ))?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE test_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                amount DECIMAL(10, 2) NOT NULL,
                FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE
            )",
        ))?;

        connection.execute(&QueryRequest::new(
            "CREATE INDEX idx_orders_user_id ON test_orders(user_id)",
        ))?;

        connection.execute(&QueryRequest::new(
            "CREATE VIEW test_user_view AS SELECT id, name FROM test_users",
        ))?;

        let db_schema = connection.schema_for_database("testdb")?;
        assert!(!db_schema.tables.is_empty());
        assert!(db_schema.tables.iter().any(|t| t.name == "test_users"));
        assert!(!db_schema.views.is_empty());
        assert!(db_schema.views.iter().any(|v| v.name == "test_user_view"));

        let table = connection.table_details("testdb", None, "test_users")?;
        assert_eq!(table.name, "test_users");

        let columns = table.columns.as_ref().expect("columns should be loaded");
        assert!(columns.len() >= 4);

        let id_col = columns.iter().find(|c| c.name == "id").expect("id column");
        assert!(id_col.is_primary_key);

        let name_col = columns
            .iter()
            .find(|c| c.name == "name")
            .expect("name column");
        assert!(!name_col.nullable);

        let indexes = table.indexes.as_ref().expect("indexes should be loaded");
        let idx_data = match indexes {
            dbflux_core::IndexData::Relational(v) => v,
            _ => panic!("expected relational index data"),
        };
        assert!(idx_data.iter().any(|i| i.is_primary));

        let view = connection.view_details("testdb", None, "test_user_view")?;
        assert_eq!(view.name, "test_user_view");

        let orders = connection.table_details("testdb", None, "test_orders")?;
        let fks = orders
            .foreign_keys
            .as_ref()
            .expect("foreign keys should be loaded");
        assert!(!fks.is_empty());
        assert_eq!(fks[0].referenced_table, "test_users");

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// CRUD operations
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_crud_operations() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        connection.set_active_database(Some("testdb"))?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE crud_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INT DEFAULT 0
            )",
        ))?;

        let insert_result = connection.insert_row(&RowInsert::new(
            "crud_test".to_string(),
            None,
            vec!["name".to_string(), "value".to_string()],
            vec![Value::Text("alice".to_string()), Value::Int(42)],
        ))?;
        assert_eq!(insert_result.affected_rows, 1);

        let rows = connection
            .execute(&QueryRequest::new(
                "SELECT * FROM crud_test WHERE name = 'alice'",
            ))?
            .rows;
        assert_eq!(rows.len(), 1);

        let update_result = connection.update_row(&RowPatch::new(
            RecordIdentity::composite(
                vec!["name".to_string()],
                vec![Value::Text("alice".to_string())],
            ),
            "crud_test".to_string(),
            None,
            vec![("value".to_string(), Value::Int(99))],
        ))?;
        assert_eq!(update_result.affected_rows, 1);

        let rows = connection
            .execute(&QueryRequest::new(
                "SELECT value FROM crud_test WHERE name = 'alice'",
            ))?
            .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int(99));

        let delete_result = connection.delete_row(&RowDelete::new(
            RecordIdentity::composite(
                vec!["name".to_string()],
                vec![Value::Text("alice".to_string())],
            ),
            "crud_test".to_string(),
            None,
        ))?;
        assert_eq!(delete_result.affected_rows, 1);

        let rows = connection
            .execute(&QueryRequest::new("SELECT * FROM crud_test"))?
            .rows;
        assert!(rows.is_empty());

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Browse and count
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_browse_and_count() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        connection.set_active_database(Some("testdb"))?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE browse_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            )",
        ))?;

        for i in 1..=25 {
            connection.execute(&QueryRequest::new(format!(
                "INSERT INTO browse_test (name) VALUES ('item_{}')",
                i
            )))?;
        }

        let table_ref = TableRef::new("browse_test");

        let count = connection.count_table(&TableCountRequest::new(table_ref.clone()))?;
        assert_eq!(count, 25);

        let filtered_count = connection.count_table(
            &TableCountRequest::new(table_ref.clone()).with_filter("name LIKE 'item_1%'"),
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
                .with_filter("name = 'item_5'")
                .with_pagination(Pagination::Offset {
                    limit: 100,
                    offset: 0,
                }),
        )?;
        assert_eq!(filtered.rows.len(), 1);

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Explain and describe
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_explain() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        connection.set_active_database(Some("testdb"))?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE explain_test (id INT PRIMARY KEY, name TEXT)",
        ))?;

        let table_ref = TableRef::new("explain_test");
        let result = connection.explain(&ExplainRequest::new(table_ref))?;
        assert!(!result.rows.is_empty() || result.text_body.is_some());

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_describe_table() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        connection.set_active_database(Some("testdb"))?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE describe_test (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                active BOOLEAN DEFAULT TRUE
            )",
        ))?;

        let table_ref = TableRef::new("describe_test");
        let result = connection.describe_table(&DescribeRequest::new(table_ref))?;
        assert!(result.rows.len() >= 3);

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Active database switching
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_set_active_database() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        connection.set_active_database(Some("testdb"))?;
        let active = connection.active_database();
        assert_eq!(active.as_deref(), Some("testdb"));

        connection.execute(&QueryRequest::new("CREATE DATABASE IF NOT EXISTS testdb2"))?;
        connection.set_active_database(Some("testdb2"))?;
        let active = connection.active_database();
        assert_eq!(active.as_deref(), Some("testdb2"));

        connection.set_active_database(Some("testdb"))?;
        let active = connection.active_database();
        assert_eq!(active.as_deref(), Some("testdb"));

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Query cancellation
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_cancel_query() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        let cancel_handle = connection.cancel_handle();
        let cancel_result = cancel_handle.cancel();
        assert!(cancel_result.is_ok());

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Code generators
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_code_generators() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        connection.set_active_database(Some("testdb"))?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE codegen_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )",
        ))?;

        let generators = connection.code_generators();
        assert!(!generators.is_empty());

        let table = connection.table_details("testdb", None, "codegen_test")?;

        for generator in generators {
            let code = connection.generate_code(&generator.id, &table)?;
            assert!(
                !code.is_empty(),
                "generator '{}' returned empty code",
                generator.id
            );
        }

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Referential integrity toggle (data-transfer engine)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_set_referential_integrity_disables_and_restores_fk_checks() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE parent_ri (id INT PRIMARY KEY)",
        ))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE child_ri (id INT PRIMARY KEY, parent_id INT, \
             FOREIGN KEY (parent_id) REFERENCES parent_ri(id))",
        ))?;

        let violates =
            connection.execute(&QueryRequest::new("INSERT INTO child_ri VALUES (1, 999)"));
        assert!(violates.is_err(), "FK violation must fail with RI enabled");

        connection.set_referential_integrity(false)?;
        connection.execute(&QueryRequest::new("INSERT INTO child_ri VALUES (1, 999)"))?;

        connection.set_referential_integrity(true)?;
        let still_violates =
            connection.execute(&QueryRequest::new("INSERT INTO child_ri VALUES (2, 998)"));
        assert!(
            still_violates.is_err(),
            "FK violation must fail again after RI is restored"
        );

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Transaction state after a failed execution
//
// MySQL rejects `START TRANSACTION` and `BEGIN` in the prepared-statement
// protocol the editor path uses (error 1295), so these tests open their
// transactions by turning `autocommit` off, which MySQL and MariaDB both accept
// there.
// ---------------------------------------------------------------------------

fn assert_transaction_note(error: &DbError, note: TransactionStateNote) {
    let hint = error
        .formatted()
        .and_then(|formatted| formatted.hint.as_deref());

    assert!(
        hint.is_some_and(|hint| hint.contains(note.message())),
        "expected {note:?} in the error hint, got {error:?}"
    );
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_failed_transaction_script_rolls_back_the_transaction_it_opened() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        connection.set_active_database(Some("testdb"))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE tx_failed_script (id INT PRIMARY KEY)",
        ))?;

        let Err(error) = connection.execute(&QueryRequest::new(
            "SET autocommit = 0; \
             INSERT INTO tx_failed_script VALUES (1); \
             INSERT INTO tx_failed_script VALUES (1); \
             COMMIT;",
        )) else {
            panic!("the duplicate key must fail the script");
        };
        assert_transaction_note(&error, TransactionStateNote::RolledBack);

        let rows = connection
            .execute(&QueryRequest::new("SELECT id FROM tx_failed_script"))?
            .rows;
        assert!(rows.is_empty(), "the partial insert must be rolled back");

        connection.execute(&QueryRequest::new(
            "SET autocommit = 1; INSERT INTO tx_failed_script VALUES (2);",
        ))?;

        let rows = connection
            .execute(&QueryRequest::new("SELECT id FROM tx_failed_script"))?
            .rows;
        assert_eq!(rows, vec![vec![Value::Int(2)]]);

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_failed_statement_leaves_an_earlier_transaction_open() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        connection.set_active_database(Some("testdb"))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE tx_earlier (id INT PRIMARY KEY)",
        ))?;

        connection.execute(&QueryRequest::new("SET autocommit = 0"))?;
        connection.execute(&QueryRequest::new("INSERT INTO tx_earlier VALUES (1)"))?;

        let Err(error) =
            connection.execute(&QueryRequest::new("INSERT INTO tx_earlier VALUES (1)"))
        else {
            panic!("the duplicate key must fail the statement");
        };
        assert_transaction_note(&error, TransactionStateNote::StillOpen);

        let rows = connection
            .execute(&QueryRequest::new("SELECT id FROM tx_earlier"))?
            .rows;
        assert_eq!(rows.len(), 1, "the earlier insert must still be visible");

        connection.execute(&QueryRequest::new("ROLLBACK"))?;

        let rows = connection
            .execute(&QueryRequest::new("SELECT id FROM tx_earlier"))?
            .rows;
        assert!(
            rows.is_empty(),
            "ROLLBACK must discard the earlier insert, proving the transaction stayed open"
        );

        Ok(())
    })
}

fn assert_explicit_transaction_statement_opens_a_transaction(
    connection: &dyn dbflux_core::Connection,
    statement: &str,
    table: &str,
) -> Result<(), DbError> {
    connection.execute(&QueryRequest::new(format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY)"
    )))?;

    connection.execute(&QueryRequest::new(statement))?;
    connection.execute(&QueryRequest::new(format!(
        "INSERT INTO {table} VALUES (1)"
    )))?;
    connection.execute(&QueryRequest::new("ROLLBACK"))?;

    let rows = connection
        .execute(&QueryRequest::new(format!("SELECT id FROM {table}")))?
        .rows;
    assert!(
        rows.is_empty(),
        "ROLLBACK must discard the insert, proving `{statement}` opened a transaction"
    );

    Ok(())
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_start_transaction_statement_opens_a_transaction() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;
        connection.set_active_database(Some("testdb"))?;

        assert_explicit_transaction_statement_opens_a_transaction(
            connection.as_ref(),
            "START TRANSACTION",
            "tx_start_transaction",
        )
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_begin_statement_opens_a_transaction() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;
        connection.set_active_database(Some("testdb"))?;

        assert_explicit_transaction_statement_opens_a_transaction(
            connection.as_ref(),
            "BEGIN",
            "tx_begin",
        )
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_script_starting_with_start_transaction_rolls_back_and_commits() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;
        connection.set_active_database(Some("testdb"))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE tx_script (id INT PRIMARY KEY)",
        ))?;

        connection.execute(&QueryRequest::new(
            "START TRANSACTION; INSERT INTO tx_script VALUES (1); ROLLBACK;",
        ))?;
        let rows = connection
            .execute(&QueryRequest::new("SELECT id FROM tx_script"))?
            .rows;
        assert!(rows.is_empty(), "the rolled-back script must leave no row");

        connection.execute(&QueryRequest::new(
            "START TRANSACTION; INSERT INTO tx_script VALUES (2); COMMIT;",
        ))?;
        let rows = connection
            .execute(&QueryRequest::new("SELECT id FROM tx_script"))?
            .rows;
        assert_eq!(rows, vec![vec![Value::Int(2)]]);

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_bounded_script_starting_with_start_transaction_succeeds() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;
        connection.set_active_database(Some("testdb"))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE tx_bounded_script (id INT PRIMARY KEY)",
        ))?;

        let result = connection.execute(
            &QueryRequest::new(
                "START TRANSACTION; INSERT INTO tx_bounded_script VALUES (1), (2); \
                 SELECT id FROM tx_bounded_script ORDER BY id; ROLLBACK;",
            )
            .with_limit(1),
        )?;
        assert_eq!(result.additional_results.len(), 3);
        assert_eq!(result.additional_results[0].affected_rows, Some(2));
        assert_eq!(result.additional_results[1].rows, vec![vec![Value::Int(1)]]);
        assert!(result.additional_results[1].rows_truncated());

        let rows = connection
            .execute(&QueryRequest::new("SELECT id FROM tx_bounded_script"))?
            .rows;
        assert!(
            rows.is_empty(),
            "the bounded script's ROLLBACK must discard its rows"
        );

        connection.execute(&QueryRequest::new("BEGIN").with_limit(1))?;
        connection.execute(&QueryRequest::new(
            "INSERT INTO tx_bounded_script VALUES (3)",
        ))?;
        connection.execute(&QueryRequest::new("ROLLBACK"))?;
        let rows = connection
            .execute(&QueryRequest::new("SELECT id FROM tx_bounded_script"))?
            .rows;
        assert!(rows.is_empty(), "a bounded BEGIN must open a transaction");

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_failed_script_rolls_back_the_transaction_start_transaction_opened() -> Result<(), DbError>
{
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;
        connection.set_active_database(Some("testdb"))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE tx_failed_start (id INT PRIMARY KEY)",
        ))?;

        let Err(error) = connection.execute(&QueryRequest::new(
            "START TRANSACTION; \
             INSERT INTO tx_failed_start VALUES (1); \
             INSERT INTO tx_failed_start VALUES (1); \
             COMMIT;",
        )) else {
            panic!("the duplicate key must fail the script");
        };
        assert_transaction_note(&error, TransactionStateNote::RolledBack);

        let rows = connection
            .execute(&QueryRequest::new("SELECT id FROM tx_failed_start"))?
            .rows;
        assert!(rows.is_empty(), "the partial insert must be rolled back");

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_invalid_statement_still_returns_the_query_error() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        for request in [
            QueryRequest::new("SELEC 1"),
            QueryRequest::new("SELEC 1").with_limit(1),
        ] {
            let Err(error) = connection.execute(&request) else {
                panic!("an invalid statement must fail");
            };
            let DbError::QueryFailed(formatted) = &error else {
                panic!("expected a query error, got {error:?}");
            };
            assert_eq!(
                formatted.code.as_deref(),
                Some("1064"),
                "expected the server's syntax error, got {error:?}"
            );
        }

        Ok(())
    })
}

fn assert_no_truncation(result: &dbflux_core::QueryResult, expected_rows: usize) {
    assert_eq!(result.rows.len(), expected_rows);
    assert!(!result.rows_truncated());
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_query_safety_limit_below_exact_and_over_retains_and_flags() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE mysql_safety_limit (id INT PRIMARY KEY)",
        ))?;
        connection.execute(&QueryRequest::new(
            "INSERT INTO mysql_safety_limit (id) VALUES (1), (2), (3), (4), (5)",
        ))?;
        let query = "SELECT id FROM mysql_safety_limit ORDER BY id";
        assert_no_truncation(
            &connection.execute(&QueryRequest::new(query).with_limit(8))?,
            5,
        );
        assert_no_truncation(
            &connection.execute(&QueryRequest::new(query).with_limit(5))?,
            5,
        );
        let capped = connection.execute(&QueryRequest::new(query).with_limit(3))?;
        assert_eq!(capped.rows.len(), 3);
        assert_eq!(capped.rows[0][0], Value::Int(1));
        assert_eq!(capped.rows[2][0], Value::Int(3));
        assert!(capped.rows_truncated());
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_query_safety_zero_limit_retains_nothing_only_when_rows_exist() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE mysql_safety_zero (id INT PRIMARY KEY)",
        ))?;
        connection.execute(&QueryRequest::new(
            "INSERT INTO mysql_safety_zero (id) VALUES (1), (2), (3)",
        ))?;
        let nonempty = connection
            .execute(&QueryRequest::new("SELECT id FROM mysql_safety_zero").with_limit(0))?;
        assert!(nonempty.rows.is_empty());
        assert!(nonempty.rows_truncated());
        let empty = connection.execute(
            &QueryRequest::new("SELECT id FROM mysql_safety_zero WHERE id > 100").with_limit(0),
        )?;
        assert!(empty.rows.is_empty());
        assert!(!empty.rows_truncated());
        let insert = connection.execute(
            &QueryRequest::new("INSERT INTO mysql_safety_zero (id) VALUES (4)").with_limit(0),
        )?;
        assert_eq!(insert.affected_rows, Some(1));
        assert!(!insert.rows_truncated());
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_query_safety_explicit_none_limit_keeps_uncapped_behavior() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE mysql_safety_uncapped (id INT PRIMARY KEY)",
        ))?;
        connection.execute(&QueryRequest::new(
            "INSERT INTO mysql_safety_uncapped (id) VALUES (1), (2), (3), (4), (5), (6), (7)",
        ))?;

        let mut request = QueryRequest::new("SELECT id FROM mysql_safety_uncapped");
        request.limit = None;
        let result = connection.execute(&request)?;
        assert_no_truncation(&result, 7);

        let batch = connection.execute(&QueryRequest::new(
            "INSERT INTO mysql_safety_uncapped (id) VALUES (8); SELECT id FROM mysql_safety_uncapped",
        ))?;
        assert_eq!(batch.additional_results.len(), 1);
        assert_no_truncation(&batch.additional_results[0], 8);

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_query_safety_budget_spans_batch_and_later_mutations_execute() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE mysql_safety_batch (n INT PRIMARY KEY)",
        ))?;
        let result = connection.execute(&QueryRequest::new(
            "SELECT n FROM mysql_safety_batch; INSERT INTO mysql_safety_batch (n) VALUES (1), (2), (3), (4); SELECT n FROM mysql_safety_batch ORDER BY n; INSERT INTO mysql_safety_batch (n) VALUES (5); SELECT n FROM mysql_safety_batch ORDER BY n"
        ).with_limit(2))?;
        assert_no_truncation(&result, 0);
        assert_eq!(result.additional_results.len(), 4);
        assert_eq!(result.additional_results[0].affected_rows, Some(4));
        assert_eq!(result.additional_results[1].rows.len(), 2);
        assert!(result.additional_results[1].rows_truncated());
        assert_eq!(result.additional_results[2].affected_rows, Some(1));
        assert!(result.additional_results[3].rows.is_empty());
        assert!(result.additional_results[3].rows_truncated());
        let count = connection.execute(&QueryRequest::new(
            "SELECT COUNT(*) FROM mysql_safety_batch",
        ))?;
        assert_eq!(count.rows[0][0], Value::Int(5));
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_query_safety_call_server_result_sets_share_budget() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri.clone())?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE mysql_safety_call (n INT PRIMARY KEY)",
        ))?;
        connection.execute(&QueryRequest::new(
            "INSERT INTO mysql_safety_call (n) VALUES (1), (2), (3), (4), (5)",
        ))?;

        // The driver's statement splitter cannot create a multi-SELECT procedure;
        // setup and cleanup use the text protocol, while CALL uses the request API.
        let setup_opts = mysql::Opts::from_url(&uri)
            .map_err(|e| DbError::query_failed(format!("setup options invalid: {e}")))?;
        let mut setup = mysql::Conn::new(setup_opts)
            .map_err(|e| DbError::query_failed(format!("setup connect failed: {e}")))?;
        setup
            .query_drop(
                "CREATE PROCEDURE dbflux_safety_proc() \
                 BEGIN \
                 SELECT n FROM mysql_safety_call ORDER BY n; \
                 SELECT n + 100 FROM mysql_safety_call ORDER BY n; \
                 END",
            )
            .map_err(|e| DbError::query_failed(format!("procedure setup failed: {e}")))?;

        let call =
            connection.execute(&QueryRequest::new("CALL dbflux_safety_proc()").with_limit(3))?;
        assert_eq!(call.rows.len(), 3);
        assert_eq!(call.rows[2][0], Value::Int(3));
        assert!(call.rows_truncated());

        assert_eq!(call.additional_results.len(), 2);
        let second_set = &call.additional_results[0];
        assert!(second_set.rows.is_empty());
        assert!(
            second_set.rows_truncated(),
            "the second set's rows were omitted"
        );
        let completion = &call.additional_results[1];
        assert!(completion.columns.is_empty());
        assert_eq!(completion.affected_rows, Some(0));
        assert!(!completion.rows_truncated());

        setup
            .query_drop("DROP PROCEDURE dbflux_safety_proc")
            .map_err(|e| DbError::query_failed(format!("procedure cleanup failed: {e}")))?;

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_query_safety_late_stream_error_propagates_after_cap_reached() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE mysql_safety_late (g INT PRIMARY KEY)",
        ))?;
        connection.execute(&QueryRequest::new(
            "INSERT INTO mysql_safety_late (g) VALUES (1), (2), (3), (4), (5)",
        ))?;
        let result = connection.execute(&QueryRequest::new(
            "SELECT g, CAST(CASE WHEN g = 3 THEN 'not-json' ELSE '[]' END AS JSON) AS j FROM mysql_safety_late ORDER BY g"
        ).with_limit(2));
        match result {
            Err(DbError::QueryFailed(formatted)) => assert!(
                formatted
                    .to_display_string()
                    .to_lowercase()
                    .contains("json")
            ),
            Err(other) => panic!("unexpected error kind: {other:?}"),
            Ok(result) => panic!("the cap must not hide the late stream error, got {result:?}"),
        }
        assert_eq!(
            connection
                .execute(&QueryRequest::new("SELECT 1"))?
                .rows
                .len(),
            1
        );
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mysql_query_safety_statement_timeout_rejected_before_execution() -> Result<(), DbError> {
    containers::with_mysql_url(|uri| {
        let (connection, _) = connect_mysql(uri)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE mysql_safety_timeout (n INT PRIMARY KEY)",
        ))?;
        let mut request = QueryRequest::new("INSERT INTO mysql_safety_timeout (n) VALUES (1)");
        request.statement_timeout = Some(Duration::from_secs(5));
        match connection.execute(&request) {
            Err(DbError::NotSupported(reason)) => {
                assert!(reason.to_lowercase().contains("timeout"))
            }
            Err(other) => panic!("unexpected error kind: {other:?}"),
            Ok(result) => panic!("requested deadline must be rejected, got {result:?}"),
        }
        let count = connection.execute(&QueryRequest::new(
            "SELECT COUNT(*) FROM mysql_safety_timeout",
        ))?;
        assert_eq!(count.rows[0][0], Value::Int(0));
        assert_eq!(
            connection
                .execute(&QueryRequest::new("SELECT 1"))?
                .rows
                .len(),
            1
        );
        Ok(())
    })
}
