use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, DbKind, DescribeRequest, ExplainRequest,
    OrderByColumn, Pagination, QueryRequest, RecordIdentity, RowDelete, RowInsert, RowPatch,
    SchemaLoadingStrategy, SslMode, TableBrowseRequest, TableCountRequest, TableRef, Value,
};
use dbflux_driver_mysql::MysqlDriver;
use dbflux_test_support::containers;
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
            ssl_mode: SslMode::Disable,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    let connection = containers::retry_db_operation(Duration::from_secs(30), || {
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
