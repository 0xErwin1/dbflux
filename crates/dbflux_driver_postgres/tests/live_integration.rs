use dbflux_core::{
    CollectionRef, ConnectionProfile, DbConfig, DbDriver, DbError, DescribeRequest, ExplainRequest,
    OrderByColumn, Pagination, QueryRequest, RecordIdentity, RowDelete, RowInsert, RowPatch,
    SchemaLoadingStrategy, SslMode, TableBrowseRequest, TableCountRequest, TableRef, Value,
};
use dbflux_driver_postgres::PostgresDriver;
use dbflux_test_support::containers;
use std::time::Duration;

fn connect_postgres(
    uri: String,
) -> Result<(Box<dyn dbflux_core::Connection>, PostgresDriver), dbflux_core::DbError> {
    let driver = PostgresDriver::new();
    let profile = ConnectionProfile::new(
        "live-postgres",
        DbConfig::Postgres {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 5432,
            user: String::new(),
            database: "postgres".to_string(),
            ssl_mode: SslMode::Prefer,
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
fn postgres_live_connect_ping_query_and_schema() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let result = connection.execute(&QueryRequest::new("SELECT 1 AS one"))?;
        assert_eq!(result.rows.len(), 1);

        assert_eq!(
            connection.schema_loading_strategy(),
            SchemaLoadingStrategy::ConnectionPerDatabase
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
fn postgres_schema_introspection() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE test_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE,
                age INTEGER DEFAULT 0
            )",
        ))?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE test_orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES test_users(id) ON DELETE CASCADE,
                amount NUMERIC(10, 2) NOT NULL
            )",
        ))?;

        connection.execute(&QueryRequest::new(
            "CREATE INDEX idx_orders_user_id ON test_orders(user_id)",
        ))?;

        connection.execute(&QueryRequest::new(
            "CREATE VIEW test_user_view AS SELECT id, name FROM test_users",
        ))?;

        let schema = connection.schema()?;
        assert!(schema.is_relational());

        let databases = schema.databases();
        assert!(!databases.is_empty());

        let table = connection.table_details("postgres", Some("public"), "test_users")?;
        assert_eq!(table.name, "test_users");

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

        let age_col = columns
            .iter()
            .find(|c| c.name == "age")
            .expect("age column");
        assert!(age_col.nullable);

        let indexes = table.indexes.as_ref().expect("indexes should be loaded");
        let idx_data = match indexes {
            dbflux_core::IndexData::Relational(v) => v,
            _ => panic!("expected relational index data"),
        };
        assert!(idx_data.iter().any(|i| i.is_primary));

        let relational = schema.as_relational().expect("should be relational schema");
        let has_view = relational
            .schemas
            .iter()
            .flat_map(|s| s.views.iter())
            .any(|v| v.name == "test_user_view");
        assert!(has_view, "view should appear in schema");

        let orders_table = connection.table_details("postgres", Some("public"), "test_orders")?;
        let fks = orders_table
            .foreign_keys
            .as_ref()
            .expect("foreign keys should be loaded");
        assert!(!fks.is_empty());
        let fk = &fks[0];
        assert_eq!(fk.referenced_table, "test_users");
        assert_eq!(fk.columns, vec!["user_id"]);
        assert_eq!(fk.referenced_columns, vec!["id"]);

        let schema_features = connection.schema_features();
        assert!(!schema_features.is_empty());

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// CRUD operations
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_crud_operations() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE crud_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INTEGER DEFAULT 0
            )",
        ))?;

        let insert_result = connection.insert_row(&RowInsert::new(
            "crud_test".to_string(),
            Some("public".to_string()),
            vec!["name".to_string(), "value".to_string()],
            vec![Value::Text("alice".to_string()), Value::Int(42)],
        ))?;
        assert_eq!(insert_result.affected_rows, 1);
        assert!(insert_result.returning_row.is_some());

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
            Some("public".to_string()),
            vec![("value".to_string(), Value::Int(99))],
        ))?;
        assert_eq!(update_result.affected_rows, 1);
        assert!(update_result.returning_row.is_some());

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
            Some("public".to_string()),
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
fn postgres_browse_and_count() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE browse_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            )",
        ))?;

        for i in 1..=25 {
            connection.execute(&QueryRequest::new(format!(
                "INSERT INTO browse_test (name) VALUES ('item_{}')",
                i
            )))?;
        }

        let table_ref = TableRef::with_schema("public", "browse_test");

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
fn postgres_explain() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE explain_test (id SERIAL PRIMARY KEY, name TEXT)",
        ))?;

        let table_ref = TableRef::with_schema("public", "explain_test");
        let result = connection.explain(&ExplainRequest::new(table_ref))?;
        assert!(!result.rows.is_empty() || result.text_body.is_some());

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_describe_table() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE describe_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                active BOOLEAN DEFAULT true
            )",
        ))?;

        let table_ref = TableRef::with_schema("public", "describe_test");
        let result = connection.describe_table(&DescribeRequest::new(table_ref))?;
        assert!(result.rows.len() >= 3);

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Query cancellation
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_cancel_query() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

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
fn postgres_code_generators() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE codegen_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )",
        ))?;

        let generators = connection.code_generators();
        assert!(!generators.is_empty());

        let table = connection.table_details("postgres", Some("public"), "codegen_test")?;

        for generator in generators {
            let code = connection.generate_code(generator.id, &table)?;
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
// Document operations (should return NotSupported)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_document_ops_not_supported() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let browse_result = connection.browse_collection(
            &dbflux_core::CollectionBrowseRequest::new(CollectionRef::new("db", "col")),
        );
        assert!(matches!(browse_result, Err(DbError::NotSupported(_))));

        assert!(connection.key_value_api().is_none());

        Ok(())
    })
}
