use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, DescribeRequest, ExplainRequest, OrderByColumn,
    Pagination, QueryRequest, RecordIdentity, RowDelete, RowInsert, RowPatch,
    SchemaLoadingStrategy, TableBrowseRequest, TableCountRequest, TableRef, Value,
};
use dbflux_driver_sqlite::SqliteDriver;

fn connect_sqlite() -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test.sqlite");

    let driver = SqliteDriver::new();
    let profile = ConnectionProfile::new("live-sqlite", DbConfig::SQLite { path: db_path });

    let connection = driver.connect(&profile)?;
    connection.ping()?;

    // Leak the tempdir so it doesn't get cleaned up while connection is alive.
    // The OS will clean it up when the process exits.
    std::mem::forget(temp_dir);

    Ok(connection)
}

// ---------------------------------------------------------------------------
// Basic connectivity
// ---------------------------------------------------------------------------

#[test]
fn sqlite_file_connect_ping_query_and_schema() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    ))?;
    connection.execute(&QueryRequest::new(
        "INSERT INTO users (name) VALUES ('alice')",
    ))?;

    let result = connection.execute(&QueryRequest::new("SELECT id, name FROM users"))?;
    assert_eq!(result.rows.len(), 1);

    assert_eq!(
        connection.schema_loading_strategy(),
        SchemaLoadingStrategy::SingleDatabase
    );

    let databases = connection.list_databases()?;
    assert!(databases.is_empty());

    let schema = connection.schema()?;
    assert!(schema.is_relational());
    let _ = schema.databases();

    Ok(())
}

// ---------------------------------------------------------------------------
// Schema introspection
// ---------------------------------------------------------------------------

#[test]
fn sqlite_schema_introspection() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE test_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER DEFAULT 0
        )",
    ))?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE test_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES test_users(id),
            amount REAL NOT NULL
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

    let table = connection.table_details("main", None, "test_users")?;
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
    assert!(!idx_data.is_empty());

    let relational = schema.as_relational().expect("should be relational schema");
    let has_view = relational
        .schemas
        .iter()
        .flat_map(|s| s.views.iter())
        .chain(relational.views.iter())
        .any(|v| v.name == "test_user_view");
    assert!(has_view, "view should appear in schema");

    Ok(())
}

// ---------------------------------------------------------------------------
// CRUD operations
// ---------------------------------------------------------------------------

#[test]
fn sqlite_crud_operations() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE crud_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value INTEGER DEFAULT 0
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
}

// ---------------------------------------------------------------------------
// Browse and count
// ---------------------------------------------------------------------------

#[test]
fn sqlite_browse_and_count() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE browse_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
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
}

// ---------------------------------------------------------------------------
// Explain and describe
// ---------------------------------------------------------------------------

#[test]
fn sqlite_explain() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE explain_test (id INTEGER PRIMARY KEY, name TEXT)",
    ))?;

    let table_ref = TableRef::new("explain_test");
    let result = connection.explain(&ExplainRequest::new(table_ref))?;
    assert!(!result.rows.is_empty() || result.text_body.is_some());

    Ok(())
}

#[test]
fn sqlite_describe_table() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE describe_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            active INTEGER DEFAULT 1
        )",
    ))?;

    let table_ref = TableRef::new("describe_test");
    let result = connection.describe_table(&DescribeRequest::new(table_ref))?;
    assert!(result.rows.len() >= 3);

    Ok(())
}

// ---------------------------------------------------------------------------
// Query cancellation
// ---------------------------------------------------------------------------

#[test]
fn sqlite_cancel_active() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    let result = connection.cancel_active();
    assert!(result.is_ok());

    Ok(())
}

// ---------------------------------------------------------------------------
// Code generators
// ---------------------------------------------------------------------------

#[test]
fn sqlite_code_generators() -> Result<(), DbError> {
    let connection = connect_sqlite()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE codegen_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )",
    ))?;

    let generators = connection.code_generators();
    assert!(!generators.is_empty());

    let table = connection.table_details("main", None, "codegen_test")?;

    for generator in generators {
        let code = connection.generate_code(&generator.id, &table)?;
        assert!(
            !code.is_empty(),
            "generator '{}' returned empty code",
            generator.id
        );
    }

    Ok(())
}
