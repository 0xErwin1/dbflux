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

// ---------------------------------------------------------------------------
// CREATE TABLE tests (5 tests)
// ---------------------------------------------------------------------------

#[test]
fn sqlite_ddl_create_table_integer_pk() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let table = SqliteFixtures::table_integer_pk();
    connection.execute(&QueryRequest::new(&table.create_sql))?;

    let table_details = connection.table_details("main", None, &table.name)?;
    assert_eq!(table_details.name, table.name);

    let columns = table_details
        .columns
        .as_ref()
        .expect("columns should be loaded");
    assert!(columns.len() >= 4);

    let id_col = columns.iter().find(|c| c.name == "id").expect("id column");
    assert!(id_col.is_primary_key);
    assert!(!id_col.nullable);

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn sqlite_ddl_create_table_composite_pk() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let table = SqliteFixtures::table_composite_pk();
    connection.execute(&QueryRequest::new(&table.create_sql))?;

    let table_details = connection.table_details("main", None, &table.name)?;
    assert_eq!(table_details.name, table.name);

    let columns = table_details
        .columns
        .as_ref()
        .expect("columns should be loaded");

    let order_id_col = columns
        .iter()
        .find(|c| c.name == "order_id")
        .expect("order_id column");
    assert!(order_id_col.is_primary_key);

    let product_id_col = columns
        .iter()
        .find(|c| c.name == "product_id")
        .expect("product_id column");
    assert!(product_id_col.is_primary_key);

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn sqlite_ddl_create_table_with_fk() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    connection.execute(&QueryRequest::new("PRAGMA foreign_keys = ON"))?;

    let parent_table = SqliteFixtures::table_integer_pk();
    connection.execute(&QueryRequest::new(&parent_table.create_sql))?;

    let child_table = SqliteFixtures::table_with_fk();
    connection.execute(&QueryRequest::new(&child_table.create_sql))?;

    let table_details = connection.table_details("main", None, &child_table.name)?;

    let fks = table_details
        .foreign_keys
        .as_ref()
        .expect("foreign keys should be loaded");
    assert!(!fks.is_empty());

    let fk = &fks[0];
    assert_eq!(fk.referenced_table, "users");
    assert_eq!(fk.columns, vec!["user_id"]);
    assert_eq!(fk.referenced_columns, vec!["id"]);

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn sqlite_ddl_create_table_with_check_constraint() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let table = SqliteFixtures::table_with_check();
    connection.execute(&QueryRequest::new(&table.create_sql))?;

    let table_details = connection.table_details("main", None, &table.name)?;
    assert_eq!(table_details.name, table.name);

    let insert_result = connection.execute(&QueryRequest::new(
        "INSERT INTO products (name, price, stock) VALUES ('test', -10, 5)",
    ));
    assert!(insert_result.is_err(), "should violate check constraint");

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn sqlite_ddl_create_table_with_unique_constraint() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let table = SqliteFixtures::table_with_unique();
    connection.execute(&QueryRequest::new(&table.create_sql))?;

    connection.execute(&QueryRequest::new(
        "INSERT INTO accounts (email, username) VALUES ('test@example.com', 'testuser')",
    ))?;

    let duplicate_result = connection.execute(&QueryRequest::new(
        "INSERT INTO accounts (email, username) VALUES ('test@example.com', 'testuser2')",
    ));
    assert!(
        duplicate_result.is_err(),
        "should violate unique constraint"
    );

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

// ---------------------------------------------------------------------------
// CREATE INDEX tests (3 tests)
// ---------------------------------------------------------------------------

#[test]
fn sqlite_ddl_create_index_single_column() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let table = SqliteFixtures::table_integer_pk();
    connection.execute(&QueryRequest::new(&table.create_sql))?;

    let index = SqliteFixtures::index_single_column();
    connection.execute(&QueryRequest::new(&index.create_sql))?;

    let table_details = connection.table_details("main", None, &table.name)?;
    let indexes = table_details
        .indexes
        .as_ref()
        .expect("indexes should be loaded");

    let index_list = match indexes {
        IndexData::Relational(list) => list,
        _ => panic!("expected relational index data"),
    };

    let has_index = index_list
        .iter()
        .any(|i| i.name == index.name && i.columns.contains(&"email".to_string()));
    assert!(has_index, "index should exist");

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn sqlite_ddl_create_index_unique() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let table = SqliteFixtures::table_integer_pk();
    connection.execute(&QueryRequest::new(&table.create_sql))?;

    let index = SqliteFixtures::index_unique();
    connection.execute(&QueryRequest::new(&index.create_sql))?;

    let table_details = connection.table_details("main", None, &table.name)?;
    let indexes = table_details
        .indexes
        .as_ref()
        .expect("indexes should be loaded");

    let index_list = match indexes {
        IndexData::Relational(list) => list,
        _ => panic!("expected relational index data"),
    };

    let found_index = index_list
        .iter()
        .find(|i| i.name == index.name)
        .expect("index should exist");
    assert!(found_index.is_unique, "index should be unique");

    connection.execute(&QueryRequest::new(
        "INSERT INTO users (username, email) VALUES ('alice', 'alice@example.com')",
    ))?;

    let duplicate_result = connection.execute(&QueryRequest::new(
        "INSERT INTO users (username, email) VALUES ('alice', 'bob@example.com')",
    ));
    assert!(
        duplicate_result.is_err(),
        "should violate unique index constraint"
    );

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn sqlite_ddl_create_index_composite() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    connection.execute(&QueryRequest::new("PRAGMA foreign_keys = ON"))?;

    let users_table = SqliteFixtures::table_integer_pk();
    connection.execute(&QueryRequest::new(&users_table.create_sql))?;

    let orders_table = SqliteFixtures::table_with_fk();
    connection.execute(&QueryRequest::new(&orders_table.create_sql))?;

    let index = SqliteFixtures::index_composite();
    connection.execute(&QueryRequest::new(&index.create_sql))?;

    let table_details = connection.table_details("main", None, &index.table)?;
    let indexes = table_details
        .indexes
        .as_ref()
        .expect("indexes should be loaded");

    let index_list = match indexes {
        IndexData::Relational(list) => list,
        _ => panic!("expected relational index data"),
    };

    let found_index = index_list
        .iter()
        .find(|i| i.name == index.name)
        .expect("index should exist");
    assert_eq!(
        found_index.columns.len(),
        2,
        "should have two columns in composite index"
    );

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

// ---------------------------------------------------------------------------
// CREATE VIEW test (1 test)
// ---------------------------------------------------------------------------

#[test]
fn sqlite_ddl_create_view() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let table = SqliteFixtures::table_integer_pk();
    connection.execute(&QueryRequest::new(&table.create_sql))?;

    let view = SqliteFixtures::view_simple();
    connection.execute(&QueryRequest::new(&view.create_sql))?;

    let schema = connection.schema()?;
    let relational = schema.as_relational().expect("should be relational schema");

    let has_view = relational
        .schemas
        .iter()
        .flat_map(|s| s.views.iter())
        .any(|v| v.name == view.name);
    assert!(has_view, "view should appear in schema");

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

// ---------------------------------------------------------------------------
// ALTER TABLE tests (2 tests - SQLite has limited ALTER TABLE support)
// ---------------------------------------------------------------------------

#[test]
fn sqlite_ddl_alter_table_add_column() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let scenario = SqliteFixtures::alter_add_column();
    for sql in &scenario.setup_sql {
        connection.execute(&QueryRequest::new(sql))?;
    }

    let before = connection.table_details("main", None, "alter_test")?;
    let before_cols = before.columns.as_ref().expect("columns should exist");
    let before_count = before_cols.len();

    connection.execute(&QueryRequest::new(&scenario.test_sql))?;

    let after = connection.table_details("main", None, "alter_test")?;
    let after_cols = after.columns.as_ref().expect("columns should exist");
    let after_count = after_cols.len();

    assert_eq!(after_count, before_count + 1, "should have one more column");

    let has_age = after_cols.iter().any(|c| c.name == "age");
    assert!(has_age, "should have age column");

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn sqlite_ddl_alter_table_rename_column() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let scenario = SqliteFixtures::alter_rename_column();
    for sql in &scenario.setup_sql {
        connection.execute(&QueryRequest::new(sql))?;
    }

    let before = connection.table_details("main", None, "alter_test")?;
    let before_cols = before.columns.as_ref().expect("columns should exist");
    assert!(before_cols.iter().any(|c| c.name == "old_name"));

    connection.execute(&QueryRequest::new(&scenario.test_sql))?;

    let after = connection.table_details("main", None, "alter_test")?;
    let after_cols = after.columns.as_ref().expect("columns should exist");

    assert!(!after_cols.iter().any(|c| c.name == "old_name"));
    assert!(after_cols.iter().any(|c| c.name == "new_name"));

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

// ---------------------------------------------------------------------------
// DROP tests (3 tests)
// ---------------------------------------------------------------------------

#[test]
fn sqlite_ddl_drop_table() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let table = SqliteFixtures::table_integer_pk();
    connection.execute(&QueryRequest::new(&table.create_sql))?;

    let before = connection.table_details("main", None, &table.name);
    assert!(before.is_ok(), "table should exist");

    connection.execute(&QueryRequest::new(format!("DROP TABLE {}", table.name)))?;

    let after = connection.table_details("main", None, &table.name);
    assert!(after.is_err(), "table should not exist");

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn sqlite_ddl_drop_index() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let table = SqliteFixtures::table_integer_pk();
    connection.execute(&QueryRequest::new(&table.create_sql))?;

    let index = SqliteFixtures::index_single_column();
    connection.execute(&QueryRequest::new(&index.create_sql))?;

    let before = connection.table_details("main", None, &table.name)?;
    let before_indexes = before.indexes.as_ref().expect("indexes should exist");
    let before_list = match before_indexes {
        IndexData::Relational(list) => list,
        _ => panic!("expected relational index data"),
    };
    assert!(before_list.iter().any(|i| i.name == index.name));

    connection.execute(&QueryRequest::new(format!("DROP INDEX {}", index.name)))?;

    let after = connection.table_details("main", None, &table.name)?;
    let after_indexes = after.indexes.as_ref().expect("indexes should exist");
    let after_list = match after_indexes {
        IndexData::Relational(list) => list,
        _ => panic!("expected relational index data"),
    };
    assert!(!after_list.iter().any(|i| i.name == index.name));

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn sqlite_ddl_drop_view() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let table = SqliteFixtures::table_integer_pk();
    connection.execute(&QueryRequest::new(&table.create_sql))?;

    let view = SqliteFixtures::view_simple();
    connection.execute(&QueryRequest::new(&view.create_sql))?;

    let before_schema = connection.schema()?;
    let before_relational = before_schema
        .as_relational()
        .expect("should be relational schema");
    let has_view_before = before_relational
        .schemas
        .iter()
        .flat_map(|s| s.views.iter())
        .any(|v| v.name == view.name);
    assert!(has_view_before, "view should exist");

    connection.execute(&QueryRequest::new(format!("DROP VIEW {}", view.name)))?;

    let after_schema = connection.schema()?;
    let after_relational = after_schema
        .as_relational()
        .expect("should be relational schema");
    let has_view_after = after_relational
        .schemas
        .iter()
        .flat_map(|s| s.views.iter())
        .any(|v| v.name == view.name);
    assert!(!has_view_after, "view should not exist");

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

// ---------------------------------------------------------------------------
// DELETE (TRUNCATE equivalent) test
// ---------------------------------------------------------------------------

#[test]
fn sqlite_ddl_delete_all_rows() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    connection.execute(&QueryRequest::new(
        "CREATE TABLE truncate_test (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)",
    ))?;

    for i in 1..=10 {
        connection.execute(&QueryRequest::new(format!(
            "INSERT INTO truncate_test (value) VALUES ('item_{}')",
            i
        )))?;
    }

    let before = connection.execute(&QueryRequest::new("SELECT COUNT(*) FROM truncate_test"))?;
    let count_before = match &before.rows[0][0] {
        Value::Int(n) => *n,
        _ => panic!("expected integer count"),
    };
    assert_eq!(count_before, 10);

    connection.execute(&QueryRequest::new("DELETE FROM truncate_test"))?;

    let after = connection.execute(&QueryRequest::new("SELECT COUNT(*) FROM truncate_test"))?;
    let count_after = match &after.rows[0][0] {
        Value::Int(n) => *n,
        _ => panic!("expected integer count"),
    };
    assert_eq!(count_after, 0);

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

// ---------------------------------------------------------------------------
// Error scenario tests
// ---------------------------------------------------------------------------

#[test]
fn sqlite_ddl_error_constraint_violation() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    let table = SqliteFixtures::table_with_check();
    connection.execute(&QueryRequest::new(&table.create_sql))?;

    let result = connection.execute(&QueryRequest::new(
        "INSERT INTO products (name, price, stock) VALUES ('bad', -5, 10)",
    ));
    assert!(result.is_err());

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn sqlite_ddl_error_fk_violation() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    connection.execute(&QueryRequest::new("PRAGMA foreign_keys = ON"))?;

    let parent_table = SqliteFixtures::table_integer_pk();
    connection.execute(&QueryRequest::new(&parent_table.create_sql))?;

    let child_table = SqliteFixtures::table_with_fk();
    connection.execute(&QueryRequest::new(&child_table.create_sql))?;

    let result = connection.execute(&QueryRequest::new(
        "INSERT INTO orders (user_id, total, status) VALUES (9999, 100.00, 'pending')",
    ));
    assert!(result.is_err());

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn sqlite_ddl_error_drop_with_dependents() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    cleanup_test_tables(&*connection);

    connection.execute(&QueryRequest::new("PRAGMA foreign_keys = ON"))?;

    // Create parent table
    let parent_table = SqliteFixtures::table_integer_pk();
    connection.execute(&QueryRequest::new(&parent_table.create_sql))?;

    // Create child table WITHOUT CASCADE - this tests FK enforcement properly
    // Note: Using a direct CREATE TABLE instead of fixture because the fixture has CASCADE
    connection.execute(&QueryRequest::new(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            total REAL NOT NULL,
            status TEXT DEFAULT 'pending'
        )",
    ))?;

    // Insert a user first
    connection.execute(&QueryRequest::new(
        "INSERT INTO users (username, email) VALUES ('test', 'test@test.com')",
    ))?;

    // Insert a dependent row into the child table
    connection.execute(&QueryRequest::new(
        "INSERT INTO orders (user_id, total, status) VALUES (1, 100.0, 'pending')",
    ))?;

    // Now DROP TABLE should fail because orders has a dependent row
    let result = connection.execute(&QueryRequest::new("DROP TABLE users"));
    assert!(result.is_err(), "should fail to drop table with dependents");

    connection.execute(&QueryRequest::new("PRAGMA foreign_keys = OFF"))?;
    // With FK checks off, should be able to drop
    let drop_result = connection.execute(&QueryRequest::new("DROP TABLE users"));
    assert!(drop_result.is_ok(), "should succeed with FK checks off");

    cleanup_test_tables(&*connection);
    drop(connection);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn sqlite_native_drop_prepare_is_read_only_for_special_shapes_and_attached_catalogs()
-> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
    for statement in [
        "CREATE TABLE people (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            retained TEXT NOT NULL,
            obsolete TEXT,
            CHECK (length(retained) > 0)
        )",
        "INSERT INTO people (retained, obsolete) VALUES ('kept', 'removed')",
        "CREATE VIEW retained_people AS SELECT id, retained FROM people",
        "ATTACH DATABASE ':memory:' AS auxiliary",
    ] {
        connection.execute(&QueryRequest::new(statement))?;
    }
    rusqlite::Connection::open(&db_path)
        .and_then(|raw| {
            raw.execute_batch(
                "CREATE TRIGGER people_after_insert AFTER INSERT ON people
                 BEGIN INSERT INTO people (retained) VALUES ('triggered'); END",
            )
        })
        .map_err(|error| {
            DbError::query_failed(format!("could not create fixture trigger: {error}"))
        })?;
    let schema_before = connection.execute(&QueryRequest::new(
        "SELECT type, name, sql FROM main.sqlite_master ORDER BY type, name",
    ))?;
    let rows_before = connection.execute(&QueryRequest::new(
        "SELECT id, retained, obsolete FROM main.people ORDER BY id",
    ))?;
    let foreign_keys_before = connection.execute(&QueryRequest::new("PRAGMA foreign_keys"))?;

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![TableAlterOperation::DropColumn {
                name: "obsolete".to_string(),
            }],
            expected_before: Vec::new(),
        })?;

    assert_eq!(plan.preview().route, TableAlterRoute::Native);
    assert_eq!(
        schema_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT type, name, sql FROM main.sqlite_master ORDER BY type, name",
            ))?
            .rows,
        "preparing native DROP must not alter the catalog"
    );
    assert_eq!(
        rows_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT id, retained, obsolete FROM main.people ORDER BY id",
            ))?
            .rows,
        "preparing native DROP must not alter table data"
    );
    assert_eq!(
        foreign_keys_before.rows,
        connection
            .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
            .rows,
        "preparing native DROP must not change connection settings"
    );

    plan.execute()?;
    let retained = connection.execute(&QueryRequest::new(
        "SELECT id, retained FROM main.people ORDER BY id",
    ))?;
    assert_eq!(retained.rows.len(), 1);
    assert_eq!(
        connection
            .execute(&QueryRequest::new("SELECT retained FROM retained_people"))?
            .rows
            .len(),
        1,
        "an unrelated retained-column view must keep working"
    );
    Ok(())
}

#[test]
fn sqlite_native_drop_rejects_unsafe_connection_settings_without_mutation() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE people (id INTEGER PRIMARY KEY, obsolete TEXT, retained TEXT);
         INSERT INTO people VALUES (1, 'remove', 'keep');
         PRAGMA writable_schema = ON",
    ))?;
    let schema_before = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'people'",
    ))?;
    let rows_before = connection.execute(&QueryRequest::new("SELECT * FROM main.people"))?;

    let error = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![TableAlterOperation::DropColumn {
                name: "obsolete".to_string(),
            }],
            expected_before: Vec::new(),
        })
        .err()
        .expect("unsafe connection settings must reject planning");
    assert!(
        error.to_string().contains("writable_schema"),
        "the rejection must name the unsafe setting: {error}"
    );
    assert_eq!(
        schema_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'people'",
            ))?
            .rows
    );
    assert_eq!(
        rows_before.rows,
        connection
            .execute(&QueryRequest::new("SELECT * FROM main.people"))?
            .rows
    );
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = OFF"))?;
    Ok(())
}

#[test]
fn sqlite_native_drop_preflights_index_and_foreign_key_dependencies() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    for statement in [
        "PRAGMA foreign_keys = ON",
        "CREATE TABLE indexed (id INTEGER PRIMARY KEY, obsolete TEXT, retained TEXT)",
        "CREATE INDEX indexed_obsolete ON indexed(obsolete)",
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, obsolete TEXT, retained TEXT)",
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_obsolete TEXT REFERENCES parent(obsolete))",
    ] {
        connection.execute(&QueryRequest::new(statement))?;
    }

    for (table, column, dependency) in [
        ("indexed", "obsolete", "index"),
        ("indexed", "id", "primary key"),
        ("parent", "obsolete", "foreign key"),
    ] {
        let error = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new(table),
                operations: vec![TableAlterOperation::DropColumn {
                    name: column.to_string(),
                }],
                expected_before: Vec::new(),
            })
            .err()
            .expect("known selected-column dependencies must reject planning");
        assert!(
            error
                .to_string()
                .contains(&format!("known dependency: {dependency}")),
            "the driver must identify the {dependency} before execution: {error}"
        );
    }

    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT COUNT(*) FROM pragma_table_info('indexed') WHERE name = 'obsolete'",
            ))?
            .rows[0][0],
        Value::Int(1)
    );
    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT COUNT(*) FROM pragma_table_info('parent') WHERE name = 'obsolete'",
            ))?
            .rows[0][0],
        Value::Int(1)
    );
    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT COUNT(*) FROM pragma_table_info('indexed') WHERE name = 'id'",
            ))?
            .rows[0][0],
        Value::Int(1)
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_accepts_distinct_foreign_key_relations_to_the_same_parent() -> Result<(), DbError>
{
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY); \
         CREATE TABLE child (
             a INTEGER,
             b INTEGER,
             payload TEXT,
             FOREIGN KEY (a) REFERENCES parent(id) ON DELETE CASCADE,
             FOREIGN KEY (b) REFERENCES parent(id) ON DELETE SET NULL
         )",
    ))?;
    let schema_before = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'child'",
    ))?;

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("child"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "payload".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        })?;

    assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    assert_eq!(
        schema_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'child'",
            ))?
            .rows,
        "FK proof during prepare must remain read-only"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_rejects_catalog_sql_that_disagrees_with_cached_table_xinfo() -> Result<(), DbError>
{
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, retained TEXT, payload TEXT)",
    ))?;
    connection.execute(&QueryRequest::new("PRAGMA main.table_xinfo('t')"))?;
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = ON"))?;
    connection.execute(&QueryRequest::new(
        "UPDATE main.sqlite_master \
         SET sql = 'CREATE TABLE t (id INTEGER PRIMARY KEY, retained INTEGER, payload TEXT)' \
         WHERE type = 'table' AND name = 't'",
    ))?;
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = OFF"))?;
    let snapshot_before = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
    ))?;

    let result = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "payload".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        });
    let error = match result {
        Ok(_) => panic!("rebuild must reject catalog and cached table_xinfo disagreement"),
        Err(error) => error,
    };

    assert!(
        error.to_string().contains("catalog proof"),
        "metadata disagreement must be actionable: {error}"
    );
    assert_eq!(
        snapshot_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
            ))?
            .rows,
        "prepare must not mutate the catalog snapshot"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_rejects_cached_autoindex_not_declared_by_catalog_sql() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT UNIQUE, note TEXT)",
    ))?;
    let autoindex_before = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
    assert!(
        !autoindex_before.rows.is_empty(),
        "fixture must expose its UNIQUE autoindex before catalog-only mutation"
    );
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = ON"))?;
    connection.execute(&QueryRequest::new(
        "UPDATE main.sqlite_master \
         SET sql = 'CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT, note TEXT)' \
         WHERE type = 'table' AND name = 't'",
    ))?;
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = OFF"))?;
    let catalog_after = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
    ))?;
    assert!(
        catalog_after.rows[0][0]
            .to_string()
            .contains("payload TEXT, note TEXT"),
        "fixture must remove UNIQUE only from catalog SQL"
    );
    let autoindex_after = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
    assert_eq!(
        autoindex_before.rows, autoindex_after.rows,
        "fixture must retain cached engine index metadata without a schema-version bump"
    );

    let error = match connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "note".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        }) {
        Ok(_) => panic!("an undeclared cached autoindex must reject rebuild planning"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("index"),
        "the catalog proof must identify the unaccounted index: {error}"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_rejects_cached_explicit_index_metadata_that_disagrees_with_catalog_sql()
-> Result<(), DbError> {
    for (name, cached_sql, catalog_sql) in [
        (
            "unique",
            "CREATE UNIQUE INDEX idx ON t(a, b)",
            "CREATE INDEX idx ON t(a, b)",
        ),
        (
            "direction",
            "CREATE INDEX idx ON t(a ASC, b DESC)",
            "CREATE INDEX idx ON t(a DESC, b DESC)",
        ),
        (
            "collation",
            "CREATE INDEX idx ON t(a, b)",
            "CREATE INDEX idx ON t(a COLLATE BINARY, b)",
        ),
        (
            "name",
            "CREATE INDEX idx ON t(a, b)",
            "CREATE INDEX other_idx ON t(a, b)",
        ),
        (
            "target",
            "CREATE INDEX idx ON t(a, b)",
            "CREATE INDEX idx ON other(a, b)",
        ),
        (
            "key order",
            "CREATE INDEX idx ON t(a, b)",
            "CREATE INDEX idx ON t(b, a)",
        ),
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT COLLATE NOCASE, b TEXT, note TEXT); \
             CREATE TABLE other (a TEXT COLLATE NOCASE, b TEXT);",
        ))?;
        connection.execute(&QueryRequest::new(cached_sql))?;
        let index_before =
            connection.execute(&QueryRequest::new("PRAGMA main.index_xinfo('idx')"))?;
        assert!(
            !index_before.rows.is_empty(),
            "{name} fixture must warm the explicit index metadata"
        );
        connection.execute(&QueryRequest::new("PRAGMA writable_schema = ON"))?;
        connection.execute(&QueryRequest::new(format!(
            "UPDATE main.sqlite_master SET sql = '{}' WHERE type = 'index' AND name = 'idx'",
            catalog_sql
        )))?;
        connection.execute(&QueryRequest::new("PRAGMA writable_schema = OFF"))?;
        let catalog_after = connection.execute(&QueryRequest::new(
            "SELECT sql FROM main.sqlite_master WHERE type = 'index' AND name = 'idx'",
        ))?;
        assert_eq!(
            catalog_after.rows[0][0].to_string(),
            catalog_sql,
            "{name} fixture must alter only the catalog index SQL"
        );
        let index_after =
            connection.execute(&QueryRequest::new("PRAGMA main.index_xinfo('idx')"))?;
        assert_eq!(
            index_before.rows, index_after.rows,
            "{name} fixture must retain cached index metadata without a schema-version bump"
        );

        let error = match connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("t"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "note".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            }) {
            Ok(_) => panic!("catalog SQL must prove every explicit index detail"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("index"),
            "{name} disagreement must reject with index evidence: {error}"
        );
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_accepts_table_integer_primary_key_desc_without_autoindex() -> Result<(), DbError>
{
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t(id INTEGER,note TEXT,PRIMARY KEY(id DESC)); \
         INSERT INTO t(id, note) VALUES (7, 'before')",
    ))?;
    let indexes = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
    assert!(
        indexes.rows.is_empty(),
        "table-level INTEGER PRIMARY KEY DESC must not create a physical autoindex"
    );
    let xinfo = connection.execute(&QueryRequest::new(
        "SELECT name, pk, hidden FROM pragma_table_xinfo('t') ORDER BY cid",
    ))?;
    assert_eq!(
        xinfo.rows[0],
        vec![Value::Text("id".to_string()), Value::Int(1), Value::Int(0)]
    );
    let rowid = connection.execute(&QueryRequest::new("SELECT rowid, id FROM main.t"))?;
    assert_eq!(rowid.rows, vec![vec![Value::Int(7), Value::Int(7)]]);
    let schema_before = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
    ))?;
    let rows_before =
        connection.execute(&QueryRequest::new("SELECT rowid, id, note FROM main.t"))?;

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "note".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        })?;

    assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    assert_eq!(
        schema_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
            ))?
            .rows,
        "rebuild preparation must remain read-only"
    );
    assert_eq!(
        rows_before.rows,
        connection
            .execute(&QueryRequest::new("SELECT rowid, id, note FROM main.t"))?
            .rows,
        "rebuild preparation must preserve the rowid-alias fixture"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_triangulates_table_integer_primary_key_sort_orders_and_physical_keys()
-> Result<(), DbError> {
    for source in [
        "CREATE TABLE t(id INTEGER,note TEXT,PRIMARY KEY(id));",
        "CREATE TABLE t(id INTEGER,note TEXT,PRIMARY KEY(id ASC));",
        "CREATE TABLE t(id INTEGER,note TEXT,PRIMARY KEY(id DESC));",
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(source))?;
        let indexes = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
        assert!(
            indexes.rows.is_empty(),
            "table-level INTEGER PRIMARY KEY sort order must retain rowid-alias semantics"
        );
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("t"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "note".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    }
    for source in [
        "CREATE TABLE t(id INT,note TEXT,PRIMARY KEY(id DESC));",
        "CREATE TABLE t(id INTEGER,part TEXT,note TEXT,PRIMARY KEY(id DESC,part ASC));",
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(source))?;
        let indexes = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
        assert_eq!(
            indexes.rows.len(),
            1,
            "non-INTEGER and composite primary keys must retain their physical autoindex"
        );
        assert_eq!(indexes.rows[0][2], Value::Int(1));
        assert_eq!(indexes.rows[0][3], Value::Text("pk".to_string()));
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("t"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "note".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_preserves_inline_integer_primary_key_desc_physical_index_and_identity()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t(id INTEGER PRIMARY KEY DESC,note TEXT); \
         INSERT INTO t(id, note) VALUES (7, 'before')",
    ))?;
    let indexes = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
    assert_eq!(
        indexes.rows.len(),
        1,
        "inline DESC fixture must create an autoindex"
    );
    assert_eq!(indexes.rows[0][2], Value::Int(1));
    assert_eq!(indexes.rows[0][3], Value::Text("pk".to_string()));
    let autoindex = indexes.rows[0][1].to_string();
    let keys = connection.execute(&QueryRequest::new(format!(
        "SELECT name, \"desc\" FROM pragma_index_xinfo('{autoindex}') \
         WHERE \"key\" = 1 ORDER BY seqno"
    )))?;
    assert_eq!(
        keys.rows,
        vec![vec![Value::Text("id".to_string()), Value::Int(1)]]
    );
    let rowid = connection.execute(&QueryRequest::new("SELECT rowid, id FROM main.t"))?;
    assert_eq!(rowid.rows, vec![vec![Value::Int(1), Value::Int(7)]]);

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "note".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        })?;
    assert!(plan.preview().statements.iter().any(|statement| {
        statement.starts_with("INSERT INTO") && statement.contains("SELECT \"rowid\", \"id\"")
    }));
    Ok(())
}

#[test]
fn sqlite_rebuild_accepts_direction_coalesced_autoindexes() -> Result<(), DbError> {
    for (source, expected_origin) in [
        (
            "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT,note TEXT,UNIQUE(a ASC),UNIQUE(a DESC));",
            "u",
        ),
        (
            "CREATE TABLE t(a TEXT,note TEXT,UNIQUE(a ASC),PRIMARY KEY(a DESC));",
            "pk",
        ),
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(source))?;
        let schema_before = connection.execute(&QueryRequest::new(
            "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
        ))?;
        let indexes = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
        assert_eq!(
            indexes.rows.len(),
            1,
            "fixture must coalesce equivalent declarations into one physical autoindex"
        );
        let autoindex = &indexes.rows[0];
        assert_eq!(autoindex[2], Value::Int(1));
        assert_eq!(autoindex[3], Value::Text(expected_origin.to_string()));
        let keys = connection.execute(&QueryRequest::new(format!(
            "SELECT seqno, name, \"desc\", coll, \"key\" FROM pragma_index_xinfo('{}') \
             WHERE \"key\" = 1 ORDER BY seqno",
            autoindex[1]
        )))?;
        assert_eq!(
            keys.rows,
            vec![vec![
                Value::Int(0),
                Value::Text("a".to_string()),
                Value::Int(0),
                Value::Text("BINARY".to_string()),
                Value::Int(1),
            ]],
            "the first declaration must retain the physical autoindex key direction"
        );

        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("t"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "note".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;

        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
        assert_eq!(
            schema_before.rows,
            connection
                .execute(&QueryRequest::new(
                    "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
                ))?
                .rows,
            "rebuild preparation must remain read-only"
        );
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_triangulates_autoindex_coalescing_order_and_integer_primary_keys()
-> Result<(), DbError> {
    for (source, expected_origin, expected_column, expected_descending) in [
        (
            "CREATE TABLE t(a TEXT,note TEXT,UNIQUE(a DESC),UNIQUE(a ASC));",
            "u",
            "a",
            1,
        ),
        (
            "CREATE TABLE t(a TEXT,note TEXT,PRIMARY KEY(a DESC),UNIQUE(a ASC));",
            "pk",
            "a",
            1,
        ),
        (
            "CREATE TABLE t(a TEXT,note TEXT,UNIQUE(a DESC),UNIQUE(a DESC));",
            "u",
            "a",
            1,
        ),
        (
            "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT,note TEXT,UNIQUE(a));",
            "u",
            "a",
            0,
        ),
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(source))?;
        let schema_before = connection.execute(&QueryRequest::new(
            "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
        ))?;
        let indexes = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
        assert_eq!(
            indexes.rows.len(),
            1,
            "fixture must expose exactly one coalesced or independent physical autoindex"
        );
        let autoindex = &indexes.rows[0];
        assert_eq!(autoindex[2], Value::Int(1));
        assert_eq!(autoindex[3], Value::Text(expected_origin.to_string()));
        let keys = connection.execute(&QueryRequest::new(format!(
            "SELECT seqno, name, \"desc\", coll, \"key\" FROM pragma_index_xinfo('{}') \
             WHERE \"key\" = 1 ORDER BY seqno",
            autoindex[1]
        )))?;
        assert_eq!(
            keys.rows,
            vec![vec![
                Value::Int(0),
                Value::Text(expected_column.to_string()),
                Value::Int(expected_descending),
                Value::Text("BINARY".to_string()),
                Value::Int(1),
            ]],
            "physical autoindex proof must retain the first declaration's complete key intent"
        );

        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("t"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "note".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
        assert_eq!(
            schema_before.rows,
            connection
                .execute(&QueryRequest::new(
                    "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
                ))?
                .rows,
            "rebuild preparation must remain read-only"
        );
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_rejects_cached_autoindex_direction_that_disagrees_with_catalog_sql()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT,note TEXT,UNIQUE(a ASC));",
    ))?;
    let indexes_before = connection.execute(&QueryRequest::new("PRAGMA main.index_list('t')"))?;
    assert_eq!(
        indexes_before.rows.len(),
        1,
        "fixture must expose an autoindex"
    );
    let autoindex = indexes_before.rows[0][1].to_string();
    let keys_before = connection.execute(&QueryRequest::new(format!(
        "SELECT seqno, name, \"desc\", coll, \"key\" FROM pragma_index_xinfo('{autoindex}') \
         WHERE \"key\" = 1 ORDER BY seqno"
    )))?;
    assert_eq!(keys_before.rows[0][2], Value::Int(0));
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = ON"))?;
    connection.execute(&QueryRequest::new(
        "UPDATE main.sqlite_master \
         SET sql = 'CREATE TABLE t(id INTEGER PRIMARY KEY,a TEXT,note TEXT,UNIQUE(a DESC))' \
         WHERE type = 'table' AND name = 't'",
    ))?;
    connection.execute(&QueryRequest::new("PRAGMA writable_schema = OFF"))?;
    let catalog_after = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 't'",
    ))?;
    assert!(
        catalog_after.rows[0][0]
            .to_string()
            .contains("UNIQUE(a DESC)"),
        "fixture must change only catalog SQL to DESC"
    );
    let keys_after = connection.execute(&QueryRequest::new(format!(
        "SELECT seqno, name, \"desc\", coll, \"key\" FROM pragma_index_xinfo('{autoindex}') \
         WHERE \"key\" = 1 ORDER BY seqno"
    )))?;
    assert_eq!(
        keys_before.rows, keys_after.rows,
        "fixture must retain cached physical autoindex direction without a schema-version bump"
    );

    let result = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "note".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        });
    let error = match result {
        Ok(_) => panic!("a cached autoindex direction mismatch must reject rebuild planning"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("catalog proof"),
        "physical autoindex direction must remain part of the proof: {error}"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_proves_autoindex_coalescing_and_key_term_details() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE coalesced (
             id INTEGER PRIMARY KEY,
             key_value TEXT COLLATE NOCASE,
             note TEXT,
             UNIQUE (key_value),
             UNIQUE (key_value COLLATE NOCASE ASC)
         );
         CREATE TABLE integer_primary (
             id INTEGER PRIMARY KEY,
             unique_value TEXT COLLATE RTRIM UNIQUE,
             note TEXT
         );
         CREATE TABLE composite_primary (
             a TEXT COLLATE NOCASE,
             b INTEGER,
             note TEXT,
             PRIMARY KEY (a COLLATE NOCASE DESC, b ASC)
         );
         CREATE TABLE explicit_index (
             id INTEGER PRIMARY KEY,
             payload TEXT COLLATE NOCASE,
             note TEXT
         );
         CREATE UNIQUE INDEX \"ix: payload\" ON explicit_index(payload COLLATE NOCASE DESC)",
    ))?;
    let coalesced_indexes =
        connection.execute(&QueryRequest::new("PRAGMA main.index_list('coalesced')"))?;
    assert_eq!(
        coalesced_indexes.rows.len(),
        1,
        "SQLite must coalesce equivalent UNIQUE declarations into one physical autoindex"
    );

    for table in [
        "coalesced",
        "integer_primary",
        "composite_primary",
        "explicit_index",
    ] {
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new(table),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "note".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
        if table == "explicit_index" {
            assert!(plan.preview().statements.iter().any(|statement| {
                statement.contains(
                    "CREATE UNIQUE INDEX \"ix: payload\" ON explicit_index(payload COLLATE NOCASE DESC)",
                )
            }));
        }
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_preserves_proven_quoted_explicit_index_sql() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT, note TEXT); \
         CREATE UNIQUE INDEX \"ix: payload\" ON t(payload DESC)",
    ))?;

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "note".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        })?;

    assert!(plan.preview().statements.iter().any(|statement| {
        statement.contains("CREATE UNIQUE INDEX \"ix: payload\" ON t(payload DESC)")
    }));
    Ok(())
}

#[test]
fn sqlite_rebuild_rejects_all_shadowed_rowid_aliases() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t (rowid TEXT, _rowid_ TEXT, oid TEXT, payload TEXT)",
    ))?;

    let result = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "payload".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        });
    let error = match result {
        Ok(_) => panic!("all rowid aliases must be rejected without an INTEGER PRIMARY KEY"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("requires an INTEGER PRIMARY KEY")
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_prepare_is_read_only_and_execute_applies_requested_delta() -> Result<(), DbError>
{
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "PRAGMA foreign_keys = ON; \
         CREATE TABLE people (id INTEGER PRIMARY KEY, legacy TEXT DEFAULT 'NULL', retained TEXT NOT NULL DEFAULT ('keep')); \
         INSERT INTO people (id, legacy, retained) VALUES (0, 'old', 'kept'), (-7, 'older', 'also kept')",
    ))?;
    let schema_before = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'people'",
    ))?;
    let rows_before = connection.execute(&QueryRequest::new(
        "SELECT id, legacy, retained FROM main.people ORDER BY id",
    ))?;
    let settings_before = connection.execute(&QueryRequest::new("PRAGMA foreign_keys"))?;
    let before = rebuild_boundary_snapshot(&*connection, "people")?;
    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "legacy".to_string(),
                new_type: Some("VARCHAR(32)".to_string()),
                nullable: Some(false),
                default: Some(OwnedDefaultSpec::Set("NULL".to_string())),
            }],
            expected_before: vec![TableAlterExpectedColumn {
                name: "legacy".to_string(),
                type_name: Some("TEXT".to_string()),
                nullable: Some(true),
                default: Some(Some("'NULL'".to_string())),
            }],
        })?;
    assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    assert!(plan.preview().driver_managed);
    assert!(plan.preview().table_atomic);
    assert!(
        plan.preview()
            .statements
            .iter()
            .any(|statement| { statement.contains("VARCHAR(32) NOT NULL DEFAULT NULL") })
    );
    assert_eq!(
        schema_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'people'",
            ))?
            .rows,
        "rebuild preparation must not mutate the source schema"
    );
    assert_eq!(
        rows_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT id, legacy, retained FROM main.people ORDER BY id",
            ))?
            .rows,
        "rebuild preparation must not mutate populated source data"
    );
    assert_eq!(
        settings_before.rows,
        connection
            .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
            .rows,
        "rebuild preparation must not mutate connection settings"
    );
    assert_eq!(
        before,
        rebuild_boundary_snapshot(&*connection, "people")?,
        "preparation must retain the complete people boundary"
    );

    plan.execute()?;

    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT type, \"notnull\", dflt_value FROM pragma_table_xinfo('people') WHERE name = 'legacy'",
            ))?
            .rows,
        vec![vec![
            Value::Text("VARCHAR(32)".to_string()),
            Value::Int(1),
            Value::Text("NULL".to_string()),
        ]],
        "execution must apply only the requested legacy-column definition"
    );
    assert_eq!(
        rows_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT id, legacy, retained FROM main.people ORDER BY id",
            ))?
            .rows,
        "execution must preserve populated source rows exactly"
    );
    assert_eq!(
        settings_before.rows,
        connection
            .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
            .rows,
        "execution must restore connection settings"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_mixes_payload_alter_and_independent_drop_without_mutation() -> Result<(), DbError>
{
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE people (id INTEGER PRIMARY KEY, legacy TEXT, obsolete TEXT, retained TEXT);
         INSERT INTO people VALUES (1, 'old', 'remove', 'keep')",
    ))?;
    let before = connection.execute(&QueryRequest::new("SELECT * FROM main.people"))?;
    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![
                TableAlterOperation::AlterColumn {
                    name: "legacy".to_string(),
                    new_type: Some("VARCHAR(16)".to_string()),
                    nullable: Some(false),
                    default: None,
                },
                TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                },
            ],
            expected_before: Vec::new(),
        })?;
    let create = plan
        .preview()
        .statements
        .iter()
        .find(|statement| statement.starts_with("CREATE TABLE"))
        .expect("rebuild preview must include replacement CREATE TABLE");
    assert!(create.contains("legacy VARCHAR(16) NOT NULL"));
    assert!(!create.contains("obsolete"));
    assert!(create.contains("retained TEXT"));
    assert_eq!(
        before.rows,
        connection
            .execute(&QueryRequest::new("SELECT * FROM main.people"))?
            .rows
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_uses_hidden_rowid_for_composite_primary_keys_and_shadowed_aliases()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE composite (a INTEGER, b TEXT, p TEXT, PRIMARY KEY(a, b));
         INSERT INTO composite VALUES (-3, 'negative', 'payload');
         INSERT INTO composite VALUES (0, 'zero', 'payload');
         CREATE TABLE shadowed (rowid TEXT, p TEXT);
         INSERT INTO shadowed VALUES ('shadow', 'payload')",
    ))?;

    for (table, expected_identity) in [("composite", "\"rowid\""), ("shadowed", "\"_rowid_\"")] {
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new(table),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "p".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert!(
            plan.preview()
                .statements
                .iter()
                .any(|statement| statement.starts_with("INSERT INTO")
                    && statement.contains(expected_identity)),
            "rebuild must retain the proven hidden identity for {table}"
        );
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_rejects_global_objects_without_mutation() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE people (id INTEGER PRIMARY KEY, legacy TEXT, retained TEXT);
         INSERT INTO people VALUES (1, 'before', 'retained');
         CREATE TABLE audit (id INTEGER PRIMARY KEY, message TEXT);
         INSERT INTO audit VALUES (1, 'view source');
         CREATE VIEW audit_view AS SELECT message FROM audit",
    ))?;
    let before = rebuild_boundary_snapshot(&*connection, "people")?;

    let error = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "legacy".to_string(),
                new_type: Some("TEXT".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        })
        .err()
        .expect("rebuild must reject global view and trigger uncertainty");
    assert!(
        error.to_string().contains("view or trigger"),
        "global-object rejection must be actionable: {error}"
    );
    assert_eq!(
        before,
        rebuild_boundary_snapshot(&*connection, "people")?,
        "rejection must retain catalog, target schema and rows, and connection settings together"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_accepts_implicit_parent_primary_key_foreign_keys() -> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY); \
         CREATE TABLE child (\
             id INTEGER PRIMARY KEY,\
             parent_id INTEGER REFERENCES parent,\
             payload TEXT\
         ); \
         INSERT INTO parent (id) VALUES (7); \
         INSERT INTO child (id, parent_id, payload) VALUES (1, 7, 'kept')",
    ))?;
    let violations = connection.execute(&QueryRequest::new("PRAGMA main.foreign_key_check"))?;
    assert!(
        violations.rows.is_empty(),
        "fixture must start with no global foreign key violations"
    );
    let schema_before = connection.execute(&QueryRequest::new(
        "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'child'",
    ))?;

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("child"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "payload".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        })?;

    assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    assert_eq!(
        schema_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'child'",
            ))?
            .rows,
        "rebuild preparation must remain read-only"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_resolves_composite_implicit_parent_primary_keys_in_pk_order()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE parent (a INTEGER, b INTEGER, PRIMARY KEY (b, a)); \
         CREATE TABLE child (\
             id INTEGER PRIMARY KEY,\
             x INTEGER,\
             y INTEGER,\
             payload TEXT,\
             FOREIGN KEY (x, y) REFERENCES parent\
         ); \
         INSERT INTO parent (a, b) VALUES (10, 20); \
         INSERT INTO child (id, x, y, payload) VALUES (1, 20, 10, 'kept')",
    ))?;
    let violations = connection.execute(&QueryRequest::new("PRAGMA main.foreign_key_check"))?;
    assert!(
        violations.rows.is_empty(),
        "fixture must prove implicit composite parent lookup uses primary-key ordinal order"
    );

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("child"),
            operations: vec![TableAlterOperation::AlterColumn {
                name: "payload".to_string(),
                new_type: Some("VARCHAR(9)".to_string()),
                nullable: None,
                default: None,
            }],
            expected_before: Vec::new(),
        })?;

    assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    Ok(())
}

#[test]
fn sqlite_rebuild_preserves_mixed_foreign_key_declarations_and_related_table_metadata()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, payload TEXT); \
         CREATE TABLE mixed (\
             id INTEGER PRIMARY KEY,\
             inline_parent INTEGER REFERENCES parent ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,\
             table_parent INTEGER,\
             duplicate_parent INTEGER,\
             payload TEXT,\
             FOREIGN KEY (table_parent) REFERENCES parent ON DELETE SET NULL,\
             FOREIGN KEY (duplicate_parent) REFERENCES parent\
         ); \
         CREATE TABLE node (\
             id INTEGER PRIMARY KEY,\
             parent_id INTEGER REFERENCES node,\
             payload TEXT\
         ); \
         CREATE TABLE inbound (\
             id INTEGER PRIMARY KEY,\
             parent_id INTEGER REFERENCES parent\
         ) WITHOUT ROWID",
    ))?;
    let violations = connection.execute(&QueryRequest::new("PRAGMA main.foreign_key_check"))?;
    assert!(
        violations.rows.is_empty(),
        "mixed, self, and inbound fixtures must start globally foreign-key clean"
    );

    for table in ["mixed", "node", "parent"] {
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new(table),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "payload".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
        if table == "mixed" {
            assert!(plan.preview().statements.iter().any(|statement| {
                statement.contains("DEFERRABLE INITIALLY DEFERRED")
                    && statement.contains("ON DELETE CASCADE")
            }));
        }
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_executes_preserving_foreign_key_timing_forms() -> Result<(), DbError> {
    for (label, timing) in [
        ("omitted timing", ""),
        ("DEFERRABLE", " DEFERRABLE"),
        (
            "DEFERRABLE INITIALLY IMMEDIATE",
            " DEFERRABLE INITIALLY IMMEDIATE",
        ),
        (
            "DEFERRABLE INITIALLY DEFERRED",
            " DEFERRABLE INITIALLY DEFERRED",
        ),
        ("NOT DEFERRABLE", " NOT DEFERRABLE"),
        (
            "NOT DEFERRABLE INITIALLY IMMEDIATE",
            " NOT DEFERRABLE INITIALLY IMMEDIATE",
        ),
        (
            "NOT DEFERRABLE INITIALLY DEFERRED",
            " NOT DEFERRABLE INITIALLY DEFERRED",
        ),
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(format!(
            "PRAGMA foreign_keys = ON; \
             CREATE TABLE parent (id INTEGER PRIMARY KEY); \
             CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, payload TEXT, \
                FOREIGN KEY (parent_id) REFERENCES parent(id) {timing}); \
             INSERT INTO parent VALUES (7); \
             INSERT INTO child VALUES (1, 7, 'kept')"
        )))?;
        let violations = connection.execute(&QueryRequest::new("PRAGMA main.foreign_key_check"))?;
        assert!(
            violations.rows.is_empty(),
            "{label} fixture must start with valid foreign key data"
        );
        let schema_before = connection.execute(&QueryRequest::new(
            "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'child'",
        ))?;
        let before = rebuild_boundary_snapshot(&*connection, "child")?;
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("child"),
                operations: vec![TableAlterOperation::AlterColumn {
                    name: "payload".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: None,
                    default: None,
                }],
                expected_before: Vec::new(),
            })?;
        assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
        assert!(
            plan.preview().statements.iter().any(|statement| {
                statement.starts_with("CREATE TABLE")
                    && statement.contains("FOREIGN KEY (parent_id) REFERENCES parent(id)")
                    && if timing.is_empty() {
                        !statement.contains("DEFERRABLE")
                    } else {
                        statement.contains(timing)
                    }
            }),
            "rebuild preview must preserve {label} exactly"
        );
        assert_eq!(
            schema_before.rows,
            connection
                .execute(&QueryRequest::new(
                    "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'child'",
                ))?
                .rows,
            "preparation must remain read-only for {label}"
        );
        assert_eq!(
            before,
            rebuild_boundary_snapshot(&*connection, "child")?,
            "preparation must retain the complete {label} boundary"
        );

        plan.execute()?;

        let schema = connection.execute(&QueryRequest::new(
            "SELECT sql FROM main.sqlite_master WHERE type = 'table' AND name = 'child'",
        ))?;
        let expected_schema = format!(
            "CREATE TABLE \"child\" (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, \
             payload VARCHAR(9), FOREIGN KEY (parent_id) REFERENCES parent(id) {timing})"
        );
        assert_eq!(
            schema.rows,
            vec![vec![Value::Text(expected_schema)]],
            "execution must produce the exact requested schema for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT group_concat(metadata, ';') FROM ( \
                     SELECT printf('%d|%s|%s|%d|%s|%d|%d', cid, name, type, \"notnull\", \
                     COALESCE(dflt_value, '<NULL>'), pk, hidden) AS metadata \
                     FROM pragma_table_xinfo('child') ORDER BY cid)",
                ))?
                .rows,
            vec![vec![Value::Text(
                "0|id|INTEGER|0|<NULL>|1|0;1|parent_id|INTEGER|1|<NULL>|0|0;2|payload|VARCHAR(9)|0|<NULL>|0|0".to_string(),
            )]],
            "execution must retain exact column metadata and the requested payload type for {label}"
        );
        assert!(
            schema.rows[0][0]
                .to_string()
                .contains("FOREIGN KEY (parent_id) REFERENCES parent(id)"),
            "execution must preserve the foreign-key relationship for {label}"
        );
        if timing.is_empty() {
            assert!(
                !schema.rows[0][0].to_string().contains("DEFERRABLE"),
                "execution must preserve omitted timing for {label}"
            );
        } else {
            assert!(
                schema.rows[0][0].to_string().contains(timing),
                "execution must preserve {label}"
            );
        }
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT id, parent_id, payload FROM main.child ORDER BY id",
                ))?
                .rows,
            vec![vec![
                Value::Int(1),
                Value::Int(7),
                Value::Text("kept".to_string()),
            ]],
            "execution must preserve rows for {label}"
        );
        assert_eq!(
            before.foreign_keys,
            connection
                .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
                .rows,
            "execution must restore the foreign-key setting for {label}"
        );
        assert!(
            connection
                .execute(&QueryRequest::new("PRAGMA main.foreign_key_check"))?
                .rows
                .is_empty(),
            "execution must remain foreign-key clean for {label}"
        );
    }
    Ok(())
}

#[test]
fn sqlite_rebuild_preview_declares_ordered_lifecycle_and_exact_streamed_comparison_intent()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, p TEXT, a TEXT, b TEXT, retained TEXT, UNIQUE (id)); \
         CREATE INDEX t_retained_desc ON t(retained DESC); \
         INSERT INTO t VALUES (1, 'payload', 'drop-a', 'drop-b', 'keep')",
    ))?;
    let before = rebuild_boundary_snapshot(&*connection, "t")?;
    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("t"),
            operations: vec![
                TableAlterOperation::AlterColumn {
                    name: "p".to_string(),
                    new_type: Some("VARCHAR(9)".to_string()),
                    nullable: Some(false),
                    default: Some(OwnedDefaultSpec::Set("NULL".to_string())),
                },
                TableAlterOperation::DropColumn {
                    name: "a".to_string(),
                },
                TableAlterOperation::DropColumn {
                    name: "b".to_string(),
                },
            ],
            expected_before: Vec::new(),
        })?;
    let statements = &plan.preview().statements;
    let position = |needle: &str| {
        statements
            .iter()
            .position(|statement| statement.contains(needle))
            .unwrap_or_else(|| panic!("preview must declare {needle}"))
    };
    let context = position("verify usable connection, autocommit, safe settings, source freshness");
    let save_foreign_keys = position("save foreign_keys; set foreign_keys = OFF");
    let begin = position("BEGIN IMMEDIATE;");
    let under_lock =
        position("under lock revalidate freshness and baseline foreign-key cleanliness");
    let create = statements
        .iter()
        .position(|statement| statement.starts_with("CREATE TABLE main.\"__dbflux_rebuild_t\""))
        .expect("preview must create the private replacement table");
    let copy = statements
        .iter()
        .position(|statement| statement.starts_with("INSERT INTO main.\"__dbflux_rebuild_t\""))
        .expect("preview must declare the explicit copy projection");
    let comparison =
        position("exact streamed comparison of ordered source/replacement projections");
    let finalization = position("statement finalization completes before destructive DROP");
    let drop_original = position("DROP TABLE main.\"t\";");
    let rename = position("ALTER TABLE main.\"__dbflux_rebuild_t\" RENAME TO \"t\";");
    let restore_index = position("restore exact index main.t_retained_desc");
    let final_checks = position("verify final schema, explicit indexes, non-target catalog");
    let commit = position("COMMIT;");
    let restore_foreign_keys = position("after transaction ends, restore prior foreign_keys");
    assert!(statements[context].contains("private replacement-name availability"));
    assert!(statements[save_foreign_keys].contains("foreign_keys readback"));
    assert!(statements[under_lock].contains("freshness"));
    assert!(statements[under_lock].contains("baseline foreign-key cleanliness"));
    assert_eq!(
        statements[copy],
        "INSERT INTO main.\"__dbflux_rebuild_t\" (\"id\", \"p\", \"retained\") SELECT \"id\", \"p\", \"retained\" FROM main.\"t\";"
    );
    for forbidden in ["CAST(", "COALESCE(", "OR IGNORE", "OR REPLACE"] {
        assert!(
            !statements[copy].contains(forbidden),
            "public copy projection must not use {forbidden}"
        );
    }
    assert!(statements[comparison].contains("ValueRef"));
    for policy in [
        "rowcount=true",
        "integer identity=true",
        "storage class=true",
        "exact integer=true",
        "REAL bits=true",
        "TEXT/BLOB bytes=true",
        "invalid UTF-8/NUL=true",
    ] {
        assert!(
            statements[comparison].contains(policy),
            "comparison must declare {policy}"
        );
    }
    assert!(statements[final_checks].contains("no private-name leak"));
    assert!(statements[final_checks].contains("foreign_key_check"));
    assert!(statements[final_checks].contains("one-row integrity_check result of ok"));
    assert!(
        context < save_foreign_keys
            && save_foreign_keys < begin
            && begin < under_lock
            && under_lock < create
            && create < copy
            && copy < comparison
            && comparison < finalization
            && finalization < drop_original
            && drop_original < rename
            && rename < restore_index
            && restore_index < final_checks
            && final_checks < commit
            && commit < restore_foreign_keys,
        "rebuild lifecycle intent must declare the complete ordered chain"
    );
    let failure_intent = statements
        .iter()
        .find(|statement| statement.starts_with("INTENT FAILURE:"))
        .expect("preview must state failure handling intent");
    for concept in [
        "rollback",
        "autocommit",
        "restore foreign_keys",
        "cleanup failure",
        "quarantine",
        "commit certainty",
    ] {
        assert!(
            failure_intent.contains(concept),
            "failure intent must declare {concept}"
        );
    }
    assert!(plan.preview().warnings.iter().any(|warning| {
        warning.contains("NOT EXECUTABLE") && warning.contains("illustrative lifecycle intent")
    }));
    assert!(plan.preview().warnings.iter().any(|warning| {
        warning.contains("Preparation remains read-only")
            && warning.contains("exact streamed ValueRef comparison")
    }));
    assert_eq!(
        before,
        rebuild_boundary_snapshot(&*connection, "t")?,
        "preparation must retain the complete boundary"
    );

    plan.execute()?;

    assert_eq!(
        connection
            .execute(&QueryRequest::new("SELECT id, p, retained FROM main.t"))?
            .rows,
        vec![vec![
            Value::Int(1),
            Value::Text("payload".to_string()),
            Value::Text("keep".to_string())
        ]],
        "execution must preserve the retained projection"
    );
    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT type, \"notnull\", dflt_value FROM pragma_table_xinfo('t') WHERE name = 'p'",
            ))?
            .rows,
        vec![vec![
            Value::Text("VARCHAR(9)".to_string()),
            Value::Int(1),
            Value::Text("NULL".to_string()),
        ]],
        "execution must apply the selected payload alteration"
    );
    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT COUNT(*) FROM pragma_table_xinfo('t') WHERE name IN ('a', 'b')",
            ))?
            .rows,
        vec![vec![Value::Int(0)]],
        "execution must apply only the selected drops"
    );
    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master WHERE type = 'index' AND name = 't_retained_desc'",
            ))?
            .rows,
        vec![vec![Value::Text(
            "CREATE INDEX t_retained_desc ON t(retained DESC)".to_string(),
        )]],
        "execution must retain the unaffected explicit index"
    );
    assert_eq!(
        before.foreign_keys,
        connection
            .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
            .rows,
        "execution must restore the foreign-key setting"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_prepare_and_execute_preserve_complete_boundaries() -> Result<(), DbError> {
    for (label, foreign_keys, insert) in [
        ("empty-fk-on", "ON", ""),
        (
            "populated-fk-on",
            "ON",
            "INSERT INTO t(id, legacy, a, b, retained) VALUES (-7, 'old', 'drop-a', 'drop-b', 'keep');",
        ),
        ("empty-fk-off", "OFF", ""),
        (
            "populated-fk-off",
            "OFF",
            "INSERT INTO t(id, legacy, a, b, retained) VALUES (-7, 'old', 'drop-a', 'drop-b', 'keep');",
        ),
    ] {
        let (connection, _, _db_path) = connect_sqlite()?;
        connection.execute(&QueryRequest::new(format!(
            "PRAGMA foreign_keys = {foreign_keys}; \
             CREATE TABLE t(id INTEGER PRIMARY KEY, legacy TEXT DEFAULT 'NULL', a TEXT, b TEXT, retained TEXT NOT NULL DEFAULT ('keep'), UNIQUE (id)); \
             CREATE TABLE main_rebuild_boundary(marker TEXT); \
             INSERT INTO main_rebuild_boundary VALUES ('main-sentinel'); \
             CREATE TEMP TABLE temp_rebuild_boundary(marker TEXT); \
             INSERT INTO temp_rebuild_boundary VALUES ('sentinel'); \
             {insert}"
        )))?;
        let before = rebuild_boundary_snapshot(&*connection, "t")?;
        let plan = connection
            .table_alter_planner()
            .expect("SQLite must opt into table alteration planning")
            .prepare(&TableAlterRequest {
                table: TableRef::new("t"),
                operations: vec![
                    TableAlterOperation::AlterColumn {
                        name: "legacy".to_string(),
                        new_type: Some("VARCHAR(32)".to_string()),
                        nullable: Some(false),
                        default: Some(OwnedDefaultSpec::Set("NULL".to_string())),
                    },
                    TableAlterOperation::DropColumn {
                        name: "a".to_string(),
                    },
                    TableAlterOperation::DropColumn {
                        name: "b".to_string(),
                    },
                ],
                expected_before: Vec::new(),
            })?;
        let create = plan
            .preview()
            .statements
            .iter()
            .find(|statement| statement.starts_with("CREATE TABLE"))
            .expect("rebuild lifecycle must include the rewritten CREATE TABLE");
        assert!(create.contains("legacy VARCHAR(32) NOT NULL DEFAULT NULL"));
        assert!(create.contains("retained TEXT NOT NULL DEFAULT ('keep')"));
        assert!(!create.contains(" a TEXT") && !create.contains(" b TEXT"));
        assert!(plan.preview().statements.iter().any(|statement| {
            statement.starts_with("INSERT INTO")
                && statement.contains("\"id\", \"legacy\", \"retained\"")
        }));
        assert_eq!(
            before,
            rebuild_boundary_snapshot(&*connection, "t")?,
            "prepare must retain the complete {label} boundary"
        );

        plan.execute()?;

        let after = rebuild_boundary_snapshot(&*connection, "t")?;
        let non_target_catalog = |catalog: &[Vec<Value>]| {
            catalog
                .iter()
                .filter(|entry| !matches!(entry.get(2), Some(Value::Text(table)) if table == "t"))
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(
            non_target_catalog(&before.main_catalog),
            non_target_catalog(&after.main_catalog),
            "execution must preserve the complete unaffected MAIN catalog for {label}"
        );
        assert_eq!(
            before.temp_catalog, after.temp_catalog,
            "execution must preserve the complete TEMP catalog for {label}"
        );
        assert_eq!(
            before.databases, after.databases,
            "execution must retain databases for {label}"
        );
        assert_eq!(
            before.deferred_foreign_keys, after.deferred_foreign_keys,
            "execution must retain deferred foreign-key settings for {label}"
        );
        assert_ne!(
            before.main_schema_version, after.main_schema_version,
            "the MAIN schema version must advance for the requested delta in {label}"
        );
        assert_eq!(
            before.temp_schema_version, after.temp_schema_version,
            "the TEMP schema version must not change for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT group_concat(metadata, ';') FROM ( \
                     SELECT printf('%d|%s|%s|%d|%s|%d|%d', cid, name, type, \"notnull\", \
                     COALESCE(dflt_value, '<NULL>'), pk, hidden) AS metadata \
                     FROM pragma_table_xinfo('t') ORDER BY cid)",
                ))?
                .rows,
            vec![vec![Value::Text(
                "0|id|INTEGER|0|<NULL>|1|0;1|legacy|VARCHAR(32)|1|NULL|0|0;2|retained|TEXT|1|'keep'|0|0".to_string(),
            )]],
            "execution must retain exact column definitions and apply only the requested target delta for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT name, \"unique\", origin, partial FROM pragma_index_list('t') ORDER BY name",
                ))?
                .rows,
            vec![vec![
                Value::Text("sqlite_autoindex_t_1".to_string()),
                Value::Int(1),
                Value::Text("u".to_string()),
                Value::Int(0),
            ]],
            "execution must retain UNIQUE index metadata for {label}"
        );

        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT type, \"notnull\", dflt_value FROM pragma_table_xinfo('t') WHERE name = 'legacy'",
                ))?
                .rows,
            vec![vec![
                Value::Text("VARCHAR(32)".to_string()),
                Value::Int(1),
                Value::Text("NULL".to_string()),
            ]],
            "execution must apply the selected alteration for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT COUNT(*) FROM pragma_table_xinfo('t') WHERE name IN ('a', 'b')",
                ))?
                .rows,
            vec![vec![Value::Int(0)]],
            "execution must apply selected drops for {label}"
        );
        let expected_rows = if insert.is_empty() {
            Vec::new()
        } else {
            vec![vec![
                Value::Int(-7),
                Value::Text("old".to_string()),
                Value::Text("keep".to_string()),
            ]]
        };
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT id, legacy, retained FROM main.t",
                ))?
                .rows,
            expected_rows,
            "execution must preserve retained rows for {label}"
        );
        assert_eq!(
            before.foreign_keys,
            connection
                .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
                .rows,
            "execution must restore the foreign-key setting for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT marker FROM main.main_rebuild_boundary",
                ))?
                .rows,
            vec![vec![Value::Text("main-sentinel".to_string())]],
            "execution must preserve the unaffected MAIN sentinel for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT marker FROM temp.temp_rebuild_boundary",
                ))?
                .rows,
            vec![vec![Value::Text("sentinel".to_string())]],
            "execution must preserve the TEMP sentinel for {label}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT COUNT(*) FROM main.sqlite_master WHERE name = '__dbflux_rebuild_t'",
                ))?
                .rows,
            vec![vec![Value::Int(0)]],
            "execution must not leak its private replacement table for {label}"
        );
    }
    Ok(())
}

#[test]
fn sqlite_native_drop_rolls_back_all_selected_columns_when_later_drop_is_rejected()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    for statement in [
        "CREATE TABLE people (id INTEGER PRIMARY KEY, first_drop TEXT, second_drop TEXT, retained TEXT)",
        "INSERT INTO people VALUES (1, 'first', 'second', 'kept')",
        "CREATE VIEW second_drop_view AS SELECT second_drop FROM people",
    ] {
        connection.execute(&QueryRequest::new(statement))?;
    }

    let schema_before = connection.execute(&QueryRequest::new(
        "SELECT type, name, sql FROM main.sqlite_master ORDER BY type, name",
    ))?;
    let data_before = connection.execute(&QueryRequest::new(
        "SELECT id, first_drop, second_drop, retained FROM main.people",
    ))?;
    let foreign_keys_before = connection.execute(&QueryRequest::new("PRAGMA foreign_keys"))?;

    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("people"),
            operations: vec![
                TableAlterOperation::DropColumn {
                    name: "first_drop".to_string(),
                },
                TableAlterOperation::DropColumn {
                    name: "second_drop".to_string(),
                },
            ],
            expected_before: Vec::new(),
        })?;

    let error = plan
        .execute()
        .expect_err("SQLite must reject the later view-dependent DROP at execution");
    assert!(
        error.to_string().contains("rolled back"),
        "native execution must report the rollback: {error}"
    );
    for column in ["first_drop", "second_drop", "retained"] {
        assert_eq!(
            connection
                .execute(&QueryRequest::new(format!(
                    "SELECT COUNT(*) FROM pragma_table_info('people') WHERE name = '{column}'"
                )))?
                .rows[0][0],
            Value::Int(1),
            "the failed later DROP must roll back every selected earlier column"
        );
    }
    assert_eq!(
        schema_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT type, name, sql FROM main.sqlite_master ORDER BY type, name",
            ))?
            .rows,
        "the failed native transaction must restore the complete main schema"
    );
    assert_eq!(
        data_before.rows,
        connection
            .execute(&QueryRequest::new(
                "SELECT id, first_drop, second_drop, retained FROM main.people",
            ))?
            .rows,
        "the failed native transaction must restore the complete table data"
    );
    assert_eq!(
        foreign_keys_before.rows,
        connection
            .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
            .rows,
        "native execution must leave connection settings unchanged on rollback"
    );
    Ok(())
}

#[test]
fn sqlite_rebuild_executes_payload_alter_and_independent_drop_preserving_data_index_and_fk_state()
-> Result<(), DbError> {
    let (connection, _, _db_path) = connect_sqlite()?;
    connection.execute(&QueryRequest::new(
        "PRAGMA foreign_keys = ON;
         CREATE TABLE target (
            id INTEGER PRIMARY KEY,
            payload TEXT NOT NULL,
            retained TEXT NOT NULL,
            obsolete TEXT
         );
         CREATE INDEX target_retained_index ON target(retained);
         INSERT INTO target (id, payload, retained, obsolete) VALUES
            (-7, 'first payload', 'first retained', 'remove first'),
            (42, 'second payload', 'second retained', 'remove second')",
    ))?;

    let before = rebuild_boundary_snapshot(&*connection, "target")?;
    let plan = connection
        .table_alter_planner()
        .expect("SQLite must opt into table alteration planning")
        .prepare(&TableAlterRequest {
            table: TableRef::new("target"),
            operations: vec![
                TableAlterOperation::AlterColumn {
                    name: "payload".to_string(),
                    new_type: Some("VARCHAR(64)".to_string()),
                    nullable: Some(false),
                    default: Some(OwnedDefaultSpec::Set("'future'".to_string())),
                },
                TableAlterOperation::DropColumn {
                    name: "obsolete".to_string(),
                },
            ],
            expected_before: Vec::new(),
        })?;

    assert_eq!(plan.preview().route, TableAlterRoute::Rebuild);
    assert_eq!(
        before,
        rebuild_boundary_snapshot(&*connection, "target")?,
        "prepare must preserve the complete populated foreign-key-on boundary"
    );

    plan.execute()?;

    assert_eq!(
        connection
            .execute(&QueryRequest::new("PRAGMA main.table_xinfo('target')"))?
            .rows,
        vec![
            vec![
                Value::Int(0),
                Value::Text("id".to_string()),
                Value::Text("INTEGER".to_string()),
                Value::Int(0),
                Value::Null,
                Value::Int(1),
                Value::Int(0),
            ],
            vec![
                Value::Int(1),
                Value::Text("payload".to_string()),
                Value::Text("VARCHAR(64)".to_string()),
                Value::Int(1),
                Value::Text("'future'".to_string()),
                Value::Int(0),
                Value::Int(0),
            ],
            vec![
                Value::Int(2),
                Value::Text("retained".to_string()),
                Value::Text("TEXT".to_string()),
                Value::Int(1),
                Value::Null,
                Value::Int(0),
                Value::Int(0),
            ],
        ],
        "rebuild must produce the exact final target schema"
    );
    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT sql FROM main.sqlite_master \
                 WHERE type = 'index' AND name = 'target_retained_index'",
            ))?
            .rows,
        vec![vec![Value::Text(
            "CREATE INDEX target_retained_index ON target(retained)".to_string()
        )]],
        "rebuild must restore the unaffected explicit index SQL"
    );

    connection.execute(&QueryRequest::new(
        "INSERT INTO target (id, retained) VALUES (100, 'future retained')",
    ))?;
    assert_eq!(
        connection
            .execute(&QueryRequest::new(
                "SELECT id, payload, retained FROM target ORDER BY id",
            ))?
            .rows,
        vec![
            vec![
                Value::Int(-7),
                Value::Text("first payload".to_string()),
                Value::Text("first retained".to_string()),
            ],
            vec![
                Value::Int(42),
                Value::Text("second payload".to_string()),
                Value::Text("second retained".to_string()),
            ],
            vec![
                Value::Int(100),
                Value::Text("future".to_string()),
                Value::Text("future retained".to_string()),
            ],
        ],
        "rebuild must preserve retained identities and values while applying the default only to future inserts"
    );
    assert_eq!(
        before.foreign_keys,
        connection
            .execute(&QueryRequest::new("PRAGMA foreign_keys"))?
            .rows,
        "rebuild must restore the prior foreign_keys setting"
    );
    assert!(
        connection
            .execute(&QueryRequest::new("PRAGMA main.foreign_key_check"))?
            .rows
            .is_empty(),
        "rebuild must leave foreign-key clean"
    );
    assert_eq!(
        connection
            .execute(&QueryRequest::new("PRAGMA main.integrity_check"))?
            .rows,
        vec![vec![Value::Text("ok".to_string())]],
        "rebuild must leave integrity_check clean"
    );
    Ok(())
}
