#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err,
    clippy::unwrap_in_result
)]

use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, IndexData, QueryRequest, TableAlterOperation,
    TableAlterRequest, TableAlterRoute, TableRef, Value,
};
use dbflux_driver_sqlite::SqliteDriver;
use dbflux_test_support::ddl_fixtures::SqliteFixtures;
use std::path::PathBuf;

fn connect_sqlite() -> Result<(Box<dyn dbflux_core::Connection>, SqliteDriver, PathBuf), DbError> {
    let driver = SqliteDriver::new();
    let temp_dir = std::env::temp_dir();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path = temp_dir.join(format!("test_ddl_{}.db", timestamp));

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
