use super::*;

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

    drop(connection);
    std::fs::remove_file(&db_path).map_err(|error| {
        DbError::query_failed(format!("failed to remove the test database: {error}"))
    })?;

    Ok(())
}

#[test]
fn sqlite_native_drop_rejects_unsafe_connection_settings_without_mutation() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
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

    drop(connection);
    std::fs::remove_file(&db_path).map_err(|error| {
        DbError::query_failed(format!("failed to remove the test database: {error}"))
    })?;

    Ok(())
}

#[test]
fn sqlite_native_drop_preflights_index_and_foreign_key_dependencies() -> Result<(), DbError> {
    let (connection, _, db_path) = connect_sqlite()?;
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

    drop(connection);
    std::fs::remove_file(&db_path).map_err(|error| {
        DbError::query_failed(format!("failed to remove the test database: {error}"))
    })?;

    Ok(())
}
