use super::*;

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
