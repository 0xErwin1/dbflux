use super::*;

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
