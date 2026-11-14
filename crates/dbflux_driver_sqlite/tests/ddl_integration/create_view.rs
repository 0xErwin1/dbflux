use super::*;

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
