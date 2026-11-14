use super::*;

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
