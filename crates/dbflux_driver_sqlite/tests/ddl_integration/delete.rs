use super::*;

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
