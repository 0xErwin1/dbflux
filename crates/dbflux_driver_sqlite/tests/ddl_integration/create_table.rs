use super::*;

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
