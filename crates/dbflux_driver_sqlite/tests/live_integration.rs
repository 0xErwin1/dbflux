use dbflux_core::{ConnectionProfile, DbConfig, DbDriver, QueryRequest};
use dbflux_driver_sqlite::SqliteDriver;

#[test]
fn sqlite_file_connect_ping_query_and_schema() -> Result<(), dbflux_core::DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("driver-live.sqlite");

    let driver = SqliteDriver::new();
    let profile = ConnectionProfile::new("live-sqlite", DbConfig::SQLite { path: db_path });

    let connection = driver.connect(&profile)?;
    connection.ping()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    ))?;
    connection.execute(&QueryRequest::new(
        "INSERT INTO users (name) VALUES ('alice')",
    ))?;

    let result = connection.execute(&QueryRequest::new("SELECT id, name FROM users"))?;
    assert_eq!(result.rows.len(), 1);

    let schema = connection.schema()?;
    let _ = schema.databases();

    Ok(())
}
