use dbflux_storage::migrations::MigrationRegistry;
use rusqlite::{Connection, Result as SqlResult};

#[test]
#[expect(
    clippy::unwrap_in_result,
    reason = "fixture setup uses expect() so a failure names the setup step that broke, not just a propagated error"
)]
fn grid_fk_labels_migration_defaults_to_off_and_persists_setting() -> SqlResult<()> {
    let connection = Connection::open_in_memory()?;
    MigrationRegistry::new()
        .run_all(&connection)
        .expect("existing migration chain must apply");

    let default_value: i64 = connection.query_row(
        "SELECT grid_fk_labels FROM cfg_general_settings WHERE id = 1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(default_value, 0, "grid_fk_labels must default to off");

    connection.execute(
        "UPDATE cfg_general_settings SET grid_fk_labels = ?1 WHERE id = 1",
        [1],
    )?;
    let persisted_value: i64 = connection.query_row(
        "SELECT grid_fk_labels FROM cfg_general_settings WHERE id = 1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(persisted_value, 1);

    Ok(())
}

#[test]
#[expect(
    clippy::unwrap_in_result,
    reason = "fixture setup uses expect() so a failure names the setup step that broke, not just a propagated error"
)]
fn grid_fk_labels_migration_upgrades_pre_column_schema_preserving_settings() -> SqlResult<()> {
    let connection = Connection::open_in_memory()?;
    let registry = MigrationRegistry::new();
    registry
        .run_all(&connection)
        .expect("migration chain must establish the fixture");

    connection
        .execute_batch("ALTER TABLE cfg_general_settings DROP COLUMN grid_fk_labels;")
        .expect("SQLite must support dropping the added column for the pre-045 fixture");
    connection.execute(
        "DELETE FROM sys_migrations WHERE name = '045_general_settings_grid_fk_labels'",
        [],
    )?;
    connection.execute(
        "UPDATE cfg_general_settings SET theme = 'light', max_history_entries = 321 WHERE id = 1",
        [],
    )?;

    registry
        .run_all(&connection)
        .expect("migration 045 must upgrade populated settings");
    let (theme, history_entries, grid_fk_labels): (String, i64, i64) = connection.query_row(
        "SELECT theme, max_history_entries, grid_fk_labels FROM cfg_general_settings WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(theme, "light");
    assert_eq!(history_entries, 321);
    assert_eq!(grid_fk_labels, 0);

    registry
        .run_all(&connection)
        .expect("rerunning the registry must be idempotent");
    let applied: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sys_migrations WHERE name = '045_general_settings_grid_fk_labels'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(applied, 1, "migration 045 must remain recorded once");

    Ok(())
}
