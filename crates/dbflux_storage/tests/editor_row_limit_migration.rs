use dbflux_storage::migrations::MigrationRegistry;
use rusqlite::{Connection, Result as SqlResult};

#[test]
fn editor_row_limit_migration_preserves_snapshot_and_persists_setting() -> SqlResult<()> {
    let connection = Connection::open_in_memory()?;
    MigrationRegistry::new()
        .run_all(&connection)
        .expect("existing migration chain must apply");

    let snapshot_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sys_migrations WHERE name = '029_sch_snapshot_creation_metadata'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(snapshot_count, 1, "migration 029 must remain registered");

    let default_limit: i64 = connection.query_row(
        "SELECT editor_row_limit FROM cfg_general_settings WHERE id = 1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(default_limit, 10_000);

    connection.execute(
        "UPDATE cfg_general_settings SET editor_row_limit = ?1 WHERE id = 1",
        [5_000],
    )?;
    let persisted_limit: i64 = connection.query_row(
        "SELECT editor_row_limit FROM cfg_general_settings WHERE id = 1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(persisted_limit, 5_000);

    Ok(())
}

#[test]
fn editor_row_limit_migration_upgrades_populated_settings_and_rejects_zero() -> SqlResult<()> {
    let connection = Connection::open_in_memory()?;
    let registry = MigrationRegistry::new();
    registry
        .run_all(&connection)
        .expect("migration chain must establish the fixture");

    connection
        .execute_batch("ALTER TABLE cfg_general_settings DROP COLUMN editor_row_limit;")
        .expect("SQLite must support dropping the added column for the pre-030 fixture");
    connection.execute(
        "DELETE FROM sys_migrations WHERE name = '030_general_settings_editor_row_limit'",
        [],
    )?;
    connection.execute(
        "UPDATE cfg_general_settings SET theme = 'light', max_history_entries = 321 WHERE id = 1",
        [],
    )?;

    registry
        .run_all(&connection)
        .expect("migration 030 must upgrade populated settings");
    let (theme, history_entries, limit): (String, i64, i64) = connection.query_row(
        "SELECT theme, max_history_entries, editor_row_limit FROM cfg_general_settings WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(theme, "light");
    assert_eq!(history_entries, 321);
    assert_eq!(limit, 10_000);

    let zero_result = connection.execute(
        "UPDATE cfg_general_settings SET editor_row_limit = 0 WHERE id = 1",
        [],
    );
    assert!(zero_result.is_err(), "SQLite CHECK must reject zero");

    registry
        .run_all(&connection)
        .expect("rerunning the registry must be idempotent");
    for migration in [
        "029_sch_snapshot_creation_metadata",
        "030_general_settings_editor_row_limit",
    ] {
        let count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM sys_migrations WHERE name = ?1",
            [migration],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1, "{migration} must remain recorded once");
    }
    let limit_after_rerun: i64 = connection.query_row(
        "SELECT editor_row_limit FROM cfg_general_settings WHERE id = 1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(limit_after_rerun, 10_000);

    Ok(())
}
