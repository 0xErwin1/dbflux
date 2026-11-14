use dbflux_storage::migrations::MigrationRegistry;
use rusqlite::{Connection, Result as SqlResult};

fn read_scrollbar_visibility(connection: &Connection) -> SqlResult<String> {
    connection.query_row(
        "SELECT scrollbar_visibility FROM cfg_general_settings WHERE id = 1",
        [],
        |row| row.get(0),
    )
}

#[test]
#[expect(
    clippy::unwrap_in_result,
    reason = "fixture setup uses expect() so a failure names the setup step that broke, not just a propagated error"
)]
fn scrollbar_visibility_migration_adds_default_and_persists_setting() -> SqlResult<()> {
    let connection = Connection::open_in_memory()?;
    MigrationRegistry::new()
        .run_all(&connection)
        .expect("migration chain must apply");

    assert_eq!(read_scrollbar_visibility(&connection)?, "while_scrolling");

    connection.execute(
        "UPDATE cfg_general_settings SET scrollbar_visibility = 'always' WHERE id = 1",
        [],
    )?;
    assert_eq!(read_scrollbar_visibility(&connection)?, "always");

    let null_value = connection.execute(
        "UPDATE cfg_general_settings SET scrollbar_visibility = NULL WHERE id = 1",
        [],
    );
    assert!(null_value.is_err(), "scrollbar_visibility must be NOT NULL");

    Ok(())
}

#[test]
#[expect(
    clippy::unwrap_in_result,
    reason = "fixture setup uses expect() so a failure names the setup step that broke, not just a propagated error"
)]
fn scrollbar_visibility_migration_upgrades_populated_settings() -> SqlResult<()> {
    let connection = Connection::open_in_memory()?;
    let registry = MigrationRegistry::new();
    registry
        .run_all(&connection)
        .expect("migration chain must establish the fixture");

    connection
        .execute_batch("ALTER TABLE cfg_general_settings DROP COLUMN scrollbar_visibility;")
        .expect("SQLite must support dropping the added column for the pre-048 fixture");
    connection.execute(
        "DELETE FROM sys_migrations WHERE name = '048_general_settings_scrollbar_visibility'",
        [],
    )?;
    connection.execute(
        "UPDATE cfg_general_settings SET theme = 'light', ui_font_size = 15.0 WHERE id = 1",
        [],
    )?;

    registry
        .run_all(&connection)
        .expect("migration 048 must upgrade populated settings");

    let (theme, ui_font_size): (String, f64) = connection.query_row(
        "SELECT theme, ui_font_size FROM cfg_general_settings WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(theme, "light");
    assert_eq!(ui_font_size, 15.0);
    assert_eq!(read_scrollbar_visibility(&connection)?, "while_scrolling");

    registry
        .run_all(&connection)
        .expect("rerunning the registry must be idempotent");
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sys_migrations WHERE name = '048_general_settings_scrollbar_visibility'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(count, 1, "048 must remain recorded once");

    Ok(())
}
