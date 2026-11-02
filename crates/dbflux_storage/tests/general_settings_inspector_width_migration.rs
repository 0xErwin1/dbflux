use dbflux_storage::migrations::MigrationRegistry;
use rusqlite::{Connection, Result as SqlResult};

fn read_inspector_width(connection: &Connection) -> SqlResult<Option<f64>> {
    connection.query_row(
        "SELECT workspace_inspector_width_px FROM cfg_general_settings WHERE id = 1",
        [],
        |row| row.get(0),
    )
}

#[test]
#[expect(
    clippy::unwrap_in_result,
    reason = "fixture setup uses expect() so a failure names the setup step that broke, not just a propagated error"
)]
fn inspector_width_migration_adds_a_nullable_column() -> SqlResult<()> {
    let connection = Connection::open_in_memory()?;
    MigrationRegistry::new()
        .run_all(&connection)
        .expect("migration chain must apply");

    assert_eq!(read_inspector_width(&connection)?, None);

    connection.execute(
        "UPDATE cfg_general_settings SET workspace_inspector_width_px = 420.5 WHERE id = 1",
        [],
    )?;
    assert_eq!(read_inspector_width(&connection)?, Some(420.5));

    connection.execute(
        "UPDATE cfg_general_settings SET workspace_inspector_width_px = NULL WHERE id = 1",
        [],
    )?;
    assert_eq!(read_inspector_width(&connection)?, None);

    Ok(())
}

#[test]
#[expect(
    clippy::unwrap_in_result,
    reason = "fixture setup uses expect() so a failure names the setup step that broke, not just a propagated error"
)]
fn inspector_width_migration_upgrades_populated_settings() -> SqlResult<()> {
    let connection = Connection::open_in_memory()?;
    let registry = MigrationRegistry::new();
    registry
        .run_all(&connection)
        .expect("migration chain must establish the fixture");

    connection
        .execute_batch("ALTER TABLE cfg_general_settings DROP COLUMN workspace_inspector_width_px;")
        .expect("SQLite must support dropping the added column for the pre-049 fixture");
    connection.execute(
        "DELETE FROM sys_migrations WHERE name = '049_general_settings_inspector_width'",
        [],
    )?;
    connection.execute(
        "UPDATE cfg_general_settings SET theme = 'light', ui_font_size = 15.0 WHERE id = 1",
        [],
    )?;

    registry
        .run_all(&connection)
        .expect("migration 049 must upgrade populated settings");

    let (theme, ui_font_size): (String, f64) = connection.query_row(
        "SELECT theme, ui_font_size FROM cfg_general_settings WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(theme, "light");
    assert_eq!(ui_font_size, 15.0);
    assert_eq!(
        read_inspector_width(&connection)?,
        None,
        "an upgraded install keeps the default width"
    );

    registry
        .run_all(&connection)
        .expect("rerunning the registry must be idempotent");
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sys_migrations WHERE name = '049_general_settings_inspector_width'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(count, 1, "049 must remain recorded once");

    Ok(())
}
