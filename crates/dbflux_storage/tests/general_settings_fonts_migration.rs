use dbflux_storage::migrations::MigrationRegistry;
use rusqlite::{Connection, Result as SqlResult};

type FontRow = (
    Option<String>,
    f64,
    Option<String>,
    f64,
    Option<String>,
    f64,
);

fn read_fonts(connection: &Connection) -> SqlResult<FontRow> {
    connection.query_row(
        "SELECT ui_font_family, ui_font_size, editor_font_family, editor_font_size,
                grid_font_family, grid_font_size
         FROM cfg_general_settings WHERE id = 1",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )
}

#[test]
fn fonts_migration_adds_defaults_and_persists_settings() -> SqlResult<()> {
    let connection = Connection::open_in_memory()?;
    MigrationRegistry::new()
        .run_all(&connection)
        .expect("migration chain must apply");

    assert_eq!(
        read_fonts(&connection)?,
        (None, 13.0, None, 13.0, None, 12.5)
    );

    connection.execute(
        "UPDATE cfg_general_settings
         SET ui_font_family = 'Inter', ui_font_size = 15.0,
             editor_font_family = 'Fira Code', editor_font_size = 17.5,
             grid_font_family = 'Iosevka', grid_font_size = 14.0
         WHERE id = 1",
        [],
    )?;
    assert_eq!(
        read_fonts(&connection)?,
        (
            Some("Inter".to_string()),
            15.0,
            Some("Fira Code".to_string()),
            17.5,
            Some("Iosevka".to_string()),
            14.0,
        )
    );

    let null_size = connection.execute(
        "UPDATE cfg_general_settings SET grid_font_size = NULL WHERE id = 1",
        [],
    );
    assert!(null_size.is_err(), "font sizes must be NOT NULL");

    Ok(())
}

#[test]
fn fonts_migration_upgrades_populated_pre_040_settings() -> SqlResult<()> {
    let connection = Connection::open_in_memory()?;
    let registry = MigrationRegistry::new();
    registry
        .run_all(&connection)
        .expect("migration chain must establish the fixture");

    connection
        .execute_batch(
            "ALTER TABLE cfg_general_settings DROP COLUMN ui_font_family;
             ALTER TABLE cfg_general_settings DROP COLUMN ui_font_size;
             ALTER TABLE cfg_general_settings DROP COLUMN editor_font_family;
             ALTER TABLE cfg_general_settings DROP COLUMN editor_font_size;
             ALTER TABLE cfg_general_settings DROP COLUMN grid_font_family;
             ALTER TABLE cfg_general_settings DROP COLUMN grid_font_size;",
        )
        .expect("SQLite must support dropping the added columns for the pre-040 fixture");
    connection.execute(
        "DELETE FROM sys_migrations WHERE name = '040_general_settings_fonts'",
        [],
    )?;
    connection.execute(
        "UPDATE cfg_general_settings SET theme = 'light', vim_leader = ',' WHERE id = 1",
        [],
    )?;

    registry
        .run_all(&connection)
        .expect("migration 040 must upgrade populated settings");

    let (theme, vim_leader): (String, String) = connection.query_row(
        "SELECT theme, vim_leader FROM cfg_general_settings WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(theme, "light");
    assert_eq!(vim_leader, ",");
    assert_eq!(
        read_fonts(&connection)?,
        (None, 13.0, None, 13.0, None, 12.5)
    );

    registry
        .run_all(&connection)
        .expect("rerunning the registry must be idempotent");
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sys_migrations WHERE name = '040_general_settings_fonts'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(count, 1, "040 must remain recorded once");

    Ok(())
}
