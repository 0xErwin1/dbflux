//! Migration 051: Add `accent_dark` and `accent_light` columns to `cfg_general_settings`.
//!
//! Each holds the `#RRGGBB` accent the user picked for that palette. Both are
//! nullable: `NULL` keeps the palette's own accent, so existing installations
//! look exactly as before.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub(crate) struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "051_general_settings_accent_color"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let table_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cfg_general_settings'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count > 0)
            .map_err(sqlite_err)?;

        if !table_exists {
            return Ok(());
        }

        for column in ["accent_dark", "accent_light"] {
            let column_exists: bool = tx
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('cfg_general_settings') WHERE name = ?1",
                    [column],
                    |row| row.get::<_, i64>(0),
                )
                .map(|count| count > 0)
                .map_err(sqlite_err)?;

            if !column_exists {
                tx.execute_batch(&format!(
                    "ALTER TABLE cfg_general_settings ADD COLUMN {column} TEXT;"
                ))
                .map_err(sqlite_err)?;
            }
        }

        Ok(())
    }
}

fn sqlite_err(source: rusqlite::Error) -> MigrationError {
    MigrationError::Sqlite {
        path: std::path::PathBuf::from("<unknown>"),
        source,
    }
}
