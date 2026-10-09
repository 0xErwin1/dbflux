//! Migration 045: Add `accent_color_dark` and `accent_color_light` to
//! `cfg_general_settings`.
//!
//! Persists the accent color the user picked in place of the palette's, as
//! `#RRGGBB` text per palette variant. `NULL` keeps the palette's accent.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub(crate) struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "045_general_settings_accent_colors"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let table_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cfg_general_settings'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)?;

        if !table_exists {
            return Ok(());
        }

        for column in ["accent_color_dark", "accent_color_light"] {
            let column_exists: bool = tx
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('cfg_general_settings') WHERE name = ?1",
                    [column],
                    |row| row.get::<_, i64>(0),
                )
                .map(|n| n > 0)?;

            if !column_exists {
                tx.execute_batch(&format!(
                    "ALTER TABLE cfg_general_settings ADD COLUMN {column} TEXT;"
                ))?;
            }
        }

        Ok(())
    }
}
