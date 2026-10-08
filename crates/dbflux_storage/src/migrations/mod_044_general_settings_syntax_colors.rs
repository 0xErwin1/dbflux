//! Migration 044: Add `syntax_colors_json` to `cfg_general_settings`.
//!
//! Persists the syntax colors the user picked in place of the palette's, as
//! JSON keyed by palette variant and syntax role. The column defaults to an
//! empty string, which keeps every palette color.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub(crate) struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "044_general_settings_syntax_colors"
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

        let column_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('cfg_general_settings') WHERE name = 'syntax_colors_json'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)?;

        if !column_exists {
            tx.execute_batch(
                "ALTER TABLE cfg_general_settings ADD COLUMN syntax_colors_json TEXT NOT NULL DEFAULT '';",
            )?;
        }

        Ok(())
    }
}
