//! Migration 040: Add font family and size columns to `cfg_general_settings`.
//!
//! Persists the interface, editor and data grid fonts. A NULL family selects
//! the bundled font (the grid falls back to the editor family). Sizes default
//! to the sizes a new install renders with.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

const FONT_COLUMNS: [(&str, &str); 6] = [
    ("ui_font_family", "TEXT"),
    ("ui_font_size", "REAL NOT NULL DEFAULT 13.0"),
    ("editor_font_family", "TEXT"),
    ("editor_font_size", "REAL NOT NULL DEFAULT 13.0"),
    ("grid_font_family", "TEXT"),
    ("grid_font_size", "REAL NOT NULL DEFAULT 12.5"),
];

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "040_general_settings_fonts"
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

        for (column, definition) in FONT_COLUMNS {
            let column_exists: bool = tx
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('cfg_general_settings') WHERE name = ?1",
                    [column],
                    |row| row.get::<_, i64>(0),
                )
                .map(|n| n > 0)?;

            if !column_exists {
                tx.execute_batch(&format!(
                    "ALTER TABLE cfg_general_settings ADD COLUMN {column} {definition};"
                ))?;
            }
        }

        Ok(())
    }
}
