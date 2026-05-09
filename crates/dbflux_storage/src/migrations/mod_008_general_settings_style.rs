//! Migration 008: Add `style` column to `cfg_general_settings`.
//!
//! Adds `style TEXT NOT NULL DEFAULT 'default'` so the app can persist
//! the selected layout style (Default vs Compact) across restarts.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

/// Adds the `style` column to `cfg_general_settings`.
pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "008_general_settings_style"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        // SQLite does not support IF NOT EXISTS on ALTER TABLE, so we check
        // whether the column already exists before attempting to add it.
        let column_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('cfg_general_settings') WHERE name = 'style'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;

        if !column_exists {
            tx.execute_batch(
                "ALTER TABLE cfg_general_settings ADD COLUMN style TEXT NOT NULL DEFAULT 'default';",
            )
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;
        }

        Ok(())
    }
}
