//! Migration 046: Add `grid_fk_labels` column to `cfg_general_settings`.
//!
//! Persists the per-user opt-in for resolving foreign-key cells to the
//! referenced row's label in the data grid. The column defaults to 0, so
//! existing installations keep the resolution off.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub(crate) struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "046_general_settings_grid_fk_labels"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let table_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cfg_general_settings'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(sqlite_err)?;

        if !table_exists {
            return Ok(());
        }

        let column_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('cfg_general_settings') WHERE name = 'grid_fk_labels'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(sqlite_err)?;

        if !column_exists {
            tx.execute_batch(
                "ALTER TABLE cfg_general_settings ADD COLUMN grid_fk_labels INTEGER NOT NULL DEFAULT 0;",
            )
            .map_err(sqlite_err)?;
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
