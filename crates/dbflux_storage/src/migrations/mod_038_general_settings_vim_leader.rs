//! Migration 038: Add `vim_leader` column to `cfg_general_settings`.
//!
//! Persists the key that starts Vim leader sequences, in the keymap's stored
//! key form. The column defaults to `space`, the leader of a new install.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "038_general_settings_vim_leader"
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
                "SELECT COUNT(*) FROM pragma_table_info('cfg_general_settings') WHERE name = 'vim_leader'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)?;

        if !column_exists {
            tx.execute_batch(
                "ALTER TABLE cfg_general_settings ADD COLUMN vim_leader TEXT NOT NULL DEFAULT 'space';",
            )?;
        }

        Ok(())
    }
}
