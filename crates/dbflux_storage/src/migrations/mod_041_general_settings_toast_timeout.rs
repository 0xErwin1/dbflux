//! Migration 041: Add `toast_auto_dismiss_secs` to `cfg_general_settings`.
//!
//! Persists the auto-dismiss delay for toasts, in seconds. The column
//! defaults to 8, the delay of a new install; `0` keeps toasts until the
//! user dismisses them.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "041_general_settings_toast_timeout"
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
                "SELECT COUNT(*) FROM pragma_table_info('cfg_general_settings') WHERE name = 'toast_auto_dismiss_secs'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)?;

        if !column_exists {
            tx.execute_batch(
                "ALTER TABLE cfg_general_settings ADD COLUMN toast_auto_dismiss_secs INTEGER NOT NULL DEFAULT 8;",
            )?;
        }

        Ok(())
    }
}
