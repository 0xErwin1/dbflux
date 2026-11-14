//! Migration 045: Add `table_alias_completion` and `table_alias_use_as` to
//! `cfg_general_settings`.
//!
//! Persists whether accepting a table completion after `FROM` / `JOIN` appends
//! a generated alias (on by default), and whether that alias is written with
//! `AS` (off by default).

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub(crate) struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "045_general_settings_table_alias"
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

        for (column, default) in [("table_alias_completion", 1), ("table_alias_use_as", 0)] {
            let column_exists: bool = tx
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('cfg_general_settings') WHERE name = ?1",
                    [column],
                    |row| row.get::<_, i64>(0),
                )
                .map(|n| n > 0)?;

            if !column_exists {
                tx.execute_batch(&format!(
                    "ALTER TABLE cfg_general_settings ADD COLUMN {column} INTEGER NOT NULL DEFAULT {default};"
                ))?;
            }
        }

        Ok(())
    }
}
