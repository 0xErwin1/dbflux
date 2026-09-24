use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "031_general_settings_editor_row_limit"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let exists: i64 = tx.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'cfg_general_settings'",
            [],
            |row| row.get(0),
        )?;
        if exists == 0 {
            return Ok(());
        }

        let exists: i64 = tx.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('cfg_general_settings') WHERE name = 'editor_row_limit'",
            [],
            |row| row.get(0),
        )?;
        if exists == 0 {
            tx.execute_batch(
                "ALTER TABLE cfg_general_settings ADD COLUMN editor_row_limit INTEGER NOT NULL DEFAULT 10000 CHECK (editor_row_limit >= 1);",
            )?;
        }
        Ok(())
    }
}
