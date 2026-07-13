use rusqlite::Transaction;

use super::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "023_profile_hook_interpreter"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let table_exists: bool = tx.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'cfg_connection_profile_hooks'",
            [],
            |row| row.get::<_, i64>(0),
        )? > 0;

        if !table_exists {
            return Ok(());
        }

        let column_exists: bool = tx.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('cfg_connection_profile_hooks') WHERE name = 'script_interpreter'",
            [],
            |row| row.get::<_, i64>(0),
        )? > 0;

        if !column_exists {
            tx.execute_batch(
                "ALTER TABLE cfg_connection_profile_hooks ADD COLUMN script_interpreter TEXT NULL",
            )?;
        }

        Ok(())
    }
}
