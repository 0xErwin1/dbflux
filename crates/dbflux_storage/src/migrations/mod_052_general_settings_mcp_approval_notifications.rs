//! Migration 052: Add `mcp_approval_notifications` to `cfg_general_settings`.
//!
//! Whether a new pending MCP approval raises a notification. The column is
//! `NOT NULL DEFAULT 1`, so existing installations get the notification
//! enabled without a backfill.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub(crate) struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "052_general_settings_mcp_approval_notifications"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let table_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cfg_general_settings'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count > 0)
            .map_err(sqlite_err)?;

        if !table_exists {
            return Ok(());
        }

        let column_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('cfg_general_settings') WHERE name = 'mcp_approval_notifications'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count > 0)
            .map_err(sqlite_err)?;

        if !column_exists {
            tx.execute_batch(
                "ALTER TABLE cfg_general_settings ADD COLUMN mcp_approval_notifications INTEGER NOT NULL DEFAULT 1;",
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

#[cfg(test)]
mod tests {
    use super::MigrationImpl;
    use crate::migrations::Migration;

    #[test]
    fn existing_row_gets_notifications_enabled_and_rerun_is_idempotent() {
        let mut connection = rusqlite::Connection::open_in_memory().expect("scratch database");
        connection
            .execute_batch(
                "CREATE TABLE cfg_general_settings (id INTEGER PRIMARY KEY);
                 INSERT INTO cfg_general_settings (id) VALUES (1);",
            )
            .expect("legacy table");

        for _ in 0..2 {
            let tx = connection.transaction().expect("transaction");
            MigrationImpl.run(&tx).expect("migration");
            tx.commit().expect("commit");
        }

        let value: i64 = connection
            .query_row(
                "SELECT mcp_approval_notifications FROM cfg_general_settings WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .expect("column exists");
        assert_eq!(value, 1);
    }
}
