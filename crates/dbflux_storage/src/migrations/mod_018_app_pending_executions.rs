use rusqlite::Transaction;

use super::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "018_app_pending_executions"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        tx.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS app_pending_executions (
                id             TEXT    PRIMARY KEY,
                tool_id        TEXT    NOT NULL,
                connection_id  TEXT    NOT NULL,
                actor_id       TEXT    NOT NULL,
                classification TEXT    NOT NULL,
                payload_json   TEXT    NOT NULL,
                status         TEXT    NOT NULL DEFAULT 'pending',
                created_at     INTEGER NOT NULL,
                expires_at     INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_app_pending_executions_status
                ON app_pending_executions (status);
            ",
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<018_app_pending_executions>"),
            source,
        })?;

        Ok(())
    }
}
