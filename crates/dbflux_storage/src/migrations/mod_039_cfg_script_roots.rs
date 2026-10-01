use rusqlite::Transaction;

use super::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "039_cfg_script_roots"
    }

    /// Creates the table of external script folders.
    ///
    /// A row registers a folder outside the managed scripts directory whose
    /// scripts the sidebar lists in place. `path` is the canonical path the
    /// folder had when it was registered; it is unique so the same folder cannot
    /// be registered twice. `position` keeps the order the user added them in.
    /// Removing a row never touches the folder on disk.
    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        tx.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS cfg_script_roots (
                id         TEXT PRIMARY KEY,
                path       TEXT NOT NULL UNIQUE,
                label      TEXT NOT NULL,
                position   INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            ",
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<039_cfg_script_roots>"),
            source,
        })?;

        Ok(())
    }
}
