//! Migration 033: Add `environment` column to `cfg_connection_profiles`.
//!
//! Stores the optional deployment environment of a connection
//! (`development`, `staging` or `production`). The column is nullable, so
//! existing profiles keep no environment.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "033_connection_profile_environment"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let table_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cfg_connection_profiles'",
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
                "SELECT COUNT(*) FROM pragma_table_info('cfg_connection_profiles') WHERE name = 'environment'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count > 0)
            .map_err(sqlite_err)?;

        if !column_exists {
            tx.execute_batch("ALTER TABLE cfg_connection_profiles ADD COLUMN environment TEXT;")
                .map_err(sqlite_err)?;
        }

        Ok(())
    }
}

fn sqlite_err(source: rusqlite::Error) -> MigrationError {
    MigrationError::Sqlite {
        path: std::path::PathBuf::from("<033_connection_profile_environment>"),
        source,
    }
}
