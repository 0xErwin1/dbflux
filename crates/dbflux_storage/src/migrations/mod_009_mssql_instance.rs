//! Migration 009: Add `mssql_instance` and `mssql_trust_server_certificate`
//! columns to `cfg_connection_driver_configs`.
//!
//! Without these columns, SQL Server connection profiles silently lost
//! their named instance and `TrustServerCertificate` setting on every save,
//! so a profile saved with `instance = "MSSQLSERVER2019"` would come back
//! after a restart with `instance = None` and the driver would dial
//! `host:port` directly instead of going through SQL Browser.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "009_mssql_instance"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let table_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cfg_connection_driver_configs'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;

        if !table_exists {
            return Ok(());
        }

        // Add `mssql_instance` if missing.
        let instance_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('cfg_connection_driver_configs') WHERE name = 'mssql_instance'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;

        if !instance_exists {
            tx.execute_batch(
                "ALTER TABLE cfg_connection_driver_configs ADD COLUMN mssql_instance TEXT;",
            )
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;
        }

        // Add `mssql_trust_server_certificate` if missing. Defaults to 1
        // (true) — matches the driver's existing form-mode default for the
        // common dev/self-signed-cert case.
        let trust_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('cfg_connection_driver_configs') WHERE name = 'mssql_trust_server_certificate'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;

        if !trust_exists {
            tx.execute_batch(
                "ALTER TABLE cfg_connection_driver_configs ADD COLUMN mssql_trust_server_certificate INTEGER NOT NULL DEFAULT 1;",
            )
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;
        }

        Ok(())
    }
}
