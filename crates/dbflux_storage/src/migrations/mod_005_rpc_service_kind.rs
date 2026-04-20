//! Migration 005: Add service kind classification to RPC services.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

/// Adds the `service_kind` column to `cfg_services` with a backward-compatible default.
pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "005_rpc_service_kind"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        if has_service_kind_column(tx)? {
            return Ok(());
        }

        tx.execute(
            "ALTER TABLE cfg_services ADD COLUMN service_kind TEXT NOT NULL DEFAULT 'driver'",
            [],
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        Ok(())
    }
}

fn has_service_kind_column(tx: &Transaction) -> Result<bool, MigrationError> {
    let mut stmt = tx
        .prepare("PRAGMA table_info(cfg_services)")
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

    for column in columns {
        let column = column.map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        if column == "service_kind" {
            return Ok(true);
        }
    }

    Ok(false)
}
