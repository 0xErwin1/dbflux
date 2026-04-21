//! Migration 006: Add RPC API contract metadata to services.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "006_rpc_service_api_contract"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        add_column_if_missing(tx, "api_family", "TEXT")?;
        add_column_if_missing(tx, "api_major", "INTEGER")?;
        add_column_if_missing(tx, "api_minor", "INTEGER")?;

        Ok(())
    }
}

fn add_column_if_missing(
    tx: &Transaction,
    column_name: &str,
    column_definition: &str,
) -> Result<(), MigrationError> {
    if has_column(tx, column_name)? {
        return Ok(());
    }

    tx.execute(
        &format!("ALTER TABLE cfg_services ADD COLUMN {column_name} {column_definition}"),
        [],
    )
    .map_err(|source| MigrationError::Sqlite {
        path: std::path::PathBuf::from("<unknown>"),
        source,
    })?;

    Ok(())
}

fn has_column(tx: &Transaction, column_name: &str) -> Result<bool, MigrationError> {
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

        if column == column_name {
            return Ok(true);
        }
    }

    Ok(false)
}
