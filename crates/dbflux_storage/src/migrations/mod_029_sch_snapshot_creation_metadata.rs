//! Migration 029: Add nullable `creation_metadata_json` to `sch_snapshot_tables`.
//!
//! Persists structured [`TableCreationMetadata`](dbflux_core::TableCreationMetadata)
//! for deep schema snapshots alongside each captured table row. The column is
//! nullable so rows written before this migration (and shallow snapshots,
//! which never carry creation metadata) remain readable without backfill.
//! Metadata is matched to its table row by the row's native `schema_name` /
//! `name` components — never a concatenated key.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "029_sch_snapshot_creation_metadata"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let table_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='sch_snapshot_tables'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(sqlite_err)?;

        if !table_exists {
            return Ok(());
        }

        let column_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('sch_snapshot_tables') WHERE name = 'creation_metadata_json'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(sqlite_err)?;

        if !column_exists {
            tx.execute_batch(
                "ALTER TABLE sch_snapshot_tables ADD COLUMN creation_metadata_json TEXT;",
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
