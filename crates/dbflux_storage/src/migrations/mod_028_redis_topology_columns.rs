//! Migration 028: Add `redis_topology`, `redis_sentinel_master_name`, and
//! `redis_additional_nodes` columns to `cfg_connection_driver_configs`.
//!
//! `DbConfig::Redis` gained explicit deployment-topology fields (standalone,
//! cluster, sentinel) with no equivalent existing column, so this migration
//! adds them as native nullable TEXT columns rather than the generic JSON
//! field.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "028_redis_topology_columns"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let table_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cfg_connection_driver_configs'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(sqlite_err)?;

        if !table_exists {
            return Ok(());
        }

        for column in [
            "redis_topology",
            "redis_sentinel_master_name",
            "redis_additional_nodes",
        ] {
            let column_exists: bool = tx
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM pragma_table_info('cfg_connection_driver_configs') WHERE name = '{column}'"
                    ),
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map(|n| n > 0)
                .map_err(sqlite_err)?;

            if !column_exists {
                tx.execute_batch(&format!(
                    "ALTER TABLE cfg_connection_driver_configs ADD COLUMN {column} TEXT;"
                ))
                .map_err(sqlite_err)?;
            }
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
