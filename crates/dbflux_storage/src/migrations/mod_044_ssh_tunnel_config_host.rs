//! Migration 044: Add `ssh_config_host` column to `cfg_ssh_tunnel_profiles` and
//! `cfg_connection_driver_configs`.
//!
//! Stores the alias of a host in the user's SSH config that the tunnel
//! references, if any (`NULL` means "no alias", i.e. the manual host, port and
//! user fields are the source of the target). Nullable and without a default,
//! so existing rows read as `NULL`.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub(crate) struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "044_ssh_tunnel_config_host"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        for table in ["cfg_ssh_tunnel_profiles", "cfg_connection_driver_configs"] {
            let table_exists: bool = tx
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table}'"
                    ),
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map(|count| count > 0)
                .map_err(sqlite_err)?;

            if !table_exists {
                continue;
            }

            let column_exists: bool = tx
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = 'ssh_config_host'"
                    ),
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map(|count| count > 0)
                .map_err(sqlite_err)?;

            if !column_exists {
                tx.execute_batch(&format!(
                    "ALTER TABLE {table} ADD COLUMN ssh_config_host TEXT;"
                ))
                .map_err(sqlite_err)?;
            }
        }

        Ok(())
    }
}

fn sqlite_err(source: rusqlite::Error) -> MigrationError {
    MigrationError::Sqlite {
        path: std::path::PathBuf::from("<044_ssh_tunnel_config_host>"),
        source,
    }
}
