//! Migration 024: Add `s3_access_key_id` and `s3_path_style` columns to
//! `cfg_connection_driver_configs`.
//!
//! `DbConfig::S3` reuses the existing `dynamo_region`/`dynamo_profile`/
//! `dynamo_endpoint` columns for region/profile/endpoint (same convention
//! already used by `CloudWatchLogs`), but access key id and path-style
//! addressing have no equivalent existing column, so this migration adds
//! them as native columns rather than falling back to the generic JSON field.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "024_s3_config_columns"
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

        let access_key_id_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('cfg_connection_driver_configs') WHERE name = 's3_access_key_id'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;

        if !access_key_id_exists {
            tx.execute_batch(
                "ALTER TABLE cfg_connection_driver_configs ADD COLUMN s3_access_key_id TEXT;",
            )
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;
        }

        let path_style_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('cfg_connection_driver_configs') WHERE name = 's3_path_style'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;

        if !path_style_exists {
            tx.execute_batch(
                "ALTER TABLE cfg_connection_driver_configs ADD COLUMN s3_path_style INTEGER NOT NULL DEFAULT 0;",
            )
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;
        }

        Ok(())
    }
}
