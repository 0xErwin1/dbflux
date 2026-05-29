//! Migration 010: Add `sys_app_meta` table and `dangling_origin` column to
//! `cfg_auth_profiles`.
//!
//! ## `sys_app_meta`
//!
//! A general-purpose key-value table for one-time migration flags and other
//! app-level metadata that does not belong in user-facing config tables.
//!
//! The one-time AWS config reflection migration uses the key
//! `aws_config_reflect_migrated` with value `1` to prevent re-execution.
//!
//! ## `cfg_auth_profiles.dangling_origin`
//!
//! Optional text column. When set, the profile is considered dangling — the
//! stored row exists but the backing credential source is gone. Possible values:
//! - `keyring-only`: the profile had a secret stored in the DBFlux keyring but
//!   no matching section exists in `~/.aws/config` or `~/.aws/credentials`.
//! - `file-gone`: the profile name no longer appears in the AWS config file.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "010_aws_reflect_migration_flag"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let map_err = |source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        };

        // Create sys_app_meta table if it does not exist.
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS sys_app_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );",
        )
        .map_err(map_err)?;

        // Add dangling_origin column to cfg_auth_profiles if the table exists
        // and the column is absent. The table may be absent in partial test
        // databases that simulate an intermediate migration state.
        let table_exists: bool = tx
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='table' AND name='cfg_auth_profiles'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n > 0)
            .map_err(map_err)?;

        if table_exists {
            let column_exists: bool = tx
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('cfg_auth_profiles') \
                     WHERE name = 'dangling_origin'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map(|n| n > 0)
                .map_err(map_err)?;

            if !column_exists {
                tx.execute_batch("ALTER TABLE cfg_auth_profiles ADD COLUMN dangling_origin TEXT;")
                    .map_err(map_err)?;
            }
        }

        Ok(())
    }
}
