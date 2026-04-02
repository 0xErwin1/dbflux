//! Migration 003: Add audit settings table.
//!
//! This migration adds the `cfg_audit_settings` table for storing
//! audit system configuration including retention policy and capture settings.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

/// The audit settings migration.
pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "003_audit_settings"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        tx.execute_batch(
            r#"
            -- Audit settings singleton table
            CREATE TABLE IF NOT EXISTS cfg_audit_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                enabled INTEGER NOT NULL DEFAULT 1,
                retention_days INTEGER NOT NULL DEFAULT 30,
                capture_user_actions INTEGER NOT NULL DEFAULT 1,
                capture_system_events INTEGER NOT NULL DEFAULT 1,
                capture_query_text INTEGER NOT NULL DEFAULT 0,
                capture_hook_output_metadata INTEGER NOT NULL DEFAULT 1,
                redact_sensitive_values INTEGER NOT NULL DEFAULT 1,
                max_detail_bytes INTEGER NOT NULL DEFAULT 65536,
                purge_on_startup INTEGER NOT NULL DEFAULT 0,
                background_purge_interval_minutes INTEGER NOT NULL DEFAULT 360,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            -- Insert default settings if not exists
            INSERT OR IGNORE INTO cfg_audit_settings (id) VALUES (1);
            "#,
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;
        Ok(())
    }
}
