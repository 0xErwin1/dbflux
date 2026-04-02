//! Migration 004: Audit saved filters.
//!
//! This migration creates the `aud_saved_filters` table for persisting
//! user-defined audit filter presets.

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

/// Saved filters migration.
pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "004_audit_saved_filters"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        // Create the aud_saved_filters table
        tx.execute(
            r#"
            CREATE TABLE IF NOT EXISTS aud_saved_filters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT,
                -- Filter fields stored as JSON for flexibility
                filter_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            "#,
            [],
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        // Create index on name for fast lookups
        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_aud_saved_filters_name ON aud_saved_filters(name)",
            [],
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        Ok(())
    }
}
