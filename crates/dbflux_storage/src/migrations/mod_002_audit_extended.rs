//! Migration 002: Extended audit events schema.
//!
//! This migration extends the `aud_audit_events` table with new nullable columns
//! to support the full RF-050/RF-051 audit event schema. The new columns capture:
//!
//! - Event classification (severity, category, source)
//! - Actor information (actor_type)
//! - Connection context (driver_id)
//! - Object references (object_type, object_id)
//! - Timing information (duration_ms)
//! - Session/correlation tracking (session_id, correlation_id)
//!
//! All new columns are nullable to maintain backward compatibility with existing
//! MCP governance events that use the legacy `append()` API.

use rusqlite::{Result as SqliteResult, Transaction};

use crate::migrations::{Migration, MigrationError};

/// Extended audit events schema migration.
pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "002_audit_extended"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        let table = "aud_audit_events";

        // Add new columns one by one with proper error handling
        // Each column is added only if it doesn't already exist

        add_column_if_not_exists(tx, table, "level", "TEXT")?;
        add_column_if_not_exists(tx, table, "category", "TEXT")?;
        add_column_if_not_exists(tx, table, "action", "TEXT")?;
        add_column_if_not_exists(tx, table, "outcome", "TEXT")?;
        add_column_if_not_exists(tx, table, "actor_type", "TEXT")?;
        add_column_if_not_exists(tx, table, "source_id", "TEXT")?;
        add_column_if_not_exists(tx, table, "summary", "TEXT")?;
        add_column_if_not_exists(tx, table, "connection_id", "TEXT")?;
        add_column_if_not_exists(tx, table, "database_name", "TEXT")?;
        add_column_if_not_exists(tx, table, "driver_id", "TEXT")?;
        add_column_if_not_exists(tx, table, "object_type", "TEXT")?;
        add_column_if_not_exists(tx, table, "object_id", "TEXT")?;
        add_column_if_not_exists(tx, table, "details_json", "TEXT")?;
        add_column_if_not_exists(tx, table, "error_code", "TEXT")?;
        add_column_if_not_exists(tx, table, "error_message", "TEXT")?;
        add_column_if_not_exists(tx, table, "duration_ms", "INTEGER")?;
        add_column_if_not_exists(tx, table, "session_id", "TEXT")?;
        add_column_if_not_exists(tx, table, "correlation_id", "TEXT")?;

        // Create indices
        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_aud_audit_events_ts_ms ON aud_audit_events(created_at_epoch_ms DESC)",
            [],
        ).map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_aud_audit_events_level ON aud_audit_events(level)",
            [],
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_aud_audit_events_category ON aud_audit_events(category)",
            [],
        ).map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_aud_audit_events_outcome ON aud_audit_events(outcome)",
            [],
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_aud_audit_events_source_id ON aud_audit_events(source_id)",
            [],
        ).map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_aud_audit_events_actor_type ON aud_audit_events(actor_type)",
            [],
        ).map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_aud_audit_events_connection_id ON aud_audit_events(connection_id)",
            [],
        ).map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_aud_audit_events_driver_id ON aud_audit_events(driver_id)",
            [],
        ).map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        // Composite index for common query pattern: time DESC, category, level
        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_aud_audit_events_ts_category_level ON aud_audit_events(created_at_epoch_ms DESC, category, level)",
            [],
        ).map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;

        Ok(())
    }
}

/// Adds a column to a table if it doesn't already exist.
///
/// SQLite doesn't support `ALTER TABLE ADD COLUMN IF NOT EXISTS` in older versions,
/// so we check the schema first before adding.
fn add_column_if_not_exists(
    tx: &Transaction,
    table: &str,
    column: &str,
    col_type: &str,
) -> Result<(), MigrationError> {
    // Check if column already exists
    let exists = column_exists(tx, table, column).map_err(|source| MigrationError::Sqlite {
        path: std::path::PathBuf::from("<unknown>"),
        source,
    })?;

    if !exists {
        tx.execute(
            &format!("ALTER TABLE {} ADD COLUMN {} {}", table, column, col_type),
            [],
        )
        .map_err(|source| MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        })?;
    }

    Ok(())
}

/// Checks if a column exists in a table.
fn column_exists(tx: &Transaction, table: &str, column: &str) -> SqliteResult<bool> {
    let mut stmt = tx.prepare(&format!(
        "SELECT COUNT(*) FROM pragma_table_info('{}') WHERE name = ?",
        table
    ))?;

    let count: i64 = stmt.query_row([column], |row| row.get(0))?;

    Ok(count > 0)
}
