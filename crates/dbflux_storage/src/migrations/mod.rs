//! Dynamic migration system for DBFlux SQLite consolidation.
//!
//! This module provides a trait-based, dynamically-discovered migration system where each
//! migration is a separate file implementing the [`Migration`] trait. The [`MigrationRegistry`]
//! holds all migrations and runs them sequentially, tracking executed migrations in the
//! `sys_migrations` table.
//!
//! ## Architecture
//!
//! - **One file per migration** — e.g., `001_initial.rs`, `002_add_foo.rs`
//! - **Trait-based** — each migration implements [`Migration`] with `name()` and `run()`
//! - **Dynamic discovery** — registry collects all migrations and runs pending ones
//! - **Sequential execution** — migrations run in order, tracked in `sys_migrations`
//!
//! ## Domain Prefix Convention
//!
//! - `cfg_*` — Config domain (profiles, auth, hooks, services, governance)
//! - `st_*`  — State domain (sessions, query history, UI state)
//! - `aud_*` — Audit domain (audit events)
//! - `sys_*` — System domain (migrations, metadata)
//!
//! ## Usage
//!
//! ```ignore
//! let registry = MigrationRegistry::new();
//! registry.register(mod_001_initial::MigrationImpl);
//! registry.run_all(&conn)?;
//! ```

use log::info;
use rusqlite::{Connection, Transaction};

use crate::error::StorageError;

// ---------------------------------------------------------------------------
// Migration trait and error types
// ---------------------------------------------------------------------------

/// Error type for migration-specific failures.
#[derive(Debug)]
pub enum MigrationError {
    /// The database returned an error during migration.
    Sqlite {
        path: std::path::PathBuf,
        source: rusqlite::Error,
    },
    /// A migration failed to apply.
    Failed { name: String, details: String },
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationError::Sqlite { path, source } => {
                write!(
                    f,
                    "migration sqlite error for {}: {}",
                    path.display(),
                    source
                )
            }
            MigrationError::Failed { name, details } => {
                write!(f, "migration '{}' failed: {}", name, details)
            }
        }
    }
}

impl std::error::Error for MigrationError {}

impl From<MigrationError> for StorageError {
    fn from(err: MigrationError) -> Self {
        match err {
            MigrationError::Sqlite { path, source } => StorageError::Sqlite { path, source },
            MigrationError::Failed { name, details } => StorageError::Migration {
                kind: name,
                details,
            },
        }
    }
}

impl From<rusqlite::Error> for MigrationError {
    fn from(source: rusqlite::Error) -> Self {
        MigrationError::Sqlite {
            path: std::path::PathBuf::from("<unknown>"),
            source,
        }
    }
}

/// Trait implemented by each database migration.
///
/// Each migration is a self-contained file that:
/// - Has a unique name returned by `name()`
/// - Contains all DDL in `run()` that creates/modifies schema
///
/// Migrations are idempotent by nature (CREATE TABLE IF NOT EXISTS etc.).
pub trait Migration: Send {
    /// Returns the unique name of this migration.
    ///
    /// Must match the filename prefix (e.g., `001_initial` for `001_initial.rs`).
    fn name(&self) -> &str;

    /// Runs this migration against the given transaction.
    ///
    /// Implementations should use `CREATE TABLE IF NOT EXISTS` etc. to ensure
    /// idempotency. The transaction will be rolled back on error.
    fn run(&self, tx: &Transaction) -> Result<(), MigrationError>;
}

// ---------------------------------------------------------------------------
// MigrationRegistry
// ---------------------------------------------------------------------------

/// A registry that holds all migrations and can run them sequentially.
///
/// # Example
///
/// ```ignore
/// let registry = MigrationRegistry::new();
/// registry.run_all(&conn)?;
/// ```
pub struct MigrationRegistry {
    migrations: Vec<Box<dyn Migration>>,
}

impl MigrationRegistry {
    /// Creates a new registry with all registered migrations.
    pub fn new() -> Self {
        let mut registry = Self {
            migrations: Vec::new(),
        };
        registry.register(mod_001_initial::MigrationImpl);
        registry.register(mod_002_audit_extended::MigrationImpl);
        registry.register(mod_003_audit_settings::MigrationImpl);
        registry.register(mod_004_audit_saved_filters::MigrationImpl);
        registry
    }

    /// Registers a migration with the registry.
    ///
    /// Migrations are executed in the order they are registered.
    pub fn register<M: Migration + 'static>(&mut self, migration: M) {
        self.migrations.push(Box::new(migration));
    }

    /// Runs all pending migrations that have not yet been applied.
    ///
    /// Checks the `sys_migrations` table to determine which migrations have
    /// already been applied and skips them. Runs remaining migrations in
    /// registration order.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError`] if:
    /// - A database error occurs while checking applied migrations
    /// - A migration's `run()` method returns an error
    pub fn run_all(&self, conn: &Connection) -> Result<(), MigrationError> {
        // Ensure sys_migrations table exists
        self.ensure_sys_migrations(conn)?;

        // Get set of already-applied migration names
        let applied: std::collections::HashSet<String> = self
            .get_applied_migrations(conn)
            .map_err(|e| MigrationError::Sqlite {
                path: conn_path(conn),
                source: e,
            })?;

        info!(
            "MigrationRegistry: {} migrations already applied, checking {} registered",
            applied.len(),
            self.migrations.len()
        );

        // Run each pending migration
        for migration in &self.migrations {
            let name = migration.name();

            if applied.contains(name) {
                info!("MigrationRegistry: skipping '{}' (already applied)", name);
                continue;
            }

            info!("MigrationRegistry: applying migration '{}'", name);

            // Run migration in a transaction
            let tx = conn
                .unchecked_transaction()
                .map_err(|source| MigrationError::Sqlite {
                    path: conn_path(conn),
                    source,
                })?;

            migration.run(&tx)?;

            // Record the migration
            tx.execute(
                "INSERT INTO sys_migrations (name, applied_at) VALUES (?1, datetime('now'))",
                rusqlite::params![name],
            )
            .map_err(|source| MigrationError::Sqlite {
                path: conn_path(conn),
                source,
            })?;

            // Commit the transaction
            tx.commit().map_err(|source| MigrationError::Sqlite {
                path: conn_path(conn),
                source,
            })?;

            info!("MigrationRegistry: '{}' applied successfully", name);
        }

        Ok(())
    }

    /// Returns a list of migrations that have not yet been applied.
    pub fn get_pending(&self, conn: &Connection) -> Result<Vec<&dyn Migration>, MigrationError> {
        self.ensure_sys_migrations(conn)?;

        let applied: std::collections::HashSet<String> = self
            .get_applied_migrations(conn)
            .map_err(|source| MigrationError::Sqlite {
                path: conn_path(conn),
                source,
            })?;

        Ok(self
            .migrations
            .iter()
            .filter(|m| !applied.contains(m.name()))
            .map(|m| m.as_ref())
            .collect())
    }

    /// Ensures the sys_migrations table exists.
    fn ensure_sys_migrations(&self, conn: &Connection) -> Result<(), MigrationError> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sys_migrations (
                name TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
            [],
        )
        .map_err(|source| MigrationError::Sqlite {
            path: conn_path(conn),
            source,
        })?;

        Ok(())
    }

    /// Returns the set of migration names that have already been applied.
    fn get_applied_migrations(
        &self,
        conn: &Connection,
    ) -> Result<std::collections::HashSet<String>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT name FROM sys_migrations")?;
        let names: std::collections::HashSet<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(names)
    }
}

impl Default for MigrationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

mod mod_001_initial;
mod mod_002_audit_extended;
mod mod_003_audit_settings;
mod mod_004_audit_saved_filters;

pub use mod_001_initial::MigrationImpl;
pub use mod_002_audit_extended::MigrationImpl as MigrationImplAuditExtended;
pub use mod_003_audit_settings::MigrationImpl as MigrationImplAuditSettings;
pub use mod_004_audit_saved_filters::MigrationImpl as MigrationImplAuditSavedFilters;

// ---------------------------------------------------------------------------
// Database verification utilities
// ---------------------------------------------------------------------------

/// Verifies database integrity using `PRAGMA integrity_check`.
pub fn verify_integrity(conn: &Connection) -> Result<bool, StorageError> {
    let result: String = conn
        .pragma_query_value(None, "integrity_check", |row| row.get(0))
        .map_err(|source| StorageError::Sqlite {
            path: conn_path(conn),
            source,
        })?;
    Ok(result == "ok")
}

fn conn_path(conn: &Connection) -> std::path::PathBuf {
    conn.path()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("<unknown>"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    /// Helper to get all table names from sqlite_master.
    fn table_names(conn: &Connection) -> std::collections::HashSet<String> {
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap();
        stmt.query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    }

    #[test]
    fn test_run_all_idempotent() {
        let temp_dir = std::env::temp_dir().join("dbflux_migration_idempotent");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("test.db");

        let conn = Connection::open(&db_path).unwrap();
        let registry = MigrationRegistry::new();

        registry.run_all(&conn).unwrap();

        let count_first: i64 = conn
            .query_row("SELECT COUNT(*) FROM sys_migrations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count_first, 4, "expected 4 migrations after first run");

        registry.run_all(&conn).unwrap();

        let count_second: i64 = conn
            .query_row("SELECT COUNT(*) FROM sys_migrations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            count_second, 4,
            "expected still 4 migrations after second run (idempotent)"
        );

        drop(conn);
        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_run_all_creates_all_tables() {
        let temp_dir = std::env::temp_dir().join("dbflux_migration_tables");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("test.db");

        let conn = Connection::open(&db_path).unwrap();
        let registry = MigrationRegistry::new();
        registry.run_all(&conn).unwrap();

        let tables = table_names(&conn);

        // Config domain tables
        assert!(
            tables.contains("cfg_auth_profiles"),
            "missing cfg_auth_profiles"
        );
        assert!(
            tables.contains("cfg_connection_profiles"),
            "missing cfg_connection_profiles"
        );
        assert!(
            tables.contains("cfg_proxy_profiles"),
            "missing cfg_proxy_profiles"
        );
        assert!(
            tables.contains("cfg_ssh_tunnel_profiles"),
            "missing cfg_ssh_tunnel_profiles"
        );
        assert!(
            tables.contains("cfg_hook_definitions"),
            "missing cfg_hook_definitions"
        );
        assert!(tables.contains("cfg_services"), "missing cfg_services");
        assert!(
            tables.contains("cfg_governance_settings"),
            "missing cfg_governance_settings"
        );
        assert!(
            tables.contains("cfg_trusted_clients"),
            "missing cfg_trusted_clients"
        );
        assert!(
            tables.contains("cfg_policy_roles"),
            "missing cfg_policy_roles"
        );
        assert!(
            tables.contains("cfg_tool_policies"),
            "missing cfg_tool_policies"
        );
        assert!(
            tables.contains("cfg_connection_folders"),
            "missing cfg_connection_folders"
        );
        assert!(
            tables.contains("cfg_driver_overrides"),
            "missing cfg_driver_overrides"
        );

        // State domain tables
        assert!(tables.contains("st_sessions"), "missing st_sessions");
        assert!(
            tables.contains("st_session_tabs"),
            "missing st_session_tabs"
        );
        assert!(
            tables.contains("st_query_history"),
            "missing st_query_history"
        );
        assert!(
            tables.contains("st_saved_queries"),
            "missing st_saved_queries"
        );
        assert!(tables.contains("st_ui_state"), "missing st_ui_state");
        assert!(
            tables.contains("st_recent_items"),
            "missing st_recent_items"
        );
        assert!(
            tables.contains("st_schema_cache"),
            "missing st_schema_cache"
        );
        assert!(tables.contains("st_event_log"), "missing st_event_log");

        // Audit domain tables
        assert!(
            tables.contains("aud_audit_events"),
            "missing aud_audit_events"
        );
        assert!(
            tables.contains("aud_audit_event_entities"),
            "missing aud_audit_event_entities"
        );
        assert!(
            tables.contains("aud_audit_event_attributes"),
            "missing aud_audit_event_attributes"
        );

        // System domain tables
        assert!(tables.contains("sys_migrations"), "missing sys_migrations");

        drop(conn);
        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_get_pending_returns_unapplied() {
        let temp_dir = std::env::temp_dir().join("dbflux_migration_pending");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("test.db");

        let conn = Connection::open(&db_path).unwrap();
        let registry = MigrationRegistry::new();

        let pending_before = registry.get_pending(&conn).unwrap();
        assert_eq!(
            pending_before.len(),
            4,
            "expected 4 pending migrations before running"
        );
        assert_eq!(pending_before[0].name(), "001_initial");
        assert_eq!(pending_before[1].name(), "002_audit_extended");
        assert_eq!(pending_before[2].name(), "003_audit_settings");
        assert_eq!(pending_before[3].name(), "004_audit_saved_filters");

        registry.run_all(&conn).unwrap();

        let pending_after = registry.get_pending(&conn).unwrap();
        assert!(
            pending_after.is_empty(),
            "expected no pending migrations after running"
        );

        drop(conn);
        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_verification_passes() {
        let temp_dir = std::env::temp_dir().join("dbflux_migration_verify");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("test.db");

        let conn = Connection::open(&db_path).unwrap();
        let registry = MigrationRegistry::new();
        registry.run_all(&conn).unwrap();

        // Verify PRAGMA integrity_check passes
        let integrity_result: String = conn
            .pragma_query_value(None, "integrity_check", |row| row.get(0))
            .unwrap();
        assert_eq!(
            integrity_result, "ok",
            "integrity_check did not return 'ok'"
        );

        // Verify PRAGMA foreign_key_check passes
        // foreign_key_check returns an empty result set when there are no violations
        // (it returns rows only when FK violations exist), so QueryReturnedNoRows = success
        let fk_check_result: Result<String, _> =
            conn.pragma_query_value(None, "foreign_key_check", |row| row.get(0));
        assert!(
            fk_check_result.is_err(),
            "foreign_key_check should return no rows (no violations)"
        );
        // The error being Err(QueryReturnedNoRows) means success - no FK violations
        if let Err(rusqlite::Error::QueryReturnedNoRows) = fk_check_result {
            // Expected: no FK violations
        } else {
            panic!(
                "foreign_key_check returned unexpected error: {:?}",
                fk_check_result
            );
        }

        drop(conn);
        let _ = std::fs::remove_dir_all(temp_dir);
    }
}
