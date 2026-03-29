//! Database migration infrastructure for DBFlux internal storage.
//!
//! This module provides migration execution for both `config.db` and `state.db`.
//! Migrations are versioned and tracked in a `schema_migrations` table.
//!
//! Migration bookkeeping is transactional: the `user_version` PRAGMA and the
//! `schema_migrations` table are updated together so that on failure the DB
//! is left at a consistent version.

use log::info;
use rusqlite::Connection;

use crate::error::StorageError;

pub mod state;

/// Current migration versions for each database.
pub mod config_migrations {
    /// Initial migration version for config.db.
    pub const INITIAL_VERSION: u32 = 1;
    /// Version 2: adds system_metadata table for existing installs
    /// that ran the v1 migration before this table was added.
    pub const SYSTEM_METADATA_VERSION: u32 = 2;
}

/// Runs all pending config database migrations.
///
/// This function should be called on every startup. It checks which migrations
/// have already been applied (via `user_version` pragma) and runs only those
/// that are new. Bookkeeping is transactional.
pub fn run_config_migrations(conn: &Connection) -> Result<(), StorageError> {
    let current_version = current_schema_version(conn)?;

    info!(
        "Config database current schema version: {}",
        current_version
    );

    if current_version < config_migrations::INITIAL_VERSION {
        let tx = conn
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        run_config_initial_migration_in(&tx)?;

        // Update user_version and insert migration record in the same transaction
        tx.pragma_update(None, "user_version", config_migrations::INITIAL_VERSION)
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        tx.execute(
            "INSERT INTO schema_migrations (version, name) VALUES (?1, ?2)",
            rusqlite::params![config_migrations::INITIAL_VERSION, "0001_initial"],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        tx.commit().map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        info!(
            "Config initial migration {} applied successfully",
            config_migrations::INITIAL_VERSION
        );
    }

    // Additive v2 migration: add system_metadata for existing installs that
    // ran the v1 migration before this table was added.
    if current_version < config_migrations::SYSTEM_METADATA_VERSION {
        let tx = conn
            .unchecked_transaction()
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        tx.execute(
            "CREATE TABLE IF NOT EXISTS system_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT (datetime('now')))",
            [],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        tx.pragma_update(
            None,
            "user_version",
            config_migrations::SYSTEM_METADATA_VERSION,
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        tx.execute(
            "INSERT INTO schema_migrations (version, name) VALUES (?1, ?2)",
            rusqlite::params![
                config_migrations::SYSTEM_METADATA_VERSION,
                "0002_system_metadata"
            ],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        tx.commit().map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

        info!(
            "Config system_metadata migration {} applied successfully",
            config_migrations::SYSTEM_METADATA_VERSION
        );
    }

    Ok(())
}

/// Reads the current schema version from `user_version` pragma.
/// Returns `Ok(0)` if the pragma read itself fails (e.g., brand-new database
/// before any migrations have run and set the version). This is the only case
/// where we treat a pragma failure as version 0 — all other storage errors
/// surface as `StorageError`.
fn current_schema_version(conn: &Connection) -> Result<u32, StorageError> {
    match conn.pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0)) {
        Ok(v) => Ok(v),
        // Brand-new DB before any migration has ever run — version 0
        Err(rusqlite::Error::InvalidQuery) => Ok(0),
        Err(e) => Err(StorageError::Sqlite {
            path: "config.db".into(),
            source: e,
        }),
    }
}

/// Runs the initial migration that creates all config database tables.
///
/// This migration creates the baseline tables for durable configuration:
/// - app_settings
/// - connection_profiles
/// - auth_profiles
/// - proxy_profiles
/// - ssh_tunnel_profiles
/// - hook_definitions
/// - hook_bindings
/// - services
/// - driver_settings
fn run_config_initial_migration_in(conn: &Connection) -> Result<(), StorageError> {
    // Ensure migration tracking table exists first (before we start tracking)
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        "#,
    )
    .map_err(|source| StorageError::Sqlite {
        path: "config.db".into(),
        source,
    })?;

    // Create all config tables
    conn.execute_batch(
        r#"
        -- Settings table for app configuration (key-value style)
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- Connection profiles
        CREATE TABLE IF NOT EXISTS connection_profiles (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            driver_id TEXT,
            description TEXT,
            favorite INTEGER DEFAULT 0,
            color TEXT,
            icon TEXT,
            config_json TEXT NOT NULL,
            auth_profile_id TEXT,
            proxy_profile_id TEXT,
            ssh_tunnel_profile_id TEXT,
            access_profile_id TEXT,
            settings_overrides_json TEXT,
            connection_settings_json TEXT,
            hooks_json TEXT,
            hook_bindings_json TEXT,
            value_refs_json TEXT,
            mcp_governance_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- Auth profiles
        CREATE TABLE IF NOT EXISTS auth_profiles (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            provider_id TEXT NOT NULL,
            fields_json TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- Proxy profiles
        CREATE TABLE IF NOT EXISTS proxy_profiles (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            kind TEXT NOT NULL,
            host TEXT NOT NULL,
            port INTEGER NOT NULL,
            auth_json TEXT NOT NULL,
            no_proxy TEXT,
            enabled INTEGER DEFAULT 1,
            save_secret INTEGER DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- SSH tunnel profiles
        CREATE TABLE IF NOT EXISTS ssh_tunnel_profiles (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            config_json TEXT NOT NULL,
            save_secret INTEGER DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- Hook definitions (reusable hooks)
        CREATE TABLE IF NOT EXISTS hook_definitions (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            kind_json TEXT NOT NULL,
            execution_mode TEXT NOT NULL DEFAULT 'Command',
            script_ref TEXT,
            command_json TEXT,
            cwd TEXT,
            env_json TEXT,
            inherit_env INTEGER DEFAULT 1,
            timeout_ms INTEGER,
            ready_signal TEXT,
            on_failure TEXT NOT NULL DEFAULT 'Warn',
            enabled INTEGER DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- Hook bindings (connections to hooks)
        CREATE TABLE IF NOT EXISTS hook_bindings (
            id TEXT PRIMARY KEY,
            hook_id TEXT NOT NULL,
            target_kind TEXT NOT NULL,
            target_id TEXT NOT NULL,
            phase TEXT NOT NULL,
            order_index INTEGER DEFAULT 0,
            FOREIGN KEY (hook_id) REFERENCES hook_definitions(id) ON DELETE CASCADE
        );

        -- Services/RPC definitions
        CREATE TABLE IF NOT EXISTS services (
            socket_id TEXT PRIMARY KEY,
            enabled INTEGER DEFAULT 1,
            command TEXT,
            args_json TEXT,
            env_json TEXT,
            startup_timeout_ms INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- Driver-level settings and overrides
        CREATE TABLE IF NOT EXISTS driver_settings (
            driver_key TEXT PRIMARY KEY,
            overrides_json TEXT,
            settings_json TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        "#,
    )
    .map_err(|source| StorageError::Sqlite {
        path: "config.db".into(),
        source,
    })?;

    Ok(())
}

/// Runs migrations for the state database.
pub fn run_state_migrations(conn: &Connection) -> Result<(), StorageError> {
    state::run_state_migrations(conn)
}

/// Verifies that a database is in a consistent state by running integrity check.
pub fn verify_integrity(conn: &Connection) -> Result<bool, StorageError> {
    let result: String = conn
        .pragma_query_value(None, "integrity_check", |row| row.get(0))
        .map_err(|source| StorageError::Sqlite {
            path: "unknown".into(),
            source,
        })?;

    Ok(result == "ok")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::open_database;
    use std::path::PathBuf;

    fn temp_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("dbflux_storage_migrations_{}.sqlite", name))
    }

    #[test]
    fn config_initial_migration_creates_tables() {
        let path = temp_db("initial_migration");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");

        run_config_migrations(&conn).expect("migration should run");

        // Verify migration was recorded (0001_initial + 0002_system_metadata)
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 2);

        // Verify key tables exist
        conn.execute("SELECT 1 FROM app_settings", [])
            .expect("app_settings should exist");
        conn.execute("SELECT 1 FROM connection_profiles", [])
            .expect("connection_profiles should exist");
        conn.execute("SELECT 1 FROM auth_profiles", [])
            .expect("auth_profiles should exist");
        conn.execute("SELECT 1 FROM proxy_profiles", [])
            .expect("proxy_profiles should exist");
        conn.execute("SELECT 1 FROM ssh_tunnel_profiles", [])
            .expect("ssh_tunnel_profiles should exist");
        conn.execute("SELECT 1 FROM hook_definitions", [])
            .expect("hook_definitions should exist");
        conn.execute("SELECT 1 FROM services", [])
            .expect("services should exist");
        conn.execute("SELECT 1 FROM driver_settings", [])
            .expect("driver_settings should exist");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn config_migration_is_idempotent() {
        let path = temp_db("idempotent_migration");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");

        // First run
        run_config_migrations(&conn).expect("first migration should run");

        // Second run should succeed (idempotent)
        run_config_migrations(&conn).expect("second migration should be idempotent");

        // Still only two migrations recorded (0001_initial + 0002_system_metadata)
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 2);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
