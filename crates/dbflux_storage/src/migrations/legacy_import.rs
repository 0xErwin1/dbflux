//! Legacy configuration import for DBFlux storage consolidation.
//!
//! This module imports data from legacy config.json files into the unified `dbflux.db`:
//! - `config.json` → cfg_services table (RPC services)
//!
//! ## Idempotency
//!
//! Each source file is tracked in `sys_legacy_imports` by source path.
//! If a source was already imported, subsequent calls skip that source.
//!
//! ## Import Order
//!
//! 1. Check if config.json exists and has rpc_services
//! 2. Import RPC services from it
//! 3. Record success in sys_legacy_imports

use log::{info, warn};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::StorageError;

/// Legacy database paths (simplified for JSON-only import).
#[derive(Debug, Clone)]
pub struct LegacyPaths {
    pub config_json: PathBuf,
    pub unified_db: PathBuf,
}

impl LegacyPaths {
    /// Resolves legacy database paths from standard locations.
    ///
    /// - JSON config: `~/.config/dbflux/config.json`
    /// - Target DB: `~/.local/share/dbflux/dbflux.db`
    pub fn resolve() -> Result<Self, StorageError> {
        let config_dir = crate::paths::config_data_dir()?;
        let data_dir = crate::paths::data_dir()?;

        Ok(Self {
            config_json: config_dir.join("config.json"),
            unified_db: data_dir.join("dbflux.db"),
        })
    }
}

/// Result of a legacy database import operation.
#[derive(Debug, Clone, Default)]
pub struct ImportResult {
    pub rpc_services_imported: bool,
    pub warnings: Vec<ImportWarning>,
}

impl ImportResult {
    pub fn any_imported(&self) -> bool {
        self.rpc_services_imported
    }

    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

/// A warning issued during import (non-fatal).
#[derive(Debug, Clone)]
pub struct ImportWarning {
    pub source: String,
    pub message: String,
}

/// Import error types.
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("SQLite error for {path}: {source}")]
    Sqlite {
        path: PathBuf,
        source: rusqlite::Error,
    },

    #[error("IO error for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("import failed: {0}")]
    Failed(String),
}

impl From<rusqlite::Error> for ImportError {
    fn from(source: rusqlite::Error) -> Self {
        ImportError::Sqlite {
            path: PathBuf::from("<unknown>"),
            source,
        }
    }
}

impl From<std::io::Error> for ImportError {
    fn from(source: std::io::Error) -> Self {
        ImportError::Io {
            path: PathBuf::from("<unknown>"),
            source,
        }
    }
}

impl From<StorageError> for ImportError {
    fn from(source: StorageError) -> Self {
        match source {
            StorageError::Sqlite { path, source } => ImportError::Sqlite { path, source },
            StorageError::Io { path, source } => ImportError::Io { path, source },
            _ => ImportError::Failed(source.to_string()),
        }
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Main entry point: imports RPC services from legacy config.json into dbflux.db.
///
/// This function is idempotent — if config.json was already imported,
/// it will be skipped on subsequent calls.
///
/// # Arguments
/// * `unified_db_path` — path to the unified `dbflux.db`
/// * `legacy_paths` — paths to the legacy files
///
/// # Returns
/// `Ok(ImportResult)` with flags indicating what was imported and any warnings.
pub fn import_legacy_databases(
    unified_db_path: &Path,
    legacy_paths: &LegacyPaths,
) -> Result<ImportResult, ImportError> {
    info!(
        "Starting legacy database import into: {}",
        unified_db_path.display()
    );

    let mut result = ImportResult::default();

    // Open unified database connection
    let unified_conn = open_unified_db(unified_db_path)?;

    // Check if config.json has already been imported
    let already_imported = check_imported_sources(&unified_conn)?;

    // Check if config.json exists
    if !legacy_paths.config_json.exists() {
        info!("config.json does not exist, nothing to import");
        return Ok(result);
    }

    if already_imported.contains("config.json") {
        info!("config.json already imported, skipping");
        return Ok(result);
    }

    // Import RPC services from config.json
    match import_rpc_services_from_config_json(&unified_conn, &legacy_paths.config_json) {
        Ok(_) => {
            result.rpc_services_imported = true;
            record_import_success(
                &unified_conn,
                legacy_paths.config_json.to_str().unwrap_or("config.json"),
                "config",
            )?;
        }
        Err(e) => {
            warn!("Failed to import RPC services from config.json: {}", e);
            result.warnings.push(ImportWarning {
                source: "config.json".to_string(),
                message: format!("import failed: {}", e),
            });
        }
    }

    // Backup config.json after successful import
    if result.rpc_services_imported
        && legacy_paths.config_json.exists()
        && let Err(e) = backup_legacy_file(&legacy_paths.config_json, "config.json")
    {
        warn!("Failed to backup config.json: {}", e);
        result.warnings.push(ImportWarning {
            source: "backup".to_string(),
            message: format!("backup failed: {}", e),
        });
    }

    info!(
        "Legacy import complete: rpc={}, warnings={}",
        result.rpc_services_imported,
        result.warnings.len()
    );

    Ok(result)
}

// ============================================================================
// Idempotency - Check Import Status
// ============================================================================

/// Returns the set of source paths that have already been successfully imported.
fn check_imported_sources(conn: &Connection) -> Result<HashSet<String>, ImportError> {
    let mut stmt = conn
        .prepare("SELECT source_path FROM sys_legacy_imports WHERE status = 'completed'")
        .map_err(|source| ImportError::Sqlite {
            path: PathBuf::from("dbflux.db"),
            source,
        })?;

    let paths: HashSet<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|source| ImportError::Sqlite {
            path: PathBuf::from("dbflux.db"),
            source,
        })?
        .filter_map(|r| r.ok())
        .collect();

    Ok(paths)
}

/// Records a successful import in sys_legacy_imports.
fn record_import_success(
    conn: &Connection,
    source_path: &str,
    domain: &str,
) -> Result<(), ImportError> {
    conn.execute(
        r#"
        INSERT OR REPLACE INTO sys_legacy_imports
            (id, source_path, source_hash, imported_at, record_count, domain, status, error_message)
        VALUES (?1, ?2, ?3, datetime('now'), 0, ?4, 'completed', NULL)
        "#,
        params![
            uuid::Uuid::new_v4().to_string(),
            source_path,
            "json",
            domain
        ],
    )
    .map_err(|source| ImportError::Sqlite {
        path: PathBuf::from("dbflux.db"),
        source,
    })?;

    Ok(())
}

// ============================================================================
// Config.json RPC Services Import
// ============================================================================

/// Deserializes config.json RPC services section.
/// The JSON format uses a Map with socket_id as keys:
/// {
///   "rpc_services": {
///     "socket_id": { "command": "...", "args": [...], "env": {...}, "timeout_ms": 30000 }
///   }
/// }
#[derive(Debug, Deserialize)]
struct ConfigJson {
    #[serde(default)]
    rpc_services: Option<std::collections::HashMap<String, RpcServiceConfig>>,
}

/// Individual RPC service entry from config.json.
/// socket_id is the map key, not a field.
#[derive(Debug, Deserialize)]
struct RpcServiceConfig {
    #[serde(default)]
    enabled: Option<bool>,

    #[serde(default)]
    command: Option<String>,

    #[serde(default)]
    args: Option<Vec<String>>,

    #[serde(default)]
    env: Option<std::collections::HashMap<String, String>>,

    #[serde(default)]
    timeout_ms: Option<i64>,
}

/// Imports RPC services from config.json into the unified database.
///
/// This function reads the legacy config.json file (which uses a Map structure
/// with socket_id as keys) and populates the cfg_services, cfg_service_args,
/// and cfg_service_env tables.
fn import_rpc_services_from_config_json(
    unified_conn: &Connection,
    config_json_path: &Path,
) -> Result<(), ImportError> {
    info!(
        "Importing RPC services from: {}",
        config_json_path.display()
    );

    let content = fs::read_to_string(config_json_path).map_err(|source| ImportError::Io {
        path: config_json_path.to_path_buf(),
        source,
    })?;

    let config: ConfigJson = serde_json::from_str(&content)
        .map_err(|e| ImportError::Failed(format!("failed to parse config.json: {}", e)))?;

    let Some(services) = config.rpc_services else {
        info!("No rpc_services in config.json, skipping");
        return Ok(());
    };

    let tx = unified_conn
        .unchecked_transaction()
        .map_err(|source| ImportError::Sqlite {
            path: PathBuf::from("dbflux.db"),
            source,
        })?;

    let mut _count = 0;
    for (socket_id, service) in services {
        let existing: Option<String> = tx
            .query_row(
                "SELECT socket_id FROM cfg_services WHERE socket_id = ?1",
                [&socket_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| ImportError::Sqlite {
                path: PathBuf::from("dbflux.db"),
                source,
            })?;

        if existing.is_some() {
            continue;
        }

        let now = chrono::Utc::now().to_rfc3339();
        tx.execute(
            r#"
            INSERT INTO cfg_services (socket_id, enabled, command, startup_timeout_ms, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                socket_id,
                service.enabled.unwrap_or(true) as i32,
                service.command,
                service.timeout_ms,
                now,
                now
            ],
        )
        .map_err(|source| ImportError::Sqlite {
            path: PathBuf::from("dbflux.db"),
            source,
        })?;

        // Import args
        if let Some(args) = service.args {
            for (position, value) in args.into_iter().enumerate() {
                tx.execute(
                    r#"
                    INSERT INTO cfg_service_args (id, service_id, position, value)
                    VALUES (?1, ?2, ?3, ?4)
                    "#,
                    params![
                        uuid::Uuid::new_v4().to_string(),
                        socket_id,
                        position as i32,
                        value
                    ],
                )
                .map_err(|source| ImportError::Sqlite {
                    path: PathBuf::from("dbflux.db"),
                    source,
                })?;
            }
        }

        // Import env
        if let Some(env) = service.env {
            for (key, value) in env {
                tx.execute(
                    r#"
                    INSERT INTO cfg_service_env (id, service_id, key, value)
                    VALUES (?1, ?2, ?3, ?4)
                    "#,
                    params![uuid::Uuid::new_v4().to_string(), socket_id, key, value],
                )
                .map_err(|source| ImportError::Sqlite {
                    path: PathBuf::from("dbflux.db"),
                    source,
                })?;
            }
        }

        _count += 1;
    }

    tx.commit().map_err(|source| ImportError::Sqlite {
        path: PathBuf::from("dbflux.db"),
        source,
    })?;

    info!("Imported {} RPC services from config.json", _count);
    Ok(())
}

// ============================================================================
// Backup
// ============================================================================

/// Creates a backup of a legacy file by renaming it with a .migrationbackup extension.
fn backup_legacy_file(path: &Path, name: &str) -> Result<PathBuf, ImportError> {
    let backup_dir = path.parent().unwrap().join("backup");
    fs::create_dir_all(&backup_dir).map_err(|source| ImportError::Io {
        path: backup_dir.clone(),
        source,
    })?;

    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let backup_name = format!("{}.{}.migrationbackup", name, timestamp);
    let backup_path = backup_dir.join(&backup_name);

    fs::rename(path, &backup_path).map_err(|source| ImportError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    info!("Backed up {} to {}", name, backup_path.display());
    Ok(backup_path)
}

// ============================================================================
// Helpers
// ============================================================================

fn open_unified_db(path: &Path) -> Result<Connection, ImportError> {
    if !path.exists() {
        return Err(ImportError::Failed(format!(
            "unified database does not exist at: {}",
            path.display()
        )));
    }

    Connection::open(path).map_err(|source| ImportError::Sqlite {
        path: path.to_path_buf(),
        source,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Test helper: creates a unified dbflux.db with the initial schema.
    fn create_unified_db(temp_dir: &TempDir) -> PathBuf {
        let unified_path = temp_dir.path().join("dbflux.db");
        let conn = rusqlite::Connection::open(&unified_path).unwrap();

        // Run the initial migration to create all tables
        let registry = crate::migrations::MigrationRegistry::new();
        registry.run_all(&conn).unwrap();

        // Also create the sys_legacy_imports tracking table (included in initial schema)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sys_legacy_imports (
                id TEXT PRIMARY KEY,
                source_path TEXT NOT NULL,
                source_hash TEXT NOT NULL UNIQUE,
                imported_at TEXT NOT NULL DEFAULT (datetime('now')),
                record_count INTEGER NOT NULL DEFAULT 0,
                domain TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'completed',
                error_message TEXT
            )",
            [],
        )
        .unwrap();

        unified_path
    }

    /// Test helper: creates a legacy config.json with RPC services.
    fn create_legacy_config_json(temp_dir: &TempDir) -> PathBuf {
        let config_path = temp_dir.path().join("config.json");
        let content = r#"{
            "rpc_services": {
                "test-socket": {
                    "enabled": true,
                    "command": "test-command",
                    "args": ["--flag"],
                    "env": {"TEST_VAR": "value"},
                    "timeout_ms": 30000
                }
            }
        }"#;
        std::fs::write(&config_path, content).unwrap();
        config_path
    }

    #[test]
    fn test_import_idempotent() {
        let temp_dir = TempDir::new().unwrap();

        // Create unified db and legacy config.json
        let unified_path = create_unified_db(&temp_dir);
        let config_json_path = create_legacy_config_json(&temp_dir);

        let legacy_paths = LegacyPaths {
            config_json: config_json_path,
            unified_db: unified_path.clone(),
        };

        // First import
        let result1 = import_legacy_databases(&unified_path, &legacy_paths).unwrap();
        assert!(
            result1.rpc_services_imported,
            "config.json should be imported first time"
        );

        // Check sys_legacy_imports has record
        let conn = rusqlite::Connection::open(&unified_path).unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sys_legacy_imports WHERE status = 'completed'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "should have 1 import record after first import");

        // Second import - should skip
        let result2 = import_legacy_databases(&unified_path, &legacy_paths).unwrap();
        assert!(
            !result2.rpc_services_imported,
            "config.json should be skipped second time"
        );

        // Count should still be 1 (no new records)
        let count2: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sys_legacy_imports WHERE status = 'completed'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count2, 1,
            "should still have 1 import record after second import (idempotent)"
        );
    }

    #[test]
    fn test_import_handles_missing_config_json() {
        let temp_dir = TempDir::new().unwrap();

        // Create unified db only - no config.json exists
        let unified_path = create_unified_db(&temp_dir);

        let legacy_paths = LegacyPaths {
            config_json: temp_dir.path().join("nonexistent_config.json"),
            unified_db: unified_path.clone(),
        };

        // Import should succeed with no imports
        let result = import_legacy_databases(&unified_path, &legacy_paths).unwrap();
        assert!(
            !result.any_imported(),
            "no databases should be imported when config.json doesn't exist"
        );
        assert!(
            !result.has_warnings(),
            "no warnings when legacy config.json doesn't exist"
        );
    }

    #[test]
    fn test_import_records_success() {
        let temp_dir = TempDir::new().unwrap();

        let unified_path = create_unified_db(&temp_dir);
        let config_json_path = create_legacy_config_json(&temp_dir);

        let legacy_paths = LegacyPaths {
            config_json: config_json_path,
            unified_db: unified_path.clone(),
        };

        let result = import_legacy_databases(&unified_path, &legacy_paths).unwrap();

        assert!(
            result.rpc_services_imported,
            "config.json should be imported"
        );

        // Verify sys_legacy_imports has correct record
        let conn = rusqlite::Connection::open(&unified_path).unwrap();

        let records: Vec<(String, String, String)> = {
            let mut stmt = conn
                .prepare("SELECT source_path, domain, status FROM sys_legacy_imports ORDER BY source_path")
                .unwrap();
            stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
        };

        assert_eq!(records.len(), 1, "should have 1 import record");
        assert!(records.iter().all(|(_, _, status)| status == "completed"));

        // Verify RPC service was imported
        let service_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM cfg_services WHERE socket_id = 'test-socket'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(service_count, 1, "RPC service should be imported");
    }
}
