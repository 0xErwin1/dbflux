//! Diagnostics API for DBFlux internal storage.
//!
//! Provides reporting for storage paths, integrity status, and migration/import
//! status. Intended for debugging, support, and Settings UI integration.

use std::path::{Path, PathBuf};

use crate::migrations;
use crate::paths;

/// Complete diagnostics report for all DBFlux internal storage.
#[derive(Debug, Clone)]
pub struct DiagnosticsReport {
    pub config_db: DatabaseDiagnostics,
    pub state_db: DatabaseDiagnostics,
    pub artifact_store: ArtifactDiagnostics,
    pub legacy_files: Vec<LegacyFileStatus>,
    pub overall_status: OverallStatus,
}

/// Per-database diagnostics.
#[derive(Debug, Clone)]
pub struct DatabaseDiagnostics {
    pub path: PathBuf,
    pub exists: bool,
    pub size_bytes: Option<u64>,
    pub integrity_ok: bool,
    pub schema_version: Option<u32>,
    pub migration_count: usize,
    pub table_counts: Vec<TableCount>,
}

/// Artifact store diagnostics.
#[derive(Debug, Clone)]
pub struct ArtifactDiagnostics {
    pub root_path: PathBuf,
    pub exists: bool,
    pub total_files: Option<usize>,
    pub scratch_count: Option<usize>,
    pub shadow_count: Option<usize>,
}

/// Status of a legacy JSON file.
#[derive(Debug, Clone)]
pub struct LegacyFileStatus {
    pub path: PathBuf,
    pub exists: bool,
    pub size_bytes: Option<u64>,
    pub imported: bool,
}

/// Overall storage health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverallStatus {
    /// All databases present, integrity checks pass.
    Healthy,
    /// Minor issues (missing legacy files, non-critical warnings).
    Warning,
    /// Serious issues (missing databases, integrity failures).
    Unhealthy,
}

/// Row count for a single table.
#[derive(Debug, Clone)]
pub struct TableCount {
    pub name: String,
    pub count: i64,
}

/// Runs diagnostics with explicit paths, bypassing `dirs::*` lookups.
///
/// This is useful for testing and for callers that already have paths resolved.
pub fn run_diagnostics_for_paths(
    config_db_path: &Path,
    state_db_path: &Path,
    config_dir: &Path,
    data_dir: &Path,
) -> DiagnosticsReport {
    let config_db = collect_config_db_diagnostics(config_db_path);
    let state_db = collect_state_db_diagnostics(state_db_path);
    let artifact_store = collect_artifact_diagnostics(data_dir);
    let legacy_files = collect_legacy_file_statuses(config_dir, data_dir);

    let overall_status = compute_overall_status(&config_db, &state_db, &artifact_store);

    DiagnosticsReport {
        config_db,
        state_db,
        artifact_store,
        legacy_files,
        overall_status,
    }
}

/// Runs a complete diagnostics report using the default runtime paths.
///
/// This function resolves paths via `paths::config_db_path()`, `paths::state_db_path()`,
/// and `paths::data_dir()`. If any path is unavailable, the report includes
/// `<unavailable>` placeholders rather than panicking.
pub fn run_diagnostics() -> DiagnosticsReport {
    let config_db_path = match paths::config_db_path() {
        Ok(p) => p,
        Err(_) => PathBuf::from("<unavailable>"),
    };
    let state_db_path = match paths::state_db_path() {
        Ok(p) => p,
        Err(_) => PathBuf::from("<unavailable>"),
    };
    let data_dir_path = match paths::data_dir() {
        Ok(p) => p,
        Err(_) => PathBuf::from("<unavailable>"),
    };

    run_diagnostics_for_paths(
        &config_db_path,
        &state_db_path,
        config_db_path.parent().unwrap(),
        &data_dir_path,
    )
}

/// Collects diagnostics for the config database.
fn collect_config_db_diagnostics(path: &Path) -> DatabaseDiagnostics {
    let exists = path.exists();
    let size_bytes = exists
        .then(|| std::fs::metadata(path).ok())
        .flatten()
        .map(|m| m.len());

    let (integrity_ok, schema_version, migration_count, table_counts) = if exists {
        match crate::sqlite::open_database(path) {
            Ok(conn) => {
                let integrity_ok = migrations::verify_integrity(&conn).unwrap_or(false);
                // Config db now uses name-based migrations via the `migrations` table.
                // user_version is no longer used for config db (kept for compatibility).
                let schema_version = conn
                    .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
                    .ok();
                // Use `migrations` table for migration count (name-based tracking)
                let migration_count = conn
                    .query_row("SELECT COUNT(*) FROM migrations", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap_or(0) as usize;
                let table_counts = collect_table_counts(&conn);
                (integrity_ok, schema_version, migration_count, table_counts)
            }
            Err(_) => (false, None, 0, Vec::new()),
        }
    } else {
        (false, None, 0, Vec::new())
    };

    DatabaseDiagnostics {
        path: path.to_path_buf(),
        exists,
        size_bytes,
        integrity_ok,
        schema_version,
        migration_count,
        table_counts,
    }
}

/// Collects diagnostics for the state database.
fn collect_state_db_diagnostics(path: &Path) -> DatabaseDiagnostics {
    let exists = path.exists();
    let size_bytes = exists
        .then(|| std::fs::metadata(path).ok())
        .flatten()
        .map(|m| m.len());

    let (integrity_ok, schema_version, migration_count, table_counts) = if exists {
        match crate::sqlite::open_database(path) {
            Ok(conn) => {
                let integrity_ok = migrations::verify_integrity(&conn).unwrap_or(false);
                let schema_version = conn
                    .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
                    .ok();
                let migration_count = conn
                    .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap_or(0) as usize;
                let table_counts = collect_table_counts(&conn);
                (integrity_ok, schema_version, migration_count, table_counts)
            }
            Err(_) => (false, None, 0, Vec::new()),
        }
    } else {
        (false, None, 0, Vec::new())
    };

    DatabaseDiagnostics {
        path: path.to_path_buf(),
        exists,
        size_bytes,
        integrity_ok,
        schema_version,
        migration_count,
        table_counts,
    }
}

/// Collects diagnostics for the artifact store.
fn collect_artifact_diagnostics(data_dir: &Path) -> ArtifactDiagnostics {
    // Artifact store root is <data_dir>/sessions/ (data_dir already includes "dbflux" suffix)
    let sessions_dir = data_dir.join("sessions");
    let exists = sessions_dir.is_dir();

    let (total_files, scratch_count, shadow_count) = if exists {
        let entries: Vec<_> = std::fs::read_dir(&sessions_dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.file_type().map(|ft| ft.is_file()).unwrap_or(false))
            .collect();
        let total = entries.len();
        let scratch = entries
            .iter()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".sql"))
            .count();
        let shadow = entries
            .iter()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".shadow"))
            .count();
        (Some(total), Some(scratch), Some(shadow))
    } else {
        (None, None, None)
    };

    ArtifactDiagnostics {
        root_path: sessions_dir,
        exists,
        total_files,
        scratch_count,
        shadow_count,
    }
}

/// Collects status for all legacy JSON files.
fn collect_legacy_file_statuses(config_dir: &Path, data_dir: &Path) -> Vec<LegacyFileStatus> {
    let legacy_files = vec![
        (
            "profiles.json",
            config_dir.join("dbflux").join("profiles.json"),
        ),
        (
            "auth_profiles.json",
            config_dir.join("dbflux").join("auth_profiles.json"),
        ),
        (
            "proxies.json",
            config_dir.join("dbflux").join("proxies.json"),
        ),
        (
            "ssh_tunnels.json",
            config_dir.join("dbflux").join("ssh_tunnels.json"),
        ),
        (
            "history.json",
            config_dir.join("dbflux").join("history.json"),
        ),
        (
            "saved_queries.json",
            config_dir.join("dbflux").join("saved_queries.json"),
        ),
        (
            "recent_files.json",
            config_dir.join("dbflux").join("recent_files.json"),
        ),
        ("state.json", data_dir.join("dbflux").join("state.json")),
    ];

    legacy_files
        .into_iter()
        .map(|(_name, path)| {
            let exists = path.exists();
            let size_bytes = exists
                .then(|| std::fs::metadata(&path).ok())
                .flatten()
                .map(|m| m.len());

            LegacyFileStatus {
                path,
                exists,
                size_bytes,
                // `imported` is true if the file was NOT found (already imported/migrated)
                // or if it never existed. We can't know for sure from here,
                // so we just report existence.
                imported: !exists,
            }
        })
        .collect()
}

/// Computes overall status from component diagnostics.
fn compute_overall_status(
    config_db: &DatabaseDiagnostics,
    state_db: &DatabaseDiagnostics,
    artifacts: &ArtifactDiagnostics,
) -> OverallStatus {
    // Unhealthy if either database is missing or has integrity failure
    if !config_db.exists || !state_db.exists {
        return OverallStatus::Unhealthy;
    }

    if !config_db.integrity_ok || !state_db.integrity_ok {
        return OverallStatus::Unhealthy;
    }

    // Warning if state is healthy but artifact store is missing
    if !artifacts.exists {
        return OverallStatus::Warning;
    }

    OverallStatus::Healthy
}

/// Collects row counts for all user tables in a database.
fn collect_table_counts(conn: &rusqlite::Connection) -> Vec<TableCount> {
    // Use pragma table_list with fallback to sqlite_master for older SQLite versions
    let table_names: Vec<String> =
        match conn.pragma_query_value(None, "table_list", |row| row.get::<_, String>(2)) {
            Ok(json_str) => {
                // table_list returns: name, type, sql, ncol, wr, strict, rows
                // The 'name' column is at index 2
                // Parse as array of objects [{"name": "foo", ...}]
                let parsed: Vec<serde_json::Value> =
                    serde_json::from_str(&json_str).unwrap_or_default();
                parsed
                    .into_iter()
                    .filter_map(|v| v.get("name").and_then(|n| n.as_str()).map(String::from))
                    .collect()
            }
            Err(_) => {
                // Fallback: query sqlite_master directly
                let mut stmt = match conn.prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            ) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
                let rows = match stmt.query_map([], |row| row.get::<_, String>(0)) {
                    Ok(r) => r,
                    Err(_) => return Vec::new(),
                };
                let mut names = Vec::new();
                for name in rows.flatten() {
                    names.push(name);
                }
                names
            }
        };

    let user_tables: Vec<String> = table_names
        .into_iter()
        .filter(|name| !name.starts_with("sqlite_") && name != "schema_migrations")
        .collect();

    let mut counts = Vec::new();
    for table in user_tables {
        if let Ok(count) =
            conn.query_row(&format!("SELECT COUNT(*) FROM \"{}\"", table), [], |row| {
                row.get::<_, i64>(0)
            })
        {
            counts.push(TableCount { name: table, count });
        }
    }

    counts
}

impl std::fmt::Display for OverallStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OverallStatus::Healthy => write!(f, "healthy"),
            OverallStatus::Warning => write!(f, "warning"),
            OverallStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_runs_without_panic() {
        // Use path-aware API with isolated temp roots to avoid touching ~/.config/dbflux
        let temp_dir = std::env::temp_dir().join(format!(
            "dbflux_diag_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let config_dir = temp_dir.join("config");
        let data_dir = temp_dir.join("data");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        // Run diagnostics with isolated roots
        let report = run_diagnostics_for_paths(
            &temp_dir.join("config.db"),
            &temp_dir.join("state.db"),
            &config_dir,
            &data_dir,
        );

        // Should always return a report, even if databases don't exist
        assert!(matches!(
            report.overall_status,
            OverallStatus::Healthy | OverallStatus::Warning | OverallStatus::Unhealthy
        ));

        // Config DB path should always be set
        assert!(!report.config_db.path.as_os_str().is_empty());
        assert!(!report.state_db.path.as_os_str().is_empty());

        // Legacy files should be listed
        assert!(!report.legacy_files.is_empty());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn overall_status_display() {
        assert_eq!(format!("{}", OverallStatus::Healthy), "healthy");
        assert_eq!(format!("{}", OverallStatus::Warning), "warning");
        assert_eq!(format!("{}", OverallStatus::Unhealthy), "unhealthy");
    }

    #[test]
    fn database_diagnostics_has_all_fields() {
        let diag = DatabaseDiagnostics {
            path: std::path::PathBuf::from("/test/path.db"),
            exists: true,
            size_bytes: Some(1024),
            integrity_ok: true,
            schema_version: Some(1),
            migration_count: 1,
            table_counts: vec![TableCount {
                name: "test_table".to_string(),
                count: 10,
            }],
        };

        assert!(diag.exists);
        assert_eq!(diag.size_bytes, Some(1024));
        assert!(diag.integrity_ok);
        assert_eq!(diag.schema_version, Some(1));
    }

    #[test]
    fn legacy_file_status_nonexistent() {
        let status = LegacyFileStatus {
            path: PathBuf::from("/nonexistent/file.json"),
            exists: false,
            size_bytes: None,
            imported: true, // file doesn't exist, so considered already imported
        };

        assert!(!status.exists);
        assert!(status.imported);
    }

    #[test]
    fn diagnostics_report_with_real_dbs() {
        // Test diagnostics with actual runtime-created databases using isolated paths
        let base = std::env::temp_dir().join(format!(
            "dbflux_diag_real_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&base).unwrap();
        let config_dir = base.join("config");
        let data_dir = base.join("data");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        let config_db = base.join("config.db");
        let state_db = base.join("state.db");

        // Create real databases
        let _conn = crate::sqlite::open_database(&config_db).unwrap();
        let _conn2 = crate::sqlite::open_database(&state_db).unwrap();
        crate::migrations::run_config_migrations(&_conn).unwrap();
        crate::migrations::run_state_migrations(&_conn2).unwrap();

        // Create sessions directory (artifact store root) so artifact diagnostics pass.
        // The sessions dir is data_dir.join("sessions"), matching collect_artifact_diagnostics.
        let sessions_dir = data_dir.join("sessions");
        std::fs::create_dir_all(&sessions_dir).unwrap();

        // Run diagnostics with real paths.
        // Note: config_dir and data_dir are passed as-is (they are already the storage root dirs).
        // The legacy files collection uses config_dir.join("dbflux/...") and data_dir.join("dbflux/..."),
        // but the test uses bare temp dirs without the "dbflux" subdirectory, so legacy file
        // diagnostics will show them as not-imported (file missing). That's fine — the important
        // check is that the DB and artifact store diagnostics work.
        let report = run_diagnostics_for_paths(&config_db, &state_db, &config_dir, &data_dir);

        // With real DBs that have passed migrations, status should be healthy
        assert!(report.config_db.exists);
        assert!(report.state_db.exists);
        assert!(report.config_db.integrity_ok);
        assert!(report.state_db.integrity_ok);
        assert!(matches!(report.overall_status, OverallStatus::Healthy));

        // Schema version for config is no longer meaningful (user_version not used).
        // Config now uses name-based migrations via the `migrations` table.
        // State still uses user_version-based migrations.
        assert_eq!(report.config_db.schema_version, Some(0)); // user_version is 0 for new install
        assert_eq!(report.state_db.schema_version, Some(3));

        // Migration count: new installations run all 3 config migrations (0001_initial,
        // 0004_connection_profiles_fk, 0005_governance_normalize_tool_policies).
        // State DB has 3 migrations (0001_initial + 0002_system_metadata + 0003_event_session_native_columns)
        assert_eq!(report.config_db.migration_count, 3);
        assert_eq!(report.state_db.migration_count, 3);

        let _ = std::fs::remove_dir_all(&base);
    }
}
