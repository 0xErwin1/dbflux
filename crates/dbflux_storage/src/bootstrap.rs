use std::path::PathBuf;

use log::info;

use crate::error::StorageError;
use crate::paths;
use crate::sqlite;

/// Holds the open connections for every internal DBFlux database.
///
/// Obtained exclusively via [`initialize`] — callers never construct this
/// directly.
pub struct StorageRuntime {
    app_db_path: PathBuf,
}

impl StorageRuntime {
    /// Returns the path to the main application database.
    pub fn app_db_path(&self) -> &std::path::Path {
        &self.app_db_path
    }

    /// Opens a **new** connection to the application database.
    ///
    /// Each call creates a fresh `rusqlite::Connection`; the PRAGMA set is
    /// re-applied. This keeps `StorageRuntime` cheaply-cloneable (it only
    /// stores a path) and avoids sharing a single connection across threads.
    pub fn open_app_db(&self) -> Result<rusqlite::Connection, StorageError> {
        sqlite::open_database(&self.app_db_path)
    }
}

/// Bootstraps the internal storage layer.
///
/// This must be called once during application startup.  If it returns `Err`,
/// the application should abort — internal storage is mandatory.
///
/// What it does:
/// 1. Resolves `~/.config/dbflux/`, creating directories as needed.
/// 2. Opens (or creates) `dbflux.sqlite` with the standard PRAGMA set.
/// 3. Returns a [`StorageRuntime`] that can hand out connections on demand.
pub fn initialize() -> Result<StorageRuntime, StorageError> {
    let app_db_path = paths::app_db_path()?;
    info!("Internal storage path: {}", app_db_path.display());

    // Open and immediately drop — validates the file is a valid SQLite DB and
    // PRAGMAs can be applied.  The runtime will hand out fresh connections.
    let _conn = sqlite::open_database(&app_db_path)?;
    info!("Internal application database ready");

    Ok(StorageRuntime { app_db_path })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite;
    use std::path::Path;

    fn unique_temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "dbflux_storage_{}_{}_{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn initialize_succeeds_with_default_paths() {
        let runtime = initialize().expect("bootstrap should succeed");
        assert!(runtime.app_db_path().exists());
    }

    #[test]
    fn storage_runtime_opens_app_db() {
        let runtime = initialize().expect("bootstrap should succeed");
        let conn = runtime.open_app_db().expect("should open app db");

        // Quick sanity: we can query.
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 0);
    }

    #[test]
    fn temp_dir_bootstrap_creates_directories_and_database() {
        let dir = unique_temp_dir("bootstrap");
        assert!(!dir.exists());

        std::fs::create_dir_all(&dir).expect("should create temp dir");
        let db_path = dir.join("test.sqlite");

        let conn = sqlite::open_database(&db_path).expect("should open");
        assert!(db_path.exists());

        // Verify PRAGMAs applied.
        let mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn nested_directory_creation_succeeds() {
        let base = unique_temp_dir("nested");
        let dir = base.join("a").join("b").join("c");

        std::fs::create_dir_all(&dir).expect("nested dirs should be created");
        let db_path = dir.join("nested.sqlite");

        let conn = sqlite::open_database(&db_path).expect("should open in nested dir");
        assert!(db_path.exists());

        let _: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn open_database_fails_on_readonly_path() {
        let bad_path = Path::new("/proc/nonexistent_subdir/test.sqlite");
        let result = sqlite::open_database(bad_path);
        assert!(result.is_err(), "should fail on unwritable path");
    }

    #[test]
    fn open_database_fails_on_directory_instead_of_file() {
        let dir = unique_temp_dir("isdir");
        std::fs::create_dir_all(&dir).unwrap();

        let result = sqlite::open_database(&dir);
        assert!(result.is_err(), "should fail when path is a directory");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
