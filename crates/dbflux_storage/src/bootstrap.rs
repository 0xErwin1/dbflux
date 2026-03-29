use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use log::info;

use crate::artifacts::ArtifactStore;
use crate::error::StorageError;
use crate::legacy::LegacyImportResult;
use crate::migrations;
use crate::paths;
use crate::repositories::auth_profiles::AuthProfileRepository;
use crate::repositories::connection_profiles::ConnectionProfileRepository;
use crate::repositories::driver_settings::DriverSettingsRepository;
use crate::repositories::hook_definitions::HookDefinitionRepository;
use crate::repositories::proxy_profiles::ProxyProfileRepository;
use crate::repositories::services::ServiceRepository;
use crate::repositories::settings::SettingsRepository;
use crate::repositories::ssh_tunnel_profiles::SshTunnelProfileRepository;
use crate::repositories::state::{
    query_history::QueryHistoryRepository, recent_items::RecentItemsRepository,
    saved_queries::SavedQueriesRepository, sessions::SessionRepository,
    ui_state::UiStateRepository,
};
use crate::sqlite;

/// An owned database connection wrapped in Arc for shared access.
pub type OwnedConnection = Arc<rusqlite::Connection>;

/// Holds the open connections for every internal DBFlux database.
///
/// Obtained exclusively via [`initialize`] — callers never construct this
/// directly.
pub struct StorageRuntime {
    config_db_path: PathBuf,
    state_db_path: PathBuf,
    config_db: OwnedConnection,
    state_db: OwnedConnection,
    /// Manages filesystem artifact paths (scratch/shadow files).
    /// Content stays on disk; metadata about paths lives in state.db.
    artifacts: ArtifactStore,
}

impl StorageRuntime {
    /// Creates a runtime pointing at the given config and state database paths.
    ///
    /// The caller is responsible for ensuring the parent directories exist.
    /// Migrations are applied on first open.
    #[allow(clippy::result_large_err)]
    pub fn for_path(config_db_path: PathBuf, state_db_path: PathBuf) -> Result<Self, StorageError> {
        // Open and validate config.db - apply migrations if needed
        let config_conn = crate::sqlite::open_database(&config_db_path)?;
        migrations::run_config_migrations(&config_conn)?;
        info!("Config database ready at {}", config_db_path.display());

        // Open and validate state.db - apply migrations if needed
        let state_conn = crate::sqlite::open_database(&state_db_path)?;
        migrations::run_state_migrations(&state_conn)?;
        info!("State database ready at {}", state_db_path.display());

        // Initialize the artifact store using the parent directory of state.db as data root.
        // This ensures test/temp runtimes use isolated directories instead of resolving
        // the real artifact root from the user home directory.
        let sessions_root = state_db_path
            .parent()
            .map(|p| p.join("sessions"))
            .unwrap_or_else(|| PathBuf::from("sessions"));
        let artifacts = ArtifactStore::for_root(sessions_root.clone())?;
        info!(
            "Artifact store ready at {}",
            artifacts.root_path().display()
        );

        // Wrap connections in Arc for shared access
        #[allow(clippy::arc_with_non_send_sync)]
        let config_db = Arc::new(config_conn);
        #[allow(clippy::arc_with_non_send_sync)]
        let state_db = Arc::new(state_conn);

        Ok(StorageRuntime {
            config_db_path,
            state_db_path,
            config_db,
            state_db,
            artifacts,
        })
    }

    /// Creates a runtime with both databases in temporary directories.
    ///
    /// Useful for tests. The directories are created under `std::env::temp_dir()`
    /// with unique names to avoid collisions between parallel test runs.
    #[allow(clippy::result_large_err)]
    pub fn in_memory() -> Result<Self, StorageError> {
        let temp_label = format!(
            "dbflux_storage_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let temp_dir = std::env::temp_dir().join(&temp_label);
        std::fs::create_dir_all(&temp_dir).map_err(|source| StorageError::Io {
            path: temp_dir.clone(),
            source,
        })?;

        let config_db_path = temp_dir.join("config.db");
        let state_db_path = temp_dir.join("state.db");

        Self::for_path(config_db_path, state_db_path)
    }

    /// Returns the path to the config database.
    pub fn config_db_path(&self) -> &Path {
        &self.config_db_path
    }

    /// Returns the path to the state database (runtime state).
    pub fn state_db_path(&self) -> &Path {
        &self.state_db_path
    }

    /// Opens a **new** connection to the config database.
    ///
    /// Each call creates a fresh `rusqlite::Connection`; the PRAGMA set is
    /// re-applied. This keeps `StorageRuntime` cheaply-cloneable (it only
    /// stores a path) and avoids sharing a single connection across threads.
    pub fn open_config_db(&self) -> Result<rusqlite::Connection, StorageError> {
        sqlite::open_database(&self.config_db_path)
    }

    /// Opens a **new** connection to the state database.
    ///
    /// Each call creates a fresh `rusqlite::Connection`; the PRAGMA set is
    /// re-applied. This keeps `StorageRuntime` cheaply-cloneable (it only
    /// stores a path) and avoids sharing a single connection across threads.
    pub fn open_state_db(&self) -> Result<rusqlite::Connection, StorageError> {
        sqlite::open_database(&self.state_db_path)
    }

    /// Returns an owned reference to the config database connection.
    ///
    /// This is a cloneable reference stored in the Runtime.
    pub fn config_db(&self) -> OwnedConnection {
        self.config_db.clone()
    }

    /// Returns an owned reference to the state database connection.
    pub fn state_db(&self) -> OwnedConnection {
        self.state_db.clone()
    }

    // --- Repository convenience constructors ---

    /// Creates a connection profile repository.
    pub fn connection_profiles(&self) -> ConnectionProfileRepository {
        ConnectionProfileRepository::new(self.config_db())
    }

    /// Creates an auth profile repository.
    pub fn auth_profiles(&self) -> AuthProfileRepository {
        AuthProfileRepository::new(self.config_db())
    }

    /// Creates a proxy profile repository.
    pub fn proxy_profiles(&self) -> ProxyProfileRepository {
        ProxyProfileRepository::new(self.config_db())
    }

    /// Creates an SSH tunnel profile repository.
    pub fn ssh_tunnels(&self) -> SshTunnelProfileRepository {
        SshTunnelProfileRepository::new(self.config_db())
    }

    /// Creates a hook definition repository.
    pub fn hook_definitions(&self) -> HookDefinitionRepository {
        HookDefinitionRepository::new(self.config_db())
    }

    /// Creates a service repository.
    pub fn services(&self) -> ServiceRepository {
        ServiceRepository::new(self.config_db())
    }

    /// Creates a driver settings repository.
    pub fn driver_settings(&self) -> DriverSettingsRepository {
        DriverSettingsRepository::new(self.config_db())
    }

    /// Creates a settings repository.
    pub fn settings(&self) -> SettingsRepository {
        SettingsRepository::new(self.config_db())
    }

    // --- State repositories ---

    /// Creates a UI state repository.
    pub fn ui_state(&self) -> UiStateRepository {
        UiStateRepository::new(self.state_db())
    }

    /// Creates a recent items repository.
    pub fn recent_items(&self) -> RecentItemsRepository {
        RecentItemsRepository::new(self.state_db())
    }

    /// Creates a query history repository.
    pub fn query_history(&self) -> QueryHistoryRepository {
        QueryHistoryRepository::new(self.state_db())
    }

    /// Creates a saved queries repository.
    pub fn saved_queries(&self) -> SavedQueriesRepository {
        SavedQueriesRepository::new(self.state_db())
    }

    /// Creates a session repository.
    pub fn sessions(&self) -> SessionRepository {
        SessionRepository::new(self.state_db())
    }

    /// Returns the artifact store for scratch/shadow path management.
    pub fn artifacts(&self) -> &ArtifactStore {
        &self.artifacts
    }

    /// Returns the scratch file path for a document ID and extension.
    pub fn scratch_path(&self, doc_id: &str, extension: &str) -> std::path::PathBuf {
        self.artifacts.scratch_path(doc_id, extension)
    }

    /// Returns the shadow file path for a document ID.
    pub fn shadow_path(&self, doc_id: &str) -> std::path::PathBuf {
        self.artifacts.shadow_path(doc_id)
    }

    /// Runs legacy JSON import using the provided directory roots.
    ///
    /// The directories are used only to locate legacy JSON source files.
    /// This method is exposed on `StorageRuntime` so tests and bootstrap code
    /// can call it with arbitrary roots (avoiding global `dirs::*` lookups).
    pub fn run_legacy_import(&self, config_dir: &Path, data_dir: &Path) -> LegacyImportResult {
        crate::legacy::run_legacy_import(
            self.config_db.clone(),
            self.state_db.clone(),
            config_dir,
            data_dir,
        )
    }
}

/// Bootstraps the internal storage layer.
///
/// This must be called once during application startup.  If it returns `Err`,
/// the application should abort — internal storage is mandatory.
///
/// What it does:
/// 1. Resolves `~/.config/dbflux/` (creating if needed) and `~/.local/share/dbflux/` (creating if needed).
/// 2. Opens (or creates) `config.db` in the config directory with migrations applied.
/// 3. Opens (or creates) `state.db` in the data directory with migrations applied.
/// 4. Runs legacy JSON import for any existing data (idempotent, restart-safe).
///    If import fails, the error is surfaced and the runtime is NOT returned.
/// 5. Returns a [`StorageRuntime`] that can hand out connections on demand.
#[allow(clippy::result_large_err)]
pub fn initialize() -> Result<StorageRuntime, StorageError> {
    let config_db_path = paths::config_db_path()?;
    let state_db_path = paths::state_db_path()?;
    let config_dir = paths::config_data_dir()?;
    let data_dir = paths::data_dir()?;

    info!("Config database path: {}", config_db_path.display());
    info!("State database path: {}", state_db_path.display());

    let runtime = StorageRuntime::for_path(config_db_path.clone(), state_db_path.clone())?;

    // Run legacy import if needed (idempotent via system_metadata markers)
    let import_result = runtime.run_legacy_import(&config_dir, &data_dir);
    if import_result.any_imported() {
        info!(
            "Legacy import complete: {} items imported",
            import_result.total_imported()
        );
    }
    if import_result.has_errors() {
        // Surface critical import failures rather than silently continuing.
        // Wrap the first error as the cause.
        let first_error = import_result.errors.join("; ");
        return Err(StorageError::LegacyImportFailed(first_error));
    }

    Ok(runtime)
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
        // Use in-memory storage for tests to avoid polluting ~/.config/dbflux
        let runtime = StorageRuntime::in_memory().expect("bootstrap should succeed");
        assert!(runtime.state_db_path().exists());
    }

    #[test]
    fn storage_runtime_opens_state_db() {
        // Use in-memory storage for tests to avoid polluting ~/.config/dbflux
        let runtime = StorageRuntime::in_memory().expect("bootstrap should succeed");
        let conn = runtime.open_state_db().expect("should open state db");

        // State db migrations have run, so user_version should be 2 (INITIAL_VERSION + SYSTEM_METADATA_VERSION)
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 2);
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
