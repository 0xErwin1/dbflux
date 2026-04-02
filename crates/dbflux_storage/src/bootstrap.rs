use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use log::info;

use crate::artifacts::ArtifactStore;
use crate::error::StorageError;
use crate::legacy::LegacyImportResult;
use crate::migrations::MigrationRegistry;
use crate::paths;
use crate::repositories::audit::AuditRepository;
use crate::repositories::audit_settings::AuditSettingsRepository;
use crate::repositories::auth_profiles::AuthProfileRepository;
use crate::repositories::connection_profiles::ConnectionProfileRepository;
use crate::repositories::driver_overrides::DriverOverridesRepository;
use crate::repositories::driver_setting_values::DriverSettingValuesRepository;
use crate::repositories::driver_settings::DriverSettingsRepository;
use crate::repositories::general_settings::GeneralSettingsRepository;
use crate::repositories::governance_settings::GovernanceSettingsRepository;
use crate::repositories::hook_definitions::HookDefinitionRepository;
use crate::repositories::proxy_profiles::ProxyProfileRepository;
use crate::repositories::saved_filters::SavedFiltersRepository;
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

/// Holds the open connection for the unified DBFlux database.
///
/// The single `dbflux.db` database contains all domains (config, state, audit) using
/// domain-prefixed table names (`cfg_*`, `st_*`, `aud_*`, `sys_*`).
///
/// Obtained exclusively via [`initialize`] — callers never construct this
/// directly.
pub struct StorageRuntime {
    dbflux_db_path: PathBuf,
    dbflux_db: OwnedConnection,
    /// Manages filesystem artifact paths (scratch/shadow files).
    /// Content stays on disk; metadata about paths lives in dbflux.db.
    artifacts: ArtifactStore,
}

impl StorageRuntime {
    /// Creates a runtime pointing at the given unified database path.
    ///
    /// The caller is responsible for ensuring the parent directories exist.
    /// Migrations are applied on first open using the unified schema.
    #[allow(clippy::result_large_err)]
    pub fn for_path(dbflux_db_path: PathBuf) -> Result<Self, StorageError> {
        // Open and validate dbflux.db - apply migrations if needed
        let dbflux_conn = crate::sqlite::open_database(&dbflux_db_path)?;
        let registry = MigrationRegistry::new();
        registry.run_all(&dbflux_conn)?;
        info!("Unified database ready at {}", dbflux_db_path.display());

        // Initialize the artifact store using the parent directory of dbflux.db as data root.
        // This ensures test/temp runtimes use isolated directories instead of resolving
        // the real artifact root from the user home directory.
        let sessions_root = dbflux_db_path
            .parent()
            .map(|p| p.join("sessions"))
            .unwrap_or_else(|| PathBuf::from("sessions"));
        let artifacts = ArtifactStore::for_root(sessions_root.clone())?;
        info!(
            "Artifact store ready at {}",
            artifacts.root_path().display()
        );

        // Wrap connection in Arc for shared access
        #[allow(clippy::arc_with_non_send_sync)]
        let dbflux_db = Arc::new(dbflux_conn);

        Ok(StorageRuntime {
            dbflux_db_path,
            dbflux_db,
            artifacts,
        })
    }

    /// Creates a runtime with the database in a temporary directory.
    ///
    /// Useful for tests. The directory is created under `std::env::temp_dir()`
    /// with a unique name to avoid collisions between parallel test runs.
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

        let dbflux_db_path = temp_dir.join("dbflux.db");

        Self::for_path(dbflux_db_path)
    }

    /// Returns the path to the unified database.
    pub fn dbflux_db_path(&self) -> &Path {
        &self.dbflux_db_path
    }

    /// Opens a **new** connection to the unified database.
    ///
    /// Each call creates a fresh `rusqlite::Connection`; the PRAGMA set is
    /// re-applied. This keeps `StorageRuntime` cheaply-cloneable (it only
    /// stores a path) and avoids sharing a single connection across threads.
    pub fn open_dbflux_db(&self) -> Result<rusqlite::Connection, StorageError> {
        sqlite::open_database(&self.dbflux_db_path)
    }

    /// Returns an owned reference to the unified database connection.
    ///
    /// This is a cloneable reference stored in the Runtime.
    pub fn dbflux_db(&self) -> OwnedConnection {
        self.dbflux_db.clone()
    }

    // --- Repository convenience constructors ---
    //
    // All repositories now use the single unified database connection.
    // Config-domain and state-domain tables coexist in the same database
    // with domain-prefixed names (cfg_*, st_*).

    /// Creates a connection profile repository.
    pub fn connection_profiles(&self) -> ConnectionProfileRepository {
        ConnectionProfileRepository::new(self.dbflux_db())
    }

    /// Creates an auth profile repository.
    pub fn auth_profiles(&self) -> AuthProfileRepository {
        AuthProfileRepository::new(self.dbflux_db())
    }

    /// Creates a proxy profile repository.
    pub fn proxy_profiles(&self) -> ProxyProfileRepository {
        ProxyProfileRepository::new(self.dbflux_db())
    }

    /// Creates an SSH tunnel profile repository.
    pub fn ssh_tunnels(&self) -> SshTunnelProfileRepository {
        SshTunnelProfileRepository::new(self.dbflux_db())
    }

    /// Creates a hook definition repository.
    pub fn hook_definitions(&self) -> HookDefinitionRepository {
        HookDefinitionRepository::new(self.dbflux_db())
    }

    /// Creates a service repository.
    pub fn services(&self) -> ServiceRepository {
        ServiceRepository::new(self.dbflux_db())
    }

    /// Creates a driver settings repository.
    pub fn driver_settings(&self) -> DriverSettingsRepository {
        DriverSettingsRepository::new(self.dbflux_db())
    }

    /// Creates a settings repository.
    pub fn settings(&self) -> SettingsRepository {
        SettingsRepository::new(self.dbflux_db())
    }

    /// Creates a general settings repository.
    pub fn general_settings(&self) -> GeneralSettingsRepository {
        GeneralSettingsRepository::new(self.dbflux_db())
    }

    /// Creates a governance settings repository.
    pub fn governance_settings(&self) -> GovernanceSettingsRepository {
        GovernanceSettingsRepository::new(self.dbflux_db())
    }

    /// Creates a driver overrides repository.
    pub fn driver_overrides(&self) -> DriverOverridesRepository {
        DriverOverridesRepository::new(self.dbflux_db())
    }

    /// Creates a driver setting values repository.
    pub fn driver_setting_values(&self) -> DriverSettingValuesRepository {
        DriverSettingValuesRepository::new(self.dbflux_db())
    }

    // --- State repositories ---

    /// Creates a UI state repository.
    pub fn ui_state(&self) -> UiStateRepository {
        UiStateRepository::new(self.dbflux_db())
    }

    /// Creates a recent items repository.
    pub fn recent_items(&self) -> RecentItemsRepository {
        RecentItemsRepository::new(self.dbflux_db())
    }

    /// Creates a query history repository.
    pub fn query_history(&self) -> QueryHistoryRepository {
        QueryHistoryRepository::new(self.dbflux_db())
    }

    /// Creates a saved queries repository.
    pub fn saved_queries(&self) -> SavedQueriesRepository {
        SavedQueriesRepository::new(self.dbflux_db())
    }

    /// Creates a session repository.
    pub fn sessions(&self) -> SessionRepository {
        SessionRepository::new(self.dbflux_db())
    }

    /// Creates an audit repository.
    pub fn audit(&self) -> AuditRepository {
        use std::sync::Mutex;
        // Wrap the connection in a Mutex for thread-safe access
        let conn = self.open_dbflux_db().expect("should open dbflux db");
        AuditRepository::new(Arc::new(Mutex::new(conn)))
    }

    /// Creates an audit settings repository.
    pub fn audit_settings(&self) -> AuditSettingsRepository {
        AuditSettingsRepository::new(self.dbflux_db())
    }

    /// Creates a saved filters repository.
    pub fn saved_filters(&self) -> SavedFiltersRepository {
        use std::sync::Mutex;
        // Wrap the connection in a Mutex for thread-safe access
        let conn = self.open_dbflux_db().expect("should open dbflux db");
        SavedFiltersRepository::new(Arc::new(Mutex::new(conn)))
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
    ///
    /// Note: Legacy import now uses a single connection since config and state
    /// domains have been unified into dbflux.db.
    pub fn run_legacy_import(&self, config_dir: &Path, data_dir: &Path) -> LegacyImportResult {
        crate::legacy::run_legacy_import(
            self.dbflux_db.clone(),
            self.dbflux_db.clone(),
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
/// 1. Resolves `~/.local/share/dbflux/` (creating if needed).
/// 2. Opens (or creates) `dbflux.db` in the data directory with unified migrations applied.
/// 3. Runs legacy JSON import for any existing data (idempotent, restart-safe).
///    If import fails, the error is surfaced and the runtime is NOT returned.
/// 4. Returns a [`StorageRuntime`] that can hand out connections on demand.
#[allow(clippy::result_large_err)]
pub fn initialize() -> Result<StorageRuntime, StorageError> {
    let dbflux_db_path = paths::dbflux_db_path()?;
    let config_dir = paths::config_data_dir()?;
    let data_dir = paths::data_dir()?;

    info!("Unified database path: {}", dbflux_db_path.display());

    let runtime = StorageRuntime::for_path(dbflux_db_path)?;

    // Run legacy import if needed (idempotent via legacy_imports markers)
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
        // Use in-memory storage for tests to avoid polluting ~/.local/share/dbflux
        let runtime = StorageRuntime::in_memory().expect("bootstrap should succeed");
        assert!(runtime.dbflux_db_path().exists());
    }

    #[test]
    fn storage_runtime_opens_unified_db() {
        // Use in-memory storage for tests to avoid polluting ~/.local/share/dbflux
        let runtime = StorageRuntime::in_memory().expect("bootstrap should succeed");
        let conn = runtime.open_dbflux_db().expect("should open dbflux db");

        // MigrationRegistry has run, so sys_migrations should have the initial migration
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sys_migrations WHERE name = '001_initial'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "001_initial migration should be recorded");
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
