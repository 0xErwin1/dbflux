use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex, RwLock};

#[cfg(feature = "mysql")]
use dbflux_core::DbKind;
use dbflux_core::auth::DynAuthProvider;
use dbflux_core::values::DynSecretProvider;
use dbflux_core::{
    AccessKind, AuthProfileManager, CancelToken, ConnectionMcpGovernance,
    ConnectionMcpPolicyBinding, ConnectionProfile, DbDriver, DetachedProcessHandle, DriverKey,
    FormValues, GovernanceSettings, KeyringSecretStore, ProfileManager, RpcServiceKind,
    SecretManager, ServiceConfig, ValueRef,
};
use dbflux_ipc::{
    AUTH_PROVIDER_RPC_API_CONTRACT, BUILT_IN_SECRET_PROVIDER_IDS, RpcAuthProvider,
    RpcSecretProvider, SECRET_PROVIDER_RPC_API_CONTRACT,
};
use dbflux_mcp::{
    McpGovernanceService, McpRuntime, PolicyRoleDto, ToolPolicyDto, TrustedClientDto,
    builtin_policies, builtin_roles,
};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::paths as storage_paths;
use dbflux_storage::repositories::governance_settings::GovernanceSettingsRepository;
use dbflux_storage::sqlite as storage_sqlite;

use crate::connection_cache::{CachedConnection, ConnectionCache};
use crate::error_messages;

/// Shared ownership for an MCP detached hook process. The supervisor and the
/// connection teardown path both need to act on the same child handle.
pub struct SharedDetachedProcess {
    pub(crate) handle: StdMutex<DetachedProcessHandle>,
    pub(crate) finished: AtomicBool,
    pub(crate) terminated: AtomicBool,
}

#[derive(Default)]
pub struct DetachedProcessRegistry {
    processes: StdMutex<HashMap<String, Vec<Arc<SharedDetachedProcess>>>>,
    shutdown_cancel: CancelToken,
    pending_session_shutdowns: SessionShutdownRegistry,
}

impl DetachedProcessRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registry of drained connections whose session factories still need a
    /// shutdown attempt; see [`SessionShutdownRegistry`].
    pub(crate) fn pending_session_shutdowns(&self) -> &SessionShutdownRegistry {
        &self.pending_session_shutdowns
    }

    pub(crate) fn register(&self, connection_id: &str, processes: Vec<Arc<SharedDetachedProcess>>) {
        if processes.is_empty() {
            return;
        }

        let mut registered = match self.processes.lock() {
            Ok(registered) => registered,
            Err(poisoned) => poisoned.into_inner(),
        };
        registered
            .entry(connection_id.to_string())
            .or_default()
            .extend(processes);
    }

    pub(crate) fn take(&self, connection_id: &str) -> Vec<Arc<SharedDetachedProcess>> {
        let mut registered = match self.processes.lock() {
            Ok(registered) => registered,
            Err(poisoned) => poisoned.into_inner(),
        };
        registered.remove(connection_id).unwrap_or_default()
    }

    pub(crate) fn take_variants(
        &self,
        profile_id: &str,
    ) -> Vec<(String, Vec<Arc<SharedDetachedProcess>>)> {
        let mut registered = match self.processes.lock() {
            Ok(registered) => registered,
            Err(poisoned) => poisoned.into_inner(),
        };
        let prefix = format!("{profile_id}:");
        let mut keys: Vec<String> = registered
            .keys()
            .filter(|key| *key == profile_id || key.starts_with(&prefix))
            .cloned()
            .collect();
        keys.sort_unstable();
        keys.into_iter()
            .filter_map(|key| registered.remove(&key).map(|processes| (key, processes)))
            .collect()
    }

    pub(crate) fn connection_ids(&self) -> Vec<String> {
        let registered = match self.processes.lock() {
            Ok(registered) => registered,
            Err(poisoned) => poisoned.into_inner(),
        };
        registered
            .keys()
            .map(|key| {
                key.split_once(':')
                    .map_or(key.as_str(), |(profile_id, _)| profile_id)
            })
            .map(str::to_string)
            .collect()
    }

    pub(crate) fn shutdown_cancel_token(&self) -> CancelToken {
        self.shutdown_cancel.clone()
    }

    pub(crate) fn cancel_running_hooks(&self) {
        self.shutdown_cancel.cancel();
    }
}

impl SharedDetachedProcess {
    pub(crate) fn new(handle: DetachedProcessHandle) -> Self {
        Self {
            handle: StdMutex::new(handle),
            finished: AtomicBool::new(false),
            terminated: AtomicBool::new(false),
        }
    }
}

/// A cached connection drained from the cache whose execution session factory
/// has not been shut down yet.
pub struct PendingSessionShutdown {
    connection: Arc<CachedConnection>,
    attempted: AtomicBool,
}

impl PendingSessionShutdown {
    fn new(connection: Arc<CachedConnection>) -> Arc<Self> {
        Arc::new(Self {
            connection,
            attempted: AtomicBool::new(false),
        })
    }

    /// Returns the connection when this caller is the first to attempt the
    /// shutdown, so a shutdown already dispatched before a deadline expiry is
    /// never dispatched twice.
    pub(crate) fn claim_for_shutdown(&self) -> Option<Arc<CachedConnection>> {
        self.attempted
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
            .then(|| Arc::clone(&self.connection))
    }
}

/// Holds connections whose session factories still need a shutdown attempt.
///
/// Disconnect publishes its drained entries here before running hooks; a
/// shutdown that gets cancelled past its deadline can therefore still recover
/// the drained entries and attempt their session teardown instead of losing
/// the connections with the cancelled future.
#[derive(Default)]
pub struct SessionShutdownRegistry {
    pending: StdMutex<HashMap<String, Vec<Arc<PendingSessionShutdown>>>>,
}

impl SessionShutdownRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(&self, entries: &[(String, Arc<CachedConnection>)]) {
        if entries.is_empty() {
            return;
        }

        let mut pending = match self.pending.lock() {
            Ok(pending) => pending,
            Err(poisoned) => poisoned.into_inner(),
        };
        for (connection_key, connection) in entries {
            pending
                .entry(connection_key.clone())
                .or_default()
                .push(PendingSessionShutdown::new(Arc::clone(connection)));
        }
    }

    /// Removes the entries registered under exactly these cache keys.
    pub(crate) fn take_keys(&self, keys: &[String]) -> Vec<Arc<PendingSessionShutdown>> {
        let mut pending = match self.pending.lock() {
            Ok(pending) => pending,
            Err(poisoned) => poisoned.into_inner(),
        };
        keys.iter()
            .filter_map(|key| pending.remove(key))
            .flatten()
            .collect()
    }

    /// Removes the entries of a profile id and all its per-database variants.
    pub(crate) fn take_variants(&self, profile_id: &str) -> Vec<Arc<PendingSessionShutdown>> {
        let prefix = format!("{profile_id}:");
        let mut pending = match self.pending.lock() {
            Ok(pending) => pending,
            Err(poisoned) => poisoned.into_inner(),
        };
        let keys: Vec<String> = pending
            .keys()
            .filter(|key| *key == profile_id || key.starts_with(&prefix))
            .cloned()
            .collect();
        keys.into_iter()
            .filter_map(|key| pending.remove(&key))
            .flatten()
            .collect()
    }

    /// Removes every remaining entry; used by the shutdown final sweep.
    pub(crate) fn take_all(&self) -> Vec<Arc<PendingSessionShutdown>> {
        let mut pending = match self.pending.lock() {
            Ok(pending) => pending,
            Err(poisoned) => poisoned.into_inner(),
        };
        pending.drain().flat_map(|(_, entries)| entries).collect()
    }
}

/// All state loaded at startup that the server needs to handle requests.
/// This struct is Clone-able and uses Arc internally for shared state.
#[derive(Clone)]
pub struct ServerState {
    pub client_id: String,
    pub runtime: Arc<RwLock<McpRuntime>>,
    pub profile_manager: Arc<RwLock<ProfileManager>>,
    pub auth_profile_manager: Arc<RwLock<AuthProfileManager>>,
    pub driver_registry: Arc<HashMap<String, Arc<dyn DbDriver>>>,
    pub auth_provider_registry: Arc<HashMap<String, Arc<dyn DynAuthProvider>>>,
    /// External secret providers discovered from RPC services, registered into every
    /// connection resolver.
    pub secret_provider_registry: Arc<HashMap<String, Arc<dyn DynSecretProvider>>>,
    pub driver_settings: Arc<HashMap<DriverKey, FormValues>>,
    pub connection_cache: Arc<RwLock<ConnectionCache>>,
    pub connection_setup_lock: Arc<Mutex<HashMap<String, std::sync::Weak<Mutex<()>>>>>,
    pub detached_processes: Arc<DetachedProcessRegistry>,
    pub secret_manager: Arc<SecretManager>,
    pub mcp_enabled_by_default: bool,
    /// Path of the `dbflux.db` the profiles were loaded from.
    ///
    /// The read-only gate reads the profile flag from here on every call, so a
    /// profile switched to read-only in the GUI takes effect without restarting
    /// the server. `None` keeps the gate on the profiles loaded at startup.
    pub profile_store_path: Option<PathBuf>,
}

impl ServerState {
    /// Loads config and governance from disk, builds the driver registry,
    /// and returns a fully-initialized `ServerState`.
    ///
    /// `config_dir` is accepted for CLI compatibility but runtime state is loaded
    /// exclusively from `dbflux.db`.
    pub fn new(client_id: String, config_dir: Option<PathBuf>) -> Result<Self, String> {
        let storage_runtime = open_storage_runtime()?;
        let (profiles, profile_load_errors) = load_profiles(&storage_runtime)?;
        let stored_auth_profiles = load_auth_profiles(&storage_runtime)?;
        let services = dbflux_storage::load_service_configs(&storage_runtime);
        let (runtime, governance_settings) =
            build_runtime(&storage_runtime, config_dir.as_deref())?;

        // Validate that the client_id exists as a trusted client
        validate_client_id(&runtime, &client_id, config_dir.as_deref())?;

        let profile_manager =
            ProfileManager::with_profiles_and_errors(profiles, profile_load_errors);
        let driver_registry = build_driver_registry();
        let auth_provider_registry = build_auth_provider_registry(&services);
        let secret_provider_registry = build_secret_provider_registry(&services);
        let auth_profile_manager =
            build_auth_profile_manager(stored_auth_profiles, &auth_provider_registry);
        let driver_settings = Arc::new(load_driver_settings(&storage_runtime)?);
        let secret_manager = Arc::new(SecretManager::new(Box::new(KeyringSecretStore::new())));

        let state = ServerState {
            client_id,
            runtime: Arc::new(RwLock::new(runtime)),
            profile_manager: Arc::new(RwLock::new(profile_manager)),
            auth_profile_manager: Arc::new(RwLock::new(auth_profile_manager)),
            driver_registry: Arc::new(driver_registry),
            auth_provider_registry: Arc::new(auth_provider_registry),
            secret_provider_registry: Arc::new(secret_provider_registry),
            driver_settings,
            connection_cache: Arc::new(RwLock::new(ConnectionCache::new())),
            connection_setup_lock: Arc::new(Mutex::new(HashMap::new())),
            detached_processes: Arc::new(DetachedProcessRegistry::new()),
            secret_manager,
            mcp_enabled_by_default: governance_settings.mcp_enabled_by_default,
            profile_store_path: Some(storage_runtime.dbflux_db_path().to_path_buf()),
        };

        // Load connection policy assignments
        let runtime_clone = state.runtime.clone();
        let profile_manager_clone = state.profile_manager.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                load_connection_policy_assignments(runtime_clone, profile_manager_clone).await
            })
        })?;

        Ok(state)
    }
}

/// Mirrors `AppState::list_auth_profiles`: AWS profiles are not stored, they are
/// reflected from `~/.aws`, and connections reference their stable reflected ids.
fn build_auth_profile_manager(
    stored_profiles: Vec<dbflux_core::AuthProfile>,
    auth_provider_registry: &HashMap<String, Arc<dyn DynAuthProvider>>,
) -> AuthProfileManager {
    const AWS_REFLECTED_PROVIDER_IDS: &[&str] =
        &["aws-sso", "aws-sso-session", "aws-shared-credentials"];

    let mut profiles: Vec<dbflux_core::AuthProfile> = stored_profiles
        .into_iter()
        .filter(|profile| !AWS_REFLECTED_PROVIDER_IDS.contains(&profile.provider_id.as_str()))
        .collect();

    for provider in auth_provider_registry.values() {
        profiles.extend(provider.reflect_profiles());
    }

    AuthProfileManager::with_items(profiles, None, "auth profiles")
}

fn load_driver_settings(
    runtime: &StorageRuntime,
) -> Result<HashMap<DriverKey, FormValues>, String> {
    let overrides_repo = runtime.driver_overrides();
    let values_repo = runtime.driver_setting_values();

    let mut driver_keys = std::collections::BTreeSet::new();

    for entry in overrides_repo
        .all()
        .map_err(|e| format!("Failed to load driver overrides: {}", e))?
    {
        driver_keys.insert(entry.driver_key);
    }

    for entry in values_repo
        .all()
        .map_err(|e| format!("Failed to load driver setting values: {}", e))?
    {
        driver_keys.insert(entry.driver_key);
    }

    let mut settings = HashMap::new();

    for driver_key in driver_keys {
        let values = values_repo
            .get_for_driver(&driver_key)
            .map_err(|e| format!("Failed to load driver settings for {}: {}", driver_key, e))?;

        let mut form_values = FormValues::new();
        for value in values {
            if let Some(setting_value) = value.setting_value {
                form_values.insert(value.setting_key, setting_value);
            }
        }

        if !form_values.is_empty() {
            settings.insert(driver_key, form_values);
        }
    }

    Ok(settings)
}

fn build_runtime(
    storage_runtime: &StorageRuntime,
    config_dir: Option<&std::path::Path>,
) -> Result<(McpRuntime, GovernanceSettings), String> {
    // Use the unified dbflux.db path for audit service regardless of config_dir.
    let dbflux_db_path = storage_paths::dbflux_db_path()
        .map_err(|e| error_messages::config_error("resolve unified dbflux.db path", None, e))?;

    let audit_service = dbflux_audit::AuditService::new_sqlite(&dbflux_db_path).map_err(|e| {
        error_messages::config_error("initialize audit database", Some(&dbflux_db_path), e)
    })?;

    let mut runtime = McpRuntime::new(audit_service, pending_execution_store(storage_runtime));

    // Pass config_dir for CLI compatibility only; governance state is read from SQLite.
    let governance_settings = load_governance_into_runtime(&mut runtime, config_dir)?;

    // Drain startup events — governance load is not observable to callers.
    runtime.drain_events();

    Ok((runtime, governance_settings))
}

/// The pending execution queue shared with the DBFlux app through
/// `dbflux.db`, so a call a policy sends to approval shows up in the app's
/// Pending Approvals and an approval given there reaches this server.
fn pending_execution_store(
    storage_runtime: &StorageRuntime,
) -> Box<dyn dbflux_approval::PendingExecutionStore> {
    match storage_runtime.pending_executions() {
        Ok(store) => Box::new(store),
        Err(error) => {
            log::error!(
                "Failed to open the shared pending executions store; calls that need \
                 approval will be queued in memory and cannot be approved from the app: {error}"
            );
            Box::new(dbflux_approval::InMemoryPendingExecutionStore::default())
        }
    }
}

fn validate_client_id(
    runtime: &McpRuntime,
    client_id: &str,
    _config_dir: Option<&std::path::Path>,
) -> Result<(), String> {
    let clients = runtime
        .list_trusted_clients()
        .map_err(|e| format!("Failed to list trusted clients: {}", e))?;

    let client_exists = clients
        .iter()
        .any(|client| client.id == client_id && client.active);

    if !client_exists {
        let settings_db_path = storage_paths::dbflux_db_path()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|_| "~/.local/share/dbflux/dbflux.db".to_string());

        return Err(format!(
            "Client ID '{}' is not registered as a trusted client.\n\
             \n\
             To fix this:\n\
             1. Open DBFlux GUI and go to Settings → MCP → Clients\n\
             2. Add a new trusted client with ID '{}'\n\
             \n\
             Or insert a trusted client row into the DBFlux settings database:\n\
             {}\n\
             \n\
             Table: cfg_trusted_clients",
            client_id, client_id, settings_db_path
        ));
    }

    Ok(())
}

fn apply_governance_settings(
    runtime: &mut McpRuntime,
    governance: &GovernanceSettings,
) -> Result<(), String> {
    for client in &governance.trusted_clients {
        runtime
            .upsert_trusted_client_mut(TrustedClientDto {
                id: client.id.clone(),
                name: client.name.clone(),
                issuer: client.issuer.clone(),
                active: client.active,
            })
            .map_err(|e| format!("failed to load trusted client: {e}"))?;
    }

    for role in &governance.roles {
        runtime
            .upsert_role_mut(PolicyRoleDto {
                id: role.id.clone(),
                policy_ids: role.policy_ids.clone(),
            })
            .map_err(|e| format!("failed to load role: {e}"))?;
    }

    for policy in &governance.policies {
        runtime
            .upsert_policy_mut(ToolPolicyDto {
                id: policy.id.clone(),
                allowed_tools: policy.allowed_tools.clone(),
                allowed_classes: policy.allowed_classes.clone(),
                approval_classes: policy.approval_classes.clone(),
            })
            .map_err(|e| format!("failed to load policy: {e}"))?;
    }

    Ok(())
}

fn load_governance_into_runtime(
    runtime: &mut McpRuntime,
    config_dir: Option<&std::path::Path>,
) -> Result<GovernanceSettings, String> {
    for role in builtin_roles() {
        runtime
            .upsert_role_mut(role)
            .map_err(|e| format!("failed to load built-in role: {e}"))?;
    }

    for policy in builtin_policies() {
        runtime
            .upsert_policy_mut(policy)
            .map_err(|e| format!("failed to load built-in policy: {e}"))?;
    }

    let governance = load_governance_settings(config_dir)?;

    apply_governance_settings(runtime, &governance)?;

    Ok(governance)
}

fn open_config_db(_config_dir: Option<&std::path::Path>) -> Result<rusqlite::Connection, String> {
    // Always use the unified dbflux.db for governance settings.
    let path = storage_paths::dbflux_db_path()
        .map_err(|e| format!("Failed to resolve unified dbflux.db path: {}", e))?;

    storage_sqlite::open_database(&path)
        .map_err(|e| error_messages::config_error("open unified database", Some(&path), e))
}

fn open_storage_runtime() -> Result<StorageRuntime, String> {
    let dbflux_db_path = storage_paths::dbflux_db_path()
        .map_err(|e| error_messages::config_error("resolve unified dbflux.db path", None, e))?;

    StorageRuntime::for_path(dbflux_db_path.clone()).map_err(|e| {
        error_messages::config_error("open unified database", Some(&dbflux_db_path), e)
    })
}

fn load_auth_profiles(runtime: &StorageRuntime) -> Result<Vec<dbflux_core::AuthProfile>, String> {
    let repo = runtime.auth_profiles();

    repo.all()
        .map_err(|e| format!("Failed to load auth profiles: {}", e))?
        .into_iter()
        .map(|dto| {
            let id = uuid::Uuid::parse_str(&dto.id)
                .map_err(|e| format!("Invalid auth profile id '{}': {}", dto.id, e))?;
            let fields = repo.get_fields(&dto.id).map_err(|e| {
                format!("Failed to load auth profile fields for '{}': {}", dto.id, e)
            })?;

            Ok(dbflux_core::AuthProfile {
                id,
                name: dto.name,
                provider_id: dto.provider_id,
                fields,
                // Secret-kind fields live in the keyring, not in SQLite; the MCP
                // server lists persisted profile metadata and does not hydrate
                // them.
                secret_fields: std::collections::HashMap::new(),
                enabled: dto.enabled,
                read_only: false,
                dangling_origin: dto.dangling_origin,
            })
        })
        .collect()
}

fn load_profiles(
    runtime: &StorageRuntime,
) -> Result<(Vec<ConnectionProfile>, HashMap<uuid::Uuid, String>), String> {
    let repo = runtime.connection_profiles();
    let dtos = repo
        .all()
        .map_err(|e| format!("Failed to load connection profiles: {}", e))?;

    let mut profiles = Vec::with_capacity(dtos.len());
    let mut profile_load_errors = HashMap::new();

    for dto in dtos {
        let profile_id = dto.id.clone();
        let id = uuid::Uuid::parse_str(&profile_id)
            .map_err(|e| format!("Invalid connection profile id '{}': {}", profile_id, e))?;

        let driver_config = repo
            .driver_configs()
            .get_for_profile(&profile_id)
            .map_err(|e| format!("Failed to load driver config for '{}': {}", profile_id, e))?;

        let config = driver_config
            .and_then(|entry| entry.to_db_config())
            .or_else(|| {
                dto.kind
                    .as_deref()
                    .and_then(str_to_db_kind)
                    .map(default_db_config_for_kind)
            })
            .ok_or_else(|| {
                format!(
                    "Connection profile '{}' is missing a valid driver config",
                    profile_id
                )
            })?;

        let connection_settings = load_connection_settings(&repo, &profile_id)?;
        let value_refs = load_profile_value_refs(&repo, &profile_id)?;
        let access_kind = load_access_kind(&repo, &dto, &profile_id)?;
        let mcp_governance = load_profile_governance(&repo, &profile_id)?;

        let mut hook_load_error = None;
        let hooks = match repo.hooks().get_for_profile(&profile_id) {
            Ok(inline_hook_dtos) if inline_hook_dtos.is_empty() => None,
            Ok(inline_hook_dtos) => {
                match load_inline_hooks(&repo, &inline_hook_dtos, &profile_id) {
                    Ok(hooks) => Some(hooks),
                    Err(error) => {
                        hook_load_error = Some(error);
                        None
                    }
                }
            }
            Err(error) => {
                hook_load_error = Some(format!(
                    "Failed to load inline hooks for '{}': {}",
                    profile_id, error
                ));
                None
            }
        };
        let hook_bindings = if hook_load_error.is_none() {
            match repo.hook_bindings().get_for_profile(&profile_id) {
                Ok(hook_binding_dtos) if hook_binding_dtos.is_empty() => None,
                Ok(hook_binding_dtos) => {
                    match load_hook_bindings(&hook_binding_dtos, &profile_id) {
                        Ok(hook_bindings) => Some(hook_bindings),
                        Err(error) => {
                            hook_load_error = Some(error);
                            None
                        }
                    }
                }
                Err(error) => {
                    hook_load_error = Some(format!(
                        "Failed to load hook bindings for '{}': {}",
                        profile_id, error
                    ));
                    None
                }
            }
        } else {
            None
        };

        let mut profile = ConnectionProfile {
            id,
            name: dto.name,
            kind: dto.kind.as_deref().and_then(str_to_db_kind),
            driver_id: dto.driver_id,
            config,
            save_password: dto.save_password,
            settings_overrides: None,
            connection_settings,
            hooks,
            hook_bindings,
            proxy_profile_id: dto
                .proxy_profile_id
                .as_deref()
                .and_then(|value| uuid::Uuid::parse_str(value).ok()),
            auth_profile_id: dto
                .auth_profile_id
                .as_deref()
                .and_then(|value| uuid::Uuid::parse_str(value).ok()),
            value_refs,
            access_kind,
            mcp_governance,
            read_only_flag: dto.read_only,
            environment: None,
            navigator_view: dbflux_core::NavigatorView::Advanced,
            show_all_databases: true,
        };

        if hook_load_error.is_none()
            && let Some(bindings) = profile.hook_bindings.clone()
        {
            match load_bound_hook_definitions(runtime, &profile, &bindings) {
                Ok(definitions) => {
                    profile.hooks = Some(dbflux_core::ConnectionHooks::resolve_from_bindings(
                        &profile,
                        &definitions,
                    ));
                }
                Err(error) => hook_load_error = Some(error),
            }
            profile.hook_bindings = None;
        }

        if let Some(error) = hook_load_error {
            profile.hooks = None;
            profile.hook_bindings = None;
            profile_load_errors.insert(profile.id, error);
        }

        profiles.push(profile);
    }

    Ok((profiles, profile_load_errors))
}

fn inline_hook_kind(
    repo: &dbflux_storage::repositories::connection_profiles::ConnectionProfileRepository,
    dto: &dbflux_storage::repositories::connection_profile_hooks::ConnectionProfileHookDto,
    profile_id: &str,
) -> Result<dbflux_core::HookKind, String> {
    use dbflux_core::{HookKind, ScriptLanguage};

    match dto.hook_kind.as_str() {
        "command" => {
            let command = dto.command.clone().ok_or_else(|| {
                format!(
                    "Connection profile '{}' command hook '{}' has no command",
                    profile_id, dto.id
                )
            })?;
            let args = repo
                .hook_args()
                .get_for_hook(&dto.id)
                .map_err(|error| {
                    format!(
                        "Failed to load arguments for connection profile '{}' hook '{}': {}",
                        profile_id, dto.id, error
                    )
                })?
                .into_iter()
                .map(|argument| argument.value)
                .collect();
            Ok(HookKind::Command { command, args })
        }
        "script" => {
            let language = match dto.script_language.as_deref() {
                Some("bash" | "sh") => ScriptLanguage::Bash,
                Some("python") => ScriptLanguage::Python,
                Some(language) => {
                    return Err(format!(
                        "Connection profile '{}' hook '{}' has unsupported script language '{}'",
                        profile_id, dto.id, language
                    ));
                }
                None => {
                    return Err(format!(
                        "Connection profile '{}' script hook '{}' has no language",
                        profile_id, dto.id
                    ));
                }
            };
            Ok(HookKind::Script {
                language,
                source: load_script_source(
                    dto.script_source_type.as_deref(),
                    dto.script_content.as_deref(),
                    dto.script_path.as_deref(),
                    profile_id,
                    &dto.id,
                )?,
                interpreter: dto.script_interpreter.clone(),
            })
        }
        "lua" => Ok(HookKind::Lua {
            source: load_script_source(
                dto.lua_source_type.as_deref(),
                dto.lua_content.as_deref(),
                dto.lua_path.as_deref(),
                profile_id,
                &dto.id,
            )?,
            capabilities: dbflux_core::LuaCapabilities {
                logging: dto.lua_log,
                env_read: dto.lua_env_read,
                connection_metadata: dto.lua_conn_metadata,
                process_run: dto.lua_process_run,
            },
        }),
        kind => Err(format!(
            "Connection profile '{}' hook '{}' has unknown kind '{}'",
            profile_id, dto.id, kind
        )),
    }
}

fn load_inline_hooks(
    repo: &dbflux_storage::repositories::connection_profiles::ConnectionProfileRepository,
    dtos: &[dbflux_storage::repositories::connection_profile_hooks::ConnectionProfileHookDto],
    profile_id: &str,
) -> Result<dbflux_core::ConnectionHooks, String> {
    let mut hooks = dbflux_core::ConnectionHooks::default();
    let hook_envs = repo.hook_envs();

    for dto in dtos {
        let phase = parse_hook_phase(&dto.phase).ok_or_else(|| {
            format!(
                "Connection profile '{}' has hook '{}' with unknown phase '{}'",
                profile_id, dto.id, dto.phase
            )
        })?;
        let kind = inline_hook_kind(repo, dto, profile_id)?;
        let env = hook_envs
            .get_for_hook(&dto.id)
            .map_err(|error| {
                format!(
                    "Failed to load environment for connection profile '{}' hook '{}': {}",
                    profile_id, dto.id, error
                )
            })?
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect();
        let hook = connection_hook_from_fields(
            dto.enabled,
            kind,
            dto.cwd.as_deref(),
            env,
            dto.inherit_env,
            dto.env_denylist.clone(),
            dto.timeout_ms,
            &dto.execution_mode,
            dto.ready_signal.clone(),
            &dto.on_failure,
            profile_id,
            &dto.id,
        )?;
        hooks.phase_hooks_mut(phase).push(hook);
    }

    Ok(hooks)
}

fn load_hook_bindings(
    dtos: &[dbflux_storage::repositories::connection_profile_hook_bindings::ConnectionProfileHookBindingDto],
    profile_id: &str,
) -> Result<dbflux_core::ConnectionHookBindings, String> {
    let mut result = dbflux_core::ConnectionHookBindings::default();
    for phase in [
        "pre_connect",
        "post_connect",
        "pre_disconnect",
        "post_disconnect",
    ] {
        let mut bindings = dtos
            .iter()
            .filter(|binding| binding.phase == phase)
            .collect::<Vec<_>>();
        bindings.sort_by_key(|binding| binding.order_index);
        let hook_ids = bindings
            .into_iter()
            .map(|binding| binding.hook_id.clone())
            .collect();
        match phase {
            "pre_connect" => result.pre_connect = hook_ids,
            "post_connect" => result.post_connect = hook_ids,
            "pre_disconnect" => result.pre_disconnect = hook_ids,
            "post_disconnect" => result.post_disconnect = hook_ids,
            _ => unreachable!("only supported phases are iterated"),
        }
    }

    if let Some(binding) = dtos
        .iter()
        .find(|binding| parse_hook_phase(&binding.phase).is_none())
    {
        return Err(format!(
            "Connection profile '{}' has hook binding '{}' with unknown phase '{}'",
            profile_id, binding.id, binding.phase
        ));
    }

    Ok(result)
}

fn load_bound_hook_definitions(
    runtime: &StorageRuntime,
    profile: &ConnectionProfile,
    bindings: &dbflux_core::ConnectionHookBindings,
) -> Result<HashMap<String, dbflux_core::ConnectionHook>, String> {
    let definitions_repo = runtime.hook_definitions();
    let mut definitions = HashMap::new();

    for phase in [
        dbflux_core::HookPhase::PreConnect,
        dbflux_core::HookPhase::PostConnect,
        dbflux_core::HookPhase::PreDisconnect,
        dbflux_core::HookPhase::PostDisconnect,
    ] {
        for hook_id in bindings.phase_bindings(phase) {
            if definitions.contains_key(hook_id) {
                continue;
            }

            let dto = definitions_repo
                .get(hook_id)
                .map_err(|error| {
                    format!(
                        "Failed to load {} hook '{}' bound to connection profile '{}': {}",
                        phase.label(),
                        hook_id,
                        profile.name,
                        error
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "Connection profile '{}' references missing {} hook '{}'",
                        profile.name,
                        phase.label().to_ascii_lowercase(),
                        hook_id
                    )
                })?;

            let kind = match dto.kind_json.as_deref() {
                Some(kind_json) => serde_json::from_str(kind_json).map_err(|error| {
                    format!(
                        "Global hook '{}' bound to connection profile '{}' has invalid kind data: {}",
                        dto.name, profile.name, error
                    )
                })?,
                None => {
                    let command = dto.script_ref.clone().ok_or_else(|| {
                        format!(
                            "Global hook '{}' bound to connection profile '{}' has no executable definition",
                            dto.name, profile.name
                        )
                    })?;
                    dbflux_core::HookKind::Command {
                        command,
                        args: Vec::new(),
                    }
                }
            };
            let env = definitions_repo.get_env(hook_id).map_err(|error| {
                format!(
                    "Failed to load environment for global hook '{}' bound to connection profile '{}': {}",
                    dto.name, profile.name, error
                )
            })?;
            let hook = connection_hook_from_fields(
                dto.enabled,
                kind,
                dto.cwd.as_deref(),
                env,
                dto.inherit_env,
                dto.env_denylist,
                dto.timeout_ms,
                &dto.execution_mode,
                dto.ready_signal,
                &dto.on_failure,
                &profile.id.to_string(),
                hook_id,
            )?;
            definitions.insert(hook_id.clone(), hook);
        }
    }

    Ok(definitions)
}

fn load_script_source(
    source_type: Option<&str>,
    content: Option<&str>,
    path: Option<&str>,
    profile_id: &str,
    hook_id: &str,
) -> Result<dbflux_core::ScriptSource, String> {
    match source_type.unwrap_or("inline") {
        "inline" => Ok(dbflux_core::ScriptSource::Inline {
            content: content.unwrap_or_default().to_string(),
        }),
        "file" => path
            .map(|path| dbflux_core::ScriptSource::File { path: path.into() })
            .ok_or_else(|| {
                format!(
                    "Connection profile '{}' file hook '{}' has no script path",
                    profile_id, hook_id
                )
            }),
        source_type => Err(format!(
            "Connection profile '{}' hook '{}' has unknown script source type '{}'",
            profile_id, hook_id, source_type
        )),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "maps each persisted field while retaining profile and hook context for validation errors"
)]
fn connection_hook_from_fields(
    enabled: bool,
    kind: dbflux_core::HookKind,
    cwd: Option<&str>,
    env: HashMap<String, String>,
    inherit_env: bool,
    env_denylist: Vec<String>,
    timeout_ms: Option<i64>,
    execution_mode: &str,
    ready_signal: Option<String>,
    on_failure: &str,
    profile_id: &str,
    hook_id: &str,
) -> Result<dbflux_core::ConnectionHook, String> {
    let timeout_ms = timeout_ms
        .map(|timeout| {
            u64::try_from(timeout).map_err(|_| {
                format!(
                    "Connection profile '{}' hook '{}' has invalid negative timeout '{}'",
                    profile_id, hook_id, timeout
                )
            })
        })
        .transpose()?;
    let execution_mode = if execution_mode.eq_ignore_ascii_case("detached") {
        dbflux_core::HookExecutionMode::Detached
    } else {
        dbflux_core::HookExecutionMode::Blocking
    };
    let on_failure = if on_failure.eq_ignore_ascii_case("disconnect") {
        dbflux_core::HookFailureMode::Disconnect
    } else if on_failure.eq_ignore_ascii_case("ignore") {
        dbflux_core::HookFailureMode::Ignore
    } else {
        dbflux_core::HookFailureMode::Warn
    };

    Ok(dbflux_core::ConnectionHook {
        enabled,
        kind,
        cwd: cwd.map(Into::into),
        env,
        inherit_env,
        env_denylist,
        timeout_ms,
        execution_mode,
        ready_signal,
        on_failure,
    })
}

fn parse_hook_phase(value: &str) -> Option<dbflux_core::HookPhase> {
    match value {
        "pre_connect" => Some(dbflux_core::HookPhase::PreConnect),
        "post_connect" => Some(dbflux_core::HookPhase::PostConnect),
        "pre_disconnect" => Some(dbflux_core::HookPhase::PreDisconnect),
        "post_disconnect" => Some(dbflux_core::HookPhase::PostDisconnect),
        _ => None,
    }
}

fn load_connection_settings(
    repo: &dbflux_storage::repositories::connection_profiles::ConnectionProfileRepository,
    profile_id: &str,
) -> Result<Option<FormValues>, String> {
    let settings = repo
        .settings()
        .get_for_profile(profile_id)
        .map_err(|e| format!("Failed to load settings for '{}': {}", profile_id, e))?;

    let mut values = FormValues::new();

    for setting in settings {
        if !setting.setting_key.starts_with("conn.") {
            continue;
        }

        if let Some(value) = setting.setting_value {
            values.insert(
                setting.setting_key.trim_start_matches("conn.").to_string(),
                value,
            );
        }
    }

    Ok((!values.is_empty()).then_some(values))
}

fn load_profile_value_refs(
    repo: &dbflux_storage::repositories::connection_profiles::ConnectionProfileRepository,
    profile_id: &str,
) -> Result<HashMap<String, ValueRef>, String> {
    let value_refs = repo
        .value_refs()
        .get_for_profile(profile_id)
        .map_err(|e| format!("Failed to load value refs for '{}': {}", profile_id, e))?;

    let mut resolved = HashMap::with_capacity(value_refs.len());

    for entry in value_refs {
        let Some(kind) =
            dbflux_storage::repositories::connection_profile_value_refs::RefKind::try_parse(
                &entry.ref_kind,
            )
        else {
            continue;
        };

        let value_ref = match kind {
            dbflux_storage::repositories::connection_profile_value_refs::RefKind::Literal => {
                ValueRef::Literal {
                    value: entry.literal_value.unwrap_or(entry.ref_value),
                }
            }
            dbflux_storage::repositories::connection_profile_value_refs::RefKind::Env => {
                ValueRef::Env {
                    key: entry.env_key.unwrap_or(entry.ref_value),
                }
            }
            dbflux_storage::repositories::connection_profile_value_refs::RefKind::Dotenv => {
                ValueRef::Dotenv {
                    key: entry.env_key.ok_or_else(|| {
                        format!("Dotenv value ref '{}' is missing its key", entry.ref_key)
                    })?,
                    path: entry.ref_value,
                }
            }
            dbflux_storage::repositories::connection_profile_value_refs::RefKind::File => {
                ValueRef::File {
                    path: entry.ref_value,
                    json_key: entry.ref_json_key,
                }
            }
            dbflux_storage::repositories::connection_profile_value_refs::RefKind::Secret => {
                ValueRef::Secret {
                    provider: entry.ref_provider.ok_or_else(|| {
                        format!("Secret value ref '{}' is missing provider", entry.ref_key)
                    })?,
                    locator: entry.secret_locator.unwrap_or(entry.ref_value),
                    json_key: entry.ref_json_key,
                }
            }
            dbflux_storage::repositories::connection_profile_value_refs::RefKind::Param => {
                ValueRef::Parameter {
                    provider: entry.ref_provider.ok_or_else(|| {
                        format!(
                            "Parameter value ref '{}' is missing provider",
                            entry.ref_key
                        )
                    })?,
                    name: entry.param_name.unwrap_or(entry.ref_value),
                    json_key: entry.ref_json_key,
                }
            }
            dbflux_storage::repositories::connection_profile_value_refs::RefKind::Auth => {
                ValueRef::Auth {
                    field: entry.auth_field.unwrap_or(entry.ref_value),
                }
            }
        };

        resolved.insert(entry.ref_key, value_ref);
    }

    Ok(resolved)
}

fn load_access_kind(
    repo: &dbflux_storage::repositories::connection_profiles::ConnectionProfileRepository,
    dto: &dbflux_storage::repositories::connection_profiles::ConnectionProfileDto,
    profile_id: &str,
) -> Result<Option<AccessKind>, String> {
    let access_params = repo
        .access_params()
        .get_for_profile(profile_id)
        .map_err(|e| format!("Failed to load access params for '{}': {}", profile_id, e))?;

    Ok(match dto.access_kind.as_deref() {
        Some("direct") => Some(AccessKind::Direct),
        Some("ssh") => dto.ssh_tunnel_profile_id.as_deref().and_then(|value| {
            uuid::Uuid::parse_str(value)
                .ok()
                .map(|ssh_tunnel_profile_id| AccessKind::Ssh {
                    ssh_tunnel_profile_id,
                })
        }),
        Some("proxy") => dto.proxy_profile_id.as_deref().and_then(|value| {
            uuid::Uuid::parse_str(value)
                .ok()
                .map(|proxy_profile_id| AccessKind::Proxy { proxy_profile_id })
        }),
        Some("managed") => Some(AccessKind::Managed {
            provider: dto.access_provider.clone().unwrap_or_default(),
            params: access_params
                .into_iter()
                .map(|param| (param.param_key, param.param_value))
                .collect(),
        }),
        _ => None,
    })
}

fn load_profile_governance(
    repo: &dbflux_storage::repositories::connection_profiles::ConnectionProfileRepository,
    profile_id: &str,
) -> Result<Option<ConnectionMcpGovernance>, String> {
    let governance_entries = repo.governance().get_for_profile(profile_id).map_err(|e| {
        format!(
            "Failed to load governance settings for '{}': {}",
            profile_id, e
        )
    })?;
    let binding_entries = repo
        .governance_bindings()
        .get_for_profile(profile_id)
        .map_err(|e| {
            format!(
                "Failed to load governance bindings for '{}': {}",
                profile_id, e
            )
        })?;

    let enabled = governance_entries
        .into_iter()
        .find(|entry| entry.governance_key == "enabled")
        .and_then(|entry| entry.governance_value)
        .and_then(|value| value.parse().ok())
        .unwrap_or(false);

    if !enabled && binding_entries.is_empty() {
        return Ok(None);
    }

    let mut policy_bindings = Vec::with_capacity(binding_entries.len());

    for binding in binding_entries {
        let role_ids = repo
            .governance_binding_roles()
            .get_for_binding(&binding.id)
            .map_err(|e| {
                format!(
                    "Failed to load governance roles for '{}': {}",
                    binding.id, e
                )
            })?
            .into_iter()
            .map(|entry| entry.role_id)
            .collect();

        let policy_ids = repo
            .governance_binding_policies()
            .get_for_binding(&binding.id)
            .map_err(|e| {
                format!(
                    "Failed to load governance policies for '{}': {}",
                    binding.id, e
                )
            })?
            .into_iter()
            .map(|entry| entry.policy_id)
            .collect();

        policy_bindings.push(ConnectionMcpPolicyBinding {
            actor_id: binding.actor_id,
            role_ids,
            policy_ids,
        });
    }

    Ok(Some(ConnectionMcpGovernance {
        enabled,
        policy_bindings,
    }))
}

fn str_to_db_kind(value: &str) -> Option<dbflux_core::DbKind> {
    match value {
        "Postgres" => Some(dbflux_core::DbKind::Postgres),
        "SQLite" => Some(dbflux_core::DbKind::SQLite),
        "MySQL" => Some(dbflux_core::DbKind::MySQL),
        "MariaDB" => Some(dbflux_core::DbKind::MariaDB),
        "MongoDB" => Some(dbflux_core::DbKind::MongoDB),
        "Redis" => Some(dbflux_core::DbKind::Redis),
        "DynamoDB" => Some(dbflux_core::DbKind::DynamoDB),
        "CloudWatchLogs" => Some(dbflux_core::DbKind::CloudWatchLogs),
        "InfluxDB" => Some(dbflux_core::DbKind::InfluxDB),
        "SqlServer" => Some(dbflux_core::DbKind::SqlServer),
        "Redshift" => Some(dbflux_core::DbKind::Redshift),
        "ClickHouse" => Some(dbflux_core::DbKind::ClickHouse),
        "S3" => Some(dbflux_core::DbKind::S3),
        "Turso" => Some(dbflux_core::DbKind::Turso),
        "DuckDB" => Some(dbflux_core::DbKind::DuckDB),
        "AwsSecrets" => Some(dbflux_core::DbKind::AwsSecrets),
        _ => None,
    }
}

fn default_db_config_for_kind(kind: dbflux_core::DbKind) -> dbflux_core::DbConfig {
    match kind {
        dbflux_core::DbKind::Postgres => dbflux_core::DbConfig::default_postgres(),
        dbflux_core::DbKind::SQLite => dbflux_core::DbConfig::default_sqlite(),
        dbflux_core::DbKind::MySQL => dbflux_core::DbConfig::default_mysql(),
        dbflux_core::DbKind::MariaDB => dbflux_core::DbConfig::default_mysql(),
        dbflux_core::DbKind::MongoDB => dbflux_core::DbConfig::default_mongodb(),
        dbflux_core::DbKind::Redis => dbflux_core::DbConfig::default_redis(),
        dbflux_core::DbKind::DynamoDB => dbflux_core::DbConfig::default_dynamodb(),
        dbflux_core::DbKind::CloudWatchLogs => dbflux_core::DbConfig::default_cloudwatch_logs(),
        dbflux_core::DbKind::InfluxDB => dbflux_core::DbConfig::default_influxdb(),
        dbflux_core::DbKind::SqlServer => dbflux_core::DbConfig::default_sqlserver(),
        dbflux_core::DbKind::Redshift => dbflux_core::DbConfig::default_redshift(),
        dbflux_core::DbKind::ClickHouse => dbflux_core::DbConfig::default_clickhouse(),
        dbflux_core::DbKind::S3 => dbflux_core::DbConfig::default_s3(),
        dbflux_core::DbKind::AwsSecrets => dbflux_core::DbConfig::default_aws_secrets(),
        dbflux_core::DbKind::Turso => dbflux_core::DbConfig::default_turso(),
        dbflux_core::DbKind::DuckDB => dbflux_core::DbConfig::default_duckdb(),
    }
}

fn load_governance_settings(
    config_dir: Option<&std::path::Path>,
) -> Result<GovernanceSettings, String> {
    let conn = open_config_db(config_dir)?;
    #[allow(clippy::arc_with_non_send_sync)]
    let repo = GovernanceSettingsRepository::new(Arc::new(conn));

    // Load governance settings (mcp_enabled_by_default)
    let dto = repo
        .get()
        .map_err(|e| format!("Failed to load governance settings from dbflux.db: {}", e))?;

    let mcp_enabled_by_default = dto.map(|d| d.mcp_enabled_by_default != 0).unwrap_or(false);

    // Load trusted clients
    let trusted_clients = repo
        .get_trusted_clients()
        .map_err(|e| format!("Failed to load trusted clients from dbflux.db: {}", e))?
        .into_iter()
        .map(|c| dbflux_core::TrustedClientConfig {
            id: c.client_id,
            name: c.name,
            issuer: c.issuer,
            active: c.active != 0,
        })
        .collect();

    // Load policy roles
    let roles = repo
        .get_policy_roles()
        .map_err(|e| format!("Failed to load policy roles from dbflux.db: {}", e))?
        .into_iter()
        .map(|r| dbflux_core::PolicyRoleConfig {
            id: r.role_id,
            policy_ids: vec![], // roles don't store policy_ids in the repo DTO
        })
        .collect();

    // Load tool policies
    let policies = repo
        .get_tool_policies()
        .map_err(|e| format!("Failed to load tool policies from dbflux.db: {}", e))?
        .into_iter()
        .map(|p| dbflux_core::ToolPolicyConfig {
            id: p.policy_id,
            allowed_tools: p.allowed_tools,
            allowed_classes: p.allowed_classes,
            approval_classes: p.approval_classes,
        })
        .collect();

    Ok(GovernanceSettings {
        mcp_enabled_by_default,
        trusted_clients,
        roles,
        policies,
    })
}

/// Creates the assignment for tools that take no connection (connection_id = "")
/// for each active trusted client.
///
/// Script and audit tools are not bound to a connection, so no connection-scoped
/// binding ever applies to them. Each client gets `builtin/read-only` plus every
/// role and policy it holds on any connection. Policies are additive grants, so a
/// client trusted with writes on a connection can manage its scripts, while a
/// client with no connection bindings keeps read-only access. This must run after
/// the connection assignments are loaded.
fn create_global_assignments(runtime: &mut McpRuntime) -> Result<(), String> {
    let clients = runtime
        .list_trusted_clients()
        .map_err(|e| format!("Failed to list trusted clients: {}", e))?;

    if clients.is_empty() {
        return Ok(());
    }

    let connection_assignments: Vec<dbflux_policy::ConnectionPolicyAssignment> = runtime
        .policy_assignments_for_engine()
        .into_iter()
        .filter(|assignment| !assignment.scope.connection_id.is_empty())
        .collect();

    let known_role_ids: std::collections::HashSet<String> = runtime
        .roles_for_engine()
        .into_iter()
        .map(|role| role.id)
        .collect();

    let assignments: Vec<dbflux_policy::ConnectionPolicyAssignment> = clients
        .into_iter()
        .filter(|client| client.active)
        .map(|client| global_assignment_for(client.id, &connection_assignments, &known_role_ids))
        .collect();

    if !assignments.is_empty() {
        runtime
            .save_connection_policy_assignment_mut(dbflux_mcp::ConnectionPolicyAssignmentDto {
                connection_id: String::new(),
                assignments,
            })
            .map_err(|e| format!("Failed to save global assignments: {}", e))?;
    }

    Ok(())
}

fn global_assignment_for(
    actor_id: String,
    connection_assignments: &[dbflux_policy::ConnectionPolicyAssignment],
    known_role_ids: &std::collections::HashSet<String>,
) -> dbflux_policy::ConnectionPolicyAssignment {
    let mut role_ids = std::collections::BTreeSet::from(["builtin/read-only".to_string()]);
    let mut policy_ids = std::collections::BTreeSet::new();

    for assignment in connection_assignments
        .iter()
        .filter(|assignment| assignment.actor_id == actor_id)
    {
        // A role deleted after it was bound lingers in the connection binding; copying it
        // would make the engine fail every connectionless call with `MissingRole`.
        role_ids.extend(
            assignment
                .role_ids
                .iter()
                .filter(|role_id| known_role_ids.contains(*role_id))
                .cloned(),
        );
        policy_ids.extend(assignment.policy_ids.iter().cloned());
    }

    dbflux_policy::ConnectionPolicyAssignment {
        actor_id,
        scope: dbflux_policy::PolicyBindingScope {
            connection_id: String::new(),
        },
        role_ids: role_ids.into_iter().collect(),
        policy_ids: policy_ids.into_iter().collect(),
    }
}

async fn load_connection_policy_assignments(
    runtime: Arc<RwLock<McpRuntime>>,
    profile_manager: Arc<RwLock<ProfileManager>>,
) -> Result<(), String> {
    let profiles = {
        let pm = profile_manager.read().await;
        pm.profiles.clone()
    };

    log::info!(
        "Loading connection policy assignments for {} profiles",
        profiles.len()
    );

    let mut rt = runtime.write().await;
    let mut loaded_count = 0;
    for profile in profiles {
        if load_profile_assignment(&mut rt, &profile) {
            loaded_count += 1;
        }
    }

    log::info!("Loaded {} connection policy assignments", loaded_count);

    create_global_assignments(&mut rt)?;

    // Drain events after loading all assignments
    rt.drain_events();

    Ok(())
}

fn load_profile_assignment(runtime: &mut McpRuntime, profile: &ConnectionProfile) -> bool {
    let Some(governance) = &profile.mcp_governance else {
        return false;
    };

    if !governance.enabled {
        return false;
    };

    let assignments: Vec<dbflux_policy::ConnectionPolicyAssignment> = governance
        .policy_bindings
        .iter()
        .map(|binding| dbflux_policy::ConnectionPolicyAssignment {
            actor_id: binding.actor_id.clone(),
            scope: dbflux_policy::PolicyBindingScope {
                connection_id: profile.id.to_string(),
            },
            role_ids: binding.role_ids.clone(),
            policy_ids: binding.policy_ids.clone(),
        })
        .collect();

    if !assignments.is_empty() {
        log::info!(
            "Loading assignment for connection {} ({}) with {} bindings",
            profile.name,
            profile.id,
            assignments.len()
        );
        match runtime.save_connection_policy_assignment_mut(
            dbflux_mcp::ConnectionPolicyAssignmentDto {
                connection_id: profile.id.to_string(),
                assignments,
            },
        ) {
            Ok(_) => true,
            Err(e) => {
                log::error!("Failed to save assignment for {}: {}", profile.name, e);
                false
            }
        }
    } else {
        false
    }
}

impl ServerState {
    /// Returns `true` if the given connection has MCP access enabled.
    pub async fn is_mcp_enabled_for_connection(&self, connection_id: &str) -> bool {
        let Ok(profile_uuid) = connection_id.parse::<uuid::Uuid>() else {
            return false;
        };

        let profile_manager = self.profile_manager.read().await;
        let Some(profile) = profile_manager.find_by_id(profile_uuid) else {
            return false;
        };

        match &profile.mcp_governance {
            Some(governance) => governance.enabled,
            None => self.mcp_enabled_by_default,
        }
    }

    /// Returns `true` if the given connection's profile is set to read-only.
    ///
    /// The flag is read from `profile_store_path` on every call. When the
    /// store cannot be read, the connection counts as read-only: the gate only
    /// refuses calls above `read`, so reads keep working, and a write never
    /// slips through on a profile switched to read-only after startup. When
    /// the profile is not in the store, or no store is configured, the profile
    /// loaded at startup decides.
    ///
    /// An unknown connection reports `false`: it is already denied as not
    /// MCP-enabled, so this check never has to decide for it.
    pub async fn is_read_only_connection(&self, connection_id: &str) -> bool {
        let Ok(profile_uuid) = connection_id.parse::<uuid::Uuid>() else {
            return false;
        };

        if let Some(stored_flag) = self.stored_read_only_flag(profile_uuid).await {
            return stored_flag;
        }

        let profile_manager = self.profile_manager.read().await;
        profile_manager
            .find_by_id(profile_uuid)
            .is_some_and(|profile| profile.read_only_flag)
    }

    async fn stored_read_only_flag(&self, profile_uuid: uuid::Uuid) -> Option<bool> {
        let db_path = self.profile_store_path.clone()?;

        let read = tokio::task::spawn_blocking(move || {
            read_stored_read_only_flag(&db_path, &profile_uuid.to_string())
        })
        .await;

        match read {
            Ok(Ok(flag)) => flag,
            Ok(Err(error)) => {
                log::warn!(
                    "Failed to read the read-only flag of profile {} from storage, treating the connection as read-only: {}",
                    profile_uuid,
                    error
                );
                Some(true)
            }
            Err(error) => {
                log::warn!(
                    "Read-only flag lookup for profile {} did not complete, treating the connection as read-only: {}",
                    profile_uuid,
                    error
                );
                Some(true)
            }
        }
    }
}

fn read_stored_read_only_flag(
    db_path: &std::path::Path,
    profile_id: &str,
) -> Result<Option<bool>, dbflux_storage::error::StorageError> {
    let connection = storage_sqlite::open_database(db_path)?;

    #[allow(clippy::arc_with_non_send_sync)]
    let repository =
        dbflux_storage::repositories::connection_profiles::ConnectionProfileRepository::new(
            Arc::new(connection),
        );

    Ok(repository.get(profile_id)?.map(|profile| profile.read_only))
}

fn build_driver_registry() -> HashMap<String, Arc<dyn DbDriver>> {
    #[allow(unused_mut)]
    let mut registry: HashMap<String, Arc<dyn DbDriver>> = HashMap::new();

    #[cfg(feature = "sqlite")]
    {
        registry.insert(
            "sqlite".to_string(),
            Arc::new(dbflux_driver_sqlite::SqliteDriver),
        );
    }

    #[cfg(feature = "postgres")]
    {
        registry.insert(
            "postgres".to_string(),
            Arc::new(dbflux_driver_postgres::PostgresDriver),
        );
    }

    #[cfg(feature = "mysql")]
    {
        registry.insert(
            "mysql".to_string(),
            Arc::new(dbflux_driver_mysql::MysqlDriver::new(DbKind::MySQL)),
        );
        registry.insert(
            "mariadb".to_string(),
            Arc::new(dbflux_driver_mysql::MysqlDriver::new(DbKind::MariaDB)),
        );
    }

    #[cfg(feature = "mongodb")]
    {
        registry.insert(
            "mongodb".to_string(),
            Arc::new(dbflux_driver_mongodb::MongoDriver),
        );
    }

    #[cfg(feature = "redis")]
    {
        registry.insert(
            "redis".to_string(),
            Arc::new(dbflux_driver_redis::RedisDriver),
        );
    }

    #[cfg(feature = "dynamodb")]
    {
        registry.insert(
            "dynamodb".to_string(),
            Arc::new(dbflux_driver_dynamodb::DynamoDriver::new()),
        );
    }

    #[cfg(feature = "redshift")]
    {
        registry.insert(
            "redshift".to_string(),
            Arc::new(dbflux_driver_redshift::RedshiftDriver::new()),
        );
    }

    #[cfg(feature = "clickhouse")]
    {
        registry.insert(
            "clickhouse".to_string(),
            Arc::new(dbflux_driver_clickhouse::ClickHouseDriver::new()),
        );
    }

    #[cfg(feature = "turso")]
    {
        registry.insert(
            "turso".to_string(),
            Arc::new(dbflux_driver_turso::TursoDriver::new()),
        );
    }

    #[cfg(feature = "duckdb")]
    {
        registry.insert(
            "duckdb".to_string(),
            Arc::new(dbflux_driver_duckdb::DuckDbDriver::new()),
        );
    }

    registry
}

fn build_auth_provider_registry(
    services: &[ServiceConfig],
) -> HashMap<String, Arc<dyn DynAuthProvider>> {
    #[allow(unused_mut)]
    let mut registry: HashMap<String, Arc<dyn DynAuthProvider>> = HashMap::new();

    #[cfg(feature = "aws")]
    {
        let sso = Arc::new(dbflux_aws::AwsSsoAuthProvider::new()) as Arc<dyn DynAuthProvider>;
        registry.insert(sso.provider_id().to_string(), sso);

        let shared = Arc::new(dbflux_aws::AwsSharedCredentialsAuthProvider::new())
            as Arc<dyn DynAuthProvider>;
        registry.insert(shared.provider_id().to_string(), shared);

        let sso_session =
            Arc::new(dbflux_aws::AwsSsoSessionAuthProvider::new()) as Arc<dyn DynAuthProvider>;
        registry.insert(sso_session.provider_id().to_string(), sso_session);

        // aws-static-credentials provider removed (ADR-7): DBFlux no longer
        // stores AWS long-lived secrets. Shared-credentials profiles are
        // reflected from ~/.aws/credentials via AwsSharedCredentialsAuthProvider.
    }

    load_external_auth_providers(&mut registry, services);

    registry
}

fn load_external_auth_providers(
    registry: &mut HashMap<String, Arc<dyn DynAuthProvider>>,
    services: &[ServiceConfig],
) {
    for service in services {
        if !service.enabled {
            continue;
        }

        if service.kind != RpcServiceKind::AuthProvider {
            continue;
        }

        if let Err(message) = validate_auth_provider_contract(service) {
            log::warn!(
                "Skipping RPC auth-provider service '{}': {}",
                service.socket_id,
                message
            );
            continue;
        }

        let launch = match RpcAuthProvider::build_launch_config(
            &service.socket_id,
            service.command.as_deref(),
            &service.args,
            &service.env,
            service.startup_timeout_ms,
        ) {
            Ok(launch) => launch,
            Err(error) => {
                log::warn!(
                    "Skipping RPC auth-provider service '{}': {}",
                    service.socket_id,
                    error
                );
                continue;
            }
        };

        let provider = match RpcAuthProvider::probe(&service.socket_id, launch) {
            Ok(provider) => Arc::new(provider) as Arc<dyn DynAuthProvider>,
            Err(error) => {
                log::warn!(
                    "Skipping RPC auth-provider service '{}': {}",
                    service.socket_id,
                    error
                );
                continue;
            }
        };

        let provider_id = provider.provider_id().to_string();
        if registry.contains_key(&provider_id) {
            log::warn!(
                "Skipping RPC auth-provider service '{}': provider id '{}' already exists",
                service.socket_id,
                provider_id
            );
            continue;
        }

        registry.insert(provider_id, provider);
    }
}

fn validate_auth_provider_contract(service: &ServiceConfig) -> Result<(), String> {
    let contract = service.resolved_api_contract();

    if contract.family != "auth_provider_rpc" {
        return Err(format!(
            "auth-provider service declares incompatible RPC family '{}'",
            contract.family
        ));
    }

    if contract.major != AUTH_PROVIDER_RPC_API_CONTRACT.version.major {
        return Err(format!(
            "auth-provider service declares incompatible RPC major version {}",
            contract.major
        ));
    }

    Ok(())
}

/// Probes every enabled secret-provider service; built-in AWS providers are registered per
/// connection by their auth provider, so they never enter this map and their ids are reserved.
fn build_secret_provider_registry(
    services: &[ServiceConfig],
) -> HashMap<String, Arc<dyn DynSecretProvider>> {
    let mut registry: HashMap<String, Arc<dyn DynSecretProvider>> = HashMap::new();

    for service in services {
        if !service.enabled || service.kind != RpcServiceKind::SecretProvider {
            continue;
        }

        match load_external_secret_provider(service) {
            Ok(provider) => {
                let provider_id = provider.provider_id().to_string();
                if registry.contains_key(&provider_id) {
                    log::warn!(
                        "Skipping RPC secret-provider service '{}': provider id '{}' already exists",
                        service.socket_id,
                        provider_id
                    );
                    continue;
                }

                registry.insert(provider_id, provider);
            }
            Err(message) => {
                log::warn!(
                    "Skipping RPC secret-provider service '{}': {}",
                    service.socket_id,
                    message
                );
            }
        }
    }

    registry
}

fn load_external_secret_provider(
    service: &ServiceConfig,
) -> Result<Arc<dyn DynSecretProvider>, String> {
    validate_secret_provider_contract(service)?;

    let launch = RpcSecretProvider::build_launch_config(
        &service.socket_id,
        service.command.as_deref(),
        &service.args,
        &service.env,
        service.startup_timeout_ms,
    )
    .map_err(|error| error.to_string())?;

    RpcSecretProvider::probe(&service.socket_id, launch, &BUILT_IN_SECRET_PROVIDER_IDS)
        .map(|provider| Arc::new(provider) as Arc<dyn DynSecretProvider>)
        .map_err(|error| error.to_string())
}

fn validate_secret_provider_contract(service: &ServiceConfig) -> Result<(), String> {
    let contract = service.resolved_api_contract();

    if contract.family != "secret_provider_rpc" {
        return Err(format!(
            "secret-provider service declares incompatible RPC family '{}'",
            contract.family
        ));
    }

    if contract.major != SECRET_PROVIDER_RPC_API_CONTRACT.version.major {
        return Err(format!(
            "secret-provider service declares incompatible RPC major version {}",
            contract.major
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests;
