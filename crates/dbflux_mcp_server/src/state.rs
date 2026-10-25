use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex, RwLock};

#[cfg(feature = "mysql")]
use dbflux_core::DbKind;
use dbflux_core::auth::DynAuthProvider;
use dbflux_core::{
    AccessKind, AuthProfileManager, CancelToken, ConnectionMcpGovernance,
    ConnectionMcpPolicyBinding, ConnectionProfile, DbDriver, DetachedProcessHandle, DriverKey,
    FormValues, GovernanceSettings, KeyringSecretStore, ProfileManager, RpcServiceKind,
    SecretManager, ServiceConfig, ValueRef,
};
use dbflux_ipc::{AUTH_PROVIDER_RPC_API_CONTRACT, RpcAuthProvider};
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
    pub driver_settings: Arc<HashMap<DriverKey, FormValues>>,
    pub connection_cache: Arc<RwLock<ConnectionCache>>,
    pub connection_setup_lock: Arc<Mutex<HashMap<String, std::sync::Weak<Mutex<()>>>>>,
    pub detached_processes: Arc<DetachedProcessRegistry>,
    pub secret_manager: Arc<SecretManager>,
    pub mcp_enabled_by_default: bool,
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
            driver_settings,
            connection_cache: Arc::new(RwLock::new(ConnectionCache::new())),
            connection_setup_lock: Arc::new(Mutex::new(HashMap::new())),
            detached_processes: Arc::new(DetachedProcessRegistry::new()),
            secret_manager,
            mcp_enabled_by_default: governance_settings.mcp_enabled_by_default,
        };

        // Load connection policy assignments
        let runtime_clone = state.runtime.clone();
        let profile_manager_clone = state.profile_manager.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                load_connection_policy_assignments(runtime_clone, profile_manager_clone).await;
            });
        });

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

    create_global_assignments(runtime)?;

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

/// Creates a global policy assignment (connection_id = "") for each trusted client.
/// This allows clients to use tools that don't require a specific connection.
fn create_global_assignments(runtime: &mut McpRuntime) -> Result<(), String> {
    let clients = runtime
        .list_trusted_clients()
        .map_err(|e| format!("Failed to list trusted clients: {}", e))?;

    if clients.is_empty() {
        return Ok(());
    }

    // Create assignments for each active client
    let assignments: Vec<dbflux_policy::ConnectionPolicyAssignment> = clients
        .into_iter()
        .filter(|client| client.active)
        .map(|client| dbflux_policy::ConnectionPolicyAssignment {
            actor_id: client.id,
            scope: dbflux_policy::PolicyBindingScope {
                connection_id: String::new(),
            },
            // Grant read-only role by default for global operations
            role_ids: vec!["builtin/read-only".to_string()],
            policy_ids: vec![],
        })
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

async fn load_connection_policy_assignments(
    runtime: Arc<RwLock<McpRuntime>>,
    profile_manager: Arc<RwLock<ProfileManager>>,
) {
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

    // Drain events after loading all assignments
    rt.drain_events();
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
    /// An unknown connection reports `false`: it is already denied as not
    /// MCP-enabled, so this check never has to decide for it.
    pub async fn is_read_only_connection(&self, connection_id: &str) -> bool {
        let Ok(profile_uuid) = connection_id.parse::<uuid::Uuid>() else {
            return false;
        };

        let profile_manager = self.profile_manager.read().await;
        profile_manager
            .find_by_id(profile_uuid)
            .is_some_and(|profile| profile.read_only_flag)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    use dbflux_core::auth::{AuthProfile, AuthSessionState, UrlCallback};
    use dbflux_core::secrecy::ExposeSecret;
    use dbflux_storage::repositories::connection_profile_hook_bindings::ConnectionProfileHookBindingDto;
    use dbflux_storage::repositories::connection_profile_hooks::ConnectionProfileHookDto;
    use dbflux_storage::repositories::connection_profiles::ConnectionProfileDto;
    use dbflux_storage::repositories::hook_definitions::HookDefinitionDto;
    use dbflux_storage::repositories::services::ServiceDto;
    use dbflux_test_support::{
        FakeAuthProviderRpcConfig, FakeAuthProviderRpcServer, FakeAuthRpcResult,
    };

    #[test]
    #[cfg(feature = "clickhouse")]
    fn clickhouse_is_available_to_mcp_when_feature_enabled() {
        assert_eq!(
            str_to_db_kind("ClickHouse"),
            Some(dbflux_core::DbKind::ClickHouse)
        );

        let registry = build_driver_registry();
        let driver = registry
            .get("clickhouse")
            .expect("clickhouse driver must be registered");
        assert_eq!(driver.driver_key(), "builtin:clickhouse");
    }

    struct ReflectingAuthProvider {
        provider_id: &'static str,
        reflected: Vec<AuthProfile>,
    }

    #[async_trait::async_trait]
    impl DynAuthProvider for ReflectingAuthProvider {
        fn provider_id(&self) -> &str {
            self.provider_id
        }

        fn display_name(&self) -> &str {
            "Reflecting Test Provider"
        }

        fn form_def(&self) -> &dbflux_core::AuthFormDef {
            static FORM: std::sync::OnceLock<dbflux_core::AuthFormDef> = std::sync::OnceLock::new();
            FORM.get_or_init(|| dbflux_core::AuthFormDef { tabs: vec![] })
        }

        async fn validate_session(
            &self,
            _profile: &AuthProfile,
        ) -> Result<AuthSessionState, dbflux_core::DbError> {
            Ok(AuthSessionState::LoginRequired)
        }

        async fn login(
            &self,
            _profile: &AuthProfile,
            _url_callback: UrlCallback,
        ) -> Result<dbflux_core::auth::AuthSession, dbflux_core::DbError> {
            Err(dbflux_core::DbError::connection_failed("not supported"))
        }

        async fn resolve_credentials(
            &self,
            _profile: &AuthProfile,
        ) -> Result<dbflux_core::auth::ResolvedCredentials, dbflux_core::DbError> {
            Ok(dbflux_core::auth::ResolvedCredentials::default())
        }

        fn reflect_profiles(&self) -> Vec<AuthProfile> {
            self.reflected.clone()
        }
    }

    fn reflecting_registry(
        provider_id: &'static str,
        reflected: Vec<AuthProfile>,
    ) -> HashMap<String, Arc<dyn DynAuthProvider>> {
        HashMap::from([(
            provider_id.to_string(),
            Arc::new(ReflectingAuthProvider {
                provider_id,
                reflected,
            }) as Arc<dyn DynAuthProvider>,
        )])
    }

    #[test]
    fn auth_profile_manager_includes_reflected_profile_without_stored_row() {
        let mut reflected = AuthProfile::new("dev", "aws-sso", HashMap::new());
        reflected.read_only = true;
        let reflected_id = reflected.id;
        let registry = reflecting_registry("aws-sso", vec![reflected]);

        let manager = build_auth_profile_manager(Vec::new(), &registry);

        assert!(
            manager
                .items
                .iter()
                .any(|profile| profile.id == reflected_id)
        );
    }

    #[test]
    fn auth_profile_manager_replaces_stored_row_of_reflecting_provider() {
        let mut reflected = AuthProfile::new("dev", "aws-sso", HashMap::new());
        reflected.read_only = true;
        let reflected_id = reflected.id;
        let registry = reflecting_registry("aws-sso", vec![reflected]);

        let stale_stored = AuthProfile::new("dev", "aws-sso", HashMap::new());
        let other_stored = AuthProfile::new("rpc", "rpc-auth", HashMap::new());
        let other_stored_id = other_stored.id;

        let manager = build_auth_profile_manager(vec![stale_stored, other_stored], &registry);

        let ids: Vec<_> = manager.items.iter().map(|profile| profile.id).collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&reflected_id));
        assert!(ids.contains(&other_stored_id));
    }

    fn temp_runtime() -> (tempfile::TempDir, StorageRuntime) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let runtime = StorageRuntime::for_path(temp_dir.path().join("dbflux.db")).expect("runtime");
        (temp_dir, runtime)
    }

    fn insert_profile(runtime: &StorageRuntime, name: &str) -> uuid::Uuid {
        let id = uuid::Uuid::new_v4();
        let mut dto = ConnectionProfileDto::new(id, name.to_string());
        dto.driver_id = Some("postgres".to_string());
        dto.kind = Some("Postgres".to_string());
        runtime
            .connection_profiles()
            .insert(&dto)
            .expect("insert test profile");
        id
    }

    #[test]
    fn load_profiles_keeps_inline_hooks_and_resolves_global_hook_bindings() {
        let (_temp_dir, runtime) = temp_runtime();
        let inline_profile_id = insert_profile(&runtime, "inline");
        let bound_profile_id = insert_profile(&runtime, "bound");
        let mixed_profile_id = insert_profile(&runtime, "mixed");

        let profiles_repo = runtime.connection_profiles();
        let inline_hook = ConnectionProfileHookDto::new_command(
            inline_profile_id.to_string(),
            "pre_connect".to_string(),
            0,
            "refresh-token".to_string(),
        );
        profiles_repo
            .hooks()
            .insert(&inline_hook)
            .expect("insert inline hook");
        profiles_repo
            .hook_args()
            .insert_batch(&inline_hook.id, &["--renew".to_string()])
            .expect("insert inline hook args");
        profiles_repo
            .hook_envs()
            .insert_batch(
                &inline_hook.id,
                &HashMap::from([("TOKEN_SOURCE".to_string(), "keyring".to_string())]),
            )
            .expect("insert inline hook environment");
        let mixed_inline_hook = ConnectionProfileHookDto::new_command(
            mixed_profile_id.to_string(),
            "pre_connect".to_string(),
            0,
            "inline-refresh".to_string(),
        );
        profiles_repo
            .hooks()
            .insert(&mixed_inline_hook)
            .expect("insert mixed inline hook");

        let global_hook_id = uuid::Uuid::new_v4().to_string();
        let global_hook = dbflux_core::ConnectionHook {
            enabled: true,
            kind: dbflux_core::HookKind::Command {
                command: "vpn-connect".to_string(),
                args: vec!["--profile".to_string(), "work".to_string()],
            },
            cwd: None,
            env: HashMap::from([("VPN_MODE".to_string(), "work".to_string())]),
            inherit_env: true,
            env_denylist: vec!["DBFLUX_PASSWORD".to_string()],
            timeout_ms: Some(5_000),
            execution_mode: dbflux_core::HookExecutionMode::Blocking,
            ready_signal: None,
            on_failure: dbflux_core::HookFailureMode::Disconnect,
        };
        let mut definition = HookDefinitionDto::new(
            uuid::Uuid::parse_str(&global_hook_id).expect("global hook id"),
            "Connect VPN".to_string(),
            "Blocking".to_string(),
        );
        definition.kind_json = Some(serde_json::to_string(&global_hook.kind).expect("hook JSON"));
        definition.env_denylist = global_hook.env_denylist.clone();
        definition.timeout_ms = global_hook.timeout_ms.map(|value| value as i64);
        definition.on_failure = "Disconnect".to_string();
        runtime
            .hook_definitions()
            .insert(&definition)
            .expect("insert global hook");
        runtime
            .hook_definitions()
            .set_env(&global_hook_id, &global_hook.env)
            .expect("insert global hook environment");
        profiles_repo
            .hook_bindings()
            .insert(&ConnectionProfileHookBindingDto::new(
                bound_profile_id.to_string(),
                global_hook_id.clone(),
                "post_connect".to_string(),
                0,
            ))
            .expect("insert hook binding");
        profiles_repo
            .hook_bindings()
            .insert(&ConnectionProfileHookBindingDto::new(
                mixed_profile_id.to_string(),
                global_hook_id,
                "pre_connect".to_string(),
                0,
            ))
            .expect("insert mixed profile hook binding");

        let (profiles, profile_load_errors) = load_profiles(&runtime).expect("load profiles");
        assert!(profile_load_errors.is_empty());
        let inline_profile = profiles
            .iter()
            .find(|profile| profile.id == inline_profile_id)
            .expect("inline profile loaded");
        let inline_hook = inline_profile
            .hooks
            .as_ref()
            .expect("inline hooks loaded")
            .pre_connect
            .first()
            .expect("inline pre-connect hook loaded");
        match &inline_hook.kind {
            dbflux_core::HookKind::Command { command, args } => {
                assert_eq!(command, "refresh-token");
                assert_eq!(args, &["--renew"]);
            }
            other => panic!("expected command hook, got {other:?}"),
        }
        assert_eq!(
            inline_hook.env.get("TOKEN_SOURCE").map(String::as_str),
            Some("keyring")
        );

        let bound_profile = profiles
            .iter()
            .find(|profile| profile.id == bound_profile_id)
            .expect("bound profile loaded");
        let bound_hook = bound_profile
            .hooks
            .as_ref()
            .expect("resolved global hook loaded")
            .post_connect
            .first()
            .expect("post-connect binding resolved");
        match &bound_hook.kind {
            dbflux_core::HookKind::Command { command, args } => {
                assert_eq!(command, "vpn-connect");
                assert_eq!(args, &["--profile", "work"]);
            }
            other => panic!("expected command hook, got {other:?}"),
        }
        assert_eq!(
            bound_hook.env.get("VPN_MODE").map(String::as_str),
            Some("work")
        );
        assert_eq!(bound_hook.timeout_ms, Some(5_000));
        assert_eq!(bound_hook.env_denylist, ["DBFLUX_PASSWORD"]);

        let mixed_profile = profiles
            .iter()
            .find(|profile| profile.id == mixed_profile_id)
            .expect("mixed profile loaded");
        let mixed_hooks = mixed_profile.hooks.as_ref().expect("mixed hooks loaded");
        assert_eq!(mixed_hooks.pre_connect.len(), 1);
        assert!(matches!(
            &mixed_hooks.pre_connect[0].kind,
            dbflux_core::HookKind::Command { command, .. } if command == "vpn-connect"
        ));
    }

    #[test]
    fn load_profiles_scopes_invalid_hook_errors_to_affected_profile() {
        let (_temp_dir, runtime) = temp_runtime();
        let invalid_profile_id = insert_profile(&runtime, "invalid-hook");
        let valid_profile_id = insert_profile(&runtime, "valid-profile");
        let invalid_hook = ConnectionProfileHookDto::new_command(
            invalid_profile_id.to_string(),
            "unknown_phase".to_string(),
            0,
            "will-not-run".to_string(),
        );
        runtime
            .connection_profiles()
            .hooks()
            .insert(&invalid_hook)
            .expect("insert malformed hook");

        let (profiles, profile_load_errors) = load_profiles(&runtime).expect("load profiles");

        assert_eq!(profiles.len(), 2);
        assert!(
            profiles
                .iter()
                .any(|profile| profile.id == valid_profile_id)
        );
        assert!(
            profiles
                .iter()
                .any(|profile| profile.id == invalid_profile_id)
        );
        assert!(
            profile_load_errors
                .get(&invalid_profile_id)
                .is_some_and(|error| error.contains("unknown phase"))
        );
        assert!(!profile_load_errors.contains_key(&valid_profile_id));
    }

    fn insert_auth_provider_service(runtime: &StorageRuntime, socket_id: &str) {
        let repo = runtime.services();

        repo.insert(&ServiceDto {
            socket_id: socket_id.to_string(),
            enabled: true,
            command: None,
            startup_timeout_ms: None,
            service_kind: "auth_provider".to_string(),
            api_family: Some("auth_provider_rpc".to_string()),
            api_major: Some(1),
            api_minor: Some(0),
            created_at: String::new(),
            updated_at: String::new(),
        })
        .expect("insert service");
    }

    #[tokio::test]
    async fn build_auth_provider_registry_loads_persisted_external_provider() {
        let (_temp_dir, runtime) = temp_runtime();
        insert_auth_provider_service(&runtime, "mcp-auth");

        let profile = AuthProfile::new("rpc profile", "rpc-auth", HashMap::new());
        let profile_id = profile.id;

        let mut credentials = dbflux_ipc::ResolvedCredentialsDto {
            fields: HashMap::new(),
            secret_fields: HashMap::new(),
        };
        credentials
            .fields
            .insert("region".to_string(), "us-east-1".to_string());
        credentials
            .secret_fields
            .insert("token".to_string(), "secret-token".to_string());

        let server = FakeAuthProviderRpcServer::start(FakeAuthProviderRpcConfig {
            socket_id: "mcp-auth".to_string(),
            provider_id: "rpc-auth".to_string(),
            display_name: "RPC Auth".to_string(),
            form_definition: dbflux_core::AuthFormDef { tabs: vec![] },
            capabilities: dbflux_core::auth::AuthProviderCapabilities {
                login: dbflux_core::auth::AuthProviderLoginCapabilities {
                    supported: true,
                    verification_url_progress: true,
                },
                edit: None,
            },
            supported_versions: dbflux_ipc::auth_provider_rpc_supported_versions().to_vec(),
            expected_connections: 5,
            validate_session: FakeAuthRpcResult::Ok(AuthSessionState::Valid { expires_at: None }),
            validate_session_sequence: Vec::new(),
            login_progress: Some(Some("https://verify.example".to_string())),
            login: FakeAuthRpcResult::Ok(dbflux_ipc::AuthSessionDto {
                provider_id: "rpc-auth".to_string(),
                profile_id,
                expires_at: None,
                session_data: None,
            }),
            resolve_credentials: FakeAuthRpcResult::Ok(credentials),
            fetch_dynamic_options: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
                code:
                    dbflux_ipc::auth_provider_protocol::AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "fetch_dynamic_options not configured".to_string(),
                retriable: false,
            }),
            expected_auth_token: std::env::var(dbflux_ipc::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
                .ok()
                .filter(|token| !token.is_empty()),
        })
        .expect("start fake auth provider server");

        let direct_probe = dbflux_ipc::RpcAuthProvider::probe("mcp-auth", None)
            .expect("direct probe should succeed before registry build");
        assert_eq!(direct_probe.provider_id(), "rpc-auth");

        let services = dbflux_storage::load_service_configs(&runtime);
        let registry = build_auth_provider_registry(&services);
        let provider = registry.get("rpc-auth").expect("registered provider");

        let state = provider
            .validate_session(&profile)
            .await
            .expect("validate session");
        assert!(matches!(state, AuthSessionState::Valid { .. }));

        let callback_url = Arc::new(std::sync::Mutex::new(None));
        let callback_url_for_login = callback_url.clone();
        let session = provider
            .login(
                &profile,
                Box::new(move |url| {
                    *callback_url_for_login.lock().expect("callback lock") = url;
                }) as UrlCallback,
            )
            .await
            .expect("login");

        assert_eq!(session.provider_id, "rpc-auth");
        assert_eq!(
            callback_url.lock().expect("callback lock").as_deref(),
            Some("https://verify.example")
        );

        let resolved = provider
            .resolve_credentials(&profile)
            .await
            .expect("resolve credentials");
        assert_eq!(
            resolved.fields.get("region").map(String::as_str),
            Some("us-east-1")
        );
        assert_eq!(
            resolved
                .secret_fields
                .get("token")
                .map(|value| value.expose_secret()),
            Some("secret-token")
        );

        server.wait().expect("fake server join");
    }

    #[test]
    fn build_auth_provider_registry_skips_incompatible_persisted_service() {
        let mut services = vec![ServiceConfig {
            socket_id: "bad-auth".to_string(),
            enabled: true,
            command: None,
            args: Vec::new(),
            env: HashMap::new(),
            startup_timeout_ms: None,
            kind: RpcServiceKind::AuthProvider,
            api_contract: Some(dbflux_core::ServiceRpcApiContract::new("driver_rpc", 1, 1)),
        }];

        let registry = build_auth_provider_registry(&services);
        assert!(!registry.contains_key("bad-auth"));

        services[0].api_contract = Some(dbflux_core::ServiceRpcApiContract::new(
            "auth_provider_rpc",
            2,
            0,
        ));

        let registry = build_auth_provider_registry(&services);
        assert!(!registry.contains_key("bad-auth"));
    }

    #[tokio::test]
    async fn rpc_auth_provider_login_without_progress_reports_none_callback() {
        let server = FakeAuthProviderRpcServer::start(FakeAuthProviderRpcConfig {
            socket_id: "no-progress-auth".to_string(),
            provider_id: "rpc-auth".to_string(),
            display_name: "RPC Auth".to_string(),
            form_definition: dbflux_core::AuthFormDef { tabs: vec![] },
            capabilities: dbflux_core::auth::AuthProviderCapabilities {
                login: dbflux_core::auth::AuthProviderLoginCapabilities {
                    supported: true,
                    verification_url_progress: false,
                },
                edit: None,
            },
            supported_versions: dbflux_ipc::auth_provider_rpc_supported_versions().to_vec(),
            expected_connections: 2,
            validate_session: FakeAuthRpcResult::Ok(AuthSessionState::LoginRequired),
            validate_session_sequence: Vec::new(),
            login_progress: None,
            login: FakeAuthRpcResult::Ok(dbflux_ipc::AuthSessionDto {
                provider_id: "rpc-auth".to_string(),
                profile_id: uuid::Uuid::nil(),
                expires_at: None,
                session_data: None,
            }),
            resolve_credentials: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
                code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "unused".to_string(),
                retriable: false,
            }),
            fetch_dynamic_options: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
                code:
                    dbflux_ipc::auth_provider_protocol::AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "fetch_dynamic_options not configured".to_string(),
                retriable: false,
            }),
            expected_auth_token: std::env::var(dbflux_ipc::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
                .ok()
                .filter(|token| !token.is_empty()),
        })
        .expect("start fake auth provider server");

        let provider = dbflux_ipc::RpcAuthProvider::probe("no-progress-auth", None)
            .expect("probe provider without progress url");
        let profile = AuthProfile::new("rpc profile", "rpc-auth", HashMap::new());

        let callback_url = Arc::new(std::sync::Mutex::new(Some("unexpected".to_string())));
        let callback_url_for_login = callback_url.clone();

        provider
            .login(
                &profile,
                Box::new(move |url| {
                    *callback_url_for_login.lock().expect("callback lock") = url;
                }) as UrlCallback,
            )
            .await
            .expect("login without progress");

        assert!(callback_url.lock().expect("callback lock").is_none());
        server.wait().expect("fake server join");
    }

    #[tokio::test]
    async fn rpc_auth_provider_login_refreshes_session_after_successful_generic_flow() {
        let profile = AuthProfile::new("rpc profile", "rpc-auth", HashMap::new());
        let profile_id = profile.id;

        let server = FakeAuthProviderRpcServer::start(FakeAuthProviderRpcConfig {
            socket_id: "refreshing-auth".to_string(),
            provider_id: "rpc-auth".to_string(),
            display_name: "RPC Auth".to_string(),
            form_definition: dbflux_core::AuthFormDef { tabs: vec![] },
            capabilities: dbflux_core::auth::AuthProviderCapabilities {
                login: dbflux_core::auth::AuthProviderLoginCapabilities {
                    supported: true,
                    verification_url_progress: true,
                },
                edit: None,
            },
            supported_versions: dbflux_ipc::auth_provider_rpc_supported_versions().to_vec(),
            expected_connections: 4,
            validate_session: FakeAuthRpcResult::Ok(AuthSessionState::Valid { expires_at: None }),
            validate_session_sequence: vec![
                FakeAuthRpcResult::Ok(AuthSessionState::LoginRequired),
                FakeAuthRpcResult::Ok(AuthSessionState::Valid { expires_at: None }),
            ],
            login_progress: Some(Some("https://verify.example/device".to_string())),
            login: FakeAuthRpcResult::Ok(dbflux_ipc::AuthSessionDto {
                provider_id: "rpc-auth".to_string(),
                profile_id,
                expires_at: None,
                session_data: None,
            }),
            resolve_credentials: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
                code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "unused".to_string(),
                retriable: false,
            }),
            fetch_dynamic_options: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
                code:
                    dbflux_ipc::auth_provider_protocol::AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "fetch_dynamic_options not configured".to_string(),
                retriable: false,
            }),
            expected_auth_token: std::env::var(dbflux_ipc::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
                .ok()
                .filter(|token| !token.is_empty()),
        })
        .expect("start refreshing fake auth provider server");

        let provider = dbflux_ipc::RpcAuthProvider::probe("refreshing-auth", None)
            .expect("probe provider with login capability");

        let initial_state = provider
            .validate_session(&profile)
            .await
            .expect("initial validate_session");
        assert!(matches!(initial_state, AuthSessionState::LoginRequired));

        let callback_url = Arc::new(std::sync::Mutex::new(None));
        let callback_url_for_login = callback_url.clone();

        let session = provider
            .login(
                &profile,
                Box::new(move |url| {
                    *callback_url_for_login.lock().expect("callback lock") = url;
                }) as UrlCallback,
            )
            .await
            .expect("generic login should succeed");

        assert_eq!(session.provider_id, "rpc-auth");
        assert_eq!(session.profile_id, profile_id);
        assert_eq!(
            callback_url.lock().expect("callback lock").as_deref(),
            Some("https://verify.example/device")
        );

        let refreshed_state = provider
            .validate_session(&profile)
            .await
            .expect("refreshed validate_session");
        assert!(matches!(
            refreshed_state,
            AuthSessionState::Valid { expires_at: None }
        ));

        server.wait().expect("fake server join");
    }

    #[test]
    fn build_auth_provider_registry_skips_version_mismatch_probe_failure() {
        let (_temp_dir, runtime) = temp_runtime();
        insert_auth_provider_service(&runtime, "version-mismatch-auth");

        let server = FakeAuthProviderRpcServer::start(FakeAuthProviderRpcConfig {
            socket_id: "version-mismatch-auth".to_string(),
            provider_id: "rpc-auth".to_string(),
            display_name: "RPC Auth".to_string(),
            form_definition: dbflux_core::AuthFormDef { tabs: vec![] },
            capabilities: dbflux_core::auth::AuthProviderCapabilities::default(),
            supported_versions: vec![dbflux_ipc::ProtocolVersion::new(2, 0)],
            expected_connections: 1,
            validate_session: FakeAuthRpcResult::Ok(AuthSessionState::LoginRequired),
            validate_session_sequence: Vec::new(),
            login_progress: None,
            login: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
                code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "unused".to_string(),
                retriable: false,
            }),
            resolve_credentials: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
                code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "unused".to_string(),
                retriable: false,
            }),
            fetch_dynamic_options: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
                code:
                    dbflux_ipc::auth_provider_protocol::AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "fetch_dynamic_options not configured".to_string(),
                retriable: false,
            }),
            expected_auth_token: std::env::var(dbflux_ipc::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
                .ok()
                .filter(|token| !token.is_empty()),
        })
        .expect("start mismatch fake auth provider server");

        let services = dbflux_storage::load_service_configs(&runtime);
        let registry = build_auth_provider_registry(&services);

        assert!(!registry.contains_key("rpc-auth"));
        server.wait().expect("fake server join");
    }

    #[test]
    fn build_auth_provider_registry_accepts_legacy_v1_0_auth_provider_probe() {
        let (_temp_dir, runtime) = temp_runtime();
        insert_auth_provider_service(&runtime, "legacy-auth");

        let server = FakeAuthProviderRpcServer::start(FakeAuthProviderRpcConfig {
            socket_id: "legacy-auth".to_string(),
            provider_id: "legacy-rpc-auth".to_string(),
            display_name: "Legacy RPC Auth".to_string(),
            form_definition: dbflux_core::AuthFormDef { tabs: vec![] },
            capabilities: dbflux_core::auth::AuthProviderCapabilities::default(),
            supported_versions: vec![dbflux_ipc::ProtocolVersion::new(1, 0)],
            expected_connections: 1,
            validate_session: FakeAuthRpcResult::Ok(AuthSessionState::LoginRequired),
            validate_session_sequence: Vec::new(),
            login_progress: None,
            login: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
                code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "unused".to_string(),
                retriable: false,
            }),
            resolve_credentials: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
                code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "unused".to_string(),
                retriable: false,
            }),
            fetch_dynamic_options: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
                code:
                    dbflux_ipc::auth_provider_protocol::AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "fetch_dynamic_options not configured".to_string(),
                retriable: false,
            }),
            expected_auth_token: std::env::var(dbflux_ipc::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
                .ok()
                .filter(|token| !token.is_empty()),
        })
        .expect("start legacy fake auth provider server");

        let services = dbflux_storage::load_service_configs(&runtime);
        let registry = build_auth_provider_registry(&services);

        let provider = registry
            .get("legacy-rpc-auth")
            .expect("legacy provider must register");
        assert_eq!(provider.provider_id(), "legacy-rpc-auth");
        assert!(!provider.capabilities().login.supported);

        server.wait().expect("fake server join");
    }

    fn runtime_for_governance_tests(file_name: &str) -> McpRuntime {
        let path = dbflux_audit::temp_sqlite_path(&format!(
            "{}_{}.sqlite",
            file_name.trim_end_matches(".sqlite"),
            uuid::Uuid::new_v4()
        ));
        let audit =
            dbflux_audit::AuditService::new_sqlite(&path).expect("audit service should initialize");
        McpRuntime::new(
            audit,
            Box::new(dbflux_approval::InMemoryPendingExecutionStore::default()),
        )
    }

    #[test]
    fn apply_governance_settings_rejects_client_with_empty_id() {
        use dbflux_core::{GovernanceSettings, TrustedClientConfig};

        let mut runtime = runtime_for_governance_tests("governance-empty-client-id.sqlite");

        let governance = GovernanceSettings {
            mcp_enabled_by_default: false,
            trusted_clients: vec![TrustedClientConfig {
                id: String::new(),
                name: "bad client".to_string(),
                issuer: None,
                active: true,
            }],
            roles: vec![],
            policies: vec![],
        };

        let result = apply_governance_settings(&mut runtime, &governance);
        assert!(result.is_err(), "empty client id must cause an Err");
        assert!(
            result
                .unwrap_err()
                .contains("failed to load trusted client"),
            "error message must identify the failed operation"
        );
    }

    #[test]
    fn apply_governance_settings_rejects_role_with_empty_id() {
        use dbflux_core::{GovernanceSettings, PolicyRoleConfig};

        let mut runtime = runtime_for_governance_tests("governance-empty-role-id.sqlite");

        let governance = GovernanceSettings {
            mcp_enabled_by_default: false,
            trusted_clients: vec![],
            roles: vec![PolicyRoleConfig {
                id: String::new(),
                policy_ids: vec![],
            }],
            policies: vec![],
        };

        let result = apply_governance_settings(&mut runtime, &governance);
        assert!(result.is_err(), "empty role id must cause an Err");
        assert!(
            result.unwrap_err().contains("failed to load role"),
            "error message must identify the failed operation"
        );
    }

    #[test]
    fn apply_governance_settings_rejects_policy_with_empty_id() {
        use dbflux_core::{GovernanceSettings, ToolPolicyConfig};

        let mut runtime = runtime_for_governance_tests("governance-empty-policy-id.sqlite");

        let governance = GovernanceSettings {
            mcp_enabled_by_default: false,
            trusted_clients: vec![],
            roles: vec![],
            policies: vec![ToolPolicyConfig {
                id: String::new(),
                allowed_tools: vec![],
                allowed_classes: vec![],
                approval_classes: vec![],
            }],
        };

        let result = apply_governance_settings(&mut runtime, &governance);
        assert!(result.is_err(), "empty policy id must cause an Err");
        assert!(
            result.unwrap_err().contains("failed to load policy"),
            "error message must identify the failed operation"
        );
    }

    #[test]
    fn apply_governance_settings_accepts_well_formed_entries() {
        use dbflux_core::{
            GovernanceSettings, PolicyRoleConfig, ToolPolicyConfig, TrustedClientConfig,
        };

        let mut runtime = runtime_for_governance_tests("governance-well-formed.sqlite");

        let governance = GovernanceSettings {
            mcp_enabled_by_default: true,
            trusted_clients: vec![TrustedClientConfig {
                id: "claude".to_string(),
                name: "Claude".to_string(),
                issuer: None,
                active: true,
            }],
            roles: vec![PolicyRoleConfig {
                id: "custom/analyst".to_string(),
                policy_ids: vec!["read-only".to_string()],
            }],
            policies: vec![ToolPolicyConfig {
                id: "read-only".to_string(),
                allowed_tools: vec!["select_data".to_string()],
                allowed_classes: vec![],
                approval_classes: vec![],
            }],
        };

        let result = apply_governance_settings(&mut runtime, &governance);
        assert!(
            result.is_ok(),
            "well-formed governance settings must succeed"
        );

        let clients = runtime
            .list_trusted_clients()
            .expect("list trusted clients");
        assert!(clients.iter().any(|c| c.id == "claude"));

        let roles = runtime.list_roles().expect("list roles");
        assert!(roles.iter().any(|r| r.id == "custom/analyst"));

        let policies = runtime.list_policies().expect("list policies");
        assert!(policies.iter().any(|p| p.id == "read-only"));
    }

    #[test]
    fn load_profiles_reads_the_persisted_read_only_flag() {
        let runtime = StorageRuntime::in_memory().expect("in-memory storage runtime");
        let repo = runtime.connection_profiles();

        let locked_id = uuid::Uuid::new_v4();
        let mut locked =
            dbflux_storage::repositories::connection_profiles::ConnectionProfileDto::new(
                locked_id,
                "locked".to_string(),
            );
        locked.kind = Some("Turso".to_string());
        locked.read_only = true;
        repo.insert(&locked).expect("insert read-only profile");

        let writable_id = uuid::Uuid::new_v4();
        let mut writable =
            dbflux_storage::repositories::connection_profiles::ConnectionProfileDto::new(
                writable_id,
                "writable".to_string(),
            );
        writable.kind = Some("Turso".to_string());
        repo.insert(&writable).expect("insert writable profile");

        let (profiles, profile_load_errors) = load_profiles(&runtime).expect("load profiles");
        assert!(profile_load_errors.is_empty());
        let find = |id| {
            profiles
                .iter()
                .find(|profile| profile.id == id)
                .expect("loaded profile")
        };

        assert!(find(locked_id).read_only_flag);
        assert!(!find(writable_id).read_only_flag);
    }

    #[test]
    fn turso_headless_config_uses_the_same_empty_url_default() {
        assert_eq!(str_to_db_kind("Turso"), Some(dbflux_core::DbKind::Turso));
        assert!(matches!(
            default_db_config_for_kind(dbflux_core::DbKind::Turso),
            dbflux_core::DbConfig::Turso { ref url } if url.is_empty()
        ));
    }
}
