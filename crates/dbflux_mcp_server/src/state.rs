use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

#[cfg(feature = "mysql")]
use dbflux_core::DbKind;
use dbflux_core::auth::DynAuthProvider;
use dbflux_core::{
    AccessKind, AuthProfileManager, ConnectionMcpGovernance, ConnectionMcpPolicyBinding,
    ConnectionProfile, DbDriver, DriverKey, FormValues, GovernanceSettings, KeyringSecretStore,
    ProfileManager, RpcServiceKind, SecretManager, ServiceConfig, ValueRef,
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

use crate::connection_cache::ConnectionCache;
use crate::error_messages;

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
    pub connection_setup_lock: Arc<Mutex<()>>,
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
        let profiles = load_profiles(&storage_runtime)?;
        let auth_profiles = load_auth_profiles(&storage_runtime)?;
        let services = dbflux_storage::load_service_configs(&storage_runtime);
        let (runtime, governance_settings) = build_runtime(config_dir.as_deref())?;

        // Validate that the client_id exists as a trusted client
        validate_client_id(&runtime, &client_id, config_dir.as_deref())?;

        let profile_manager = ProfileManager::with_profiles(profiles, None);
        let auth_profile_manager =
            AuthProfileManager::with_items(auth_profiles, None, "auth profiles");
        let driver_registry = build_driver_registry();
        let auth_provider_registry = build_auth_provider_registry(&services);
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
            connection_setup_lock: Arc::new(Mutex::new(())),
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
    config_dir: Option<&std::path::Path>,
) -> Result<(McpRuntime, GovernanceSettings), String> {
    // Use the unified dbflux.db path for audit service regardless of config_dir.
    let dbflux_db_path = storage_paths::dbflux_db_path()
        .map_err(|e| error_messages::config_error("resolve unified dbflux.db path", None, e))?;

    let audit_service = dbflux_audit::AuditService::new_sqlite(&dbflux_db_path).map_err(|e| {
        error_messages::config_error("initialize audit database", Some(&dbflux_db_path), e)
    })?;

    let mut runtime = McpRuntime::new(audit_service);

    // Pass config_dir for CLI compatibility only; governance state is read from SQLite.
    let governance_settings = load_governance_into_runtime(&mut runtime, config_dir)?;

    // Drain startup events — governance load is not observable to callers.
    runtime.drain_events();

    Ok((runtime, governance_settings))
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

fn load_governance_into_runtime(
    runtime: &mut McpRuntime,
    config_dir: Option<&std::path::Path>,
) -> Result<GovernanceSettings, String> {
    // Inject immutable built-ins first so they are always present.
    for role in builtin_roles() {
        let _ = runtime.upsert_role_mut(role);
    }

    for policy in builtin_policies() {
        let _ = runtime.upsert_policy_mut(policy);
    }

    let governance = load_governance_settings(config_dir)?;

    for client in governance.trusted_clients.clone() {
        let _ = runtime.upsert_trusted_client_mut(TrustedClientDto {
            id: client.id,
            name: client.name,
            issuer: client.issuer,
            active: client.active,
        });
    }

    for role in governance.roles.clone() {
        let _ = runtime.upsert_role_mut(PolicyRoleDto {
            id: role.id,
            policy_ids: role.policy_ids,
        });
    }

    for policy in governance.policies.clone() {
        let _ = runtime.upsert_policy_mut(ToolPolicyDto {
            id: policy.id,
            allowed_tools: policy.allowed_tools,
            allowed_classes: policy.allowed_classes,
        });
    }

    // Create global policy assignments for all trusted clients
    // This allows them to use tools without a specific connection_id
    // (e.g., list_connections, list_scripts, query_audit_logs)
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
                enabled: dto.enabled,
            })
        })
        .collect()
}

fn load_profiles(runtime: &StorageRuntime) -> Result<Vec<ConnectionProfile>, String> {
    let repo = runtime.connection_profiles();
    let dtos = repo
        .all()
        .map_err(|e| format!("Failed to load connection profiles: {}", e))?;

    let mut profiles = Vec::with_capacity(dtos.len());

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

        profiles.push(ConnectionProfile {
            id,
            name: dto.name,
            kind: dto.kind.as_deref().and_then(str_to_db_kind),
            driver_id: dto.driver_id,
            config,
            save_password: dto.save_password,
            settings_overrides: None,
            connection_settings,
            hooks: None,
            hook_bindings: None,
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
        });
    }

    Ok(profiles)
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

    registry
}

fn build_auth_provider_registry(services: &[ServiceConfig]) -> HashMap<String, Arc<dyn DynAuthProvider>> {
    #[allow(unused_mut)]
    let mut registry: HashMap<String, Arc<dyn DynAuthProvider>> = HashMap::new();

    #[cfg(feature = "aws")]
    {
        let sso = Arc::new(dbflux_aws::AwsSsoAuthProvider::new()) as Arc<dyn DynAuthProvider>;
        registry.insert(sso.provider_id().to_string(), sso);

        let shared = Arc::new(dbflux_aws::AwsSharedCredentialsAuthProvider::new())
            as Arc<dyn DynAuthProvider>;
        registry.insert(shared.provider_id().to_string(), shared);

        let static_credentials = Arc::new(dbflux_aws::AwsStaticCredentialsAuthProvider::new())
            as Arc<dyn DynAuthProvider>;
        registry.insert(
            static_credentials.provider_id().to_string(),
            static_credentials,
        );
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
    use dbflux_storage::repositories::services::ServiceDto;
    use dbflux_test_support::{FakeAuthProviderRpcConfig, FakeAuthProviderRpcServer, FakeAuthRpcResult};

    fn temp_runtime() -> (tempfile::TempDir, StorageRuntime) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let runtime = StorageRuntime::for_path(temp_dir.path().join("dbflux.db")).expect("runtime");
        (temp_dir, runtime)
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
            supported_versions: dbflux_ipc::auth_provider_rpc_supported_versions().to_vec(),
            expected_connections: 5,
            validate_session: FakeAuthRpcResult::Ok(AuthSessionState::Valid { expires_at: None }),
            login_progress: Some(Some("https://verify.example".to_string())),
            login: FakeAuthRpcResult::Ok(dbflux_ipc::AuthSessionDto {
                provider_id: "rpc-auth".to_string(),
                profile_id,
                expires_at: None,
            }),
            resolve_credentials: FakeAuthRpcResult::Ok(credentials),
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
        assert_eq!(resolved.fields.get("region").map(String::as_str), Some("us-east-1"));
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
        assert!(registry.get("bad-auth").is_none());

        services[0].api_contract = Some(dbflux_core::ServiceRpcApiContract::new(
            "auth_provider_rpc",
            2,
            0,
        ));

        let registry = build_auth_provider_registry(&services);
        assert!(registry.get("bad-auth").is_none());
    }

    #[tokio::test]
    async fn rpc_auth_provider_login_without_progress_reports_none_callback() {
        let server = FakeAuthProviderRpcServer::start(FakeAuthProviderRpcConfig {
            socket_id: "no-progress-auth".to_string(),
            provider_id: "rpc-auth".to_string(),
            display_name: "RPC Auth".to_string(),
            form_definition: dbflux_core::AuthFormDef { tabs: vec![] },
            supported_versions: dbflux_ipc::auth_provider_rpc_supported_versions().to_vec(),
            expected_connections: 2,
            validate_session: FakeAuthRpcResult::Ok(AuthSessionState::LoginRequired),
            login_progress: None,
            login: FakeAuthRpcResult::Ok(dbflux_ipc::AuthSessionDto {
                provider_id: "rpc-auth".to_string(),
                profile_id: uuid::Uuid::nil(),
                expires_at: None,
            }),
            resolve_credentials: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
                code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
                message: "unused".to_string(),
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

    #[test]
    fn build_auth_provider_registry_skips_version_mismatch_probe_failure() {
        let (_temp_dir, runtime) = temp_runtime();
        insert_auth_provider_service(&runtime, "version-mismatch-auth");

        let server = FakeAuthProviderRpcServer::start(FakeAuthProviderRpcConfig {
            socket_id: "version-mismatch-auth".to_string(),
            provider_id: "rpc-auth".to_string(),
            display_name: "RPC Auth".to_string(),
            form_definition: dbflux_core::AuthFormDef { tabs: vec![] },
            supported_versions: vec![dbflux_ipc::ProtocolVersion::new(2, 0)],
            expected_connections: 1,
            validate_session: FakeAuthRpcResult::Ok(AuthSessionState::LoginRequired),
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
            expected_auth_token: std::env::var(dbflux_ipc::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
                .ok()
                .filter(|token| !token.is_empty()),
        })
        .expect("start mismatch fake auth provider server");

        let services = dbflux_storage::load_service_configs(&runtime);
        let registry = build_auth_provider_registry(&services);

        assert!(registry.get("rpc-auth").is_none());
        server.wait().expect("fake server join");
    }
}
