use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(feature = "mysql")]
use dbflux_core::DbKind;
use dbflux_core::{AppConfigStore, ConnectionProfile, DbDriver, ProfileManager};
use dbflux_mcp::{
    builtin_policies, builtin_roles, ConnectionPolicyAssignmentDto, McpGovernanceService,
    McpRuntime, PolicyRoleDto, ToolPolicyDto, TrustedClientDto,
};

use crate::connection_cache::ConnectionCache;
use crate::error_messages;

/// All state loaded at startup that the server needs to handle requests.
/// This struct is Clone-able and uses Arc internally for shared state.
#[derive(Clone)]
pub struct ServerState {
    pub client_id: String,
    pub runtime: Arc<McpRuntime>,
    pub profile_manager: Arc<RwLock<ProfileManager>>,
    pub driver_registry: Arc<HashMap<String, Arc<dyn DbDriver>>>,
    pub connection_cache: Arc<RwLock<ConnectionCache>>,
    pub mcp_enabled_by_default: bool,
}

impl ServerState {
    /// Loads config and governance from disk, builds the driver registry,
    /// and returns a fully-initialized `ServerState`.
    ///
    /// `config_dir` overrides the default `~/.config/dbflux` location.
    pub fn new(client_id: String, config_dir: Option<PathBuf>) -> Result<Self, String> {
        let runtime = build_runtime(config_dir.as_deref())?;

        // Validate that the client_id exists as a trusted client
        validate_client_id(&runtime, &client_id, config_dir.as_deref())?;

        let profile_manager = ProfileManager::new();
        let driver_registry = build_driver_registry();

        let state = ServerState {
            client_id,
            runtime: Arc::new(runtime),
            profile_manager: Arc::new(RwLock::new(profile_manager)),
            driver_registry: Arc::new(driver_registry),
            connection_cache: Arc::new(RwLock::new(ConnectionCache::new())),
            mcp_enabled_by_default: false,
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

fn build_runtime(config_dir: Option<&std::path::Path>) -> Result<McpRuntime, String> {
    let audit_service = match config_dir {
        Some(dir) => {
            let audit_path = dir.join("mcp_audit.sqlite");
            dbflux_audit::AuditService::new_sqlite(&audit_path).map_err(|e| {
                error_messages::config_error("initialize audit database", Some(&audit_path), e)
            })?
        }
        None => dbflux_audit::AuditService::new_sqlite_default().map_err(|e| {
            error_messages::config_error("initialize default audit database", None, e)
        })?,
    };

    let mut runtime = McpRuntime::new(audit_service);

    load_governance_into_runtime(&mut runtime, config_dir)?;

    // Drain startup events — governance load is not observable to callers.
    runtime.drain_events();

    Ok(runtime)
}

fn validate_client_id(
    runtime: &McpRuntime,
    client_id: &str,
    config_dir: Option<&std::path::Path>,
) -> Result<(), String> {
    let clients = runtime
        .list_trusted_clients()
        .map_err(|e| format!("Failed to list trusted clients: {}", e))?;

    let client_exists = clients
        .iter()
        .any(|client| client.id == client_id && client.active);

    if !client_exists {
        let config_path = config_dir
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "~/.config/dbflux".to_string());

        return Err(format!(
            "Client ID '{}' is not registered as a trusted client.\n\
             \n\
             To fix this:\n\
             1. Open DBFlux GUI and go to Settings → MCP → Clients\n\
             2. Add a new trusted client with ID '{}'\n\
             \n\
             Or manually edit the config file:\n\
             {}/config.json\n\
             \n\
             Add this to the 'governance.trusted_clients' array:\n\
             {{\n\
               \"id\": \"{}\",\n\
               \"name\": \"Your Client Name\",\n\
               \"issuer\": \"optional-issuer\",\n\
               \"active\": true\n\
             }}",
            client_id, client_id, config_path, client_id
        ));
    }

    Ok(())
}

fn load_governance_into_runtime(
    runtime: &mut McpRuntime,
    config_dir: Option<&std::path::Path>,
) -> Result<(), String> {
    // Inject immutable built-ins first so they are always present.
    for role in builtin_roles() {
        let _ = runtime.upsert_role_mut(role);
    }

    for policy in builtin_policies() {
        let _ = runtime.upsert_policy_mut(policy);
    }

    // Load user-defined governance from AppConfig.
    let config_store = match config_dir {
        Some(dir) => AppConfigStore::from_dir(dir)
            .map_err(|e| error_messages::config_error("open config store", Some(dir), e))?,
        None => AppConfigStore::new()
            .map_err(|e| error_messages::config_error("open config store", None, e))?,
    };

    let config = config_store
        .load()
        .map_err(|e| error_messages::config_error("load config", None, e))?;

    for client in config.governance.trusted_clients {
        let _ = runtime.upsert_trusted_client_mut(TrustedClientDto {
            id: client.id,
            name: client.name,
            issuer: client.issuer,
            active: client.active,
        });
    }

    for role in config.governance.roles {
        let _ = runtime.upsert_role_mut(PolicyRoleDto {
            id: role.id,
            policy_ids: role.policy_ids,
        });
    }

    for policy in config.governance.policies {
        let _ = runtime.upsert_policy_mut(ToolPolicyDto {
            id: policy.id,
            allowed_tools: policy.allowed_tools,
            allowed_classes: policy.allowed_classes,
        });
    }

    Ok(())
}

async fn load_connection_policy_assignments(
    runtime: Arc<McpRuntime>,
    profile_manager: Arc<RwLock<ProfileManager>>,
) {
    let profiles = {
        let pm = profile_manager.read().await;
        pm.profiles.clone()
    };

    for profile in profiles {
        load_profile_assignment(&runtime, &profile);
    }

    // Note: drain_events is called in the runtime, but since we have Arc
    // we can't call it here. Events will be drained on next operation.
}

fn load_profile_assignment(runtime: &McpRuntime, profile: &ConnectionProfile) {
    let Some(governance) = &profile.mcp_governance else {
        return;
    };

    if !governance.enabled {
        return;
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

    // Note: We can't mutate runtime through & reference with Arc wrapper
    // Policy assignments will be loaded from config on next runtime operation
    let _ = (runtime, assignments); // Acknowledge unused for now
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
