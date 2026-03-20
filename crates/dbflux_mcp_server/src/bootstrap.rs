use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "mysql")]
use dbflux_core::DbKind;
use dbflux_core::{AppConfigStore, ConnectionProfile, DbDriver, ProfileManager};
use dbflux_mcp::{
    ConnectionPolicyAssignmentDto, McpRuntime, PolicyRoleDto, ToolPolicyDto, TrustedClientDto,
    builtin_policies, builtin_roles,
};

use crate::connection_cache::ConnectionCache;

/// All state loaded at startup that the server needs to handle requests.
pub struct ServerState {
    pub client_id: String,
    pub runtime: McpRuntime,
    pub profile_manager: ProfileManager,
    pub driver_registry: HashMap<String, Arc<dyn DbDriver>>,
    pub connection_cache: ConnectionCache,
    pub mcp_enabled_by_default: bool,
}

/// Loads config and governance from disk, builds the driver registry,
/// and returns a fully-initialized `ServerState`.
///
/// `config_dir` overrides the default `~/.config/dbflux` location.
pub fn init(client_id: String, config_dir: Option<PathBuf>) -> Result<ServerState, String> {
    let runtime = build_runtime(config_dir.as_deref())?;
    let profile_manager = ProfileManager::new();
    let driver_registry = build_driver_registry();

    let mut state = ServerState {
        client_id,
        runtime,
        profile_manager,
        driver_registry,
        connection_cache: ConnectionCache::new(),
        mcp_enabled_by_default: false,
    };

    load_connection_policy_assignments(&mut state);

    Ok(state)
}

fn build_runtime(config_dir: Option<&std::path::Path>) -> Result<McpRuntime, String> {
    let audit_service = match config_dir {
        Some(dir) => {
            let audit_path = dir.join("mcp_audit.sqlite");
            dbflux_audit::AuditService::new_sqlite(&audit_path)
                .map_err(|e| format!("Failed to init audit service: {e}"))?
        }
        None => dbflux_audit::AuditService::new_sqlite_default()
            .map_err(|e| format!("Failed to init default audit service: {e}"))?,
    };

    let mut runtime = McpRuntime::new(audit_service);

    load_governance_into_runtime(&mut runtime, config_dir)?;

    // Drain startup events — governance load is not observable to callers.
    runtime.drain_events();

    Ok(runtime)
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
            .map_err(|e| format!("Failed to open config store: {e}"))?,
        None => AppConfigStore::new().map_err(|e| format!("Failed to open config store: {e}"))?,
    };

    let config = config_store
        .load()
        .map_err(|e| format!("Failed to load config: {e}"))?;

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

fn load_connection_policy_assignments(state: &mut ServerState) {
    for profile in state.profile_manager.profiles.clone() {
        load_profile_assignment(&mut state.runtime, &profile);
    }

    state.runtime.drain_events();
}

fn load_profile_assignment(runtime: &mut McpRuntime, profile: &ConnectionProfile) {
    let Some(governance) = &profile.mcp_governance else {
        return;
    };

    if !governance.enabled {
        return;
    }

    let assignments = governance
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

    let _ = runtime.save_connection_policy_assignment_mut(ConnectionPolicyAssignmentDto {
        connection_id: profile.id.to_string(),
        assignments,
    });
}

/// Returns `true` if the given connection has MCP access enabled.
pub fn is_mcp_enabled_for_connection(
    profile_manager: &ProfileManager,
    mcp_enabled_by_default: bool,
    connection_id: &str,
) -> bool {
    let Ok(profile_uuid) = connection_id.parse::<uuid::Uuid>() else {
        return false;
    };

    let Some(profile) = profile_manager.find_by_id(profile_uuid) else {
        return false;
    };

    match &profile.mcp_governance {
        Some(governance) => governance.enabled,
        None => mcp_enabled_by_default,
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
