//! Configuration loader that reads and writes all durable config from `config.db` repositories.
//!
//! This is the authoritative config-loading path for the app. It replaces
//! `AppConfigStore` (which reads `config.json`) for all covered durable config domains.

use std::collections::HashMap;

use dbflux_core::{
    AccessKind, ConnectionHook, ConnectionHookBindings, ConnectionHooks, ConnectionMcpGovernance,
    ConnectionMcpPolicyBinding, ConnectionProfile, DbConfig, DbKind, DriverKey, FormValues,
    GeneralSettings, GlobalOverrides, HookExecutionMode, HookFailureMode, HookKind, HookPhase,
    ProxyProfile, ScriptLanguage, ScriptSource, ServiceConfig, SshAuthMethod, SshTunnelConfig,
    SshTunnelProfile, SslMode, ValueRef,
};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::repositories::connection_driver_configs::ConnectionDriverConfigDto;
use dbflux_storage::repositories::connection_profile_governance_binding_policies::ConnectionProfileGovernanceBindingPolicyDto;
use dbflux_storage::repositories::connection_profile_governance_binding_roles::ConnectionProfileGovernanceBindingRoleDto;
use dbflux_storage::repositories::connection_profile_governance_bindings::ConnectionProfileGovernanceBindingDto;
use dbflux_storage::repositories::connection_profile_hook_bindings::ConnectionProfileHookBindingDto;
use dbflux_storage::repositories::connection_profile_hooks::ConnectionProfileHookDto;
use dbflux_storage::repositories::connection_profile_settings::ConnectionProfileSettingDto;
use dbflux_storage::repositories::connection_profile_value_refs::ConnectionProfileValueRefDto;
use dbflux_storage::repositories::connection_profiles::ConnectionProfileDto;
use dbflux_storage::repositories::driver_overrides::DriverOverridesDto;
use dbflux_storage::repositories::driver_setting_values::DriverSettingValueDto;
use dbflux_storage::repositories::general_settings::GeneralSettingsDto;

pub fn save_general_settings(
    runtime: &StorageRuntime,
    settings: &GeneralSettings,
) -> Result<(), dbflux_storage::error::StorageError> {
    // Save to normalized general_settings table
    let repo = runtime.general_settings();
    let dto = GeneralSettingsDto {
        id: 1,
        theme: match settings.theme {
            dbflux_core::ThemeSetting::Light => "light".to_string(),
            dbflux_core::ThemeSetting::Dark => "dark".to_string(),
        },
        restore_session_on_startup: if settings.restore_session_on_startup {
            1
        } else {
            0
        },
        reopen_last_connections: if settings.reopen_last_connections {
            1
        } else {
            0
        },
        default_focus_on_startup: match settings.default_focus_on_startup {
            dbflux_core::StartupFocus::LastTab => "last_tab".to_string(),
            dbflux_core::StartupFocus::Sidebar => "sidebar".to_string(),
        },
        max_history_entries: settings.max_history_entries as i64,
        auto_save_interval_ms: settings.auto_save_interval_ms as i64,
        default_refresh_policy: match settings.default_refresh_policy {
            dbflux_core::RefreshPolicySetting::Interval => "interval".to_string(),
            dbflux_core::RefreshPolicySetting::Manual => "manual".to_string(),
        },
        default_refresh_interval_secs: settings.default_refresh_interval_secs as i32,
        max_concurrent_background_tasks: settings.max_concurrent_background_tasks as i64,
        auto_refresh_pause_on_error: if settings.auto_refresh_pause_on_error {
            1
        } else {
            0
        },
        auto_refresh_only_if_visible: if settings.auto_refresh_only_if_visible {
            1
        } else {
            0
        },
        confirm_dangerous_queries: if settings.confirm_dangerous_queries {
            1
        } else {
            0
        },
        dangerous_requires_where: if settings.dangerous_requires_where {
            1
        } else {
            0
        },
        dangerous_requires_preview: if settings.dangerous_requires_preview {
            1
        } else {
            0
        },
        updated_at: String::new(),
    };
    repo.upsert(&dto)?;

    Ok(())
}

pub fn save_driver_settings(
    runtime: &StorageRuntime,
    overrides: &HashMap<DriverKey, GlobalOverrides>,
    settings: &HashMap<DriverKey, FormValues>,
) -> Result<(), dbflux_storage::error::StorageError> {
    let overrides_repo = runtime.driver_overrides();
    let values_repo = runtime.driver_setting_values();

    // Get existing driver keys from both new and old tables
    let existing_overrides = overrides_repo.all().unwrap_or_default();
    let existing_overrides_keys: std::collections::HashSet<_> = existing_overrides
        .iter()
        .map(|d| d.driver_key.clone())
        .collect();

    // Build the full set of keys present in the desired state.
    let desired: std::collections::HashSet<_> =
        overrides.keys().chain(settings.keys()).cloned().collect();

    // Upsert all driver keys to new tables
    for key in &desired {
        // Save overrides to driver_overrides table
        if let Some(ov) = overrides.get(key) {
            let dto = DriverOverridesDto {
                driver_key: key.clone(),
                refresh_policy: ov.refresh_policy.map(|rp| match rp {
                    dbflux_core::RefreshPolicySetting::Interval => "interval".to_string(),
                    dbflux_core::RefreshPolicySetting::Manual => "manual".to_string(),
                }),
                refresh_interval_secs: ov.refresh_interval_secs.map(|v| v as i32),
                confirm_dangerous: ov.confirm_dangerous.map(|v| if v { 1 } else { 0 }),
                requires_where: ov.requires_where.map(|v| if v { 1 } else { 0 }),
                requires_preview: ov.requires_preview.map(|v| if v { 1 } else { 0 }),
                updated_at: String::new(),
            };
            overrides_repo.upsert(&dto)?;
        } else {
            // If no overrides provided, check if there's an existing override to delete
            if existing_overrides_keys.contains(key) {
                overrides_repo.delete(key)?;
            }
        }

        // Save settings values to driver_setting_values table
        if let Some(sv) = settings.get(key) {
            let values: Vec<DriverSettingValueDto> = sv
                .iter()
                .map(|(k, v)| DriverSettingValueDto {
                    id: uuid::Uuid::new_v4().to_string(),
                    driver_key: key.clone(),
                    setting_key: k.clone(),
                    setting_value: Some(v.clone()),
                })
                .collect();
            values_repo.replace_for_driver(key, &values)?;
        } else {
            values_repo.delete_for_driver(key)?;
        }
    }

    // Delete overrides and values for keys that are in DB but not in the desired state
    for key in existing_overrides_keys.difference(&desired) {
        overrides_repo.delete(key)?;
        values_repo.delete_for_driver(key)?;
    }

    // Also save to old driver_settings table for backward compatibility during migration
    let old_repo = runtime.driver_settings();
    let existing_old = old_repo.all().unwrap_or_default();
    let existing_old_keys: std::collections::HashSet<_> =
        existing_old.iter().map(|d| d.driver_key.clone()).collect();

    for key in &desired {
        let dto = dbflux_storage::repositories::driver_settings::DriverSettingsDto {
            driver_key: key.clone(),
            overrides_json: overrides
                .get(key)
                .map(|ov| serde_json::to_string(ov).unwrap_or_default()),
            settings_json: settings
                .get(key)
                .map(|s| serde_json::to_string(s).unwrap_or_default()),
            updated_at: String::new(),
        };
        old_repo.upsert(&dto)?;
    }

    for key in existing_old_keys.difference(&desired) {
        old_repo.delete(key)?;
    }

    Ok(())
}

pub fn save_hook_definitions(
    runtime: &StorageRuntime,
    hooks: &HashMap<String, dbflux_core::ConnectionHook>,
) -> Result<(), dbflux_storage::error::StorageError> {
    let repo = runtime.hook_definitions();

    // Propagate read errors from the repository.
    let existing_rows = repo.all()?;
    let existing_ids: std::collections::HashSet<_> =
        existing_rows.iter().map(|d| d.id.clone()).collect();

    // Build a name→id map from existing rows for stable IDs.
    let existing_name_to_id: std::collections::HashMap<_, _> = existing_rows
        .iter()
        .map(|d| (d.name.clone(), d.id.clone()))
        .collect();

    // Build the full set of names present in the desired state.
    let desired_names: std::collections::HashSet<_> = hooks.keys().cloned().collect();

    // Upsert all hooks that are in the desired state, using the existing ID or generating a new UUID.
    for (name, hook) in hooks {
        let id = existing_name_to_id
            .get(name)
            .cloned()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let execution_mode = match hook.execution_mode {
            dbflux_core::HookExecutionMode::Blocking => "Blocking",
            dbflux_core::HookExecutionMode::Detached => "Detached",
        }
        .to_string();
        let on_failure = match hook.on_failure {
            dbflux_core::HookFailureMode::Warn => "Warn",
            dbflux_core::HookFailureMode::Ignore => "Ignore",
            dbflux_core::HookFailureMode::Disconnect => "Disconnect",
        }
        .to_string();

        let dto = dbflux_storage::repositories::hook_definitions::HookDefinitionDto {
            id,
            name: name.clone(),
            execution_mode,
            script_ref: hook.ready_signal.clone(),
            cwd: hook.cwd.as_ref().map(|p| p.to_string_lossy().to_string()),
            inherit_env: hook.inherit_env,
            timeout_ms: hook.timeout_ms.map(|v| v as i64),
            ready_signal: hook.ready_signal.clone(),
            on_failure,
            enabled: hook.enabled,
            created_at: String::new(),
            updated_at: String::new(),
        };

        if existing_ids.contains(&dto.id) {
            repo.upsert(&dto)?;
        } else {
            repo.insert(&dto)?;
        }

        let hook_env_repo = repo.env_repo();
        let hook_id_for_child = dto.id.clone();
        hook_env_repo.insert_many(&hook_id_for_child, &hook.env)?;
    }

    // Delete hooks that are in DB but not in the desired state.
    for (name, id) in &existing_name_to_id {
        if !desired_names.contains(name) {
            repo.delete(id)?;
        }
    }

    Ok(())
}

pub fn save_services(
    runtime: &StorageRuntime,
    services: &[ServiceConfig],
) -> Result<(), dbflux_storage::error::StorageError> {
    let repo = runtime.services();

    // Propagate read errors from the repository.
    let existing_rows = repo.all()?;
    let existing_ids: std::collections::HashSet<_> =
        existing_rows.iter().map(|d| d.socket_id.clone()).collect();

    // Build the full set of IDs present in the desired state.
    let desired_ids: std::collections::HashSet<_> =
        services.iter().map(|s| s.socket_id.clone()).collect();

    // Upsert all services that are in the desired state.
    for svc in services {
        let dto = dbflux_storage::repositories::services::ServiceDto {
            socket_id: svc.socket_id.clone(),
            enabled: svc.enabled,
            command: svc.command.clone(),
            startup_timeout_ms: svc.startup_timeout_ms.map(|v| v as i64),
            created_at: String::new(),
            updated_at: String::new(),
        };
        repo.upsert(&dto)?;

        repo.set_args(&svc.socket_id, &svc.args)?;
        repo.set_env(&svc.socket_id, &svc.env)?;
    }

    // Delete services that are in DB but not in the desired state.
    for socket_id in existing_ids.difference(&desired_ids) {
        repo.delete(socket_id)?;
    }

    Ok(())
}

pub fn save_profiles(
    runtime: &StorageRuntime,
    profiles: &[ConnectionProfile],
) -> Result<(), dbflux_storage::error::StorageError> {
    let repo = runtime.connection_profiles();

    for profile in profiles {
        let (access_kind_str, access_provider_str, ssh_tunnel_profile_id_str) =
            access_kind_columns(&profile.access_kind);

        let dto = ConnectionProfileDto {
            id: profile.id.to_string(),
            name: profile.name.clone(),
            driver_id: Some(profile.driver_id()),
            description: None,
            favorite: false,
            color: None,
            icon: None,
            save_password: profile.save_password,
            kind: Some(db_kind_to_str(profile.kind())),
            access_kind: access_kind_str,
            access_provider: access_provider_str,
            auth_profile_id: profile.auth_profile_id.map(|u| u.to_string()),
            proxy_profile_id: profile.proxy_profile_id.map(|u| u.to_string()),
            ssh_tunnel_profile_id: ssh_tunnel_profile_id_str,
            access_profile_id: None,
            created_at: String::new(),
            updated_at: String::new(),
        };

        repo.upsert(&dto)?;

        let profile_id = &profile.id.to_string();

        // DbConfig → connection_driver_configs (native columns)
        let driver_configs_repo = repo.driver_configs();
        driver_configs_repo.delete_for_profile(profile_id)?;
        let driver_dto = db_config_to_dto(profile_id, &profile.config);
        driver_configs_repo.upsert(&driver_dto)?;

        // settings_overrides → connection_profile_settings with "overrides." prefix
        let settings_repo = repo.settings();
        settings_repo.delete_by_key_prefix(profile_id, "overrides.")?;
        if let Some(ref ov) = profile.settings_overrides {
            save_global_overrides(&settings_repo, profile_id, ov)?;
        }

        // connection_settings → connection_profile_settings with "conn." prefix
        settings_repo.delete_by_key_prefix(profile_id, "conn.")?;
        if let Some(ref cs) = profile.connection_settings {
            for (k, v) in cs {
                let setting_dto = dbflux_storage::repositories::connection_profile_settings::ConnectionProfileSettingDto::new(
                    profile_id.clone(),
                    format!("conn.{}", k),
                    Some(v.clone()),
                );
                settings_repo.upsert(&setting_dto)?;
            }
        }

        // hooks → connection_profile_hooks (normalized)
        let hooks_repo = repo.hooks();
        let hook_args_repo = repo.hook_args();
        let hook_envs_repo = repo.hook_envs();
        hooks_repo.delete_for_profile(profile_id)?;
        if let Some(ref hooks) = profile.hooks {
            save_connection_hooks(
                &hooks_repo,
                &hook_args_repo,
                &hook_envs_repo,
                profile_id,
                hooks,
            )?;
        }

        // hook_bindings → connection_profile_hook_bindings (proper rows)
        let bindings_repo = repo.hook_bindings();
        bindings_repo.delete_for_profile(profile_id)?;
        if let Some(ref bindings) = profile.hook_bindings {
            save_hook_bindings(&bindings_repo, profile_id, bindings)?;
        }

        // value_refs → connection_profile_value_refs
        let value_refs_repo = repo.value_refs();
        value_refs_repo.delete_for_profile(profile_id)?;
        for (key, value_ref) in &profile.value_refs {
            let dto = value_ref_to_dto(profile_id, key, value_ref);
            value_refs_repo.insert(&dto)?;
        }

        // access_kind params → connection_profile_access_params
        let access_params_repo = repo.access_params();
        access_params_repo.delete_for_profile(profile_id)?;
        if let Some(AccessKind::Managed { ref params, .. }) = profile.access_kind {
            access_params_repo.upsert_batch(profile_id, params)?;
        }

        // mcp_governance → governance table + binding tables
        let gov_repo = repo.governance();
        let gov_bindings_repo = repo.governance_bindings();
        gov_repo.delete_for_profile(profile_id)?;
        gov_bindings_repo.delete_for_profile(profile_id)?;
        if let Some(ref gov) = profile.mcp_governance {
            let enabled_dto =
                dbflux_storage::repositories::connection_profile_governance::ConnectionProfileGovernanceDto::new(
                    profile_id.clone(),
                    "enabled".to_string(),
                    Some(gov.enabled.to_string()),
                );
            gov_repo.upsert(&enabled_dto)?;

            let gov_binding_roles_repo = repo.governance_binding_roles();
            let gov_binding_policies_repo = repo.governance_binding_policies();
            for (i, binding) in gov.policy_bindings.iter().enumerate() {
                let b_dto = ConnectionProfileGovernanceBindingDto::new(
                    profile_id.clone(),
                    binding.actor_id.clone(),
                    i as i32,
                );
                gov_bindings_repo.insert(&b_dto)?;
                for role_id in &binding.role_ids {
                    let r_dto = ConnectionProfileGovernanceBindingRoleDto::new(
                        b_dto.id.clone(),
                        role_id.clone(),
                    );
                    gov_binding_roles_repo.insert(&r_dto)?;
                }
                for policy_id in &binding.policy_ids {
                    let p_dto = ConnectionProfileGovernanceBindingPolicyDto::new(
                        b_dto.id.clone(),
                        policy_id.clone(),
                    );
                    gov_binding_policies_repo.insert(&p_dto)?;
                }
            }
        }
    }

    Ok(())
}

fn db_kind_to_str(kind: DbKind) -> String {
    match kind {
        DbKind::Postgres => "Postgres",
        DbKind::SQLite => "SQLite",
        DbKind::MySQL => "MySQL",
        DbKind::MariaDB => "MariaDB",
        DbKind::MongoDB => "MongoDB",
        DbKind::Redis => "Redis",
        DbKind::DynamoDB => "DynamoDB",
    }
    .to_string()
}

fn str_to_db_kind(s: &str) -> Option<DbKind> {
    match s {
        "Postgres" => Some(DbKind::Postgres),
        "SQLite" => Some(DbKind::SQLite),
        "MySQL" => Some(DbKind::MySQL),
        "MariaDB" => Some(DbKind::MariaDB),
        "MongoDB" => Some(DbKind::MongoDB),
        "Redis" => Some(DbKind::Redis),
        "DynamoDB" => Some(DbKind::DynamoDB),
        _ => None,
    }
}

fn default_db_config_for_kind(kind: DbKind) -> dbflux_core::DbConfig {
    match kind {
        DbKind::Postgres => dbflux_core::DbConfig::default_postgres(),
        DbKind::SQLite => dbflux_core::DbConfig::default_sqlite(),
        DbKind::MySQL => dbflux_core::DbConfig::default_mysql(),
        DbKind::MongoDB => dbflux_core::DbConfig::default_mongodb(),
        DbKind::Redis => dbflux_core::DbConfig::default_redis(),
        DbKind::DynamoDB => dbflux_core::DbConfig::default_dynamodb(),
        _ => dbflux_core::DbConfig::default_postgres(),
    }
}

fn ssl_mode_to_str(mode: &SslMode) -> String {
    match mode {
        SslMode::Disable => "disable".to_string(),
        SslMode::Prefer => "prefer".to_string(),
        SslMode::Require => "require".to_string(),
    }
}

fn str_to_ssl_mode(s: &str) -> SslMode {
    match s {
        "disable" => SslMode::Disable,
        "require" => SslMode::Require,
        _ => SslMode::Prefer,
    }
}

fn ssh_auth_method_to_str(method: &SshAuthMethod) -> String {
    match method {
        SshAuthMethod::PrivateKey { .. } => "private_key".to_string(),
        SshAuthMethod::Password => "password".to_string(),
    }
}

fn str_to_ssh_auth_method(s: &str) -> SshAuthMethod {
    match s {
        "password" => SshAuthMethod::Password,
        _ => SshAuthMethod::PrivateKey { key_path: None },
    }
}

/// Converts a DbConfig to a ConnectionDriverConfigDto for storage.
fn db_config_to_dto(profile_id: &str, config: &DbConfig) -> ConnectionDriverConfigDto {
    let mut dto =
        ConnectionDriverConfigDto::new(profile_id.to_string(), db_kind_to_str(config.kind()));

    match config {
        DbConfig::Postgres {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            ssl_mode,
            ssh_tunnel,
            ..
        } => {
            dto.use_uri = *use_uri;
            dto.uri = uri.clone();
            dto.host = Some(host.clone());
            dto.port = Some(*port as i32);
            dto.user = Some(user.clone());
            dto.database_name = Some(database.clone());
            dto.ssl_mode = ssl_mode_to_str(ssl_mode);
            if let Some(tunnel) = ssh_tunnel {
                dto.ssh_tunnel_host = Some(tunnel.host.clone());
                dto.ssh_tunnel_port = Some(tunnel.port as i32);
                dto.ssh_tunnel_user = Some(tunnel.user.clone());
                dto.ssh_tunnel_auth_method = ssh_auth_method_to_str(&tunnel.auth_method);
                if let SshAuthMethod::PrivateKey { key_path } = &tunnel.auth_method {
                    dto.ssh_tunnel_key_path =
                        key_path.as_ref().map(|p| p.to_string_lossy().to_string());
                }
            }
        }
        DbConfig::MySQL {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            ssl_mode,
            ssh_tunnel,
            ..
        } => {
            dto.use_uri = *use_uri;
            dto.uri = uri.clone();
            dto.host = Some(host.clone());
            dto.port = Some(*port as i32);
            dto.user = Some(user.clone());
            dto.database_name = database.clone();
            dto.ssl_mode = ssl_mode_to_str(ssl_mode);
            if let Some(tunnel) = ssh_tunnel {
                dto.ssh_tunnel_host = Some(tunnel.host.clone());
                dto.ssh_tunnel_port = Some(tunnel.port as i32);
                dto.ssh_tunnel_user = Some(tunnel.user.clone());
                dto.ssh_tunnel_auth_method = ssh_auth_method_to_str(&tunnel.auth_method);
                if let SshAuthMethod::PrivateKey { key_path } = &tunnel.auth_method {
                    dto.ssh_tunnel_key_path =
                        key_path.as_ref().map(|p| p.to_string_lossy().to_string());
                }
            }
        }
        DbConfig::MongoDB {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            auth_database,
            ssh_tunnel,
            ..
        } => {
            dto.use_uri = *use_uri;
            dto.uri = uri.clone();
            dto.host = Some(host.clone());
            dto.port = Some(*port as i32);
            dto.user = user.clone();
            dto.database_name = database.clone();
            dto.mongo_auth_database = auth_database.clone();
            // Note: MongoDB doesn't have ssl_mode in DbConfig
            if let Some(tunnel) = ssh_tunnel {
                dto.ssh_tunnel_host = Some(tunnel.host.clone());
                dto.ssh_tunnel_port = Some(tunnel.port as i32);
                dto.ssh_tunnel_user = Some(tunnel.user.clone());
                dto.ssh_tunnel_auth_method = ssh_auth_method_to_str(&tunnel.auth_method);
                if let SshAuthMethod::PrivateKey { key_path } = &tunnel.auth_method {
                    dto.ssh_tunnel_key_path =
                        key_path.as_ref().map(|p| p.to_string_lossy().to_string());
                }
            }
        }
        DbConfig::Redis {
            use_uri,
            uri,
            host,
            port,
            user,
            database,
            tls,
            ssh_tunnel,
            ..
        } => {
            dto.use_uri = *use_uri;
            dto.uri = uri.clone();
            dto.host = Some(host.clone());
            dto.port = Some(*port as i32);
            dto.user = user.clone();
            dto.database_name = database.map(|d| d.to_string());
            dto.redis_tls = *tls;
            dto.redis_database = database.map(|d| d as i32);
            if let Some(tunnel) = ssh_tunnel {
                dto.ssh_tunnel_host = Some(tunnel.host.clone());
                dto.ssh_tunnel_port = Some(tunnel.port as i32);
                dto.ssh_tunnel_user = Some(tunnel.user.clone());
                dto.ssh_tunnel_auth_method = ssh_auth_method_to_str(&tunnel.auth_method);
                if let SshAuthMethod::PrivateKey { key_path } = &tunnel.auth_method {
                    dto.ssh_tunnel_key_path =
                        key_path.as_ref().map(|p| p.to_string_lossy().to_string());
                }
            }
        }
        DbConfig::SQLite {
            path,
            connection_id,
        } => {
            dto.sqlite_path = Some(path.to_string_lossy().to_string());
            dto.sqlite_connection_id = connection_id.clone();
        }
        DbConfig::DynamoDB {
            region,
            profile,
            endpoint,
            table,
        } => {
            dto.dynamo_region = Some(region.clone());
            dto.dynamo_profile = profile.clone();
            dto.dynamo_endpoint = endpoint.clone();
            dto.dynamo_table = table.clone();
        }
        DbConfig::External { kind, values } => {
            dto.external_kind = Some(db_kind_to_str(*kind));
            dto.external_values_json = Some(serde_json::to_string(values).unwrap_or_default());
        }
    }

    dto
}

/// Converts a ConnectionDriverConfigDto to a DbConfig for loading.
fn dto_to_db_config(dto: &ConnectionDriverConfigDto) -> Option<DbConfig> {
    let kind = str_to_db_kind(&dto.config_key)?;

    match kind {
        DbKind::Postgres | DbKind::MySQL => {
            let ssh_tunnel = if dto.ssh_tunnel_host.is_some() {
                Some(SshTunnelConfig {
                    host: dto.ssh_tunnel_host.clone()?,
                    port: dto.ssh_tunnel_port? as u16,
                    user: dto.ssh_tunnel_user.clone()?,
                    auth_method: str_to_ssh_auth_method(&dto.ssh_tunnel_auth_method),
                })
            } else {
                None
            };

            Some(DbConfig::Postgres {
                use_uri: dto.use_uri,
                uri: dto.uri.clone(),
                host: dto.host.clone().unwrap_or_default(),
                port: dto.port.unwrap_or(5432) as u16,
                user: dto.user.clone().unwrap_or_default(),
                database: dto.database_name.clone().unwrap_or_default(),
                ssl_mode: str_to_ssl_mode(&dto.ssl_mode),
                ssh_tunnel,
                ssh_tunnel_profile_id: None,
            })
        }
        DbKind::MongoDB => {
            let ssh_tunnel = if dto.ssh_tunnel_host.is_some() {
                Some(SshTunnelConfig {
                    host: dto.ssh_tunnel_host.clone()?,
                    port: dto.ssh_tunnel_port? as u16,
                    user: dto.ssh_tunnel_user.clone()?,
                    auth_method: str_to_ssh_auth_method(&dto.ssh_tunnel_auth_method),
                })
            } else {
                None
            };

            Some(DbConfig::MongoDB {
                use_uri: dto.use_uri,
                uri: dto.uri.clone(),
                host: dto.host.clone().unwrap_or_default(),
                port: dto.port.unwrap_or(27017) as u16,
                user: dto.user.clone(),
                database: dto.database_name.clone(),
                auth_database: dto.mongo_auth_database.clone(),
                ssh_tunnel,
                ssh_tunnel_profile_id: None,
            })
        }
        DbKind::Redis => {
            let ssh_tunnel = if dto.ssh_tunnel_host.is_some() {
                Some(SshTunnelConfig {
                    host: dto.ssh_tunnel_host.clone()?,
                    port: dto.ssh_tunnel_port? as u16,
                    user: dto.ssh_tunnel_user.clone()?,
                    auth_method: str_to_ssh_auth_method(&dto.ssh_tunnel_auth_method),
                })
            } else {
                None
            };

            Some(DbConfig::Redis {
                use_uri: dto.use_uri,
                uri: dto.uri.clone(),
                host: dto.host.clone().unwrap_or_default(),
                port: dto.port.unwrap_or(6379) as u16,
                user: dto.user.clone(),
                database: dto.redis_database.map(|d| d as u32),
                tls: dto.redis_tls,
                ssh_tunnel,
                ssh_tunnel_profile_id: None,
            })
        }
        DbKind::SQLite => Some(DbConfig::SQLite {
            path: std::path::PathBuf::from(dto.sqlite_path.clone().unwrap_or_default()),
            connection_id: dto.sqlite_connection_id.clone(),
        }),
        DbKind::DynamoDB => Some(DbConfig::DynamoDB {
            region: dto.dynamo_region.clone().unwrap_or_default(),
            profile: dto.dynamo_profile.clone(),
            endpoint: dto.dynamo_endpoint.clone(),
            table: dto.dynamo_table.clone(),
        }),
        _ => None,
    }
}

fn access_kind_columns(
    access_kind: &Option<AccessKind>,
) -> (Option<String>, Option<String>, Option<String>) {
    match access_kind {
        None => (None, None, None),
        Some(AccessKind::Direct) => (Some("direct".to_string()), None, None),
        Some(AccessKind::Ssh {
            ssh_tunnel_profile_id,
        }) => (
            Some("ssh".to_string()),
            None,
            Some(ssh_tunnel_profile_id.to_string()),
        ),
        Some(AccessKind::Proxy {
            proxy_profile_id: _,
        }) => (Some("proxy".to_string()), None, None),
        Some(AccessKind::Managed { provider, .. }) => {
            (Some("managed".to_string()), Some(provider.clone()), None)
        }
    }
}

fn save_global_overrides(
    settings_repo: &dbflux_storage::repositories::connection_profile_settings::ConnectionProfileSettingsRepository,
    profile_id: &str,
    ov: &GlobalOverrides,
) -> Result<(), dbflux_storage::error::StorageError> {
    use dbflux_core::RefreshPolicySetting;
    use dbflux_storage::repositories::connection_profile_settings::ConnectionProfileSettingDto;

    if let Some(ref policy) = ov.refresh_policy {
        let v = match policy {
            RefreshPolicySetting::Interval => "interval",
            RefreshPolicySetting::Manual => "manual",
        };
        settings_repo.upsert(&ConnectionProfileSettingDto::new(
            profile_id.to_string(),
            "overrides.refresh_policy".to_string(),
            Some(v.to_string()),
        ))?;
    }
    if let Some(secs) = ov.refresh_interval_secs {
        settings_repo.upsert(&ConnectionProfileSettingDto::new(
            profile_id.to_string(),
            "overrides.refresh_interval_secs".to_string(),
            Some(secs.to_string()),
        ))?;
    }
    if let Some(v) = ov.confirm_dangerous {
        settings_repo.upsert(&ConnectionProfileSettingDto::new(
            profile_id.to_string(),
            "overrides.confirm_dangerous".to_string(),
            Some(v.to_string()),
        ))?;
    }
    if let Some(v) = ov.requires_where {
        settings_repo.upsert(&ConnectionProfileSettingDto::new(
            profile_id.to_string(),
            "overrides.requires_where".to_string(),
            Some(v.to_string()),
        ))?;
    }
    if let Some(v) = ov.requires_preview {
        settings_repo.upsert(&ConnectionProfileSettingDto::new(
            profile_id.to_string(),
            "overrides.requires_preview".to_string(),
            Some(v.to_string()),
        ))?;
    }
    Ok(())
}

fn save_connection_hooks(
    hooks_repo: &dbflux_storage::repositories::connection_profile_hooks::ConnectionProfileHooksRepository,
    hook_args_repo: &dbflux_storage::repositories::connection_profile_hook_args::ConnectionProfileHookArgsRepository,
    hook_envs_repo: &dbflux_storage::repositories::connection_profile_hook_envs::ConnectionProfileHookEnvsRepository,
    profile_id: &str,
    hooks: &ConnectionHooks,
) -> Result<(), dbflux_storage::error::StorageError> {
    let phases = [
        (HookPhase::PreConnect, "pre_connect"),
        (HookPhase::PostConnect, "post_connect"),
        (HookPhase::PreDisconnect, "pre_disconnect"),
        (HookPhase::PostDisconnect, "post_disconnect"),
    ];

    for (phase, phase_str) in &phases {
        for (i, hook) in hooks.phase_hooks(*phase).iter().enumerate() {
            let hook_dto = connection_hook_to_dto(profile_id, phase_str, i as i32, hook);
            let hook_id = hook_dto.id.clone();
            hooks_repo.insert(&hook_dto)?;

            // args
            if let HookKind::Command { ref args, .. } = hook.kind {
                hook_args_repo.insert_batch(&hook_id, args)?;
            }

            // env
            hook_envs_repo.insert_batch(&hook_id, &hook.env)?;
        }
    }

    Ok(())
}

fn connection_hook_to_dto(
    profile_id: &str,
    phase: &str,
    order_index: i32,
    hook: &ConnectionHook,
) -> ConnectionProfileHookDto {
    let execution_mode = match hook.execution_mode {
        HookExecutionMode::Blocking => "blocking",
        HookExecutionMode::Detached => "detached",
    };
    let on_failure = match hook.on_failure {
        HookFailureMode::Disconnect => "disconnect",
        HookFailureMode::Warn => "warn",
        HookFailureMode::Ignore => "ignore",
    };

    let mut dto = ConnectionProfileHookDto {
        id: uuid::Uuid::new_v4().to_string(),
        profile_id: profile_id.to_string(),
        phase: phase.to_string(),
        order_index,
        enabled: hook.enabled,
        hook_kind: String::new(),
        command: None,
        script_language: None,
        script_source_type: None,
        script_content: None,
        script_path: None,
        lua_source_type: None,
        lua_content: None,
        lua_path: None,
        lua_log: true,
        lua_env_read: true,
        lua_conn_metadata: true,
        lua_process_run: false,
        cwd: hook.cwd.as_ref().map(|p| p.to_string_lossy().to_string()),
        inherit_env: hook.inherit_env,
        timeout_ms: hook.timeout_ms.map(|v| v as i64),
        execution_mode: execution_mode.to_string(),
        ready_signal: hook.ready_signal.clone(),
        on_failure: on_failure.to_string(),
    };

    match &hook.kind {
        HookKind::Command { command, .. } => {
            dto.hook_kind = "command".to_string();
            dto.command = Some(command.clone());
        }
        HookKind::Script {
            language, source, ..
        } => {
            dto.hook_kind = "script".to_string();
            dto.script_language = Some(match language {
                ScriptLanguage::Bash => "bash".to_string(),
                ScriptLanguage::Python => "python".to_string(),
            });
            match source {
                ScriptSource::Inline { content } => {
                    dto.script_source_type = Some("inline".to_string());
                    dto.script_content = Some(content.clone());
                }
                ScriptSource::File { path } => {
                    dto.script_source_type = Some("file".to_string());
                    dto.script_path = Some(path.to_string_lossy().to_string());
                }
            }
        }
        HookKind::Lua {
            source,
            capabilities,
        } => {
            dto.hook_kind = "lua".to_string();
            dto.lua_log = capabilities.logging;
            dto.lua_env_read = capabilities.env_read;
            dto.lua_conn_metadata = capabilities.connection_metadata;
            dto.lua_process_run = capabilities.process_run;
            match source {
                ScriptSource::Inline { content } => {
                    dto.lua_source_type = Some("inline".to_string());
                    dto.lua_content = Some(content.clone());
                }
                ScriptSource::File { path } => {
                    dto.lua_source_type = Some("file".to_string());
                    dto.lua_path = Some(path.to_string_lossy().to_string());
                }
            }
        }
    }

    dto
}

fn save_hook_bindings(
    bindings_repo: &dbflux_storage::repositories::connection_profile_hook_bindings::ConnectionProfileHookBindingsRepository,
    profile_id: &str,
    bindings: &ConnectionHookBindings,
) -> Result<(), dbflux_storage::error::StorageError> {
    use dbflux_storage::repositories::connection_profile_hook_bindings::ConnectionProfileHookBindingDto;

    let phases = [
        (HookPhase::PreConnect, "pre_connect"),
        (HookPhase::PostConnect, "post_connect"),
        (HookPhase::PreDisconnect, "pre_disconnect"),
        (HookPhase::PostDisconnect, "post_disconnect"),
    ];

    for (phase, phase_str) in &phases {
        for (i, hook_id) in bindings.phase_bindings(*phase).iter().enumerate() {
            let dto = ConnectionProfileHookBindingDto::new(
                profile_id.to_string(),
                hook_id.clone(),
                phase_str.to_string(),
                i as i32,
            );
            bindings_repo.insert(&dto)?;
        }
    }

    Ok(())
}

fn value_ref_to_dto(
    profile_id: &str,
    key: &str,
    value_ref: &ValueRef,
) -> ConnectionProfileValueRefDto {
    match value_ref {
        ValueRef::Literal { value } => ConnectionProfileValueRefDto::new_literal(
            profile_id.to_string(),
            key.to_string(),
            value.clone(),
        ),
        ValueRef::Env { key: env_key } => ConnectionProfileValueRefDto::new_env(
            profile_id.to_string(),
            key.to_string(),
            env_key.clone(),
        ),
        ValueRef::Secret {
            provider,
            locator,
            json_key,
        } => ConnectionProfileValueRefDto::new_secret(
            profile_id.to_string(),
            key.to_string(),
            provider.clone(),
            locator.clone(),
            json_key.clone(),
        ),
        ValueRef::Parameter {
            provider,
            name,
            json_key,
        } => ConnectionProfileValueRefDto::new_param(
            profile_id.to_string(),
            key.to_string(),
            provider.clone(),
            name.clone(),
            json_key.clone(),
        ),
        ValueRef::Auth { field } => ConnectionProfileValueRefDto::new_auth(
            profile_id.to_string(),
            key.to_string(),
            field.clone(),
        ),
    }
}

pub fn save_auth_profiles(
    runtime: &StorageRuntime,
    profiles: &[dbflux_core::AuthProfile],
) -> Result<(), dbflux_storage::error::StorageError> {
    let repo = runtime.auth_profiles();

    // Propagate read errors from the repository.
    let existing_rows = repo.all()?;
    let existing_ids: std::collections::HashSet<_> =
        existing_rows.iter().map(|d| d.id.clone()).collect();

    // Build the full set of IDs present in the desired state.
    let desired_ids: std::collections::HashSet<_> =
        profiles.iter().map(|p| p.id.to_string()).collect();

    for profile in profiles {
        let dto = dbflux_storage::repositories::auth_profiles::AuthProfileDto {
            id: profile.id.to_string(),
            name: profile.name.clone(),
            provider_id: profile.provider_id.clone(),
            enabled: profile.enabled,
            created_at: String::new(),
            updated_at: String::new(),
        };

        if existing_ids.contains(&dto.id) {
            repo.update(&dto)?;
            // Update fields in child table
            repo.set_fields(&dto.id, &profile.fields)?;
        } else {
            repo.insert(&dto)?;
            // Insert fields in child table
            repo.set_fields(&dto.id, &profile.fields)?;
        }
    }

    // Delete profiles that are in DB but not in the desired state.
    for row in &existing_rows {
        if !desired_ids.contains(&row.id) {
            repo.delete(&row.id)?;
        }
    }

    Ok(())
}

pub fn save_proxy_profiles(
    runtime: &StorageRuntime,
    profiles: &[ProxyProfile],
) -> Result<(), dbflux_storage::error::StorageError> {
    let repo = runtime.proxy_profiles();

    for profile in profiles {
        let kind_str = match profile.kind {
            dbflux_core::ProxyKind::Http => "Http",
            dbflux_core::ProxyKind::Https => "Https",
            dbflux_core::ProxyKind::Socks5 => "Socks5",
        };

        let dto = dbflux_storage::repositories::proxy_profiles::ProxyProfileDto {
            id: profile.id.to_string(),
            name: profile.name.clone(),
            kind: kind_str.to_string(),
            host: profile.host.clone(),
            port: profile.port as i32,
            auth_kind: match &profile.auth {
                dbflux_core::ProxyAuth::None => "none".to_string(),
                dbflux_core::ProxyAuth::Basic { .. } => "basic".to_string(),
            },
            no_proxy: profile.no_proxy.clone(),
            enabled: profile.enabled,
            save_secret: profile.save_secret,
            created_at: String::new(),
            updated_at: String::new(),
        };

        // Convert ProxyAuth to ProxyAuthDto for child table
        let auth_dto = match &profile.auth {
            dbflux_core::ProxyAuth::Basic { username } => {
                Some(dbflux_storage::repositories::proxy_auth::ProxyAuthDto {
                    proxy_profile_id: profile.id.to_string(),
                    username: Some(username.clone()),
                    domain: None,
                    password_secret_ref: None,
                })
            }
            dbflux_core::ProxyAuth::None => None,
        };

        repo.upsert(&dto, auth_dto.as_ref())?;
    }

    Ok(())
}

pub fn save_ssh_tunnels(
    runtime: &StorageRuntime,
    tunnels: &[SshTunnelProfile],
) -> Result<(), dbflux_storage::error::StorageError> {
    let repo = runtime.ssh_tunnels();

    for tunnel in tunnels {
        let auth_method_str = match &tunnel.config.auth_method {
            dbflux_core::SshAuthMethod::PrivateKey { .. } => "key",
            dbflux_core::SshAuthMethod::Password => "password",
        };

        let dto = dbflux_storage::repositories::ssh_tunnel_profiles::SshTunnelProfileDto {
            id: tunnel.id.to_string(),
            name: tunnel.name.clone(),
            host: tunnel.config.host.clone(),
            port: tunnel.config.port as i32,
            user: tunnel.config.user.clone(),
            auth_method: auth_method_str.to_string(),
            key_path: None,
            passphrase_secret_ref: None,
            password_secret_ref: None,
            save_secret: tunnel.save_secret,
            created_at: String::new(),
            updated_at: String::new(),
        };

        // Convert SshAuthMethod to SshTunnelAuthDto for child table
        let auth_dto = match &tunnel.config.auth_method {
            dbflux_core::SshAuthMethod::PrivateKey { key_path } => Some(
                dbflux_storage::repositories::ssh_tunnel_auth::SshTunnelAuthDto {
                    ssh_tunnel_profile_id: tunnel.id.to_string(),
                    key_path: key_path.as_ref().map(|p| p.to_string_lossy().to_string()),
                    password_secret_ref: None,
                    passphrase_secret_ref: None,
                },
            ),
            dbflux_core::SshAuthMethod::Password => Some(
                dbflux_storage::repositories::ssh_tunnel_auth::SshTunnelAuthDto {
                    ssh_tunnel_profile_id: tunnel.id.to_string(),
                    key_path: None,
                    password_secret_ref: Some("dbflux:secret:ssh:password:placeholder".to_string()),
                    passphrase_secret_ref: None,
                },
            ),
        };

        repo.upsert(&dto, auth_dto.as_ref())?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Configuration loading (read path - already migrated)
// ---------------------------------------------------------------------------

/// Loaded durable configuration from `config.db`.
pub struct LoadedConfig {
    pub general_settings: GeneralSettings,
    pub driver_overrides: HashMap<DriverKey, GlobalOverrides>,
    pub driver_settings: HashMap<DriverKey, FormValues>,
    pub hook_definitions: HashMap<String, dbflux_core::ConnectionHook>,
    pub services: Vec<ServiceConfig>,
    pub profiles: Vec<ConnectionProfile>,
    pub auth_profiles: Vec<dbflux_core::AuthProfile>,
    pub proxy_profiles: Vec<ProxyProfile>,
    pub ssh_tunnels: Vec<SshTunnelProfile>,
}

/// Loads all durable config domains from `config.db`.
///
/// Uses sensible defaults when repositories are empty (fresh install).
/// This function is the single entry point for loading all covered durable config
/// domains from SQLite storage.
pub fn load_config(runtime: &StorageRuntime) -> LoadedConfig {
    let profiles_repo = runtime.connection_profiles();
    let auth_repo = runtime.auth_profiles();
    let proxy_repo = runtime.proxy_profiles();
    let ssh_repo = runtime.ssh_tunnels();
    let hooks_repo = runtime.hook_definitions();
    let services_repo = runtime.services();

    let general_settings = load_general_settings(&runtime.general_settings());
    let (driver_overrides, driver_settings) = load_driver_maps(
        &runtime.driver_overrides(),
        &runtime.driver_setting_values(),
    );
    let hook_definitions = load_hook_definitions(&hooks_repo);
    let services = load_services(&services_repo);
    let profiles = load_profiles(&profiles_repo);
    let auth_profiles = load_auth_profiles(&auth_repo);
    let proxy_profiles = load_proxy_profiles(&proxy_repo, &proxy_repo.auth_repo());
    let ssh_tunnels = load_ssh_tunnels(&ssh_repo);

    LoadedConfig {
        general_settings,
        driver_overrides,
        driver_settings,
        hook_definitions,
        services,
        profiles,
        auth_profiles,
        proxy_profiles,
        ssh_tunnels,
    }
}

// ---------------------------------------------------------------------------
// General Settings helpers
// ---------------------------------------------------------------------------

fn load_general_settings(
    repo: &dbflux_storage::repositories::general_settings::GeneralSettingsRepository,
) -> GeneralSettings {
    let dto = match repo.get() {
        Ok(Some(dto)) => dto,
        Ok(None) => {
            // No settings yet, use defaults
            return GeneralSettings::default();
        }
        Err(e) => {
            log::warn!("Failed to load general settings, using defaults: {}", e);
            return GeneralSettings::default();
        }
    };

    GeneralSettings {
        theme: match dto.theme.as_str() {
            "light" => dbflux_core::ThemeSetting::Light,
            _ => dbflux_core::ThemeSetting::Dark,
        },
        restore_session_on_startup: dto.restore_session_on_startup != 0,
        reopen_last_connections: dto.reopen_last_connections != 0,
        default_focus_on_startup: match dto.default_focus_on_startup.as_str() {
            "last_tab" => dbflux_core::StartupFocus::LastTab,
            _ => dbflux_core::StartupFocus::Sidebar,
        },
        max_history_entries: dto.max_history_entries as usize,
        auto_save_interval_ms: dto.auto_save_interval_ms as u64,
        default_refresh_policy: match dto.default_refresh_policy.as_str() {
            "interval" => dbflux_core::RefreshPolicySetting::Interval,
            _ => dbflux_core::RefreshPolicySetting::Manual,
        },
        default_refresh_interval_secs: dto.default_refresh_interval_secs as u32,
        max_concurrent_background_tasks: dto.max_concurrent_background_tasks as usize,
        auto_refresh_pause_on_error: dto.auto_refresh_pause_on_error != 0,
        auto_refresh_only_if_visible: dto.auto_refresh_only_if_visible != 0,
        confirm_dangerous_queries: dto.confirm_dangerous_queries != 0,
        dangerous_requires_where: dto.dangerous_requires_where != 0,
        dangerous_requires_preview: dto.dangerous_requires_preview != 0,
    }
}

// ---------------------------------------------------------------------------
// Hook Definitions helpers
// ---------------------------------------------------------------------------
// Driver Maps helpers
// ---------------------------------------------------------------------------

fn load_driver_maps(
    overrides_repo: &dbflux_storage::repositories::driver_overrides::DriverOverridesRepository,
    values_repo: &dbflux_storage::repositories::driver_setting_values::DriverSettingValuesRepository,
) -> (
    HashMap<DriverKey, GlobalOverrides>,
    HashMap<DriverKey, FormValues>,
) {
    let mut overrides = HashMap::new();
    let mut settings = HashMap::new();

    if let Ok(entries) = overrides_repo.all() {
        for entry in entries {
            let key = entry.driver_key.clone();
            let refresh_policy = entry.refresh_policy.as_ref().map(|rp| match rp.as_str() {
                "interval" => dbflux_core::RefreshPolicySetting::Interval,
                _ => dbflux_core::RefreshPolicySetting::Manual,
            });

            let ov = GlobalOverrides {
                refresh_policy,
                refresh_interval_secs: entry.refresh_interval_secs.map(|v| v as u32),
                confirm_dangerous: entry.confirm_dangerous.map(|v| v != 0),
                requires_where: entry.requires_where.map(|v| v != 0),
                requires_preview: entry.requires_preview.map(|v| v != 0),
            };

            if !ov.is_empty() {
                overrides.insert(key.clone(), ov);
            }

            if let Ok(values) = values_repo.get_for_driver(&key) {
                let mut form_values = FormValues::new();
                for v in values {
                    if let Some(val) = v.setting_value {
                        form_values.insert(v.setting_key, val);
                    }
                }
                if !form_values.is_empty() {
                    settings.insert(key, form_values);
                }
            }
        }
    }

    (overrides, settings)
}

// ---------------------------------------------------------------------------
// Hook Definitions helpers
// ---------------------------------------------------------------------------

fn load_hook_definitions(
    repo: &dbflux_storage::repositories::hook_definitions::HookDefinitionRepository,
) -> HashMap<String, dbflux_core::ConnectionHook> {
    let mut map = HashMap::new();

    if let Ok(hooks) = repo.all() {
        for dto in hooks {
            let execution_mode = match dto.execution_mode.as_str() {
                "Detached" => dbflux_core::HookExecutionMode::Detached,
                _ => dbflux_core::HookExecutionMode::Blocking,
            };

            let on_failure = match dto.on_failure.as_str() {
                "Disconnect" => dbflux_core::HookFailureMode::Disconnect,
                "Ignore" => dbflux_core::HookFailureMode::Ignore,
                _ => dbflux_core::HookFailureMode::Warn,
            };

            // Get env vars from child table
            let env = repo.get_env(&dto.id).unwrap_or_default();

            // Construct HookKind::Command using script_ref as the command
            // Note: The new schema doesn't preserve full HookKind (Command/Script/Lua) info
            // We assume Command for backward compatibility
            let kind = dbflux_core::HookKind::Command {
                command: dto.script_ref.clone().unwrap_or_default(),
                args: vec![],
            };

            let hook = dbflux_core::ConnectionHook {
                enabled: dto.enabled,
                kind,
                cwd: dto.cwd.as_ref().map(std::path::PathBuf::from),
                env,
                inherit_env: dto.inherit_env,
                timeout_ms: dto.timeout_ms.map(|v| v as u64),
                execution_mode,
                ready_signal: dto.ready_signal.clone(),
                on_failure,
            };

            map.insert(dto.name, hook);
        }
    }

    map
}

// ---------------------------------------------------------------------------
// Services helpers
// ---------------------------------------------------------------------------

fn load_services(
    repo: &dbflux_storage::repositories::services::ServiceRepository,
) -> Vec<ServiceConfig> {
    if let Ok(entries) = repo.all() {
        entries
            .into_iter()
            .map(|dto| {
                let args = repo.get_args(&dto.socket_id).unwrap_or_default();
                let env = repo.get_env(&dto.socket_id).unwrap_or_default();

                ServiceConfig {
                    socket_id: dto.socket_id,
                    enabled: dto.enabled,
                    command: dto.command,
                    args,
                    env,
                    startup_timeout_ms: dto.startup_timeout_ms.map(|v| v as u64),
                }
            })
            .collect()
    } else {
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// Profile helpers
// ---------------------------------------------------------------------------

/// Loads settings_overrides and connection_settings from profile settings DTOs.
fn load_profile_settings(
    settings: &[ConnectionProfileSettingDto],
) -> (Option<GlobalOverrides>, Option<FormValues>) {
    let mut settings_overrides = GlobalOverrides::default();
    let mut connection_settings = FormValues::default();
    let mut has_overrides = false;
    let mut has_conn_settings = false;

    for setting in settings {
        let key = &setting.setting_key;
        let value = setting.setting_value.as_ref();

        if key.starts_with("overrides.") {
            has_overrides = true;
            match key.as_str() {
                "overrides.refresh_policy" => {
                    if let Some(v) = value {
                        settings_overrides.refresh_policy = match v.as_str() {
                            "interval" => Some(dbflux_core::RefreshPolicySetting::Interval),
                            "manual" => Some(dbflux_core::RefreshPolicySetting::Manual),
                            _ => None,
                        };
                    }
                }
                "overrides.refresh_interval_secs" => {
                    if let Some(v) = value {
                        settings_overrides.refresh_interval_secs = v.parse().ok();
                    }
                }
                "overrides.confirm_dangerous" => {
                    if let Some(v) = value {
                        settings_overrides.confirm_dangerous = v.parse().ok();
                    }
                }
                "overrides.requires_where" => {
                    if let Some(v) = value {
                        settings_overrides.requires_where = v.parse().ok();
                    }
                }
                "overrides.requires_preview" => {
                    if let Some(v) = value {
                        settings_overrides.requires_preview = v.parse().ok();
                    }
                }
                _ => {}
            }
        } else if key.starts_with("conn.") {
            has_conn_settings = true;
            let conn_key = key.trim_start_matches("conn.").to_string();
            if let Some(v) = value {
                connection_settings.insert(conn_key, v.clone());
            }
        }
    }

    let settings_overrides = if has_overrides {
        Some(settings_overrides)
    } else {
        None
    };
    let connection_settings = if has_conn_settings {
        Some(connection_settings)
    } else {
        None
    };

    (settings_overrides, connection_settings)
}

/// Loads ConnectionHooks from hook DTOs.
fn load_connection_hooks_from_dtos(hooks: &[ConnectionProfileHookDto]) -> ConnectionHooks {
    let mut result = ConnectionHooks::default();

    for hook_dto in hooks {
        let phase = match hook_dto.phase.as_str() {
            "pre_connect" => HookPhase::PreConnect,
            "post_connect" => HookPhase::PostConnect,
            "pre_disconnect" => HookPhase::PreDisconnect,
            "post_disconnect" => HookPhase::PostDisconnect,
            _ => continue,
        };

        let execution_mode = match hook_dto.execution_mode.as_str() {
            "detached" => HookExecutionMode::Detached,
            _ => HookExecutionMode::Blocking,
        };

        let on_failure = match hook_dto.on_failure.as_str() {
            "disconnect" => HookFailureMode::Disconnect,
            "ignore" => HookFailureMode::Ignore,
            _ => HookFailureMode::Warn,
        };

        let kind = if hook_dto.command.as_ref().is_some_and(|c| !c.is_empty()) {
            HookKind::Command {
                command: hook_dto.command.clone().unwrap_or_default(),
                args: vec![],
            }
        } else if hook_dto.script_language.as_deref() == Some("lua") {
            HookKind::Lua {
                source: ScriptSource::Inline {
                    content: hook_dto.lua_content.clone().unwrap_or_default(),
                },
                capabilities: dbflux_core::LuaCapabilities {
                    logging: hook_dto.lua_log,
                    env_read: hook_dto.lua_env_read,
                    connection_metadata: hook_dto.lua_conn_metadata,
                    process_run: hook_dto.lua_process_run,
                },
            }
        } else if hook_dto.script_language.as_deref() == Some("python") {
            HookKind::Script {
                language: ScriptLanguage::Python,
                source: ScriptSource::Inline {
                    content: hook_dto.script_content.clone().unwrap_or_default(),
                },
                interpreter: None,
            }
        } else if hook_dto.script_language.as_deref() == Some("bash")
            || hook_dto.script_language.as_deref() == Some("sh")
        {
            HookKind::Script {
                language: ScriptLanguage::Bash,
                source: ScriptSource::Inline {
                    content: hook_dto.script_content.clone().unwrap_or_default(),
                },
                interpreter: None,
            }
        } else {
            continue;
        };

        let hook = ConnectionHook {
            enabled: hook_dto.enabled,
            kind,
            cwd: hook_dto.cwd.as_ref().map(std::path::PathBuf::from),
            env: Default::default(),
            inherit_env: hook_dto.inherit_env,
            timeout_ms: hook_dto.timeout_ms.map(|v| v as u64),
            execution_mode,
            ready_signal: hook_dto.ready_signal.clone(),
            on_failure,
        };

        result.phase_hooks_mut(phase).push(hook);
    }

    result
}

/// Loads ConnectionHookBindings from binding DTOs.
fn load_hook_bindings_from_dtos(
    bindings: &[ConnectionProfileHookBindingDto],
) -> ConnectionHookBindings {
    use std::collections::HashMap;

    // Group by phase and sort by order_index
    let mut by_phase: HashMap<String, Vec<(i32, String)>> = HashMap::new();
    for b in bindings {
        by_phase
            .entry(b.phase.clone())
            .or_default()
            .push((b.order_index, b.hook_id.clone()));
    }

    // Sort each phase's bindings by order_index and extract hook names
    let mut result = ConnectionHookBindings::default();
    for (phase, mut items) in by_phase {
        items.sort_by_key(|k| k.0);
        let hook_names: Vec<String> = items.into_iter().map(|(_, name)| name).collect();
        match phase.as_str() {
            "pre_connect" => result.pre_connect = hook_names,
            "post_connect" => result.post_connect = hook_names,
            "pre_disconnect" => result.pre_disconnect = hook_names,
            "post_disconnect" => result.post_disconnect = hook_names,
            _ => {}
        }
    }

    result
}

fn load_profiles(
    repo: &dbflux_storage::repositories::connection_profiles::ConnectionProfileRepository,
) -> Vec<ConnectionProfile> {
    let Ok(dtos) = repo.all() else {
        return Vec::new();
    };

    dtos
        .into_iter()
        .filter_map(|dto| {
            let profile_id = &dto.id;
            let id = uuid::Uuid::parse_str(profile_id).ok()?;

            // Load DbConfig from connection_driver_configs (native columns)
            let driver_configs_repo = repo.driver_configs();
            let driver_dto = driver_configs_repo.get_for_profile(profile_id).ok().flatten();
            let config = driver_dto
                .and_then(|d| dto_to_db_config(&d))
                .or_else(|| {
                    // Fallback: construct default config based on kind if driver config is missing
                    dto.kind.as_ref().and_then(|kind_str| {
                        let kind = str_to_db_kind(kind_str)?;
                        Some(default_db_config_for_kind(kind))
                    })
                })?;

            // Load settings overrides and connection settings from connection_profile_settings
            let settings_repo = repo.settings();
            let settings = settings_repo.get_for_profile(profile_id).ok().unwrap_or_default();
            let (settings_overrides, connection_settings) = load_profile_settings(&settings);

            // Load value refs from connection_profile_value_refs
            let value_refs_repo = repo.value_refs();
            let value_refs = value_refs_repo.get_for_profile(profile_id).ok().unwrap_or_default();
            let value_refs_map = value_refs
                .into_iter()
                .filter_map(|vr| {
                    let kind = dbflux_storage::repositories::connection_profile_value_refs::RefKind::try_parse(&vr.ref_kind)?;
                    let value_ref = match kind {
                        dbflux_storage::repositories::connection_profile_value_refs::RefKind::Secret => {
                            ValueRef::Secret {
                                locator: vr.ref_value,
                                provider: vr.ref_provider?,
                                json_key: vr.ref_json_key,
                            }
                        }
                        dbflux_storage::repositories::connection_profile_value_refs::RefKind::Param => {
                            ValueRef::Parameter {
                                name: vr.ref_value,
                                provider: vr.ref_provider?,
                                json_key: vr.ref_json_key,
                            }
                        }
                        dbflux_storage::repositories::connection_profile_value_refs::RefKind::Auth => {
                            ValueRef::Auth { field: vr.ref_value }
                        }
                    };
                    Some((vr.ref_key, value_ref))
                })
                .collect();

            // Load access_kind from connection_profile_access_params
            let access_params_repo = repo.access_params();
            let access_params = access_params_repo.get_for_profile(profile_id).ok().unwrap_or_default();
            let access_kind = if dto.access_kind.as_deref() == Some("direct") {
                Some(AccessKind::Direct)
            } else if dto.access_kind.as_deref() == Some("ssh") {
                dto.ssh_tunnel_profile_id.as_ref().and_then(|s| {
                    uuid::Uuid::parse_str(s).ok().map(|id| AccessKind::Ssh {
                        ssh_tunnel_profile_id: id,
                    })
                })
            } else if dto.access_kind.as_deref() == Some("proxy") {
                dto.proxy_profile_id.as_ref().and_then(|s| {
                    uuid::Uuid::parse_str(s).ok().map(|id| AccessKind::Proxy {
                        proxy_profile_id: id,
                    })
                })
            } else if dto.access_kind.as_deref() == Some("managed") {
                let params = access_params
                    .into_iter()
                    .map(|p| (p.param_key, p.param_value))
                    .collect();
                Some(AccessKind::Managed {
                    provider: dto.access_provider.unwrap_or_default(),
                    params,
                })
            } else {
                None
            };

            // Load hooks from connection_profile_hooks
            let hooks_repo = repo.hooks();
            let hooks_dtos = hooks_repo.get_for_profile(profile_id).ok().unwrap_or_default();
            let hooks = if hooks_dtos.is_empty() {
                None
            } else {
                Some(load_connection_hooks_from_dtos(&hooks_dtos))
            };

            // Load hook bindings from connection_profile_hook_bindings
            let bindings_repo = repo.hook_bindings();
            let bindings = bindings_repo.get_for_profile(profile_id).ok().unwrap_or_default();
            let hook_bindings = if bindings.is_empty() {
                None
            } else {
                Some(load_hook_bindings_from_dtos(&bindings))
            };

            // Load mcp_governance from connection_profile_governance
            let gov_repo = repo.governance();
            let gov_enabled = gov_repo
                .get_for_profile(profile_id)
                .ok()
                .and_then(|entries| {
                    entries
                        .into_iter()
                        .find(|e| e.governance_key == "enabled")
                        .and_then(|e| e.governance_value.and_then(|v| v.parse().ok()))
                });
            let gov_bindings_repo = repo.governance_bindings();
            let gov_bindings = gov_bindings_repo.get_for_profile(profile_id).ok().unwrap_or_default();
            let mcp_governance = if gov_enabled.is_some() || !gov_bindings.is_empty() {
                let mut policy_bindings = Vec::new();
                for binding in &gov_bindings {
                    let roles_repo = repo.governance_binding_roles();
                    let policies_repo = repo.governance_binding_policies();
                    let role_ids = roles_repo
                        .get_for_binding(&binding.id)
                        .ok()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|r| r.role_id)
                        .collect();
                    let policy_ids = policies_repo
                        .get_for_binding(&binding.id)
                        .ok()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|p| p.policy_id)
                        .collect();
                    policy_bindings.push(ConnectionMcpPolicyBinding {
                        actor_id: binding.actor_id.clone(),
                        role_ids,
                        policy_ids,
                    });
                }
                Some(ConnectionMcpGovernance {
                    enabled: gov_enabled.unwrap_or(false),
                    policy_bindings,
                })
            } else {
                None
            };

            Some(ConnectionProfile {
                id,
                name: dto.name,
                kind: dto.kind.as_ref().and_then(|k| str_to_db_kind(k)),
                driver_id: dto.driver_id,
                config,
                save_password: dto.save_password,
                settings_overrides,
                connection_settings,
                hooks,
                hook_bindings,
                proxy_profile_id: dto.proxy_profile_id.as_ref().and_then(|s| uuid::Uuid::parse_str(s).ok()),
                auth_profile_id: dto.auth_profile_id.as_ref().and_then(|s| uuid::Uuid::parse_str(s).ok()),
                value_refs: value_refs_map,
                access_kind,
                mcp_governance,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Auth Profile helpers
// ---------------------------------------------------------------------------

fn load_auth_profiles(
    repo: &dbflux_storage::repositories::auth_profiles::AuthProfileRepository,
) -> Vec<dbflux_core::AuthProfile> {
    if let Ok(entries) = repo.all() {
        entries
            .into_iter()
            .filter_map(|dto| {
                let fields = repo.get_fields(&dto.id).unwrap_or_default();
                let id = uuid::Uuid::parse_str(&dto.id).ok()?;
                Some(dbflux_core::AuthProfile {
                    id,
                    name: dto.name,
                    provider_id: dto.provider_id,
                    fields,
                    enabled: dto.enabled,
                })
            })
            .collect()
    } else {
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// Proxy Profile helpers
// ---------------------------------------------------------------------------

fn load_proxy_profiles(
    repo: &dbflux_storage::repositories::proxy_profiles::ProxyProfileRepository,
    auth_repo: &dbflux_storage::repositories::proxy_auth::ProxyAuthRepository,
) -> Vec<ProxyProfile> {
    if let Ok(entries) = repo.all() {
        entries
            .into_iter()
            .filter_map(|dto| {
                let id = uuid::Uuid::parse_str(&dto.id).ok()?;
                let auth = match dto.auth_kind.as_str() {
                    "basic" => {
                        if let Ok(Some(auth_dto)) = auth_repo.get(&dto.id) {
                            dbflux_core::ProxyAuth::Basic {
                                username: auth_dto.username.unwrap_or_default(),
                            }
                        } else {
                            dbflux_core::ProxyAuth::None
                        }
                    }
                    _ => dbflux_core::ProxyAuth::None,
                };
                let kind = match dto.kind.to_lowercase().as_str() {
                    "http" => dbflux_core::ProxyKind::Http,
                    "https" => dbflux_core::ProxyKind::Https,
                    "socks5" | "socks" => dbflux_core::ProxyKind::Socks5,
                    _ => dbflux_core::ProxyKind::Http,
                };
                Some(ProxyProfile {
                    id,
                    name: dto.name,
                    kind,
                    host: dto.host,
                    port: dto.port as u16,
                    auth,
                    no_proxy: dto.no_proxy,
                    enabled: dto.enabled,
                    save_secret: dto.save_secret,
                })
            })
            .collect()
    } else {
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// SSH Tunnel helpers
// ---------------------------------------------------------------------------

fn load_ssh_tunnels(
    repo: &dbflux_storage::repositories::ssh_tunnel_profiles::SshTunnelProfileRepository,
) -> Vec<SshTunnelProfile> {
    if let Ok(entries) = repo.all() {
        entries
            .into_iter()
            .filter_map(|dto| {
                let id = uuid::Uuid::parse_str(&dto.id).ok()?;
                let auth_method = match dto.auth_method.as_str() {
                    "key" => {
                        if let Ok(Some(auth_dto)) = repo.get_auth(&dto.id) {
                            dbflux_core::SshAuthMethod::PrivateKey {
                                key_path: auth_dto.key_path.map(std::path::PathBuf::from),
                            }
                        } else {
                            dbflux_core::SshAuthMethod::PrivateKey { key_path: None }
                        }
                    }
                    _ => dbflux_core::SshAuthMethod::Password,
                };
                let config = dbflux_core::SshTunnelConfig {
                    host: dto.host,
                    port: dto.port as u16,
                    user: dto.user,
                    auth_method,
                };
                Some(SshTunnelProfile {
                    id,
                    name: dto.name,
                    config,
                    save_secret: dto.save_secret,
                })
            })
            .collect()
    } else {
        Vec::new()
    }
}
