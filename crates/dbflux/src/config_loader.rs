//! Configuration loader that reads and writes all durable config from `config.db` repositories.
//!
//! This is the authoritative config-loading path for the app. It replaces
//! `AppConfigStore` (which reads `config.json`) for all covered durable config domains.

use std::collections::HashMap;

use dbflux_core::{
    ConnectionProfile, DriverKey, FormValues, GeneralSettings, GlobalOverrides, ProxyProfile,
    ServiceConfig, SshTunnelProfile,
};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::error::StorageError;
use dbflux_storage::repositories::settings::SettingsRepository;

pub fn save_general_settings(
    runtime: &StorageRuntime,
    settings: &GeneralSettings,
) -> Result<(), dbflux_storage::error::StorageError> {
    let repo = runtime.settings();
    repo.set(
        "general_settings",
        &serde_json::to_string(settings).unwrap_or_default(),
    )?;

    let set_key = |repo: &SettingsRepository,
                   key: &str,
                   value: String|
     -> Result<(), dbflux_storage::error::StorageError> { repo.set(key, &value) };

    let theme_val = match settings.theme {
        dbflux_core::ThemeSetting::Light => "light",
        dbflux_core::ThemeSetting::Dark => "dark",
    };
    set_key(&repo, "theme", theme_val.to_string())?;
    set_key(
        &repo,
        "restore_session_on_startup",
        settings.restore_session_on_startup.to_string(),
    )?;
    set_key(
        &repo,
        "reopen_last_connections",
        settings.reopen_last_connections.to_string(),
    )?;
    let focus_val = match settings.default_focus_on_startup {
        dbflux_core::StartupFocus::LastTab => "last_tab",
        dbflux_core::StartupFocus::Sidebar => "sidebar",
    };
    set_key(&repo, "default_focus", focus_val.to_string())?;
    set_key(
        &repo,
        "max_history_entries",
        settings.max_history_entries.to_string(),
    )?;
    set_key(
        &repo,
        "auto_save_interval_ms",
        settings.auto_save_interval_ms.to_string(),
    )?;
    let refresh_val = match settings.default_refresh_policy {
        dbflux_core::RefreshPolicySetting::Interval => "interval",
        dbflux_core::RefreshPolicySetting::Manual => "manual",
    };
    set_key(&repo, "default_refresh_policy", refresh_val.to_string())?;
    set_key(
        &repo,
        "default_refresh_interval_secs",
        settings.default_refresh_interval_secs.to_string(),
    )?;
    set_key(
        &repo,
        "max_concurrent_background_tasks",
        settings.max_concurrent_background_tasks.to_string(),
    )?;
    set_key(
        &repo,
        "auto_refresh_pause_on_error",
        settings.auto_refresh_pause_on_error.to_string(),
    )?;
    set_key(
        &repo,
        "auto_refresh_only_if_visible",
        settings.auto_refresh_only_if_visible.to_string(),
    )?;
    set_key(
        &repo,
        "confirm_dangerous_queries",
        settings.confirm_dangerous_queries.to_string(),
    )?;
    set_key(
        &repo,
        "dangerous_requires_where",
        settings.dangerous_requires_where.to_string(),
    )?;
    set_key(
        &repo,
        "dangerous_requires_preview",
        settings.dangerous_requires_preview.to_string(),
    )?;

    Ok(())
}

pub fn save_driver_settings(
    runtime: &StorageRuntime,
    overrides: &HashMap<DriverKey, GlobalOverrides>,
    settings: &HashMap<DriverKey, FormValues>,
) -> Result<(), dbflux_storage::error::StorageError> {
    let repo = runtime.driver_settings();

    // Propagate read errors from the repository.
    let existing_rows = repo.all()?;
    let existing: std::collections::HashSet<_> =
        existing_rows.iter().map(|d| d.driver_key.clone()).collect();

    // Build the full set of keys present in the desired state.
    let desired: std::collections::HashSet<_> =
        overrides.keys().chain(settings.keys()).cloned().collect();

    // Upsert all keys that are in the desired state.
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
        repo.upsert(&dto)?;
    }

    // Delete keys that are in DB but not in the desired state.
    for key in existing.difference(&desired) {
        repo.delete(key)?;
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
        let kind_json = serde_json::to_string(hook).unwrap_or_default();
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
            kind_json,
            execution_mode,
            script_ref: hook.ready_signal.clone(),
            command_json: None,
            cwd: hook.cwd.as_ref().map(|p| p.to_string_lossy().to_string()),
            env_json: Some(serde_json::to_string(&hook.env).unwrap_or_default()),
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
            args_json: Some(serde_json::to_string(&svc.args).unwrap_or_default()),
            env_json: Some(serde_json::to_string(&svc.env).unwrap_or_default()),
            startup_timeout_ms: svc.startup_timeout_ms.map(|v| v as i64),
            created_at: String::new(),
            updated_at: String::new(),
        };
        repo.upsert(&dto)?;
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
    let existing: std::collections::HashSet<_> = repo
        .all()
        .unwrap_or_default()
        .iter()
        .map(|d| d.id.clone())
        .collect();

    for profile in profiles {
        let config_json = serde_json::to_string(profile).unwrap_or_default();
        let dto = dbflux_storage::repositories::connection_profiles::ConnectionProfileDto {
            id: profile.id.to_string(),
            name: profile.name.clone(),
            driver_id: Some(profile.driver_id()),
            description: None,
            favorite: false,
            color: None,
            icon: None,
            config_json,
            auth_profile_id: profile.auth_profile_id.map(|u| u.to_string()),
            proxy_profile_id: profile.proxy_profile_id.map(|u| u.to_string()),
            ssh_tunnel_profile_id: profile.access_kind.as_ref().and_then(|a| {
                if let dbflux_core::AccessKind::Ssh {
                    ssh_tunnel_profile_id,
                } = a
                {
                    Some(ssh_tunnel_profile_id.to_string())
                } else {
                    None
                }
            }),
            access_profile_id: None,
            settings_overrides_json: profile
                .settings_overrides
                .as_ref()
                .map(|s| serde_json::to_string(s).unwrap_or_default()),
            connection_settings_json: profile
                .connection_settings
                .as_ref()
                .map(|s| serde_json::to_string(s).unwrap_or_default()),
            hooks_json: profile
                .hooks
                .as_ref()
                .map(|h| serde_json::to_string(h).unwrap_or_default()),
            hook_bindings_json: profile
                .hook_bindings
                .as_ref()
                .map(|h| serde_json::to_string(h).unwrap_or_default()),
            value_refs_json: Some(serde_json::to_string(&profile.value_refs).unwrap_or_default()),
            mcp_governance_json: profile
                .mcp_governance
                .as_ref()
                .map(|m| serde_json::to_string(m).unwrap_or_default()),
            created_at: String::new(),
            updated_at: String::new(),
        };
        if existing.contains(&profile.id.to_string()) {
            repo.upsert(&dto)?;
        }
    }

    Ok(())
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
        let fields_json = serde_json::to_string(&profile.fields).map_err(|e| StorageError::Io {
            path: "config.db".into(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
        })?;

        let dto = dbflux_storage::repositories::auth_profiles::AuthProfileDto {
            id: profile.id.to_string(),
            name: profile.name.clone(),
            provider_id: profile.provider_id.clone(),
            fields_json,
            enabled: profile.enabled,
            created_at: String::new(),
            updated_at: String::new(),
        };

        if existing_ids.contains(&dto.id) {
            repo.update(&dto)?;
        } else {
            repo.insert(&dto)?;
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
        let dto = dbflux_storage::repositories::proxy_profiles::ProxyProfileDto {
            id: profile.id.to_string(),
            name: profile.name.clone(),
            kind: serde_json::to_string(&profile.kind).unwrap_or_default(),
            host: profile.host.clone(),
            port: profile.port as i32,
            auth_json: serde_json::to_string(&profile.auth).unwrap_or_default(),
            no_proxy: profile.no_proxy.clone(),
            enabled: profile.enabled,
            save_secret: profile.save_secret,
            created_at: String::new(),
            updated_at: String::new(),
        };
        repo.upsert(&dto)?;
    }

    Ok(())
}

pub fn save_ssh_tunnels(
    runtime: &StorageRuntime,
    tunnels: &[SshTunnelProfile],
) -> Result<(), dbflux_storage::error::StorageError> {
    let repo = runtime.ssh_tunnels();

    for tunnel in tunnels {
        let dto = dbflux_storage::repositories::ssh_tunnel_profiles::SshTunnelProfileDto {
            id: tunnel.id.to_string(),
            name: tunnel.name.clone(),
            config_json: serde_json::to_string(&tunnel.config).unwrap_or_default(),
            save_secret: tunnel.save_secret,
            created_at: String::new(),
            updated_at: String::new(),
        };
        repo.upsert(&dto)?;
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
    let settings = runtime.settings();
    let profiles_repo = runtime.connection_profiles();
    let auth_repo = runtime.auth_profiles();
    let proxy_repo = runtime.proxy_profiles();
    let ssh_repo = runtime.ssh_tunnels();
    let hooks_repo = runtime.hook_definitions();
    let services_repo = runtime.services();
    let driver_repo = runtime.driver_settings();

    let general_settings = load_general_settings(&settings);
    let (driver_overrides, driver_settings) = load_driver_maps(&driver_repo);
    let hook_definitions = load_hook_definitions(&hooks_repo);
    let services = load_services(&services_repo);
    let profiles = load_profiles(&profiles_repo);
    let auth_profiles = load_auth_profiles(&auth_repo);
    let proxy_profiles = load_proxy_profiles(&proxy_repo);
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
    repo: &dbflux_storage::repositories::settings::SettingsRepository,
) -> GeneralSettings {
    if let Ok(Some(json)) = repo.get("general_settings")
        && let Ok(settings) = serde_json::from_str::<GeneralSettings>(&json)
    {
        return settings;
    }

    let theme = load_enum::<String>(repo, "theme")
        .map(|s| match s.as_str() {
            "light" => dbflux_core::ThemeSetting::Light,
            _ => dbflux_core::ThemeSetting::Dark,
        })
        .unwrap_or(dbflux_core::ThemeSetting::Dark);

    let default_focus = load_enum::<String>(repo, "default_focus")
        .map(|s| match s.as_str() {
            "last_tab" => dbflux_core::StartupFocus::LastTab,
            _ => dbflux_core::StartupFocus::Sidebar,
        })
        .unwrap_or(dbflux_core::StartupFocus::Sidebar);

    GeneralSettings {
        theme,
        restore_session_on_startup: load_bool(repo, "restore_session_on_startup").unwrap_or(true),
        reopen_last_connections: load_bool(repo, "reopen_last_connections").unwrap_or(false),
        default_focus_on_startup: default_focus,
        max_history_entries: load_usize(repo, "max_history_entries").unwrap_or(1000),
        auto_save_interval_ms: load_u64(repo, "auto_save_interval_ms").unwrap_or(2000),
        default_refresh_policy: load_enum::<String>(repo, "default_refresh_policy")
            .map(|s| match s.as_str() {
                "interval" => dbflux_core::RefreshPolicySetting::Interval,
                _ => dbflux_core::RefreshPolicySetting::Manual,
            })
            .unwrap_or(dbflux_core::RefreshPolicySetting::Manual),
        default_refresh_interval_secs: load_u32(repo, "default_refresh_interval_secs").unwrap_or(5),
        max_concurrent_background_tasks: load_usize(repo, "max_concurrent_background_tasks")
            .unwrap_or(8),
        auto_refresh_pause_on_error: load_bool(repo, "auto_refresh_pause_on_error").unwrap_or(true),
        auto_refresh_only_if_visible: load_bool(repo, "auto_refresh_only_if_visible")
            .unwrap_or(false),
        confirm_dangerous_queries: load_bool(repo, "confirm_dangerous_queries").unwrap_or(true),
        dangerous_requires_where: load_bool(repo, "dangerous_requires_where").unwrap_or(true),
        dangerous_requires_preview: load_bool(repo, "dangerous_requires_preview").unwrap_or(false),
    }
}

fn load_bool(
    repo: &dbflux_storage::repositories::settings::SettingsRepository,
    key: &str,
) -> Option<bool> {
    repo.get(key).ok().flatten().and_then(|s| s.parse().ok())
}

fn load_usize(
    repo: &dbflux_storage::repositories::settings::SettingsRepository,
    key: &str,
) -> Option<usize> {
    repo.get(key).ok().flatten().and_then(|s| s.parse().ok())
}

fn load_u64(
    repo: &dbflux_storage::repositories::settings::SettingsRepository,
    key: &str,
) -> Option<u64> {
    repo.get(key).ok().flatten().and_then(|s| s.parse().ok())
}

fn load_u32(
    repo: &dbflux_storage::repositories::settings::SettingsRepository,
    key: &str,
) -> Option<u32> {
    repo.get(key).ok().flatten().and_then(|s| s.parse().ok())
}

fn load_enum<T: std::str::FromStr>(
    repo: &dbflux_storage::repositories::settings::SettingsRepository,
    key: &str,
) -> Option<T> {
    repo.get(key).ok().flatten().and_then(|s| s.parse().ok())
}

// ---------------------------------------------------------------------------
// Driver Maps helpers
// ---------------------------------------------------------------------------

fn load_driver_maps(
    repo: &dbflux_storage::repositories::driver_settings::DriverSettingsRepository,
) -> (
    HashMap<DriverKey, GlobalOverrides>,
    HashMap<DriverKey, FormValues>,
) {
    let mut overrides = HashMap::new();
    let mut settings = HashMap::new();

    if let Ok(entries) = repo.all() {
        for entry in entries {
            let key = entry.driver_key;

            if let Some(o) = entry
                .overrides_json
                .as_ref()
                .and_then(|j| serde_json::from_str::<GlobalOverrides>(j).ok())
            {
                overrides.insert(key.clone(), o);
            }

            if let Some(v) = entry
                .settings_json
                .as_ref()
                .and_then(|j| serde_json::from_str::<FormValues>(j).ok())
            {
                settings.insert(key, v);
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
            if let Ok(hook) = serde_json::from_str::<dbflux_core::ConnectionHook>(&dto.kind_json) {
                map.insert(dto.name, hook);
            } else {
                log::warn!("Failed to deserialize hook definition: {}", dto.name);
            }
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
                let args_json = dto.args_json.as_ref();
                let env_json = dto.env_json.as_ref();

                ServiceConfig {
                    socket_id: dto.socket_id,
                    enabled: dto.enabled,
                    command: dto.command,
                    args: args_json
                        .and_then(|j| serde_json::from_str(j).ok())
                        .unwrap_or_default(),
                    env: env_json
                        .and_then(|j| serde_json::from_str(j).ok())
                        .unwrap_or_default(),
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

fn load_profiles(
    repo: &dbflux_storage::repositories::connection_profiles::ConnectionProfileRepository,
) -> Vec<ConnectionProfile> {
    if let Ok(entries) = repo.all() {
        entries
            .into_iter()
            .filter_map(|dto| serde_json::from_str(&dto.config_json).ok())
            .collect()
    } else {
        Vec::new()
    }
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
                let fields: std::collections::HashMap<String, String> =
                    serde_json::from_str(&dto.fields_json).unwrap_or_default();
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
) -> Vec<ProxyProfile> {
    if let Ok(entries) = repo.all() {
        entries
            .into_iter()
            .filter_map(|dto| {
                let auth: dbflux_core::ProxyAuth =
                    serde_json::from_str(&dto.auth_json).unwrap_or(dbflux_core::ProxyAuth::None);
                let id = uuid::Uuid::parse_str(&dto.id).ok()?;
                Some(ProxyProfile {
                    id,
                    name: dto.name,
                    kind: serde_json::from_str(&dto.kind).unwrap_or(dbflux_core::ProxyKind::Http),
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
                let config: dbflux_core::SshTunnelConfig =
                    serde_json::from_str(&dto.config_json).ok()?;
                let id = uuid::Uuid::parse_str(&dto.id).ok()?;
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
