//! Configuration loader that reads all durable config from `config.db` repositories.
//!
//! This is the authoritative config-loading path for the app. It replaces
//! `AppConfigStore` (which reads `config.json`) for all covered durable config domains.

use std::collections::HashMap;

use dbflux_core::{
    ConnectionProfile, DriverKey, FormValues, GeneralSettings, GlobalOverrides, ProxyProfile,
    ServiceConfig, SshTunnelProfile,
};
use dbflux_storage::bootstrap::StorageRuntime;

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
    if let Ok(Some(json)) = repo.get("general_settings") {
        if let Ok(settings) = serde_json::from_str::<GeneralSettings>(&json) {
            return settings;
        }
    }

    let theme = load_enum::<String>(repo, "theme")
        .and_then(|s| match s.as_str() {
            "light" => Some(dbflux_core::ThemeSetting::Light),
            _ => Some(dbflux_core::ThemeSetting::Dark),
        })
        .unwrap_or(dbflux_core::ThemeSetting::Dark);

    let default_focus = load_enum::<String>(repo, "default_focus")
        .and_then(|s| match s.as_str() {
            "last_tab" => Some(dbflux_core::StartupFocus::LastTab),
            _ => Some(dbflux_core::StartupFocus::Sidebar),
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
            .and_then(|s| match s.as_str() {
                "interval" => Some(dbflux_core::RefreshPolicySetting::Interval),
                _ => Some(dbflux_core::RefreshPolicySetting::Manual),
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
                let auth: dbflux_core::ProxyAuth = serde_json::from_str(&dto.auth_json)
                    .unwrap_or_else(|_| dbflux_core::ProxyAuth::None);
                let id = uuid::Uuid::parse_str(&dto.id).ok()?;
                Some(ProxyProfile {
                    id,
                    name: dto.name,
                    kind: serde_json::from_str(&dto.kind)
                        .unwrap_or_else(|_| dbflux_core::ProxyKind::Http),
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
