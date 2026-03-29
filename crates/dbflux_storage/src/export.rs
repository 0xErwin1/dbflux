//! Config export/import support in portable JSON format.
//!
//! Exports all durable config domains (connection profiles, auth profiles,
//! proxy profiles, SSH tunnel profiles, hook definitions, services, settings,
//! driver settings) to a single portable JSON file. Supports importing from
//! such files to restore configuration.

use log::info;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

use crate::error::StorageError;
use crate::repositories::auth_profiles::AuthProfileRepository;
use crate::repositories::connection_profiles::ConnectionProfileRepository;
use crate::repositories::driver_settings::DriverSettingsRepository;
use crate::repositories::hook_definitions::HookDefinitionRepository;
use crate::repositories::proxy_profiles::ProxyProfileRepository;
use crate::repositories::services::ServiceRepository;
use crate::repositories::settings::SettingsRepository;
use crate::repositories::ssh_tunnel_profiles::SshTunnelProfileRepository;
use crate::repositories::state::query_history::{QueryHistoryDto, QueryHistoryRepository};
use crate::repositories::state::recent_items::{RecentItemDto, RecentItemsRepository};
use crate::repositories::state::saved_queries::{SavedQueriesRepository, SavedQueryDto};
use crate::repositories::state::ui_state::UiStateRepository;

/// Current export format version.
const EXPORT_VERSION: u32 = 1;

/// Magic string to identify DBFlux export files.
const EXPORT_MAGIC: &str = "DBFLUX_EXPORT_V1";

/// Export file root structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportFile {
    pub magic: String,
    pub version: u32,
    pub exported_at: String,
    pub domains: ExportDomains,
}

/// All exportable config domains.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExportDomains {
    #[serde(default)]
    pub settings: Vec<SettingEntry>,

    #[serde(default)]
    pub connection_profiles: Vec<ConnectionProfileEntry>,

    #[serde(default)]
    pub auth_profiles: Vec<AuthProfileEntry>,

    #[serde(default)]
    pub proxy_profiles: Vec<ProxyProfileEntry>,

    #[serde(default)]
    pub ssh_tunnels: Vec<SshTunnelEntry>,

    #[serde(default)]
    pub hook_definitions: Vec<HookDefinitionEntry>,

    #[serde(default)]
    pub services: Vec<ServiceEntry>,

    #[serde(default)]
    pub driver_settings: Vec<DriverSettingsEntry>,

    #[serde(default)]
    pub history: Vec<HistoryEntry>,

    #[serde(default)]
    pub saved_queries: Vec<SavedQueryEntry>,

    #[serde(default)]
    pub recent_items: Vec<RecentItemEntry>,

    #[serde(default)]
    pub ui_state: Vec<UiStateEntry>,
}

/// A key-value setting entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingEntry {
    pub key: String,
    pub value_json: String,
}

/// Connection profile entry with its JSON config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileEntry {
    pub id: String,
    pub name: String,
    pub driver_id: Option<String>,
    pub description: Option<String>,
    pub favorite: bool,
    pub color: Option<String>,
    pub icon: Option<String>,
    pub config_json: String,
    pub auth_profile_id: Option<String>,
    pub proxy_profile_id: Option<String>,
    pub ssh_tunnel_profile_id: Option<String>,
    pub access_profile_id: Option<String>,
    pub settings_overrides_json: Option<String>,
    pub connection_settings_json: Option<String>,
    pub hooks_json: Option<String>,
    pub hook_bindings_json: Option<String>,
    pub value_refs_json: Option<String>,
    pub mcp_governance_json: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Auth profile entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProfileEntry {
    pub id: String,
    pub name: String,
    pub provider_id: String,
    pub fields_json: String,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// Proxy profile entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyProfileEntry {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub host: String,
    pub port: i32,
    pub auth_json: String,
    pub no_proxy: Option<String>,
    pub enabled: bool,
    pub save_secret: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// SSH tunnel profile entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshTunnelEntry {
    pub id: String,
    pub name: String,
    pub config_json: String,
    pub save_secret: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// Hook definition entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookDefinitionEntry {
    pub id: String,
    pub name: String,
    pub kind_json: String,
    pub execution_mode: String,
    pub script_ref: Option<String>,
    pub command_json: Option<String>,
    pub cwd: Option<String>,
    pub env_json: Option<String>,
    pub inherit_env: bool,
    pub timeout_ms: Option<i64>,
    pub ready_signal: Option<String>,
    pub on_failure: String,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// Service entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceEntry {
    pub socket_id: String,
    pub enabled: bool,
    pub command: Option<String>,
    pub args_json: Option<String>,
    pub env_json: Option<String>,
    pub startup_timeout_ms: Option<i64>,
    pub created_at: String,
    pub updated_at: String,
}

/// Driver settings entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverSettingsEntry {
    pub driver_key: String,
    pub overrides_json: Option<String>,
    pub settings_json: Option<String>,
    pub updated_at: String,
}

/// History entry (subset of full DTO).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub id: String,
    pub connection_profile_id: Option<String>,
    pub driver_id: Option<String>,
    pub database_name: Option<String>,
    pub query_text: String,
    pub query_kind: String,
    pub executed_at: String,
    pub duration_ms: Option<i64>,
    pub succeeded: bool,
    pub error_summary: Option<String>,
    pub row_count: Option<i64>,
    pub is_favorite: bool,
}

/// Saved query entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQueryEntry {
    pub id: String,
    pub folder_id: Option<String>,
    pub name: String,
    pub sql: String,
    pub is_favorite: bool,
    pub connection_id: Option<String>,
    pub created_at: String,
    pub last_used_at: String,
}

/// Recent item entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentItemEntry {
    pub id: String,
    pub kind: String,
    pub profile_id: Option<String>,
    pub path: Option<String>,
    pub title: String,
    pub accessed_at: String,
}

/// UI state entry (key-value).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiStateEntry {
    pub key: String,
    pub value_json: String,
}

/// Result of an export operation.
#[derive(Debug, Clone)]
pub struct ExportResult {
    pub path: PathBuf,
    pub domain_counts: ExportDomainCounts,
    pub total_items: usize,
}

/// Counts per domain.
#[derive(Debug, Clone, Default)]
pub struct ExportDomainCounts {
    pub settings: usize,
    pub profiles: usize,
    pub auth_profiles: usize,
    pub proxies: usize,
    pub ssh_tunnels: usize,
    pub hooks: usize,
    pub services: usize,
    pub driver_settings: usize,
    pub history: usize,
    pub saved_queries: usize,
    pub recent_items: usize,
    pub ui_state: usize,
}

impl ExportDomainCounts {
    pub fn total(&self) -> usize {
        self.settings
            + self.profiles
            + self.auth_profiles
            + self.proxies
            + self.ssh_tunnels
            + self.hooks
            + self.services
            + self.driver_settings
            + self.history
            + self.saved_queries
            + self.recent_items
            + self.ui_state
    }
}

/// Result of an import operation.
#[derive(Debug, Clone, Default)]
pub struct ImportResult {
    pub domains_imported: usize,
    pub total_items: usize,
    pub errors: Vec<String>,
}

impl ImportResult {
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------

/// Exports all durable config to a portable JSON file at the given path.
///
/// The file can later be imported to restore configuration on another machine
/// or after a reset. Secrets (passwords) are NOT included — only references.
pub fn export_to_file(
    path: &PathBuf,
    runtime: &crate::bootstrap::StorageRuntime,
) -> Result<ExportResult, StorageError> {
    let domains = collect_all_domains(runtime)?;

    let export = ExportFile {
        magic: EXPORT_MAGIC.to_string(),
        version: EXPORT_VERSION,
        exported_at: chrono::Utc::now().to_rfc3339(),
        domains,
    };

    let content = serde_json::to_string_pretty(&export).map_err(|e| StorageError::Io {
        path: path.clone(),
        source: std::io::Error::other(e.to_string()),
    })?;

    fs::write(path, content).map_err(|source| StorageError::Io {
        path: path.clone(),
        source,
    })?;

    let counts = ExportDomainCounts {
        settings: export.domains.settings.len(),
        profiles: export.domains.connection_profiles.len(),
        auth_profiles: export.domains.auth_profiles.len(),
        proxies: export.domains.proxy_profiles.len(),
        ssh_tunnels: export.domains.ssh_tunnels.len(),
        hooks: export.domains.hook_definitions.len(),
        services: export.domains.services.len(),
        driver_settings: export.domains.driver_settings.len(),
        history: export.domains.history.len(),
        saved_queries: export.domains.saved_queries.len(),
        recent_items: export.domains.recent_items.len(),
        ui_state: export.domains.ui_state.len(),
    };

    let total = counts.total();

    info!("Exported {} items to {}", total, path.display());

    Ok(ExportResult {
        path: path.clone(),
        domain_counts: counts,
        total_items: total,
    })
}

/// Collects all exportable domains from the storage runtime.
fn collect_all_domains(
    runtime: &crate::bootstrap::StorageRuntime,
) -> Result<ExportDomains, StorageError> {
    let config_conn = runtime.config_db();
    let state_conn = runtime.state_db();

    // Config domains
    let settings_repo = SettingsRepository::new(config_conn.clone());
    let profiles_repo = ConnectionProfileRepository::new(config_conn.clone());
    let auth_repo = AuthProfileRepository::new(config_conn.clone());
    let proxy_repo = ProxyProfileRepository::new(config_conn.clone());
    let ssh_repo = SshTunnelProfileRepository::new(config_conn.clone());
    let hooks_repo = HookDefinitionRepository::new(config_conn.clone());
    let services_repo = ServiceRepository::new(config_conn.clone());
    let driver_repo = DriverSettingsRepository::new(config_conn.clone());

    let settings = settings_repo
        .all()?
        .into_iter()
        .map(|e| SettingEntry {
            key: e.key,
            value_json: e.value_json,
        })
        .collect();

    let profiles = profiles_repo
        .all()?
        .into_iter()
        .map(|e| ConnectionProfileEntry {
            id: e.id,
            name: e.name,
            driver_id: e.driver_id,
            description: e.description,
            favorite: e.favorite,
            color: e.color,
            icon: e.icon,
            config_json: e.config_json,
            auth_profile_id: e.auth_profile_id,
            proxy_profile_id: e.proxy_profile_id,
            ssh_tunnel_profile_id: e.ssh_tunnel_profile_id,
            access_profile_id: e.access_profile_id,
            settings_overrides_json: e.settings_overrides_json,
            connection_settings_json: e.connection_settings_json,
            hooks_json: e.hooks_json,
            hook_bindings_json: e.hook_bindings_json,
            value_refs_json: e.value_refs_json,
            mcp_governance_json: e.mcp_governance_json,
            created_at: e.created_at,
            updated_at: e.updated_at,
        })
        .collect();

    let auth_profiles = auth_repo
        .all()?
        .into_iter()
        .map(|e| AuthProfileEntry {
            id: e.id,
            name: e.name,
            provider_id: e.provider_id,
            fields_json: e.fields_json,
            enabled: e.enabled,
            created_at: e.created_at,
            updated_at: e.updated_at,
        })
        .collect();

    let proxy_profiles = proxy_repo
        .all()?
        .into_iter()
        .map(|e| ProxyProfileEntry {
            id: e.id,
            name: e.name,
            kind: e.kind,
            host: e.host,
            port: e.port,
            auth_json: e.auth_json,
            no_proxy: e.no_proxy,
            enabled: e.enabled,
            save_secret: e.save_secret,
            created_at: e.created_at,
            updated_at: e.updated_at,
        })
        .collect();

    let ssh_tunnels = ssh_repo
        .all()?
        .into_iter()
        .map(|e| SshTunnelEntry {
            id: e.id,
            name: e.name,
            config_json: e.config_json,
            save_secret: e.save_secret,
            created_at: e.created_at,
            updated_at: e.updated_at,
        })
        .collect();

    let hooks = hooks_repo
        .all()?
        .into_iter()
        .map(|e| HookDefinitionEntry {
            id: e.id,
            name: e.name,
            kind_json: e.kind_json,
            execution_mode: e.execution_mode,
            script_ref: e.script_ref,
            command_json: e.command_json,
            cwd: e.cwd,
            env_json: e.env_json,
            inherit_env: e.inherit_env,
            timeout_ms: e.timeout_ms,
            ready_signal: e.ready_signal,
            on_failure: e.on_failure,
            enabled: e.enabled,
            created_at: e.created_at,
            updated_at: e.updated_at,
        })
        .collect();

    let services = services_repo
        .all()?
        .into_iter()
        .map(|e| ServiceEntry {
            socket_id: e.socket_id,
            enabled: e.enabled,
            command: e.command,
            args_json: e.args_json,
            env_json: e.env_json,
            startup_timeout_ms: e.startup_timeout_ms,
            created_at: e.created_at,
            updated_at: e.updated_at,
        })
        .collect();

    let driver_settings = driver_repo
        .all()?
        .into_iter()
        .map(|e| DriverSettingsEntry {
            driver_key: e.driver_key,
            overrides_json: e.overrides_json,
            settings_json: e.settings_json,
            updated_at: e.updated_at,
        })
        .collect();

    // State domains
    let history_repo = QueryHistoryRepository::new(state_conn.clone());
    let saved_queries_repo = SavedQueriesRepository::new(state_conn.clone());
    let recent_repo = RecentItemsRepository::new(state_conn.clone());
    let ui_state_repo = UiStateRepository::new(state_conn.clone());

    let history = history_repo
        .all()?
        .into_iter()
        .map(|e| HistoryEntry {
            id: e.id,
            connection_profile_id: e.connection_profile_id,
            driver_id: e.driver_id,
            database_name: e.database_name,
            query_text: e.query_text,
            query_kind: e.query_kind,
            executed_at: e.executed_at,
            duration_ms: e.duration_ms,
            succeeded: e.succeeded,
            error_summary: e.error_summary,
            row_count: e.row_count,
            is_favorite: e.is_favorite,
        })
        .collect();

    let saved_queries = saved_queries_repo
        .all()?
        .into_iter()
        .map(|e| SavedQueryEntry {
            id: e.id,
            folder_id: e.folder_id,
            name: e.name,
            sql: e.sql,
            is_favorite: e.is_favorite,
            connection_id: e.connection_id,
            created_at: e.created_at,
            last_used_at: e.last_used_at,
        })
        .collect();

    let recent_items = recent_repo
        .all()?
        .into_iter()
        .map(|e| RecentItemEntry {
            id: e.id,
            kind: e.kind,
            profile_id: e.profile_id,
            path: e.path,
            title: e.title,
            accessed_at: e.accessed_at,
        })
        .collect();

    let ui_state = ui_state_repo
        .all()?
        .into_iter()
        .map(|(key, value_json)| UiStateEntry { key, value_json })
        .collect();

    Ok(ExportDomains {
        settings,
        connection_profiles: profiles,
        auth_profiles,
        proxy_profiles,
        ssh_tunnels,
        hook_definitions: hooks,
        services,
        driver_settings,
        history,
        saved_queries,
        recent_items,
        ui_state,
    })
}

// ---------------------------------------------------------------------------
// Import
// ---------------------------------------------------------------------------

/// Imports config from a portable export file into the storage runtime.
///
/// Items with IDs already present in the database are skipped (idempotent merge).
pub fn import_from_file(
    path: &PathBuf,
    runtime: &crate::bootstrap::StorageRuntime,
) -> Result<ImportResult, StorageError> {
    let content = fs::read_to_string(path).map_err(|source| StorageError::Io {
        path: path.clone(),
        source,
    })?;

    let export: ExportFile = serde_json::from_str(&content).map_err(|e| StorageError::Io {
        path: path.clone(),
        source: std::io::Error::other(format!("invalid export file: {}", e)),
    })?;

    if export.magic != EXPORT_MAGIC {
        return Err(StorageError::Io {
            path: path.clone(),
            source: std::io::Error::other(format!(
                "not a DBFlux export file (magic='{}')",
                export.magic
            )),
        });
    }

    if export.version > EXPORT_VERSION {
        return Err(StorageError::Io {
            path: path.clone(),
            source: std::io::Error::other(format!(
                "unsupported export version {} (max supported: {})",
                export.version, EXPORT_VERSION
            )),
        });
    }

    let mut result = ImportResult::default();

    let config_conn = runtime.config_db();
    let state_conn = runtime.state_db();

    // Import settings
    let settings_repo = SettingsRepository::new(config_conn.clone());
    for entry in &export.domains.settings {
        if let Err(e) = settings_repo.set(&entry.key, &entry.value_json) {
            result.errors.push(format!("settings.{}: {}", entry.key, e));
        }
    }
    result.domains_imported += 1;
    result.total_items += export.domains.settings.len();

    // Import connection profiles
    let profiles_repo = ConnectionProfileRepository::new(config_conn.clone());
    let existing_ids: std::collections::HashSet<String> = profiles_repo
        .all()
        .map(|v| v.into_iter().map(|p| p.id).collect())
        .unwrap_or_default();
    for entry in &export.domains.connection_profiles {
        if existing_ids.contains(&entry.id) {
            continue;
        }
        let dto = crate::repositories::connection_profiles::ConnectionProfileDto {
            id: entry.id.clone(),
            name: entry.name.clone(),
            driver_id: entry.driver_id.clone(),
            description: entry.description.clone(),
            favorite: entry.favorite,
            color: entry.color.clone(),
            icon: entry.icon.clone(),
            config_json: entry.config_json.clone(),
            auth_profile_id: entry.auth_profile_id.clone(),
            proxy_profile_id: entry.proxy_profile_id.clone(),
            ssh_tunnel_profile_id: entry.ssh_tunnel_profile_id.clone(),
            access_profile_id: entry.access_profile_id.clone(),
            settings_overrides_json: entry.settings_overrides_json.clone(),
            connection_settings_json: entry.connection_settings_json.clone(),
            hooks_json: entry.hooks_json.clone(),
            hook_bindings_json: entry.hook_bindings_json.clone(),
            value_refs_json: entry.value_refs_json.clone(),
            mcp_governance_json: entry.mcp_governance_json.clone(),
            created_at: entry.created_at.clone(),
            updated_at: entry.updated_at.clone(),
        };
        if let Err(e) = profiles_repo.insert(&dto) {
            result.errors.push(format!("profiles.{}: {}", entry.id, e));
        }
    }
    result.total_items += export.domains.connection_profiles.len();
    result.domains_imported += 1;

    // Import auth profiles
    let auth_repo = AuthProfileRepository::new(config_conn.clone());
    let existing_ids: std::collections::HashSet<String> = auth_repo
        .all()
        .map(|v| v.into_iter().map(|p| p.id.clone()).collect())
        .unwrap_or_default();
    for entry in &export.domains.auth_profiles {
        if existing_ids.contains(&entry.id) {
            continue;
        }
        let dto = crate::repositories::auth_profiles::AuthProfileDto {
            id: entry.id.clone(),
            name: entry.name.clone(),
            provider_id: entry.provider_id.clone(),
            fields_json: entry.fields_json.clone(),
            enabled: entry.enabled,
            created_at: entry.created_at.clone(),
            updated_at: entry.updated_at.clone(),
        };
        if let Err(e) = auth_repo.insert(&dto) {
            result
                .errors
                .push(format!("auth_profiles.{}: {}", entry.id, e));
        }
    }
    result.total_items += export.domains.auth_profiles.len();
    result.domains_imported += 1;

    // Import proxy profiles
    let proxy_repo = ProxyProfileRepository::new(config_conn.clone());
    let existing_ids: std::collections::HashSet<String> = proxy_repo
        .all()
        .map(|v| v.into_iter().map(|p| p.id.clone()).collect())
        .unwrap_or_default();
    for entry in &export.domains.proxy_profiles {
        if existing_ids.contains(&entry.id) {
            continue;
        }
        let dto = crate::repositories::proxy_profiles::ProxyProfileDto {
            id: entry.id.clone(),
            name: entry.name.clone(),
            kind: entry.kind.clone(),
            host: entry.host.clone(),
            port: entry.port,
            auth_json: entry.auth_json.clone(),
            no_proxy: entry.no_proxy.clone(),
            enabled: entry.enabled,
            save_secret: entry.save_secret,
            created_at: entry.created_at.clone(),
            updated_at: entry.updated_at.clone(),
        };
        if let Err(e) = proxy_repo.insert(&dto) {
            result.errors.push(format!("proxies.{}: {}", entry.id, e));
        }
    }
    result.total_items += export.domains.proxy_profiles.len();
    result.domains_imported += 1;

    // Import SSH tunnels
    let ssh_repo = SshTunnelProfileRepository::new(config_conn.clone());
    let existing_ids: std::collections::HashSet<String> = ssh_repo
        .all()
        .map(|v| v.into_iter().map(|p| p.id.clone()).collect())
        .unwrap_or_default();
    for entry in &export.domains.ssh_tunnels {
        if existing_ids.contains(&entry.id) {
            continue;
        }
        let dto = crate::repositories::ssh_tunnel_profiles::SshTunnelProfileDto {
            id: entry.id.clone(),
            name: entry.name.clone(),
            config_json: entry.config_json.clone(),
            save_secret: entry.save_secret,
            created_at: entry.created_at.clone(),
            updated_at: entry.updated_at.clone(),
        };
        if let Err(e) = ssh_repo.insert(&dto) {
            result
                .errors
                .push(format!("ssh_tunnels.{}: {}", entry.id, e));
        }
    }
    result.total_items += export.domains.ssh_tunnels.len();
    result.domains_imported += 1;

    // Import hook definitions
    let hooks_repo = HookDefinitionRepository::new(config_conn.clone());
    let existing_ids: std::collections::HashSet<String> = hooks_repo
        .all()
        .map(|v| v.into_iter().map(|h| h.id.clone()).collect())
        .unwrap_or_default();
    for entry in &export.domains.hook_definitions {
        if existing_ids.contains(&entry.id) {
            continue;
        }
        let dto = crate::repositories::hook_definitions::HookDefinitionDto {
            id: entry.id.clone(),
            name: entry.name.clone(),
            kind_json: entry.kind_json.clone(),
            execution_mode: entry.execution_mode.clone(),
            script_ref: entry.script_ref.clone(),
            command_json: entry.command_json.clone(),
            cwd: entry.cwd.clone(),
            env_json: entry.env_json.clone(),
            inherit_env: entry.inherit_env,
            timeout_ms: entry.timeout_ms,
            ready_signal: entry.ready_signal.clone(),
            on_failure: entry.on_failure.clone(),
            enabled: entry.enabled,
            created_at: entry.created_at.clone(),
            updated_at: entry.updated_at.clone(),
        };
        if let Err(e) = hooks_repo.insert(&dto) {
            result.errors.push(format!("hooks.{}: {}", entry.id, e));
        }
    }
    result.total_items += export.domains.hook_definitions.len();
    result.domains_imported += 1;

    // Import services
    let services_repo = ServiceRepository::new(config_conn.clone());
    for entry in &export.domains.services {
        let dto = crate::repositories::services::ServiceDto {
            socket_id: entry.socket_id.clone(),
            enabled: entry.enabled,
            command: entry.command.clone(),
            args_json: entry.args_json.clone(),
            env_json: entry.env_json.clone(),
            startup_timeout_ms: entry.startup_timeout_ms,
            created_at: entry.created_at.clone(),
            updated_at: entry.updated_at.clone(),
        };
        if let Err(e) = services_repo.upsert(&dto) {
            result
                .errors
                .push(format!("services.{}: {}", entry.socket_id, e));
        }
    }
    result.total_items += export.domains.services.len();
    result.domains_imported += 1;

    // Import driver settings
    let driver_repo = DriverSettingsRepository::new(config_conn.clone());
    for entry in &export.domains.driver_settings {
        let dto = crate::repositories::driver_settings::DriverSettingsDto {
            driver_key: entry.driver_key.clone(),
            overrides_json: entry.overrides_json.clone(),
            settings_json: entry.settings_json.clone(),
            updated_at: entry.updated_at.clone(),
        };
        if let Err(e) = driver_repo.upsert(&dto) {
            result
                .errors
                .push(format!("driver_settings.{}: {}", entry.driver_key, e));
        }
    }
    result.total_items += export.domains.driver_settings.len();
    result.domains_imported += 1;

    // State domains
    let history_repo = QueryHistoryRepository::new(state_conn.clone());
    let existing_ids: std::collections::HashSet<String> = history_repo
        .all()
        .map(|v| v.into_iter().map(|h| h.id).collect())
        .unwrap_or_default();
    for entry in &export.domains.history {
        if existing_ids.contains(&entry.id) {
            continue;
        }
        let dto = QueryHistoryDto {
            id: entry.id.clone(),
            connection_profile_id: entry.connection_profile_id.clone(),
            driver_id: entry.driver_id.clone(),
            database_name: entry.database_name.clone(),
            query_text: entry.query_text.clone(),
            query_kind: entry.query_kind.clone(),
            executed_at: entry.executed_at.clone(),
            duration_ms: entry.duration_ms,
            succeeded: entry.succeeded,
            error_summary: entry.error_summary.clone(),
            row_count: entry.row_count,
            is_favorite: entry.is_favorite,
        };
        if let Err(e) = history_repo.add(&dto) {
            result.errors.push(format!("history.{}: {}", entry.id, e));
        }
    }
    result.total_items += export.domains.history.len();
    result.domains_imported += 1;

    let saved_queries_repo = SavedQueriesRepository::new(state_conn.clone());
    let existing_ids: std::collections::HashSet<String> = saved_queries_repo
        .all()
        .map(|v| v.into_iter().map(|q| q.id).collect())
        .unwrap_or_default();
    for entry in &export.domains.saved_queries {
        if existing_ids.contains(&entry.id) {
            continue;
        }
        let dto = SavedQueryDto {
            id: entry.id.clone(),
            folder_id: entry.folder_id.clone(),
            name: entry.name.clone(),
            sql: entry.sql.clone(),
            is_favorite: entry.is_favorite,
            connection_id: entry.connection_id.clone(),
            created_at: entry.created_at.clone(),
            last_used_at: entry.last_used_at.clone(),
        };
        if let Err(e) = saved_queries_repo.insert(&dto) {
            result
                .errors
                .push(format!("saved_queries.{}: {}", entry.id, e));
        }
    }
    result.total_items += export.domains.saved_queries.len();
    result.domains_imported += 1;

    let recent_repo = RecentItemsRepository::new(state_conn.clone());
    let existing_ids: std::collections::HashSet<String> = recent_repo
        .all()
        .map(|v| v.into_iter().map(|r| r.id).collect())
        .unwrap_or_default();
    for entry in &export.domains.recent_items {
        if existing_ids.contains(&entry.id) {
            continue;
        }
        let dto = RecentItemDto {
            id: entry.id.clone(),
            kind: entry.kind.clone(),
            profile_id: entry.profile_id.clone(),
            path: entry.path.clone(),
            title: entry.title.clone(),
            accessed_at: entry.accessed_at.clone(),
        };
        if let Err(e) = recent_repo.record_access(&dto) {
            result
                .errors
                .push(format!("recent_items.{}: {}", entry.id, e));
        }
    }
    result.total_items += export.domains.recent_items.len();
    result.domains_imported += 1;

    // UI state
    let ui_state_repo = UiStateRepository::new(state_conn.clone());
    for entry in &export.domains.ui_state {
        if let Err(e) = ui_state_repo.set(&entry.key, &entry.value_json) {
            result.errors.push(format!("ui_state.{}: {}", entry.key, e));
        }
    }
    result.total_items += export.domains.ui_state.len();
    result.domains_imported += 1;

    info!(
        "Imported {} items ({} domains, {} errors)",
        result.total_items,
        result.domains_imported,
        result.errors.len()
    );

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_export_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux_export_{}_{}.json",
            name,
            std::process::id()
        ))
    }

    fn temp_runtime_with_isolated_dirs(name: &str) -> (crate::bootstrap::StorageRuntime, PathBuf) {
        let temp_label = format!("dbflux_export_test_{}_{}", name, std::process::id());
        let temp_dir = std::env::temp_dir().join(&temp_label);
        std::fs::create_dir_all(&temp_dir).unwrap();
        let config_db_path = temp_dir.join("config.db");
        let state_db_path = temp_dir.join("state.db");
        let runtime = crate::bootstrap::StorageRuntime::for_path(config_db_path, state_db_path)
            .expect("temp runtime");
        (runtime, temp_dir)
    }

    #[test]
    fn export_creates_valid_file() {
        let (runtime, temp_dir) = temp_runtime_with_isolated_dirs("create_valid");
        let path = temp_export_path("create_valid");

        let result = export_to_file(&path, &runtime).expect("export should succeed");
        assert!(path.exists());

        let content = std::fs::read_to_string(&path).unwrap();
        let export: ExportFile = serde_json::from_str(&content).unwrap();
        assert_eq!(export.magic, EXPORT_MAGIC);
        assert_eq!(export.version, EXPORT_VERSION);
        assert!(!export.exported_at.is_empty());

        // A fresh runtime has empty stores, so the export is empty but valid
        assert_eq!(result.domain_counts.total(), 0);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn export_import_roundtrip() {
        let (runtime, temp_dir) = temp_runtime_with_isolated_dirs("roundtrip");
        let path = temp_export_path("roundtrip");

        // Export
        let export_result = export_to_file(&path, &runtime).expect("export");
        assert!(path.exists());

        // Import into fresh runtime
        let (runtime2, temp_dir2) = temp_runtime_with_isolated_dirs("roundtrip2");
        let import_result = import_from_file(&path, &runtime2).expect("import should succeed");

        assert!(!import_result.has_errors());
        assert_eq!(import_result.total_items, export_result.total_items);

        // Re-export and compare counts
        let path2 = temp_export_path("roundtrip2");
        let export_result2 = export_to_file(&path2, &runtime2).expect("re-export");
        assert_eq!(
            export_result2.domain_counts.total(),
            export_result.domain_counts.total()
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&path2);
        let _ = std::fs::remove_dir_all(&temp_dir);
        let _ = std::fs::remove_dir_all(&temp_dir2);
    }

    #[test]
    fn import_rejects_invalid_magic() {
        let (runtime, temp_dir) = temp_runtime_with_isolated_dirs("invalid_magic");
        let path = temp_export_path("invalid_magic");

        let invalid: ExportFile = ExportFile {
            magic: "NOT_A_DBFLUX_FILE".to_string(),
            version: 1,
            exported_at: chrono::Utc::now().to_rfc3339(),
            domains: ExportDomains::default(),
        };
        std::fs::write(&path, serde_json::to_string(&invalid).unwrap()).unwrap();

        let result = import_from_file(&path, &runtime);
        assert!(result.is_err(), "should reject invalid magic");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn import_skips_existing_items() {
        let (runtime, temp_dir) = temp_runtime_with_isolated_dirs("skip_existing");
        let path = temp_export_path("skip_existing");

        // Export first
        export_to_file(&path, &runtime).expect("export");

        // Import again - should skip existing items (idempotent)
        let result = import_from_file(&path, &runtime).expect("re-import should succeed");
        assert!(!result.has_errors());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn export_domain_counts_accurate() {
        let (runtime, temp_dir) = temp_runtime_with_isolated_dirs("counts");
        let path = temp_export_path("counts");

        let result = export_to_file(&path, &runtime).expect("export");
        assert_eq!(result.total_items, result.domain_counts.total());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn import_rejects_future_version() {
        let (runtime, temp_dir) = temp_runtime_with_isolated_dirs("future_version");
        let path = temp_export_path("future_version");

        let future: ExportFile = ExportFile {
            magic: EXPORT_MAGIC.to_string(),
            version: EXPORT_VERSION + 1,
            exported_at: chrono::Utc::now().to_rfc3339(),
            domains: ExportDomains::default(),
        };
        std::fs::write(&path, serde_json::to_string(&future).unwrap()).unwrap();

        let result = import_from_file(&path, &runtime);
        assert!(result.is_err(), "should reject future version");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
