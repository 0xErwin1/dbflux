//! Config export/import support in portable JSON format.
//!
//! Exports all durable config domains (connection profiles, auth profiles,
//! proxy profiles, SSH tunnel profiles, hook definitions, services, settings,
//! driver settings) to a single portable JSON file. Supports importing from
//! such files to restore configuration.

use dbflux_core::{ProxyAuth, SshAuthMethod, SshTunnelConfig};
use log::info;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

use crate::error::StorageError;
use crate::repositories::auth_profiles::AuthProfileRepository;
use crate::repositories::connection_profiles::ConnectionProfileRepository;
use crate::repositories::driver_overrides::DriverOverridesRepository;
use crate::repositories::driver_setting_values::DriverSettingValuesRepository;
use crate::repositories::general_settings::{GeneralSettingsDto, GeneralSettingsRepository};
use crate::repositories::governance_settings::{
    GovernanceSettingsDto, GovernanceSettingsRepository, PolicyRoleDto, ToolPolicyDto,
    TrustedClientDto,
};
use crate::repositories::hook_definitions::HookDefinitionRepository;
use crate::repositories::proxy_auth::ProxyAuthRepository;
use crate::repositories::proxy_profiles::ProxyProfileRepository;
use crate::repositories::services::ServiceRepository;
use crate::repositories::ssh_tunnel_auth::SshTunnelAuthRepository;
use crate::repositories::ssh_tunnel_profiles::SshTunnelProfileRepository;
use crate::repositories::state::query_history::{QueryHistoryDto, QueryHistoryRepository};
use crate::repositories::state::recent_items::{RecentItemDto, RecentItemsRepository};
use crate::repositories::state::saved_queries::{SavedQueriesRepository, SavedQueryDto};
use crate::repositories::state::ui_state::UiStateRepository;

/// Current export format version.
const EXPORT_VERSION: u32 = 2;

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
    pub general_settings: Option<GeneralSettingsExport>,

    #[serde(default)]
    pub governance_settings: Option<GovernanceSettingsExport>,

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
    pub driver_overrides: Vec<DriverOverrideEntry>,

    #[serde(default)]
    pub driver_setting_values: Vec<DriverSettingValueExport>,

    #[serde(default)]
    pub history: Vec<HistoryEntry>,

    #[serde(default)]
    pub saved_queries: Vec<SavedQueryEntry>,

    #[serde(default)]
    pub recent_items: Vec<RecentItemEntry>,

    #[serde(default)]
    pub ui_state: Vec<UiStateEntry>,
}

/// General settings export (singleton).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GeneralSettingsExport {
    pub theme: String,
    pub restore_session_on_startup: bool,
    pub reopen_last_connections: bool,
    pub default_focus_on_startup: String,
    pub max_history_entries: i64,
    pub auto_save_interval_ms: i64,
    pub default_refresh_policy: String,
    pub default_refresh_interval_secs: i32,
    pub max_concurrent_background_tasks: i64,
    pub auto_refresh_pause_on_error: bool,
    pub auto_refresh_only_if_visible: bool,
    pub confirm_dangerous_queries: bool,
    pub dangerous_requires_where: bool,
    pub dangerous_requires_preview: bool,
}

/// Governance settings export (singleton with children).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GovernanceSettingsExport {
    pub mcp_enabled_by_default: bool,
    #[serde(default)]
    pub trusted_clients: Vec<TrustedClientExport>,
    #[serde(default)]
    pub policy_roles: Vec<PolicyRoleExport>,
    #[serde(default)]
    pub tool_policies: Vec<ToolPolicyExport>,
}

/// Trusted client export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustedClientExport {
    pub id: String,
    pub client_id: String,
    pub name: String,
    pub issuer: Option<String>,
    pub active: bool,
}

/// Policy role export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRoleExport {
    pub id: String,
    pub role_id: String,
}

/// Tool policy export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolPolicyExport {
    pub id: String,
    pub policy_id: String,
    /// Allowed tool names (normalized from tool_policy_allowed_tools child table)
    pub allowed_tools: Vec<String>,
    /// Allowed class names (normalized from tool_policy_allowed_classes child table)
    pub allowed_classes: Vec<String>,
}

/// Connection profile entry.
/// Note: config_json, settings_overrides_json, connection_settings_json, hooks_json,
/// hook_bindings_json, value_refs_json, mcp_governance_json all dropped in v10/v12.
/// Driver config is now stored in connection_profile_configs child table.
/// Note: access_profile_id removed - it was dead code (never populated).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfileEntry {
    pub id: String,
    pub name: String,
    pub driver_id: Option<String>,
    pub description: Option<String>,
    pub favorite: bool,
    pub color: Option<String>,
    pub icon: Option<String>,
    pub save_password: bool,
    pub kind: Option<String>,
    pub access_kind: Option<String>,
    pub access_provider: Option<String>,
    pub auth_profile_id: Option<String>,
    pub proxy_profile_id: Option<String>,
    pub ssh_tunnel_profile_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Auth profile entry.
/// Note: fields_json dropped in migration v10.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProfileEntry {
    pub id: String,
    pub name: String,
    pub provider_id: String,
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
/// Note: kind_json, command_json, env_json dropped in migration v10.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookDefinitionEntry {
    pub id: String,
    pub name: String,
    pub execution_mode: String,
    pub script_ref: Option<String>,
    pub cwd: Option<String>,
    pub inherit_env: bool,
    pub timeout_ms: Option<i64>,
    pub ready_signal: Option<String>,
    pub on_failure: String,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// Service entry.
/// Note: args_json, env_json dropped in migration v10.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceEntry {
    pub socket_id: String,
    pub enabled: bool,
    pub command: Option<String>,
    pub startup_timeout_ms: Option<i64>,
    pub created_at: String,
    pub updated_at: String,
}

/// Driver override export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverOverrideEntry {
    pub driver_key: String,
    pub refresh_policy: Option<String>,
    pub refresh_interval_secs: Option<i32>,
    pub confirm_dangerous: Option<bool>,
    pub requires_where: Option<bool>,
    pub requires_preview: Option<bool>,
}

/// Driver setting value export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverSettingValueExport {
    pub id: String,
    pub driver_key: String,
    pub setting_key: String,
    pub setting_value: Option<String>,
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
    pub general_settings: usize,
    pub governance_settings: usize,
    pub profiles: usize,
    pub auth_profiles: usize,
    pub proxies: usize,
    pub ssh_tunnels: usize,
    pub hooks: usize,
    pub services: usize,
    pub driver_overrides: usize,
    pub driver_setting_values: usize,
    pub history: usize,
    pub saved_queries: usize,
    pub recent_items: usize,
    pub ui_state: usize,
}

impl ExportDomainCounts {
    pub fn total(&self) -> usize {
        self.general_settings
            + self.governance_settings
            + self.profiles
            + self.auth_profiles
            + self.proxies
            + self.ssh_tunnels
            + self.hooks
            + self.services
            + self.driver_overrides
            + self.driver_setting_values
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
        general_settings: if export.domains.general_settings.is_some() {
            1
        } else {
            0
        },
        governance_settings: if export.domains.governance_settings.is_some() {
            1
        } else {
            0
        },
        profiles: export.domains.connection_profiles.len(),
        auth_profiles: export.domains.auth_profiles.len(),
        proxies: export.domains.proxy_profiles.len(),
        ssh_tunnels: export.domains.ssh_tunnels.len(),
        hooks: export.domains.hook_definitions.len(),
        services: export.domains.services.len(),
        driver_overrides: export.domains.driver_overrides.len(),
        driver_setting_values: export.domains.driver_setting_values.len(),
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
    let general_settings_repo = GeneralSettingsRepository::new(config_conn.clone());
    let governance_settings_repo = GovernanceSettingsRepository::new(config_conn.clone());
    let profiles_repo = ConnectionProfileRepository::new(config_conn.clone());
    let auth_repo = AuthProfileRepository::new(config_conn.clone());
    let proxy_repo = ProxyProfileRepository::new(config_conn.clone());
    let ssh_repo = SshTunnelProfileRepository::new(config_conn.clone());
    let hooks_repo = HookDefinitionRepository::new(config_conn.clone());
    let services_repo = ServiceRepository::new(config_conn.clone());
    let driver_overrides_repo = DriverOverridesRepository::new(config_conn.clone());
    let driver_values_repo = DriverSettingValuesRepository::new(config_conn.clone());

    // Export general settings (singleton)
    let general_settings = general_settings_repo
        .get()?
        .map(|dto| GeneralSettingsExport {
            theme: dto.theme,
            restore_session_on_startup: dto.restore_session_on_startup != 0,
            reopen_last_connections: dto.reopen_last_connections != 0,
            default_focus_on_startup: dto.default_focus_on_startup,
            max_history_entries: dto.max_history_entries,
            auto_save_interval_ms: dto.auto_save_interval_ms,
            default_refresh_policy: dto.default_refresh_policy,
            default_refresh_interval_secs: dto.default_refresh_interval_secs,
            max_concurrent_background_tasks: dto.max_concurrent_background_tasks,
            auto_refresh_pause_on_error: dto.auto_refresh_pause_on_error != 0,
            auto_refresh_only_if_visible: dto.auto_refresh_only_if_visible != 0,
            confirm_dangerous_queries: dto.confirm_dangerous_queries != 0,
            dangerous_requires_where: dto.dangerous_requires_where != 0,
            dangerous_requires_preview: dto.dangerous_requires_preview != 0,
        });

    // Export governance settings (singleton with children)
    let governance_settings = governance_settings_repo.get()?.map(|dto| {
        let trusted_clients = governance_settings_repo
            .get_trusted_clients()
            .unwrap_or_default()
            .into_iter()
            .map(|c| TrustedClientExport {
                id: c.id,
                client_id: c.client_id,
                name: c.name,
                issuer: c.issuer,
                active: c.active != 0,
            })
            .collect();
        let policy_roles = governance_settings_repo
            .get_policy_roles()
            .unwrap_or_default()
            .into_iter()
            .map(|r| PolicyRoleExport {
                id: r.id,
                role_id: r.role_id,
            })
            .collect();
        let tool_policies = governance_settings_repo
            .get_tool_policies()
            .unwrap_or_default()
            .into_iter()
            .map(|p| ToolPolicyExport {
                id: p.id,
                policy_id: p.policy_id,
                allowed_tools: p.allowed_tools,
                allowed_classes: p.allowed_classes,
            })
            .collect();
        GovernanceSettingsExport {
            mcp_enabled_by_default: dto.mcp_enabled_by_default != 0,
            trusted_clients,
            policy_roles,
            tool_policies,
        }
    });

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
            save_password: e.save_password,
            kind: e.kind,
            access_kind: e.access_kind,
            access_provider: e.access_provider,
            auth_profile_id: e.auth_profile_id,
            proxy_profile_id: e.proxy_profile_id,
            ssh_tunnel_profile_id: e.ssh_tunnel_profile_id,
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
            enabled: e.enabled,
            created_at: e.created_at,
            updated_at: e.updated_at,
        })
        .collect();

    // Export proxy profiles with auth_json reconstructed from native columns
    let proxy_auth_repo = ProxyAuthRepository::new(config_conn.clone());
    let proxy_profiles = proxy_repo
        .all()?
        .into_iter()
        .map(|e| {
            // Reconstruct auth_json from native auth_kind
            let auth_json = match e.auth_kind.to_lowercase().as_str() {
                "basic" => {
                    let auth_data = proxy_auth_repo.get(&e.id).ok().flatten();
                    let username = auth_data
                        .as_ref()
                        .and_then(|a| a.username.clone())
                        .unwrap_or_default();
                    serde_json::to_string(&ProxyAuth::Basic { username })
                        .unwrap_or_else(|_| r#"{"None":{}}"#.to_string())
                }
                _ => serde_json::to_string(&ProxyAuth::None)
                    .unwrap_or_else(|_| r#"{"None":{}}"#.to_string()),
            };
            ProxyProfileEntry {
                id: e.id,
                name: e.name,
                kind: e.kind,
                host: e.host,
                port: e.port,
                auth_json,
                no_proxy: e.no_proxy,
                enabled: e.enabled,
                save_secret: e.save_secret,
                created_at: e.created_at,
                updated_at: e.updated_at,
            }
        })
        .collect();

    // Export SSH tunnels with config_json reconstructed from native columns
    let ssh_auth_repo = SshTunnelAuthRepository::new(config_conn.clone());
    let ssh_tunnels = ssh_repo
        .all()?
        .into_iter()
        .map(|e| {
            // Reconstruct config_json from native columns
            let auth_method = match e.auth_method.to_lowercase().as_str() {
                "key" => {
                    let auth_data = ssh_auth_repo.get(&e.id).ok().flatten();
                    let key_path = auth_data
                        .and_then(|a| a.key_path.clone())
                        .map(PathBuf::from);
                    SshAuthMethod::PrivateKey { key_path }
                }
                _ => SshAuthMethod::Password,
            };
            let config = SshTunnelConfig {
                host: e.host.clone(),
                port: e.port as u16,
                user: e.user.clone(),
                auth_method,
            };
            let config_json = serde_json::to_string(&config)
                .unwrap_or_else(|_| r#"{"host":"","port":22,"user":""}"#.to_string());
            SshTunnelEntry {
                id: e.id,
                name: e.name,
                config_json,
                save_secret: e.save_secret,
                created_at: e.created_at,
                updated_at: e.updated_at,
            }
        })
        .collect();

    let hooks = hooks_repo
        .all()?
        .into_iter()
        .map(|e| HookDefinitionEntry {
            id: e.id,
            name: e.name,
            execution_mode: e.execution_mode,
            script_ref: e.script_ref,
            cwd: e.cwd,
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
            startup_timeout_ms: e.startup_timeout_ms,
            created_at: e.created_at,
            updated_at: e.updated_at,
        })
        .collect();

    // Export driver overrides
    let driver_overrides = driver_overrides_repo
        .all()?
        .into_iter()
        .map(|e| DriverOverrideEntry {
            driver_key: e.driver_key,
            refresh_policy: e.refresh_policy,
            refresh_interval_secs: e.refresh_interval_secs,
            confirm_dangerous: e.confirm_dangerous.map(|v| v != 0),
            requires_where: e.requires_where.map(|v| v != 0),
            requires_preview: e.requires_preview.map(|v| v != 0),
        })
        .collect();

    // Export driver setting values - collect all values for all drivers
    let mut driver_setting_values = Vec::new();
    for override_entry in driver_overrides_repo.all()?.iter() {
        let values = driver_values_repo.get_for_driver(&override_entry.driver_key)?;
        for v in values {
            driver_setting_values.push(DriverSettingValueExport {
                id: v.id,
                driver_key: v.driver_key,
                setting_key: v.setting_key,
                setting_value: v.setting_value,
            });
        }
    }

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
        general_settings,
        governance_settings,
        connection_profiles: profiles,
        auth_profiles,
        proxy_profiles,
        ssh_tunnels,
        hook_definitions: hooks,
        services,
        driver_overrides,
        driver_setting_values,
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

    // Import general settings
    if let Some(settings) = &export.domains.general_settings {
        let repo = GeneralSettingsRepository::new(config_conn.clone());
        let dto = GeneralSettingsDto {
            id: 1,
            theme: settings.theme.clone(),
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
            default_focus_on_startup: settings.default_focus_on_startup.clone(),
            max_history_entries: settings.max_history_entries,
            auto_save_interval_ms: settings.auto_save_interval_ms,
            default_refresh_policy: settings.default_refresh_policy.clone(),
            default_refresh_interval_secs: settings.default_refresh_interval_secs,
            max_concurrent_background_tasks: settings.max_concurrent_background_tasks,
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
        if let Err(e) = repo.upsert(&dto) {
            result.errors.push(format!("general_settings: {}", e));
        }
        result.total_items += 1;
    }
    result.domains_imported += 1;

    // Import governance settings
    if let Some(governance) = &export.domains.governance_settings {
        let repo = GovernanceSettingsRepository::new(config_conn.clone());
        let dto = GovernanceSettingsDto {
            id: 1,
            mcp_enabled_by_default: if governance.mcp_enabled_by_default {
                1
            } else {
                0
            },
            updated_at: String::new(),
        };
        if let Err(e) = repo.upsert(&dto) {
            result.errors.push(format!("governance_settings: {}", e));
        }

        // Import trusted clients
        let trusted_clients: Vec<TrustedClientDto> = governance
            .trusted_clients
            .iter()
            .map(|c| TrustedClientDto {
                id: c.id.clone(),
                governance_id: 1,
                client_id: c.client_id.clone(),
                name: c.name.clone(),
                issuer: c.issuer.clone(),
                active: if c.active { 1 } else { 0 },
            })
            .collect();
        if let Err(e) = repo.replace_trusted_clients(&trusted_clients) {
            result
                .errors
                .push(format!("governance.trusted_clients: {}", e));
        }

        // Import policy roles
        let policy_roles: Vec<PolicyRoleDto> = governance
            .policy_roles
            .iter()
            .map(|r| PolicyRoleDto {
                id: r.id.clone(),
                governance_id: 1,
                role_id: r.role_id.clone(),
            })
            .collect();
        if let Err(e) = repo.replace_policy_roles(&policy_roles) {
            result
                .errors
                .push(format!("governance.policy_roles: {}", e));
        }

        // Import tool policies
        let tool_policies: Vec<ToolPolicyDto> = governance
            .tool_policies
            .iter()
            .map(|p| ToolPolicyDto {
                id: p.id.clone(),
                governance_id: 1,
                policy_id: p.policy_id.clone(),
                allowed_tools: p.allowed_tools.clone(),
                allowed_classes: p.allowed_classes.clone(),
            })
            .collect();
        if let Err(e) = repo.replace_tool_policies(&tool_policies) {
            result
                .errors
                .push(format!("governance.tool_policies: {}", e));
        }

        result.total_items += 1;
    }
    result.domains_imported += 1;

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
            save_password: entry.save_password,
            kind: entry.kind.clone(),
            access_kind: entry.access_kind.clone(),
            access_provider: entry.access_provider.clone(),
            auth_profile_id: entry.auth_profile_id.clone(),
            proxy_profile_id: entry.proxy_profile_id.clone(),
            ssh_tunnel_profile_id: entry.ssh_tunnel_profile_id.clone(),
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
        // Parse auth_json to extract auth_kind and credentials
        let auth: ProxyAuth = serde_json::from_str(&entry.auth_json).unwrap_or(ProxyAuth::None);
        let (auth_kind, auth_username) = match &auth {
            ProxyAuth::None => ("none".to_string(), None),
            ProxyAuth::Basic { username } => ("basic".to_string(), Some(username.clone())),
        };
        let dto = crate::repositories::proxy_profiles::ProxyProfileDto {
            id: entry.id.clone(),
            name: entry.name.clone(),
            kind: entry.kind.clone(),
            host: entry.host.clone(),
            port: entry.port,
            auth_kind,
            no_proxy: entry.no_proxy.clone(),
            enabled: entry.enabled,
            save_secret: entry.save_secret,
            created_at: entry.created_at.clone(),
            updated_at: entry.updated_at.clone(),
        };
        // Create auth DTO if we have credentials
        let auth_dto = if auth_username.is_some() {
            Some(crate::repositories::proxy_auth::ProxyAuthDto {
                proxy_profile_id: entry.id.clone(),
                username: auth_username,
                domain: None,
                password_secret_ref: None,
            })
        } else {
            None
        };
        if let Err(e) = proxy_repo.insert(&dto, auth_dto.as_ref()) {
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
        // Parse config_json to extract native columns
        let config: SshTunnelConfig =
            serde_json::from_str(&entry.config_json).unwrap_or_else(|_| SshTunnelConfig {
                host: String::new(),
                port: 22,
                user: String::new(),
                auth_method: SshAuthMethod::Password,
            });
        let auth_method_str = match config.auth_method {
            SshAuthMethod::Password => "password".to_string(),
            SshAuthMethod::PrivateKey { .. } => "key".to_string(),
        };
        // Extract credentials before moving from config
        let (key_path_str, password_ref) = match config.auth_method {
            SshAuthMethod::Password => (None, None),
            SshAuthMethod::PrivateKey { key_path } => {
                (key_path.map(|p| p.to_string_lossy().to_string()), None)
            }
        };
        let dto = crate::repositories::ssh_tunnel_profiles::SshTunnelProfileDto {
            id: entry.id.clone(),
            name: entry.name.clone(),
            host: config.host,
            port: config.port as i32,
            user: config.user,
            auth_method: auth_method_str,
            key_path: None,
            passphrase_secret_ref: None,
            password_secret_ref: None,
            save_secret: entry.save_secret,
            created_at: entry.created_at.clone(),
            updated_at: entry.updated_at.clone(),
        };
        // Create auth DTO if we have credentials
        let auth_dto = if key_path_str.is_some() || password_ref.is_some() {
            Some(crate::repositories::ssh_tunnel_auth::SshTunnelAuthDto {
                ssh_tunnel_profile_id: entry.id.clone(),
                key_path: key_path_str,
                password_secret_ref: password_ref,
                passphrase_secret_ref: None,
            })
        } else {
            None
        };
        if let Err(e) = ssh_repo.insert(&dto, auth_dto.as_ref()) {
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
            execution_mode: entry.execution_mode.clone(),
            script_ref: entry.script_ref.clone(),
            cwd: entry.cwd.clone(),
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

    // Import driver overrides and setting values
    let overrides_repo = DriverOverridesRepository::new(config_conn.clone());
    let values_repo = DriverSettingValuesRepository::new(config_conn.clone());
    for entry in &export.domains.driver_overrides {
        let dto = crate::repositories::driver_overrides::DriverOverridesDto {
            driver_key: entry.driver_key.clone(),
            refresh_policy: entry.refresh_policy.clone(),
            refresh_interval_secs: entry.refresh_interval_secs,
            confirm_dangerous: entry.confirm_dangerous.map(|v| if v { 1 } else { 0 }),
            requires_where: entry.requires_where.map(|v| if v { 1 } else { 0 }),
            requires_preview: entry.requires_preview.map(|v| if v { 1 } else { 0 }),
            updated_at: String::new(),
        };
        if let Err(e) = overrides_repo.upsert(&dto) {
            result
                .errors
                .push(format!("driver_overrides.{}: {}", entry.driver_key, e));
        }
    }
    result.total_items += export.domains.driver_overrides.len();

    for entry in &export.domains.driver_setting_values {
        let dto = crate::repositories::driver_setting_values::DriverSettingValueDto {
            id: entry.id.clone(),
            driver_key: entry.driver_key.clone(),
            setting_key: entry.setting_key.clone(),
            setting_value: entry.setting_value.clone(),
        };
        if let Err(e) = values_repo.upsert(&dto) {
            result
                .errors
                .push(format!("driver_setting_values.{}: {}", entry.driver_key, e));
        }
    }
    result.total_items += export.domains.driver_setting_values.len();
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

        // A fresh runtime has the settings singletons populated with defaults
        // (general_settings and governance_settings), so the count reflects that.
        // The exact count depends on how many singleton domains exist.
        assert!(
            result.domain_counts.general_settings == 1,
            "general_settings should be present"
        );
        assert!(
            result.domain_counts.governance_settings == 1,
            "governance_settings should be present"
        );

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
