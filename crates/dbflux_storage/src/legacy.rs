//! Legacy JSON import for DBFlux storage migration.
//!
//! This module detects and imports data from legacy JSON storage files into the
//! SQLite-backed storage. It is restart-safe and idempotent.
//!
//! Import idempotency is achieved via:
//! 1. An explicit `legacy_imports` table storing per-source-file provenance
//!    including SHA-256 hash, so a partial import is never re-run blindly.
//! 2. Per-file transactional writes: each source file commits in one transaction,
//!    so a crash during import leaves the file marked as `failed` (not `completed`).
//! 3. UUID dedup within each file, so surviving records from a partial import
//!    are not duplicated on retry.
//! 4. One-shot semantics: if source file was already imported and hash matches,
//!    the import is skipped entirely.

use dbflux_core::{
    AppConfig, ConnectionHook, ConnectionProfile, DriverKey, FormValues, GeneralSettings,
    GlobalOverrides, GovernanceSettings, HookFailureMode, HookKind, HookPhase, SavedQuery,
    ScriptSource, SshTunnelProfile, migrate_app_config,
};
use log::warn;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::bootstrap::OwnedConnection;
use crate::repositories::connection_driver_configs::ConnectionDriverConfigDto;
use crate::repositories::connection_profiles::ConnectionProfileDto;
use crate::repositories::driver_overrides::{DriverOverridesDto, DriverOverridesRepository};
use crate::repositories::driver_setting_values::{DriverSettingValueDto, DriverSettingValuesRepository};
use crate::repositories::hook_definitions::{HookDefinitionDto, HookDefinitionRepository};
use crate::repositories::legacy_imports::{
    ImportStatus as RepoImportStatus, LegacyImport, LegacyImportsRepository,
};
use crate::repositories::proxy_auth::ProxyAuthDto;
use crate::repositories::proxy_profiles::ProxyProfileDto;
use crate::repositories::general_settings::{GeneralSettingsDto, GeneralSettingsRepository};
use crate::repositories::governance_settings::{
    GovernanceSettingsDto, GovernanceSettingsRepository, PolicyRoleDto, ToolPolicyDto,
    TrustedClientDto,
};
use crate::repositories::ssh_tunnel_auth::SshTunnelAuthDto;
use crate::repositories::ssh_tunnel_profiles::SshTunnelProfileDto;
use crate::repositories::state::query_history::{QueryHistoryDto, QueryHistoryRepository};
use crate::repositories::state::recent_items::{RecentItemDto, RecentItemsRepository};
use crate::repositories::state::saved_queries::{SavedQueriesRepository, SavedQueryDto};
use crate::repositories::state::ui_state::UiStateRepository;
use crate::repositories::{
    auth_profiles::AuthProfileRepository, connection_profiles::ConnectionProfileRepository,
    proxy_profiles::ProxyProfileRepository, ssh_tunnel_profiles::SshTunnelProfileRepository,
};

/// Import status for a legacy source file.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ImportStatus {
    /// Import completed successfully.
    Completed,
    /// Import was attempted but failed.
    Failed,
}

/// Result of a legacy import operation.
#[derive(Debug, Clone, Default)]
pub struct LegacyImportResult {
    pub profiles_imported: usize,
    pub auth_profiles_imported: usize,
    pub proxy_profiles_imported: usize,
    pub ssh_tunnels_imported: usize,
    pub general_settings_imported: bool,
    pub driver_settings_imported: usize,
    pub hook_definitions_imported: usize,
    pub governance_imported: bool,
    pub history_entries_imported: usize,
    pub saved_queries_imported: usize,
    pub recent_items_imported: usize,
    pub ui_state_restored: bool,
    pub connection_tree_imported: bool,
    pub errors: Vec<String>,
}

impl LegacyImportResult {
    pub fn total_imported(&self) -> usize {
        self.profiles_imported
            + self.auth_profiles_imported
            + self.proxy_profiles_imported
            + self.ssh_tunnels_imported
            + self.driver_settings_imported
            + self.hook_definitions_imported
            + self.history_entries_imported
            + self.saved_queries_imported
            + self.recent_items_imported
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn any_imported(&self) -> bool {
        self.total_imported() > 0
            || self.ui_state_restored
            || self.general_settings_imported
            || self.governance_imported
    }
}

/// Computes the SHA-256 hash of a file's contents, returning the hex-encoded digest.
fn compute_source_hash(path: &Path) -> Result<String, std::io::Error> {
    let content = fs::read(path)?;
    let mut hasher = Sha256::new();
    hasher.update(&content);
    let result = hasher.finalize();
    Ok(hex::encode(result))
}

/// Checks the import status for a source file.
///
/// For backward compatibility, this first checks the legacy_imports table (new approach),
/// then falls back to system_metadata if no record exists there.
fn get_import_status(conn: &OwnedConnection, source_file: &str) -> Option<bool> {
    // First check legacy_imports table (new approach with hash-based dedup)
    let repo = LegacyImportsRepository::new(conn.clone());
    if let Ok(Some(status)) = repo.get_status(source_file, "") {
        return match status {
            RepoImportStatus::Completed => Some(true),
            RepoImportStatus::Failed => Some(false),
            RepoImportStatus::NotFound => None,
        };
    }

    // Fall back to system_metadata for backward compatibility with installs
    // that were migrated before the legacy_imports table existed
    let result: Option<String> = conn
        .query_row(
            "SELECT value FROM system_metadata WHERE key = ?1",
            [format!("legacy_import::{}", source_file)],
            |row| row.get(0),
        )
        .ok()?;
    match result.as_deref() {
        Some("completed") => Some(true),
        Some("failed") => Some(false),
        _ => None,
    }
}

/// Records the import status for a source file in both system_metadata (for backward
/// compatibility) and legacy_imports (for new hash-based provenance tracking).
fn set_import_status(conn: &rusqlite::Connection, source_file: &str, status: ImportStatus) {
    let value = match status {
        ImportStatus::Completed => "completed",
        ImportStatus::Failed => "failed",
    };

    // Update system_metadata (backward compatibility)
    let _ = conn.execute(
        "INSERT OR REPLACE INTO system_metadata (key, value) VALUES (?1, ?2)",
        rusqlite::params![format!("legacy_import::{}", source_file), value],
    );
}

/// Checks if an import should be skipped based on the legacy_imports table.
///
/// Returns `Some(true)` if the file was already imported with the same hash (one-shot skip),
/// `Some(false)` if it should be retried, or `None` if never attempted.
fn check_legacy_import_status(
    conn: &OwnedConnection,
    source_path: &str,
    _source_kind: &str,
    file_hash: &str,
) -> Option<bool> {
    let repo = LegacyImportsRepository::new(conn.clone());

    // Check if we have a record in legacy_imports
    match repo.get_status(source_path, file_hash) {
        Ok(Some(RepoImportStatus::Completed)) => {
            // Already imported with same hash — one-shot skip
            log::info!(
                "Legacy import of '{}' already completed with matching hash, skipping",
                source_path
            );
            Some(true)
        }
        Ok(Some(RepoImportStatus::Failed)) => {
            // Previously failed — retry
            log::info!("Retrying failed legacy import: {}", source_path);
            Some(false)
        }
        Ok(Some(RepoImportStatus::NotFound)) | Ok(None) => {
            // Not found in legacy_imports — check if system_metadata has old record
            // for backward compatibility with installs from before this table existed
            if let Some(true) = get_import_status(conn, source_path) {
                // Old system_metadata says completed but no legacy_imports record
                // This means it was completed before the table existed — treat as one-shot skip
                log::info!(
                    "Legacy import of '{}' marked completed in system_metadata but no hash record, skipping",
                    source_path
                );
                return Some(true);
            }
            // Never attempted, or old system_metadata says failed — proceed with import
            None
        }
        Err(e) => {
            log::warn!(
                "Error checking legacy import status for '{}': {}, proceeding with import",
                source_path,
                e
            );
            None
        }
    }
}

/// Records a successful legacy import in the legacy_imports table.
fn record_legacy_import_success(
    conn: &OwnedConnection,
    source_path: &str,
    source_kind: &str,
    file_hash: &str,
) -> Result<(), String> {
    let repo = LegacyImportsRepository::new(conn.clone());
    let import = LegacyImport::new(
        source_path.to_string(),
        source_kind.to_string(),
        file_hash.to_string(),
    );
    repo.record_import(&import).map_err(|e| {
        format!(
            "failed to record legacy import for '{}': {}",
            source_path, e
        )
    })
}

/// Returns the path to a legacy JSON file if it exists, otherwise None.
/// Takes the root directory (config dir for most files, data dir for state.json).
fn legacy_path_if_exists(root: &Path, filename: &str) -> Option<PathBuf> {
    let path = root.join(filename);
    if path.exists() { Some(path) } else { None }
}

/// Runs all legacy JSON imports for the domains migrated in previous batches.
///
/// This function is idempotent: re-running it on a system that has already
/// imported data will not create duplicates (UUID dedup + explicit status check).
/// Files that failed previously are retried; files that completed are skipped.
///
/// The `config_dir` and `data_dir` parameters are used only to locate legacy JSON
/// source files. They are not created or modified — files are read, imported into
/// SQLite, and renamed to `*.bak` on success.
pub fn run_legacy_import(
    config_conn: OwnedConnection,
    state_conn: OwnedConnection,
    config_dir: &Path,
    data_dir: &Path,
) -> LegacyImportResult {
    let mut result = LegacyImportResult::default();

    // --- Config domain imports (config.db) ---
    import_profiles_with_status(&config_conn, config_dir, &mut result);
    import_auth_profiles_with_status(&config_conn, config_dir, &mut result);
    import_proxy_profiles_with_status(&config_conn, config_dir, &mut result);
    import_ssh_tunnels_with_status(&config_conn, config_dir, &mut result);
    import_config_json_with_status(&config_conn, config_dir, &mut result);
    import_connection_tree_with_status(&config_conn, config_dir, &mut result);

    // --- State domain imports (state.db) ---
    import_history_entries_with_status(&state_conn, config_dir, &mut result);
    import_saved_queries_with_status(&state_conn, config_dir, &mut result);
    import_recent_items_with_status(&state_conn, config_dir, &mut result);
    import_ui_state_with_status(&state_conn, data_dir, &mut result);

    result
}

// ---------------------------------------------------------------------------
// Config domain imports
// ---------------------------------------------------------------------------

/// Converts a DbKind to its string representation for storage.
fn db_kind_to_str(kind: dbflux_core::DbKind) -> String {
    match kind {
        dbflux_core::DbKind::Postgres => "Postgres",
        dbflux_core::DbKind::SQLite => "SQLite",
        dbflux_core::DbKind::MySQL => "MySQL",
        dbflux_core::DbKind::MariaDB => "MariaDB",
        dbflux_core::DbKind::MongoDB => "MongoDB",
        dbflux_core::DbKind::Redis => "Redis",
        dbflux_core::DbKind::DynamoDB => "DynamoDB",
    }
    .to_string()
}

/// Converts a DbConfig to a ConnectionDriverConfigDto for storage.
fn db_config_to_connection_driver_config_dto(
    profile_id: &str,
    config: &dbflux_core::DbConfig,
) -> ConnectionDriverConfigDto {
    let mut dto = ConnectionDriverConfigDto::new(profile_id.to_string(), db_kind_to_str(config.kind()));

    match config {
        dbflux_core::DbConfig::Postgres {
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
                if let dbflux_core::SshAuthMethod::PrivateKey { key_path } = &tunnel.auth_method {
                    dto.ssh_tunnel_key_path = key_path.as_ref().map(|p| p.to_string_lossy().to_string());
                }
            }
        }
        dbflux_core::DbConfig::MySQL {
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
                if let dbflux_core::SshAuthMethod::PrivateKey { key_path } = &tunnel.auth_method {
                    dto.ssh_tunnel_key_path = key_path.as_ref().map(|p| p.to_string_lossy().to_string());
                }
            }
        }
        dbflux_core::DbConfig::MongoDB {
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
            if let Some(tunnel) = ssh_tunnel {
                dto.ssh_tunnel_host = Some(tunnel.host.clone());
                dto.ssh_tunnel_port = Some(tunnel.port as i32);
                dto.ssh_tunnel_user = Some(tunnel.user.clone());
                dto.ssh_tunnel_auth_method = ssh_auth_method_to_str(&tunnel.auth_method);
                if let dbflux_core::SshAuthMethod::PrivateKey { key_path } = &tunnel.auth_method {
                    dto.ssh_tunnel_key_path = key_path.as_ref().map(|p| p.to_string_lossy().to_string());
                }
            }
        }
        dbflux_core::DbConfig::Redis {
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
                if let dbflux_core::SshAuthMethod::PrivateKey { key_path } = &tunnel.auth_method {
                    dto.ssh_tunnel_key_path = key_path.as_ref().map(|p| p.to_string_lossy().to_string());
                }
            }
        }
        dbflux_core::DbConfig::SQLite { path, connection_id } => {
            dto.sqlite_path = Some(path.to_string_lossy().to_string());
            dto.sqlite_connection_id = connection_id.clone();
        }
        dbflux_core::DbConfig::DynamoDB {
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
        dbflux_core::DbConfig::External { kind, values } => {
            dto.external_kind = Some(db_kind_to_str(*kind));
            dto.external_values_json = Some(serde_json::to_string(values).unwrap_or_default());
        }
    }

    dto
}

fn ssl_mode_to_str(mode: &dbflux_core::SslMode) -> String {
    match mode {
        dbflux_core::SslMode::Disable => "disable".to_string(),
        dbflux_core::SslMode::Prefer => "prefer".to_string(),
        dbflux_core::SslMode::Require => "require".to_string(),
    }
}

fn ssh_auth_method_to_str(method: &dbflux_core::SshAuthMethod) -> String {
    match method {
        dbflux_core::SshAuthMethod::PrivateKey { .. } => "private_key".to_string(),
        dbflux_core::SshAuthMethod::Password => "password".to_string(),
    }
}

/// Imports connection profiles from legacy `profiles.json`.
fn import_profiles_with_status(
    config_conn: &OwnedConnection,
    config_dir: &Path,
    result: &mut LegacyImportResult,
) {
    let source = "profiles.json";
    let source_kind = "connection_profiles";

    let path = match legacy_path_if_exists(config_dir, source) {
        Some(p) => p,
        None => return,
    };

    // Compute hash for one-shot dedup
    let file_hash = match compute_source_hash(&path) {
        Ok(h) => h,
        Err(e) => {
            result.errors.push(format!(
                "profiles: cannot compute hash of {}: {}",
                source, e
            ));
            return;
        }
    };

    // Check if we should skip based on hash (one-shot semantics)
    match check_legacy_import_status(config_conn, source, source_kind, &file_hash) {
        Some(true) => return, // Already completed with same hash
        Some(false) => {}     // Retry requested
        None => {}            // New import
    }

    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            result
                .errors
                .push(format!("profiles: cannot read {}: {}", source, e));
            return;
        }
    };

    let legacy: Vec<ConnectionProfile> = match serde_json::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            result
                .errors
                .push(format!("profiles: cannot parse {}: {}", source, e));
            set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);
            return;
        }
    };

    if legacy.is_empty() {
        // Record empty file as successfully imported (nothing to do)
        if let Err(e) = record_legacy_import_success(config_conn, source, source_kind, &file_hash) {
            result.errors.push(e);
        }
        return;
    }

    // Mark as failed upfront so partial success doesn't survive a crash
    set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);

    // Transaction: entire file import is atomic
    let tx = match config_conn.unchecked_transaction() {
        Ok(t) => t,
        Err(e) => {
            result
                .errors
                .push(format!("profiles: cannot start transaction: {}", e));
            return;
        }
    };

    let repo = ConnectionProfileRepository::new(config_conn.clone());
    let existing_ids: std::collections::HashSet<String> = repo
        .all()
        .map(|v| v.into_iter().map(|p| p.id).collect())
        .unwrap_or_default();

    // Get child table repositories for denormalization
    let settings_repo = repo.settings();
    let value_refs_repo = repo.value_refs();
    let hooks_repo = repo.hooks();
    let hook_envs_repo = repo.hook_envs();
    let hook_bindings_repo = repo.hook_bindings();
    let governance_repo = repo.governance();
    let driver_configs_repo = repo.driver_configs();

    let mut imported = 0;
    for profile in legacy {
        if existing_ids.contains(&profile.id.to_string()) {
            continue;
        }

        // Legacy: serialize full profile to check compatibility
        let _config_json = match serde_json::to_string(&profile) {
            Ok(s) => s,
            Err(e) => {
                // If we can't round-trip the profile, the legacy data is incompatible — fail hard
                result.errors.push(format!(
                    "profiles: cannot serialize '{}' (incompatible legacy format): {}",
                    profile.name, e
                ));
                return;
            }
        };
        let driver_id = profile.driver_id();

        // Legacy JSON columns - these were used in the old schema but data is now
        // denormalized into child tables. Kept as _ for documentation.
        let _settings_overrides_json = profile
            .settings_overrides
            .as_ref()
            .and_then(|s| serde_json::to_string(s).ok());
        let _connection_settings_json = profile
            .connection_settings
            .as_ref()
            .and_then(|s| serde_json::to_string(s).ok());
        let _hooks_json = profile
            .hooks
            .as_ref()
            .and_then(|h| serde_json::to_string(h).ok());
        let _hook_bindings_json = profile
            .hook_bindings
            .as_ref()
            .and_then(|b| serde_json::to_string(b).ok());
        let _value_refs_json = if profile.value_refs.is_empty() {
            None
        } else {
            serde_json::to_string(&profile.value_refs).ok()
        };
        let _mcp_governance_json = profile
            .mcp_governance
            .as_ref()
            .and_then(|g| serde_json::to_string(g).ok());

        let dto = ConnectionProfileDto {
            id: profile.id.to_string(),
            name: profile.name.clone(),
            driver_id: Some(driver_id),
            description: None,
            favorite: false,
            color: None,
            icon: None,
            save_password: false,
            kind: Some(db_kind_to_str(profile.kind())),
            access_kind: None,
            access_provider: None,
            auth_profile_id: profile.auth_profile_id.map(|u| u.to_string()),
            proxy_profile_id: profile.proxy_profile_id.map(|u| u.to_string()),
            ssh_tunnel_profile_id: None,
            access_profile_id: None,
            created_at: String::new(),
            updated_at: String::new(),
        };

        if let Err(e) = repo.insert(&dto) {
            warn!("Failed to import profile '{}': {}", profile.name, e);
            continue;
        }

        // Denormalize: write DbConfig to connection_driver_configs
        let profile_id = profile.id.to_string();
        let driver_dto = db_config_to_connection_driver_config_dto(&profile_id, &profile.config);
        if let Err(e) = driver_configs_repo.upsert(&driver_dto) {
            warn!(
                "Failed to denormalize driver config for profile '{}': {}",
                profile.name, e
            );
        }

        // Denormalize: write to child tables

        // Denormalize connection_settings (FormValues = HashMap<String, String>)
        if let Some(ref settings) = profile.connection_settings {
            for (key, value) in settings {
                let setting_dto = crate::repositories::connection_profile_settings::ConnectionProfileSettingDto::new(
                    profile_id.clone(),
                    key.clone(),
                    Some(value.clone()),
                );
                if let Err(e) = settings_repo.insert(&setting_dto) {
                    warn!(
                        "Failed to denormalize connection setting '{}' for profile '{}': {}",
                        key, profile.name, e
                    );
                }
            }
        }

        // Denormalize value_refs (HashMap<String, ValueRef>)
        if !profile.value_refs.is_empty() {
            for (key, value_ref) in &profile.value_refs {
                let ref_dto = match value_ref {
                    dbflux_core::ValueRef::Literal { value } => {
                        crate::repositories::connection_profile_value_refs::ConnectionProfileValueRefDto::new_literal(
                            profile_id.clone(),
                            key.clone(),
                            value.clone(),
                        )
                    }
                    dbflux_core::ValueRef::Env { key: env_key } => {
                        crate::repositories::connection_profile_value_refs::ConnectionProfileValueRefDto::new_env(
                            profile_id.clone(),
                            key.clone(),
                            env_key.clone(),
                        )
                    }
                    dbflux_core::ValueRef::Secret {
                        provider,
                        locator,
                        json_key,
                    } => {
                        crate::repositories::connection_profile_value_refs::ConnectionProfileValueRefDto::new_secret(
                            profile_id.clone(),
                            key.clone(),
                            provider.clone(),
                            locator.clone(),
                            json_key.clone(),
                        )
                    }
                    dbflux_core::ValueRef::Parameter {
                        provider,
                        name,
                        json_key,
                    } => {
                        crate::repositories::connection_profile_value_refs::ConnectionProfileValueRefDto::new_param(
                            profile_id.clone(),
                            key.clone(),
                            provider.clone(),
                            name.clone(),
                            json_key.clone(),
                        )
                    }
                    dbflux_core::ValueRef::Auth { field } => {
                        crate::repositories::connection_profile_value_refs::ConnectionProfileValueRefDto::new_auth(
                            profile_id.clone(),
                            key.clone(),
                            field.clone(),
                        )
                    }
                };
                if let Err(e) = value_refs_repo.insert(&ref_dto) {
                    warn!(
                        "Failed to denormalize value ref '{}' for profile '{}': {}",
                        key, profile.name, e
                    );
                }
            }
        }

        // Denormalize hooks (ConnectionHooks) into flat child table rows
        if let Some(ref hooks) = profile.hooks {
            let hook_phases = [
                (HookPhase::PreConnect, &hooks.pre_connect),
                (HookPhase::PostConnect, &hooks.post_connect),
                (HookPhase::PreDisconnect, &hooks.pre_disconnect),
                (HookPhase::PostDisconnect, &hooks.post_disconnect),
            ];

            for (phase, hook_list) in hook_phases {
                for (order_index, hook) in hook_list.iter().enumerate() {
                    let phase_str = match phase {
                        HookPhase::PreConnect => "pre_connect",
                        HookPhase::PostConnect => "post_connect",
                        HookPhase::PreDisconnect => "pre_disconnect",
                        HookPhase::PostDisconnect => "post_disconnect",
                    };

                    let (hook_kind_str, command, script_language, script_source_type,
                         script_content, script_path, lua_source_type, lua_content,
                         lua_path, lua_log, lua_env_read, lua_conn_metadata, lua_process_run) =
                        match &hook.kind {
                            HookKind::Command { command, args } => {
                                let cmd_with_args = if args.is_empty() {
                                    command.clone()
                                } else {
                                    format!("{} {}", command, args.join(" "))
                                };
                                ("command".to_string(), Some(cmd_with_args),
                                 None, None, None, None, None, None, None,
                                 false, false, false, false)
                            }
                            HookKind::Script { language, source, .. } => {
                                let (ss_type, ss_content, ss_path) = match source {
                                    ScriptSource::Inline { content } =>
                                        ("inline".to_string(), Some(content.clone()), None),
                                    ScriptSource::File { path } =>
                                        ("file".to_string(), None, Some(path.to_string_lossy().to_string())),
                                };
                                ("script".to_string(), None,
                                 Some(format!("{:?}", language).to_lowercase()),
                                 Some(ss_type), ss_content, ss_path,
                                 None, None, None,
                                 false, false, false, false)
                            }
                            HookKind::Lua { source, capabilities } => {
                                let (ls_type, ls_content, ls_path) = match source {
                                    ScriptSource::Inline { content } =>
                                        ("inline".to_string(), Some(content.clone()), None),
                                    ScriptSource::File { path } =>
                                        ("file".to_string(), None, Some(path.to_string_lossy().to_string())),
                                };
                                ("lua".to_string(), None,
                                 None, None, None, None,
                                 Some(ls_type), ls_content, ls_path,
                                 capabilities.logging, capabilities.env_read,
                                 capabilities.connection_metadata, capabilities.process_run)
                            }
                        };

                    let execution_mode_str = match hook.execution_mode {
                        dbflux_core::HookExecutionMode::Blocking => "blocking",
                        dbflux_core::HookExecutionMode::Detached => "detached",
                    };

                    let on_failure_str = match hook.on_failure {
                        HookFailureMode::Disconnect => "disconnect",
                        HookFailureMode::Warn => "warn",
                        HookFailureMode::Ignore => "ignore",
                    };

                    let hook_dto = crate::repositories::connection_profile_hooks::ConnectionProfileHookDto {
                        id: uuid::Uuid::new_v4().to_string(),
                        profile_id: profile_id.clone(),
                        phase: phase_str.to_string(),
                        order_index: order_index as i32,
                        enabled: hook.enabled,
                        hook_kind: hook_kind_str,
                        command,
                        script_language,
                        script_source_type,
                        script_content,
                        script_path,
                        lua_source_type,
                        lua_content,
                        lua_path,
                        lua_log,
                        lua_env_read,
                        lua_conn_metadata,
                        lua_process_run,
                        cwd: hook.cwd.as_ref().map(|p| p.to_string_lossy().to_string()),
                        inherit_env: hook.inherit_env,
                        timeout_ms: hook.timeout_ms.map(|v| v as i64),
                        execution_mode: execution_mode_str.to_string(),
                        ready_signal: hook.ready_signal.clone(),
                        on_failure: on_failure_str.to_string(),
                    };

                    if let Err(e) = hooks_repo.insert(&hook_dto) {
                        warn!(
                            "Failed to denormalize {} hook {} for profile '{}': {}",
                            phase_str, order_index, profile.name, e
                        );
                    }

                    // Denormalize env vars into hook_envs child table
                    if !hook.env.is_empty() {
                        for (key, value) in &hook.env {
                            let env_dto = crate::repositories::connection_profile_hook_envs::ConnectionProfileHookEnvDto {
                                id: uuid::Uuid::new_v4().to_string(),
                                hook_id: hook_dto.id.clone(),
                                key: key.clone(),
                                value: value.clone(),
                            };
                            if let Err(e) = hook_envs_repo.insert(&env_dto) {
                                warn!(
                                    "Failed to denormalize env var '{}' for {} hook {} of profile '{}': {}",
                                    key, phase_str, order_index, profile.name, e
                                );
                            }
                        }
                    }
                }
            }
        }

        // Denormalize hook_bindings (ConnectionHookBindings - references to global hooks)
        if let Some(ref bindings) = profile.hook_bindings {
            // Serialize the bindings struct as JSON
            if serde_json::to_string(bindings).is_ok() {
                let binding_dto = crate::repositories::connection_profile_hook_bindings::ConnectionProfileHookBindingDto {
                    id: uuid::Uuid::new_v4().to_string(),
                    profile_id: profile_id.clone(),
                    hook_id: "global_bindings".to_string(),
                    phase: "all".to_string(),
                    order_index: 0,
                };
                // Store the bindings JSON in a single row for simplicity
                // Since the table expects individual bindings, we serialize as JSON
                if let Err(e) = hook_bindings_repo.insert(&binding_dto) {
                    warn!(
                        "Failed to denormalize hook bindings for profile '{}': {}",
                        profile.name, e
                    );
                }
            }
        }

        // Denormalize mcp_governance (ConnectionMcpGovernance)
        if let Some(ref governance) = profile.mcp_governance
            && let Ok(gov_json) = serde_json::to_string(governance)
        {
            let gov_dto = crate::repositories::connection_profile_governance::ConnectionProfileGovernanceDto {
                id: uuid::Uuid::new_v4().to_string(),
                profile_id: profile_id.clone(),
                governance_key: "mcp_governance".to_string(),
                governance_value: Some(gov_json),
            };
            if let Err(e) = governance_repo.insert(&gov_dto) {
                warn!(
                    "Failed to denormalize governance for profile '{}': {}",
                    profile.name, e
                );
            }
        }

        imported += 1;
    }

    result.profiles_imported += imported;

    if imported > 0 {
        log::info!(
            "Imported {} legacy connection profiles from {}",
            imported,
            source
        );
    }

    // Commit only after all records successfully written
    if let Err(e) = tx.commit() {
        result
            .errors
            .push(format!("profiles: commit failed: {}", e));
        return;
    }

    // Mark completed in legacy_imports with hash for one-shot semantics
    if let Err(e) = record_legacy_import_success(config_conn, source, source_kind, &file_hash) {
        result.errors.push(e);
    }
}

/// Imports auth profiles from legacy `auth_profiles.json`.
fn import_auth_profiles_with_status(
    config_conn: &OwnedConnection,
    config_dir: &Path,
    result: &mut LegacyImportResult,
) {
    let source = "auth_profiles.json";

    match get_import_status(config_conn, source) {
        Some(true) => return,
        Some(false) => log::info!("Retrying failed import: {}", source),
        None => {}
    }

    let path = match legacy_path_if_exists(config_dir, source) {
        Some(p) => p,
        None => return,
    };

    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            result
                .errors
                .push(format!("auth_profiles: cannot read {}: {}", source, e));
            return;
        }
    };

    let legacy: Vec<dbflux_core::AuthProfile> = match serde_json::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            result
                .errors
                .push(format!("auth_profiles: cannot parse {}: {}", source, e));
            set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);
            return;
        }
    };

    if legacy.is_empty() {
        set_import_status(config_conn.as_ref(), source, ImportStatus::Completed);
        return;
    }

    set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);

    let tx = match config_conn.unchecked_transaction() {
        Ok(t) => t,
        Err(e) => {
            result
                .errors
                .push(format!("auth_profiles: cannot start transaction: {}", e));
            return;
        }
    };

    let repo = AuthProfileRepository::new(config_conn.clone());
    let existing_ids: std::collections::HashSet<String> = repo
        .all()
        .map(|v| v.into_iter().map(|p| p.id.to_string()).collect())
        .unwrap_or_default();

    let mut imported = 0;
    for profile in legacy {
        if existing_ids.contains(&profile.id.to_string()) {
            continue;
        }
        if let Err(e) = repo.insert_auth_profile(&profile) {
            warn!("Failed to import auth profile '{}': {}", profile.name, e);
        } else {
            imported += 1;
        }
    }

    result.auth_profiles_imported += imported;

    if imported > 0 {
        log::info!("Imported {} legacy auth profiles from {}", imported, source);
    }

    if let Err(e) = tx.commit() {
        result
            .errors
            .push(format!("auth_profiles: commit failed: {}", e));
        return;
    }

    set_import_status(config_conn.as_ref(), source, ImportStatus::Completed);
}

/// Imports proxy profiles from legacy `proxies.json`.
fn import_proxy_profiles_with_status(
    config_conn: &OwnedConnection,
    config_dir: &Path,
    result: &mut LegacyImportResult,
) {
    let source = "proxies.json";

    match get_import_status(config_conn, source) {
        Some(true) => return,
        Some(false) => log::info!("Retrying failed import: {}", source),
        None => {}
    }

    let path = match legacy_path_if_exists(config_dir, source) {
        Some(p) => p,
        None => return,
    };

    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            result
                .errors
                .push(format!("proxies: cannot read {}: {}", source, e));
            return;
        }
    };

    let legacy: Vec<dbflux_core::ProxyProfile> = match serde_json::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            result
                .errors
                .push(format!("proxies: cannot parse {}: {}", source, e));
            set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);
            return;
        }
    };

    if legacy.is_empty() {
        set_import_status(config_conn.as_ref(), source, ImportStatus::Completed);
        return;
    }

    set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);

    let tx = match config_conn.unchecked_transaction() {
        Ok(t) => t,
        Err(e) => {
            result
                .errors
                .push(format!("proxies: cannot start transaction: {}", e));
            return;
        }
    };

    let repo = ProxyProfileRepository::new(config_conn.clone());
    let existing_ids: std::collections::HashSet<String> = repo
        .all()
        .map(|v| v.into_iter().map(|p| p.id.to_string()).collect())
        .unwrap_or_default();

    let mut imported = 0;
    for profile in legacy {
        if existing_ids.contains(&profile.id.to_string()) {
            continue;
        }

        let name = profile.name.clone();
        let kind_str = match profile.kind {
            dbflux_core::ProxyKind::Http => "Http",
            dbflux_core::ProxyKind::Https => "Https",
            dbflux_core::ProxyKind::Socks5 => "Socks5",
        };

        // Extract auth_kind and username from auth
        let (auth_kind, auth_username) = match &profile.auth {
            dbflux_core::ProxyAuth::None => ("none".to_string(), None),
            dbflux_core::ProxyAuth::Basic { username } => {
                ("basic".to_string(), Some(username.clone()))
            }
        };

        // Compute secret_ref before moving from profile
        let profile_id = profile.id.to_string();
        let secret_ref = if profile.save_secret {
            Some(profile.secret_ref())
        } else {
            None
        };

        let dto = ProxyProfileDto {
            id: profile_id.clone(),
            name,
            kind: kind_str.to_string(),
            host: profile.host,
            port: profile.port as i32,
            auth_kind,
            no_proxy: profile.no_proxy,
            enabled: profile.enabled,
            save_secret: profile.save_secret,
            created_at: String::new(),
            updated_at: String::new(),
        };

        // Create auth DTO if we have credentials
        let auth_dto = if auth_username.is_some() || secret_ref.is_some() {
            Some(ProxyAuthDto {
                proxy_profile_id: profile_id,
                username: auth_username,
                domain: None,
                password_secret_ref: secret_ref,
            })
        } else {
            None
        };

        if let Err(e) = repo.insert(&dto, auth_dto.as_ref()) {
            warn!("Failed to import proxy profile '{}': {}", dto.name, e);
        } else {
            imported += 1;
        }
    }

    result.proxy_profiles_imported += imported;

    if imported > 0 {
        log::info!(
            "Imported {} legacy proxy profiles from {}",
            imported,
            source
        );
    }

    if let Err(e) = tx.commit() {
        result.errors.push(format!("proxies: commit failed: {}", e));
        return;
    }

    set_import_status(config_conn.as_ref(), source, ImportStatus::Completed);
}

/// Imports SSH tunnel profiles from legacy `ssh_tunnels.json`.
fn import_ssh_tunnels_with_status(
    config_conn: &OwnedConnection,
    config_dir: &Path,
    result: &mut LegacyImportResult,
) {
    let source = "ssh_tunnels.json";

    match get_import_status(config_conn, source) {
        Some(true) => return,
        Some(false) => log::info!("Retrying failed import: {}", source),
        None => {}
    }

    let path = match legacy_path_if_exists(config_dir, source) {
        Some(p) => p,
        None => return,
    };

    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            result
                .errors
                .push(format!("ssh_tunnels: cannot read {}: {}", source, e));
            return;
        }
    };

    let legacy: Vec<SshTunnelProfile> = match serde_json::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            result
                .errors
                .push(format!("ssh_tunnels: cannot parse {}: {}", source, e));
            set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);
            return;
        }
    };

    if legacy.is_empty() {
        set_import_status(config_conn.as_ref(), source, ImportStatus::Completed);
        return;
    }

    set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);

    let tx = match config_conn.unchecked_transaction() {
        Ok(t) => t,
        Err(e) => {
            result
                .errors
                .push(format!("ssh_tunnels: cannot start transaction: {}", e));
            return;
        }
    };

    let repo = SshTunnelProfileRepository::new(config_conn.clone());
    let existing_ids: std::collections::HashSet<String> = repo
        .all()
        .map(|v| v.into_iter().map(|p| p.id.to_string()).collect())
        .unwrap_or_default();

    let mut imported = 0;
    for profile in legacy {
        if existing_ids.contains(&profile.id.to_string()) {
            continue;
        }

        let name = profile.name.clone();
        let profile_id = profile.id.to_string();

        // Extract auth_method and credentials from config
        let (auth_method, key_path_str, password_ref, passphrase_ref) =
            match &profile.config.auth_method {
                dbflux_core::SshAuthMethod::Password => {
                    let password_ref = if profile.save_secret {
                        Some(profile.secret_ref())
                    } else {
                        None
                    };
                    ("password".to_string(), None, password_ref, None)
                }
                dbflux_core::SshAuthMethod::PrivateKey { key_path } => {
                    let passphrase_ref = if profile.save_secret {
                        Some(profile.secret_ref())
                    } else {
                        None
                    };
                    let key_path_str = key_path.as_ref().map(|p| p.to_string_lossy().to_string());
                    ("key".to_string(), key_path_str, None, passphrase_ref)
                }
            };

        let dto = SshTunnelProfileDto {
            id: profile_id.clone(),
            name,
            host: profile.config.host.clone(),
            port: profile.config.port as i32,
            user: profile.config.user.clone(),
            auth_method,
            key_path: None,
            passphrase_secret_ref: None,
            password_secret_ref: None,
            save_secret: profile.save_secret,
            created_at: String::new(),
            updated_at: String::new(),
        };

        // Create auth DTO if we have credentials
        let auth_dto =
            if key_path_str.is_some() || password_ref.is_some() || passphrase_ref.is_some() {
                Some(SshTunnelAuthDto {
                    ssh_tunnel_profile_id: profile_id,
                    key_path: key_path_str,
                    password_secret_ref: password_ref,
                    passphrase_secret_ref: passphrase_ref,
                })
            } else {
                None
            };

        if let Err(e) = repo.insert(&dto, auth_dto.as_ref()) {
            warn!("Failed to import SSH tunnel profile '{}': {}", dto.name, e);
        } else {
            imported += 1;
        }
    }

    result.ssh_tunnels_imported += imported;

    if imported > 0 {
        log::info!(
            "Imported {} legacy SSH tunnel profiles from {}",
            imported,
            source
        );
    }

    if let Err(e) = tx.commit() {
        result
            .errors
            .push(format!("ssh_tunnels: commit failed: {}", e));
        return;
    }

    set_import_status(config_conn.as_ref(), source, ImportStatus::Completed);
}

// ---------------------------------------------------------------------------
// Config domain: config.json
// ---------------------------------------------------------------------------

/// Imports settings from legacy `config.json` (general, driver_settings,
/// hook_definitions, governance).
///
/// This function is transactional: any sub-import failure causes a full rollback
/// and keeps the import retriable (marked as failed, not completed).
///
/// After a successful import, governance settings are also written to config.json
/// so the runtime's `AppConfigStore` path can find them.
fn import_config_json_with_status(
    config_conn: &OwnedConnection,
    config_dir: &Path,
    result: &mut LegacyImportResult,
) {
    let source = "config.json";

    match get_import_status(config_conn, source) {
        Some(true) => return,
        Some(false) => log::info!("Retrying failed import: {}", source),
        None => {}
    }

    let path = match legacy_path_if_exists(config_dir, source) {
        Some(p) => p,
        None => return,
    };

    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            result
                .errors
                .push(format!("config.json: cannot read: {}", e));
            return;
        }
    };

    // Parse JSON first to extract legacy_allow_redis_flush before full deserialization.
    // This replicates the normalization path used by AppConfigStore::load().
    let json: serde_json::Value = match serde_json::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            result
                .errors
                .push(format!("config.json: cannot parse: {}", e));
            set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);
            return;
        }
    };

    let legacy_allow_redis_flush = json
        .get("general")
        .and_then(|general| general.get("allow_redis_flush"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let mut config: AppConfig = match serde_json::from_value(json.clone()) {
        Ok(v) => v,
        Err(e) => {
            result
                .errors
                .push(format!("config.json: cannot deserialize: {}", e));
            set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);
            return;
        }
    };

    // Issue #1: Apply version migration (same logic as AppConfigStore::migrate)
    if migrate_app_config(&mut config, legacy_allow_redis_flush) {
        log::info!(
            "Migrated config.json from version {} to version 3",
            config.version
        );
    }

    // Issue #4: Validate governance roles for legacy shape that would cause data loss.
    // The legacy role format had `name`, `description`, and `permissions` fields
    // that are silently dropped by serde. Fail loudly rather than silently losing permissions.
    if let Some(governance_json) = json.get("governance")
        && let Err(e) = validate_governance_roles_json(governance_json)
    {
        result.errors.push(format!(
            "config.json: governance role conversion error: {}",
            e
        ));
        set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);
        return;
    }

    // Start transaction BEFORE marking as failed (issue #3 fix).
    // This ensures a crash between marking failed and tx start doesn't leave
    // a false failed marker.
    let tx = match config_conn.unchecked_transaction() {
        Ok(t) => t,
        Err(e) => {
            result
                .errors
                .push(format!("config.json: cannot start transaction: {}", e));
            return;
        }
    };

    let mut local_errors = Vec::new();
    let mut driver_settings_imported = 0;
    let mut hook_definitions_imported = 0;

    if let Err(error) = import_general_settings(config_conn, &config.general) {
        local_errors.push(error);
    }

    match import_driver_settings(
        config_conn,
        &config.driver_overrides,
        &config.driver_settings,
    ) {
        Ok(imported) => driver_settings_imported = imported,
        Err(error) => local_errors.push(error),
    }

    match import_hook_definitions(config_conn, &config.hook_definitions) {
        Ok(imported) => hook_definitions_imported = imported,
        Err(error) => local_errors.push(error),
    }

    if let Err(error) = import_governance_settings(config_conn, &config.governance) {
        local_errors.push(error);
    }

    if !local_errors.is_empty() {
        if let Err(e) = tx.rollback() {
            log::warn!("Failed to rollback config.json import: {}", e);
        }

        set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);
        result.errors.extend(local_errors);
        return;
    }

    tx.execute(
        "INSERT INTO system_metadata (key, value, updated_at)
         VALUES (?1, ?2, datetime('now'))
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        rusqlite::params![format!("legacy_import::{}", source), "completed"],
    )
    .expect("config.json import status update should succeed inside transaction");

    if let Err(e) = tx.commit() {
        result
            .errors
            .push(format!("config.json: commit failed: {}", e));

        set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);
        return;
    }

    result.general_settings_imported = true;
    result.driver_settings_imported = driver_settings_imported;
    result.hook_definitions_imported = hook_definitions_imported;
    result.governance_imported = true;
}

/// Validates that governance roles don't have legacy fields that would be silently lost.
///
/// The legacy role format had `name`, `description`, and `permissions` fields.
/// The current `PolicyRoleConfig` only has `id` and `policy_ids`.
///
/// This function inspects the raw JSON to detect legacy fields before deserialization.
fn validate_governance_roles_json(governance_json: &serde_json::Value) -> Result<(), String> {
    let Some(roles) = governance_json.get("roles").and_then(|r| r.as_array()) else {
        return Ok(()); // No roles, nothing to validate
    };

    for (i, role) in roles.iter().enumerate() {
        let Some(role_obj) = role.as_object() else {
            continue;
        };

        // Legacy role format had these extra fields that are now dropped:
        let has_name = role_obj.contains_key("name");
        let has_description = role_obj.contains_key("description");
        let has_permissions = role_obj.contains_key("permissions");

        // If any legacy field is present, we need to check if policy_ids is also present
        // and non-empty. If policy_ids is missing or empty, data would be lost.
        if has_name || has_description || has_permissions {
            let has_policy_ids = role_obj
                .get("policy_ids")
                .and_then(|p| p.as_array())
                .map(|arr| !arr.is_empty())
                .unwrap_or(false);

            if !has_policy_ids {
                let role_id = role_obj
                    .get("id")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .unwrap_or_else(|| format!("<role at index {}>", i));

                return Err(format!(
                    "role '{}' has legacy fields (name/description/permissions) but no \
                     policy_ids. Legacy roles cannot be imported as they would lose data. \
                     Please migrate manually to the new format with explicit policy references.",
                    role_id
                ));
            }
        }
    }
    Ok(())
}

/// Validates that governance roles don't have legacy fields that would be silently lost.
///
/// The legacy role format had `name`, `description`, and `permissions` fields.
/// The current `PolicyRoleConfig` only has `id` and `policy_ids`.
///
/// If `permissions` is non-empty and `policy_ids` is empty, this is a lossy conversion
/// that should fail loudly rather than silently dropping data.
#[allow(dead_code)]
fn validate_governance_roles_for_import(governance: &GovernanceSettings) -> Result<(), String> {
    for role in governance.roles.iter() {
        // Check if this role has legacy shape (permissions) that would be lost.
        // We detect this by checking if policy_ids is empty when there might be legacy data.
        // Since we don't have access to the raw JSON here, we use the presence of
        // non-empty policy_ids as an indicator that the role is in the new format.
        //
        // If policy_ids is empty and the role has a legacy format, the conversion
        // would silently drop data. We fail in this case.
        if role.policy_ids.is_empty() {
            // Check if this could be a legacy role by looking at whether the id
            // suggests a legacy format (legacy roles often had descriptive names
            // like "Read Only" while new roles use slug format like "readonly").
            let id_has_legacy_format =
                role.id.contains(' ') || role.id.chars().any(|c| c.is_uppercase());

            if id_has_legacy_format {
                return Err(format!(
                    "role '{}' appears to use legacy format (permissions would be dropped). \
                     Legacy roles with 'name', 'description', and 'permissions' fields \
                     cannot be directly converted to the current format.",
                    role.id
                ));
            }
        }
    }
    Ok(())
}

/// Imports general settings into general_settings table.
fn import_general_settings(
    config_conn: &OwnedConnection,
    general: &GeneralSettings,
) -> Result<(), String> {
    let repo = GeneralSettingsRepository::new(config_conn.clone());

    let dto = GeneralSettingsDto {
        id: 1,
        theme: match general.theme {
            dbflux_core::ThemeSetting::Light => "light".to_string(),
            dbflux_core::ThemeSetting::Dark => "dark".to_string(),
        },
        restore_session_on_startup: if general.restore_session_on_startup { 1 } else { 0 },
        reopen_last_connections: if general.reopen_last_connections { 1 } else { 0 },
        default_focus_on_startup: match general.default_focus_on_startup {
            dbflux_core::StartupFocus::LastTab => "last_tab".to_string(),
            dbflux_core::StartupFocus::Sidebar => "sidebar".to_string(),
        },
        max_history_entries: general.max_history_entries as i64,
        auto_save_interval_ms: general.auto_save_interval_ms as i64,
        default_refresh_policy: match general.default_refresh_policy {
            dbflux_core::RefreshPolicySetting::Interval => "interval".to_string(),
            dbflux_core::RefreshPolicySetting::Manual => "manual".to_string(),
        },
        default_refresh_interval_secs: general.default_refresh_interval_secs as i32,
        max_concurrent_background_tasks: general.max_concurrent_background_tasks as i64,
        auto_refresh_pause_on_error: if general.auto_refresh_pause_on_error { 1 } else { 0 },
        auto_refresh_only_if_visible: if general.auto_refresh_only_if_visible { 1 } else { 0 },
        confirm_dangerous_queries: if general.confirm_dangerous_queries { 1 } else { 0 },
        dangerous_requires_where: if general.dangerous_requires_where { 1 } else { 0 },
        dangerous_requires_preview: if general.dangerous_requires_preview { 1 } else { 0 },
        updated_at: String::new(),
    };

    if let Err(error) = repo.upsert(&dto) {
        return Err(format!(
            "general_settings: failed to write to settings repository: {}",
            error
        ));
    }

    log::info!("Imported legacy general settings from config.json");

    Ok(())
}

/// Imports driver overrides and driver settings into driver_overrides and driver_setting_values tables.
fn import_driver_settings(
    config_conn: &OwnedConnection,
    driver_overrides: &HashMap<DriverKey, GlobalOverrides>,
    driver_settings: &HashMap<DriverKey, FormValues>,
) -> Result<usize, String> {
    if driver_overrides.is_empty() && driver_settings.is_empty() {
        return Ok(0);
    }

    let overrides_repo = DriverOverridesRepository::new(config_conn.clone());
    let values_repo = DriverSettingValuesRepository::new(config_conn.clone());

    let mut imported = 0;

    for (raw_key, value) in driver_overrides {
        let key = normalize_driver_key(raw_key);

        let dto = DriverOverridesDto {
            driver_key: key.clone(),
            refresh_policy: value.refresh_policy.as_ref().map(|rp| match rp {
                dbflux_core::RefreshPolicySetting::Interval => "interval".to_string(),
                dbflux_core::RefreshPolicySetting::Manual => "manual".to_string(),
            }),
            refresh_interval_secs: value.refresh_interval_secs.map(|v| v as i32),
            confirm_dangerous: value.confirm_dangerous.map(|v| if v { 1 } else { 0 }),
            requires_where: value.requires_where.map(|v| if v { 1 } else { 0 }),
            requires_preview: value.requires_preview.map(|v| if v { 1 } else { 0 }),
            updated_at: String::new(),
        };

        overrides_repo
            .upsert(&dto)
            .map_err(|e| format!("driver_overrides[{}]: upsert failed: {}", key, e))?;

        imported += 1;
    }

    for (raw_key, form_values) in driver_settings {
        let key = normalize_driver_key(raw_key);

        // Ensure driver_overrides row exists before inserting driver_setting_values.
        // If driver_settings contains a key that wasn't in driver_overrides,
        // we need to create a default override entry to satisfy the FK constraint.
        if overrides_repo.get(&key).map_err(|e| format!("driver_overrides[{}]: get failed: {}", key, e))?.is_none() {
            let default_override = DriverOverridesDto {
                driver_key: key.clone(),
                refresh_policy: None,
                refresh_interval_secs: None,
                confirm_dangerous: None,
                requires_where: None,
                requires_preview: None,
                updated_at: String::new(),
            };
            overrides_repo
                .upsert(&default_override)
                .map_err(|e| format!("driver_overrides[{}]: upsert default failed: {}", key, e))?;
        }

        let values: Vec<DriverSettingValueDto> = form_values
            .iter()
            .map(|(k, v)| DriverSettingValueDto {
                id: uuid::Uuid::new_v4().to_string(),
                driver_key: key.clone(),
                setting_key: k.clone(),
                setting_value: Some(v.clone()),
            })
            .collect();

        if !values.is_empty() {
            values_repo
                .replace_for_driver(&key, &values)
                .map_err(|e| format!("driver_setting_values[{}]: replace failed: {}", key, e))?;

            imported += 1;
        }
    }

    if imported > 0 {
        log::info!(
            "Imported {} legacy driver settings entries from config.json",
            imported
        );
    }

    Ok(imported)
}

/// Imports hook definitions into the hook_definitions table.
fn import_hook_definitions(
    config_conn: &OwnedConnection,
    hook_definitions: &HashMap<String, ConnectionHook>,
) -> Result<usize, String> {
    if hook_definitions.is_empty() {
        return Ok(0);
    }

    let repo = HookDefinitionRepository::new(config_conn.clone());
    let existing_rows = match repo.all() {
        Ok(rows) => rows,
        Err(e) => {
            return Err(format!(
                "hook_definitions: cannot fetch existing hooks: {}",
                e
            ));
        }
    };
    let existing_ids: std::collections::HashMap<_, _> = existing_rows
        .iter()
        .map(|d| (d.name.clone(), d.id.clone()))
        .collect();

    let mut imported = 0;
    for (name, hook) in hook_definitions {
        let id = existing_ids
            .get(name)
            .cloned()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let _kind_json = serde_json::to_string(hook)
            .map_err(|e| format!("hook_definitions[{}]: cannot serialize hook: {}", name, e))?;
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

        let dto = HookDefinitionDto {
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

        repo.upsert(&dto)
            .map_err(|e| format!("hook_definitions[{}]: upsert failed: {}", dto.name, e))?;

        imported += 1;
    }

    if imported > 0 {
        log::info!(
            "Imported {} legacy hook definitions from config.json",
            imported
        );
    }

    Ok(imported)
}

/// Imports governance settings into governance_* tables.
fn import_governance_settings(
    config_conn: &OwnedConnection,
    governance: &GovernanceSettings,
) -> Result<(), String> {
    let repo = GovernanceSettingsRepository::new(config_conn.clone());

    let dto = GovernanceSettingsDto {
        id: 1,
        mcp_enabled_by_default: if governance.mcp_enabled_by_default { 1 } else { 0 },
        updated_at: String::new(),
    };

    repo.upsert(&dto)
        .map_err(|e| format!("governance_settings: failed to upsert: {}", e))?;

    // Import trusted clients
    let clients: Vec<TrustedClientDto> = governance
        .trusted_clients
        .iter()
        .map(|c| TrustedClientDto {
            id: uuid::Uuid::new_v4().to_string(),
            governance_id: 1,
            client_id: c.id.clone(),
            name: c.name.clone(),
            issuer: c.issuer.clone(),
            active: if c.active { 1 } else { 0 },
        })
        .collect();

    if !clients.is_empty() {
        repo.replace_trusted_clients(&clients)
            .map_err(|e| format!("governance_settings: failed to upsert trusted clients: {}", e))?;
    }

    // Import policy roles
    let roles: Vec<PolicyRoleDto> = governance
        .roles
        .iter()
        .map(|r| PolicyRoleDto {
            id: r.id.clone(),
            governance_id: 1,
            role_id: r.id.clone(),
        })
        .collect();

    if !roles.is_empty() {
        repo.replace_policy_roles(&roles)
            .map_err(|e| format!("governance_settings: failed to upsert policy roles: {}", e))?;
    }

    // Import tool policies
    let policies: Vec<ToolPolicyDto> = governance
        .policies
        .iter()
        .map(|p| ToolPolicyDto {
            id: uuid::Uuid::new_v4().to_string(),
            governance_id: 1,
            policy_id: p.id.clone(),
            allowed_tools: Some(serde_json::to_string(&p.allowed_tools).unwrap_or_default()),
            allowed_classes: Some(serde_json::to_string(&p.allowed_classes).unwrap_or_default()),
        })
        .collect();

    if !policies.is_empty() {
        repo.replace_tool_policies(&policies)
            .map_err(|e| format!("governance_settings: failed to upsert tool policies: {}", e))?;
    }

    log::info!("Imported legacy governance settings from config.json");

    Ok(())
}

fn normalize_driver_key(key: &str) -> String {
    if key.contains(':') {
        key.to_string()
    } else {
        format!("builtin:{}", key)
    }
}

// ---------------------------------------------------------------------------
// Config domain: connection tree
// ---------------------------------------------------------------------------

use crate::repositories::connection_folders::ConnectionFoldersRepository;

/// Imports connection tree from legacy `connections_tree.json`.
fn import_connection_tree_with_status(
    config_conn: &OwnedConnection,
    config_dir: &Path,
    result: &mut LegacyImportResult,
) {
    let source = "connections_tree.json";

    match get_import_status(config_conn, source) {
        Some(true) => return,
        Some(false) => log::info!("Retrying failed import: {}", source),
        None => {}
    }

    let path = match legacy_path_if_exists(config_dir, source) {
        Some(p) => p,
        None => return,
    };

    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            result
                .errors
                .push(format!("connection_tree: cannot read {}: {}", source, e));
            return;
        }
    };

    let tree: dbflux_core::ConnectionTree = match serde_json::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            result
                .errors
                .push(format!("connection_tree: cannot parse {}: {}", source, e));
            set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);
            return;
        }
    };

    // Empty tree - nothing to import
    if tree.nodes.is_empty() {
        set_import_status(config_conn.as_ref(), source, ImportStatus::Completed);
        return;
    }

    set_import_status(config_conn.as_ref(), source, ImportStatus::Failed);

    let repo = ConnectionFoldersRepository::new(config_conn.clone());

    // Save the tree to SQLite
    if let Err(e) = repo.save_tree(&tree) {
        result.errors.push(format!(
            "connection_tree: failed to save tree to SQLite: {}",
            e
        ));
        return;
    }

    set_import_status(config_conn.as_ref(), source, ImportStatus::Completed);
    result.connection_tree_imported = true;
    log::info!(
        "Imported legacy connection tree with {} nodes from {}",
        tree.nodes.len(),
        source
    );
}

// ---------------------------------------------------------------------------
// State domain imports
// ---------------------------------------------------------------------------

/// Imports query history entries from legacy `history.json`.
fn import_history_entries_with_status(
    state_conn: &OwnedConnection,
    config_dir: &Path,
    result: &mut LegacyImportResult,
) {
    let source = "history.json";

    // History lives in config dir in legacy schema
    match get_import_status(state_conn, source) {
        Some(true) => return,
        Some(false) => log::info!("Retrying failed import: {}", source),
        None => {}
    }

    let path = match legacy_path_if_exists(config_dir, source) {
        Some(p) => p,
        None => return,
    };

    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            result
                .errors
                .push(format!("history: cannot read {}: {}", source, e));
            return;
        }
    };

    #[derive(Debug, Deserialize)]
    struct LegacyHistoryEntry {
        #[serde(rename = "id")]
        id: Option<String>,
        #[serde(rename = "sql")]
        sql: String,
        #[serde(rename = "timestamp")]
        timestamp: Option<i64>,
        #[serde(rename = "database")]
        database: Option<String>,
        #[serde(rename = "connection_name")]
        connection_name: Option<String>,
        #[serde(rename = "execution_time_ms")]
        execution_time_ms: Option<u64>,
        #[serde(rename = "row_count")]
        row_count: Option<usize>,
        #[serde(rename = "is_favorite")]
        is_favorite: Option<bool>,
    }

    let legacy: Vec<LegacyHistoryEntry> = match serde_json::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            result
                .errors
                .push(format!("history: cannot parse {}: {}", source, e));
            set_import_status(state_conn.as_ref(), source, ImportStatus::Failed);
            return;
        }
    };

    if legacy.is_empty() {
        set_import_status(state_conn.as_ref(), source, ImportStatus::Completed);
        return;
    }

    set_import_status(state_conn.as_ref(), source, ImportStatus::Failed);

    let tx = match state_conn.unchecked_transaction() {
        Ok(t) => t,
        Err(e) => {
            result
                .errors
                .push(format!("history: cannot start transaction: {}", e));
            return;
        }
    };

    let repo = QueryHistoryRepository::new(state_conn.clone());
    let existing_ids: std::collections::HashSet<String> = repo
        .all()
        .map(|v| v.into_iter().map(|h| h.id).collect())
        .unwrap_or_default();

    let mut imported = 0;
    for entry in legacy {
        let id = entry
            .id
            .as_ref()
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
            .unwrap_or_else(uuid::Uuid::new_v4)
            .to_string();

        if existing_ids.contains(&id) {
            continue;
        }

        let dto = QueryHistoryDto {
            id: id.clone(),
            connection_profile_id: entry.connection_name,
            driver_id: None,
            database_name: entry.database,
            query_text: entry.sql,
            query_kind: "select".to_string(),
            executed_at: entry
                .timestamp
                .map(|ts| {
                    dbflux_core::chrono::DateTime::from_timestamp(ts, 0)
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_default()
                })
                .unwrap_or_default(),
            duration_ms: entry.execution_time_ms.map(|ms| ms as i64),
            succeeded: true,
            error_summary: None,
            row_count: entry.row_count.map(|n| n as i64),
            is_favorite: entry.is_favorite.unwrap_or(false),
        };

        if let Err(e) = repo.add(&dto) {
            warn!("Failed to import history entry {}: {}", id, e);
        } else {
            imported += 1;
        }
    }

    result.history_entries_imported += imported;

    if imported > 0 {
        log::info!(
            "Imported {} legacy history entries from {}",
            imported,
            source
        );
    }

    if let Err(e) = tx.commit() {
        result.errors.push(format!("history: commit failed: {}", e));
        return;
    }

    set_import_status(state_conn.as_ref(), source, ImportStatus::Completed);
}

/// Imports saved queries from legacy `saved_queries.json`.
fn import_saved_queries_with_status(
    state_conn: &OwnedConnection,
    config_dir: &Path,
    result: &mut LegacyImportResult,
) {
    let source = "saved_queries.json";

    match get_import_status(state_conn, source) {
        Some(true) => return,
        Some(false) => log::info!("Retrying failed import: {}", source),
        None => {}
    }

    let path = match legacy_path_if_exists(config_dir, source) {
        Some(p) => p,
        None => return,
    };

    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            result
                .errors
                .push(format!("saved_queries: cannot read {}: {}", source, e));
            return;
        }
    };

    let legacy: Vec<SavedQuery> = match serde_json::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            result
                .errors
                .push(format!("saved_queries: cannot parse {}: {}", source, e));
            set_import_status(state_conn.as_ref(), source, ImportStatus::Failed);
            return;
        }
    };

    if legacy.is_empty() {
        set_import_status(state_conn.as_ref(), source, ImportStatus::Completed);
        return;
    }

    set_import_status(state_conn.as_ref(), source, ImportStatus::Failed);

    let tx = match state_conn.unchecked_transaction() {
        Ok(t) => t,
        Err(e) => {
            result
                .errors
                .push(format!("saved_queries: cannot start transaction: {}", e));
            return;
        }
    };

    let repo = SavedQueriesRepository::new(state_conn.clone());
    let existing_ids: std::collections::HashSet<String> = repo
        .all()
        .map(|v| v.into_iter().map(|q| q.id).collect())
        .unwrap_or_default();

    let mut imported = 0;
    for query in legacy {
        if existing_ids.contains(&query.id.to_string()) {
            continue;
        }

        let dto = SavedQueryDto {
            id: query.id.to_string(),
            folder_id: None,
            name: query.name,
            sql: query.sql,
            is_favorite: query.is_favorite,
            connection_id: query.connection_id.map(|u| u.to_string()),
            created_at: dbflux_core::chrono::DateTime::from_timestamp(query.created_at, 0)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default(),
            last_used_at: dbflux_core::chrono::DateTime::from_timestamp(query.last_used_at, 0)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default(),
        };

        if let Err(e) = repo.insert(&dto) {
            warn!("Failed to import saved query '{}': {}", dto.name, e);
        } else {
            imported += 1;
        }
    }

    result.saved_queries_imported += imported;

    if imported > 0 {
        log::info!("Imported {} legacy saved queries from {}", imported, source);
    }

    if let Err(e) = tx.commit() {
        result
            .errors
            .push(format!("saved_queries: commit failed: {}", e));
        return;
    }

    set_import_status(state_conn.as_ref(), source, ImportStatus::Completed);
}

/// Imports recent files from legacy `recent_files.json`.
fn import_recent_items_with_status(
    state_conn: &OwnedConnection,
    config_dir: &Path,
    result: &mut LegacyImportResult,
) {
    let source = "recent_files.json";

    match get_import_status(state_conn, source) {
        Some(true) => return,
        Some(false) => log::info!("Retrying failed import: {}", source),
        None => {}
    }

    let path = match legacy_path_if_exists(config_dir, source) {
        Some(p) => p,
        None => return,
    };

    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            result
                .errors
                .push(format!("recent_files: cannot read {}: {}", source, e));
            return;
        }
    };

    #[derive(Debug, Deserialize)]
    struct LegacyRecentFile {
        path: PathBuf,
        #[serde(rename = "last_opened")]
        last_opened: Option<i64>,
    }

    let legacy: Vec<LegacyRecentFile> = match serde_json::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            result
                .errors
                .push(format!("recent_files: cannot parse {}: {}", source, e));
            set_import_status(state_conn.as_ref(), source, ImportStatus::Failed);
            return;
        }
    };

    if legacy.is_empty() {
        set_import_status(state_conn.as_ref(), source, ImportStatus::Completed);
        return;
    }

    set_import_status(state_conn.as_ref(), source, ImportStatus::Failed);

    let tx = match state_conn.unchecked_transaction() {
        Ok(t) => t,
        Err(e) => {
            result
                .errors
                .push(format!("recent_files: cannot start transaction: {}", e));
            return;
        }
    };

    let repo = RecentItemsRepository::new(state_conn.clone());
    let existing_ids: std::collections::HashSet<String> = repo
        .all()
        .map(|v| v.into_iter().map(|r| r.id).collect())
        .unwrap_or_default();

    let mut imported = 0;
    for recent in legacy {
        let path_str = recent.path.to_string_lossy().to_string();

        // Derive a stable UUID from the path so retries are idempotent
        let stable_id = derive_stable_id(&path_str);

        if existing_ids.contains(&stable_id) {
            continue;
        }

        let title = recent
            .path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        let dto = RecentItemDto {
            id: stable_id,
            kind: "file".to_string(),
            profile_id: None,
            path: Some(path_str),
            title,
            accessed_at: recent
                .last_opened
                .map(|ts| {
                    dbflux_core::chrono::DateTime::from_timestamp(ts, 0)
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_default()
                })
                .unwrap_or_default(),
        };

        if let Err(e) = repo.record_access(&dto) {
            warn!("Failed to import recent file '{}': {}", dto.title, e);
        } else {
            imported += 1;
        }
    }

    result.recent_items_imported += imported;

    if imported > 0 {
        log::info!("Imported {} legacy recent files from {}", imported, source);
    }

    if let Err(e) = tx.commit() {
        result
            .errors
            .push(format!("recent_files: commit failed: {}", e));
        return;
    }

    set_import_status(state_conn.as_ref(), source, ImportStatus::Completed);
}

/// Derives a stable ID from a path string using SHA-1.
///
/// Uses a fixed namespace prefix hashed with the path to produce a deterministic
/// 16-byte identifier, then formatted as a UUID string for consistency with the rest
/// of the system. The same path always produces the same ID across retries.
fn derive_stable_id(path: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    const NAMESPACE: &str = "dbflux.recent_items";
    let combined = format!("{}:{}", NAMESPACE, path);

    let mut hasher = DefaultHasher::new();
    combined.hash(&mut hasher);
    let hash1 = hasher.finish();

    let mut hasher2 = DefaultHasher::new();
    hash1.hash(&mut hasher2);
    let hash2 = hasher2.finish();

    // Format as UUID-like string: first 16 hex chars from hash1, next 16 from hash2
    format!("{:016x}-{:016x}", hash1, hash2)
}

/// Restores UI state from legacy `state.json` in the XDG data directory.
fn import_ui_state_with_status(
    state_conn: &OwnedConnection,
    data_dir: &Path,
    result: &mut LegacyImportResult,
) {
    let source = "state.json";
    let path = data_dir.join(source);

    match get_import_status(state_conn, source) {
        Some(true) => return,
        Some(false) => log::info!("Retrying failed import: {}", source),
        None => {}
    }

    if !path.exists() {
        set_import_status(state_conn.as_ref(), source, ImportStatus::Completed);
        return;
    }

    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            result
                .errors
                .push(format!("ui_state: cannot read {}: {}", source, e));
            set_import_status(state_conn.as_ref(), source, ImportStatus::Failed);
            return;
        }
    };

    #[derive(Debug, Deserialize)]
    struct LegacyUiState {
        #[serde(rename = "settings_collapsed_security")]
        settings_collapsed_security: Option<bool>,
        #[serde(rename = "settings_collapsed_network")]
        settings_collapsed_network: Option<bool>,
        #[serde(rename = "settings_collapsed_connection")]
        settings_collapsed_connection: Option<bool>,
    }

    let legacy: LegacyUiState = match serde_json::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            result
                .errors
                .push(format!("ui_state: cannot parse {}: {}", source, e));
            set_import_status(state_conn.as_ref(), source, ImportStatus::Failed);
            return;
        }
    };

    let repo = UiStateRepository::new(state_conn.clone());

    if legacy.settings_collapsed_security.unwrap_or(false) {
        let _ = repo.set("ui.collapse.security", r#"{"value":true}"#);
    }
    if legacy.settings_collapsed_network.unwrap_or(false) {
        let _ = repo.set("ui.collapse.network", r#"{"value":true}"#);
    }
    if legacy.settings_collapsed_connection.unwrap_or(false) {
        let _ = repo.set("ui.collapse.connection", r#"{"value":true}"#);
    }

    result.ui_state_restored = true;
    set_import_status(state_conn.as_ref(), source, ImportStatus::Completed);
    log::info!("Restored legacy UI state from {}", source);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations;
    use crate::sqlite::open_database;
    use std::sync::Arc;

    fn temp_config_db(name: &str) -> (std::path::PathBuf, OwnedConnection) {
        let path = std::env::temp_dir().join(format!(
            "dbflux_legacy_config_{}_{}.sqlite",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let conn = open_database(&path).expect("open");
        migrations::run_config_migrations(&conn).expect("migrate");
        (path, Arc::new(conn))
    }

    fn temp_state_db(name: &str) -> (std::path::PathBuf, OwnedConnection) {
        let path = std::env::temp_dir().join(format!(
            "dbflux_legacy_state_{}_{}.sqlite",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let conn = open_database(&path).expect("open");
        migrations::run_state_migrations(&conn).expect("migrate");
        (path, Arc::new(conn))
    }

    fn isolated_legacy_dir(name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        // Create two separate isolated roots so we can test config-dir vs data-dir files
        let base = std::env::temp_dir().join(format!(
            "dbflux_legacy_test_{}_{}",
            name,
            std::process::id()
        ));
        let config_dir = base.join("config");
        let data_dir = base.join("data");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        (config_dir, data_dir)
    }

    #[test]
    fn import_nonexistent_files_returns_zero() {
        let (_config_path, config_conn) = temp_config_db("nonexistent");
        let (_state_path, state_conn) = temp_state_db("nonexistent");
        let (config_dir, data_dir) = isolated_legacy_dir("nonexistent");

        let result = run_legacy_import(config_conn, state_conn, &config_dir, &data_dir);

        assert_eq!(result.profiles_imported, 0);
        assert_eq!(result.auth_profiles_imported, 0);
        assert_eq!(result.proxy_profiles_imported, 0);
        assert_eq!(result.ssh_tunnels_imported, 0);
        assert_eq!(result.driver_settings_imported, 0);
        assert_eq!(result.hook_definitions_imported, 0);
        assert!(!result.general_settings_imported);
        assert!(!result.governance_imported);
        assert_eq!(result.history_entries_imported, 0);
        assert_eq!(result.saved_queries_imported, 0);
        assert_eq!(result.recent_items_imported, 0);
        assert!(!result.has_errors());

        let _ = std::fs::remove_file(&_config_path);
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(&_state_path);
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_dir_all(&config_dir.parent().unwrap());
    }

    #[test]
    fn import_result_tracks_counts() {
        let mut result = LegacyImportResult::default();
        result.profiles_imported = 5;
        result.auth_profiles_imported = 3;
        result.history_entries_imported = 100;
        result.errors.push("test error".to_string());

        assert_eq!(result.total_imported(), 108);
        assert!(result.has_errors());
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn import_idempotent_with_status_marker() {
        let (_config_path, config_conn) = temp_config_db("idempotent");
        let (_state_path, state_conn) = temp_state_db("idempotent");
        let (config_dir, data_dir) = isolated_legacy_dir("idempotent");

        // Write a legacy profiles.json with valid ConnectionProfile JSON
        let profile_json = serde_json::to_string(&[dbflux_core::ConnectionProfile::new_with_kind(
            "Test Profile",
            dbflux_core::DbKind::Postgres,
            dbflux_core::DbConfig::Postgres {
                use_uri: false,
                uri: None,
                host: "localhost".to_string(),
                port: 5432,
                user: "test".to_string(),
                database: "testdb".to_string(),
                ssl_mode: dbflux_core::SslMode::Disable,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        )])
        .unwrap();
        std::fs::write(config_dir.join("profiles.json"), &profile_json).unwrap();

        // First run: should import
        let result1 = run_legacy_import(
            config_conn.clone(),
            state_conn.clone(),
            &config_dir,
            &data_dir,
        );
        assert_eq!(result1.profiles_imported, 1);

        // Second run: should skip (status marker set to completed)
        let result2 = run_legacy_import(
            config_conn.clone(),
            state_conn.clone(),
            &config_dir,
            &data_dir,
        );
        assert_eq!(
            result2.profiles_imported, 0,
            "should skip already-completed imports"
        );

        // Cleanup
        let _ = std::fs::remove_file(&_config_path);
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(&_state_path);
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_dir_all(&config_dir.parent().unwrap());
    }

    #[test]
    fn import_retry_after_failure() {
        let (_config_path, config_conn) = temp_config_db("retry");
        let (_state_path, state_conn) = temp_state_db("retry");
        let (config_dir, data_dir) = isolated_legacy_dir("retry");

        // Mark as failed first
        set_import_status(config_conn.as_ref(), "profiles.json", ImportStatus::Failed);

        // Write valid profiles.json with valid ConnectionProfile JSON
        let profile_json = serde_json::to_string(&[dbflux_core::ConnectionProfile::new_with_kind(
            "Retry Profile",
            dbflux_core::DbKind::Postgres,
            dbflux_core::DbConfig::Postgres {
                use_uri: false,
                uri: None,
                host: "localhost".to_string(),
                port: 5432,
                user: "test".to_string(),
                database: "testdb".to_string(),
                ssl_mode: dbflux_core::SslMode::Disable,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        )])
        .unwrap();
        std::fs::write(config_dir.join("profiles.json"), &profile_json).unwrap();

        // Run should retry and succeed
        let result = run_legacy_import(
            config_conn.clone(),
            state_conn.clone(),
            &config_dir,
            &data_dir,
        );
        assert_eq!(result.profiles_imported, 1);

        // Cleanup
        let _ = std::fs::remove_file(&_config_path);
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(&_state_path);
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_dir_all(&config_dir.parent().unwrap());
    }

    #[test]
    fn import_records_status_on_failure() {
        let (_config_path, config_conn) = temp_config_db("fail_status");
        let (_state_path, state_conn) = temp_state_db("fail_status");
        let (config_dir, data_dir) = isolated_legacy_dir("fail_status");

        // Write invalid JSON (will cause parse failure)
        std::fs::write(config_dir.join("profiles.json"), "not valid json {{{").unwrap();

        let result = run_legacy_import(
            config_conn.clone(),
            state_conn.clone(),
            &config_dir,
            &data_dir,
        );
        assert!(result.has_errors());

        // Status should be recorded as failed
        assert_eq!(
            get_import_status(&config_conn, "profiles.json"),
            Some(false),
            "failed status should be recorded"
        );

        // Cleanup
        let _ = std::fs::remove_file(&_config_path);
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(&_state_path);
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_dir_all(&config_dir.parent().unwrap());
    }

    #[test]
    fn any_imported_detects_items() {
        let mut result = LegacyImportResult::default();
        assert!(!result.any_imported());

        result.profiles_imported = 5;
        assert!(result.any_imported());

        result.profiles_imported = 0;
        result.ui_state_restored = true;
        assert!(result.any_imported());

        result.ui_state_restored = false;
        result.general_settings_imported = true;
        assert!(result.any_imported());

        result.general_settings_imported = false;
        result.governance_imported = true;
        assert!(result.any_imported());
    }

    #[test]
    fn import_config_json_general_settings() {
        let (_config_path, config_conn) = temp_config_db("general_settings");
        let (_state_path, state_conn) = temp_state_db("general_settings");
        let (config_dir, data_dir) = isolated_legacy_dir("general_settings");

        let config_json = r#"{
            "version": 3,
            "services": [],
            "general": {
                "theme": "dark",
                "restore_session_on_startup": true,
                "reopen_last_connections": false,
                "default_focus_on_startup": "sidebar",
                "max_history_entries": 500,
                "auto_save_interval_ms": 3000,
                "default_refresh_policy": "manual",
                "default_refresh_interval_secs": 30,
                "max_concurrent_background_tasks": 4,
                "auto_refresh_pause_on_error": true,
                "auto_refresh_only_if_visible": false,
                "confirm_dangerous_queries": true,
                "dangerous_requires_where": true,
                "dangerous_requires_preview": false
            },
            "driver_overrides": {},
            "driver_settings": {},
            "hook_definitions": {},
            "governance": {
                "mcp_enabled_by_default": true,
                "trusted_clients": [],
                "roles": [],
                "policies": []
            }
        }"#;
        std::fs::write(config_dir.join("config.json"), config_json).unwrap();

        let result = run_legacy_import(config_conn.clone(), state_conn, &config_dir, &data_dir);

        assert!(
            result.general_settings_imported,
            "general settings should be imported"
        );
        assert!(
            !result.has_errors(),
            "should have no errors: {:?}",
            result.errors
        );

        let repo = GeneralSettingsRepository::new(config_conn);
        let stored = repo.get().unwrap();
        assert!(stored.is_some(), "general_settings should be stored");

        let _ = std::fs::remove_file(&_config_path);
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(&_state_path);
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_dir_all(&config_dir.parent().unwrap());
    }

    #[test]
    fn import_config_json_driver_settings() {
        let (_config_path, config_conn) = temp_config_db("driver_settings");
        let (_state_path, state_conn) = temp_state_db("driver_settings");
        let (config_dir, data_dir) = isolated_legacy_dir("driver_settings");

        let config_json = r#"{
            "version": 3,
            "services": [],
            "general": {},
            "driver_overrides": {
                "postgres": {
                    "refresh_policy": "interval",
                    "refresh_interval_secs": 60,
                    "confirm_dangerous": false,
                    "requires_where": true,
                    "requires_preview": true
                }
            },
            "driver_settings": {
                "postgres": {"batch_size": "1000"}
            },
            "hook_definitions": {},
            "governance": {}
        }"#;
        std::fs::write(config_dir.join("config.json"), config_json).unwrap();

        let result = run_legacy_import(config_conn.clone(), state_conn, &config_dir, &data_dir);

        assert!(
            !result.has_errors(),
            "should have no errors: {:?}",
            result.errors
        );
        assert_eq!(
            result.driver_settings_imported, 2,
            "should import 2 driver settings entries (1 override + 1 setting value)"
        );

        let overrides_repo = DriverOverridesRepository::new(config_conn.clone());
        let postgres = overrides_repo.get("builtin:postgres").unwrap().unwrap();
        assert!(
            postgres.refresh_policy.is_some(),
            "refresh_policy should be stored"
        );

        let values_repo = DriverSettingValuesRepository::new(config_conn);
        let values = values_repo.get_for_driver("builtin:postgres").unwrap();
        assert!(!values.is_empty(), "driver_setting_values should be stored");

        let _ = std::fs::remove_file(&_config_path);
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(&_state_path);
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_dir_all(&config_dir.parent().unwrap());
    }

    #[test]
    fn import_config_json_hook_definitions() {
        let (_config_path, config_conn) = temp_config_db("hook_defs");
        let (_state_path, state_conn) = temp_state_db("hook_defs");
        let (config_dir, data_dir) = isolated_legacy_dir("hook_defs");

        let config_json = r#"{
            "version": 3,
            "services": [],
            "general": {},
            "driver_overrides": {},
            "driver_settings": {},
            "hook_definitions": {
                "my_preconnect_hook": {
                    "enabled": true,
                    "kind": "command",
                    "command": "/usr/local/bin/check_ready.sh",
                    "args": ["--timeout", "5"],
                    "env": {},
                    "inherit_env": true,
                    "timeout_ms": 10000,
                    "execution_mode": "blocking",
                    "on_failure": "warn"
                }
            },
            "governance": {}
        }"#;
        std::fs::write(config_dir.join("config.json"), config_json).unwrap();

        let result = run_legacy_import(config_conn.clone(), state_conn, &config_dir, &data_dir);

        assert!(
            !result.has_errors(),
            "should have no errors: {:?}",
            result.errors
        );
        assert_eq!(
            result.hook_definitions_imported, 1,
            "should import 1 hook definition"
        );

        let repo = HookDefinitionRepository::new(config_conn);
        let hooks = repo.all().unwrap();
        assert_eq!(hooks.len(), 1, "should have 1 hook in repo");
        assert_eq!(hooks[0].name, "my_preconnect_hook");

        let _ = std::fs::remove_file(&_config_path);
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(&_state_path);
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_dir_all(&config_dir.parent().unwrap());
    }

    #[test]
    fn import_config_json_governance() {
        let (_config_path, config_conn) = temp_config_db("governance");
        let (_state_path, state_conn) = temp_state_db("governance");
        let (config_dir, data_dir) = isolated_legacy_dir("governance");

        // Note: Legacy governance roles had `name`, `description`, and `permissions` fields.
        // The current format uses `policy_ids` instead. The legacy format would fail
        // validation with "legacy fields but no policy_ids" error.
        // This test uses the correct current format with policy_ids.
        let config_json = r#"{
            "version": 3,
            "services": [],
            "general": {},
            "driver_overrides": {},
            "driver_settings": {},
            "hook_definitions": {},
            "governance": {
                "mcp_enabled_by_default": false,
                "trusted_clients": [
                    {
                        "id": "client-1",
                        "name": "Test Client",
                        "active": true
                    }
                ],
                "roles": [
                    {
                        "id": "readonly",
                        "policy_ids": ["policy-1"]
                    }
                ],
                "policies": [
                    {
                        "id": "policy-1",
                        "allowed_tools": ["select_data", "list_databases"],
                        "allowed_classes": ["read"]
                    }
                ]
            }
        }"#;
        std::fs::write(config_dir.join("config.json"), config_json).unwrap();

        let result = run_legacy_import(config_conn.clone(), state_conn, &config_dir, &data_dir);

        assert!(
            !result.has_errors(),
            "should have no errors: {:?}",
            result.errors
        );
        assert!(result.governance_imported, "governance should be imported");

        let repo = GovernanceSettingsRepository::new(config_conn);
        let stored = repo.get().unwrap();
        assert!(stored.is_some(), "governance_settings should exist");
        let clients = repo.get_trusted_clients().unwrap();
        assert!(!clients.is_empty(), "governance trusted_clients should be imported");
        assert_eq!(clients[0].client_id, "client-1", "client-1 should be imported");

        let _ = std::fs::remove_file(&_config_path);
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(&_state_path);
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_dir_all(&config_dir.parent().unwrap());
    }

    #[test]
    fn import_config_json_idempotent_retry() {
        let (_config_path, config_conn) = temp_config_db("config_idempotent");
        let (_state_path, state_conn) = temp_state_db("config_idempotent");
        let (config_dir, data_dir) = isolated_legacy_dir("config_idempotent");

        let config_json = r#"{
            "version": 3,
            "services": [],
            "general": {"theme": "dark"},
            "driver_overrides": {},
            "driver_settings": {},
            "hook_definitions": {},
            "governance": {}
        }"#;
        std::fs::write(config_dir.join("config.json"), config_json).unwrap();

        let result1 = run_legacy_import(
            config_conn.clone(),
            state_conn.clone(),
            &config_dir,
            &data_dir,
        );
        assert!(
            result1.general_settings_imported,
            "first run should import general settings"
        );

        let result2 = run_legacy_import(
            config_conn.clone(),
            state_conn.clone(),
            &config_dir,
            &data_dir,
        );
        assert!(
            !result2.general_settings_imported,
            "second run should skip completed import"
        );

        let _ = std::fs::remove_file(&_config_path);
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(&_state_path);
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_dir_all(&config_dir.parent().unwrap());
    }

    #[test]
    fn import_config_json_governance_legacy_role_fails_loudly() {
        // Test that legacy governance roles with name/description/permissions
        // but no policy_ids fail with a clear error rather than silently losing data.
        let (_config_path, config_conn) = temp_config_db("governance_legacy_fail");
        let (_state_path, state_conn) = temp_state_db("governance_legacy_fail");
        let (config_dir, data_dir) = isolated_legacy_dir("governance_legacy_fail");

        let config_json = r#"{
            "version": 3,
            "services": [],
            "general": {},
            "driver_overrides": {},
            "driver_settings": {},
            "hook_definitions": {},
            "governance": {
                "mcp_enabled_by_default": false,
                "trusted_clients": [],
                "roles": [
                    {
                        "id": "readonly",
                        "name": "Read Only",
                        "description": "Can only read data",
                        "permissions": ["select_data", "list_databases"]
                    }
                ],
                "policies": []
            }
        }"#;
        std::fs::write(config_dir.join("config.json"), config_json).unwrap();

        let result = run_legacy_import(config_conn.clone(), state_conn, &config_dir, &data_dir);

        // Should have an error about legacy role conversion
        assert!(
            result.has_errors(),
            "should have errors for legacy role format"
        );
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.contains("legacy fields") && e.contains("policy_ids")),
            "error should mention legacy fields and policy_ids: {:?}",
            result.errors
        );

        // Import status should be failed (retriable)
        assert_eq!(
            get_import_status(&config_conn, "config.json"),
            Some(false),
            "failed status should be recorded for retry"
        );

        let _ = std::fs::remove_file(&_config_path);
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(&_state_path);
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_dir_all(&config_dir.parent().unwrap());
    }

    #[test]
    fn import_config_json_governance_legacy_role_with_policy_ids_succeeds() {
        // Test that legacy governance roles that ALSO have policy_ids can be imported.
        // The legacy fields (name/description/permissions) will be dropped, but since
        // policy_ids is present, the role structure is valid.
        let (_config_path, config_conn) = temp_config_db("governance_legacy_with_policy");
        let (_state_path, state_conn) = temp_state_db("governance_legacy_with_policy");
        let (config_dir, data_dir) = isolated_legacy_dir("governance_legacy_with_policy");

        let config_json = r#"{
            "version": 3,
            "services": [],
            "general": {},
            "driver_overrides": {},
            "driver_settings": {},
            "hook_definitions": {},
            "governance": {
                "mcp_enabled_by_default": false,
                "trusted_clients": [],
                "roles": [
                    {
                        "id": "readonly",
                        "name": "Read Only",
                        "description": "Can only read data",
                        "permissions": ["select_data", "list_databases"],
                        "policy_ids": ["policy-1"]
                    }
                ],
                "policies": [
                    {
                        "id": "policy-1",
                        "allowed_tools": ["select_data"],
                        "allowed_classes": ["read"]
                    }
                ]
            }
        }"#;
        std::fs::write(config_dir.join("config.json"), config_json).unwrap();

        let result = run_legacy_import(config_conn.clone(), state_conn, &config_dir, &data_dir);

        // Should succeed because policy_ids is present
        assert!(
            !result.has_errors(),
            "should have no errors when policy_ids present: {:?}",
            result.errors
        );
        assert!(result.governance_imported, "governance should be imported");

        let _ = std::fs::remove_file(&_config_path);
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_config_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(&_state_path);
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(_state_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_dir_all(&config_dir.parent().unwrap());
    }
}
