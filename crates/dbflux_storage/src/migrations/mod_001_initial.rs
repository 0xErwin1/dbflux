//! Migration 001: Initial unified schema for DBFlux internal storage.
//!
//! This migration creates all tables with domain-prefixed naming in a single
//! database, replacing the previous separate databases.
//!
//! ## Domain Prefix Convention
//!
//! - `cfg_*` — Config domain (profiles, auth, hooks, services, governance)
//! - `st_*`  — State domain (sessions, query history, UI state)
//! - `aud_*` — Audit domain (audit events)
//! - `sys_*` — System domain (migrations, metadata)

use rusqlite::Transaction;

use crate::migrations::{Migration, MigrationError};

/// The initial unified schema migration.
pub struct MigrationImpl;

impl Migration for MigrationImpl {
    fn name(&self) -> &str {
        "001_initial"
    }

    fn run(&self, tx: &Transaction) -> Result<(), MigrationError> {
        tx.execute_batch(SCHEMA)
            .map_err(|source| MigrationError::Sqlite {
                path: std::path::PathBuf::from("<unknown>"),
                source,
            })?;
        Ok(())
    }
}

const SCHEMA: &str = r#"
-- ============================================================================
-- SYSTEM DOMAIN (sys_*) - Must be created first due to FK dependencies
-- ============================================================================

-- Note: sys_migrations is created by ensure_sys_migrations() in mod.rs before
-- any migration runs. It is NOT included here to avoid duplication.

-- System metadata key-value store
CREATE TABLE IF NOT EXISTS sys_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Legacy imports tracking
CREATE TABLE IF NOT EXISTS sys_legacy_imports (
    id TEXT PRIMARY KEY,
    source_path TEXT NOT NULL,
    source_hash TEXT NOT NULL UNIQUE,
    imported_at TEXT NOT NULL DEFAULT (datetime('now')),
    record_count INTEGER NOT NULL DEFAULT 0,
    domain TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'completed',
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_sys_legacy_imports_source_path
    ON sys_legacy_imports(source_path);
CREATE INDEX IF NOT EXISTS idx_sys_legacy_imports_source_hash
    ON sys_legacy_imports(source_hash);
CREATE INDEX IF NOT EXISTS idx_sys_legacy_imports_domain
    ON sys_legacy_imports(domain);

-- ============================================================================
-- CONFIG DOMAIN (cfg_*) - Connection profiles, auth, hooks, services, governance
-- ============================================================================

-- --------------------------------------------------------------------------
-- General settings (singleton)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_general_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    theme TEXT NOT NULL DEFAULT 'dark',
    restore_session_on_startup INTEGER NOT NULL DEFAULT 1,
    reopen_last_connections INTEGER NOT NULL DEFAULT 0,
    default_focus_on_startup TEXT NOT NULL DEFAULT 'sidebar',
    max_history_entries INTEGER NOT NULL DEFAULT 1000,
    auto_save_interval_ms INTEGER NOT NULL DEFAULT 2000,
    default_refresh_policy TEXT NOT NULL DEFAULT 'manual',
    default_refresh_interval_secs INTEGER NOT NULL DEFAULT 5,
    max_concurrent_background_tasks INTEGER NOT NULL DEFAULT 8,
    auto_refresh_pause_on_error INTEGER NOT NULL DEFAULT 1,
    auto_refresh_only_if_visible INTEGER NOT NULL DEFAULT 0,
    confirm_dangerous_queries INTEGER NOT NULL DEFAULT 1,
    dangerous_requires_where INTEGER NOT NULL DEFAULT 1,
    dangerous_requires_preview INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT OR IGNORE INTO cfg_general_settings (id) VALUES (1);

-- --------------------------------------------------------------------------
-- Governance settings
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_governance_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    mcp_enabled_by_default INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT OR IGNORE INTO cfg_governance_settings (id) VALUES (1);

CREATE TABLE IF NOT EXISTS cfg_trusted_clients (
    id TEXT PRIMARY KEY,
    governance_id INTEGER NOT NULL DEFAULT 1,
    client_id TEXT NOT NULL,
    name TEXT NOT NULL,
    issuer TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (governance_id) REFERENCES cfg_governance_settings(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_trusted_clients_governance
    ON cfg_trusted_clients(governance_id);

CREATE TABLE IF NOT EXISTS cfg_policy_roles (
    id TEXT PRIMARY KEY,
    governance_id INTEGER NOT NULL DEFAULT 1,
    role_id TEXT NOT NULL,
    FOREIGN KEY (governance_id) REFERENCES cfg_governance_settings(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_policy_roles_governance
    ON cfg_policy_roles(governance_id);

CREATE TABLE IF NOT EXISTS cfg_tool_policies (
    id TEXT PRIMARY KEY,
    governance_id INTEGER NOT NULL DEFAULT 1,
    policy_id TEXT NOT NULL,
    FOREIGN KEY (governance_id) REFERENCES cfg_governance_settings(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_tool_policies_governance
    ON cfg_tool_policies(governance_id);

CREATE TABLE IF NOT EXISTS cfg_tool_policy_allowed_tools (
    id TEXT PRIMARY KEY,
    tool_policy_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    FOREIGN KEY (tool_policy_id) REFERENCES cfg_tool_policies(id) ON DELETE CASCADE,
    UNIQUE(tool_policy_id, tool_name)
);

CREATE INDEX IF NOT EXISTS idx_cfg_tool_policy_allowed_tools_policy
    ON cfg_tool_policy_allowed_tools(tool_policy_id);

CREATE TABLE IF NOT EXISTS cfg_tool_policy_allowed_classes (
    id TEXT PRIMARY KEY,
    tool_policy_id TEXT NOT NULL,
    class_name TEXT NOT NULL,
    FOREIGN KEY (tool_policy_id) REFERENCES cfg_tool_policies(id) ON DELETE CASCADE,
    UNIQUE(tool_policy_id, class_name)
);

CREATE INDEX IF NOT EXISTS idx_cfg_tool_policy_allowed_classes_policy
    ON cfg_tool_policy_allowed_classes(tool_policy_id);

-- --------------------------------------------------------------------------
-- Driver settings overrides
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_driver_overrides (
    driver_key TEXT PRIMARY KEY,
    refresh_policy TEXT,
    refresh_interval_secs INTEGER,
    confirm_dangerous INTEGER,
    requires_where INTEGER,
    requires_preview INTEGER,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS cfg_driver_setting_values (
    id TEXT PRIMARY KEY,
    driver_key TEXT NOT NULL,
    setting_key TEXT NOT NULL,
    setting_value TEXT,
    FOREIGN KEY (driver_key) REFERENCES cfg_driver_overrides(driver_key) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cfg_driver_setting_values_driver_key
    ON cfg_driver_setting_values(driver_key, setting_key);

-- --------------------------------------------------------------------------
-- Auth profiles
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_auth_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    enabled INTEGER DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS cfg_auth_profile_fields (
    id TEXT PRIMARY KEY,
    auth_profile_id TEXT NOT NULL,
    field_key TEXT NOT NULL,
    value_text TEXT,
    value_bool INTEGER,
    value_number REAL,
    value_secret_ref TEXT,
    value_kind TEXT NOT NULL,
    FOREIGN KEY (auth_profile_id) REFERENCES cfg_auth_profiles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_auth_profile_fields_profile
    ON cfg_auth_profile_fields(auth_profile_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cfg_auth_profile_fields_profile_key
    ON cfg_auth_profile_fields(auth_profile_id, field_key);

-- --------------------------------------------------------------------------
-- Proxy profiles
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_proxy_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    host TEXT NOT NULL,
    port INTEGER NOT NULL,
    auth_kind TEXT NOT NULL DEFAULT 'none',
    no_proxy TEXT,
    enabled INTEGER DEFAULT 1,
    save_secret INTEGER DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS cfg_proxy_auth (
    proxy_profile_id TEXT PRIMARY KEY,
    username TEXT,
    domain TEXT,
    password_secret_ref TEXT,
    FOREIGN KEY (proxy_profile_id) REFERENCES cfg_proxy_profiles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_proxy_auth_profile
    ON cfg_proxy_auth(proxy_profile_id);

-- --------------------------------------------------------------------------
-- SSH tunnel profiles
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_ssh_tunnel_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    host TEXT NOT NULL,
    port INTEGER NOT NULL DEFAULT 22,
    user TEXT NOT NULL,
    auth_method TEXT NOT NULL DEFAULT 'password',
    key_path TEXT,
    passphrase_secret_ref TEXT,
    password_secret_ref TEXT,
    save_secret INTEGER DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS cfg_ssh_tunnel_auth (
    ssh_tunnel_profile_id TEXT PRIMARY KEY,
    key_path TEXT,
    password_secret_ref TEXT,
    passphrase_secret_ref TEXT,
    FOREIGN KEY (ssh_tunnel_profile_id) REFERENCES cfg_ssh_tunnel_profiles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_ssh_tunnel_auth_profile
    ON cfg_ssh_tunnel_auth(ssh_tunnel_profile_id);

-- --------------------------------------------------------------------------
-- Connection profiles (core)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_connection_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    driver_id TEXT,
    description TEXT,
    kind TEXT,
    save_password INTEGER NOT NULL DEFAULT 0,
    access_kind TEXT,
    access_provider TEXT,
    favorite INTEGER DEFAULT 0,
    color TEXT,
    icon TEXT,
    auth_profile_id TEXT,
    proxy_profile_id TEXT,
    ssh_tunnel_profile_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    -- Cross-domain FK constraints
    FOREIGN KEY (auth_profile_id) REFERENCES cfg_auth_profiles(id) ON DELETE SET NULL,
    FOREIGN KEY (proxy_profile_id) REFERENCES cfg_proxy_profiles(id) ON DELETE SET NULL,
    FOREIGN KEY (ssh_tunnel_profile_id) REFERENCES cfg_ssh_tunnel_profiles(id) ON DELETE SET NULL
);

-- EAV for non-DbConfig profile settings
CREATE TABLE IF NOT EXISTS cfg_connection_profile_configs (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    config_key TEXT NOT NULL,
    config_value TEXT,
    config_value_kind TEXT NOT NULL,
    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_profile_configs_profile
    ON cfg_connection_profile_configs(profile_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cfg_connection_profile_configs_profile_key
    ON cfg_connection_profile_configs(profile_id, config_key);

-- Settings overrides (FormValues)
CREATE TABLE IF NOT EXISTS cfg_connection_profile_settings (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    setting_key TEXT NOT NULL,
    setting_value TEXT,
    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_profile_settings_profile
    ON cfg_connection_profile_settings(profile_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cfg_connection_profile_settings_profile_key
    ON cfg_connection_profile_settings(profile_id, setting_key);

-- Value references (secrets, params, auth bindings)
CREATE TABLE IF NOT EXISTS cfg_connection_profile_value_refs (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    ref_key TEXT NOT NULL,
    ref_kind TEXT NOT NULL,
    ref_value TEXT NOT NULL,
    ref_provider TEXT,
    ref_json_key TEXT,
    literal_value TEXT,
    env_key TEXT,
    secret_locator TEXT,
    param_name TEXT,
    auth_field TEXT,
    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_profile_value_refs_profile
    ON cfg_connection_profile_value_refs(profile_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cfg_connection_profile_value_refs_profile_key
    ON cfg_connection_profile_value_refs(profile_id, ref_key);

-- Driver-specific config with native typed columns
CREATE TABLE IF NOT EXISTS cfg_connection_driver_configs (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL UNIQUE,
    config_key TEXT NOT NULL,
    -- Relational DB common fields
    use_uri INTEGER NOT NULL DEFAULT 0,
    uri TEXT,
    host TEXT,
    port INTEGER,
    user TEXT,
    database_name TEXT,
    ssl_mode TEXT NOT NULL DEFAULT 'prefer',
    ssl_ca TEXT,
    ssl_cert TEXT,
    ssl_key TEXT,
    password_secret_ref TEXT,
    connect_timeout_secs INTEGER,
    -- SSH tunnel inline config
    ssh_tunnel_host TEXT,
    ssh_tunnel_port INTEGER,
    ssh_tunnel_user TEXT,
    ssh_tunnel_auth_method TEXT NOT NULL DEFAULT 'private_key',
    ssh_tunnel_key_path TEXT,
    ssh_tunnel_passphrase_secret_ref TEXT,
    ssh_tunnel_password_secret_ref TEXT,
    -- SQLite-specific
    sqlite_path TEXT,
    sqlite_connection_id TEXT,
    -- MongoDB-specific
    mongo_auth_database TEXT,
    -- Redis-specific
    redis_tls INTEGER NOT NULL DEFAULT 0,
    redis_database INTEGER,
    -- DynamoDB-specific
    dynamo_region TEXT,
    dynamo_profile TEXT,
    dynamo_endpoint TEXT,
    dynamo_table TEXT,
    -- External config
    external_kind TEXT,
    external_values_json TEXT,
    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_driver_configs_profile
    ON cfg_connection_driver_configs(profile_id);

-- --------------------------------------------------------------------------
-- Connection profile hooks
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_connection_profile_hooks (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    order_index INTEGER NOT NULL DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1,
    hook_kind TEXT NOT NULL,
    command TEXT,
    script_language TEXT,
    script_source_type TEXT,
    script_content TEXT,
    script_path TEXT,
    lua_source_type TEXT,
    lua_content TEXT,
    lua_path TEXT,
    lua_log INTEGER DEFAULT 1,
    lua_env_read INTEGER DEFAULT 1,
    lua_conn_metadata INTEGER DEFAULT 1,
    lua_process_run INTEGER DEFAULT 0,
    cwd TEXT,
    inherit_env INTEGER DEFAULT 1,
    timeout_ms INTEGER,
    execution_mode TEXT NOT NULL DEFAULT 'blocking',
    ready_signal TEXT,
    on_failure TEXT NOT NULL DEFAULT 'disconnect',
    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_profile_hooks_profile
    ON cfg_connection_profile_hooks(profile_id);

CREATE TABLE IF NOT EXISTS cfg_connection_profile_hook_args (
    id TEXT PRIMARY KEY,
    hook_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (hook_id) REFERENCES cfg_connection_profile_hooks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_profile_hook_args_hook
    ON cfg_connection_profile_hook_args(hook_id);

CREATE TABLE IF NOT EXISTS cfg_connection_profile_hook_envs (
    id TEXT PRIMARY KEY,
    hook_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (hook_id) REFERENCES cfg_connection_profile_hooks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_profile_hook_envs_hook
    ON cfg_connection_profile_hook_envs(hook_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cfg_connection_profile_hook_envs_hook_key
    ON cfg_connection_profile_hook_envs(hook_id, key);

-- --------------------------------------------------------------------------
-- Connection profile governance
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_connection_profile_governance (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    governance_key TEXT NOT NULL,
    governance_value TEXT,
    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_profile_governance_profile
    ON cfg_connection_profile_governance(profile_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cfg_connection_profile_governance_profile_key
    ON cfg_connection_profile_governance(profile_id, governance_key);

CREATE TABLE IF NOT EXISTS cfg_connection_profile_governance_bindings (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    order_index INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_profile_governance_bindings_profile
    ON cfg_connection_profile_governance_bindings(profile_id);

CREATE TABLE IF NOT EXISTS cfg_connection_profile_gov_binding_roles (
    id TEXT PRIMARY KEY,
    binding_id TEXT NOT NULL,
    role_id TEXT NOT NULL,
    FOREIGN KEY (binding_id) REFERENCES cfg_connection_profile_governance_bindings(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_profile_gov_binding_roles_binding
    ON cfg_connection_profile_gov_binding_roles(binding_id);

CREATE TABLE IF NOT EXISTS cfg_connection_profile_gov_binding_policies (
    id TEXT PRIMARY KEY,
    binding_id TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    FOREIGN KEY (binding_id) REFERENCES cfg_connection_profile_governance_bindings(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_profile_gov_binding_policies_binding
    ON cfg_connection_profile_gov_binding_policies(binding_id);

-- Access params
CREATE TABLE IF NOT EXISTS cfg_connection_profile_access_params (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    param_key TEXT NOT NULL,
    param_value TEXT NOT NULL,
    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE CASCADE,
    UNIQUE(profile_id, param_key)
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_profile_access_params_profile
    ON cfg_connection_profile_access_params(profile_id);

-- --------------------------------------------------------------------------
-- Hook definitions (global)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_hook_definitions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    execution_mode TEXT NOT NULL DEFAULT 'Command',
    script_ref TEXT,
    cwd TEXT,
    inherit_env INTEGER DEFAULT 1,
    timeout_ms INTEGER,
    ready_signal TEXT,
    on_failure TEXT NOT NULL DEFAULT 'Warn',
    enabled INTEGER DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS cfg_hook_commands (
    id TEXT PRIMARY KEY,
    hook_id TEXT NOT NULL UNIQUE,
    command TEXT NOT NULL,
    working_directory TEXT,
    timeout_ms INTEGER,
    ready_signal TEXT,
    FOREIGN KEY (hook_id) REFERENCES cfg_hook_definitions(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_hook_commands_hook
    ON cfg_hook_commands(hook_id);

CREATE TABLE IF NOT EXISTS cfg_hook_environment (
    id TEXT PRIMARY KEY,
    hook_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (hook_id) REFERENCES cfg_hook_definitions(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_hook_environment_hook
    ON cfg_hook_environment(hook_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cfg_hook_environment_hook_key
    ON cfg_hook_environment(hook_id, key);

-- --------------------------------------------------------------------------
-- Hook bindings (references global hooks to profiles)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_hook_bindings (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    hook_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    order_index INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE CASCADE,
    FOREIGN KEY (hook_id) REFERENCES cfg_hook_definitions(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_hook_bindings_profile
    ON cfg_hook_bindings(profile_id);
CREATE INDEX IF NOT EXISTS idx_cfg_hook_bindings_hook
    ON cfg_hook_bindings(hook_id);

-- --------------------------------------------------------------------------
-- Services (external services/processes)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_services (
    socket_id TEXT PRIMARY KEY,
    enabled INTEGER DEFAULT 1,
    command TEXT,
    startup_timeout_ms INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS cfg_service_args (
    id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (service_id) REFERENCES cfg_services(socket_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_service_args_service
    ON cfg_service_args(service_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cfg_service_args_service_position
    ON cfg_service_args(service_id, position);

CREATE TABLE IF NOT EXISTS cfg_service_env (
    id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (service_id) REFERENCES cfg_services(socket_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_service_env_service
    ON cfg_service_env(service_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cfg_service_env_service_key
    ON cfg_service_env(service_id, key);

-- --------------------------------------------------------------------------
-- Connection folders
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg_connection_folders (
    id TEXT PRIMARY KEY,
    parent_id TEXT,
    name TEXT NOT NULL,
    position INTEGER NOT NULL DEFAULT 0,
    collapsed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (parent_id) REFERENCES cfg_connection_folders(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_folders_parent
    ON cfg_connection_folders(parent_id);

CREATE TABLE IF NOT EXISTS cfg_connection_folder_items (
    id TEXT PRIMARY KEY,
    folder_id TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    position INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (folder_id) REFERENCES cfg_connection_folders(id) ON DELETE CASCADE,
    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE CASCADE,
    UNIQUE(folder_id, profile_id)
);

CREATE INDEX IF NOT EXISTS idx_cfg_connection_folder_items_folder
    ON cfg_connection_folder_items(folder_id);
CREATE INDEX IF NOT EXISTS idx_cfg_connection_folder_items_profile
    ON cfg_connection_folder_items(profile_id);

-- ============================================================================
-- STATE DOMAIN (st_*) - Sessions, query history, UI state
-- ============================================================================

-- UI runtime state
CREATE TABLE IF NOT EXISTS st_ui_state (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Recent items
CREATE TABLE IF NOT EXISTS st_recent_items (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    profile_id TEXT,
    path TEXT,
    title TEXT,
    accessed_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_st_recent_items_profile
    ON st_recent_items(profile_id);
CREATE INDEX IF NOT EXISTS idx_st_recent_items_accessed
    ON st_recent_items(accessed_at DESC);

-- Query history
CREATE TABLE IF NOT EXISTS st_query_history (
    id TEXT PRIMARY KEY,
    connection_profile_id TEXT,
    driver_id TEXT,
    database_name TEXT,
    query_text TEXT NOT NULL,
    query_kind TEXT NOT NULL DEFAULT 'select',
    executed_at TEXT NOT NULL DEFAULT (datetime('now')),
    duration_ms INTEGER,
    succeeded INTEGER NOT NULL DEFAULT 1,
    error_summary TEXT,
    row_count INTEGER,
    is_favorite INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (connection_profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_st_query_history_profile
    ON st_query_history(connection_profile_id);
CREATE INDEX IF NOT EXISTS idx_st_query_history_executed
    ON st_query_history(executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_st_query_history_favorite
    ON st_query_history(is_favorite);

-- Saved query folders
CREATE TABLE IF NOT EXISTS st_saved_query_folders (
    id TEXT PRIMARY KEY,
    parent_id TEXT,
    name TEXT NOT NULL,
    position INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (parent_id) REFERENCES st_saved_query_folders(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_st_saved_query_folders_parent
    ON st_saved_query_folders(parent_id);

-- Saved queries
CREATE TABLE IF NOT EXISTS st_saved_queries (
    id TEXT PRIMARY KEY,
    folder_id TEXT,
    name TEXT NOT NULL,
    sql TEXT NOT NULL,
    is_favorite INTEGER NOT NULL DEFAULT 0,
    connection_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_used_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (folder_id) REFERENCES st_saved_query_folders(id) ON DELETE SET NULL,
    FOREIGN KEY (connection_id) REFERENCES cfg_connection_profiles(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_st_saved_queries_folder
    ON st_saved_queries(folder_id);
CREATE INDEX IF NOT EXISTS idx_st_saved_queries_connection
    ON st_saved_queries(connection_id);

-- Sessions
CREATE TABLE IF NOT EXISTS st_sessions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'workspace',
    active_index INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_opened_at TEXT NOT NULL DEFAULT (datetime('now')),
    is_last_active INTEGER NOT NULL DEFAULT 1
);

-- Partial unique index: only one session per kind can be "last active"
CREATE UNIQUE INDEX IF NOT EXISTS idx_st_sessions_kind_active
    ON st_sessions(kind) WHERE is_last_active = 1;

-- Index for frequent kind filtering
CREATE INDEX IF NOT EXISTS idx_st_sessions_kind
    ON st_sessions(kind);

-- Session tabs
CREATE TABLE IF NOT EXISTS st_session_tabs (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    tab_kind TEXT NOT NULL,
    title TEXT NOT NULL,
    position INTEGER NOT NULL DEFAULT 0,
    is_pinned INTEGER NOT NULL DEFAULT 0,
    scratch_file_path TEXT,
    shadow_file_path TEXT,
    language TEXT NOT NULL DEFAULT 'sql',
    file_path TEXT,
    exec_ctx_connection_id TEXT,
    exec_ctx_database TEXT,
    exec_ctx_schema TEXT,
    exec_ctx_container TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (session_id) REFERENCES st_sessions(id) ON DELETE CASCADE,
    FOREIGN KEY (exec_ctx_connection_id) REFERENCES cfg_connection_profiles(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_st_session_tabs_session
    ON st_session_tabs(session_id);
CREATE INDEX IF NOT EXISTS idx_st_session_tabs_exec_ctx_connection
    ON st_session_tabs(exec_ctx_connection_id);

-- Schema cache
CREATE TABLE IF NOT EXISTS st_schema_cache (
    id TEXT PRIMARY KEY,
    cache_key TEXT NOT NULL,
    driver_id TEXT NOT NULL,
    connection_fingerprint TEXT NOT NULL,
    resource_kind TEXT NOT NULL,
    resource_name TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_st_schema_cache_key
    ON st_schema_cache(cache_key);
CREATE INDEX IF NOT EXISTS idx_st_schema_cache_fingerprint
    ON st_schema_cache(connection_fingerprint);
CREATE INDEX IF NOT EXISTS idx_st_schema_cache_expires
    ON st_schema_cache(expires_at);

-- Event log
CREATE TABLE IF NOT EXISTS st_event_log (
    id TEXT PRIMARY KEY,
    event_kind TEXT NOT NULL,
    description TEXT NOT NULL,
    target_kind TEXT,
    target_id TEXT,
    details_json TEXT,
    actor_id TEXT,
    tool_id TEXT,
    decision TEXT,
    duration_ms INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_st_event_log_kind
    ON st_event_log(event_kind);
CREATE INDEX IF NOT EXISTS idx_st_event_log_created
    ON st_event_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_st_event_log_actor_created
    ON st_event_log(actor_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_st_event_log_tool_created
    ON st_event_log(tool_id, created_at DESC);

-- ============================================================================
-- AUDIT DOMAIN (aud_*) - Audit events with enriched schema
-- ============================================================================

-- Audit events
CREATE TABLE IF NOT EXISTS aud_audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id TEXT NOT NULL,
    tool_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT,
    profile_id TEXT,
    classification TEXT,
    duration_ms INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    created_at_epoch_ms INTEGER NOT NULL,
    FOREIGN KEY (profile_id) REFERENCES cfg_connection_profiles(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_aud_audit_events_actor
    ON aud_audit_events(actor_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_aud_audit_events_tool
    ON aud_audit_events(tool_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_aud_audit_events_profile
    ON aud_audit_events(profile_id);
CREATE INDEX IF NOT EXISTS idx_aud_audit_events_decision
    ON aud_audit_events(decision);
CREATE INDEX IF NOT EXISTS idx_aud_audit_events_created
    ON aud_audit_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_aud_audit_events_created_epoch
    ON aud_audit_events(created_at_epoch_ms DESC);

-- Audit event entities (affected objects)
CREATE TABLE IF NOT EXISTS aud_audit_event_entities (
    id TEXT PRIMARY KEY,
    audit_event_id INTEGER NOT NULL,
    entity_type TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    entity_id TEXT,
    FOREIGN KEY (audit_event_id) REFERENCES aud_audit_events(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_aud_audit_event_entities_event
    ON aud_audit_event_entities(audit_event_id);
CREATE INDEX IF NOT EXISTS idx_aud_audit_event_entities_type
    ON aud_audit_event_entities(entity_type);

-- Audit event attributes (additional context)
CREATE TABLE IF NOT EXISTS aud_audit_event_attributes (
    id TEXT PRIMARY KEY,
    audit_event_id INTEGER NOT NULL,
    attr_key TEXT NOT NULL,
    attr_value TEXT,
    attr_value_number REAL,
    attr_value_bool INTEGER,
    attr_value_kind TEXT NOT NULL,
    FOREIGN KEY (audit_event_id) REFERENCES aud_audit_events(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_aud_audit_event_attributes_event
    ON aud_audit_event_attributes(audit_event_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_aud_audit_event_attributes_event_key
    ON aud_audit_event_attributes(audit_event_id, attr_key);
"#;
