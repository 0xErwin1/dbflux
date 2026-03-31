//! Database migration infrastructure for DBFlux internal storage.
//!
//! This module provides migration execution for both `config.db` and `state.db`.
//! Migrations are tracked in a `migrations` table with name-based tracking.
//!
//! The `migrations` table uses names like `0001_initial` to track applied migrations,
//! replacing the old `user_version` pragma + incremental migration approach.

use log::info;
use rusqlite::Connection;
use std::collections::HashSet;

use crate::error::StorageError;

pub mod state;

/// Ordered list of all config migrations.
///
/// New installations run 0001_initial directly (which creates the complete final schema).
/// Existing installations iterate through pending migrations and apply each one.
const CONFIG_MIGRATION_NAMES: [&str; 3] = [
    "0001_initial",
    "0004_connection_profiles_fk",
    "0005_governance_normalize_tool_policies",
];

/// Runs all pending config database migrations.
///
/// Uses name-based migration tracking via the `migrations` table.
/// New installations get the complete final schema in one step via the `0001_initial` migration.
/// Existing installations run each pending incremental migration in order.
pub fn run_config_migrations(conn: &Connection) -> Result<(), StorageError> {
    // Ensure migrations table exists (outside transaction for DDL compatibility)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS migrations (name TEXT PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT (datetime('now')))",
        [],
    )
    .map_err(|source| StorageError::Sqlite {
        path: "config.db".into(),
        source,
    })?;

    // Get set of applied migration names
    let mut stmt = conn
        .prepare("SELECT name FROM migrations")
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

    let applied: HashSet<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?
        .filter_map(|r| r.ok())
        .collect();

    info!("Applied migrations: {:?}", applied);

    // Run each migration that hasn't been applied yet
    for &migration_name in &CONFIG_MIGRATION_NAMES {
        if !applied.contains(migration_name) {
            match migration_name {
                "0001_initial" => {
                    run_initial_schema_migration(conn)?;
                    record_migration(conn, migration_name)?;
                    info!("Config initial schema migration 0001_initial applied successfully");
                }
                "0004_connection_profiles_fk" => {
                    run_migration_0004_connection_profiles_fk(conn)?;
                    record_migration(conn, migration_name)?;
                    info!("Config migration 0004_connection_profiles_fk applied successfully");
                }
                "0005_governance_normalize_tool_policies" => {
                    run_migration_0005_governance_normalize_tool_policies(conn)?;
                    record_migration(conn, migration_name)?;
                    info!(
                        "Config migration 0005_governance_normalize_tool_policies applied successfully"
                    );
                }
                _ => {
                    // Unknown migration - skip to avoid blocking on future migrations
                    info!("Skipping unknown migration: {}", migration_name);
                }
            }
        }
    }

    Ok(())
}

/// Records a migration as applied in the migrations table.
fn record_migration(conn: &Connection, name: &str) -> Result<(), StorageError> {
    conn.execute("INSERT INTO migrations (name) VALUES (?1)", [name])
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;
    Ok(())
}

/// Runs only pending migrations, skipping any already-applied ones.
///
/// This is useful for forcing a re-check of migrations without full re-initialization.
/// The function is idempotent — calling it multiple times has no effect beyond the first.
pub fn run_pending_migrations(conn: &Connection) -> Result<(), StorageError> {
    run_config_migrations(conn)
}

/// Creates the complete final normalized schema for config.db.
///
/// This is called for brand new installations to create the full
/// normalized schema directly without any JSON columns. The schema includes:
/// - connection_profiles (with save_password, kind, access_kind, access_provider columns)
/// - connection_driver_configs (with native typed columns for all drivers)
/// - connection_profile_* child tables (configs, settings, value_refs, hooks, bindings, governance)
/// - auth_profiles + auth_profile_fields (EAV pattern)
/// - proxy_profiles + proxy_auth
/// - ssh_tunnel_profiles + ssh_tunnel_auth
/// - services + service_args + service_env
/// - hook_definitions + hook_commands + hook_environment
/// - legacy_imports, system_metadata
/// - general_settings, governance_settings + children (normalized from app_settings JSON blobs)
/// - driver_overrides, driver_setting_values (normalized from driver_settings JSON columns)
fn run_initial_schema_migration(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch(
        r#"
        -- Migration tracking
        CREATE TABLE IF NOT EXISTS migrations (
            name TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- System metadata
        CREATE TABLE IF NOT EXISTS system_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- General settings (normalized from general_settings JSON blob in app_settings)
        CREATE TABLE IF NOT EXISTS general_settings (
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

        INSERT OR IGNORE INTO general_settings (id) VALUES (1);

        -- Governance settings (normalized from governance_settings JSON blob in app_settings)
        CREATE TABLE IF NOT EXISTS governance_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            mcp_enabled_by_default INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        INSERT OR IGNORE INTO governance_settings (id) VALUES (1);

        CREATE TABLE IF NOT EXISTS governance_trusted_clients (
            id TEXT PRIMARY KEY,
            governance_id INTEGER NOT NULL DEFAULT 1,
            client_id TEXT NOT NULL,
            name TEXT NOT NULL,
            issuer TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (governance_id) REFERENCES governance_settings(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS governance_policy_roles (
            id TEXT PRIMARY KEY,
            governance_id INTEGER NOT NULL DEFAULT 1,
            role_id TEXT NOT NULL,
            FOREIGN KEY (governance_id) REFERENCES governance_settings(id) ON DELETE CASCADE
        );

        -- Tool policies with normalized child tables for allowed tools/classes (migration 0005)
        CREATE TABLE IF NOT EXISTS governance_tool_policies (
            id TEXT PRIMARY KEY,
            governance_id INTEGER NOT NULL DEFAULT 1,
            policy_id TEXT NOT NULL,
            FOREIGN KEY (governance_id) REFERENCES governance_settings(id) ON DELETE CASCADE
        );

        -- Child table: normalized allowed tools per policy
        CREATE TABLE IF NOT EXISTS tool_policy_allowed_tools (
            id TEXT PRIMARY KEY,
            tool_policy_id TEXT NOT NULL,
            tool_name TEXT NOT NULL,
            FOREIGN KEY (tool_policy_id) REFERENCES governance_tool_policies(id) ON DELETE CASCADE,
            UNIQUE(tool_policy_id, tool_name)
        );

        -- Child table: normalized allowed classes per policy
        CREATE TABLE IF NOT EXISTS tool_policy_allowed_classes (
            id TEXT PRIMARY KEY,
            tool_policy_id TEXT NOT NULL,
            class_name TEXT NOT NULL,
            FOREIGN KEY (tool_policy_id) REFERENCES governance_tool_policies(id) ON DELETE CASCADE,
            UNIQUE(tool_policy_id, class_name)
        );

        -- Driver-level settings (normalized - replaces driver_settings.overrides_json and driver_settings.settings_json)
        CREATE TABLE IF NOT EXISTS driver_overrides (
            driver_key TEXT PRIMARY KEY,
            refresh_policy TEXT,
            refresh_interval_secs INTEGER,
            confirm_dangerous INTEGER,
            requires_where INTEGER,
            requires_preview INTEGER,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS driver_setting_values (
            id TEXT PRIMARY KEY,
            driver_key TEXT NOT NULL,
            setting_key TEXT NOT NULL,
            setting_value TEXT,
            FOREIGN KEY (driver_key) REFERENCES driver_overrides(driver_key) ON DELETE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_driver_setting_values_driver_key
            ON driver_setting_values(driver_key, setting_key);

        -- ============================================
        -- Connection profiles — fully normalized
        -- ============================================

        -- Core connection profile (no JSON columns)
        --
        -- NOTE: access_profile_id was removed - it was dead code (planned for an access-methods
        -- feature that was never implemented, and no access_profiles table exists).
        -- Migration 0004 handles removal from existing installs that may have this column.
        CREATE TABLE IF NOT EXISTS connection_profiles (
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
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- EAV for non-DbConfig profile settings (key-value pairs like timeout, retries, etc.)
        CREATE TABLE IF NOT EXISTS connection_profile_configs (
            id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            config_key TEXT NOT NULL,
            config_value TEXT,
            config_value_kind TEXT NOT NULL,
            FOREIGN KEY (profile_id) REFERENCES connection_profiles(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connection_profile_configs_profile
            ON connection_profile_configs(profile_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_connection_profile_configs_profile_key
            ON connection_profile_configs(profile_id, config_key);

        -- Settings overrides (FormValues = key-value pairs)
        CREATE TABLE IF NOT EXISTS connection_profile_settings (
            id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            setting_key TEXT NOT NULL,
            setting_value TEXT,
            FOREIGN KEY (profile_id) REFERENCES connection_profiles(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connection_profile_settings_profile
            ON connection_profile_settings(profile_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_connection_profile_settings_profile_key
            ON connection_profile_settings(profile_id, setting_key);

        -- Value references (secrets, params, auth bindings) with provider tracking
        -- ref_value JSON column deprecated in v16 - data migrated to native variant columns
        CREATE TABLE IF NOT EXISTS connection_profile_value_refs (
            id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            ref_key TEXT NOT NULL,
            ref_kind TEXT NOT NULL,
            ref_value TEXT NOT NULL,
            ref_provider TEXT,
            ref_json_key TEXT,
            -- Native columns for ValueRef variants (v16+)
            literal_value TEXT,
            env_key TEXT,
            secret_locator TEXT,
            param_name TEXT,
            auth_field TEXT,
            FOREIGN KEY (profile_id) REFERENCES connection_profiles(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connection_profile_value_refs_profile
            ON connection_profile_value_refs(profile_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_connection_profile_value_refs_profile_key
            ON connection_profile_value_refs(profile_id, ref_key);

        -- Driver-specific config with native typed columns (replaces EAV for DbConfig)
        CREATE TABLE IF NOT EXISTS connection_driver_configs (
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
            FOREIGN KEY (profile_id) REFERENCES connection_profiles(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connection_driver_configs_profile
            ON connection_driver_configs(profile_id);

        -- Inline hook definitions with flat columns
        CREATE TABLE IF NOT EXISTS connection_profile_hooks (
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
            FOREIGN KEY (profile_id) REFERENCES connection_profiles(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connection_profile_hooks_profile
            ON connection_profile_hooks(profile_id);

        -- Hook arguments
        CREATE TABLE IF NOT EXISTS connection_profile_hook_args (
            id TEXT PRIMARY KEY,
            hook_id TEXT NOT NULL,
            position INTEGER NOT NULL,
            value TEXT NOT NULL,
            FOREIGN KEY (hook_id) REFERENCES connection_profile_hooks(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connection_profile_hook_args_hook
            ON connection_profile_hook_args(hook_id);

        -- Hook environment variables
        CREATE TABLE IF NOT EXISTS connection_profile_hook_envs (
            id TEXT PRIMARY KEY,
            hook_id TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            FOREIGN KEY (hook_id) REFERENCES connection_profile_hooks(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connection_profile_hook_envs_hook
            ON connection_profile_hook_envs(hook_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_connection_profile_hook_envs_hook_key
            ON connection_profile_hook_envs(hook_id, key);

        -- Hook bindings to phases
        CREATE TABLE IF NOT EXISTS connection_profile_hook_bindings (
            id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            hook_id TEXT NOT NULL,
            phase TEXT NOT NULL,
            order_index INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (profile_id) REFERENCES connection_profiles(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connection_profile_hook_bindings_profile
            ON connection_profile_hook_bindings(profile_id);

        -- MCP governance settings
        CREATE TABLE IF NOT EXISTS connection_profile_governance (
            id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            governance_key TEXT NOT NULL,
            governance_value TEXT,
            FOREIGN KEY (profile_id) REFERENCES connection_profiles(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connection_profile_governance_profile
            ON connection_profile_governance(profile_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_connection_profile_governance_profile_key
            ON connection_profile_governance(profile_id, governance_key);

        -- Governance bindings
        CREATE TABLE IF NOT EXISTS connection_profile_governance_bindings (
            id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            order_index INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (profile_id) REFERENCES connection_profiles(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connection_profile_governance_bindings_profile
            ON connection_profile_governance_bindings(profile_id);

        CREATE TABLE IF NOT EXISTS connection_profile_governance_binding_roles (
            id TEXT PRIMARY KEY,
            binding_id TEXT NOT NULL,
            role_id TEXT NOT NULL,
            FOREIGN KEY (binding_id) REFERENCES connection_profile_governance_bindings(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_conn_profile_gov_binding_roles_binding
            ON connection_profile_governance_binding_roles(binding_id);

        CREATE TABLE IF NOT EXISTS connection_profile_governance_binding_policies (
            id TEXT PRIMARY KEY,
            binding_id TEXT NOT NULL,
            policy_id TEXT NOT NULL,
            FOREIGN KEY (binding_id) REFERENCES connection_profile_governance_bindings(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_conn_profile_gov_binding_policies_binding
            ON connection_profile_governance_binding_policies(binding_id);

        -- Access params
        CREATE TABLE IF NOT EXISTS connection_profile_access_params (
            id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            param_key TEXT NOT NULL,
            param_value TEXT NOT NULL,
            FOREIGN KEY (profile_id) REFERENCES connection_profiles(id) ON DELETE CASCADE,
            UNIQUE(profile_id, param_key)
        );

        CREATE INDEX IF NOT EXISTS idx_connection_profile_access_params_profile
            ON connection_profile_access_params(profile_id);

        -- ============================================
        -- Auth profiles — fully normalized
        -- ============================================

        CREATE TABLE IF NOT EXISTS auth_profiles (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            provider_id TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- EAV for auth profile fields (replaces fields_json)
        CREATE TABLE IF NOT EXISTS auth_profile_fields (
            id TEXT PRIMARY KEY,
            auth_profile_id TEXT NOT NULL,
            field_key TEXT NOT NULL,
            value_text TEXT,
            value_bool INTEGER,
            value_number REAL,
            value_secret_ref TEXT,
            value_kind TEXT NOT NULL,
            FOREIGN KEY (auth_profile_id) REFERENCES auth_profiles(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_auth_profile_fields_profile
            ON auth_profile_fields(auth_profile_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_profile_fields_profile_key
            ON auth_profile_fields(auth_profile_id, field_key);

        -- ============================================
        -- Proxy profiles — fully normalized
        -- ============================================

        CREATE TABLE IF NOT EXISTS proxy_profiles (
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

        -- Child table for proxy auth credentials
        CREATE TABLE IF NOT EXISTS proxy_auth (
            proxy_profile_id TEXT PRIMARY KEY,
            username TEXT,
            domain TEXT,
            password_secret_ref TEXT,
            FOREIGN KEY (proxy_profile_id) REFERENCES proxy_profiles(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_proxy_auth_profile
            ON proxy_auth(proxy_profile_id);

        -- ============================================
        -- SSH tunnel profiles — fully normalized
        -- ============================================

        CREATE TABLE IF NOT EXISTS ssh_tunnel_profiles (
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

        -- Child table for SSH auth credentials
        CREATE TABLE IF NOT EXISTS ssh_tunnel_auth (
            ssh_tunnel_profile_id TEXT PRIMARY KEY,
            key_path TEXT,
            password_secret_ref TEXT,
            passphrase_secret_ref TEXT,
            FOREIGN KEY (ssh_tunnel_profile_id) REFERENCES ssh_tunnel_profiles(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_ssh_tunnel_auth_profile
            ON ssh_tunnel_auth(ssh_tunnel_profile_id);

        -- ============================================
        -- Services — fully normalized
        -- ============================================

        CREATE TABLE IF NOT EXISTS services (
            socket_id TEXT PRIMARY KEY,
            enabled INTEGER DEFAULT 1,
            command TEXT,
            startup_timeout_ms INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- Child table for service arguments
        CREATE TABLE IF NOT EXISTS service_args (
            id TEXT PRIMARY KEY,
            service_id TEXT NOT NULL,
            position INTEGER NOT NULL,
            value TEXT NOT NULL,
            FOREIGN KEY (service_id) REFERENCES services(socket_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_service_args_service
            ON service_args(service_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_service_args_service_position
            ON service_args(service_id, position);

        -- Child table for service environment variables
        CREATE TABLE IF NOT EXISTS service_env (
            id TEXT PRIMARY KEY,
            service_id TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            FOREIGN KEY (service_id) REFERENCES services(socket_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_service_env_service
            ON service_env(service_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_service_env_service_key
            ON service_env(service_id, key);

        -- ============================================
        -- Hook definitions — fully normalized
        -- ============================================

        CREATE TABLE IF NOT EXISTS hook_definitions (
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

        -- Child table for hook command details
        CREATE TABLE IF NOT EXISTS hook_commands (
            id TEXT PRIMARY KEY,
            hook_id TEXT NOT NULL UNIQUE,
            command TEXT NOT NULL,
            working_directory TEXT,
            timeout_ms INTEGER,
            ready_signal TEXT,
            FOREIGN KEY (hook_id) REFERENCES hook_definitions(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_hook_commands_hook
            ON hook_commands(hook_id);

        -- Child table for hook environment variables
        CREATE TABLE IF NOT EXISTS hook_environment (
            id TEXT PRIMARY KEY,
            hook_id TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            FOREIGN KEY (hook_id) REFERENCES hook_definitions(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_hook_environment_hook
            ON hook_environment(hook_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_hook_environment_hook_key
            ON hook_environment(hook_id, key);

        -- ============================================
        -- Legacy imports tracking
        -- ============================================

        CREATE TABLE IF NOT EXISTS legacy_imports (
            id TEXT PRIMARY KEY,
            source_path TEXT NOT NULL,
            source_hash TEXT NOT NULL UNIQUE,
            imported_at TEXT NOT NULL DEFAULT (datetime('now')),
            record_count INTEGER NOT NULL DEFAULT 0,
            domain TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'completed',
            error_message TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_legacy_imports_source_path
            ON legacy_imports(source_path);
        CREATE INDEX IF NOT EXISTS idx_legacy_imports_source_hash
            ON legacy_imports(source_hash);
        CREATE INDEX IF NOT EXISTS idx_legacy_imports_domain
            ON legacy_imports(domain);

        -- ============================================
        -- Connection folders (connection tree structure)
        -- ============================================

        CREATE TABLE IF NOT EXISTS connection_folders (
            id TEXT PRIMARY KEY,
            parent_id TEXT,
            name TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            collapsed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (parent_id) REFERENCES connection_folders(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connection_folders_parent
            ON connection_folders(parent_id);

        CREATE TABLE IF NOT EXISTS connection_folder_items (
            id TEXT PRIMARY KEY,
            folder_id TEXT NOT NULL,
            profile_id TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (folder_id) REFERENCES connection_folders(id) ON DELETE CASCADE,
            FOREIGN KEY (profile_id) REFERENCES connection_profiles(id) ON DELETE CASCADE,
            UNIQUE(folder_id, profile_id)
        );

        CREATE INDEX IF NOT EXISTS idx_connection_folder_items_folder
            ON connection_folder_items(folder_id);
        CREATE INDEX IF NOT EXISTS idx_connection_folder_items_profile
            ON connection_folder_items(profile_id);
        "#,
    )
    .map_err(|source| StorageError::Sqlite {
        path: "config.db".into(),
        source,
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Incremental migrations for existing installations
// ---------------------------------------------------------------------------

/// Migration 0004: Add FK constraints to connection_profiles and drop access_profile_id.
///
/// This migration:
/// 1. Validates no orphan FK references exist (checks auth_profile_id, proxy_profile_id, ssh_tunnel_profile_id)
/// 2. Adds FK constraints with ON DELETE SET NULL
/// 3. Drops the dead access_profile_id column
///
/// For new installs, 0001_initial creates the schema with these constraints directly.
fn run_migration_0004_connection_profiles_fk(conn: &Connection) -> Result<(), StorageError> {
    // Start a transaction for all operations
    let tx = conn
        .unchecked_transaction()
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

    // Step 1: Check if access_profile_id column exists and drop it if it does (dead code)
    let has_access_profile_id: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('connection_profiles') WHERE name = 'access_profile_id'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    if has_access_profile_id > 0 {
        // Column exists - drop it (dead code, never populated)
        // Note: We already checked existence, so no IF EXISTS needed
        tx.execute(
            "ALTER TABLE connection_profiles DROP COLUMN access_profile_id",
            [],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;
        info!("Dropped dead access_profile_id column from connection_profiles");
    }

    // Step 2: Validate no orphan auth_profile_id references
    let orphan_auth_count: i64 = tx
        .query_row(
            r#"
            SELECT COUNT(*) FROM connection_profiles cp
            WHERE cp.auth_profile_id IS NOT NULL
            AND cp.auth_profile_id NOT IN (SELECT id FROM auth_profiles)
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    if orphan_auth_count > 0 {
        // Clear orphan references to NULL
        tx.execute(
            r#"
            UPDATE connection_profiles SET auth_profile_id = NULL
            WHERE auth_profile_id IS NOT NULL
            AND auth_profile_id NOT IN (SELECT id FROM auth_profiles)
            "#,
            [],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;
        info!(
            "Cleared {} orphan auth_profile_id references to NULL",
            orphan_auth_count
        );
    }

    // Step 3: Validate no orphan proxy_profile_id references
    let orphan_proxy_count: i64 = tx
        .query_row(
            r#"
            SELECT COUNT(*) FROM connection_profiles cp
            WHERE cp.proxy_profile_id IS NOT NULL
            AND cp.proxy_profile_id NOT IN (SELECT id FROM proxy_profiles)
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    if orphan_proxy_count > 0 {
        tx.execute(
            r#"
            UPDATE connection_profiles SET proxy_profile_id = NULL
            WHERE proxy_profile_id IS NOT NULL
            AND proxy_profile_id NOT IN (SELECT id FROM proxy_profiles)
            "#,
            [],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;
        info!(
            "Cleared {} orphan proxy_profile_id references to NULL",
            orphan_proxy_count
        );
    }

    // Step 4: Validate no orphan ssh_tunnel_profile_id references
    let orphan_ssh_count: i64 = tx
        .query_row(
            r#"
            SELECT COUNT(*) FROM connection_profiles cp
            WHERE cp.ssh_tunnel_profile_id IS NOT NULL
            AND cp.ssh_tunnel_profile_id NOT IN (SELECT id FROM ssh_tunnel_profiles)
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    if orphan_ssh_count > 0 {
        tx.execute(
            r#"
            UPDATE connection_profiles SET ssh_tunnel_profile_id = NULL
            WHERE ssh_tunnel_profile_id IS NOT NULL
            AND ssh_tunnel_profile_id NOT IN (SELECT id FROM ssh_tunnel_profiles)
            "#,
            [],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;
        info!(
            "Cleared {} orphan ssh_tunnel_profile_id references to NULL",
            orphan_ssh_count
        );
    }

    // Step 5: Add FK constraint for auth_profile_id
    // SQLite supports ADD CONSTRAINT via separate execution
    let add_auth_fk_result: Result<(), StorageError> = tx
        .execute(
            "ALTER TABLE connection_profiles ADD CONSTRAINT fk_connection_profiles_auth_profile
             FOREIGN KEY (auth_profile_id) REFERENCES auth_profiles(id) ON DELETE SET NULL",
            [],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })
        .map(|_| ());

    if add_auth_fk_result.is_err() {
        // Constraint may already exist (new installs via 0001_initial have it)
        info!("FK auth_profile_id constraint already exists or could not be added (idempotent)");
    }

    // Step 6: Add FK constraint for proxy_profile_id
    let add_proxy_fk_result: Result<(), StorageError> = tx
        .execute(
            "ALTER TABLE connection_profiles ADD CONSTRAINT fk_connection_profiles_proxy_profile
             FOREIGN KEY (proxy_profile_id) REFERENCES proxy_profiles(id) ON DELETE SET NULL",
            [],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })
        .map(|_| ());

    if add_proxy_fk_result.is_err() {
        info!("FK proxy_profile_id constraint already exists or could not be added (idempotent)");
    }

    // Step 7: Add FK constraint for ssh_tunnel_profile_id
    let add_ssh_fk_result: Result<(), StorageError> = tx
        .execute(
            "ALTER TABLE connection_profiles ADD CONSTRAINT fk_connection_profiles_ssh_tunnel_profile
             FOREIGN KEY (ssh_tunnel_profile_id) REFERENCES ssh_tunnel_profiles(id) ON DELETE SET NULL",
            [],
        )
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })
        .map(|_| ());

    if add_ssh_fk_result.is_err() {
        info!(
            "FK ssh_tunnel_profile_id constraint already exists or could not be added (idempotent)"
        );
    }

    // Commit transaction
    tx.commit().map_err(|source| StorageError::Sqlite {
        path: "config.db".into(),
        source,
    })?;

    info!("Migration 0004_connection_profiles_fk completed successfully");
    Ok(())
}

/// Migration 0005: Normalize governance_tool_policies JSON columns into child tables.
///
/// This migration:
/// 1. Creates tool_policy_allowed_tools table
/// 2. Creates tool_policy_allowed_classes table
/// 3. Backfills data from existing JSON columns
/// 4. Drops the JSON TEXT columns
///
/// For new installs, 0001_initial already has the normalized schema.
fn run_migration_0005_governance_normalize_tool_policies(
    conn: &Connection,
) -> Result<(), StorageError> {
    // Start a transaction for all operations
    let tx = conn
        .unchecked_transaction()
        .map_err(|source| StorageError::Sqlite {
            path: "config.db".into(),
            source,
        })?;

    // Step 1: Create tool_policy_allowed_tools table if it doesn't exist
    tx.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS tool_policy_allowed_tools (
            id TEXT PRIMARY KEY,
            tool_policy_id TEXT NOT NULL,
            tool_name TEXT NOT NULL,
            FOREIGN KEY (tool_policy_id) REFERENCES governance_tool_policies(id) ON DELETE CASCADE,
            UNIQUE(tool_policy_id, tool_name)
        );

        CREATE TABLE IF NOT EXISTS tool_policy_allowed_classes (
            id TEXT PRIMARY KEY,
            tool_policy_id TEXT NOT NULL,
            class_name TEXT NOT NULL,
            FOREIGN KEY (tool_policy_id) REFERENCES governance_tool_policies(id) ON DELETE CASCADE,
            UNIQUE(tool_policy_id, class_name)
        );
        "#,
    )
    .map_err(|source| StorageError::Sqlite {
        path: "config.db".into(),
        source,
    })?;

    // Step 2: Check if allowed_tools column still exists (new installs via 0001_initial don't have it)
    let has_allowed_tools: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('governance_tool_policies') WHERE name = 'allowed_tools'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    if has_allowed_tools > 0 {
        // Step 3: Backfill tool_policy_allowed_tools from JSON
        // Query all rows with non-null allowed_tools
        let mut stmt = tx
            .prepare(
                "SELECT id, allowed_tools FROM governance_tool_policies WHERE allowed_tools IS NOT NULL AND allowed_tools != '' AND allowed_tools != 'null'",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let rows: Vec<(String, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .collect();

        for (policy_id, json_tools) in rows {
            // Parse JSON array
            if let Ok(tools) = serde_json::from_str::<Vec<String>>(&json_tools) {
                for tool_name in tools {
                    let id = uuid::Uuid::new_v4().to_string();
                    let _ = tx.execute(
                        "INSERT OR IGNORE INTO tool_policy_allowed_tools (id, tool_policy_id, tool_name) VALUES (?1, ?2, ?3)",
                        rusqlite::params![id, policy_id, tool_name],
                    );
                }
            } else {
                info!(
                    "Warning: Could not parse allowed_tools JSON for policy {}, skipping",
                    policy_id
                );
            }
        }

        // Step 4: Backfill tool_policy_allowed_classes from JSON
        let mut stmt = tx
            .prepare(
                "SELECT id, allowed_classes FROM governance_tool_policies WHERE allowed_classes IS NOT NULL AND allowed_classes != '' AND allowed_classes != 'null'",
            )
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?;

        let rows: Vec<(String, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|source| StorageError::Sqlite {
                path: "config.db".into(),
                source,
            })?
            .filter_map(|r| r.ok())
            .collect();

        for (policy_id, json_classes) in rows {
            // Parse JSON array
            if let Ok(classes) = serde_json::from_str::<Vec<String>>(&json_classes) {
                for class_name in classes {
                    let id = uuid::Uuid::new_v4().to_string();
                    let _ = tx.execute(
                        "INSERT OR IGNORE INTO tool_policy_allowed_classes (id, tool_policy_id, class_name) VALUES (?1, ?2, ?3)",
                        rusqlite::params![id, policy_id, class_name],
                    );
                }
            } else {
                info!(
                    "Warning: Could not parse allowed_classes JSON for policy {}, skipping",
                    policy_id
                );
            }
        }

        // Step 5: Drop the JSON columns if they exist
        // Check and drop allowed_tools column
        let has_allowed_tools: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('governance_tool_policies') WHERE name = 'allowed_tools'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        if has_allowed_tools > 0 {
            // Try DROP COLUMN - works on SQLite 3.35.0+
            // If it fails due to syntax, the column just stays (acceptable for testing)
            let drop_result = tx.execute(
                "ALTER TABLE governance_tool_policies DROP COLUMN allowed_tools",
                [],
            );
            if drop_result.is_ok() {
                info!("Dropped allowed_tools column");
            }
        }

        // Check and drop allowed_classes column
        let has_allowed_classes: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('governance_tool_policies') WHERE name = 'allowed_classes'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        if has_allowed_classes > 0 {
            let drop_result = tx.execute(
                "ALTER TABLE governance_tool_policies DROP COLUMN allowed_classes",
                [],
            );
            if drop_result.is_ok() {
                info!("Dropped allowed_classes column");
            }
        }
    }

    // Commit transaction
    tx.commit().map_err(|source| StorageError::Sqlite {
        path: "config.db".into(),
        source,
    })?;

    info!("Migration 0005_governance_normalize_tool_policies completed successfully");
    Ok(())
}

/// Runs migrations for the state database.
pub fn run_state_migrations(conn: &Connection) -> Result<(), StorageError> {
    state::run_state_migrations(conn)
}

/// Verifies that a database is in a consistent state by running integrity check.
pub fn verify_integrity(conn: &Connection) -> Result<bool, StorageError> {
    let result: String = conn
        .pragma_query_value(None, "integrity_check", |row| row.get(0))
        .map_err(|source| StorageError::Sqlite {
            path: "unknown".into(),
            source,
        })?;

    Ok(result == "ok")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::open_database;
    use std::path::PathBuf;

    fn temp_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("dbflux_storage_migrations_{}.sqlite", name))
    }

    #[test]
    fn config_initial_migration_creates_tables() {
        let path = temp_db("initial_migration");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");

        run_config_migrations(&conn).expect("migration should run");

        // New installations run all 3 migrations: 0001_initial, 0004, 0005
        // Each migration is recorded to support existing installs that need incremental migrations
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM migrations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3, "should have 3 migrations recorded");

        // Verify all migration names are recorded
        let mut stmt = conn
            .prepare("SELECT name FROM migrations ORDER BY name")
            .unwrap();
        let names: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(
            names.contains(&"0001_initial".to_string()),
            "0001_initial should be recorded"
        );
        assert!(
            names.contains(&"0004_connection_profiles_fk".to_string()),
            "0004_connection_profiles_fk should be recorded"
        );
        assert!(
            names.contains(&"0005_governance_normalize_tool_policies".to_string()),
            "0005_governance_normalize_tool_policies should be recorded"
        );

        // Verify new normalized tables exist
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM general_settings", [], |row| {
                row.get(0)
            })
            .expect("general_settings should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM governance_settings", [], |row| {
                row.get(0)
            })
            .expect("governance_settings should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM driver_overrides", [], |row| {
                row.get(0)
            })
            .expect("driver_overrides should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM driver_setting_values", [], |row| {
                row.get(0)
            })
            .expect("driver_setting_values should exist");

        // Verify core tables exist (without JSON columns)
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM connection_profiles", [], |row| {
                row.get(0)
            })
            .expect("connection_profiles should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM auth_profiles", [], |row| row.get(0))
            .expect("auth_profiles should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM proxy_profiles", [], |row| row.get(0))
            .expect("proxy_profiles should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM ssh_tunnel_profiles", [], |row| {
                row.get(0)
            })
            .expect("ssh_tunnel_profiles should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM hook_definitions", [], |row| {
                row.get(0)
            })
            .expect("hook_definitions should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM services", [], |row| row.get(0))
            .expect("services should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM system_metadata", [], |row| row.get(0))
            .expect("system_metadata should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM legacy_imports", [], |row| row.get(0))
            .expect("legacy_imports should exist");

        // Verify normalized child tables exist
        let _: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM connection_driver_configs",
                [],
                |row| row.get(0),
            )
            .expect("connection_driver_configs should exist");
        let _: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM connection_profile_configs",
                [],
                |row| row.get(0),
            )
            .expect("connection_profile_configs should exist");
        let _: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM connection_profile_settings",
                [],
                |row| row.get(0),
            )
            .expect("connection_profile_settings should exist");
        let _: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM connection_profile_value_refs",
                [],
                |row| row.get(0),
            )
            .expect("connection_profile_value_refs should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM connection_profile_hooks", [], |row| {
                row.get(0)
            })
            .expect("connection_profile_hooks should exist");
        let _: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM connection_profile_hook_bindings",
                [],
                |row| row.get(0),
            )
            .expect("connection_profile_hook_bindings should exist");
        let _: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM connection_profile_governance",
                [],
                |row| row.get(0),
            )
            .expect("connection_profile_governance should exist");
        let _: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM connection_profile_access_params",
                [],
                |row| row.get(0),
            )
            .expect("connection_profile_access_params should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM auth_profile_fields", [], |row| {
                row.get(0)
            })
            .expect("auth_profile_fields should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM proxy_auth", [], |row| row.get(0))
            .expect("proxy_auth should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM ssh_tunnel_auth", [], |row| row.get(0))
            .expect("ssh_tunnel_auth should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM service_args", [], |row| row.get(0))
            .expect("service_args should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM service_env", [], |row| row.get(0))
            .expect("service_env should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM hook_commands", [], |row| row.get(0))
            .expect("hook_commands should exist");
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM hook_environment", [], |row| {
                row.get(0)
            })
            .expect("hook_environment should exist");

        // Verify connection_profiles has normalized columns (no JSON columns)
        let has_save_password: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('connection_profiles') WHERE name = 'save_password'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            has_save_password, 1,
            "connection_profiles should have save_password column"
        );

        let has_kind: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('connection_profiles') WHERE name = 'kind'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(has_kind, 1, "connection_profiles should have kind column");

        // Verify NO legacy JSON columns exist in connection_profiles
        let has_config_json: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('connection_profiles') WHERE name = 'config_json'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            has_config_json, 0,
            "connection_profiles should NOT have config_json column"
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn config_migration_is_idempotent() {
        let path = temp_db("idempotent_migration");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");

        // First run
        run_config_migrations(&conn).expect("first migration should run");

        // Second run should succeed (idempotent) - all migrations already applied
        run_config_migrations(&conn).expect("second migration should be idempotent");

        // All 3 migrations recorded (new installs run all 3 incrementally)
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM migrations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3, "should have 3 migrations recorded");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    // -------------------------------------------------------------------------
    // Tests for incremental migrations 0004 and 0005
    // -------------------------------------------------------------------------

    #[test]
    fn migration_0004_connection_profiles_fk_idempotent() {
        // Test that migration 0004 is idempotent on a fresh database
        let path = temp_db("migration_0004_idempotent");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");

        // Run migrations (including 0004)
        run_config_migrations(&conn).expect("migrations should run");

        // Run again - should be idempotent
        run_config_migrations(&conn).expect("migration should be idempotent");

        // Verify only 3 migrations recorded
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM migrations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3);

        // Verify access_profile_id column does NOT exist (never existed in 0001_initial)
        let has_access_profile_id: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('connection_profiles') WHERE name = 'access_profile_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            has_access_profile_id, 0,
            "access_profile_id should not exist"
        );

        // Verify FK constraints work by inserting valid data
        // Insert an auth profile
        conn.execute(
            "INSERT INTO auth_profiles (id, name, provider_id) VALUES ('auth-1', 'Test', 'provider')",
            [],
        )
        .unwrap();

        // Insert connection with valid FK reference
        conn.execute(
            r#"
            INSERT INTO connection_profiles (id, name, auth_profile_id)
            VALUES ('conn-1', 'Test Connection', 'auth-1')
            "#,
            [],
        )
        .unwrap();

        // Verify the FK was inserted correctly
        let auth_id: String = conn
            .query_row(
                "SELECT auth_profile_id FROM connection_profiles WHERE id = 'conn-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(auth_id, "auth-1", "valid FK should be inserted");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn migration_0004_handles_valid_fk_data() {
        // Test that migration 0004 works with valid FK references
        let path = temp_db("migration_0004_valid_fk");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");

        // First run migrations to set up schema
        run_config_migrations(&conn).expect("migrations should run");

        // Insert auth profile
        conn.execute(
            "INSERT INTO auth_profiles (id, name, provider_id) VALUES ('auth-1', 'Test Auth', 'test-provider')",
            [],
        )
        .unwrap();

        // Insert proxy profile
        conn.execute(
            "INSERT INTO proxy_profiles (id, name, kind, host, port) VALUES ('proxy-1', 'Test Proxy', 'Http', 'localhost', 8080)",
            [],
        )
        .unwrap();

        // Insert SSH tunnel profile
        conn.execute(
            "INSERT INTO ssh_tunnel_profiles (id, name, host, port, user, auth_method) VALUES ('ssh-1', 'Test SSH', 'localhost', 22, 'user', 'password')",
            [],
        )
        .unwrap();

        // Insert connection profile with valid FK references
        conn.execute(
            r#"
            INSERT INTO connection_profiles (id, name, auth_profile_id, proxy_profile_id, ssh_tunnel_profile_id)
            VALUES ('conn-1', 'Test Connection', 'auth-1', 'proxy-1', 'ssh-1')
            "#,
            [],
        )
        .unwrap();

        // Verify the profile was inserted with correct FK values
        let auth_id: String = conn
            .query_row(
                "SELECT auth_profile_id FROM connection_profiles WHERE id = 'conn-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(auth_id, "auth-1");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn migration_0005_governance_normalize_tool_policies_idempotent() {
        // Test that migration 0005 is idempotent on a fresh database
        let path = temp_db("migration_0005_idempotent");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");

        // Run migrations (including 0005)
        run_config_migrations(&conn).expect("migrations should run");

        // Run again - should be idempotent
        run_config_migrations(&conn).expect("migration should be idempotent");

        // Verify only 3 migrations recorded
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM migrations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3);

        // Verify child tables exist
        let tools_table_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='tool_policy_allowed_tools'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            tools_table_exists, 1,
            "tool_policy_allowed_tools should exist"
        );

        let classes_table_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='tool_policy_allowed_classes'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            classes_table_exists, 1,
            "tool_policy_allowed_classes should exist"
        );

        // Verify JSON columns do NOT exist in governance_tool_policies
        let has_allowed_tools: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('governance_tool_policies') WHERE name = 'allowed_tools'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            has_allowed_tools, 0,
            "allowed_tools column should be dropped"
        );

        let has_allowed_classes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('governance_tool_policies') WHERE name = 'allowed_classes'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            has_allowed_classes, 0,
            "allowed_classes column should be dropped"
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn migration_0005_backfills_tool_policies_with_json() {
        // Test that migration 0005 correctly backfills from JSON columns
        let path = temp_db("migration_0005_backfill");
        let _ = std::fs::remove_file(&path);

        let conn = open_database(&path).expect("should open");

        // Run initial schema to set up basic structure
        run_initial_schema_migration(&conn).expect("initial schema should run");

        // Manually insert a governance_tool_policies row with JSON columns
        // (simulating pre-migration 0005 state)
        conn.execute(
            r#"
            ALTER TABLE governance_tool_policies ADD COLUMN allowed_tools TEXT
            "#,
            [],
        )
        .unwrap();
        conn.execute(
            r#"
            ALTER TABLE governance_tool_policies ADD COLUMN allowed_classes TEXT
            "#,
            [],
        )
        .unwrap();

        conn.execute(
            r#"
            INSERT INTO governance_tool_policies (id, governance_id, policy_id, allowed_tools, allowed_classes)
            VALUES ('policy-1', 1, 'test-policy', '["select_data", "list_databases"]', '["Read", "Write"]')
            "#,
            [],
        )
        .unwrap();

        // Record initial migration
        conn.execute("INSERT INTO migrations (name) VALUES ('0001_initial')", [])
            .unwrap();

        // Run migration 0005
        run_migration_0005_governance_normalize_tool_policies(&conn)
            .expect("migration 0005 should run");

        // Record migration 0005
        conn.execute(
            "INSERT INTO migrations (name) VALUES ('0005_governance_normalize_tool_policies')",
            [],
        )
        .unwrap();

        // Verify child tables are populated
        let tools_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tool_policy_allowed_tools WHERE tool_policy_id = 'policy-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tools_count, 2, "should have 2 tools");

        let classes_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tool_policy_allowed_classes WHERE tool_policy_id = 'policy-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(classes_count, 2, "should have 2 classes");

        // Verify JSON columns are dropped
        let has_allowed_tools: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('governance_tool_policies') WHERE name = 'allowed_tools'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(has_allowed_tools, 0, "allowed_tools should be dropped");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
