//! Application state for DBFlux.
//!
//! This module contains the core `AppState` struct which manages all application-level
//! state including connections, profiles, settings, and audit services.

use dbflux_core::observability::actions::{
    CONFIG_CHANGE, CONFIG_CREATE, CONFIG_DELETE, CONFIG_UPDATE,
};
use dbflux_core::observability::{
    EventCategory, EventOrigin, EventOutcome, EventRecord, EventSeverity,
};
use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    AuthProfile, CancelToken, Connection, ConnectionHook, ConnectionHooks, ConnectionMcpGovernance,
    ConnectionProfile, DbDriver, DbSchemaInfo, DriverKey, EffectiveSettings, FormValues,
    GeneralSettings, GlobalOverrides, HistoryEntry, HistoryManager, HookContext, HookPhase,
    ProfileManager, ProxyProfile, SavedQuery, SavedQueryManager, SchemaForeignKeyInfo,
    SchemaIndexInfo, SchemaSnapshot, ScriptsDirectory, SecretStore, ServiceConfig, SessionFacade,
    ShutdownPhase, SshTunnelProfile, TaskId, TaskKind, TaskSnapshot,
};
use dbflux_driver_ipc::{IpcDriver, driver::IpcDriverLaunchConfig};
use dbflux_storage::bootstrap::StorageRuntime;

#[cfg(feature = "mcp")]
use dbflux_mcp::{
    AuditEntry, AuditExportFormat, AuditQuery, ConnectionPolicyAssignmentDto, McpGovernanceService,
    McpRuntime, McpRuntimeEvent, PendingExecutionDetail, PendingExecutionSummary, PolicyRoleDto,
    ToolPolicyDto, TrustedClientDto,
};

#[cfg(feature = "sqlite")]
use dbflux_driver_sqlite::SqliteDriver;

#[cfg(feature = "postgres")]
use dbflux_driver_postgres::PostgresDriver;

#[cfg(feature = "mysql")]
use dbflux_core::DbKind;

#[cfg(feature = "mysql")]
use dbflux_driver_mysql::MysqlDriver;

#[cfg(feature = "mongodb")]
use dbflux_driver_mongodb::MongoDriver;

#[cfg(feature = "redis")]
use dbflux_driver_redis::RedisDriver;

#[cfg(feature = "dynamodb")]
use dbflux_driver_dynamodb::DynamoDriver;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use uuid::Uuid;

use crate::auth_provider_registry::{AuthProviderRegistry, RegistryAuthProviderWrapper};

pub use dbflux_core::{
    ConnectProfileParams, ConnectedProfile, DangerousQuerySuppressions, FetchDatabaseSchemaParams,
    FetchSchemaForeignKeysParams, FetchSchemaIndexesParams, FetchSchemaTypesParams,
    FetchTableDetailsParams, SwitchDatabaseParams,
};

fn rpc_registry_id(socket_id: &str) -> String {
    format!("rpc:{}", socket_id)
}

struct BuiltDrivers {
    drivers: HashMap<String, Arc<dyn DbDriver>>,
    general_settings: GeneralSettings,
    driver_overrides: HashMap<DriverKey, GlobalOverrides>,
    driver_settings: HashMap<DriverKey, FormValues>,
    hook_definitions: HashMap<String, ConnectionHook>,
}

pub struct AppState {
    pub facade: SessionFacade,
    general_settings: GeneralSettings,
    driver_overrides: HashMap<DriverKey, GlobalOverrides>,
    driver_settings: HashMap<DriverKey, FormValues>,
    hook_definitions: HashMap<String, ConnectionHook>,
    detached_hook_tasks: HashMap<Uuid, HashSet<TaskId>>,
    auth_provider_registry: AuthProviderRegistry,
    history_manager: crate::history_manager_sqlite::HistoryManager,
    scripts_directory: Option<ScriptsDirectory>,
    storage_runtime: StorageRuntime,
    audit_service: dbflux_audit::AuditService,
    /// Tracks whether the audit service was initialized from a degraded (in-memory)
    /// store because the real SQLite database could not be opened. When true,
    /// bootstrap_audit_settings will not enable the service even if persisted
    /// settings say enabled=true, preserving an honest degraded-state signal.
    audit_degraded: bool,
    #[cfg(feature = "mcp")]
    mcp_runtime: McpRuntime,
}

impl AppState {
    pub fn new() -> Self {
        let (built, storage_runtime, profiles, auth_profiles, proxies, ssh_tunnels) =
            Self::build_default_drivers();

        Self::new_with_drivers_and_settings(
            built.drivers,
            built.general_settings,
            built.driver_overrides,
            built.driver_settings,
            built.hook_definitions,
            storage_runtime,
            profiles,
            auth_profiles,
            proxies,
            ssh_tunnels,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_drivers_and_settings(
        drivers: HashMap<String, Arc<dyn DbDriver>>,
        general_settings: GeneralSettings,
        driver_overrides: HashMap<DriverKey, GlobalOverrides>,
        driver_settings: HashMap<DriverKey, FormValues>,
        hook_definitions: HashMap<String, ConnectionHook>,
        storage_runtime: dbflux_storage::bootstrap::StorageRuntime,
        profiles: Vec<ConnectionProfile>,
        auth_profiles: Vec<dbflux_core::AuthProfile>,
        proxies: Vec<dbflux_core::ProxyProfile>,
        ssh_tunnels: Vec<SshTunnelProfile>,
    ) -> Self {
        let scripts_directory = ScriptsDirectory::new()
            .inspect_err(|e| log::warn!("Failed to initialize scripts directory: {}", e))
            .ok();

        let profile_manager = ProfileManager::with_profiles(profiles, None);
        let ssh_manager =
            dbflux_core::SshTunnelManager::with_items(ssh_tunnels, None, "SSH tunnel profiles");
        let proxy_manager = dbflux_core::ProxyManager::with_items(proxies, None, "proxy profiles");
        let auth_manager =
            dbflux_core::AuthProfileManager::with_items(auth_profiles, None, "auth profiles");

        let tree_store: Box<dyn dbflux_core::TreeStore> =
            Box::new(dbflux_storage::sqlite_tree_store::SqliteTreeStore::new(
                storage_runtime.dbflux_db_path().to_path_buf(),
            ));

        let facade = SessionFacade::with_all_custom_managers_and_tree_store(
            drivers,
            profile_manager,
            ssh_manager,
            proxy_manager,
            auth_manager,
            HistoryManager::new(),
            SavedQueryManager::new(),
            tree_store,
        );

        let mut history_manager =
            crate::history_manager_sqlite::HistoryManager::new(&storage_runtime);
        history_manager.set_max_entries(general_settings.max_history_entries);

        let mut auth_provider_registry = AuthProviderRegistry::new();
        #[cfg(feature = "aws")]
        {
            auth_provider_registry.register(Arc::new(dbflux_aws::AwsSsoAuthProvider::new()));
            auth_provider_registry
                .register(Arc::new(dbflux_aws::AwsSharedCredentialsAuthProvider::new()));
            auth_provider_registry
                .register(Arc::new(dbflux_aws::AwsStaticCredentialsAuthProvider::new()));
        }

        let (audit_service, audit_degraded) =
            match dbflux_audit::AuditService::new_sqlite(storage_runtime.dbflux_db_path()) {
                Ok(service) => (service, false),
                Err(e) => {
                    log::error!(
                        "Failed to initialize audit service at {:?}: {}",
                        storage_runtime.dbflux_db_path(),
                        e
                    );
                    let store = dbflux_audit::store::sqlite::SqliteAuditStore::new(":memory:")
                        .expect("in-memory audit store must work: rusqlite :memory: unavailable");
                    let svc = dbflux_audit::AuditService::new(store);
                    svc.set_enabled(false);
                    (svc, true)
                }
            };

        #[cfg(feature = "mcp")]
        let mcp_runtime = McpRuntime::new(audit_service.clone());

        let mut state = Self {
            facade,
            general_settings,
            driver_overrides,
            driver_settings,
            hook_definitions,
            detached_hook_tasks: HashMap::new(),
            auth_provider_registry,
            history_manager,
            scripts_directory,
            storage_runtime,
            audit_service,
            audit_degraded,
            #[cfg(feature = "mcp")]
            mcp_runtime,
        };

        #[cfg(feature = "mcp")]
        if let Err(e) = state.bootstrap_mcp_runtime_from_persistence() {
            log::warn!("Failed to bootstrap MCP runtime from persistence: {}", e);
        }

        if let Err(e) = state.bootstrap_audit_settings() {
            log::warn!("Failed to bootstrap audit settings: {}", e);
            let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
            let event = EventRecord::new(
                now_ms,
                EventSeverity::Error,
                EventCategory::System,
                EventOutcome::Failure,
            )
            .with_typed_action(CONFIG_CHANGE)
            .with_summary(format!("Audit bootstrap failed: {}", e))
            .with_actor_id("system");
            if let Err(rec_err) = state.audit_service().record(event) {
                log::warn!(
                    "Failed to record audit bootstrap failure event: {}",
                    rec_err
                );
            }
        }

        state
    }

    #[cfg(feature = "mcp")]
    fn bootstrap_mcp_runtime_from_persistence(&mut self) -> Result<(), String> {
        let repo = self.storage_runtime.governance_settings();

        if let Some(settings) = repo.get().map_err(|e| e.to_string())? {
            self.mcp_runtime
                .set_mcp_enabled(settings.mcp_enabled_by_default != 0);
        }

        let storage_clients = repo.get_trusted_clients().map_err(|e| e.to_string())?;
        for client in storage_clients {
            self.mcp_runtime
                .upsert_trusted_client_mut(TrustedClientDto {
                    id: client.client_id,
                    name: client.name,
                    issuer: client.issuer,
                    active: client.active != 0,
                })
                .map_err(|e| format!("failed to upsert trusted client: {}", e))?;
        }

        let storage_roles = repo.get_policy_roles().map_err(|e| e.to_string())?;
        for role in storage_roles {
            self.mcp_runtime
                .upsert_role_mut(PolicyRoleDto {
                    id: role.role_id,
                    policy_ids: Vec::new(),
                })
                .map_err(|e| format!("failed to upsert policy role: {}", e))?;
        }

        let storage_policies = repo.get_tool_policies().map_err(|e| e.to_string())?;
        for policy in storage_policies {
            self.mcp_runtime
                .upsert_policy_mut(ToolPolicyDto {
                    id: policy.policy_id,
                    allowed_tools: policy.allowed_tools,
                    allowed_classes: policy.allowed_classes,
                })
                .map_err(|e| format!("failed to upsert tool policy: {}", e))?;
        }

        for profile in self.facade.profiles.profiles.clone() {
            let Some(profile_governance) = profile.mcp_governance else {
                continue;
            };

            if !profile_governance.enabled {
                continue;
            }

            let assignments = profile_governance
                .policy_bindings
                .into_iter()
                .map(|binding| dbflux_policy::ConnectionPolicyAssignment {
                    actor_id: binding.actor_id,
                    scope: dbflux_policy::PolicyBindingScope {
                        connection_id: profile.id.to_string(),
                    },
                    role_ids: binding.role_ids,
                    policy_ids: binding.policy_ids,
                })
                .collect();

            self.mcp_runtime
                .save_connection_policy_assignment_mut(ConnectionPolicyAssignmentDto {
                    connection_id: profile.id.to_string(),
                    assignments,
                })
                .map_err(|e| format!("failed to save connection policy assignment: {}", e))?;
        }

        self.mcp_runtime.drain_events();
        Ok(())
    }

    fn bootstrap_audit_settings(&mut self) -> Result<(), String> {
        use dbflux_storage::repositories::audit_settings::AuditSettingsDto;

        let repo = self.storage_runtime.audit_settings();

        let settings = match repo.get().map_err(|e| e.to_string())? {
            Some(s) => s,
            None => {
                let defaults = AuditSettingsDto::default();
                repo.upsert(&defaults).map_err(|e| e.to_string())?;
                defaults
            }
        };

        if self.audit_degraded {
            log::warn!(
                "Audit service is in degraded state (DB init failed); preserving disabled status \
                 even though settings have enabled={}. Audit events will not be recorded.",
                settings.enabled
            );
        } else {
            self.audit_service.set_enabled(settings.enabled);
        }
        self.audit_service
            .set_redact_sensitive(settings.redact_sensitive_values);
        self.audit_service
            .set_capture_query_text(settings.capture_query_text);
        self.audit_service
            .set_max_detail_bytes(settings.max_detail_bytes);

        if settings.purge_on_startup && settings.retention_days > 0 {
            log::info!(
                "Audit purge_on_startup enabled (retention_days={}), running purge...",
                settings.retention_days
            );
            match self
                .audit_service
                .purge_old_events(settings.retention_days, 500)
            {
                Ok(stats) => {
                    log::info!(
                        "Audit purge completed: deleted {} events in {} batches ({}ms)",
                        stats.deleted_count,
                        stats.batches,
                        stats.duration_ms
                    );
                    let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
                    let event = EventRecord::new(
                        now_ms,
                        EventSeverity::Info,
                        EventCategory::System,
                        EventOutcome::Success,
                    )
                    .with_typed_action(CONFIG_CHANGE)
                    .with_summary(format!(
                        "Startup audit purge completed: deleted {} events",
                        stats.deleted_count
                    ))
                    .with_duration_ms(stats.duration_ms as i64)
                    .with_actor_id("system");
                    if let Err(rec_err) = self.audit_service.record(event) {
                        log::warn!(
                            "Failed to record startup purge success audit event: {}",
                            rec_err
                        );
                    }
                }
                Err(e) => {
                    log::warn!("Audit purge on startup failed: {}", e);
                    let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
                    let event = EventRecord::new(
                        now_ms,
                        EventSeverity::Error,
                        EventCategory::System,
                        EventOutcome::Failure,
                    )
                    .with_typed_action(CONFIG_CHANGE)
                    .with_summary(format!("Startup audit purge failed: {}", e))
                    .with_actor_id("system");
                    if let Err(rec_err) = self.audit_service.record(event) {
                        log::warn!(
                            "Failed to record startup purge failure audit event: {}",
                            rec_err
                        );
                    }
                }
            }
        }

        if settings.background_purge_interval_minutes > 0 {
            log::info!(
                "Background audit purge configured (interval={}min, retention={}days)",
                settings.background_purge_interval_minutes,
                settings.retention_days
            );
        }

        let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
        let event = EventRecord::new(
            now_ms,
            EventSeverity::Info,
            EventCategory::System,
            EventOutcome::Success,
        )
        .with_typed_action(CONFIG_CHANGE)
        .with_summary("Audit settings bootstrapped successfully")
        .with_actor_id("system");
        if let Err(rec_err) = self.audit_service.record(event) {
            log::warn!(
                "Failed to record audit bootstrap success event: {}",
                rec_err
            );
        }

        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn build_default_drivers() -> (
        BuiltDrivers,
        dbflux_storage::bootstrap::StorageRuntime,
        Vec<ConnectionProfile>,
        Vec<AuthProfile>,
        Vec<ProxyProfile>,
        Vec<SshTunnelProfile>,
    ) {
        let drivers = Self::build_builtin_drivers();

        let (
            general_settings,
            driver_overrides,
            driver_settings,
            hook_definitions,
            services,
            runtime,
        ) = Self::load_app_config_from_storage();

        if !services.is_empty() {
            Self::launch_rpc_services(&mut drivers.clone(), services);
        }

        let loaded = crate::config_loader::load_config(&runtime);

        (
            BuiltDrivers {
                drivers,
                general_settings,
                driver_overrides,
                driver_settings,
                hook_definitions,
            },
            runtime,
            loaded.profiles,
            loaded.auth_profiles,
            loaded.proxy_profiles,
            loaded.ssh_tunnels,
        )
    }

    #[allow(clippy::type_complexity)]
    fn load_app_config_from_storage() -> (
        GeneralSettings,
        HashMap<DriverKey, GlobalOverrides>,
        HashMap<DriverKey, FormValues>,
        HashMap<String, ConnectionHook>,
        Vec<ServiceConfig>,
        dbflux_storage::bootstrap::StorageRuntime,
    ) {
        let runtime = dbflux_storage::bootstrap::initialize()
            .expect("failed to initialize internal storage — cannot continue");

        let loaded = crate::config_loader::load_config(&runtime);

        (
            loaded.general_settings,
            loaded.driver_overrides,
            loaded.driver_settings,
            loaded.hook_definitions,
            loaded.services,
            runtime,
        )
    }

    fn launch_rpc_services(
        drivers: &mut HashMap<String, Arc<dyn DbDriver>>,
        services: Vec<ServiceConfig>,
    ) {
        for service in services {
            if !service.enabled {
                log::info!("Skipping disabled service '{}'", service.socket_id);
                continue;
            }

            let driver_id = rpc_registry_id(&service.socket_id);

            if drivers.contains_key(&driver_id) {
                log::warn!(
                    "Skipping external RPC service '{}': driver id already exists",
                    service.socket_id
                );
                continue;
            }

            let launch = IpcDriverLaunchConfig {
                program: service
                    .command
                    .clone()
                    .unwrap_or_else(|| "dbflux-driver-host".to_string()),
                args: service.args.clone(),
                env: service.env.into_iter().collect(),
                startup_timeout: std::time::Duration::from_millis(
                    service.startup_timeout_ms.unwrap_or(5_000),
                ),
            };

            let (kind, metadata, form_definition, settings_schema) =
                match IpcDriver::probe_driver(&service.socket_id, Some(&launch)) {
                    Ok(info) => info,
                    Err(error) => {
                        log::warn!(
                            "Skipping RPC service '{}': failed to probe driver metadata: {}",
                            service.socket_id,
                            error
                        );
                        continue;
                    }
                };

            let ipc_driver = IpcDriver::new(
                service.socket_id.clone(),
                kind,
                metadata,
                form_definition,
                settings_schema,
            )
            .with_launch_config(launch);

            drivers.insert(driver_id, Arc::new(ipc_driver));
        }
    }

    fn build_builtin_drivers() -> HashMap<String, Arc<dyn DbDriver>> {
        #[allow(unused_mut)]
        let mut drivers: HashMap<String, Arc<dyn DbDriver>> = HashMap::new();

        #[cfg(feature = "sqlite")]
        {
            drivers.insert("sqlite".to_string(), Arc::new(SqliteDriver::new()));
        }

        #[cfg(feature = "postgres")]
        {
            drivers.insert("postgres".to_string(), Arc::new(PostgresDriver::new()));
        }

        #[cfg(feature = "mysql")]
        {
            drivers.insert(
                "mysql".to_string(),
                Arc::new(MysqlDriver::new(DbKind::MySQL)),
            );
            drivers.insert(
                "mariadb".to_string(),
                Arc::new(MysqlDriver::new(DbKind::MariaDB)),
            );
        }

        #[cfg(feature = "mongodb")]
        {
            drivers.insert("mongodb".to_string(), Arc::new(MongoDriver::new()));
        }

        #[cfg(feature = "redis")]
        {
            drivers.insert("redis".to_string(), Arc::new(RedisDriver::new()));
        }

        #[cfg(feature = "dynamodb")]
        {
            drivers.insert("dynamodb".to_string(), Arc::new(DynamoDriver::new()));
        }

        drivers
    }

    // --- ConnectionManager ---

    pub fn active_connection(&self) -> Option<&ConnectedProfile> {
        self.facade.connections.active_connection()
    }

    #[allow(dead_code)]
    pub fn is_connected(&self) -> bool {
        self.facade.connections.is_connected()
    }

    pub fn has_connections(&self) -> bool {
        self.facade.connections.has_connections()
    }

    #[allow(dead_code)]
    pub fn connection_display_name(&self) -> Option<&str> {
        self.facade.connections.connection_display_name()
    }

    #[allow(dead_code)]
    pub fn active_schema(&self) -> Option<&SchemaSnapshot> {
        self.facade.connections.active_schema()
    }

    pub fn get_connection(&self, profile_id: Uuid) -> Option<Arc<dyn Connection>> {
        self.facade.connections.get_connection(profile_id)
    }

    pub fn set_active_connection(&mut self, profile_id: Uuid) {
        self.facade.connections.set_active_connection(profile_id);
    }

    pub fn disconnect(&mut self, profile_id: Uuid) {
        self.facade.connections.disconnect(profile_id);
    }

    #[allow(dead_code)]
    pub fn disconnect_all(&mut self) {
        self.facade.connections.disconnect_all();
    }

    // --- Schema cache ---

    #[allow(dead_code)]
    pub fn get_database_schema(&self, profile_id: Uuid, database: &str) -> Option<&DbSchemaInfo> {
        self.facade
            .connections
            .get_database_schema(profile_id, database)
    }

    pub fn set_database_schema(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: DbSchemaInfo,
    ) {
        self.facade
            .connections
            .set_database_schema(profile_id, database, schema);
    }

    pub fn needs_database_schema(&self, profile_id: Uuid, database: &str) -> bool {
        self.facade
            .connections
            .needs_database_schema(profile_id, database)
    }

    #[allow(dead_code)]
    pub fn get_table_details(
        &self,
        profile_id: Uuid,
        database: &str,
        table: &str,
    ) -> Option<&dbflux_core::TableInfo> {
        self.facade
            .connections
            .get_table_details(profile_id, database, table)
    }

    #[allow(dead_code)]
    pub fn set_table_details(
        &mut self,
        profile_id: Uuid,
        database: String,
        table: String,
        details: dbflux_core::TableInfo,
    ) {
        self.facade
            .connections
            .set_table_details(profile_id, database, table, details);
    }

    #[allow(dead_code)]
    pub fn needs_table_details(&self, profile_id: Uuid, database: &str, table: &str) -> bool {
        self.facade
            .connections
            .needs_table_details(profile_id, database, table)
    }

    #[allow(dead_code)]
    pub fn get_schema_types(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Option<&Vec<dbflux_core::CustomTypeInfo>> {
        self.facade
            .connections
            .get_schema_types(profile_id, database, schema)
    }

    pub fn set_schema_types(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        types: Vec<dbflux_core::CustomTypeInfo>,
    ) {
        self.facade
            .connections
            .set_schema_types(profile_id, database, schema, types);
    }

    pub fn needs_schema_types(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        self.facade
            .connections
            .needs_schema_types(profile_id, database, schema)
    }

    pub fn set_schema_indexes(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        indexes: Vec<SchemaIndexInfo>,
    ) {
        self.facade
            .connections
            .set_schema_indexes(profile_id, database, schema, indexes);
    }

    pub fn needs_schema_indexes(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        self.facade
            .connections
            .needs_schema_indexes(profile_id, database, schema)
    }

    pub fn set_schema_foreign_keys(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        foreign_keys: Vec<SchemaForeignKeyInfo>,
    ) {
        self.facade
            .connections
            .set_schema_foreign_keys(profile_id, database, schema, foreign_keys);
    }

    pub fn needs_schema_foreign_keys(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        self.facade
            .connections
            .needs_schema_foreign_keys(profile_id, database, schema)
    }

    #[allow(dead_code)]
    pub fn get_active_database(&self, profile_id: Uuid) -> Option<String> {
        self.facade.connections.get_active_database(profile_id)
    }

    pub fn set_active_database(&mut self, profile_id: Uuid, database: Option<String>) {
        self.facade
            .connections
            .set_active_database(profile_id, database);
    }

    // --- Redis key cache ---

    #[allow(dead_code)]
    pub fn get_redis_cached_keys(&self, profile_id: Uuid, keyspace: &str) -> Option<Arc<[String]>> {
        self.facade
            .connections
            .connections
            .get(&profile_id)
            .and_then(|conn| conn.redis_key_cache.get_keys(keyspace))
    }

    pub fn set_redis_cached_keys(&mut self, profile_id: Uuid, keyspace: String, keys: Vec<String>) {
        if let Some(conn) = self.facade.connections.connections.get_mut(&profile_id) {
            conn.redis_key_cache.set_keys(keyspace, keys);
        }
    }

    #[allow(dead_code)]
    pub fn redis_keys_stale(&self, profile_id: Uuid, keyspace: &str) -> bool {
        self.facade
            .connections
            .connections
            .get(&profile_id)
            .map(|conn| conn.redis_key_cache.is_stale(keyspace))
            .unwrap_or(true)
    }

    // --- Pending operations ---

    pub fn is_operation_pending(&self, profile_id: Uuid, database: Option<&str>) -> bool {
        self.facade
            .connections
            .is_operation_pending(profile_id, database)
    }

    pub fn start_pending_operation(&mut self, profile_id: Uuid, database: Option<&str>) -> bool {
        self.facade
            .connections
            .start_pending_operation(profile_id, database)
    }

    pub fn finish_pending_operation(&mut self, profile_id: Uuid, database: Option<&str>) {
        self.facade
            .connections
            .finish_pending_operation(profile_id, database);
    }

    // --- Prepare/Apply ---

    pub fn prepare_connect_profile(
        &self,
        profile_id: Uuid,
    ) -> Result<ConnectProfileParams, String> {
        let secrets = &self.facade.secrets;

        let proxy_secret = {
            let profile = self
                .facade
                .profiles
                .profiles
                .iter()
                .find(|p| p.id == profile_id);
            match profile {
                Some(p) => secrets.get_proxy_secret_for_profile(p, &self.facade.proxies.items),
                None => None,
            }
        };

        self.facade.connections.prepare_connect_profile(
            profile_id,
            &self.facade.profiles.profiles,
            &self.facade.ssh_tunnels.items,
            &self.facade.proxies.items,
            &secrets.secret_store_arc(),
            |profile, ssh_tunnels| secrets.get_ssh_secret_for_profile(profile, ssh_tunnels),
            proxy_secret,
        )
    }

    pub fn apply_connect_profile(
        &mut self,
        profile: ConnectionProfile,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
        proxy_tunnel: Option<Box<dyn std::any::Any + Send + Sync>>,
    ) {
        self.facade
            .connections
            .apply_connect_profile(profile, connection, schema, proxy_tunnel);
    }

    pub fn prepare_database_connection(
        &self,
        profile_id: Uuid,
        database: &str,
    ) -> Result<SwitchDatabaseParams, String> {
        self.facade.connections.prepare_database_connection(
            profile_id,
            database,
            &self.facade.secrets.secret_store_arc(),
        )
    }

    #[allow(dead_code)]
    pub fn prepare_switch_database(
        &self,
        profile_id: Uuid,
        database: &str,
    ) -> Result<SwitchDatabaseParams, String> {
        self.facade.connections.prepare_switch_database(
            profile_id,
            database,
            &self.facade.secrets.secret_store_arc(),
        )
    }

    #[allow(dead_code)]
    pub fn apply_switch_database(
        &mut self,
        profile_id: Uuid,
        original_profile: ConnectionProfile,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
    ) {
        self.facade.connections.apply_switch_database(
            profile_id,
            original_profile,
            connection,
            schema,
        );
    }

    pub fn add_database_connection(
        &mut self,
        profile_id: Uuid,
        database: String,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
    ) {
        self.facade
            .connections
            .add_database_connection(profile_id, database, connection, schema);
    }

    pub fn prepare_fetch_database_schema(
        &self,
        profile_id: Uuid,
        database: &str,
    ) -> Result<FetchDatabaseSchemaParams, String> {
        self.facade
            .connections
            .prepare_fetch_database_schema(profile_id, database)
    }

    #[allow(dead_code)]
    pub fn prepare_fetch_table_details(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<FetchTableDetailsParams, String> {
        self.facade
            .connections
            .prepare_fetch_table_details(profile_id, database, schema, table)
    }

    pub fn prepare_fetch_schema_types(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<FetchSchemaTypesParams, String> {
        self.facade
            .connections
            .prepare_fetch_schema_types(profile_id, database, schema)
    }

    pub fn prepare_fetch_schema_indexes(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<FetchSchemaIndexesParams, String> {
        self.facade
            .connections
            .prepare_fetch_schema_indexes(profile_id, database, schema)
    }

    pub fn prepare_fetch_schema_foreign_keys(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<FetchSchemaForeignKeysParams, String> {
        self.facade
            .connections
            .prepare_fetch_schema_foreign_keys(profile_id, database, schema)
    }

    // --- SecretManager ---

    pub fn secret_store_available(&self) -> bool {
        self.facade.secrets.is_available()
    }

    #[allow(dead_code)]
    pub fn secret_store(&self) -> Arc<RwLock<Box<dyn SecretStore>>> {
        self.facade.secrets.secret_store_arc()
    }

    pub fn save_password(&self, profile: &ConnectionProfile, password: &SecretString) {
        self.facade.secrets.save_password(profile, password);
    }

    pub fn delete_password(&self, profile: &ConnectionProfile) {
        self.facade.secrets.delete_password(profile);
    }

    pub fn get_password(&self, profile: &ConnectionProfile) -> Option<SecretString> {
        self.facade.secrets.get_password(profile)
    }

    pub fn get_ssh_password(&self, profile: &ConnectionProfile) -> Option<SecretString> {
        self.facade.secrets.get_ssh_password(profile)
    }

    pub fn save_ssh_password(&self, profile: &ConnectionProfile, secret: &SecretString) {
        self.facade.secrets.save_ssh_password(profile, secret);
    }

    pub fn delete_ssh_password(&self, profile: &ConnectionProfile) {
        self.facade.secrets.delete_ssh_password(profile);
    }

    pub fn get_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile) -> Option<SecretString> {
        self.facade.secrets.get_ssh_tunnel_secret(tunnel)
    }

    pub fn save_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile, secret: &SecretString) {
        self.facade.secrets.save_ssh_tunnel_secret(tunnel, secret);
    }

    fn record_config_event(
        &self,
        outcome: EventOutcome,
        action: dbflux_core::observability::AuditAction,
        object_type: &'static str,
        object_id: String,
        summary: String,
        error_message: Option<String>,
    ) {
        let severity = match outcome {
            EventOutcome::Failure => EventSeverity::Error,
            EventOutcome::Cancelled => EventSeverity::Warn,
            EventOutcome::Pending => EventSeverity::Info,
            EventOutcome::Success => EventSeverity::Info,
        };

        let mut event = EventRecord::new(
            dbflux_core::chrono::Utc::now().timestamp_millis(),
            severity,
            EventCategory::Config,
            outcome,
        );
        event = event.with_origin(EventOrigin::local());

        let mut event = event
            .with_action(action.as_str())
            .with_summary(summary)
            .with_actor_id("local")
            .with_object_ref(object_type, object_id);

        if let Some(error_message) = error_message {
            event.error_message = Some(error_message);
        }

        if let Err(error) = self.audit_service.record(event) {
            log::warn!(
                "Failed to record {} audit event for {}: {}",
                action.as_str(),
                object_type,
                error
            );
        }
    }

    // --- ProfileManager ---

    pub fn add_profile_in_folder(&mut self, profile: ConnectionProfile, folder_id: Option<Uuid>) {
        let profile_name = profile.name.clone();
        let profile_id = profile.id.to_string();
        self.facade.add_profile_in_folder(profile, folder_id);

        self.record_config_event(
            EventOutcome::Success,
            CONFIG_CREATE,
            "connection_profile",
            profile_id,
            format!("Created connection profile '{}'", profile_name),
            None,
        );
    }

    pub fn remove_profile(&mut self, idx: usize) -> Option<ConnectionProfile> {
        let removed = self.facade.remove_profile(idx)?;

        self.record_config_event(
            EventOutcome::Success,
            CONFIG_DELETE,
            "connection_profile",
            removed.id.to_string(),
            format!("Deleted connection profile '{}'", removed.name),
            None,
        );

        Some(removed)
    }

    pub fn update_profile(&mut self, profile: ConnectionProfile) {
        let profile_name = profile.name.clone();
        let profile_id = profile.id.to_string();
        self.facade.profiles.update(profile);

        self.record_config_event(
            EventOutcome::Success,
            CONFIG_UPDATE,
            "connection_profile",
            profile_id,
            format!("Updated connection profile '{}'", profile_name),
            None,
        );
    }

    pub fn save_profiles(&self) {
        if let Err(e) = crate::config_loader::save_profiles(
            &self.storage_runtime,
            &self.facade.profiles.profiles,
        ) {
            log::error!("Failed to save connection profiles: {}", e);
        }
    }

    // --- SshTunnelManager ---

    pub fn add_ssh_tunnel(&mut self, tunnel: SshTunnelProfile) {
        let tunnel_name = tunnel.name.clone();
        let tunnel_id = tunnel.id.to_string();
        self.facade.ssh_tunnels.items.push(tunnel.clone());
        let save_result = crate::config_loader::save_ssh_tunnels(
            &self.storage_runtime,
            &self.facade.ssh_tunnels.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_CREATE,
            "ssh_tunnel_profile",
            tunnel_id,
            format!("Created SSH tunnel '{}'", tunnel_name),
            error_msg,
        );

        if let Err(e) = save_result {
            log::error!("Failed to save SSH tunnel profiles: {}", e);
        }
    }

    #[allow(dead_code)]
    pub fn remove_ssh_tunnel(&mut self, idx: usize) -> Option<SshTunnelProfile> {
        let removed = self.facade.remove_ssh_tunnel(idx)?;
        let tunnel_name = removed.name.clone();
        let tunnel_id = removed.id.to_string();

        let save_result = crate::config_loader::save_ssh_tunnels(
            &self.storage_runtime,
            &self.facade.ssh_tunnels.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_DELETE,
            "ssh_tunnel_profile",
            tunnel_id,
            format!("Deleted SSH tunnel '{}'", tunnel_name),
            error_msg,
        );

        if let Err(e) = save_result {
            log::error!("Failed to save SSH tunnel profiles after remove: {}", e);
        }
        Some(removed)
    }

    #[allow(dead_code)]
    pub fn update_ssh_tunnel(&mut self, tunnel: SshTunnelProfile) {
        let tunnel_name = tunnel.name.clone();
        let tunnel_id = tunnel.id.to_string();
        if let Some(existing) = self
            .facade
            .ssh_tunnels
            .items
            .iter_mut()
            .find(|t| t.id == tunnel.id)
        {
            *existing = tunnel.clone();
            let save_result = crate::config_loader::save_ssh_tunnels(
                &self.storage_runtime,
                &self.facade.ssh_tunnels.items,
            );

            let (outcome, error_msg) = match &save_result {
                Ok(()) => (EventOutcome::Success, None),
                Err(e) => (EventOutcome::Failure, Some(e.to_string())),
            };

            self.record_config_event(
                outcome,
                CONFIG_UPDATE,
                "ssh_tunnel_profile",
                tunnel_id,
                format!("Updated SSH tunnel '{}'", tunnel_name),
                error_msg,
            );

            if let Err(e) = save_result {
                log::error!("Failed to save SSH tunnel profiles: {}", e);
            }
        }
    }

    // --- ProxyManager ---

    pub fn add_proxy(&mut self, proxy: dbflux_core::ProxyProfile) {
        let proxy_name = proxy.name.clone();
        let proxy_id = proxy.id.to_string();
        self.facade.proxies.items.push(proxy.clone());
        let save_result = crate::config_loader::save_proxy_profiles(
            &self.storage_runtime,
            &self.facade.proxies.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_CREATE,
            "proxy_profile",
            proxy_id,
            format!("Created proxy '{}'", proxy_name),
            error_msg,
        );

        if let Err(e) = save_result {
            log::error!("Failed to save proxy profiles: {}", e);
        }
    }

    pub fn remove_proxy(&mut self, idx: usize) -> Option<dbflux_core::ProxyProfile> {
        let removed = self.facade.remove_proxy(idx)?;
        let proxy_name = removed.name.clone();
        let proxy_id = removed.id.to_string();

        let save_result = crate::config_loader::save_proxy_profiles(
            &self.storage_runtime,
            &self.facade.proxies.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_DELETE,
            "proxy_profile",
            proxy_id,
            format!("Deleted proxy '{}'", proxy_name),
            error_msg,
        );

        if let Err(e) = save_result {
            log::error!("Failed to save proxy profiles after remove: {}", e);
        }
        Some(removed)
    }

    pub fn update_proxy(&mut self, proxy: dbflux_core::ProxyProfile) {
        let proxy_name = proxy.name.clone();
        let proxy_id = proxy.id.to_string();
        if let Some(existing) = self
            .facade
            .proxies
            .items
            .iter_mut()
            .find(|p| p.id == proxy.id)
        {
            *existing = proxy.clone();
            let save_result = crate::config_loader::save_proxy_profiles(
                &self.storage_runtime,
                &self.facade.proxies.items,
            );

            let (outcome, error_msg) = match &save_result {
                Ok(()) => (EventOutcome::Success, None),
                Err(e) => (EventOutcome::Failure, Some(e.to_string())),
            };

            self.record_config_event(
                outcome,
                CONFIG_UPDATE,
                "proxy_profile",
                proxy_id,
                format!("Updated proxy '{}'", proxy_name),
                error_msg,
            );

            if let Err(e) = save_result {
                log::error!("Failed to save proxy profiles: {}", e);
            }
        }
    }

    pub fn get_proxy_secret(&self, proxy: &dbflux_core::ProxyProfile) -> Option<SecretString> {
        self.facade.secrets.get_proxy_secret(proxy)
    }

    pub fn save_proxy_secret(&self, proxy: &dbflux_core::ProxyProfile, secret: &SecretString) {
        self.facade.secrets.save_proxy_secret(proxy, secret);
    }

    pub fn delete_proxy_secret(&self, proxy: &dbflux_core::ProxyProfile) {
        self.facade.secrets.delete_proxy_secret(proxy);
    }

    // --- AuthProfileManager ---

    pub fn add_auth_profile(&mut self, profile: dbflux_core::AuthProfile) {
        let profile_name = profile.name.clone();
        let profile_id = profile.id.to_string();
        self.facade.auth_profiles.items.push(profile.clone());
        let save_result = crate::config_loader::save_auth_profiles(
            &self.storage_runtime,
            &self.facade.auth_profiles.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_CREATE,
            "auth_profile",
            profile_id,
            format!("Created auth profile '{}'", profile_name),
            error_msg,
        );

        if let Err(e) = save_result {
            log::error!("Failed to save auth profiles: {}", e);
        }
    }

    pub fn remove_auth_profile(&mut self, idx: usize) -> Option<dbflux_core::AuthProfile> {
        if idx >= self.facade.auth_profiles.items.len() {
            return None;
        }
        let removed = self.facade.auth_profiles.items.remove(idx);
        let profile_name = removed.name.clone();
        let profile_id = removed.id.to_string();

        let save_result = crate::config_loader::save_auth_profiles(
            &self.storage_runtime,
            &self.facade.auth_profiles.items,
        );

        let (outcome, error_msg) = match &save_result {
            Ok(()) => (EventOutcome::Success, None),
            Err(e) => (EventOutcome::Failure, Some(e.to_string())),
        };

        self.record_config_event(
            outcome,
            CONFIG_DELETE,
            "auth_profile",
            profile_id,
            format!("Deleted auth profile '{}'", profile_name),
            error_msg,
        );

        if let Err(e) = save_result {
            log::error!("Failed to save auth profiles after remove: {}", e);
        }
        Some(removed)
    }

    pub fn update_auth_profile(&mut self, profile: dbflux_core::AuthProfile) {
        let profile_name = profile.name.clone();
        let profile_id = profile.id.to_string();
        if let Some(existing) = self
            .facade
            .auth_profiles
            .items
            .iter_mut()
            .find(|i| i.id == profile.id)
        {
            *existing = profile.clone();
            let save_result = crate::config_loader::save_auth_profiles(
                &self.storage_runtime,
                &self.facade.auth_profiles.items,
            );

            let (outcome, error_msg) = match &save_result {
                Ok(()) => (EventOutcome::Success, None),
                Err(e) => (EventOutcome::Failure, Some(e.to_string())),
            };

            self.record_config_event(
                outcome,
                CONFIG_UPDATE,
                "auth_profile",
                profile_id,
                format!("Updated auth profile '{}'", profile_name),
                error_msg,
            );

            if let Err(e) = save_result {
                log::error!("Failed to save auth profiles: {}", e);
            }
        }
    }

    pub fn auth_profiles(&self) -> &[dbflux_core::AuthProfile] {
        &self.facade.auth_profiles.items
    }

    // --- ConnectionTreeManager ---

    pub fn save_connection_tree(&self) {
        self.facade.tree.save();
    }

    pub fn create_folder(&mut self, name: impl Into<String>, parent_id: Option<Uuid>) -> Uuid {
        self.facade.tree.create_folder(name, parent_id)
    }

    pub fn rename_folder(&mut self, folder_id: Uuid, new_name: impl Into<String>) -> bool {
        self.facade.tree.rename_folder(folder_id, new_name)
    }

    pub fn delete_folder(&mut self, folder_id: Uuid) -> Vec<Uuid> {
        self.facade.tree.delete_folder(folder_id)
    }

    pub fn move_tree_node(&mut self, node_id: Uuid, new_parent_id: Option<Uuid>) -> bool {
        self.facade.tree.move_node(node_id, new_parent_id)
    }

    pub fn move_tree_node_to_position(
        &mut self,
        node_id: Uuid,
        new_parent_id: Option<Uuid>,
        after_id: Option<Uuid>,
    ) -> bool {
        self.facade
            .tree
            .move_node_to_position(node_id, new_parent_id, after_id)
    }

    #[allow(dead_code)]
    pub fn toggle_folder_collapsed(&mut self, folder_id: Uuid) -> Option<bool> {
        self.facade.tree.toggle_folder_collapsed(folder_id)
    }

    pub fn set_folder_collapsed(&mut self, folder_id: Uuid, collapsed: bool) {
        self.facade.tree.set_folder_collapsed(folder_id, collapsed);
    }

    // --- HistoryManager (SQLite-backed via history_manager_sqlite) ---

    pub fn history_entries(&self) -> &[HistoryEntry] {
        self.history_manager.entries()
    }

    pub fn add_history_entry(&mut self, entry: HistoryEntry) {
        self.history_manager.add(entry);
    }

    #[allow(dead_code)]
    pub fn toggle_history_favorite(&mut self, id: Uuid) -> bool {
        self.history_manager.toggle_favorite(id)
    }

    #[allow(dead_code)]
    pub fn remove_history_entry(&mut self, id: Uuid) {
        self.history_manager.remove(id);
    }

    // --- SavedQueryManager (SQLite-backed via history_manager_sqlite) ---

    #[allow(dead_code)]
    pub fn take_saved_query_warning(&mut self) -> Option<String> {
        None
    }

    pub fn add_saved_query(&mut self, query: SavedQuery) {
        self.history_manager.add_saved_query(query);
    }

    pub fn update_saved_query(&mut self, id: Uuid, name: String, sql: String) -> bool {
        self.history_manager.update_saved_query(id, name, sql)
    }

    pub fn remove_saved_query(&mut self, id: Uuid) -> bool {
        self.history_manager.remove_saved_query(id)
    }

    pub fn toggle_saved_query_favorite(&mut self, id: Uuid) -> bool {
        self.history_manager.toggle_saved_query_favorite(id)
    }

    pub fn update_saved_query_last_used(&mut self, id: Uuid) -> bool {
        self.history_manager.update_saved_query_last_used(id)
    }

    #[allow(dead_code)]
    pub fn update_saved_query_sql(&mut self, id: Uuid, sql: &str) -> bool {
        self.history_manager.update_saved_query_sql(id, sql)
    }

    #[allow(dead_code)]
    pub fn update_saved_query_name(&mut self, id: Uuid, name: &str) -> bool {
        self.history_manager.update_saved_query_name(id, name)
    }

    #[allow(dead_code)]
    pub fn get_saved_query(&self, id: Uuid) -> Option<&SavedQuery> {
        self.history_manager.get_saved_query(id)
    }

    pub fn saved_queries(&self) -> &[SavedQuery] {
        self.history_manager.saved_queries_list()
    }

    // --- RecentFiles (SQLite-backed) ---

    #[allow(dead_code)]
    pub fn recent_files(&self) -> &[dbflux_core::RecentFile] {
        self.history_manager.recent_files_entries()
    }

    pub fn record_recent_file(&mut self, path: PathBuf) {
        self.history_manager.record_recent_file(path);
    }

    #[allow(dead_code)]
    pub fn remove_recent_file(&mut self, path: &PathBuf) {
        self.history_manager.remove_recent_file(path);
    }

    // --- ScriptsDirectory ---

    pub fn scripts_directory(&self) -> Option<&ScriptsDirectory> {
        self.scripts_directory.as_ref()
    }

    pub fn scripts_directory_mut(&mut self) -> Option<&mut ScriptsDirectory> {
        self.scripts_directory.as_mut()
    }

    pub fn refresh_scripts(&mut self) {
        if let Some(dir) = self.scripts_directory.as_mut() {
            dir.refresh();
        }
    }

    // --- ArtifactStore (filesystem boundary for scratch/shadow) ---

    pub fn scratch_path(&self, doc_id: &str, extension: &str) -> std::path::PathBuf {
        self.storage_runtime.scratch_path(doc_id, extension)
    }

    pub fn shadow_path(&self, doc_id: &str) -> std::path::PathBuf {
        self.storage_runtime.shadow_path(doc_id)
    }

    // --- TaskManager ---

    pub fn start_task(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
    ) -> (TaskId, CancelToken) {
        self.facade.tasks.start(kind, description)
    }

    pub fn start_task_for_target(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        target: Option<dbflux_core::TaskTarget>,
    ) -> (TaskId, CancelToken) {
        self.facade
            .tasks
            .start_for_target(kind, description, target)
    }

    pub fn start_task_for_profile(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        profile_id: Option<Uuid>,
    ) -> (TaskId, CancelToken) {
        let target = profile_id.map(|profile_id| dbflux_core::TaskTarget {
            profile_id,
            database: None,
        });

        self.start_task_for_target(kind, description, target)
    }

    pub fn start_hook_task_for_profile(
        &mut self,
        phase: HookPhase,
        profile_id: Uuid,
        profile_name: &str,
        command: &str,
    ) -> (TaskId, CancelToken) {
        self.start_task_for_profile(
            TaskKind::Hook { phase },
            format!("Hook: {} — {} — {}", phase.label(), profile_name, command),
            Some(profile_id),
        )
    }

    pub fn complete_task(&mut self, id: TaskId) {
        self.facade.tasks.complete(id);
    }

    pub fn complete_task_with_details(&mut self, id: TaskId, details: impl Into<String>) {
        self.facade.tasks.complete_with_details(id, details);
    }

    pub fn append_task_details(&mut self, id: TaskId, details: impl AsRef<str>) {
        self.facade.tasks.append_details(id, details);
    }

    pub fn fail_task(&mut self, id: TaskId, error: impl Into<String>) {
        self.facade.tasks.fail(id, error);
    }

    pub fn fail_task_with_details(
        &mut self,
        id: TaskId,
        error: impl Into<String>,
        details: impl Into<String>,
    ) {
        self.facade.tasks.fail_with_details(id, error, details);
    }

    #[allow(dead_code)]
    pub fn cancel_task(&mut self, id: TaskId) -> bool {
        self.facade.tasks.cancel(id)
    }

    pub fn register_detached_hook_task(&mut self, profile_id: Uuid, task_id: TaskId) {
        self.detached_hook_tasks
            .entry(profile_id)
            .or_default()
            .insert(task_id);
    }

    pub fn unregister_detached_hook_task(&mut self, profile_id: Uuid, task_id: TaskId) {
        if let Some(tasks) = self.detached_hook_tasks.get_mut(&profile_id) {
            tasks.remove(&task_id);

            if tasks.is_empty() {
                self.detached_hook_tasks.remove(&profile_id);
            }
        }
    }

    pub fn cancel_detached_hook_tasks(&mut self, profile_id: Uuid) -> usize {
        let Some(task_ids) = self.detached_hook_tasks.remove(&profile_id) else {
            return 0;
        };

        task_ids
            .into_iter()
            .filter(|task_id| self.facade.tasks.cancel(*task_id))
            .count()
    }

    pub fn cancel_all_detached_hook_tasks(&mut self) -> usize {
        let profile_ids: Vec<Uuid> = self.detached_hook_tasks.keys().copied().collect();

        profile_ids
            .into_iter()
            .map(|profile_id| self.cancel_detached_hook_tasks(profile_id))
            .sum()
    }

    #[allow(dead_code)]
    pub fn running_tasks(&self) -> Vec<TaskSnapshot> {
        self.facade.tasks.running_tasks()
    }

    pub fn has_running_tasks(&self) -> bool {
        self.facade.tasks.has_running_tasks()
    }

    // --- Shutdown ---

    pub fn begin_shutdown(&self) -> bool {
        self.facade.begin_shutdown()
    }

    pub fn is_shutting_down(&self) -> bool {
        self.facade.is_shutting_down()
    }

    pub fn shutdown_phase(&self) -> ShutdownPhase {
        self.facade.shutdown_phase()
    }

    pub fn cancel_all_tasks(&mut self) -> usize {
        self.facade.cancel_all_tasks()
    }

    pub fn close_all_connections(&mut self) {
        self.cancel_all_detached_hook_tasks();
        self.facade.close_all_connections();
    }

    pub fn complete_shutdown(&self) {
        self.facade.complete_shutdown();
    }

    #[allow(dead_code)]
    pub fn fail_shutdown(&self) {
        self.facade.fail_shutdown();
    }
}

// --- Field accessors ---

impl AppState {
    pub fn build_hook_context(&self, profile: &ConnectionProfile) -> HookContext {
        HookContext::from_profile(profile)
    }

    pub fn drivers(&self) -> &HashMap<String, Arc<dyn DbDriver>> {
        &self.facade.connections.drivers
    }

    pub fn storage_runtime(&self) -> &StorageRuntime {
        &self.storage_runtime
    }

    pub fn driver_for_profile(&self, profile: &ConnectionProfile) -> Option<Arc<dyn DbDriver>> {
        self.facade
            .connections
            .drivers
            .get(&profile.driver_id())
            .cloned()
    }

    pub fn profiles(&self) -> &[ConnectionProfile] {
        &self.facade.profiles.profiles
    }

    pub fn profiles_mut(&mut self) -> &mut Vec<ConnectionProfile> {
        &mut self.facade.profiles.profiles
    }

    pub fn ssh_tunnels(&self) -> &[SshTunnelProfile] {
        &self.facade.ssh_tunnels.items
    }

    pub fn proxies(&self) -> &[dbflux_core::ProxyProfile] {
        &self.facade.proxies.items
    }

    pub fn connections(&self) -> &HashMap<Uuid, ConnectedProfile> {
        &self.facade.connections.connections
    }

    pub fn remove_database_connection(&mut self, profile_id: Uuid, database: &str) -> bool {
        self.facade
            .connections
            .remove_database_connection(profile_id, database)
    }

    pub fn cancel_query_for_target(&self, target: &dbflux_core::TaskTarget) {
        let Some(connection) = self.facade.connections.connection_for_task_target(target) else {
            return;
        };

        let cancel_handle = connection.cancel_handle();
        if let Err(error) = cancel_handle.cancel() {
            log::warn!("Failed to send cancel via handle: {}", error);
        }

        if let Err(error) = connection.cancel_active() {
            log::warn!("Failed to send cancel to database: {}", error);
        }
    }

    pub fn cancel_running_connect_tasks_for_profile(&mut self, profile_id: Uuid) -> usize {
        let connect_task_ids: Vec<TaskId> = self
            .facade
            .tasks
            .running_tasks()
            .into_iter()
            .filter(|task| task.kind == TaskKind::Connect && task.profile_id == Some(profile_id))
            .map(|task| task.id)
            .collect();

        connect_task_ids
            .into_iter()
            .filter(|task_id| self.facade.tasks.cancel(*task_id))
            .count()
    }

    pub fn connections_mut(&mut self) -> &mut HashMap<Uuid, ConnectedProfile> {
        &mut self.facade.connections.connections
    }

    pub fn active_connection_id(&self) -> Option<Uuid> {
        self.facade.connections.active_connection_id
    }

    pub fn tasks(&self) -> &dbflux_core::TaskManager {
        &self.facade.tasks
    }

    pub fn tasks_mut(&mut self) -> &mut dbflux_core::TaskManager {
        &mut self.facade.tasks
    }

    pub fn dangerous_query_suppressions(&self) -> &DangerousQuerySuppressions {
        &self.facade.dangerous_query_suppressions
    }

    pub fn dangerous_query_suppressions_mut(&mut self) -> &mut DangerousQuerySuppressions {
        &mut self.facade.dangerous_query_suppressions
    }

    pub fn audit_service(&self) -> &dbflux_audit::AuditService {
        &self.audit_service
    }

    pub fn is_audit_degraded(&self) -> bool {
        self.audit_degraded
    }

    pub fn connection_tree(&self) -> &dbflux_core::ConnectionTree {
        &self.facade.tree.tree
    }

    pub fn connection_tree_mut(&mut self) -> &mut dbflux_core::ConnectionTree {
        &mut self.facade.tree.tree
    }

    pub fn shutdown(&self) -> &dbflux_core::ShutdownCoordinator {
        &self.facade.shutdown
    }

    pub fn general_settings(&self) -> &GeneralSettings {
        &self.general_settings
    }

    pub fn effective_settings(&self, driver_key: &str) -> EffectiveSettings {
        let empty_values = FormValues::new();
        let driver_values = self
            .driver_settings
            .get(driver_key)
            .unwrap_or(&empty_values);

        EffectiveSettings::resolve(
            &self.general_settings,
            self.driver_overrides.get(driver_key),
            driver_values,
            None,
            None,
        )
    }

    pub fn effective_settings_for_connection(
        &self,
        connection_id: Option<Uuid>,
    ) -> EffectiveSettings {
        let empty_values = FormValues::new();

        let Some(connection_id) = connection_id else {
            return EffectiveSettings::resolve(
                &self.general_settings,
                None,
                &empty_values,
                None,
                None,
            );
        };

        let profile = self
            .connections()
            .get(&connection_id)
            .map(|connected| connected.profile.clone());

        let Some(profile) = profile else {
            return EffectiveSettings::resolve(
                &self.general_settings,
                None,
                &empty_values,
                None,
                None,
            );
        };

        let Some(driver) = self.driver_for_profile(&profile) else {
            return EffectiveSettings::resolve(
                &self.general_settings,
                None,
                &empty_values,
                None,
                None,
            );
        };

        let driver_key = driver.driver_key();
        let driver_values = self
            .driver_settings
            .get(&driver_key)
            .unwrap_or(&empty_values);

        EffectiveSettings::resolve(
            &self.general_settings,
            self.driver_overrides.get(&driver_key),
            driver_values,
            profile.settings_overrides.as_ref(),
            profile.connection_settings.as_ref(),
        )
    }

    #[allow(dead_code)]
    pub fn driver_overrides(&self) -> &HashMap<DriverKey, GlobalOverrides> {
        &self.driver_overrides
    }

    #[allow(dead_code)]
    pub fn driver_settings(&self) -> &HashMap<DriverKey, FormValues> {
        &self.driver_settings
    }

    pub fn is_background_task_limit_reached(&self) -> bool {
        let limit = self.general_settings.max_concurrent_background_tasks;
        self.facade.tasks.background_task_count() >= limit
    }

    pub fn update_general_settings(&mut self, settings: GeneralSettings) {
        self.history_manager
            .set_max_entries(settings.max_history_entries);

        self.general_settings = settings;
    }

    #[allow(dead_code)]
    pub fn update_driver_overrides(&mut self, key: DriverKey, overrides: GlobalOverrides) {
        if overrides.is_empty() {
            self.driver_overrides.remove(&key);
            return;
        }

        self.driver_overrides.insert(key, overrides);
    }

    #[allow(dead_code)]
    pub fn update_driver_settings(&mut self, key: DriverKey, values: FormValues) {
        if values.is_empty() {
            self.driver_settings.remove(&key);
            return;
        }

        self.driver_settings.insert(key, values);
    }

    pub fn hook_definitions(&self) -> &HashMap<String, ConnectionHook> {
        &self.hook_definitions
    }

    pub fn set_hook_definitions(&mut self, definitions: HashMap<String, ConnectionHook>) {
        let hook_count = definitions.len();
        self.hook_definitions = definitions;

        let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
        let summary = format!("Updated hook definitions ({} hooks)", hook_count);
        let event = EventRecord::new(
            now_ms,
            EventSeverity::Info,
            EventCategory::Config,
            EventOutcome::Success,
        )
        .with_typed_action(CONFIG_CHANGE)
        .with_summary(&summary)
        .with_origin(EventOrigin::local())
        .with_actor_id("local")
        .with_object_ref("hook_definition", "global")
        .with_details_json(serde_json::json!({ "hook_count": hook_count }).to_string());

        if let Err(error) = self.audit_service.record(event) {
            log::warn!(
                "Failed to record hook definitions update audit event: {}",
                error
            );
        }
    }
}

#[cfg(feature = "mcp")]
impl AppState {
    pub fn list_mcp_trusted_clients(&self) -> Result<Vec<TrustedClientDto>, String> {
        dbflux_mcp::McpGovernanceService::list_trusted_clients(&self.mcp_runtime)
            .map_err(|error| error.to_string())
    }

    pub fn upsert_mcp_trusted_client(&mut self, client: TrustedClientDto) -> Result<(), String> {
        self.mcp_runtime
            .upsert_trusted_client_mut(client)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    pub fn delete_mcp_trusted_client(&mut self, client_id: &str) -> Result<(), String> {
        self.mcp_runtime
            .delete_trusted_client_mut(client_id)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    #[allow(dead_code)]
    pub fn list_mcp_connection_policy_assignments(
        &self,
    ) -> Result<Vec<ConnectionPolicyAssignmentDto>, String> {
        dbflux_mcp::McpGovernanceService::list_connection_policy_assignments(&self.mcp_runtime)
            .map_err(|error| error.to_string())
    }

    pub fn save_mcp_connection_policy_assignment(
        &mut self,
        assignment: ConnectionPolicyAssignmentDto,
    ) -> Result<(), String> {
        self.mcp_runtime
            .save_connection_policy_assignment_mut(assignment)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    #[allow(dead_code)]
    pub fn request_mcp_execution(
        &mut self,
        actor_id: String,
        connection_id: String,
        tool_id: String,
        classification: dbflux_policy::ExecutionClassification,
        payload: serde_json::Value,
    ) -> PendingExecutionSummary {
        let plan = self.mcp_runtime.classify_plan(
            classification,
            payload,
            actor_id,
            connection_id,
            tool_id,
        );

        self.mcp_runtime.request_execution_mut(plan)
    }

    pub fn list_mcp_pending_executions(&self) -> Result<Vec<PendingExecutionSummary>, String> {
        dbflux_mcp::McpGovernanceService::list_pending_executions(&self.mcp_runtime)
            .map_err(|error| error.to_string())
    }

    pub fn get_mcp_pending_execution(
        &self,
        pending_id: &str,
    ) -> Result<PendingExecutionDetail, String> {
        dbflux_mcp::McpGovernanceService::get_pending_execution(&self.mcp_runtime, pending_id)
            .map_err(|error| error.to_string())
    }

    pub fn approve_mcp_pending_execution(
        &mut self,
        pending_id: &str,
    ) -> Result<AuditEntry, String> {
        self.mcp_runtime
            .approve_pending_execution_with_origin_mut(pending_id, "local", EventOrigin::local())
            .map_err(|error| error.to_string())
    }

    pub fn reject_mcp_pending_execution(&mut self, pending_id: &str) -> Result<AuditEntry, String> {
        self.mcp_runtime
            .reject_pending_execution_with_origin_mut(
                pending_id,
                "local",
                None,
                EventOrigin::local(),
            )
            .map_err(|error| error.to_string())
    }

    pub fn query_mcp_audit_entries(&self, query: &AuditQuery) -> Result<Vec<AuditEntry>, String> {
        dbflux_mcp::McpGovernanceService::query_audit_entries(&self.mcp_runtime, query)
            .map_err(|error| error.to_string())
    }

    pub fn export_mcp_audit_entries(
        &self,
        query: &AuditQuery,
        format: AuditExportFormat,
    ) -> Result<String, String> {
        dbflux_mcp::McpGovernanceService::export_audit_entries(&self.mcp_runtime, query, format)
            .map_err(|error| error.to_string())
    }

    pub fn drain_mcp_runtime_events(&mut self) -> Vec<McpRuntimeEvent> {
        self.mcp_runtime.drain_events()
    }

    pub fn list_mcp_roles(&self) -> Result<Vec<PolicyRoleDto>, String> {
        let user_roles = dbflux_mcp::McpGovernanceService::list_roles(&self.mcp_runtime)
            .map_err(|error| error.to_string())?;

        let mut all = dbflux_mcp::builtin_roles();
        all.extend(user_roles);
        Ok(all)
    }

    pub fn upsert_mcp_role(&mut self, role: PolicyRoleDto) -> Result<(), String> {
        if dbflux_mcp::is_builtin(&role.id) {
            return Err("Built-in roles cannot be modified".to_string());
        }

        self.mcp_runtime
            .upsert_role_mut(role)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    pub fn delete_mcp_role(&mut self, role_id: &str) -> Result<(), String> {
        if dbflux_mcp::is_builtin(role_id) {
            return Err("Built-in roles cannot be deleted".to_string());
        }

        self.mcp_runtime
            .delete_role_mut(role_id)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    pub fn list_mcp_policies(&self) -> Result<Vec<ToolPolicyDto>, String> {
        let user_policies = dbflux_mcp::McpGovernanceService::list_policies(&self.mcp_runtime)
            .map_err(|error| error.to_string())?;

        let mut all = dbflux_mcp::builtin_policies();
        all.extend(user_policies);
        Ok(all)
    }

    pub fn upsert_mcp_policy(&mut self, policy: ToolPolicyDto) -> Result<(), String> {
        if dbflux_mcp::is_builtin(&policy.id) {
            return Err("Built-in policies cannot be modified".to_string());
        }

        self.mcp_runtime
            .upsert_policy_mut(policy)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    pub fn delete_mcp_policy(&mut self, policy_id: &str) -> Result<(), String> {
        if dbflux_mcp::is_builtin(policy_id) {
            return Err("Built-in policies cannot be deleted".to_string());
        }

        self.mcp_runtime
            .delete_policy_mut(policy_id)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance()
    }

    #[allow(dead_code)]
    pub fn set_profile_mcp_governance(
        &mut self,
        profile_id: Uuid,
        governance: Option<ConnectionMcpGovernance>,
    ) -> Result<(), String> {
        let Some(profile) = self
            .facade
            .profiles
            .profiles
            .iter_mut()
            .find(|profile| profile.id == profile_id)
        else {
            return Err(format!("profile not found: {profile_id}"));
        };

        profile.mcp_governance = governance;
        self.save_profiles();

        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub fn persist_mcp_governance(&mut self) -> Result<(), String> {
        let repo = self.storage_runtime.governance_settings();

        let mcp_enabled_by_default = repo
            .get()
            .map_err(|e| e.to_string())?
            .map(|s| s.mcp_enabled_by_default)
            .unwrap_or(0);

        let governance_settings =
            dbflux_storage::repositories::governance_settings::GovernanceSettingsDto {
                id: 1,
                mcp_enabled_by_default,
                updated_at: String::new(),
            };
        repo.upsert(&governance_settings)
            .map_err(|e| e.to_string())?;

        let mcp_clients = self
            .mcp_runtime
            .list_trusted_clients()
            .map_err(|e| e.to_string())?;
        let storage_clients = mcp_clients
            .into_iter()
            .map(
                |client| dbflux_storage::repositories::governance_settings::TrustedClientDto {
                    id: Uuid::new_v4().to_string(),
                    governance_id: 1,
                    client_id: client.id,
                    name: client.name,
                    issuer: client.issuer,
                    active: if client.active { 1 } else { 0 },
                },
            )
            .collect::<Vec<_>>();
        repo.replace_trusted_clients(&storage_clients)
            .map_err(|e| e.to_string())?;

        let mcp_roles = self.mcp_runtime.list_roles().map_err(|e| e.to_string())?;
        let storage_roles = mcp_roles
            .into_iter()
            .map(
                |role| dbflux_storage::repositories::governance_settings::PolicyRoleDto {
                    id: Uuid::new_v4().to_string(),
                    governance_id: 1,
                    role_id: role.id,
                },
            )
            .collect::<Vec<_>>();
        repo.replace_policy_roles(&storage_roles)
            .map_err(|e| e.to_string())?;

        let mcp_policies = self
            .mcp_runtime
            .list_policies()
            .map_err(|e| e.to_string())?;
        let storage_policies = mcp_policies
            .into_iter()
            .map(
                |policy| dbflux_storage::repositories::governance_settings::ToolPolicyDto {
                    id: Uuid::new_v4().to_string(),
                    governance_id: 1,
                    policy_id: policy.id,
                    allowed_tools: policy.allowed_tools,
                    allowed_classes: policy.allowed_classes,
                },
            )
            .collect::<Vec<_>>();
        repo.replace_tool_policies(&storage_policies)
            .map_err(|e| e.to_string())?;

        self.save_profiles();

        Ok(())
    }

    #[allow(dead_code)]
    pub fn reload_mcp_runtime_from_db(&mut self) -> Result<(), String> {
        self.mcp_runtime.clear();
        self.bootstrap_mcp_runtime_from_persistence()
    }
}

impl AppState {
    pub fn auth_provider_registry(&self) -> &AuthProviderRegistry {
        &self.auth_provider_registry
    }

    pub fn auth_provider_by_id(
        &self,
        provider_id: &str,
    ) -> Option<Arc<dyn dbflux_core::DynAuthProvider>> {
        self.auth_provider_registry.get(provider_id)
    }

    pub fn resolve_profile_hooks(&self, profile: &ConnectionProfile) -> ConnectionHooks {
        ConnectionHooks::resolve_from_bindings(profile, &self.hook_definitions)
    }

    pub fn prepare_pipeline_input(
        &self,
        profile_id: Uuid,
        cancel: CancelToken,
    ) -> Result<(dbflux_core::PipelineInput, String, Arc<dyn DbDriver>), String> {
        let profile = self
            .facade
            .profiles
            .profiles
            .iter()
            .find(|p| p.id == profile_id)
            .ok_or_else(|| format!("Profile {} not found", profile_id))?
            .clone();

        let driver = self
            .driver_for_profile(&profile)
            .ok_or_else(|| format!("Driver '{}' not found", profile.driver_id()))?;

        let profile_name = profile.name.clone();
        let input = self.build_pipeline_input_for_profile(profile, cancel)?;

        Ok((input, profile_name, driver))
    }

    pub fn build_pipeline_input_for_profile(
        &self,
        profile: ConnectionProfile,
        cancel: CancelToken,
    ) -> Result<dbflux_core::PipelineInput, String> {
        let selected_auth_profile_id = profile
            .access_kind
            .as_ref()
            .and_then(|kind| match kind {
                dbflux_core::access::AccessKind::Managed { params, .. } => {
                    params.get("auth_profile_id").and_then(|s| s.parse().ok())
                }
                _ => None,
            })
            .or(profile.auth_profile_id);

        let auth_profile = selected_auth_profile_id.and_then(|auth_id| {
            self.facade
                .auth_profiles
                .items
                .iter()
                .find(|p| p.id == auth_id && p.enabled)
                .cloned()
        });

        let uses_managed_access = matches!(
            profile.access_kind,
            Some(dbflux_core::access::AccessKind::Managed { .. })
        );
        if uses_managed_access && auth_profile.is_none() {
            return Err(
                "Managed access requires an auth profile. Select one in Access > SSM Auth Profile."
                    .to_string(),
            );
        }

        let registered_auth_provider_ids: HashSet<&str> = self
            .auth_provider_registry
            .providers()
            .map(|provider| provider.provider_id())
            .collect();

        let uses_registered_auth_value_sources =
            profile
                .value_refs
                .values()
                .any(|value_ref| match value_ref {
                    dbflux_core::values::ValueRef::Secret { provider, .. }
                    | dbflux_core::values::ValueRef::Parameter { provider, .. } => {
                        registered_auth_provider_ids.contains(provider.as_str())
                    }
                    _ => false,
                });

        if uses_registered_auth_value_sources && auth_profile.is_none() {
            return Err(
                "Value sources requiring auth providers need an auth profile. Select one before connecting."
                    .to_string(),
            );
        }

        let auth_provider: Option<Box<dyn dbflux_core::auth::DynAuthProvider>> =
            if let Some(auth_profile) = auth_profile.as_ref() {
                let provider = self
                    .auth_provider_registry
                    .get(&auth_profile.provider_id)
                    .ok_or_else(|| {
                        format!(
                            "Auth provider '{}' is not available",
                            auth_profile.provider_id
                        )
                    })?;

                Some(RegistryAuthProviderWrapper::boxed(provider))
            } else {
                None
            };

        let cache = Arc::new(dbflux_core::values::ValueCache::new(
            std::time::Duration::from_secs(300),
        ));
        let resolver = dbflux_core::values::CompositeValueResolver::new(cache);

        #[cfg(feature = "aws")]
        let aws_profile_name = auth_profile
            .as_ref()
            .and_then(|p| p.fields.get("profile_name").cloned());

        let ssh_tunnels = self
            .facade
            .ssh_tunnels
            .items
            .iter()
            .map(|tunnel| {
                (
                    tunnel.id,
                    crate::access_manager::ResolvedSshTunnel {
                        config: tunnel.config.clone(),
                        secret: self.facade.secrets.get_ssh_tunnel_secret(tunnel),
                    },
                )
            })
            .collect();

        let proxy_tunnels = self
            .facade
            .proxies
            .items
            .iter()
            .map(|proxy| {
                (
                    proxy.id,
                    dbflux_core::ResolvedProxy {
                        profile: proxy.clone(),
                        secret: self.facade.secrets.get_proxy_secret(proxy),
                    },
                )
            })
            .collect();

        let access_manager: Arc<dyn dbflux_core::access::AccessManager> =
            Arc::new(crate::access_manager::AppAccessManager::new(
                ssh_tunnels,
                proxy_tunnels,
                #[cfg(feature = "aws")]
                Some(Arc::new(dbflux_ssm::SsmTunnelFactory::new(
                    aws_profile_name,
                ))),
            ));

        Ok(dbflux_core::PipelineInput {
            profile,
            auth_provider,
            auth_profile,
            resolver,
            access_manager,
            cancel,
        })
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
