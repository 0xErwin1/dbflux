use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    AppConfig, AppConfigStore, CancelToken, Connection, ConnectionHook, ConnectionHooks,
    ConnectionMcpGovernance, ConnectionProfile, DbDriver, DbSchemaInfo, DriverKey,
    EffectiveSettings, FormValues, GeneralSettings, GlobalOverrides, GovernanceSettings,
    HistoryEntry, HookContext, HookPhase, RecentFilesStore, SavedQuery, SchemaForeignKeyInfo,
    SchemaIndexInfo, SchemaSnapshot, ScriptsDirectory, SecretStore, SessionFacade, SessionStore,
    ShutdownPhase, SshTunnelProfile, TaskId, TaskKind, TaskSnapshot, TrustedClientConfig,
};
use dbflux_driver_ipc::{IpcDriver, driver::IpcDriverLaunchConfig};
use dbflux_mcp::{
    AuditEntry, AuditExportFormat, AuditQuery, ConnectionPolicyAssignmentDto, McpGovernanceService,
    McpRuntime, McpRuntimeEvent, PendingExecutionDetail, PendingExecutionSummary, TrustedClientDto,
};
use gpui::{EventEmitter, WindowHandle};
use gpui_component::Root;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use uuid::Uuid;

use crate::auth_provider_registry::{AuthProviderRegistry, RegistryAuthProviderWrapper};

pub struct AppStateChanged;

pub struct AuthProfileCreated {
    pub profile_id: Uuid,
}

pub struct McpRuntimeEventRaised {
    pub event: McpRuntimeEvent,
}

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
    pub settings_window: Option<WindowHandle<Root>>,
    general_settings: GeneralSettings,
    driver_overrides: HashMap<DriverKey, GlobalOverrides>,
    driver_settings: HashMap<DriverKey, FormValues>,
    hook_definitions: HashMap<String, ConnectionHook>,
    detached_hook_tasks: HashMap<Uuid, HashSet<TaskId>>,
    auth_provider_registry: AuthProviderRegistry,
    recent_files: Option<RecentFilesStore>,
    scripts_directory: Option<ScriptsDirectory>,
    session_store: Option<SessionStore>,
    mcp_runtime: McpRuntime,
}

impl AppState {
    pub fn new() -> Self {
        let built = Self::build_default_drivers();

        Self::new_with_drivers_and_settings(
            built.drivers,
            built.general_settings,
            built.driver_overrides,
            built.driver_settings,
            built.hook_definitions,
        )
    }

    fn new_with_drivers_and_settings(
        drivers: HashMap<String, Arc<dyn DbDriver>>,
        general_settings: GeneralSettings,
        driver_overrides: HashMap<DriverKey, GlobalOverrides>,
        driver_settings: HashMap<DriverKey, FormValues>,
        hook_definitions: HashMap<String, ConnectionHook>,
    ) -> Self {
        let recent_files = RecentFilesStore::new()
            .inspect_err(|e| log::warn!("Failed to initialize recent files store: {}", e))
            .ok();

        let scripts_directory = ScriptsDirectory::new()
            .inspect_err(|e| log::warn!("Failed to initialize scripts directory: {}", e))
            .ok();

        let session_store = SessionStore::new()
            .inspect_err(|e| log::warn!("Failed to initialize session store: {}", e))
            .ok();

        let mut facade = SessionFacade::new(drivers);
        facade
            .history
            .set_max_entries(general_settings.max_history_entries);

        let mut auth_provider_registry = AuthProviderRegistry::new();
        #[cfg(feature = "aws")]
        {
            auth_provider_registry.register(Arc::new(dbflux_aws::AwsSsoAuthProvider::new()));
            auth_provider_registry
                .register(Arc::new(dbflux_aws::AwsSharedCredentialsAuthProvider::new()));
            auth_provider_registry
                .register(Arc::new(dbflux_aws::AwsStaticCredentialsAuthProvider::new()));
        }

        let mcp_runtime = match dbflux_audit::AuditService::new_sqlite_default() {
            Ok(audit_service) => McpRuntime::new(audit_service),
            Err(error) => {
                log::warn!(
                    "Failed to initialize default audit store for MCP runtime: {}",
                    error
                );

                let fallback_path = dbflux_audit::temp_sqlite_path("dbflux-mcp-audit.sqlite");
                match dbflux_audit::AuditService::new_sqlite(&fallback_path) {
                    Ok(audit_service) => McpRuntime::new(audit_service),
                    Err(fallback_error) => {
                        panic!(
                            "failed to initialize MCP runtime audit store (default and fallback): {fallback_error}"
                        );
                    }
                }
            }
        };

        let mut state = Self {
            facade,
            settings_window: None,
            general_settings,
            driver_overrides,
            driver_settings,
            hook_definitions,
            detached_hook_tasks: HashMap::new(),
            auth_provider_registry,
            recent_files,
            scripts_directory,
            session_store,
            mcp_runtime,
        };

        state.bootstrap_mcp_runtime_from_persistence();
        state
    }

    fn bootstrap_mcp_runtime_from_persistence(&mut self) {
        if let Ok(store) = AppConfigStore::new()
            && let Ok(config) = store.load()
        {
            for client in config.governance.trusted_clients {
                let _ = self
                    .mcp_runtime
                    .upsert_trusted_client_mut(TrustedClientDto {
                        id: client.id,
                        name: client.name,
                        issuer: client.issuer,
                        active: client.active,
                    });
            }
        }

        for profile in self.facade.profiles.profiles.clone() {
            let Some(governance) = profile.mcp_governance else {
                continue;
            };

            if !governance.enabled {
                continue;
            }

            let assignments = governance
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

            let _ = self.mcp_runtime.save_connection_policy_assignment_mut(
                ConnectionPolicyAssignmentDto {
                    connection_id: profile.id.to_string(),
                    assignments,
                },
            );
        }

        self.mcp_runtime.drain_events();
    }

    #[allow(clippy::result_large_err)]
    fn build_default_drivers() -> BuiltDrivers {
        let mut drivers = Self::build_builtin_drivers();

        let app_config = AppConfigStore::new()
            .and_then(|store| store.load())
            .inspect_err(|e| log::warn!("Failed to load app config: {}", e))
            .ok();

        let (general_settings, driver_overrides, driver_settings, hook_definitions) = app_config
            .as_ref()
            .map(|config| {
                (
                    config.general.clone(),
                    config.driver_overrides.clone(),
                    config.driver_settings.clone(),
                    config.hook_definitions.clone(),
                )
            })
            .unwrap_or_else(|| {
                (
                    GeneralSettings::default(),
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                )
            });

        if let Some(config) = app_config {
            for service in config.services {
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

        BuiltDrivers {
            drivers,
            general_settings,
            driver_overrides,
            driver_settings,
            hook_definitions,
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

    // --- ProfileManager ---

    pub fn add_profile_in_folder(&mut self, profile: ConnectionProfile, folder_id: Option<Uuid>) {
        self.facade.add_profile_in_folder(profile, folder_id);
    }

    pub fn remove_profile(&mut self, idx: usize) -> Option<ConnectionProfile> {
        self.facade.remove_profile(idx)
    }

    pub fn update_profile(&mut self, profile: ConnectionProfile) {
        self.facade.profiles.update(profile);
    }

    pub fn save_profiles(&self) {
        self.facade.profiles.save();
    }

    // --- SshTunnelManager ---

    pub fn add_ssh_tunnel(&mut self, tunnel: SshTunnelProfile) {
        self.facade.ssh_tunnels.add(tunnel);
    }

    #[allow(dead_code)]
    pub fn remove_ssh_tunnel(&mut self, idx: usize) -> Option<SshTunnelProfile> {
        self.facade.remove_ssh_tunnel(idx)
    }

    #[allow(dead_code)]
    pub fn update_ssh_tunnel(&mut self, tunnel: SshTunnelProfile) {
        self.facade.ssh_tunnels.update(tunnel);
    }

    // --- ProxyManager ---

    pub fn add_proxy(&mut self, proxy: dbflux_core::ProxyProfile) {
        self.facade.proxies.add(proxy);
    }

    pub fn remove_proxy(&mut self, idx: usize) -> Option<dbflux_core::ProxyProfile> {
        self.facade.remove_proxy(idx)
    }

    pub fn update_proxy(&mut self, proxy: dbflux_core::ProxyProfile) {
        self.facade.proxies.update(proxy);
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
        self.facade.auth_profiles.add(profile);
    }

    pub fn remove_auth_profile(&mut self, idx: usize) -> Option<dbflux_core::AuthProfile> {
        self.facade.auth_profiles.remove(idx)
    }

    pub fn update_auth_profile(&mut self, profile: dbflux_core::AuthProfile) {
        self.facade.auth_profiles.update(profile);
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

    // --- HistoryManager ---

    pub fn history_entries(&self) -> &[HistoryEntry] {
        self.facade.history.entries()
    }

    pub fn add_history_entry(&mut self, entry: HistoryEntry) {
        self.facade.history.add(entry);
    }

    #[allow(dead_code)]
    pub fn toggle_history_favorite(&mut self, id: Uuid) -> bool {
        self.facade.history.toggle_favorite(id)
    }

    #[allow(dead_code)]
    pub fn remove_history_entry(&mut self, id: Uuid) {
        self.facade.history.remove(id);
    }

    // --- SavedQueryManager ---

    #[allow(dead_code)]
    pub fn take_saved_query_warning(&mut self) -> Option<String> {
        self.facade.saved_queries.take_warning()
    }

    pub fn add_saved_query(&mut self, query: SavedQuery) {
        self.facade.saved_queries.add(query);
    }

    pub fn update_saved_query(&mut self, id: Uuid, name: String, sql: String) -> bool {
        self.facade.saved_queries.update(id, name, sql)
    }

    pub fn remove_saved_query(&mut self, id: Uuid) -> bool {
        self.facade.saved_queries.remove(id)
    }

    pub fn toggle_saved_query_favorite(&mut self, id: Uuid) -> bool {
        self.facade.saved_queries.toggle_favorite(id)
    }

    pub fn update_saved_query_last_used(&mut self, id: Uuid) -> bool {
        self.facade.saved_queries.update_last_used(id)
    }

    #[allow(dead_code)]
    pub fn update_saved_query_sql(&mut self, id: Uuid, sql: &str) -> bool {
        self.facade.saved_queries.update_sql(id, sql)
    }

    #[allow(dead_code)]
    pub fn update_saved_query_name(&mut self, id: Uuid, name: &str) -> bool {
        self.facade.saved_queries.update_name(id, name)
    }

    #[allow(dead_code)]
    pub fn get_saved_query(&self, id: Uuid) -> Option<&SavedQuery> {
        self.facade.saved_queries.get(id)
    }

    pub fn saved_queries(&self) -> &[SavedQuery] {
        self.facade.saved_queries.queries()
    }

    // --- RecentFilesStore ---

    #[allow(dead_code)]
    pub fn recent_files(&self) -> &[dbflux_core::RecentFile] {
        self.recent_files
            .as_ref()
            .map(|store| store.entries())
            .unwrap_or(&[])
    }

    pub fn record_recent_file(&mut self, path: PathBuf) {
        if let Some(store) = self.recent_files.as_mut() {
            store.record_open(path);
        }
    }

    #[allow(dead_code)]
    pub fn remove_recent_file(&mut self, path: &PathBuf) {
        if let Some(store) = self.recent_files.as_mut() {
            store.remove(path);
        }
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

    // --- SessionStore ---

    pub fn session_store(&self) -> Option<&SessionStore> {
        self.session_store.as_ref()
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
        self.facade
            .history
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
        self.hook_definitions = definitions;
    }

    pub fn list_mcp_trusted_clients(&self) -> Result<Vec<TrustedClientDto>, String> {
        dbflux_mcp::McpGovernanceService::list_trusted_clients(&self.mcp_runtime)
            .map_err(|error| error.to_string())
    }

    pub fn upsert_mcp_trusted_client(&mut self, client: TrustedClientDto) -> Result<(), String> {
        self.mcp_runtime
            .upsert_trusted_client_mut(client)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance();
        Ok(())
    }

    pub fn delete_mcp_trusted_client(&mut self, client_id: &str) -> Result<(), String> {
        self.mcp_runtime
            .delete_trusted_client_mut(client_id)
            .map_err(|error| error.to_string())?;

        self.persist_mcp_governance();
        Ok(())
    }

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

        self.persist_mcp_governance();
        Ok(())
    }

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
            .approve_pending_execution_mut(pending_id)
            .map_err(|error| error.to_string())
    }

    pub fn reject_mcp_pending_execution(&mut self, pending_id: &str) -> Result<AuditEntry, String> {
        self.mcp_runtime
            .reject_pending_execution_mut(pending_id)
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

    pub fn persist_mcp_governance(&self) {
        let trusted_clients = self
            .mcp_runtime
            .list_trusted_clients()
            .unwrap_or_default()
            .into_iter()
            .map(|client| TrustedClientConfig {
                id: client.id,
                name: client.name,
                issuer: client.issuer,
                active: client.active,
            })
            .collect();

        let mut config = AppConfigStore::new()
            .and_then(|store| store.load())
            .unwrap_or_else(|_| AppConfig::default());

        config.governance = GovernanceSettings {
            mcp_enabled_by_default: false,
            trusted_clients,
        };

        if let Ok(store) = AppConfigStore::new() {
            let _ = store.save(&config);
        }

        self.save_profiles();
    }

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

    /// Build pipeline input and return it with profile name and driver.
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

        let access_manager: Arc<dyn dbflux_core::access::AccessManager> =
            Arc::new(crate::access_manager::AppAccessManager::new(
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

impl EventEmitter<AppStateChanged> for AppState {}
impl EventEmitter<AuthProfileCreated> for AppState {}
impl EventEmitter<McpRuntimeEventRaised> for AppState {}

#[cfg(test)]
mod tests {
    use super::AppState;
    use dbflux_core::{
        AuthProfile, CancelToken, DbDriver, DbKind, FormValues, GeneralSettings,
        RefreshPolicySetting,
    };
    use dbflux_mcp::{McpRuntimeEvent, TrustedClientDto};
    use dbflux_test_support::FakeDriver;
    use std::collections::HashMap;
    use std::sync::Arc;
    use uuid::Uuid;

    fn test_state(general_settings: GeneralSettings) -> AppState {
        let fake = FakeDriver::new(DbKind::SQLite);
        let mut drivers: HashMap<String, Arc<dyn DbDriver>> = HashMap::new();
        drivers.insert(fake.metadata().id.clone(), Arc::new(fake));

        AppState::new_with_drivers_and_settings(
            drivers,
            general_settings,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
    }

    #[test]
    fn saved_query_store_is_optional() {
        let state = AppState::new();
        let _ = state.saved_queries();
    }

    #[test]
    fn new_with_drivers_uses_injected_registry() {
        let fake = FakeDriver::new(DbKind::SQLite);
        let driver_id = fake.metadata().id.clone();
        let mut drivers: HashMap<String, Arc<dyn DbDriver>> = HashMap::new();
        drivers.insert(driver_id.clone(), Arc::new(fake));

        let state = AppState::new_with_drivers_and_settings(
            drivers,
            GeneralSettings::default(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );

        assert_eq!(state.drivers().len(), 1);
        assert!(state.drivers().contains_key(&driver_id));
    }

    #[test]
    fn effective_settings_use_global_defaults_without_driver_entries() {
        let mut general_settings = GeneralSettings::default();
        general_settings.default_refresh_policy = RefreshPolicySetting::Interval;
        general_settings.default_refresh_interval_secs = 15;
        general_settings.confirm_dangerous_queries = false;
        general_settings.dangerous_requires_where = false;
        general_settings.dangerous_requires_preview = true;

        let state = test_state(general_settings.clone());
        let effective = state.effective_settings("builtin:redis");

        assert_eq!(
            effective.refresh_policy,
            general_settings.default_refresh_policy
        );
        assert_eq!(
            effective.refresh_interval_secs,
            general_settings.default_refresh_interval_secs
        );
        assert_eq!(
            effective.confirm_dangerous,
            general_settings.confirm_dangerous_queries
        );
        assert_eq!(
            effective.requires_where,
            general_settings.dangerous_requires_where
        );
        assert_eq!(
            effective.requires_preview,
            general_settings.dangerous_requires_preview
        );
        assert!(effective.driver_values.is_empty());
    }

    #[test]
    fn effective_settings_apply_driver_overrides_and_values() {
        let mut state = test_state(GeneralSettings::default());
        state.update_driver_overrides(
            "builtin:redis".to_string(),
            dbflux_core::GlobalOverrides {
                refresh_policy: Some(RefreshPolicySetting::Interval),
                refresh_interval_secs: Some(3),
                confirm_dangerous: Some(false),
                requires_where: Some(false),
                requires_preview: Some(true),
            },
        );

        let mut values = FormValues::new();
        values.insert("scan_batch_size".to_string(), "500".to_string());
        state.update_driver_settings("builtin:redis".to_string(), values.clone());

        let effective = state.effective_settings("builtin:redis");

        assert_eq!(effective.refresh_policy, RefreshPolicySetting::Interval);
        assert_eq!(effective.refresh_interval_secs, 3);
        assert!(!effective.confirm_dangerous);
        assert!(!effective.requires_where);
        assert!(effective.requires_preview);
        assert_eq!(effective.driver_values, values);
    }

    fn insert_connected_profile(state: &mut AppState, profile: &dbflux_core::ConnectionProfile) {
        let driver = state
            .drivers()
            .get(&profile.driver_id())
            .expect("driver must be registered")
            .clone();

        let connection: Arc<dyn dbflux_core::Connection> = Arc::from(
            driver
                .connect_with_secrets(profile, None, None)
                .expect("FakeDriver never fails"),
        );

        state.connections_mut().insert(
            profile.id,
            dbflux_core::ConnectedProfile {
                profile: profile.clone(),
                connection,
                schema: None,
                database_schemas: HashMap::new(),
                table_details: HashMap::new(),
                schema_types: HashMap::new(),
                schema_indexes: HashMap::new(),
                schema_foreign_keys: HashMap::new(),
                active_database: None,
                redis_key_cache: Default::default(),
                database_connections: HashMap::new(),
                proxy_tunnel: None,
            },
        );
    }

    fn fake_driver_key(state: &AppState) -> String {
        state.drivers().values().next().unwrap().driver_key()
    }

    fn fake_profile(state: &AppState) -> dbflux_core::ConnectionProfile {
        let driver_id = state.drivers().keys().next().unwrap().clone();
        let mut profile =
            dbflux_core::ConnectionProfile::new("test", dbflux_core::DbConfig::default_sqlite());
        profile.set_driver_id(driver_id);
        profile
    }

    #[test]
    fn connection_overrides_win_over_driver_overrides() {
        let mut state = test_state(GeneralSettings::default());
        let driver_key = fake_driver_key(&state);

        state.update_driver_overrides(
            driver_key,
            dbflux_core::GlobalOverrides {
                confirm_dangerous: Some(true),
                requires_where: Some(true),
                ..Default::default()
            },
        );

        let mut profile = fake_profile(&state);
        profile.settings_overrides = Some(dbflux_core::GlobalOverrides {
            confirm_dangerous: Some(false),
            ..Default::default()
        });

        insert_connected_profile(&mut state, &profile);

        let effective = state.effective_settings_for_connection(Some(profile.id));

        assert!(
            !effective.confirm_dangerous,
            "connection override should win"
        );
        assert!(
            effective.requires_where,
            "driver override should fall through"
        );
    }

    #[test]
    fn connection_without_overrides_falls_through_to_driver() {
        let mut general = GeneralSettings::default();
        general.confirm_dangerous_queries = false;

        let mut state = test_state(general);
        let driver_key = fake_driver_key(&state);

        state.update_driver_overrides(
            driver_key,
            dbflux_core::GlobalOverrides {
                confirm_dangerous: Some(true),
                ..Default::default()
            },
        );

        let profile = fake_profile(&state);
        insert_connected_profile(&mut state, &profile);

        let effective = state.effective_settings_for_connection(Some(profile.id));

        assert!(
            effective.confirm_dangerous,
            "driver override should apply when connection has no overrides"
        );
    }

    #[test]
    fn connection_settings_merge_on_top_of_driver_settings() {
        let mut state = test_state(GeneralSettings::default());
        let driver_key = fake_driver_key(&state);

        let mut driver_vals = FormValues::new();
        driver_vals.insert("scan_batch_size".to_string(), "100".to_string());
        driver_vals.insert("allow_flush".to_string(), "false".to_string());
        state.update_driver_settings(driver_key, driver_vals);

        let mut profile = fake_profile(&state);

        let mut conn_settings = FormValues::new();
        conn_settings.insert("scan_batch_size".to_string(), "500".to_string());
        profile.connection_settings = Some(conn_settings);

        insert_connected_profile(&mut state, &profile);

        let effective = state.effective_settings_for_connection(Some(profile.id));

        assert_eq!(
            effective.driver_values.get("scan_batch_size"),
            Some(&"500".to_string()),
            "connection setting should override driver setting"
        );
        assert_eq!(
            effective.driver_values.get("allow_flush"),
            Some(&"false".to_string()),
            "driver setting should fall through when connection doesn't override"
        );
    }

    #[test]
    fn update_driver_maps_remove_empty_entries() {
        let mut state = test_state(GeneralSettings::default());

        state.update_driver_overrides(
            "builtin:redis".to_string(),
            dbflux_core::GlobalOverrides {
                confirm_dangerous: Some(false),
                ..Default::default()
            },
        );

        let mut values = FormValues::new();
        values.insert("allow_flush".to_string(), "true".to_string());
        state.update_driver_settings("builtin:redis".to_string(), values);

        assert!(state.driver_overrides().contains_key("builtin:redis"));
        assert!(state.driver_settings().contains_key("builtin:redis"));

        state.update_driver_overrides(
            "builtin:redis".to_string(),
            dbflux_core::GlobalOverrides::default(),
        );
        state.update_driver_settings("builtin:redis".to_string(), FormValues::new());

        assert!(!state.driver_overrides().contains_key("builtin:redis"));
        assert!(!state.driver_settings().contains_key("builtin:redis"));
    }

    #[test]
    fn build_pipeline_input_fails_for_unknown_auth_provider() {
        let mut state = test_state(GeneralSettings::default());

        let auth_profile_id = Uuid::new_v4();
        state.add_auth_profile(AuthProfile {
            id: auth_profile_id,
            name: "Unknown Provider Profile".to_string(),
            provider_id: "unknown-provider".to_string(),
            fields: HashMap::new(),
            enabled: true,
        });

        let mut profile = fake_profile(&state);
        profile.auth_profile_id = Some(auth_profile_id);

        let error = match state.build_pipeline_input_for_profile(profile, CancelToken::new()) {
            Ok(_) => panic!("unknown provider must fail before pipeline start"),
            Err(error) => error,
        };

        assert_eq!(error, "Auth provider 'unknown-provider' is not available");
    }

    #[test]
    fn mcp_trusted_client_upsert_emits_runtime_event() {
        let mut state = test_state(GeneralSettings::default());

        state
            .upsert_mcp_trusted_client(TrustedClientDto {
                id: "agent-a".to_string(),
                name: "Agent A".to_string(),
                issuer: None,
                active: true,
            })
            .expect("trusted client upsert should succeed");

        let clients = state
            .list_mcp_trusted_clients()
            .expect("trusted clients should be listable");
        assert_eq!(clients.len(), 1);

        let events = state.drain_mcp_runtime_events();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, McpRuntimeEvent::TrustedClientsUpdated))
        );
    }

    #[test]
    fn mcp_execution_request_updates_pending_queue_event() {
        let mut state = test_state(GeneralSettings::default());

        let pending = state.request_mcp_execution(
            "agent-a".to_string(),
            "conn-a".to_string(),
            "request_execution".to_string(),
            dbflux_policy::ExecutionClassification::Write,
            serde_json::json!({"query": "UPDATE users SET active = true"}),
        );

        assert_eq!(pending.actor_id, "agent-a");

        let events = state.drain_mcp_runtime_events();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, McpRuntimeEvent::PendingExecutionsUpdated))
        );
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn builtin_driver_registry_includes_dynamodb_when_feature_enabled() {
        let drivers = AppState::build_builtin_drivers();

        let driver = drivers
            .get("dynamodb")
            .expect("dynamodb driver should be registered when feature is enabled");

        assert_eq!(driver.metadata().id, "dynamodb");
    }

    #[cfg(not(feature = "dynamodb"))]
    #[test]
    fn builtin_driver_registry_omits_dynamodb_when_feature_disabled() {
        let drivers = AppState::build_builtin_drivers();

        assert!(!drivers.contains_key("dynamodb"));
        assert!(
            !drivers
                .values()
                .any(|driver| driver.metadata().id == "dynamodb"),
            "no registered builtin driver should expose dynamodb metadata when feature is disabled"
        );
    }
}
