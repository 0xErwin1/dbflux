use dbflux_core::{
    AppConfigStore, CancelToken, Connection, ConnectionProfile, DbDriver, DbKind, DbSchemaInfo,
    HistoryEntry, RecentFilesStore, SavedQuery, SchemaForeignKeyInfo, SchemaIndexInfo,
    SchemaSnapshot, ScriptsDirectory, SecretStore, SessionFacade, SessionStore, ShutdownPhase,
    SshTunnelProfile, TaskId, TaskKind, TaskSnapshot,
};
use dbflux_driver_ipc::{IpcDriver, driver::IpcDriverLaunchConfig};
use gpui::{EventEmitter, WindowHandle};
use gpui_component::Root;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use uuid::Uuid;

pub struct AppStateChanged;

#[cfg(feature = "sqlite")]
use dbflux_driver_sqlite::SqliteDriver;

#[cfg(feature = "postgres")]
use dbflux_driver_postgres::PostgresDriver;

#[cfg(feature = "mysql")]
use dbflux_driver_mysql::MysqlDriver;

#[cfg(feature = "mongodb")]
use dbflux_driver_mongodb::MongoDriver;

#[cfg(feature = "redis")]
use dbflux_driver_redis::RedisDriver;

pub use dbflux_core::{
    ConnectProfileParams, ConnectedProfile, DangerousQuerySuppressions, FetchDatabaseSchemaParams,
    FetchSchemaForeignKeysParams, FetchSchemaIndexesParams, FetchSchemaTypesParams,
    FetchTableDetailsParams, SwitchDatabaseParams,
};

fn rpc_registry_id(socket_id: &str) -> String {
    format!("rpc:{}", socket_id)
}

pub struct AppState {
    pub facade: SessionFacade,
    pub settings_window: Option<WindowHandle<Root>>,
    recent_files: Option<RecentFilesStore>,
    scripts_directory: Option<ScriptsDirectory>,
    session_store: Option<SessionStore>,
}

impl AppState {
    pub fn new() -> Self {
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

        let app_config = AppConfigStore::new()
            .and_then(|store| store.load())
            .inspect_err(|e| log::warn!("Failed to load app config: {}", e))
            .ok();

        if let Some(config) = app_config {
            for service in config.rpc_services {
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

                let (kind, metadata, form_definition) =
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

                let ipc_driver =
                    IpcDriver::new(service.socket_id.clone(), kind, metadata, form_definition)
                        .with_launch_config(launch);

                drivers.insert(driver_id, Arc::new(ipc_driver));
            }
        }

        let recent_files = RecentFilesStore::new()
            .inspect_err(|e| log::warn!("Failed to initialize recent files store: {}", e))
            .ok();

        let scripts_directory = ScriptsDirectory::new()
            .inspect_err(|e| log::warn!("Failed to initialize scripts directory: {}", e))
            .ok();

        let session_store = SessionStore::new()
            .inspect_err(|e| log::warn!("Failed to initialize session store: {}", e))
            .ok();

        Self {
            facade: SessionFacade::new(drivers),
            settings_window: None,
            recent_files,
            scripts_directory,
            session_store,
        }
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
        self.facade.connections.prepare_connect_profile(
            profile_id,
            &self.facade.profiles.profiles,
            &self.facade.ssh_tunnels.tunnels,
            &secrets.secret_store_arc(),
            |profile, ssh_tunnels| secrets.get_ssh_secret_for_profile(profile, ssh_tunnels),
        )
    }

    pub fn apply_connect_profile(
        &mut self,
        profile: ConnectionProfile,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
    ) {
        self.facade
            .connections
            .apply_connect_profile(profile, connection, schema);
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

    pub fn save_password(&self, profile: &ConnectionProfile, password: &str) {
        self.facade.secrets.save_password(profile, password);
    }

    pub fn delete_password(&self, profile: &ConnectionProfile) {
        self.facade.secrets.delete_password(profile);
    }

    pub fn get_password(&self, profile: &ConnectionProfile) -> Option<String> {
        self.facade.secrets.get_password(profile)
    }

    pub fn get_ssh_password(&self, profile: &ConnectionProfile) -> Option<String> {
        self.facade.secrets.get_ssh_password(profile)
    }

    pub fn save_ssh_password(&self, profile: &ConnectionProfile, secret: &str) {
        self.facade.secrets.save_ssh_password(profile, secret);
    }

    pub fn delete_ssh_password(&self, profile: &ConnectionProfile) {
        self.facade.secrets.delete_ssh_password(profile);
    }

    pub fn get_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile) -> Option<String> {
        self.facade.secrets.get_ssh_tunnel_secret(tunnel)
    }

    pub fn save_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile, secret: &str) {
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

    pub fn start_task_for_profile(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
        profile_id: Option<Uuid>,
    ) -> (TaskId, CancelToken) {
        self.facade
            .tasks
            .start_for_profile(kind, description, profile_id)
    }

    pub fn complete_task(&mut self, id: TaskId) {
        self.facade.tasks.complete(id);
    }

    pub fn fail_task(&mut self, id: TaskId, error: impl Into<String>) {
        self.facade.tasks.fail(id, error);
    }

    #[allow(dead_code)]
    pub fn cancel_task(&mut self, id: TaskId) -> bool {
        self.facade.tasks.cancel(id)
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
        &self.facade.ssh_tunnels.tunnels
    }

    pub fn connections(&self) -> &HashMap<Uuid, ConnectedProfile> {
        &self.facade.connections.connections
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
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl EventEmitter<AppStateChanged> for AppState {}

#[cfg(test)]
mod tests {
    use super::AppState;

    #[test]
    fn saved_query_store_is_optional() {
        let state = AppState::new();
        let _ = state.saved_queries();
    }
}
