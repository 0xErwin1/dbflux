use dbflux_core::{
    CancelToken, Connection, ConnectionProfile, ConnectionTree, ConnectionTreeNode,
    ConnectionTreeStore, CustomTypeInfo, DbConfig, DbDriver, DbKind, DbSchemaInfo, HistoryEntry,
    HistoryStore, ProfileStore, SavedQuery, SavedQueryStore, SchemaForeignKeyInfo, SchemaIndexInfo,
    SchemaLoadingStrategy, SchemaSnapshot, SecretStore, ShutdownCoordinator, ShutdownPhase,
    SshTunnelProfile, SshTunnelStore, TableInfo, TaskId, TaskKind, TaskManager, TaskSnapshot,
    create_secret_store,
};
use gpui::{EventEmitter, WindowHandle};
use gpui_component::Root;
use log::{error, info};
use std::collections::{HashMap, HashSet};
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PendingOperation {
    pub profile_id: Uuid,
    pub database: Option<String>,
}

pub struct ConnectedProfile {
    pub profile: ConnectionProfile,
    pub connection: Arc<dyn Connection>,
    pub schema: Option<SchemaSnapshot>,
    /// Lazy-loaded schemas per database (MySQL/MariaDB).
    pub database_schemas: HashMap<String, DbSchemaInfo>,
    #[allow(dead_code)]
    pub table_details: HashMap<(String, String), TableInfo>,
    /// Lazy-loaded custom types per schema (key: "database__schema" or just "schema").
    pub schema_types: HashMap<String, Vec<CustomTypeInfo>>,
    /// Lazy-loaded indexes per schema (key: "database__schema" or just "schema").
    pub schema_indexes: HashMap<String, Vec<SchemaIndexInfo>>,
    /// Lazy-loaded foreign keys per schema (key: "database__schema" or just "schema").
    pub schema_foreign_keys: HashMap<String, Vec<SchemaForeignKeyInfo>>,
    /// Active database for query context (MySQL/MariaDB USE).
    pub active_database: Option<String>,
}

/// Session-based suppressions for dangerous query confirmations.
/// TODO: Re-integrate with SqlQueryDocument.
#[allow(dead_code)]
#[derive(Default)]
pub struct DangerousQuerySuppressions {
    delete_no_where: bool,
    update_no_where: bool,
    truncate: bool,
    drop: bool,
    alter: bool,
    script: bool,
}

impl DangerousQuerySuppressions {
    #[allow(dead_code)]
    pub fn is_suppressed(&self, kind: crate::ui::dangerous_query::DangerousQueryKind) -> bool {
        use crate::ui::dangerous_query::DangerousQueryKind;
        match kind {
            DangerousQueryKind::DeleteNoWhere => self.delete_no_where,
            DangerousQueryKind::UpdateNoWhere => self.update_no_where,
            DangerousQueryKind::Truncate => self.truncate,
            DangerousQueryKind::Drop => self.drop,
            DangerousQueryKind::Alter => self.alter,
            DangerousQueryKind::Script => self.script,
        }
    }

    #[allow(dead_code)]
    pub fn set_suppressed(&mut self, kind: crate::ui::dangerous_query::DangerousQueryKind) {
        use crate::ui::dangerous_query::DangerousQueryKind;
        match kind {
            DangerousQueryKind::DeleteNoWhere => self.delete_no_where = true,
            DangerousQueryKind::UpdateNoWhere => self.update_no_where = true,
            DangerousQueryKind::Truncate => self.truncate = true,
            DangerousQueryKind::Drop => self.drop = true,
            DangerousQueryKind::Alter => self.alter = true,
            DangerousQueryKind::Script => self.script = true,
        }
    }
}

pub struct AppState {
    pub drivers: HashMap<DbKind, Arc<dyn DbDriver>>,
    pub profiles: Vec<ConnectionProfile>,
    pub ssh_tunnels: Vec<SshTunnelProfile>,
    pub connections: HashMap<Uuid, ConnectedProfile>,
    pub active_connection_id: Option<Uuid>,
    pub pending_operations: HashSet<PendingOperation>,
    pub tasks: TaskManager,
    profile_store: Option<ProfileStore>,
    ssh_tunnel_store: Option<SshTunnelStore>,
    secret_store: Arc<RwLock<Box<dyn SecretStore>>>,
    history_store: Option<HistoryStore>,
    saved_query_store: Option<SavedQueryStore>,
    #[allow(dead_code)]
    pending_saved_query_warning: Option<String>,
    #[allow(dead_code)]
    pub dangerous_query_suppressions: DangerousQuerySuppressions,

    pub settings_window: Option<WindowHandle<Root>>,

    /// Hierarchical folder organization for connection profiles.
    pub connection_tree: ConnectionTree,
    connection_tree_store: Option<ConnectionTreeStore>,

    /// Graceful shutdown coordinator.
    pub shutdown: ShutdownCoordinator,
}

impl AppState {
    /// Get read lock on secret store, recovering from poison errors.
    fn secret_store_read(&self) -> std::sync::RwLockReadGuard<'_, Box<dyn SecretStore>> {
        match self.secret_store.read() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned, recovering...");
                poison_err.into_inner()
            }
        }
    }

    pub fn new() -> Self {
        let mut drivers: HashMap<DbKind, Arc<dyn DbDriver>> = HashMap::new();

        #[cfg(feature = "sqlite")]
        {
            drivers.insert(DbKind::SQLite, Arc::new(SqliteDriver::new()));
        }

        #[cfg(feature = "postgres")]
        {
            drivers.insert(DbKind::Postgres, Arc::new(PostgresDriver::new()));
        }

        #[cfg(feature = "mysql")]
        {
            drivers.insert(DbKind::MySQL, Arc::new(MysqlDriver::new(DbKind::MySQL)));
            drivers.insert(DbKind::MariaDB, Arc::new(MysqlDriver::new(DbKind::MariaDB)));
        }

        #[cfg(feature = "mongodb")]
        {
            drivers.insert(DbKind::MongoDB, Arc::new(MongoDriver::new()));
        }

        let (profile_store, profiles) = match ProfileStore::new() {
            Ok(store) => {
                let profiles = store.load().unwrap_or_else(|e| {
                    error!("Failed to load profiles: {:?}", e);
                    Vec::new()
                });
                info!("Loaded {} profiles from disk", profiles.len());
                (Some(store), profiles)
            }
            Err(e) => {
                error!("Failed to create profile store: {:?}", e);
                error!("Application will run without persistent profile storage");
                (None, Vec::new())
            }
        };

        let (ssh_tunnel_store, ssh_tunnels) = match SshTunnelStore::new() {
            Ok(store) => {
                let tunnels = store.load().unwrap_or_else(|e| {
                    error!("Failed to load SSH tunnels: {:?}", e);
                    Vec::new()
                });
                info!("Loaded {} SSH tunnel profiles from disk", tunnels.len());
                (Some(store), tunnels)
            }
            Err(e) => {
                error!("Failed to create SSH tunnel store: {:?}", e);
                (None, Vec::new())
            }
        };

        let secret_store = create_secret_store();
        info!("Secret store available: {}", secret_store.is_available());

        let history_store = match HistoryStore::new() {
            Ok(store) => {
                info!("Loaded {} history entries", store.entries().len());
                Some(store)
            }
            Err(e) => {
                error!("Failed to create history store: {:?}", e);
                None
            }
        };

        let (saved_query_store, pending_saved_query_warning) = match SavedQueryStore::new() {
            Ok(mut store) => {
                let warning = store.take_load_warning();
                info!("Loaded {} saved queries", store.get_all().len());
                (Some(store), warning)
            }
            Err(e) => {
                error!("Failed to create saved query store: {:?}", e);
                (None, None)
            }
        };

        let (connection_tree_store, mut connection_tree) = match ConnectionTreeStore::new() {
            Ok(store) => {
                let tree = store.load().unwrap_or_else(|e| {
                    error!("Failed to load connection tree: {:?}", e);
                    ConnectionTree::new()
                });
                info!("Loaded connection tree with {} nodes", tree.nodes.len());
                (Some(store), tree)
            }
            Err(e) => {
                error!("Failed to create connection tree store: {:?}", e);
                (None, ConnectionTree::new())
            }
        };

        // Sync tree with profiles to handle orphaned nodes and new profiles
        let profile_ids: Vec<Uuid> = profiles.iter().map(|p| p.id).collect();
        let nodes_before = connection_tree.nodes.len();
        connection_tree.sync_with_profiles(&profile_ids);
        let nodes_after = connection_tree.nodes.len();

        // Save tree if sync made changes (added or removed nodes)
        if nodes_before != nodes_after {
            if let Some(ref store) = connection_tree_store {
                if let Err(e) = store.save(&connection_tree) {
                    error!("Failed to save connection tree after sync: {:?}", e);
                } else {
                    info!(
                        "Synced connection tree: {} -> {} nodes",
                        nodes_before, nodes_after
                    );
                }
            }
        }

        Self {
            drivers,
            profiles,
            ssh_tunnels,
            connections: HashMap::new(),
            active_connection_id: None,
            pending_operations: HashSet::new(),
            tasks: TaskManager::new(),
            profile_store,
            ssh_tunnel_store,
            secret_store: Arc::new(RwLock::new(secret_store)),
            history_store,
            saved_query_store,
            pending_saved_query_warning,
            dangerous_query_suppressions: DangerousQuerySuppressions::default(),
            settings_window: None,
            connection_tree,
            connection_tree_store,
            shutdown: ShutdownCoordinator::new(),
        }
    }

    pub fn active_connection(&self) -> Option<&ConnectedProfile> {
        self.active_connection_id
            .and_then(|id| self.connections.get(&id))
    }

    #[allow(dead_code)]
    pub fn is_connected(&self) -> bool {
        self.active_connection_id.is_some()
    }

    /// Returns true if there are any open connections.
    pub fn has_connections(&self) -> bool {
        !self.connections.is_empty()
    }

    #[allow(dead_code)]
    pub fn connection_display_name(&self) -> Option<&str> {
        self.active_connection().map(|c| c.profile.name.as_str())
    }

    #[allow(dead_code)]
    pub fn active_schema(&self) -> Option<&SchemaSnapshot> {
        self.active_connection().and_then(|c| c.schema.as_ref())
    }

    pub fn get_connection(&self, profile_id: Uuid) -> Option<Arc<dyn Connection>> {
        self.connections
            .get(&profile_id)
            .map(|c| c.connection.clone())
    }

    pub fn set_active_connection(&mut self, profile_id: Uuid) {
        if self.connections.contains_key(&profile_id) {
            self.active_connection_id = Some(profile_id);
        }
    }

    pub fn add_connection(
        &mut self,
        profile: ConnectionProfile,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
    ) {
        let id = profile.id;
        self.connections.insert(
            id,
            ConnectedProfile {
                profile,
                connection,
                schema,
                database_schemas: HashMap::new(),
                table_details: HashMap::new(),
                schema_types: HashMap::new(),
                schema_indexes: HashMap::new(),
                schema_foreign_keys: HashMap::new(),
                active_database: None,
            },
        );
        self.active_connection_id = Some(id);
    }

    pub fn disconnect(&mut self, profile_id: Uuid) {
        if let Some(mut connected) = self.connections.remove(&profile_id)
            && let Some(conn) = Arc::get_mut(&mut connected.connection)
            && let Err(e) = conn.close()
        {
            log::warn!(
                "Failed to close connection for {}: {:?}",
                connected.profile.name,
                e
            );
        }

        if self.active_connection_id == Some(profile_id) {
            self.active_connection_id = self.connections.keys().next().copied();
        }
    }

    #[allow(dead_code)]
    pub fn disconnect_all(&mut self) {
        let ids: Vec<Uuid> = self.connections.keys().copied().collect();
        for id in ids {
            self.disconnect(id);
        }
    }

    // --- Shutdown methods ---

    /// Begin graceful shutdown.
    ///
    /// Returns `true` if this call initiated shutdown, `false` if already shutting down.
    pub fn begin_shutdown(&self) -> bool {
        self.shutdown.request_shutdown()
    }

    /// Check if shutdown has been requested.
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.is_shutdown_requested()
    }

    /// Get the current shutdown phase.
    pub fn shutdown_phase(&self) -> ShutdownPhase {
        self.shutdown.phase()
    }

    /// Cancel all running tasks.
    ///
    /// Returns the number of tasks that were cancelled.
    pub fn cancel_all_tasks(&mut self) -> usize {
        if !self
            .shutdown
            .advance_phase(ShutdownPhase::SignalSent, ShutdownPhase::CancellingTasks)
        {
            return 0;
        }

        let count = self.tasks.cancel_all();
        info!("Cancelled {} running tasks during shutdown", count);
        count
    }

    /// Close all database connections.
    ///
    /// Cancels any active queries first, then closes the connection.
    /// Logs errors but continues closing other connections.
    pub fn close_all_connections(&mut self) {
        if !self.shutdown.advance_phase(
            ShutdownPhase::CancellingTasks,
            ShutdownPhase::ClosingConnections,
        ) {
            return;
        }

        let ids: Vec<Uuid> = self.connections.keys().copied().collect();
        let count = ids.len();

        for id in ids {
            if let Some(mut connected) = self.connections.remove(&id) {
                let name = connected.profile.name.clone();

                // Cancel any active query first
                if let Err(e) = connected.connection.cancel_active() {
                    log::debug!(
                        "Could not cancel active query for {} (may not have one): {:?}",
                        name,
                        e
                    );
                }

                // Close the connection
                if let Some(conn) = Arc::get_mut(&mut connected.connection) {
                    if let Err(e) = conn.close() {
                        error!("Failed to close connection for {}: {:?}", name, e);
                    } else {
                        info!("Closed connection: {}", name);
                    }
                } else {
                    log::warn!(
                        "Could not get exclusive access to connection {} for close",
                        name
                    );
                }
            }
        }

        info!("Closed {} connections during shutdown", count);
        self.active_connection_id = None;
    }

    /// Mark shutdown as complete.
    pub fn complete_shutdown(&self) {
        self.shutdown.complete();
    }

    /// Mark shutdown as failed.
    #[allow(dead_code)]
    pub fn fail_shutdown(&self) {
        self.shutdown.fail();
    }

    // --- Lazy schema cache ---

    #[allow(dead_code)]
    pub fn get_database_schema(&self, profile_id: Uuid, database: &str) -> Option<&DbSchemaInfo> {
        self.connections
            .get(&profile_id)
            .and_then(|c| c.database_schemas.get(database))
    }

    pub fn set_database_schema(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: DbSchemaInfo,
    ) {
        if let Some(connected) = self.connections.get_mut(&profile_id) {
            connected.database_schemas.insert(database, schema);
        }
    }

    pub fn needs_database_schema(&self, profile_id: Uuid, database: &str) -> bool {
        self.connections
            .get(&profile_id)
            .is_some_and(|c| !c.database_schemas.contains_key(database))
    }

    #[allow(dead_code)]
    pub fn get_table_details(
        &self,
        profile_id: Uuid,
        database: &str,
        table: &str,
    ) -> Option<&TableInfo> {
        self.connections.get(&profile_id).and_then(|c| {
            c.table_details
                .get(&(database.to_string(), table.to_string()))
        })
    }

    #[allow(dead_code)]
    pub fn set_table_details(
        &mut self,
        profile_id: Uuid,
        database: String,
        table: String,
        details: TableInfo,
    ) {
        if let Some(connected) = self.connections.get_mut(&profile_id) {
            connected.table_details.insert((database, table), details);
        }
    }

    #[allow(dead_code)]
    pub fn needs_table_details(&self, profile_id: Uuid, database: &str, table: &str) -> bool {
        self.connections.get(&profile_id).is_some_and(|c| {
            !c.table_details
                .contains_key(&(database.to_string(), table.to_string()))
        })
    }

    /// Returns the cache key for schema types (database__schema or just schema).
    fn schema_types_key(database: &str, schema: Option<&str>) -> String {
        match schema {
            Some(s) => format!("{}__{}", database, s),
            None => database.to_string(),
        }
    }

    #[allow(dead_code)]
    pub fn get_schema_types(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Option<&Vec<CustomTypeInfo>> {
        let key = Self::schema_types_key(database, schema);
        self.connections
            .get(&profile_id)
            .and_then(|c| c.schema_types.get(&key))
    }

    pub fn set_schema_types(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        types: Vec<CustomTypeInfo>,
    ) {
        let key = Self::schema_types_key(&database, schema.as_deref());
        if let Some(connected) = self.connections.get_mut(&profile_id) {
            connected.schema_types.insert(key, types);
        }
    }

    pub fn needs_schema_types(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        let key = Self::schema_types_key(database, schema);
        self.connections
            .get(&profile_id)
            .is_some_and(|c| !c.schema_types.contains_key(&key))
    }

    pub fn prepare_fetch_schema_types(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<FetchSchemaTypesParams, String> {
        let connected = self
            .connections
            .get(&profile_id)
            .ok_or_else(|| "Profile not connected".to_string())?;

        let key = Self::schema_types_key(database, schema);
        if connected.schema_types.contains_key(&key) {
            return Err("Schema types already cached".to_string());
        }

        Ok(FetchSchemaTypesParams {
            profile_id,
            database: database.to_string(),
            schema: schema.map(String::from),
            connection: connected.connection.clone(),
        })
    }

    // Schema-level indexes cache methods

    pub fn set_schema_indexes(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        indexes: Vec<SchemaIndexInfo>,
    ) {
        let key = Self::schema_types_key(&database, schema.as_deref());
        if let Some(connected) = self.connections.get_mut(&profile_id) {
            connected.schema_indexes.insert(key, indexes);
        }
    }

    pub fn needs_schema_indexes(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        let key = Self::schema_types_key(database, schema);
        self.connections
            .get(&profile_id)
            .is_some_and(|c| !c.schema_indexes.contains_key(&key))
    }

    pub fn prepare_fetch_schema_indexes(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<FetchSchemaIndexesParams, String> {
        let connected = self
            .connections
            .get(&profile_id)
            .ok_or_else(|| "Profile not connected".to_string())?;

        let key = Self::schema_types_key(database, schema);
        if connected.schema_indexes.contains_key(&key) {
            return Err("Schema indexes already cached".to_string());
        }

        Ok(FetchSchemaIndexesParams {
            profile_id,
            database: database.to_string(),
            schema: schema.map(String::from),
            connection: connected.connection.clone(),
        })
    }

    // Schema-level foreign keys cache methods

    pub fn set_schema_foreign_keys(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        foreign_keys: Vec<SchemaForeignKeyInfo>,
    ) {
        let key = Self::schema_types_key(&database, schema.as_deref());
        if let Some(connected) = self.connections.get_mut(&profile_id) {
            connected.schema_foreign_keys.insert(key, foreign_keys);
        }
    }

    pub fn needs_schema_foreign_keys(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        let key = Self::schema_types_key(database, schema);
        self.connections
            .get(&profile_id)
            .is_some_and(|c| !c.schema_foreign_keys.contains_key(&key))
    }

    pub fn prepare_fetch_schema_foreign_keys(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Result<FetchSchemaForeignKeysParams, String> {
        let connected = self
            .connections
            .get(&profile_id)
            .ok_or_else(|| "Profile not connected".to_string())?;

        let key = Self::schema_types_key(database, schema);
        if connected.schema_foreign_keys.contains_key(&key) {
            return Err("Schema foreign keys already cached".to_string());
        }

        Ok(FetchSchemaForeignKeysParams {
            profile_id,
            database: database.to_string(),
            schema: schema.map(String::from),
            connection: connected.connection.clone(),
        })
    }

    #[allow(dead_code)]
    pub fn get_active_database(&self, profile_id: Uuid) -> Option<String> {
        self.connections
            .get(&profile_id)
            .and_then(|c| c.active_database.clone())
    }

    pub fn set_active_database(&mut self, profile_id: Uuid, database: Option<String>) {
        if let Some(connected) = self.connections.get_mut(&profile_id) {
            connected.active_database = database;
        }
    }

    pub fn add_profile_in_folder(&mut self, profile: ConnectionProfile, folder_id: Option<Uuid>) {
        let profile_id = profile.id;
        self.profiles.push(profile);
        self.save_profiles();

        // Add to connection tree if not already present
        if self.connection_tree.find_by_profile(profile_id).is_none() {
            let sort_index = self.connection_tree.next_sort_index(folder_id);
            let node = ConnectionTreeNode::new_connection_ref(profile_id, folder_id, sort_index);
            self.connection_tree.add_node(node);
            self.save_connection_tree();
        }
    }

    pub fn remove_profile(&mut self, idx: usize) -> Option<ConnectionProfile> {
        if idx < self.profiles.len() {
            let removed = self.profiles.remove(idx);
            self.disconnect(removed.id);
            self.delete_password(&removed);
            self.save_profiles();

            // Remove from connection tree
            if let Some(node) = self.connection_tree.find_by_profile(removed.id) {
                let node_id = node.id;
                self.connection_tree.remove_node(node_id);
                self.save_connection_tree();
            }

            Some(removed)
        } else {
            None
        }
    }

    pub fn update_profile(&mut self, profile: ConnectionProfile) {
        if let Some(existing) = self.profiles.iter_mut().find(|p| p.id == profile.id) {
            *existing = profile;
            self.save_profiles();
        }
    }

    pub fn save_profiles(&self) {
        let Some(ref profile_store) = self.profile_store else {
            log::warn!("Cannot save profiles: profile store not available");
            return;
        };

        if let Err(e) = profile_store.save(&self.profiles) {
            error!("Failed to save profiles: {:?}", e);
        } else {
            info!("Saved {} profiles to disk", self.profiles.len());
        }
    }

    pub fn save_connection_tree(&self) {
        let Some(ref store) = self.connection_tree_store else {
            log::warn!("Cannot save connection tree: store not available");
            return;
        };

        if let Err(e) = store.save(&self.connection_tree) {
            error!("Failed to save connection tree: {:?}", e);
        } else {
            info!(
                "Saved connection tree with {} nodes",
                self.connection_tree.nodes.len()
            );
        }
    }

    /// Creates a new folder in the connection tree.
    ///
    /// Returns the ID of the newly created folder.
    pub fn create_folder(&mut self, name: impl Into<String>, parent_id: Option<Uuid>) -> Uuid {
        let sort_index = self.connection_tree.next_sort_index(parent_id);
        let folder = ConnectionTreeNode::new_folder(name, parent_id, sort_index);
        let folder_id = folder.id;
        self.connection_tree.add_node(folder);
        self.save_connection_tree();
        folder_id
    }

    /// Renames a folder in the connection tree.
    ///
    /// Returns `true` if the folder was found and renamed.
    pub fn rename_folder(&mut self, folder_id: Uuid, new_name: impl Into<String>) -> bool {
        if self.connection_tree.rename_folder(folder_id, new_name) {
            self.save_connection_tree();
            true
        } else {
            false
        }
    }

    /// Deletes a folder, moving its children to the folder's parent (or root).
    ///
    /// Returns the IDs of children that were moved.
    pub fn delete_folder(&mut self, folder_id: Uuid) -> Vec<Uuid> {
        let moved = self
            .connection_tree
            .delete_folder_and_reparent_children(folder_id);
        if !moved.is_empty() || self.connection_tree.find_by_id(folder_id).is_none() {
            self.save_connection_tree();
        }
        moved
    }

    /// Moves a node (folder or connection) to a new parent.
    ///
    /// Returns `true` if the move was successful, `false` if it would create a cycle.
    pub fn move_tree_node(&mut self, node_id: Uuid, new_parent_id: Option<Uuid>) -> bool {
        if self.connection_tree.move_node(node_id, new_parent_id) {
            self.save_connection_tree();
            true
        } else {
            false
        }
    }

    /// Moves a node to a specific position within a parent.
    ///
    /// - `new_parent_id`: The target parent (`None` for root).
    /// - `after_id`: Insert after this sibling (`None` to insert at the beginning).
    pub fn move_tree_node_to_position(
        &mut self,
        node_id: Uuid,
        new_parent_id: Option<Uuid>,
        after_id: Option<Uuid>,
    ) -> bool {
        if self
            .connection_tree
            .move_node_to_position(node_id, new_parent_id, after_id)
        {
            self.save_connection_tree();
            true
        } else {
            false
        }
    }

    /// Toggles the collapsed state of a folder.
    ///
    /// Returns the new collapsed state, or `None` if the folder wasn't found.
    #[allow(dead_code)]
    pub fn toggle_folder_collapsed(&mut self, folder_id: Uuid) -> Option<bool> {
        let result = self.connection_tree.toggle_folder_collapsed(folder_id);
        if result.is_some() {
            self.save_connection_tree();
        }
        result
    }

    /// Sets the collapsed state of a folder.
    pub fn set_folder_collapsed(&mut self, folder_id: Uuid, collapsed: bool) {
        self.connection_tree
            .set_folder_collapsed(folder_id, collapsed);
        self.save_connection_tree();
    }

    pub fn add_ssh_tunnel(&mut self, tunnel: SshTunnelProfile) {
        self.ssh_tunnels.push(tunnel);
        self.save_ssh_tunnels();
    }

    #[allow(dead_code)]
    pub fn remove_ssh_tunnel(&mut self, idx: usize) -> Option<SshTunnelProfile> {
        if idx < self.ssh_tunnels.len() {
            let removed = self.ssh_tunnels.remove(idx);
            self.delete_ssh_tunnel_secret(&removed);
            self.save_ssh_tunnels();
            Some(removed)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn update_ssh_tunnel(&mut self, tunnel: SshTunnelProfile) {
        if let Some(existing) = self.ssh_tunnels.iter_mut().find(|t| t.id == tunnel.id) {
            *existing = tunnel;
            self.save_ssh_tunnels();
        }
    }

    pub fn save_ssh_tunnels(&self) {
        let Some(ref store) = self.ssh_tunnel_store else {
            log::warn!("Cannot save SSH tunnels: store not available");
            return;
        };

        if let Err(e) = store.save(&self.ssh_tunnels) {
            error!("Failed to save SSH tunnels: {:?}", e);
        } else {
            info!("Saved {} SSH tunnels to disk", self.ssh_tunnels.len());
        }
    }

    pub fn get_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile) -> Option<String> {
        match self.secret_store_read().get(&tunnel.secret_ref()) {
            Ok(secret) => secret,
            Err(e) => {
                error!("Failed to get SSH tunnel secret: {:?}", e);
                None
            }
        }
    }

    pub fn save_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile, secret: &str) {
        let store = self.secret_store_read();

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.set(&tunnel.secret_ref(), secret) {
            error!("Failed to save SSH tunnel secret: {:?}", e);
        }
    }

    pub fn delete_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile) {
        let store = self.secret_store_read();

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.delete(&tunnel.secret_ref()) {
            log::warn!("Failed to delete SSH tunnel secret: {:?}", e);
        }
    }

    pub fn secret_store_available(&self) -> bool {
        self.secret_store_read().is_available()
    }

    #[allow(dead_code)]
    pub fn secret_store(&self) -> Arc<RwLock<Box<dyn SecretStore>>> {
        self.secret_store.clone()
    }

    pub fn save_password(&self, profile: &ConnectionProfile, password: &str) {
        if !profile.save_password {
            return;
        }

        let store = self.secret_store_read();

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.set(&profile.secret_ref(), password) {
            error!("Failed to save password: {:?}", e);
        }
    }

    pub fn delete_password(&self, profile: &ConnectionProfile) {
        if let Err(e) = self.secret_store_read().delete(&profile.secret_ref()) {
            error!("Failed to delete password: {:?}", e);
        }
    }

    pub fn get_ssh_password(&self, profile: &ConnectionProfile) -> Option<String> {
        let store = self.secret_store_read();

        if !store.is_available() {
            return None;
        }

        match store.get(&profile.ssh_secret_ref()) {
            Ok(secret) => secret,
            Err(e) => {
                error!("Failed to get SSH secret: {:?}", e);
                None
            }
        }
    }

    pub fn save_ssh_password(&self, profile: &ConnectionProfile, secret: &str) {
        let store = self.secret_store_read();

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.set(&profile.ssh_secret_ref(), secret) {
            error!("Failed to save SSH secret: {:?}", e);
        }
    }

    pub fn delete_ssh_password(&self, profile: &ConnectionProfile) {
        if let Err(e) = self.secret_store_read().delete(&profile.ssh_secret_ref()) {
            error!("Failed to delete SSH secret: {:?}", e);
        }
    }

    #[allow(dead_code)]
    pub fn take_saved_query_warning(&mut self) -> Option<String> {
        self.pending_saved_query_warning.take()
    }

    pub fn add_saved_query(&mut self, query: SavedQuery) {
        if let Some(ref mut store) = self.saved_query_store {
            store.add(query);
            if let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
        }
    }

    pub fn update_saved_query(&mut self, id: Uuid, name: String, sql: String) -> bool {
        if let Some(ref mut store) = self.saved_query_store {
            let updated = store.update(id, name, sql);
            if updated && let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
            return updated;
        }
        false
    }

    pub fn remove_saved_query(&mut self, id: Uuid) -> bool {
        if let Some(ref mut store) = self.saved_query_store {
            let removed = store.remove(id);
            if removed && let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
            return removed;
        }
        false
    }

    pub fn toggle_saved_query_favorite(&mut self, id: Uuid) -> bool {
        if let Some(ref mut store) = self.saved_query_store {
            let result = store.toggle_favorite(id);
            if let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
            return result;
        }
        false
    }

    pub fn update_saved_query_last_used(&mut self, id: Uuid) -> bool {
        if let Some(ref mut store) = self.saved_query_store {
            let result = store.update_last_used(id);
            if result && let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
            return result;
        }
        false
    }

    #[allow(dead_code)]
    pub fn update_saved_query_sql(&mut self, id: Uuid, sql: &str) -> bool {
        if let Some(ref mut store) = self.saved_query_store {
            let result = store.update_sql(id, sql);
            if result && let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
            return result;
        }
        false
    }

    #[allow(dead_code)]
    pub fn update_saved_query_name(&mut self, id: Uuid, name: &str) -> bool {
        if let Some(ref mut store) = self.saved_query_store {
            let result = store.update_name(id, name);
            if result && let Err(e) = store.save() {
                error!("Failed to save saved queries: {:?}", e);
            }
            return result;
        }
        false
    }

    #[allow(dead_code)]
    pub fn get_saved_query(&self, id: Uuid) -> Option<&SavedQuery> {
        self.saved_query_store.as_ref().and_then(|s| s.get(id))
    }

    pub fn saved_queries(&self) -> &[SavedQuery] {
        self.saved_query_store
            .as_ref()
            .map(|s| s.get_all())
            .unwrap_or(&[])
    }

    pub fn prepare_connect_profile(
        &self,
        profile_id: Uuid,
    ) -> Result<ConnectProfileParams, String> {
        let profile = self
            .profiles
            .iter()
            .find(|p| p.id == profile_id)
            .cloned()
            .ok_or_else(|| "Profile not found".to_string())?;

        if self.connections.contains_key(&profile_id) {
            return Err("Already connected".to_string());
        }

        let kind = profile.kind();
        let driver = self
            .drivers
            .get(&kind)
            .cloned()
            .ok_or_else(|| format!("No driver for {:?}", kind))?;

        let secret_store = if kind == DbKind::SQLite {
            None
        } else {
            Some(self.secret_store.clone())
        };

        let ssh_secret = self.get_ssh_secret_for_profile(&profile);

        Ok(ConnectProfileParams {
            profile,
            driver,
            secret_store,
            ssh_secret,
        })
    }

    fn get_ssh_secret_for_profile(&self, profile: &ConnectionProfile) -> Option<String> {
        // Extract SSH-related fields from config
        let (ssh_tunnel, ssh_tunnel_profile_id) = match &profile.config {
            DbConfig::Postgres {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => (ssh_tunnel.as_ref(), *ssh_tunnel_profile_id),
            DbConfig::MySQL {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => (ssh_tunnel.as_ref(), *ssh_tunnel_profile_id),
            DbConfig::MongoDB {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => (ssh_tunnel.as_ref(), *ssh_tunnel_profile_id),
            DbConfig::SQLite { .. } => return None,
        };

        // If using a saved tunnel profile, get secret from there
        if let Some(tunnel_profile_id) = ssh_tunnel_profile_id {
            let tunnel = self
                .ssh_tunnels
                .iter()
                .find(|t| t.id == tunnel_profile_id)?;

            if !tunnel.save_secret {
                return None;
            }

            return self.get_ssh_tunnel_secret(tunnel);
        }

        // If using inline SSH config, get secret from profile's SSH secret store
        if ssh_tunnel.is_some() {
            return self.get_ssh_password(profile);
        }

        None
    }

    pub fn apply_connect_profile(
        &mut self,
        profile: ConnectionProfile,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
    ) {
        self.add_connection(profile, connection, schema);
    }

    pub fn is_operation_pending(&self, profile_id: Uuid, database: Option<&str>) -> bool {
        self.pending_operations.contains(&PendingOperation {
            profile_id,
            database: database.map(|s| s.to_string()),
        })
    }

    pub fn start_pending_operation(&mut self, profile_id: Uuid, database: Option<&str>) -> bool {
        let op = PendingOperation {
            profile_id,
            database: database.map(|s| s.to_string()),
        };
        self.pending_operations.insert(op)
    }

    pub fn finish_pending_operation(&mut self, profile_id: Uuid, database: Option<&str>) {
        let op = PendingOperation {
            profile_id,
            database: database.map(|s| s.to_string()),
        };
        self.pending_operations.remove(&op);
    }

    pub fn history_entries(&self) -> &[HistoryEntry] {
        self.history_store
            .as_ref()
            .map(|s| s.entries())
            .unwrap_or(&[])
    }

    pub fn add_history_entry(&mut self, entry: HistoryEntry) {
        if let Some(ref mut store) = self.history_store {
            store.add(entry);
            if let Err(e) = store.save() {
                error!("Failed to save history: {:?}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub fn toggle_history_favorite(&mut self, id: Uuid) -> bool {
        if let Some(ref mut store) = self.history_store {
            let result = store.toggle_favorite(id);
            if let Err(e) = store.save() {
                error!("Failed to save history: {:?}", e);
            }
            return result;
        }
        false
    }

    pub fn start_task(
        &mut self,
        kind: TaskKind,
        description: impl Into<String>,
    ) -> (TaskId, CancelToken) {
        self.tasks.start(kind, description)
    }

    pub fn complete_task(&mut self, id: TaskId) {
        self.tasks.complete(id);
    }

    pub fn fail_task(&mut self, id: TaskId, error: impl Into<String>) {
        self.tasks.fail(id, error);
    }

    #[allow(dead_code)]
    pub fn cancel_task(&mut self, id: TaskId) -> bool {
        self.tasks.cancel(id)
    }

    #[allow(dead_code)]
    pub fn running_tasks(&self) -> Vec<TaskSnapshot> {
        self.tasks.running_tasks()
    }

    pub fn has_running_tasks(&self) -> bool {
        self.tasks.has_running_tasks()
    }

    #[allow(dead_code)]
    pub fn remove_history_entry(&mut self, id: Uuid) {
        if let Some(ref mut store) = self.history_store {
            store.remove(id);
            if let Err(e) = store.save() {
                error!("Failed to save history: {:?}", e);
            }
        }
    }

    pub fn prepare_switch_database(
        &self,
        profile_id: Uuid,
        database: &str,
    ) -> Result<SwitchDatabaseParams, String> {
        let connected = self
            .connections
            .get(&profile_id)
            .ok_or_else(|| "Profile not connected".to_string())?;

        if connected.profile.kind() != DbKind::Postgres {
            return Err("Database switching only supported for PostgreSQL".to_string());
        }

        if let Some(ref schema) = connected.schema
            && schema.current_database() == Some(database)
        {
            return Err("Already connected to this database".to_string());
        }

        let mut new_profile = connected.profile.clone();
        if let DbConfig::Postgres {
            database: ref mut db,
            ..
        } = new_profile.config
        {
            *db = database.to_string();
        }

        let driver = self
            .drivers
            .get(&DbKind::Postgres)
            .cloned()
            .ok_or_else(|| "PostgreSQL driver not available".to_string())?;

        let original_profile = connected.profile.clone();

        Ok(SwitchDatabaseParams {
            profile_id,
            database: database.to_string(),
            new_profile,
            original_profile,
            driver,
            secret_store: self.secret_store.clone(),
        })
    }

    pub fn apply_switch_database(
        &mut self,
        profile_id: Uuid,
        original_profile: ConnectionProfile,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
    ) {
        self.connections.insert(
            profile_id,
            ConnectedProfile {
                profile: original_profile,
                connection,
                schema,
                database_schemas: HashMap::new(),
                table_details: HashMap::new(),
                schema_types: HashMap::new(),
                schema_indexes: HashMap::new(),
                schema_foreign_keys: HashMap::new(),
                active_database: None,
            },
        );
    }

    /// Fetch schema for a database without reconnecting.
    /// Supported for drivers with LazyPerDatabase loading strategy (MySQL, MariaDB, MongoDB).
    pub fn prepare_fetch_database_schema(
        &self,
        profile_id: Uuid,
        database: &str,
    ) -> Result<FetchDatabaseSchemaParams, String> {
        let connected = self
            .connections
            .get(&profile_id)
            .ok_or_else(|| "Profile not connected".to_string())?;

        // Only for drivers that support lazy per-database loading
        let strategy = connected.connection.schema_loading_strategy();
        if strategy != SchemaLoadingStrategy::LazyPerDatabase {
            return Err(format!(
                "Database schema fetch not supported for {:?} strategy",
                strategy
            ));
        }

        // Check if already cached
        if connected.database_schemas.contains_key(database) {
            return Err("Schema already cached".to_string());
        }

        Ok(FetchDatabaseSchemaParams {
            profile_id,
            database: database.to_string(),
            connection: connected.connection.clone(),
        })
    }

    #[allow(dead_code)]
    pub fn prepare_fetch_table_details(
        &self,
        profile_id: Uuid,
        database: &str,
        table: &str,
    ) -> Result<FetchTableDetailsParams, String> {
        let connected = self
            .connections
            .get(&profile_id)
            .ok_or_else(|| "Profile not connected".to_string())?;

        // Check if already cached
        let key = (database.to_string(), table.to_string());
        if connected.table_details.contains_key(&key) {
            return Err("Table details already cached".to_string());
        }

        Ok(FetchTableDetailsParams {
            profile_id,
            database: database.to_string(),
            table: table.to_string(),
            connection: connected.connection.clone(),
        })
    }
}

pub struct FetchDatabaseSchemaParams {
    pub profile_id: Uuid,
    pub database: String,
    pub connection: Arc<dyn Connection>,
}

impl FetchDatabaseSchemaParams {
    pub fn execute(self) -> Result<FetchDatabaseSchemaResult, String> {
        let schema = self
            .connection
            .schema_for_database(&self.database)
            .map_err(|e| e.to_string())?;

        Ok(FetchDatabaseSchemaResult {
            profile_id: self.profile_id,
            database: self.database,
            schema,
        })
    }
}

pub struct FetchDatabaseSchemaResult {
    pub profile_id: Uuid,
    pub database: String,
    pub schema: DbSchemaInfo,
}

#[allow(dead_code)]
pub struct FetchTableDetailsParams {
    pub profile_id: Uuid,
    pub database: String,
    pub table: String,
    pub connection: Arc<dyn Connection>,
}

#[allow(dead_code)]
impl FetchTableDetailsParams {
    pub fn execute(self) -> Result<FetchTableDetailsResult, String> {
        let details = self
            .connection
            .table_details(&self.database, None, &self.table)
            .map_err(|e| e.to_string())?;

        Ok(FetchTableDetailsResult {
            profile_id: self.profile_id,
            database: self.database,
            table: self.table,
            details,
        })
    }
}

#[allow(dead_code)]
pub struct FetchTableDetailsResult {
    pub profile_id: Uuid,
    pub database: String,
    pub table: String,
    pub details: TableInfo,
}

pub struct FetchSchemaTypesParams {
    pub profile_id: Uuid,
    pub database: String,
    pub schema: Option<String>,
    pub connection: Arc<dyn Connection>,
}

impl FetchSchemaTypesParams {
    pub fn execute(self) -> Result<FetchSchemaTypesResult, String> {
        let types = self
            .connection
            .schema_types(&self.database, self.schema.as_deref())
            .map_err(|e| e.to_string())?;

        Ok(FetchSchemaTypesResult {
            profile_id: self.profile_id,
            database: self.database,
            schema: self.schema,
            types,
        })
    }
}

pub struct FetchSchemaTypesResult {
    pub profile_id: Uuid,
    pub database: String,
    pub schema: Option<String>,
    pub types: Vec<CustomTypeInfo>,
}

pub struct FetchSchemaIndexesParams {
    pub profile_id: Uuid,
    pub database: String,
    pub schema: Option<String>,
    pub connection: Arc<dyn Connection>,
}

impl FetchSchemaIndexesParams {
    pub fn execute(self) -> Result<FetchSchemaIndexesResult, String> {
        let indexes = self
            .connection
            .schema_indexes(&self.database, self.schema.as_deref())
            .map_err(|e| e.to_string())?;

        Ok(FetchSchemaIndexesResult {
            profile_id: self.profile_id,
            database: self.database,
            schema: self.schema,
            indexes,
        })
    }
}

pub struct FetchSchemaIndexesResult {
    pub profile_id: Uuid,
    pub database: String,
    pub schema: Option<String>,
    pub indexes: Vec<SchemaIndexInfo>,
}

pub struct FetchSchemaForeignKeysParams {
    pub profile_id: Uuid,
    pub database: String,
    pub schema: Option<String>,
    pub connection: Arc<dyn Connection>,
}

impl FetchSchemaForeignKeysParams {
    pub fn execute(self) -> Result<FetchSchemaForeignKeysResult, String> {
        let foreign_keys = self
            .connection
            .schema_foreign_keys(&self.database, self.schema.as_deref())
            .map_err(|e| e.to_string())?;

        Ok(FetchSchemaForeignKeysResult {
            profile_id: self.profile_id,
            database: self.database,
            schema: self.schema,
            foreign_keys,
        })
    }
}

pub struct FetchSchemaForeignKeysResult {
    pub profile_id: Uuid,
    pub database: String,
    pub schema: Option<String>,
    pub foreign_keys: Vec<SchemaForeignKeyInfo>,
}

pub struct ConnectProfileParams {
    pub profile: ConnectionProfile,
    pub driver: Arc<dyn DbDriver>,
    pub secret_store: Option<Arc<RwLock<Box<dyn SecretStore>>>>,
    pub ssh_secret: Option<String>,
}

impl ConnectProfileParams {
    pub fn execute(self) -> Result<ConnectProfileResult, String> {
        info!("Connecting to {}", self.profile.name);

        let password = self.get_password();

        let connection = self
            .driver
            .connect_with_secrets(
                &self.profile,
                password.as_deref(),
                self.ssh_secret.as_deref(),
            )
            .map_err(|e| e.to_string())?;

        let schema = match connection.schema() {
            Ok(s) => {
                info!(
                    "Fetched schema: {} databases, {} schemas",
                    s.databases().len(),
                    s.schemas().len()
                );
                Some(s)
            }
            Err(e) => {
                error!("Failed to fetch schema: {:?}", e);
                None
            }
        };

        Ok(ConnectProfileResult {
            profile: self.profile,
            connection: connection.into(),
            schema,
        })
    }

    fn get_password(&self) -> Option<String> {
        if !self.profile.save_password {
            return None;
        }

        let store_arc = self.secret_store.as_ref()?;
        let store = match store_arc.read() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned during password retrieval, recovering...");
                poison_err.into_inner()
            }
        };

        match store.get(&self.profile.secret_ref()) {
            Ok(pwd) => pwd,
            Err(e) => {
                error!("Failed to get password: {:?}", e);
                None
            }
        }
    }
}

pub struct ConnectProfileResult {
    pub profile: ConnectionProfile,
    pub connection: Arc<dyn Connection>,
    pub schema: Option<SchemaSnapshot>,
}

pub struct SwitchDatabaseParams {
    pub profile_id: Uuid,
    pub database: String,
    pub new_profile: ConnectionProfile,
    pub original_profile: ConnectionProfile,
    pub driver: Arc<dyn DbDriver>,
    pub secret_store: Arc<RwLock<Box<dyn SecretStore>>>,
}

impl SwitchDatabaseParams {
    pub fn execute(self) -> Result<SwitchDatabaseResult, String> {
        info!("Switching to database: {}", self.database);

        let password = self.get_password();

        let connection = self
            .driver
            .connect_with_password(&self.new_profile, password.as_deref())
            .map_err(|e| format!("Failed to connect to {}: {:?}", self.database, e))?;

        let schema = match connection.schema() {
            Ok(s) => {
                info!(
                    "Switched to {}: {} schemas, {} tables",
                    self.database,
                    s.schemas().len(),
                    s.schemas().iter().map(|s| s.tables.len()).sum::<usize>()
                );
                Some(s)
            }
            Err(e) => {
                error!("Failed to fetch schema for {}: {:?}", self.database, e);
                None
            }
        };

        Ok(SwitchDatabaseResult {
            profile_id: self.profile_id,
            original_profile: self.original_profile,
            connection: connection.into(),
            schema,
        })
    }

    fn get_password(&self) -> Option<String> {
        if !self.original_profile.save_password {
            return None;
        }

        let store = match self.secret_store.read() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned during password retrieval, recovering...");
                poison_err.into_inner()
            }
        };

        match store.get(&self.original_profile.secret_ref()) {
            Ok(pwd) => pwd,
            Err(e) => {
                error!("Failed to get password: {:?}", e);
                None
            }
        }
    }
}

pub struct SwitchDatabaseResult {
    pub profile_id: Uuid,
    pub original_profile: ConnectionProfile,
    pub connection: Arc<dyn Connection>,
    pub schema: Option<SchemaSnapshot>,
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
