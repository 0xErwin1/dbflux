use dbflux_core::{
    CancelToken, Connection, ConnectionProfile, DbConfig, DbDriver, DbKind, HistoryEntry,
    HistoryStore, ProfileStore, SavedQuery, SavedQueryStore, SchemaSnapshot, SecretStore,
    SshTunnelProfile, SshTunnelStore, TaskId, TaskKind, TaskManager, TaskSnapshot,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PendingOperation {
    pub profile_id: Uuid,
    pub database: Option<String>,
}

pub struct ConnectedProfile {
    pub profile: ConnectionProfile,
    pub connection: Arc<dyn Connection>,
    pub schema: Option<SchemaSnapshot>,
}

/// Session-based suppressions for dangerous query confirmations.
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
    pub fn is_suppressed(
        &self,
        kind: crate::ui::editor::dangerous_query::DangerousQueryKind,
    ) -> bool {
        use crate::ui::editor::dangerous_query::DangerousQueryKind;
        match kind {
            DangerousQueryKind::DeleteNoWhere => self.delete_no_where,
            DangerousQueryKind::UpdateNoWhere => self.update_no_where,
            DangerousQueryKind::Truncate => self.truncate,
            DangerousQueryKind::Drop => self.drop,
            DangerousQueryKind::Alter => self.alter,
            DangerousQueryKind::Script => self.script,
        }
    }

    pub fn set_suppressed(&mut self, kind: crate::ui::editor::dangerous_query::DangerousQueryKind) {
        use crate::ui::editor::dangerous_query::DangerousQueryKind;
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
    pending_saved_query_warning: Option<String>,
    pub dangerous_query_suppressions: DangerousQuerySuppressions,

    pub settings_window: Option<WindowHandle<Root>>,
}

impl AppState {
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

    #[allow(dead_code)]
    pub fn connection_display_name(&self) -> Option<&str> {
        self.active_connection().map(|c| c.profile.name.as_str())
    }

    #[allow(dead_code)]
    pub fn active_schema(&self) -> Option<&SchemaSnapshot> {
        self.active_connection().and_then(|c| c.schema.as_ref())
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

    pub fn add_profile(&mut self, profile: ConnectionProfile) {
        self.profiles.push(profile);
        self.save_profiles();
    }

    pub fn remove_profile(&mut self, idx: usize) -> Option<ConnectionProfile> {
        if idx < self.profiles.len() {
            let removed = self.profiles.remove(idx);
            self.disconnect(removed.id);
            self.delete_password(&removed);
            self.save_profiles();
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
        let store = match self.secret_store.read() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned, recovering...");
                poison_err.into_inner()
            }
        };

        match store.get(&tunnel.secret_ref()) {
            Ok(secret) => secret,
            Err(e) => {
                error!("Failed to get SSH tunnel secret: {:?}", e);
                None
            }
        }
    }

    pub fn save_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile, secret: &str) {
        let store = match self.secret_store.read() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned, recovering...");
                poison_err.into_inner()
            }
        };

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.set(&tunnel.secret_ref(), secret) {
            error!("Failed to save SSH tunnel secret: {:?}", e);
        }
    }

    pub fn delete_ssh_tunnel_secret(&self, tunnel: &SshTunnelProfile) {
        let store = match self.secret_store.read() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned, recovering...");
                poison_err.into_inner()
            }
        };

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.delete(&tunnel.secret_ref()) {
            log::warn!("Failed to delete SSH tunnel secret: {:?}", e);
        }
    }

    pub fn secret_store_available(&self) -> bool {
        match self.secret_store.read() {
            Ok(s) => s.is_available(),
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned, recovering...");
                poison_err.into_inner().is_available()
            }
        }
    }

    #[allow(dead_code)]
    pub fn secret_store(&self) -> Arc<RwLock<Box<dyn SecretStore>>> {
        self.secret_store.clone()
    }

    pub fn save_password(&self, profile: &ConnectionProfile, password: &str) {
        if !profile.save_password {
            return;
        }

        let store = match self.secret_store.read() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned, recovering...");
                poison_err.into_inner()
            }
        };

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.set(&profile.secret_ref(), password) {
            error!("Failed to save password: {:?}", e);
        }
    }

    pub fn delete_password(&self, profile: &ConnectionProfile) {
        let store = match self.secret_store.read() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned, recovering...");
                poison_err.into_inner()
            }
        };

        if let Err(e) = store.delete(&profile.secret_ref()) {
            error!("Failed to delete password: {:?}", e);
        }
    }

    pub fn get_ssh_password(&self, profile: &ConnectionProfile) -> Option<String> {
        let store = match self.secret_store.read() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned, recovering...");
                poison_err.into_inner()
            }
        };

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
        let store = match self.secret_store.read() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned, recovering...");
                poison_err.into_inner()
            }
        };

        if !store.is_available() {
            return;
        }

        if let Err(e) = store.set(&profile.ssh_secret_ref(), secret) {
            error!("Failed to save SSH secret: {:?}", e);
        }
    }

    pub fn delete_ssh_password(&self, profile: &ConnectionProfile) {
        let store = match self.secret_store.read() {
            Ok(guard) => guard,
            Err(poison_err) => {
                log::warn!("Secret store RwLock poisoned, recovering...");
                poison_err.into_inner()
            }
        };

        if let Err(e) = store.delete(&profile.ssh_secret_ref()) {
            error!("Failed to delete SSH secret: {:?}", e);
        }
    }

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
        let tunnel_profile_id = match &profile.config {
            DbConfig::Postgres {
                ssh_tunnel_profile_id: Some(id),
                ..
            } => *id,
            DbConfig::MySQL {
                ssh_tunnel_profile_id: Some(id),
                ..
            } => *id,
            _ => return None,
        };

        let tunnel = self
            .ssh_tunnels
            .iter()
            .find(|t| t.id == tunnel_profile_id)?;

        if !tunnel.save_secret {
            return None;
        }

        self.get_ssh_tunnel_secret(tunnel)
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

    #[allow(dead_code)]
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
            && schema.current_database.as_deref() == Some(database)
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
            },
        );
    }
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
                    s.databases.len(),
                    s.schemas.len()
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
                    s.schemas.len(),
                    s.schemas.iter().map(|s| s.tables.len()).sum::<usize>()
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
