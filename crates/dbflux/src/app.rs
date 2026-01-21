use dbflux_core::{
    create_secret_store, Connection, ConnectionProfile, DbConfig, DbDriver, DbKind, ProfileStore,
    SchemaSnapshot, SecretStore,
};
use log::{error, info};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::RwLock;
use uuid::Uuid;

#[cfg(feature = "sqlite")]
use dbflux_driver_sqlite::SqliteDriver;

#[cfg(feature = "postgres")]
use dbflux_driver_postgres::PostgresDriver;

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

pub struct AppState {
    pub drivers: HashMap<DbKind, Arc<dyn DbDriver>>,
    pub profiles: Vec<ConnectionProfile>,
    pub connections: HashMap<Uuid, ConnectedProfile>,
    pub active_connection_id: Option<Uuid>,
    pub pending_operations: HashSet<PendingOperation>,
    profile_store: ProfileStore,
    secret_store: Arc<RwLock<Box<dyn SecretStore>>>,
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

        let profile_store = ProfileStore::new().expect("Failed to create profile store");
        let profiles = match profile_store.load() {
            Ok(p) => {
                info!("Loaded {} profiles from disk", p.len());
                p
            }
            Err(e) => {
                error!("Failed to load profiles: {:?}", e);
                Vec::new()
            }
        };

        let secret_store = create_secret_store();
        info!("Secret store available: {}", secret_store.is_available());

        Self {
            drivers,
            profiles,
            connections: HashMap::new(),
            active_connection_id: None,
            pending_operations: HashSet::new(),
            profile_store,
            secret_store: Arc::new(RwLock::new(secret_store)),
        }
    }

    pub fn active_connection(&self) -> Option<&ConnectedProfile> {
        self.active_connection_id
            .and_then(|id| self.connections.get(&id))
    }

    pub fn is_connected(&self) -> bool {
        self.active_connection_id.is_some()
    }

    pub fn connection_display_name(&self) -> Option<&str> {
        self.active_connection().map(|c| c.profile.name.as_str())
    }

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
        self.connections.remove(&profile_id);
        if self.active_connection_id == Some(profile_id) {
            self.active_connection_id = self.connections.keys().next().copied();
        }
    }

    pub fn disconnect_all(&mut self) {
        self.connections.clear();
        self.active_connection_id = None;
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
        if let Err(e) = self.profile_store.save(&self.profiles) {
            error!("Failed to save profiles: {:?}", e);
        } else {
            info!("Saved {} profiles to disk", self.profiles.len());
        }
    }

    pub fn secret_store_available(&self) -> bool {
        self.secret_store
            .read()
            .map(|s| s.is_available())
            .unwrap_or(false)
    }

    pub fn secret_store(&self) -> Arc<RwLock<Box<dyn SecretStore>>> {
        self.secret_store.clone()
    }

    pub fn save_password(&self, profile: &ConnectionProfile, password: &str) {
        let Ok(store) = self.secret_store.read() else {
            error!("Failed to acquire secret store lock");
            return;
        };

        if !profile.save_password || !store.is_available() {
            return;
        }

        if let Err(e) = store.set(&profile.secret_ref(), password) {
            error!("Failed to save password: {:?}", e);
        }
    }

    pub fn delete_password(&self, profile: &ConnectionProfile) {
        let Ok(store) = self.secret_store.read() else {
            error!("Failed to acquire secret store lock");
            return;
        };

        if let Err(e) = store.delete(&profile.secret_ref()) {
            error!("Failed to delete password: {:?}", e);
        }
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

        Ok(ConnectProfileParams {
            profile,
            driver,
            secret_store,
        })
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

        if let Some(ref schema) = connected.schema {
            if schema.current_database.as_deref() == Some(database) {
                return Err("Already connected to this database".to_string());
            }
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
}

impl ConnectProfileParams {
    pub fn execute(self) -> Result<ConnectProfileResult, String> {
        info!("Connecting to {}", self.profile.name);

        let password = self.get_password();

        let connection = self
            .driver
            .connect_with_password(&self.profile, password.as_deref())
            .map_err(|e| format!("Connection failed: {:?}", e))?;

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

        let store = self.secret_store.as_ref()?.read().ok()?;

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
            database: self.database,
            original_profile: self.original_profile,
            connection: connection.into(),
            schema,
        })
    }

    fn get_password(&self) -> Option<String> {
        if !self.original_profile.save_password {
            return None;
        }

        let store = self.secret_store.read().ok()?;

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
    pub database: String,
    pub original_profile: ConnectionProfile,
    pub connection: Arc<dyn Connection>,
    pub schema: Option<SchemaSnapshot>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
