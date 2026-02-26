use crate::{
    Connection, ConnectionProfile, CustomTypeInfo, DbConfig, DbDriver, DbKind, DbSchemaInfo,
    SchemaForeignKeyInfo, SchemaIndexInfo, SchemaLoadingStrategy, SchemaSnapshot, SecretStore,
    ShutdownCoordinator, ShutdownPhase, SshTunnelProfile, TableInfo,
};
use log::{error, info};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Instant;
use uuid::Uuid;

/// Typed cache key for schema-level data (types, indexes, foreign keys).
///
/// Replaces the previous untyped string-based approach. Drivers and UI code
/// construct these to look up cached schema metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CacheKey {
    DatabaseSchema {
        database: String,
    },
    TableDetails {
        database: String,
        table: String,
    },
    SchemaTypes {
        database: String,
        schema: Option<String>,
    },
    SchemaIndexes {
        database: String,
        schema: Option<String>,
    },
    SchemaForeignKeys {
        database: String,
        schema: Option<String>,
    },
}

impl CacheKey {
    pub fn database_schema(database: impl Into<String>) -> Self {
        Self::DatabaseSchema {
            database: database.into(),
        }
    }

    pub fn table_details(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self::TableDetails {
            database: database.into(),
            table: table.into(),
        }
    }

    pub fn schema_types(database: impl Into<String>, schema: Option<impl Into<String>>) -> Self {
        Self::SchemaTypes {
            database: database.into(),
            schema: schema.map(|s| s.into()),
        }
    }

    pub fn schema_indexes(database: impl Into<String>, schema: Option<impl Into<String>>) -> Self {
        Self::SchemaIndexes {
            database: database.into(),
            schema: schema.map(|s| s.into()),
        }
    }

    pub fn schema_foreign_keys(
        database: impl Into<String>,
        schema: Option<impl Into<String>>,
    ) -> Self {
        Self::SchemaForeignKeys {
            database: database.into(),
            schema: schema.map(|s| s.into()),
        }
    }
}

/// Borrowed reference to a cached value, returned by `ConnectedProfile::cache_get`.
#[derive(Debug)]
pub enum CacheEntry<'a> {
    DatabaseSchema(&'a DbSchemaInfo),
    TableDetails(&'a TableInfo),
    SchemaTypes(&'a Vec<CustomTypeInfo>),
    SchemaIndexes(&'a Vec<SchemaIndexInfo>),
    SchemaForeignKeys(&'a Vec<SchemaForeignKeyInfo>),
}

/// Owned cache value for inserting into the cache via `ConnectedProfile::cache_set`.
pub enum OwnedCacheEntry {
    DatabaseSchema {
        database: String,
        schema: DbSchemaInfo,
    },
    TableDetails {
        database: String,
        table: String,
        details: TableInfo,
    },
    SchemaTypes {
        database: String,
        schema: Option<String>,
        types: Vec<CustomTypeInfo>,
    },
    SchemaIndexes {
        database: String,
        schema: Option<String>,
        indexes: Vec<SchemaIndexInfo>,
    },
    SchemaForeignKeys {
        database: String,
        schema: Option<String>,
        foreign_keys: Vec<SchemaForeignKeyInfo>,
    },
}

/// Backward-compatible alias for code that still uses `SchemaCacheKey`.
///
/// Wraps a database + optional schema pair, mapping to the appropriate
/// `CacheKey` variant depending on context.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaCacheKey {
    pub database: String,
    pub schema: Option<String>,
}

impl SchemaCacheKey {
    pub fn new(database: impl Into<String>, schema: Option<impl Into<String>>) -> Self {
        Self {
            database: database.into(),
            schema: schema.map(|s| s.into()),
        }
    }
}

pub struct RedisKeyCacheEntry {
    pub keys: Arc<[String]>,
    pub fetched_at: Instant,
}

/// Cached Redis key names per keyspace (e.g. "db0"). Keyed by keyspace
/// so the completion provider can read it without coupling to `KeyValueDocument`.
pub struct RedisKeyCache {
    entries: HashMap<String, RedisKeyCacheEntry>,
    ttl: std::time::Duration,
}

impl Default for RedisKeyCache {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            ttl: std::time::Duration::from_secs(30),
        }
    }
}

impl RedisKeyCache {
    pub fn get_keys(&self, keyspace: &str) -> Option<Arc<[String]>> {
        self.entries.get(keyspace).map(|e| e.keys.clone())
    }

    pub fn set_keys(&mut self, keyspace: String, keys: Vec<String>) {
        self.entries.insert(
            keyspace,
            RedisKeyCacheEntry {
                keys: keys.into(),
                fetched_at: Instant::now(),
            },
        );
    }

    pub fn is_stale(&self, keyspace: &str) -> bool {
        match self.entries.get(keyspace) {
            None => true,
            Some(entry) => entry.fetched_at.elapsed() > self.ttl,
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Per-database connection with its own schema snapshot.
/// Used by `ConnectionPerDatabase` drivers (e.g. PostgreSQL).
pub struct DatabaseConnection {
    pub connection: Arc<dyn Connection>,
    pub schema: Option<SchemaSnapshot>,
}

pub struct ConnectedProfile {
    pub profile: ConnectionProfile,
    pub connection: Arc<dyn Connection>,
    pub schema: Option<SchemaSnapshot>,
    /// Lazy-loaded schemas per database (MySQL/MariaDB).
    pub database_schemas: HashMap<String, DbSchemaInfo>,
    pub table_details: HashMap<(String, String), TableInfo>,
    pub schema_types: HashMap<SchemaCacheKey, Vec<CustomTypeInfo>>,
    pub schema_indexes: HashMap<SchemaCacheKey, Vec<SchemaIndexInfo>>,
    pub schema_foreign_keys: HashMap<SchemaCacheKey, Vec<SchemaForeignKeyInfo>>,
    /// Active database for query context (MySQL/MariaDB USE).
    pub active_database: Option<String>,
    pub redis_key_cache: RedisKeyCache,
    /// Per-database connections keyed by database name (`ConnectionPerDatabase` drivers).
    pub database_connections: HashMap<String, DatabaseConnection>,
}

impl ConnectedProfile {
    /// Look up any cached value by typed `CacheKey`.
    ///
    /// Returns a `CacheEntry` reference if the key is present, `None` otherwise.
    pub fn cache_get(&self, key: &CacheKey) -> Option<CacheEntry<'_>> {
        match key {
            CacheKey::DatabaseSchema { database } => self
                .database_schemas
                .get(database.as_str())
                .map(CacheEntry::DatabaseSchema),

            CacheKey::TableDetails { database, table } => self
                .table_details
                .get(&(database.clone(), table.clone()))
                .map(CacheEntry::TableDetails),

            CacheKey::SchemaTypes { database, schema } => {
                let sk = SchemaCacheKey::new(database.as_str(), schema.as_deref());
                self.schema_types.get(&sk).map(CacheEntry::SchemaTypes)
            }

            CacheKey::SchemaIndexes { database, schema } => {
                let sk = SchemaCacheKey::new(database.as_str(), schema.as_deref());
                self.schema_indexes.get(&sk).map(CacheEntry::SchemaIndexes)
            }

            CacheKey::SchemaForeignKeys { database, schema } => {
                let sk = SchemaCacheKey::new(database.as_str(), schema.as_deref());
                self.schema_foreign_keys
                    .get(&sk)
                    .map(CacheEntry::SchemaForeignKeys)
            }
        }
    }

    /// Check whether a given cache key is populated.
    pub fn cache_contains(&self, key: &CacheKey) -> bool {
        self.cache_get(key).is_some()
    }

    /// Insert a value into the cache using a typed `CacheKey`.
    pub fn cache_set(&mut self, entry: OwnedCacheEntry) {
        match entry {
            OwnedCacheEntry::DatabaseSchema { database, schema } => {
                self.database_schemas.insert(database, schema);
            }

            OwnedCacheEntry::TableDetails {
                database,
                table,
                details,
            } => {
                self.table_details.insert((database, table), details);
            }

            OwnedCacheEntry::SchemaTypes {
                database,
                schema,
                types,
            } => {
                let sk = SchemaCacheKey::new(database, schema);
                self.schema_types.insert(sk, types);
            }

            OwnedCacheEntry::SchemaIndexes {
                database,
                schema,
                indexes,
            } => {
                let sk = SchemaCacheKey::new(database, schema);
                self.schema_indexes.insert(sk, indexes);
            }

            OwnedCacheEntry::SchemaForeignKeys {
                database,
                schema,
                foreign_keys,
            } => {
                let sk = SchemaCacheKey::new(database, schema);
                self.schema_foreign_keys.insert(sk, foreign_keys);
            }
        }
    }

    /// Remove a database schema from the cache, returning it if present.
    pub fn invalidate_database_schema(&mut self, database: &str) -> Option<DbSchemaInfo> {
        self.database_schemas.remove(database)
    }

    /// Look up a per-database connection (for `ConnectionPerDatabase` drivers).
    pub fn database_connection(&self, database: &str) -> Option<&DatabaseConnection> {
        self.database_connections.get(database)
    }

    /// Store a per-database connection and its schema.
    pub fn add_database_connection(&mut self, database: String, db_conn: DatabaseConnection) {
        self.database_connections.insert(database, db_conn);
    }

    /// Returns the per-database connection if one exists, otherwise the primary.
    pub fn connection_for_database(&self, database: &str) -> Arc<dyn Connection> {
        self.database_connections
            .get(database)
            .map(|dc| dc.connection.clone())
            .unwrap_or_else(|| self.connection.clone())
    }

    /// Returns the per-database schema if available, otherwise the primary.
    pub fn schema_for_target_database(&self, database: &str) -> Option<&SchemaSnapshot> {
        self.database_connections
            .get(database)
            .and_then(|dc| dc.schema.as_ref())
            .or(self.schema.as_ref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PendingOperation {
    pub profile_id: Uuid,
    pub database: Option<String>,
}

pub struct ConnectionManager {
    pub drivers: HashMap<DbKind, Arc<dyn DbDriver>>,
    pub connections: HashMap<Uuid, ConnectedProfile>,
    pub active_connection_id: Option<Uuid>,
    pub pending_operations: HashSet<PendingOperation>,
}

impl ConnectionManager {
    pub fn new(drivers: HashMap<DbKind, Arc<dyn DbDriver>>) -> Self {
        Self {
            drivers,
            connections: HashMap::new(),
            active_connection_id: None,
            pending_operations: HashSet::new(),
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
                redis_key_cache: RedisKeyCache::default(),
                database_connections: HashMap::new(),
            },
        );
        self.active_connection_id = Some(id);
    }

    pub fn disconnect(&mut self, profile_id: Uuid) {
        if let Some(connected) = self.connections.remove(&profile_id) {
            std::thread::spawn(move || {
                let _ = connected.connection.cancel_active();
                for db_conn in connected.database_connections.values() {
                    let _ = db_conn.connection.cancel_active();
                }
                drop(connected);
            });
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

    // --- Schema cache ---

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
            connected.cache_set(OwnedCacheEntry::DatabaseSchema { database, schema });
        }
    }

    pub fn needs_database_schema(&self, profile_id: Uuid, database: &str) -> bool {
        let key = CacheKey::database_schema(database);
        self.connections
            .get(&profile_id)
            .is_some_and(|c| !c.cache_contains(&key))
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

    pub fn set_table_details(
        &mut self,
        profile_id: Uuid,
        database: String,
        table: String,
        details: TableInfo,
    ) {
        if let Some(connected) = self.connections.get_mut(&profile_id) {
            connected.cache_set(OwnedCacheEntry::TableDetails {
                database,
                table,
                details,
            });
        }
    }

    pub fn needs_table_details(&self, profile_id: Uuid, database: &str, table: &str) -> bool {
        let key = CacheKey::table_details(database, table);
        self.connections
            .get(&profile_id)
            .is_some_and(|c| !c.cache_contains(&key))
    }

    #[allow(dead_code)]
    pub fn get_schema_types(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> Option<&Vec<CustomTypeInfo>> {
        let key = SchemaCacheKey::new(database, schema);
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
        if let Some(connected) = self.connections.get_mut(&profile_id) {
            connected.cache_set(OwnedCacheEntry::SchemaTypes {
                database,
                schema,
                types,
            });
        }
    }

    pub fn needs_schema_types(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        let key = CacheKey::schema_types(database, schema);
        self.connections
            .get(&profile_id)
            .is_some_and(|c| !c.cache_contains(&key))
    }

    pub fn set_schema_indexes(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        indexes: Vec<SchemaIndexInfo>,
    ) {
        if let Some(connected) = self.connections.get_mut(&profile_id) {
            connected.cache_set(OwnedCacheEntry::SchemaIndexes {
                database,
                schema,
                indexes,
            });
        }
    }

    pub fn needs_schema_indexes(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        let key = CacheKey::schema_indexes(database, schema);
        self.connections
            .get(&profile_id)
            .is_some_and(|c| !c.cache_contains(&key))
    }

    pub fn set_schema_foreign_keys(
        &mut self,
        profile_id: Uuid,
        database: String,
        schema: Option<String>,
        foreign_keys: Vec<SchemaForeignKeyInfo>,
    ) {
        if let Some(connected) = self.connections.get_mut(&profile_id) {
            connected.cache_set(OwnedCacheEntry::SchemaForeignKeys {
                database,
                schema,
                foreign_keys,
            });
        }
    }

    pub fn needs_schema_foreign_keys(
        &self,
        profile_id: Uuid,
        database: &str,
        schema: Option<&str>,
    ) -> bool {
        let key = CacheKey::schema_foreign_keys(database, schema);
        self.connections
            .get(&profile_id)
            .is_some_and(|c| !c.cache_contains(&key))
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

    /// Store a per-database connection for a `ConnectionPerDatabase` driver.
    pub fn add_database_connection(
        &mut self,
        profile_id: Uuid,
        database: String,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
    ) {
        if let Some(connected) = self.connections.get_mut(&profile_id) {
            connected.add_database_connection(database, DatabaseConnection { connection, schema });
        }
    }

    // --- Pending operations ---

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

    // --- Prepare methods ---

    pub fn prepare_connect_profile(
        &self,
        profile_id: Uuid,
        profiles: &[ConnectionProfile],
        ssh_tunnels: &[SshTunnelProfile],
        secret_store: &Arc<RwLock<Box<dyn SecretStore>>>,
        get_ssh_secret: impl FnOnce(&ConnectionProfile, &[SshTunnelProfile]) -> Option<String>,
    ) -> Result<ConnectProfileParams, String> {
        let profile = profiles
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

        let secret_store_param = if kind == DbKind::SQLite {
            None
        } else {
            Some(secret_store.clone())
        };

        let ssh_secret = get_ssh_secret(&profile, ssh_tunnels);

        Ok(ConnectProfileParams {
            profile,
            driver,
            secret_store: secret_store_param,
            ssh_secret,
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

    pub fn prepare_switch_database(
        &self,
        profile_id: Uuid,
        database: &str,
        secret_store: &Arc<RwLock<Box<dyn SecretStore>>>,
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
            secret_store: secret_store.clone(),
        })
    }

    /// Prepare a per-database connection without replacing the primary.
    /// Rejects only if a connection to this database already exists.
    pub fn prepare_database_connection(
        &self,
        profile_id: Uuid,
        database: &str,
        secret_store: &Arc<RwLock<Box<dyn SecretStore>>>,
    ) -> Result<SwitchDatabaseParams, String> {
        let connected = self
            .connections
            .get(&profile_id)
            .ok_or_else(|| "Profile not connected".to_string())?;

        if connected.connection.schema_loading_strategy()
            != SchemaLoadingStrategy::ConnectionPerDatabase
        {
            return Err(
                "Per-database connections only supported for ConnectionPerDatabase drivers"
                    .to_string(),
            );
        }

        if let Some(ref schema) = connected.schema
            && schema.current_database() == Some(database)
        {
            return Err(format!("Already connected to database '{}'", database));
        }

        if connected.database_connections.contains_key(database) {
            return Err(format!("Already connected to database '{}'", database));
        }

        let kind = connected.profile.kind();
        let mut new_profile = connected.profile.clone();

        match &mut new_profile.config {
            DbConfig::Postgres { database: db, .. } => {
                *db = database.to_string();
            }
            _ => {
                return Err("Unsupported database config for per-database connections".to_string());
            }
        }

        let driver = self
            .drivers
            .get(&kind)
            .cloned()
            .ok_or_else(|| format!("{:?} driver not available", kind))?;

        let original_profile = connected.profile.clone();

        Ok(SwitchDatabaseParams {
            profile_id,
            database: database.to_string(),
            new_profile,
            original_profile,
            driver,
            secret_store: secret_store.clone(),
        })
    }

    pub fn apply_switch_database(
        &mut self,
        profile_id: Uuid,
        original_profile: ConnectionProfile,
        connection: Arc<dyn Connection>,
        schema: Option<SchemaSnapshot>,
    ) {
        // Preserve per-database connections from the old profile so that
        // query tabs targeting other databases keep working.
        let prev_db_connections = self
            .connections
            .get_mut(&profile_id)
            .map(|old| std::mem::take(&mut old.database_connections))
            .unwrap_or_default();

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
                redis_key_cache: RedisKeyCache::default(),
                database_connections: prev_db_connections,
            },
        );
    }

    pub fn prepare_fetch_database_schema(
        &self,
        profile_id: Uuid,
        database: &str,
    ) -> Result<FetchDatabaseSchemaParams, String> {
        let connected = self
            .connections
            .get(&profile_id)
            .ok_or_else(|| "Profile not connected".to_string())?;

        let strategy = connected.connection.schema_loading_strategy();
        if strategy != SchemaLoadingStrategy::LazyPerDatabase {
            return Err(format!(
                "Database schema fetch not supported for {:?} strategy",
                strategy
            ));
        }

        let key = CacheKey::database_schema(database);
        if connected.cache_contains(&key) {
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
        schema: Option<&str>,
        table: &str,
    ) -> Result<FetchTableDetailsParams, String> {
        let connected = self
            .connections
            .get(&profile_id)
            .ok_or_else(|| "Profile not connected".to_string())?;

        let cache_key = (database.to_string(), table.to_string());
        if let Some(details) = connected.table_details.get(&cache_key)
            && (details.columns.is_some() || details.sample_fields.is_some())
        {
            return Err("Table details already cached".to_string());
        }

        Ok(FetchTableDetailsParams {
            profile_id,
            database: database.to_string(),
            schema: schema.map(String::from),
            table: table.to_string(),
            connection: connected.connection_for_database(database),
        })
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

        let key = CacheKey::schema_types(database, schema);
        if connected.cache_contains(&key) {
            return Err("Schema types already cached".to_string());
        }

        Ok(FetchSchemaTypesParams {
            profile_id,
            database: database.to_string(),
            schema: schema.map(String::from),
            connection: connected.connection_for_database(database),
        })
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

        let key = CacheKey::schema_indexes(database, schema);
        if connected.cache_contains(&key) {
            return Err("Schema indexes already cached".to_string());
        }

        Ok(FetchSchemaIndexesParams {
            profile_id,
            database: database.to_string(),
            schema: schema.map(String::from),
            connection: connected.connection_for_database(database),
        })
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

        let key = CacheKey::schema_foreign_keys(database, schema);
        if connected.cache_contains(&key) {
            return Err("Schema foreign keys already cached".to_string());
        }

        Ok(FetchSchemaForeignKeysParams {
            profile_id,
            database: database.to_string(),
            schema: schema.map(String::from),
            connection: connected.connection_for_database(database),
        })
    }

    // --- Shutdown ---

    pub fn close_all_connections(&mut self, shutdown: &ShutdownCoordinator) {
        if !shutdown.advance_phase(
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

                if let Err(e) = connected.connection.cancel_active() {
                    log::debug!(
                        "Could not cancel active query for {} (may not have one): {:?}",
                        name,
                        e
                    );
                }

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
}

// --- Params/Result structs ---

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
    pub schema: Option<String>,
    pub table: String,
    pub connection: Arc<dyn Connection>,
}

#[allow(dead_code)]
impl FetchTableDetailsParams {
    pub fn execute(self) -> Result<FetchTableDetailsResult, String> {
        let details = self
            .connection
            .table_details(&self.database, self.schema.as_deref(), &self.table)
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
