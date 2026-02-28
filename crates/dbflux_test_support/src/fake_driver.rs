use dbflux_core::{
    Connection, ConnectionProfile, DatabaseCategory, DbConfig, DbDriver, DbError, DbKind,
    DriverCapabilities, DriverFormDef, DriverMetadata, FormValues, Icon, MONGODB_FORM, MYSQL_FORM,
    POSTGRES_FORM, QueryHandle, QueryLanguage, QueryRequest, QueryResult, REDIS_FORM,
    RedisLanguageService, SQLITE_FORM, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect,
    SqlLanguageService,
};
use dbflux_core::{DatabaseInfo, DefaultSqlDialect};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug, Clone)]
pub enum FakeQueryOutcome {
    Success(QueryResult),
    Error(String),
    Timeout,
    Cancelled,
}

impl FakeQueryOutcome {
    fn into_result(&self) -> Result<QueryResult, DbError> {
        match self {
            Self::Success(result) => Ok(result.clone()),
            Self::Error(message) => Err(DbError::query_failed(message.clone())),
            Self::Timeout => Err(DbError::Timeout),
            Self::Cancelled => Err(DbError::Cancelled),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FakeDriverStats {
    pub executed_requests: Vec<QueryRequest>,
    pub cancelled_handle_count: usize,
    pub cancel_active_calls: usize,
    pub close_calls: usize,
}

#[derive(Default)]
struct FakeDriverState {
    schema: RwLock<SchemaSnapshot>,
    query_outcomes: RwLock<HashMap<String, FakeQueryOutcome>>,
    default_outcome: RwLock<Option<FakeQueryOutcome>>,
    executed_requests: Mutex<Vec<QueryRequest>>,
    cancelled_handles: Mutex<Vec<QueryHandle>>,
    cancel_active_calls: AtomicUsize,
    close_calls: AtomicUsize,
    ping_error: RwLock<Option<String>>,
    connect_error: RwLock<Option<String>>,
}

#[derive(Clone)]
pub struct FakeDriver {
    kind: DbKind,
    state: Arc<FakeDriverState>,
}

impl FakeDriver {
    pub fn new(kind: DbKind) -> Self {
        Self {
            kind,
            state: Arc::new(FakeDriverState {
                schema: RwLock::new(SchemaSnapshot::default()),
                ..FakeDriverState::default()
            }),
        }
    }

    pub fn with_schema(self, schema: SchemaSnapshot) -> Self {
        *rwlock_write(&self.state.schema) = schema;
        self
    }

    pub fn with_query_result(self, sql: impl Into<String>, result: QueryResult) -> Self {
        rwlock_write(&self.state.query_outcomes)
            .insert(sql.into(), FakeQueryOutcome::Success(result));
        self
    }

    pub fn with_query_error(self, sql: impl Into<String>, message: impl Into<String>) -> Self {
        rwlock_write(&self.state.query_outcomes)
            .insert(sql.into(), FakeQueryOutcome::Error(message.into()));
        self
    }

    pub fn with_default_result(self, result: QueryResult) -> Self {
        *rwlock_write(&self.state.default_outcome) = Some(FakeQueryOutcome::Success(result));
        self
    }

    pub fn with_default_error(self, message: impl Into<String>) -> Self {
        *rwlock_write(&self.state.default_outcome) = Some(FakeQueryOutcome::Error(message.into()));
        self
    }

    pub fn with_ping_error(self, message: impl Into<String>) -> Self {
        *rwlock_write(&self.state.ping_error) = Some(message.into());
        self
    }

    pub fn with_connect_error(self, message: impl Into<String>) -> Self {
        *rwlock_write(&self.state.connect_error) = Some(message.into());
        self
    }

    pub fn set_query_outcome(&self, sql: impl Into<String>, outcome: FakeQueryOutcome) {
        rwlock_write(&self.state.query_outcomes).insert(sql.into(), outcome);
    }

    pub fn stats(&self) -> FakeDriverStats {
        FakeDriverStats {
            executed_requests: mutex_lock(&self.state.executed_requests).clone(),
            cancelled_handle_count: mutex_lock(&self.state.cancelled_handles).len(),
            cancel_active_calls: self.state.cancel_active_calls.load(Ordering::Relaxed),
            close_calls: self.state.close_calls.load(Ordering::Relaxed),
        }
    }

    pub fn as_driver_arc(self) -> Arc<dyn DbDriver> {
        Arc::new(self)
    }
}

impl DbDriver for FakeDriver {
    fn kind(&self) -> DbKind {
        self.kind
    }

    fn metadata(&self) -> &'static DriverMetadata {
        metadata_for_kind(self.kind)
    }

    fn form_definition(&self) -> &'static DriverFormDef {
        form_for_kind(self.kind)
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let config = match self.kind {
            DbKind::Postgres => DbConfig::Postgres {
                use_uri: false,
                uri: None,
                host: get_string(values, "host", "localhost"),
                port: get_u16(values, "port", 5432),
                user: get_string(values, "user", "postgres"),
                database: get_string(values, "database", "postgres"),
                ssl_mode: dbflux_core::SslMode::Disable,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
            DbKind::SQLite => {
                let path = values
                    .get("path")
                    .map(|path| path.trim())
                    .filter(|path| !path.is_empty())
                    .ok_or_else(|| {
                        DbError::InvalidProfile("Missing required field: path".to_string())
                    })?;

                DbConfig::SQLite { path: path.into() }
            }
            DbKind::MySQL | DbKind::MariaDB => DbConfig::MySQL {
                use_uri: false,
                uri: None,
                host: get_string(values, "host", "localhost"),
                port: get_u16(values, "port", 3306),
                user: get_string(values, "user", "root"),
                database: get_optional_string(values, "database"),
                ssl_mode: dbflux_core::SslMode::Disable,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
            DbKind::MongoDB => DbConfig::MongoDB {
                use_uri: false,
                uri: None,
                host: get_string(values, "host", "localhost"),
                port: get_u16(values, "port", 27017),
                user: get_optional_string(values, "user"),
                database: get_optional_string(values, "database"),
                auth_database: get_optional_string(values, "auth_database"),
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
            DbKind::Redis => DbConfig::Redis {
                use_uri: false,
                uri: None,
                host: get_string(values, "host", "localhost"),
                port: get_u16(values, "port", 6379),
                user: get_optional_string(values, "user"),
                database: get_u32_opt(values, "database"),
                tls: false,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        };

        Ok(config)
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = FormValues::new();

        match config {
            DbConfig::Postgres {
                host,
                port,
                user,
                database,
                ..
            } => {
                values.insert("host".to_string(), host.clone());
                values.insert("port".to_string(), port.to_string());
                values.insert("user".to_string(), user.clone());
                values.insert("database".to_string(), database.clone());
            }
            DbConfig::SQLite { path } => {
                values.insert("path".to_string(), path.display().to_string());
            }
            DbConfig::MySQL {
                host,
                port,
                user,
                database,
                ..
            } => {
                values.insert("host".to_string(), host.clone());
                values.insert("port".to_string(), port.to_string());
                values.insert("user".to_string(), user.clone());
                values.insert("database".to_string(), database.clone().unwrap_or_default());
            }
            DbConfig::MongoDB {
                host,
                port,
                user,
                database,
                auth_database,
                ..
            } => {
                values.insert("host".to_string(), host.clone());
                values.insert("port".to_string(), port.to_string());
                values.insert("user".to_string(), user.clone().unwrap_or_default());
                values.insert("database".to_string(), database.clone().unwrap_or_default());
                values.insert(
                    "auth_database".to_string(),
                    auth_database.clone().unwrap_or_default(),
                );
            }
            DbConfig::Redis {
                host,
                port,
                user,
                database,
                ..
            } => {
                values.insert("host".to_string(), host.clone());
                values.insert("port".to_string(), port.to_string());
                values.insert("user".to_string(), user.clone().unwrap_or_default());
                values.insert(
                    "database".to_string(),
                    database.map(|value| value.to_string()).unwrap_or_default(),
                );
            }
            DbConfig::External { values: vals, .. } => {
                values.extend(vals.clone());
            }
        }

        values
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        _password: Option<&str>,
        _ssh_secret: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        if let Some(message) = rwlock_read(&self.state.connect_error).clone() {
            return Err(DbError::connection_failed(message));
        }

        Ok(Box::new(FakeConnection::new(
            self.kind,
            profile,
            self.state.clone(),
        )))
    }

    fn test_connection(&self, _profile: &ConnectionProfile) -> Result<(), DbError> {
        if let Some(message) = rwlock_read(&self.state.connect_error).clone() {
            return Err(DbError::connection_failed(message));
        }

        Ok(())
    }
}

struct FakeConnection {
    kind: DbKind,
    state: Arc<FakeDriverState>,
    active_database: RwLock<Option<String>>,
}

impl FakeConnection {
    fn new(kind: DbKind, profile: &ConnectionProfile, state: Arc<FakeDriverState>) -> Self {
        Self {
            kind,
            state,
            active_database: RwLock::new(active_database_from_profile(profile)),
        }
    }

    fn execute_internal(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        mutex_lock(&self.state.executed_requests).push(req.clone());

        if let Some(database) = req.database.clone() {
            *rwlock_write(&self.active_database) = Some(database);
        }

        if let Some(outcome) = rwlock_read(&self.state.query_outcomes)
            .get(&req.sql)
            .cloned()
        {
            return outcome.into_result();
        }

        if let Some(outcome) = rwlock_read(&self.state.default_outcome).clone() {
            return outcome.into_result();
        }

        Ok(QueryResult::empty())
    }
}

impl Connection for FakeConnection {
    fn metadata(&self) -> &'static DriverMetadata {
        metadata_for_kind(self.kind)
    }

    fn ping(&self) -> Result<(), DbError> {
        if let Some(message) = rwlock_read(&self.state.ping_error).clone() {
            return Err(DbError::connection_failed(message));
        }

        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        self.state.close_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        self.execute_internal(req)
    }

    fn execute_with_handle(
        &self,
        req: &QueryRequest,
    ) -> Result<(QueryHandle, QueryResult), DbError> {
        let handle = QueryHandle::new();
        let result = self.execute_internal(req)?;
        Ok((handle, result))
    }

    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError> {
        mutex_lock(&self.state.cancelled_handles).push(handle.clone());
        Ok(())
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.state
            .cancel_active_calls
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Ok(rwlock_read(&self.state.schema).clone())
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        Ok(rwlock_read(&self.state.schema).databases().to_vec())
    }

    fn set_active_database(&self, database: Option<&str>) -> Result<(), DbError> {
        *rwlock_write(&self.active_database) = database.map(std::string::ToString::to_string);
        Ok(())
    }

    fn active_database(&self) -> Option<String> {
        rwlock_read(&self.active_database).clone()
    }

    fn kind(&self) -> DbKind {
        self.kind
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        match self.kind {
            DbKind::MySQL | DbKind::MariaDB => SchemaLoadingStrategy::LazyPerDatabase,
            DbKind::Postgres => SchemaLoadingStrategy::ConnectionPerDatabase,
            DbKind::SQLite | DbKind::MongoDB | DbKind::Redis => {
                SchemaLoadingStrategy::SingleDatabase
            }
        }
    }

    fn language_service(&self) -> &dyn dbflux_core::LanguageService {
        match self.kind {
            DbKind::Redis => &REDIS_LANGUAGE_SERVICE,
            _ => &SQL_LANGUAGE_SERVICE,
        }
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &DEFAULT_SQL_DIALECT
    }
}

fn active_database_from_profile(profile: &ConnectionProfile) -> Option<String> {
    match &profile.config {
        DbConfig::Postgres { database, .. } => Some(database.clone()),
        DbConfig::SQLite { path } => Some(path.display().to_string()),
        DbConfig::MySQL { database, .. } => database.clone(),
        DbConfig::MongoDB { database, .. } => database.clone(),
        DbConfig::Redis { database, .. } => database.map(|value| value.to_string()),
        DbConfig::External { values, .. } => values.get("database").cloned(),
    }
}

fn metadata_for_kind(kind: DbKind) -> &'static DriverMetadata {
    match kind {
        DbKind::Postgres => &FAKE_POSTGRES_METADATA,
        DbKind::SQLite => &FAKE_SQLITE_METADATA,
        DbKind::MySQL => &FAKE_MYSQL_METADATA,
        DbKind::MariaDB => &FAKE_MARIADB_METADATA,
        DbKind::MongoDB => &FAKE_MONGODB_METADATA,
        DbKind::Redis => &FAKE_REDIS_METADATA,
    }
}

fn form_for_kind(kind: DbKind) -> &'static DriverFormDef {
    match kind {
        DbKind::Postgres => &POSTGRES_FORM,
        DbKind::SQLite => &SQLITE_FORM,
        DbKind::MySQL | DbKind::MariaDB => &MYSQL_FORM,
        DbKind::MongoDB => &MONGODB_FORM,
        DbKind::Redis => &REDIS_FORM,
    }
}

fn get_string(values: &FormValues, key: &str, default: &str) -> String {
    values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or(default)
        .to_string()
}

fn get_optional_string(values: &FormValues, key: &str) -> Option<String> {
    values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(std::string::ToString::to_string)
}

fn get_u16(values: &FormValues, key: &str, default: u16) -> u16 {
    values
        .get(key)
        .and_then(|value| value.trim().parse::<u16>().ok())
        .unwrap_or(default)
}

fn get_u32_opt(values: &FormValues, key: &str) -> Option<u32> {
    values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<u32>().ok())
}

fn rwlock_read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poison_error) => poison_error.into_inner(),
    }
}

fn rwlock_write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poison_error) => poison_error.into_inner(),
    }
}

fn mutex_lock<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    match lock.lock() {
        Ok(guard) => guard,
        Err(poison_error) => poison_error.into_inner(),
    }
}

static DEFAULT_SQL_DIALECT: DefaultSqlDialect = DefaultSqlDialect;
static SQL_LANGUAGE_SERVICE: SqlLanguageService = SqlLanguageService;
static REDIS_LANGUAGE_SERVICE: RedisLanguageService = RedisLanguageService;

static FAKE_POSTGRES_METADATA: DriverMetadata = DriverMetadata {
    id: "fake-postgres",
    display_name: "Fake PostgreSQL",
    description: "Deterministic fake driver for tests",
    category: DatabaseCategory::Relational,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::RELATIONAL_BASE,
    default_port: Some(5432),
    uri_scheme: "postgresql",
    icon: Icon::Postgres,
};

static FAKE_SQLITE_METADATA: DriverMetadata = DriverMetadata {
    id: "fake-sqlite",
    display_name: "Fake SQLite",
    description: "Deterministic fake driver for tests",
    category: DatabaseCategory::Relational,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::RELATIONAL_BASE,
    default_port: None,
    uri_scheme: "sqlite",
    icon: Icon::Sqlite,
};

static FAKE_MYSQL_METADATA: DriverMetadata = DriverMetadata {
    id: "fake-mysql",
    display_name: "Fake MySQL",
    description: "Deterministic fake driver for tests",
    category: DatabaseCategory::Relational,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::RELATIONAL_BASE,
    default_port: Some(3306),
    uri_scheme: "mysql",
    icon: Icon::Mysql,
};

static FAKE_MARIADB_METADATA: DriverMetadata = DriverMetadata {
    id: "fake-mariadb",
    display_name: "Fake MariaDB",
    description: "Deterministic fake driver for tests",
    category: DatabaseCategory::Relational,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::RELATIONAL_BASE,
    default_port: Some(3306),
    uri_scheme: "mysql",
    icon: Icon::Mariadb,
};

static FAKE_MONGODB_METADATA: DriverMetadata = DriverMetadata {
    id: "fake-mongodb",
    display_name: "Fake MongoDB",
    description: "Deterministic fake driver for tests",
    category: DatabaseCategory::Document,
    query_language: QueryLanguage::MongoQuery,
    capabilities: DriverCapabilities::DOCUMENT_BASE,
    default_port: Some(27017),
    uri_scheme: "mongodb",
    icon: Icon::Mongodb,
};

static FAKE_REDIS_METADATA: DriverMetadata = DriverMetadata {
    id: "fake-redis",
    display_name: "Fake Redis",
    description: "Deterministic fake driver for tests",
    category: DatabaseCategory::KeyValue,
    query_language: QueryLanguage::RedisCommands,
    capabilities: DriverCapabilities::KEYVALUE_BASE,
    default_port: Some(6379),
    uri_scheme: "redis",
    icon: Icon::Redis,
};

#[cfg(test)]
mod tests {
    use super::{FakeDriver, FakeQueryOutcome};
    use crate::fixtures;
    use dbflux_core::{
        ConnectionProfile, DbConfig, DbDriver, DbError, DbKind, QueryRequest, SchemaLoadingStrategy,
    };

    #[test]
    fn sqlite_build_config_requires_path() {
        let driver = FakeDriver::new(DbKind::SQLite);
        let result = driver.build_config(&dbflux_core::FormValues::new());

        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn execute_uses_configured_outcome_and_records_stats() {
        let driver = FakeDriver::new(DbKind::Postgres)
            .with_query_error("SELECT boom", "boom")
            .with_default_result(dbflux_core::QueryResult::text(
                "ok".to_string(),
                std::time::Duration::ZERO,
            ));

        driver.set_query_outcome(
            "SELECT 1",
            FakeQueryOutcome::Success(dbflux_core::QueryResult::table(
                vec![],
                vec![],
                None,
                std::time::Duration::ZERO,
            )),
        );

        let profile = ConnectionProfile::new("fake", DbConfig::default_postgres());
        let connection = driver
            .connect(&profile)
            .expect("fake connection should work");

        let query_ok = connection.execute(&QueryRequest::new("SELECT 1"));
        assert!(query_ok.is_ok());

        let query_err = connection.execute(&QueryRequest::new("SELECT boom"));
        assert!(matches!(query_err, Err(DbError::QueryFailed(_))));

        let stats = driver.stats();
        assert_eq!(stats.executed_requests.len(), 2);
    }

    #[test]
    fn execute_with_database_switches_active_database() {
        let driver = FakeDriver::new(DbKind::MySQL);
        let profile = ConnectionProfile::new(
            "fake",
            DbConfig::MySQL {
                use_uri: false,
                uri: None,
                host: "localhost".to_string(),
                port: 3306,
                user: "root".to_string(),
                database: Some("default_db".to_string()),
                ssl_mode: dbflux_core::SslMode::Disable,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        );

        let connection = driver
            .connect(&profile)
            .expect("fake connection should work");

        assert_eq!(connection.active_database().as_deref(), Some("default_db"));

        let request = QueryRequest::new("SELECT 1").with_database(Some("analytics".to_string()));
        let _ = connection.execute(&request).expect("query should execute");

        assert_eq!(connection.active_database().as_deref(), Some("analytics"));
    }

    #[test]
    fn cancel_and_cancel_active_update_stats() {
        let driver = FakeDriver::new(DbKind::Postgres);
        let profile = ConnectionProfile::new("fake", DbConfig::default_postgres());
        let connection = driver
            .connect(&profile)
            .expect("fake connection should work");

        let (handle, _) = connection
            .execute_with_handle(&QueryRequest::new("SELECT 1"))
            .expect("query should execute with handle");

        connection.cancel(&handle).expect("cancel should succeed");
        connection
            .cancel_active()
            .expect("cancel active should succeed");

        let stats = driver.stats();
        assert_eq!(stats.cancelled_handle_count, 1);
        assert_eq!(stats.cancel_active_calls, 1);
    }

    #[test]
    fn schema_and_list_databases_use_configured_snapshot() {
        let driver = FakeDriver::new(DbKind::Postgres).with_schema(
            fixtures::relational_schema_with_table("app", "public", "users"),
        );
        let profile = ConnectionProfile::new("fake", DbConfig::default_postgres());
        let connection = driver
            .connect(&profile)
            .expect("fake connection should work");

        let schema = connection.schema().expect("schema should be available");
        assert_eq!(schema.databases().len(), 1);
        assert_eq!(schema.databases()[0].name, "app");

        let databases = connection
            .list_databases()
            .expect("list databases should succeed");
        assert_eq!(databases.len(), 1);
        assert_eq!(databases[0].name, "app");
    }

    #[test]
    fn connection_reports_expected_schema_loading_strategy_by_kind() {
        let cases = vec![
            (
                DbKind::Postgres,
                SchemaLoadingStrategy::ConnectionPerDatabase,
            ),
            (DbKind::MySQL, SchemaLoadingStrategy::LazyPerDatabase),
            (DbKind::MariaDB, SchemaLoadingStrategy::LazyPerDatabase),
            (DbKind::SQLite, SchemaLoadingStrategy::SingleDatabase),
            (DbKind::MongoDB, SchemaLoadingStrategy::SingleDatabase),
            (DbKind::Redis, SchemaLoadingStrategy::SingleDatabase),
        ];

        for (kind, expected_strategy) in cases {
            let driver = FakeDriver::new(kind);

            let config = match kind {
                DbKind::Postgres => DbConfig::default_postgres(),
                DbKind::SQLite => DbConfig::SQLite {
                    path: "/tmp/fake.db".into(),
                },
                DbKind::MySQL | DbKind::MariaDB => DbConfig::MySQL {
                    use_uri: false,
                    uri: None,
                    host: "localhost".to_string(),
                    port: 3306,
                    user: "root".to_string(),
                    database: Some("app".to_string()),
                    ssl_mode: dbflux_core::SslMode::Disable,
                    ssh_tunnel: None,
                    ssh_tunnel_profile_id: None,
                },
                DbKind::MongoDB => DbConfig::default_mongodb(),
                DbKind::Redis => DbConfig::default_redis(),
            };

            let profile = ConnectionProfile::new("fake", config);
            let connection = driver
                .connect(&profile)
                .expect("fake connection should work");

            assert_eq!(connection.schema_loading_strategy(), expected_strategy);
        }
    }
}
