use dbflux_core::chrono;
use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    Connection, ConnectionProfile, CrudResult, DatabaseCategory, DbConfig, DbDriver, DbError,
    DbKind, DdlCapabilities, DriverCapabilities, DriverFormDef, DriverLimits, DriverMetadata,
    FormFieldKind, FormSection, FormTab, FormValues, Icon, MutationCapabilities, QueryCapabilities,
    QueryHandle, QueryLanguage, QueryRequest, QueryResult, RowDelete, RowInsert, RowPatch,
    SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, SqlLanguageService, SyntaxInfo,
    TransactionCapabilities, TransferFamily, field, field_required,
};
use dbflux_core::{DatabaseInfo, DefaultSqlDialect};
use dbflux_core::{
    SecretDetails, SecretListPage, SecretListRequest, SecretRotation, SecretStoreCapabilities,
    SecretStoreDescriptor, SecretStoreSource, SecretSummary, SecretTag, SecretValue,
};
use dbflux_driver_cloudwatch::CLOUDWATCH_FORM;
use dbflux_driver_dynamodb::DYNAMODB_FORM;
use dbflux_driver_influxdb::INFLUXDB_FORM;
use dbflux_driver_mongodb::MONGODB_FORM;
use dbflux_driver_mssql::SQLSERVER_FORM;
use dbflux_driver_mysql::MYSQL_FORM;
use dbflux_driver_postgres::POSTGRES_FORM;
use dbflux_driver_redis::{REDIS_FORM, RedisLanguageService};
use dbflux_driver_redshift::{METADATA as REDSHIFT_METADATA, REDSHIFT_FORM};
use dbflux_driver_sqlite::SQLITE_FORM;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug, Clone)]
pub enum FakeQueryOutcome {
    // Box to keep all variants at a similar size; QueryResult grew with metadata_extra.
    Success(Box<QueryResult>),
    Error(String),
    Timeout,
    Cancelled,
}

impl FakeQueryOutcome {
    pub fn success(result: QueryResult) -> Self {
        Self::Success(Box::new(result))
    }

    fn to_result(&self) -> Result<QueryResult, Box<DbError>> {
        match self {
            Self::Success(result) => Ok(*result.clone()),
            Self::Error(message) => Err(Box::new(DbError::query_failed(message.clone()))),
            Self::Timeout => Err(Box::new(DbError::Timeout)),
            Self::Cancelled => Err(Box::new(DbError::Cancelled)),
        }
    }
}

/// A single recorded CRUD call made through `FakeConnection`.
#[derive(Debug, Clone)]
pub enum CrudOp {
    Update(RowPatch),
    Insert(RowInsert),
    Delete(RowDelete),
}

#[derive(Debug, Clone, Default)]
pub struct FakeDriverStats {
    pub executed_requests: Vec<QueryRequest>,
    pub crud_ops: Vec<CrudOp>,
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
    crud_ops: Mutex<Vec<CrudOp>>,
    cancelled_handles: Mutex<Vec<QueryHandle>>,
    cancel_active_calls: AtomicUsize,
    close_calls: AtomicUsize,
    ping_error: RwLock<Option<String>>,
    connect_error: RwLock<Option<String>>,
    refuse_read_only: AtomicBool,
    delete_row_errors: RwLock<Vec<(dbflux_core::Value, String)>>,
    secret_store_calls: Mutex<FakeSecretStoreCalls>,
}

#[derive(Clone)]
pub struct FakeDriver {
    kind: DbKind,
    state: Arc<FakeDriverState>,
    metadata: Arc<DriverMetadata>,
}

impl FakeDriver {
    pub fn new(kind: DbKind) -> Self {
        Self {
            kind,
            state: Arc::new(FakeDriverState {
                schema: RwLock::new(SchemaSnapshot::default()),
                ..FakeDriverState::default()
            }),
            metadata: Arc::new(metadata_for_kind(kind).clone()),
        }
    }

    /// Declares `supports_read_only` and runs requests that require read-only
    /// enforcement. The fake enforces nothing: it only lets callers observe
    /// that the flag reached the connection.
    pub fn with_read_only_enforcement(mut self) -> Self {
        self.set_read_only_capability(true);
        self
    }

    /// Declares `supports_read_only` but refuses every request that requires
    /// it with `NotSupported`, the way a driver does when the session is
    /// already inside a user transaction.
    pub fn with_read_only_refusal(mut self) -> Self {
        self.set_read_only_capability(true);
        self.state.refuse_read_only.store(true, Ordering::Relaxed);
        self
    }

    fn set_read_only_capability(&mut self, supported: bool) {
        let metadata = Arc::make_mut(&mut self.metadata);
        let transactions = metadata
            .transactions
            .get_or_insert_with(TransactionCapabilities::default);
        transactions.supports_read_only = supported;
    }

    pub fn with_schema(self, schema: SchemaSnapshot) -> Self {
        *rwlock_write(&self.state.schema) = schema;
        self
    }

    pub fn with_query_result(self, sql: impl Into<String>, result: QueryResult) -> Self {
        rwlock_write(&self.state.query_outcomes)
            .insert(sql.into(), FakeQueryOutcome::success(result));
        self
    }

    pub fn with_query_error(self, sql: impl Into<String>, message: impl Into<String>) -> Self {
        rwlock_write(&self.state.query_outcomes)
            .insert(sql.into(), FakeQueryOutcome::Error(message.into()));
        self
    }

    pub fn with_default_result(self, result: QueryResult) -> Self {
        *rwlock_write(&self.state.default_outcome) = Some(FakeQueryOutcome::success(result));
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

    /// Fails `delete_row` for every row whose identity holds `value`, the way
    /// a driver refuses a delete that a foreign key still references.
    pub fn with_delete_row_error(
        self,
        value: dbflux_core::Value,
        message: impl Into<String>,
    ) -> Self {
        rwlock_write(&self.state.delete_row_errors).push((value, message.into()));
        self
    }

    pub fn set_query_outcome(&self, sql: impl Into<String>, outcome: FakeQueryOutcome) {
        rwlock_write(&self.state.query_outcomes).insert(sql.into(), outcome);
    }

    pub fn stats(&self) -> FakeDriverStats {
        FakeDriverStats {
            executed_requests: mutex_lock(&self.state.executed_requests).clone(),
            crud_ops: mutex_lock(&self.state.crud_ops).clone(),
            cancelled_handle_count: mutex_lock(&self.state.cancelled_handles).len(),
            cancel_active_calls: self.state.cancel_active_calls.load(Ordering::Relaxed),
            close_calls: self.state.close_calls.load(Ordering::Relaxed),
        }
    }

    /// Every call the fake secret stores of a `DbKind::AwsSecrets` connection
    /// received, in order.
    pub fn secret_store_calls(&self) -> FakeSecretStoreCalls {
        mutex_lock(&self.state.secret_store_calls).clone()
    }

    pub fn as_driver_arc(self) -> Arc<dyn DbDriver> {
        Arc::new(self)
    }

    /// Connect and return the connection as `Arc<dyn Connection>` for use with
    /// `ConnectedProfile` in GPUI tests.
    pub fn connect_arc(&self, profile: &ConnectionProfile) -> Result<Arc<dyn Connection>, DbError> {
        let connection = self.connect_with_secrets(profile, None, None)?;
        Ok(Arc::from(connection))
    }
}

impl DbDriver for FakeDriver {
    fn kind(&self) -> DbKind {
        self.kind
    }

    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        format!("builtin:fake-{}", self.metadata().id)
    }

    fn form_definition(&self) -> &DriverFormDef {
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
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
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

                DbConfig::SQLite {
                    path: path.into(),
                    connection_id: None,
                }
            }
            DbKind::MySQL | DbKind::MariaDB => DbConfig::MySQL {
                use_uri: false,
                uri: None,
                host: get_string(values, "host", "localhost"),
                port: get_u16(values, "port", 3306),
                user: get_string(values, "user", "root"),
                database: get_optional_string(values, "database"),
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
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
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
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
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
                topology: None,
                sentinel_master_name: None,
                additional_nodes: None,
            },
            DbKind::DynamoDB => DbConfig::DynamoDB {
                region: get_string(values, "region", "us-east-1"),
                profile: get_optional_string(values, "profile"),
                endpoint: get_optional_string(values, "endpoint"),
                table: get_optional_string(values, "table"),
            },
            DbKind::CloudWatchLogs => DbConfig::CloudWatchLogs {
                region: get_string(values, "region", "us-east-1"),
                profile: get_optional_string(values, "profile"),
                endpoint: get_optional_string(values, "endpoint"),
            },
            DbKind::InfluxDB => DbConfig::InfluxDB {
                version: dbflux_core::InfluxVersion::V2,
                url: get_string(values, "url", "http://localhost:8086"),
                org: get_optional_string(values, "org"),
                default_bucket: get_optional_string(values, "default_bucket")
                    .or_else(|| get_optional_string(values, "bucket_or_database")),
                retention_policy: get_optional_string(values, "retention_policy"),
                user: get_optional_string(values, "user"),
                request_timeout_seconds: None,
            },
            DbKind::SqlServer => DbConfig::SqlServer {
                use_uri: false,
                uri: None,
                host: get_string(values, "host", "localhost"),
                port: get_u16(values, "port", 1433),
                user: get_string(values, "user", "sa"),
                database: get_optional_string(values, "database"),
                instance: get_optional_string(values, "instance"),
                ssl_mode: Some("on".to_string()),
                trust_server_certificate: true,
                ssl_root_cert_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
            DbKind::Redshift => DbConfig::Redshift {
                use_uri: false,
                uri: None,
                host: get_string(values, "host", "localhost"),
                port: get_u16(values, "port", 5439),
                user: get_string(values, "user", "awsuser"),
                database: get_string(values, "database", "dev"),
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
            DbKind::S3 => DbConfig::S3 {
                region: get_string(values, "region", "us-east-1"),
                profile: get_optional_string(values, "profile"),
                access_key_id: get_optional_string(values, "access_key_id"),
                endpoint: get_optional_string(values, "endpoint"),
                path_style: false,
            },
            DbKind::AwsSecrets => DbConfig::AwsSecrets {
                region: get_string(values, "region", "us-east-1"),
                profile: get_optional_string(values, "profile"),
                access_key_id: get_optional_string(values, "access_key_id"),
                endpoint: get_optional_string(values, "endpoint"),
            },
            DbKind::ClickHouse => DbConfig::ClickHouse {
                url: get_string(values, "url", "http://localhost:8123"),
                user: get_string(values, "user", "default"),
                database: get_string(values, "database", "default"),
                request_timeout_seconds: values
                    .get("request_timeout_seconds")
                    .and_then(|value| value.parse().ok()),
            },
            DbKind::Turso => DbConfig::Turso {
                url: get_string(values, "url", ""),
            },
            DbKind::DuckDB => DbConfig::DuckDB {
                path: get_string(values, "path", "").into(),
                ducklake_catalog: None,
                ducklake_data_path: None,
                init_sql: None,
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
            DbConfig::SQLite { path, .. } | DbConfig::DuckDB { path, .. } => {
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
            DbConfig::DynamoDB {
                region,
                profile,
                endpoint,
                table,
            } => {
                values.insert("region".to_string(), region.clone());
                values.insert("profile".to_string(), profile.clone().unwrap_or_default());
                values.insert("endpoint".to_string(), endpoint.clone().unwrap_or_default());
                values.insert("table".to_string(), table.clone().unwrap_or_default());
            }
            DbConfig::CloudWatchLogs {
                region,
                profile,
                endpoint,
            } => {
                values.insert("region".to_string(), region.clone());
                values.insert("profile".to_string(), profile.clone().unwrap_or_default());
                values.insert("endpoint".to_string(), endpoint.clone().unwrap_or_default());
            }
            DbConfig::InfluxDB {
                url,
                org,
                default_bucket,
                retention_policy,
                ..
            } => {
                values.insert("url".to_string(), url.clone());
                values.insert("org".to_string(), org.clone().unwrap_or_default());
                values.insert(
                    "default_bucket".to_string(),
                    default_bucket.clone().unwrap_or_default(),
                );
                values.insert(
                    "retention_policy".to_string(),
                    retention_policy.clone().unwrap_or_default(),
                );
            }
            DbConfig::SqlServer {
                host,
                port,
                user,
                database,
                instance,
                ..
            } => {
                values.insert("host".to_string(), host.clone());
                values.insert("port".to_string(), port.to_string());
                values.insert("user".to_string(), user.clone());
                values.insert("database".to_string(), database.clone().unwrap_or_default());
                values.insert("instance".to_string(), instance.clone().unwrap_or_default());
            }
            DbConfig::Redshift {
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
            DbConfig::S3 {
                region,
                profile,
                access_key_id,
                endpoint,
                ..
            }
            | DbConfig::AwsSecrets {
                region,
                profile,
                access_key_id,
                endpoint,
            } => {
                values.insert("region".to_string(), region.clone());
                values.insert("profile".to_string(), profile.clone().unwrap_or_default());
                values.insert(
                    "access_key_id".to_string(),
                    access_key_id.clone().unwrap_or_default(),
                );
                values.insert("endpoint".to_string(), endpoint.clone().unwrap_or_default());
            }
            DbConfig::ClickHouse {
                url,
                user,
                database,
                request_timeout_seconds,
            } => {
                values.insert("url".to_string(), url.clone());
                values.insert("user".to_string(), user.clone());
                values.insert("database".to_string(), database.clone());
                values.insert(
                    "request_timeout_seconds".to_string(),
                    request_timeout_seconds
                        .map(|timeout| timeout.to_string())
                        .unwrap_or_default(),
                );
            }
            DbConfig::Turso { url } => {
                values.insert("url".to_string(), url.clone());
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
        _password: Option<&SecretString>,
        _ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        if let Some(message) = rwlock_read(&self.state.connect_error).clone() {
            return Err(DbError::connection_failed(message));
        }

        Ok(Box::new(FakeConnection::new(
            self.kind,
            profile,
            self.state.clone(),
            self.metadata.clone(),
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
    metadata: Arc<DriverMetadata>,
    active_database: RwLock<Option<String>>,
    secret_stores: Vec<FakeSecretStore>,
}

impl FakeConnection {
    fn new(
        kind: DbKind,
        profile: &ConnectionProfile,
        state: Arc<FakeDriverState>,
        metadata: Arc<DriverMetadata>,
    ) -> Self {
        let secret_stores = match kind {
            DbKind::AwsSecrets => fake_secret_stores(&state),
            _ => Vec::new(),
        };

        Self {
            kind,
            state,
            metadata,
            active_database: RwLock::new(active_database_from_profile(profile)),
            secret_stores,
        }
    }

    fn execute_internal(&self, req: &QueryRequest) -> Result<QueryResult, Box<DbError>> {
        mutex_lock(&self.state.executed_requests).push(req.clone());

        let refuses_read_only = !self.metadata.enforces_read_only()
            || self.state.refuse_read_only.load(Ordering::Relaxed);
        if refuses_read_only {
            req.refuse_read_only_enforcement("Fake driver")
                .map_err(Box::new)?;
        }

        if let Some(database) = req.database.clone() {
            *rwlock_write(&self.active_database) = Some(database);
        }

        if let Some(outcome) = rwlock_read(&self.state.query_outcomes)
            .get(&req.sql)
            .cloned()
        {
            return outcome.to_result();
        }

        if let Some(outcome) = rwlock_read(&self.state.default_outcome).clone() {
            return outcome.to_result();
        }

        Ok(QueryResult::empty())
    }
}

impl Connection for FakeConnection {
    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
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
        self.execute_internal(req).map_err(|error| *error)
    }

    fn execute_with_handle(
        &self,
        req: &QueryRequest,
    ) -> Result<(QueryHandle, QueryResult), DbError> {
        let handle = QueryHandle::new();
        let result = self.execute_internal(req).map_err(|error| *error)?;
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
            DbKind::MySQL | DbKind::MariaDB | DbKind::SqlServer => {
                SchemaLoadingStrategy::LazyPerDatabase
            }
            DbKind::Postgres | DbKind::Redshift => SchemaLoadingStrategy::ConnectionPerDatabase,
            DbKind::SQLite | DbKind::MongoDB | DbKind::Redis => {
                SchemaLoadingStrategy::SingleDatabase
            }
            DbKind::DynamoDB | DbKind::CloudWatchLogs | DbKind::S3 | DbKind::AwsSecrets => {
                SchemaLoadingStrategy::SingleDatabase
            }
            DbKind::ClickHouse | DbKind::DuckDB | DbKind::InfluxDB => {
                SchemaLoadingStrategy::LazyPerDatabase
            }
            DbKind::Turso => SchemaLoadingStrategy::SingleDatabase,
        }
    }

    fn update_row(&self, patch: &RowPatch) -> Result<CrudResult, DbError> {
        mutex_lock(&self.state.crud_ops).push(CrudOp::Update(patch.clone()));
        Ok(CrudResult::new(1, None))
    }

    fn insert_row(&self, insert: &RowInsert) -> Result<CrudResult, DbError> {
        mutex_lock(&self.state.crud_ops).push(CrudOp::Insert(insert.clone()));
        Ok(CrudResult::new(1, None))
    }

    fn delete_row(&self, delete: &RowDelete) -> Result<CrudResult, DbError> {
        if let dbflux_core::RecordIdentity::Composite { values, .. } = &delete.identity
            && let Some((_, message)) = rwlock_read(&self.state.delete_row_errors)
                .iter()
                .find(|(value, _)| values.contains(value))
        {
            return Err(DbError::query_failed(message.clone()));
        }

        mutex_lock(&self.state.crud_ops).push(CrudOp::Delete(delete.clone()));
        Ok(CrudResult::new(1, None))
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

    fn secret_stores(&self) -> Vec<&dyn SecretStoreSource> {
        self.secret_stores
            .iter()
            .map(|store| store as &dyn SecretStoreSource)
            .collect()
    }
}

fn active_database_from_profile(profile: &ConnectionProfile) -> Option<String> {
    match &profile.config {
        DbConfig::Postgres { database, .. } => Some(database.clone()),
        DbConfig::SQLite { path, .. } | DbConfig::DuckDB { path, .. } => {
            Some(path.display().to_string())
        }
        DbConfig::MySQL { database, .. } => database.clone(),
        DbConfig::MongoDB { database, .. } => database.clone(),
        DbConfig::Redis { database, .. } => database.map(|value| value.to_string()),
        DbConfig::DynamoDB { table, .. } => table.clone(),
        DbConfig::CloudWatchLogs { .. } => None,
        DbConfig::InfluxDB { default_bucket, .. } => default_bucket.clone(),
        DbConfig::SqlServer { database, .. } => database.clone(),
        DbConfig::Redshift { database, .. } => Some(database.clone()),
        DbConfig::S3 { .. } | DbConfig::AwsSecrets { .. } => None,
        DbConfig::ClickHouse { database, .. } => Some(database.clone()),
        DbConfig::Turso { .. } => None,
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
        DbKind::DynamoDB => &FAKE_DYNAMODB_METADATA,
        DbKind::CloudWatchLogs => &FAKE_CLOUDWATCH_METADATA,
        DbKind::InfluxDB => &FAKE_INFLUXDB_METADATA,
        // Tests treat SQL Server like a relational driver — reuse postgres metadata.
        DbKind::SqlServer => &FAKE_POSTGRES_METADATA,
        // Redshift's read-only capability set differs meaningfully from
        // postgres (no write/DDL flags), so tests exercise the real driver
        // metadata instead of a hand-rolled fake.
        DbKind::Redshift => &REDSHIFT_METADATA,
        DbKind::S3 => &FAKE_S3_METADATA,
        DbKind::AwsSecrets => &FAKE_AWS_SECRETS_METADATA,
        DbKind::ClickHouse => &FAKE_CLICKHOUSE_METADATA,
        DbKind::Turso => &FAKE_TURSO_METADATA,
        // DuckDB is an embedded SQL engine; the SQLite fake covers it.
        DbKind::DuckDB => &FAKE_SQLITE_METADATA,
    }
}

fn form_for_kind(kind: DbKind) -> &'static DriverFormDef {
    match kind {
        DbKind::Postgres => &POSTGRES_FORM,
        DbKind::SQLite | DbKind::DuckDB => &SQLITE_FORM,
        DbKind::MySQL | DbKind::MariaDB => &MYSQL_FORM,
        DbKind::MongoDB => &MONGODB_FORM,
        DbKind::Redis => &REDIS_FORM,
        DbKind::DynamoDB => &DYNAMODB_FORM,
        DbKind::CloudWatchLogs => &CLOUDWATCH_FORM,
        DbKind::InfluxDB => &INFLUXDB_FORM,
        DbKind::SqlServer => &SQLSERVER_FORM,
        DbKind::Redshift => &REDSHIFT_FORM,
        // The AWS secrets driver asks for the same region, profile and
        // static-credential fields as S3.
        DbKind::S3 | DbKind::AwsSecrets => &S3_FORM,
        DbKind::ClickHouse => &CLICKHOUSE_FORM,
        DbKind::Turso => &TURSO_FORM,
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

static FAKE_POSTGRES_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-postgres".into(),
    display_name: "Fake PostgreSQL".into(),
    description: "Deterministic fake driver for tests".into(),
    category: DatabaseCategory::Relational,
    transfer_family: TransferFamily::Sql,
    deployment_class: None,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::RELATIONAL_BASE
        .union(DriverCapabilities::NATIVE_CONSOLE)
        .union(DriverCapabilities::REQUEST_ROW_LIMIT),
    default_port: Some(5432),
    uri_scheme: "postgresql".into(),
    icon: Icon::Postgres,
    syntax: Some(SyntaxInfo {
        identifier_quote: '"',
        string_quote: '\'',
        placeholder_style: dbflux_core::PlaceholderStyle::DollarNumber,
        supports_schemas: true,
        default_schema: Some("public".to_string()),
        case_sensitive_identifiers: true,
        misreads_unknown_quoted_identifiers: false,
    }),
    query: Some(QueryCapabilities::default()),
    mutation: Some(MutationCapabilities {
        supports_upsert: true,
        supports_returning: true,
        ..Default::default()
    }),
    ddl: Some(DdlCapabilities::default()),
    transactions: Some(TransactionCapabilities::default()),
    limits: Some(DriverLimits {
        max_parameters: 32767,
        max_identifier_length: 63,
        max_columns: 250,
        max_indexes_per_table: 32,
        ..Default::default()
    }),
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

static FAKE_CLICKHOUSE_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-clickhouse".into(),
    display_name: "Fake ClickHouse".into(),
    description: "Deterministic fake driver for tests".into(),
    category: DatabaseCategory::Relational,
    transfer_family: TransferFamily::Incompatible,
    deployment_class: None,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::MULTIPLE_DATABASES.bits()
            | DriverCapabilities::SSL.bits()
            | DriverCapabilities::AUTHENTICATION.bits()
            | DriverCapabilities::VIEWS.bits()
            | DriverCapabilities::PAGINATION.bits()
            | DriverCapabilities::SORTING.bits()
            | DriverCapabilities::FILTERING.bits()
            | DriverCapabilities::EXPORT_CSV.bits()
            | DriverCapabilities::EXPORT_JSON.bits()
            | DriverCapabilities::CHART_AUTHORING.bits(),
    ),
    default_port: Some(8123),
    uri_scheme: "http".into(),
    icon: Icon::Database,
    syntax: Some(SyntaxInfo {
        identifier_quote: '`',
        string_quote: '\'',
        placeholder_style: dbflux_core::PlaceholderStyle::QuestionMark,
        supports_schemas: false,
        default_schema: None,
        case_sensitive_identifiers: true,
        misreads_unknown_quoted_identifiers: false,
    }),
    query: Some(QueryCapabilities {
        pagination: vec![dbflux_core::PaginationStyle::Offset],
        where_operators: vec![
            dbflux_core::WhereOperator::Eq,
            dbflux_core::WhereOperator::Ne,
            dbflux_core::WhereOperator::Gt,
            dbflux_core::WhereOperator::Gte,
            dbflux_core::WhereOperator::Lt,
            dbflux_core::WhereOperator::Lte,
            dbflux_core::WhereOperator::Like,
            dbflux_core::WhereOperator::Null,
            dbflux_core::WhereOperator::In,
            dbflux_core::WhereOperator::NotIn,
            dbflux_core::WhereOperator::And,
            dbflux_core::WhereOperator::Or,
            dbflux_core::WhereOperator::Not,
        ],
        supports_order_by: true,
        order_by_mode: dbflux_core::OrderByMode::AnyColumns,
        supports_group_by: true,
        supports_having: true,
        supports_distinct: true,
        supports_limit: true,
        supports_offset: true,
        supports_joins: true,
        supports_subqueries: true,
        supports_union: true,
        supports_intersect: true,
        supports_except: true,
        supports_case_expressions: true,
        supports_window_functions: true,
        supports_ctes: true,
        supports_explain: true,
        max_query_parameters: 0,
        max_order_by_columns: 0,
        max_group_by_columns: 0,
    }),
    mutation: None,
    ddl: None,
    transactions: Some(TransactionCapabilities {
        supports_transactions: false,
        supported_isolation_levels: Vec::new(),
        default_isolation_level: None,
        supports_savepoints: false,
        supports_nested_transactions: false,
        supports_read_only: true,
        supports_deferrable: false,
    }),
    limits: None,
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

static CLICKHOUSE_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![FormSection {
            title: "Connection".into(),
            icon: None,
            fields: vec![
                field_required("url", "URL", FormFieldKind::Text, "http://localhost:8123"),
                field_required("user", "User", FormFieldKind::Text, "default"),
                field("password", "Password", FormFieldKind::Password, ""),
                field_required("database", "Database", FormFieldKind::Text, "default"),
                field(
                    "request_timeout_seconds",
                    "Request timeout (seconds)",
                    FormFieldKind::Number,
                    "optional",
                ),
            ],
        }],
    }],
});

static FAKE_TURSO_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-turso".into(),
    display_name: "Fake TursoDB".into(),
    description: "Deterministic fake driver for tests".into(),
    category: DatabaseCategory::Relational,
    transfer_family: TransferFamily::Sql,
    deployment_class: None,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::RELATIONAL_BASE & !DriverCapabilities::TRANSACTIONS,
    default_port: None,
    uri_scheme: "https".into(),
    icon: Icon::Database,
    syntax: Some(SyntaxInfo {
        identifier_quote: '"',
        string_quote: '\'',
        placeholder_style: dbflux_core::PlaceholderStyle::QuestionMark,
        supports_schemas: false,
        default_schema: None,
        case_sensitive_identifiers: true,
        misreads_unknown_quoted_identifiers: false,
    }),
    query: Some(QueryCapabilities::default()),
    mutation: Some(MutationCapabilities::default()),
    ddl: Some(DdlCapabilities::default()),
    transactions: None,
    limits: Some(DriverLimits {
        max_parameters: 32766,
        max_identifier_length: 100_000,
        max_columns: 32766,
        max_indexes_per_table: 64,
        ..Default::default()
    }),
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

static FAKE_SQLITE_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-sqlite".into(),
    display_name: "Fake SQLite".into(),
    description: "Deterministic fake driver for tests".into(),
    category: DatabaseCategory::Relational,
    transfer_family: TransferFamily::Sql,
    deployment_class: None,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::RELATIONAL_BASE,
    default_port: None,
    uri_scheme: "sqlite".into(),
    icon: Icon::Sqlite,
    syntax: Some(SyntaxInfo {
        identifier_quote: '"',
        string_quote: '\'',
        placeholder_style: dbflux_core::PlaceholderStyle::QuestionMark,
        supports_schemas: false,
        default_schema: None,
        case_sensitive_identifiers: true,
        misreads_unknown_quoted_identifiers: false,
    }),
    query: Some(QueryCapabilities::default()),
    mutation: Some(MutationCapabilities {
        supports_upsert: true,
        supports_returning: true,
        ..Default::default()
    }),
    ddl: Some(DdlCapabilities {
        supports_alter_table: false,
        supports_add_column: true,
        supports_rename_column: true,
        supports_drop_column: false,
        supports_alter_column: false,
        supports_add_constraint: false,
        supports_drop_constraint: false,
        transactional_ddl: false,
        ..Default::default()
    }),
    transactions: Some(TransactionCapabilities {
        supported_isolation_levels: vec![dbflux_core::IsolationLevel::ReadCommitted],
        default_isolation_level: Some(dbflux_core::IsolationLevel::ReadCommitted),
        supports_nested_transactions: false,
        supports_deferrable: true,
        ..Default::default()
    }),
    limits: Some(DriverLimits {
        max_query_length: 1_000_000_000,
        max_parameters: 32766,
        max_identifier_length: 100_000,
        max_columns: 32766,
        max_indexes_per_table: 64,
        ..Default::default()
    }),
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

static FAKE_MYSQL_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-mysql".into(),
    display_name: "Fake MySQL".into(),
    description: "Deterministic fake driver for tests".into(),
    category: DatabaseCategory::Relational,
    transfer_family: TransferFamily::Sql,
    deployment_class: None,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::RELATIONAL_BASE,
    default_port: Some(3306),
    uri_scheme: "mysql".into(),
    icon: Icon::Mysql,
    syntax: Some(SyntaxInfo {
        identifier_quote: '`',
        string_quote: '\'',
        placeholder_style: dbflux_core::PlaceholderStyle::QuestionMark,
        supports_schemas: false,
        default_schema: None,
        case_sensitive_identifiers: false,
        misreads_unknown_quoted_identifiers: false,
    }),
    query: Some(QueryCapabilities::default()),
    mutation: Some(MutationCapabilities::default()),
    ddl: Some(DdlCapabilities {
        transactional_ddl: false,
        supports_rename_column: false,
        supports_drop_column: false,
        ..Default::default()
    }),
    transactions: Some(TransactionCapabilities::default()),
    limits: Some(DriverLimits {
        max_parameters: 65535,
        max_identifier_length: 64,
        max_columns: 4096,
        max_indexes_per_table: 64,
        ..Default::default()
    }),
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

static FAKE_MARIADB_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-mariadb".into(),
    display_name: "Fake MariaDB".into(),
    description: "Deterministic fake driver for tests".into(),
    category: DatabaseCategory::Relational,
    transfer_family: TransferFamily::Sql,
    deployment_class: None,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::RELATIONAL_BASE,
    default_port: Some(3306),
    uri_scheme: "mysql".into(),
    icon: Icon::Mariadb,
    syntax: Some(SyntaxInfo {
        identifier_quote: '`',
        string_quote: '\'',
        placeholder_style: dbflux_core::PlaceholderStyle::QuestionMark,
        supports_schemas: false,
        default_schema: None,
        case_sensitive_identifiers: false,
        misreads_unknown_quoted_identifiers: false,
    }),
    query: Some(QueryCapabilities::default()),
    mutation: Some(MutationCapabilities::default()),
    ddl: Some(DdlCapabilities {
        transactional_ddl: false,
        supports_rename_column: false,
        supports_drop_column: false,
        ..Default::default()
    }),
    transactions: Some(TransactionCapabilities::default()),
    limits: Some(DriverLimits {
        max_parameters: 65535,
        max_identifier_length: 64,
        max_columns: 4096,
        max_indexes_per_table: 64,
        ..Default::default()
    }),
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

static FAKE_MONGODB_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-mongodb".into(),
    display_name: "Fake MongoDB".into(),
    description: "Deterministic fake driver for tests".into(),
    category: DatabaseCategory::Document,
    transfer_family: TransferFamily::Incompatible,
    deployment_class: None,
    query_language: QueryLanguage::MongoQuery,
    capabilities: DriverCapabilities::DOCUMENT_BASE.union(DriverCapabilities::NATIVE_CONSOLE),
    default_port: Some(27017),
    uri_scheme: "mongodb".into(),
    icon: Icon::Mongodb,
    syntax: None,
    query: Some(QueryCapabilities {
        pagination: vec![
            dbflux_core::PaginationStyle::Cursor,
            dbflux_core::PaginationStyle::PageToken,
        ],
        where_operators: vec![
            dbflux_core::WhereOperator::Eq,
            dbflux_core::WhereOperator::Ne,
            dbflux_core::WhereOperator::Gt,
            dbflux_core::WhereOperator::Gte,
            dbflux_core::WhereOperator::Lt,
            dbflux_core::WhereOperator::Lte,
            dbflux_core::WhereOperator::In,
            dbflux_core::WhereOperator::NotIn,
            dbflux_core::WhereOperator::And,
            dbflux_core::WhereOperator::Or,
            dbflux_core::WhereOperator::Not,
        ],
        supports_joins: false,
        supports_union: false,
        supports_intersect: false,
        supports_except: false,
        supports_ctes: false,
        ..Default::default()
    }),
    mutation: None,
    ddl: None,
    transactions: Some(TransactionCapabilities {
        supports_transactions: false,
        supported_isolation_levels: vec![],
        default_isolation_level: None,
        supports_savepoints: false,
        supports_nested_transactions: false,
        supports_read_only: true,
        supports_deferrable: false,
    }),
    limits: None,
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

static FAKE_REDIS_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-redis".into(),
    display_name: "Fake Redis".into(),
    description: "Deterministic fake driver for tests".into(),
    category: DatabaseCategory::KeyValue,
    transfer_family: TransferFamily::Incompatible,
    deployment_class: None,
    query_language: QueryLanguage::RedisCommands,
    capabilities: DriverCapabilities::KEYVALUE_BASE.union(DriverCapabilities::NATIVE_CONSOLE),
    default_port: Some(6379),
    uri_scheme: "redis".into(),
    icon: Icon::Redis,
    syntax: None,
    query: Some(QueryCapabilities {
        pagination: vec![dbflux_core::PaginationStyle::Cursor],
        where_operators: vec![],
        supports_order_by: false,
        supports_group_by: false,
        supports_having: false,
        supports_distinct: false,
        supports_limit: false,
        supports_offset: false,
        supports_joins: false,
        supports_subqueries: false,
        supports_union: false,
        supports_intersect: false,
        supports_except: false,
        supports_case_expressions: false,
        supports_window_functions: false,
        supports_ctes: false,
        supports_explain: false,
        ..Default::default()
    }),
    mutation: None,
    ddl: None,
    transactions: Some(TransactionCapabilities {
        supports_transactions: false,
        supported_isolation_levels: vec![],
        default_isolation_level: None,
        supports_savepoints: false,
        supports_nested_transactions: false,
        supports_read_only: true,
        supports_deferrable: false,
    }),
    limits: None,
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

static FAKE_DYNAMODB_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-dynamodb".into(),
    display_name: "Fake DynamoDB".into(),
    description: "Deterministic fake driver for tests".into(),
    category: DatabaseCategory::Document,
    transfer_family: TransferFamily::Incompatible,
    deployment_class: None,
    query_language: QueryLanguage::Custom("DynamoDB".into()),
    capabilities: DriverCapabilities::DOCUMENT_BASE,
    default_port: None,
    uri_scheme: "dynamodb".into(),
    icon: Icon::Dynamodb,
    syntax: None,
    query: None,
    mutation: None,
    ddl: None,
    transactions: Some(TransactionCapabilities {
        supports_transactions: false,
        supported_isolation_levels: vec![],
        default_isolation_level: None,
        supports_savepoints: false,
        supports_nested_transactions: false,
        supports_read_only: false,
        supports_deferrable: false,
    }),
    limits: None,
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

static FAKE_S3_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-s3".into(),
    display_name: "Fake S3".into(),
    description: "Deterministic fake driver for tests".into(),
    category: DatabaseCategory::ObjectStorage,
    transfer_family: TransferFamily::Incompatible,
    deployment_class: None,
    query_language: QueryLanguage::Custom("S3".into()),
    capabilities: DriverCapabilities::OBJECT_STORAGE | DriverCapabilities::OBJECT_PREFIX_DELETE,
    default_port: None,
    uri_scheme: "s3".into(),
    icon: Icon::S3,
    syntax: None,
    query: None,
    mutation: None,
    ddl: None,
    transactions: Some(TransactionCapabilities {
        supports_transactions: false,
        supported_isolation_levels: vec![],
        default_isolation_level: None,
        supports_savepoints: false,
        supports_nested_transactions: false,
        supports_read_only: false,
        supports_deferrable: false,
    }),
    limits: None,
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

static FAKE_AWS_SECRETS_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-aws-secrets".into(),
    display_name: "Fake AWS Secrets".into(),
    description: "Deterministic fake driver for tests".into(),
    category: DatabaseCategory::SecretStore,
    transfer_family: TransferFamily::Incompatible,
    deployment_class: None,
    query_language: QueryLanguage::Custom("Secrets".into()),
    capabilities: DriverCapabilities::SECRET_STORE,
    default_port: None,
    uri_scheme: "aws-secrets".into(),
    icon: Icon::Database,
    syntax: None,
    query: None,
    mutation: None,
    ddl: None,
    transactions: None,
    limits: None,
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

/// Minimal placeholder form for tests — the real `S3_FORM` (with the full
/// static-credentials/endpoint/path-style field set) is defined in
/// `dbflux_driver_s3` once the driver itself is implemented.
static S3_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![FormSection {
            title: "AWS".into(),
            icon: None,
            fields: vec![
                field_required("region", "Region", FormFieldKind::Text, "us-east-1"),
                field(
                    "profile",
                    "Profile",
                    FormFieldKind::AuthProfileRef { provider_id: None },
                    "",
                ),
                field(
                    "access_key_id",
                    "Access Key ID",
                    FormFieldKind::Text,
                    "optional",
                ),
                field(
                    "endpoint",
                    "Endpoint Override",
                    FormFieldKind::Text,
                    "optional",
                ),
            ],
        }],
    }],
});

static TURSO_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![FormSection {
            title: "Connection".into(),
            icon: None,
            fields: vec![
                field_required("url", "URL", FormFieldKind::Text, ""),
                field("password", "Auth Token", FormFieldKind::Password, ""),
            ],
        }],
    }],
});

static FAKE_CLOUDWATCH_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-cloudwatch".into(),
    display_name: "Fake CloudWatch Logs".into(),
    description: "Deterministic fake driver for tests".into(),
    category: DatabaseCategory::Document,
    transfer_family: TransferFamily::Incompatible,
    deployment_class: None,
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::AUTHENTICATION,
    default_port: None,
    uri_scheme: "cloudwatch".into(),
    icon: Icon::Dynamodb,
    syntax: None,
    query: None,
    mutation: None,
    ddl: None,
    transactions: Some(TransactionCapabilities {
        supports_transactions: false,
        supported_isolation_levels: vec![],
        default_isolation_level: None,
        supports_savepoints: false,
        supports_nested_transactions: false,
        supports_read_only: false,
        supports_deferrable: false,
    }),
    limits: None,
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

static FAKE_INFLUXDB_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "fake-influxdb".into(),
    display_name: "Fake InfluxDB".into(),
    description: "Deterministic fake driver for tests".into(),
    category: dbflux_core::DatabaseCategory::TimeSeries,
    transfer_family: TransferFamily::Incompatible,
    deployment_class: None,
    query_language: QueryLanguage::Flux,
    capabilities: DriverCapabilities::AUTHENTICATION,
    default_port: Some(8086),
    uri_scheme: "http".into(),
    icon: Icon::Influxdb,
    syntax: None,
    query: None,
    mutation: None,
    ddl: None,
    transactions: Some(TransactionCapabilities {
        supports_transactions: false,
        supported_isolation_levels: vec![],
        default_isolation_level: None,
        supports_savepoints: false,
        supports_nested_transactions: false,
        supports_read_only: false,
        supports_deferrable: false,
    }),
    limits: None,
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

#[cfg(test)]
mod tests {
    use super::{FakeDriver, FakeQueryOutcome};
    use crate::fixtures;
    use dbflux_core::{
        ConnectionProfile, DatabaseCategory, DbConfig, DbDriver, DbError, DbKind,
        DriverCapabilities, QueryRequest, ReadOnlyEnforcement, SchemaLoadingStrategy,
        TransferFamily,
    };

    #[test]
    fn sqlite_build_config_requires_path() {
        let driver = FakeDriver::new(DbKind::SQLite);
        let result = driver.build_config(&dbflux_core::FormValues::new());

        assert!(matches!(result, Err(DbError::InvalidProfile(_))));
    }

    #[test]
    fn clickhouse_metadata_matches_read_only_contract() {
        let driver = FakeDriver::new(DbKind::ClickHouse);
        let metadata = driver.metadata();
        let expected = DriverCapabilities::MULTIPLE_DATABASES
            | DriverCapabilities::SSL
            | DriverCapabilities::AUTHENTICATION
            | DriverCapabilities::VIEWS
            | DriverCapabilities::PAGINATION
            | DriverCapabilities::SORTING
            | DriverCapabilities::FILTERING
            | DriverCapabilities::EXPORT_CSV
            | DriverCapabilities::EXPORT_JSON
            | DriverCapabilities::CHART_AUTHORING;

        assert_eq!(metadata.category, DatabaseCategory::Relational);
        assert_eq!(metadata.transfer_family, TransferFamily::Incompatible);
        assert_eq!(metadata.capabilities, expected);
        assert!(!metadata.syntax.as_ref().expect("syntax").supports_schemas);
        assert!(metadata.query.is_some());
        assert!(metadata.mutation.is_none());
        assert!(metadata.enforces_read_only());
    }

    #[test]
    fn mongodb_metadata_matches_the_real_driver_read_only_contract() {
        let driver = FakeDriver::new(DbKind::MongoDB);

        assert!(driver.metadata().enforces_read_only());
        assert_eq!(
            driver.metadata().enforces_read_only(),
            dbflux_driver_mongodb::MONGODB_METADATA.enforces_read_only()
        );
    }

    #[test]
    fn redis_metadata_matches_the_real_driver_read_only_contract() {
        let driver = FakeDriver::new(DbKind::Redis);

        assert!(driver.metadata().enforces_read_only());
        assert_eq!(
            driver.metadata().enforces_read_only(),
            dbflux_driver_redis::REDIS_METADATA.enforces_read_only()
        );
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
            FakeQueryOutcome::success(dbflux_core::QueryResult::table(
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
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
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
            (DbKind::Turso, SchemaLoadingStrategy::SingleDatabase),
            (DbKind::DuckDB, SchemaLoadingStrategy::LazyPerDatabase),
            (DbKind::MongoDB, SchemaLoadingStrategy::SingleDatabase),
            (DbKind::Redis, SchemaLoadingStrategy::SingleDatabase),
            (DbKind::ClickHouse, SchemaLoadingStrategy::LazyPerDatabase),
            (DbKind::DynamoDB, SchemaLoadingStrategy::SingleDatabase),
            (DbKind::InfluxDB, SchemaLoadingStrategy::LazyPerDatabase),
            (
                DbKind::CloudWatchLogs,
                SchemaLoadingStrategy::SingleDatabase,
            ),
            (DbKind::SqlServer, SchemaLoadingStrategy::LazyPerDatabase),
            (
                DbKind::Redshift,
                SchemaLoadingStrategy::ConnectionPerDatabase,
            ),
        ];

        for (kind, expected_strategy) in cases {
            let driver = FakeDriver::new(kind);

            let config = match kind {
                DbKind::Postgres => DbConfig::default_postgres(),
                DbKind::SQLite => DbConfig::SQLite {
                    path: "/tmp/fake.db".into(),
                    connection_id: None,
                },
                DbKind::MySQL | DbKind::MariaDB => DbConfig::MySQL {
                    use_uri: false,
                    uri: None,
                    host: "localhost".to_string(),
                    port: 3306,
                    user: "root".to_string(),
                    database: Some("app".to_string()),
                    ssl_mode: None,
                    ssl_root_cert_path: None,
                    ssl_client_cert_path: None,
                    ssl_client_key_path: None,
                    ssh_tunnel: None,
                    ssh_tunnel_profile_id: None,
                },
                DbKind::MongoDB => DbConfig::default_mongodb(),
                DbKind::Redis => DbConfig::default_redis(),
                DbKind::DynamoDB => DbConfig::default_dynamodb(),
                DbKind::CloudWatchLogs => DbConfig::default_cloudwatch_logs(),
                DbKind::InfluxDB => DbConfig::default_influxdb(),
                DbKind::SqlServer => DbConfig::default_sqlserver(),
                DbKind::Redshift => DbConfig::default_redshift(),
                DbKind::S3 => DbConfig::default_s3(),
                DbKind::AwsSecrets => DbConfig::default_aws_secrets(),
                DbKind::ClickHouse => DbConfig::default_clickhouse(),
                DbKind::Turso => DbConfig::default_turso(),
                DbKind::DuckDB => DbConfig::default_duckdb(),
            };

            let profile = ConnectionProfile::new("fake", config);
            let connection = driver
                .connect(&profile)
                .expect("fake connection should work");

            assert_eq!(connection.schema_loading_strategy(), expected_strategy);
        }
    }

    #[test]
    fn turso_fake_driver_roundtrips_url_and_keeps_transactions_disabled() {
        let driver = FakeDriver::new(DbKind::Turso);
        let values = dbflux_core::FormValues::from([(
            "url".to_string(),
            "https://example.turso.io".to_string(),
        )]);
        let config = driver.build_config(&values).expect("build Turso config");

        assert!(matches!(
            config,
            DbConfig::Turso { ref url } if url == "https://example.turso.io"
        ));
        assert_eq!(driver.extract_values(&config), values);
        assert!(
            !driver
                .metadata()
                .capabilities
                .contains(DriverCapabilities::TRANSACTIONS)
        );
        assert!(driver.metadata().transactions.is_none());
    }

    fn connect_postgres(driver: &FakeDriver) -> Box<dyn dbflux_core::Connection> {
        let profile = ConnectionProfile::new("fake", DbConfig::default_postgres());
        driver
            .connect(&profile)
            .expect("fake connection should work")
    }

    fn read_only_request() -> QueryRequest {
        QueryRequest::new("SELECT 1").with_read_only(ReadOnlyEnforcement::Required)
    }

    #[test]
    fn fake_without_enforcement_records_and_refuses_read_only_requests() {
        let driver = FakeDriver::new(DbKind::Postgres);
        let connection = connect_postgres(&driver);

        assert!(!connection.metadata().enforces_read_only());

        let result = connection.execute(&read_only_request());

        assert!(matches!(result, Err(DbError::NotSupported(_))));
        let executed = driver.stats().executed_requests;
        assert_eq!(executed.len(), 1);
        assert_eq!(executed[0].read_only, ReadOnlyEnforcement::Required);
    }

    #[test]
    fn fake_with_read_only_enforcement_declares_it_and_runs_the_request() {
        let driver = FakeDriver::new(DbKind::Postgres).with_read_only_enforcement();
        let connection = connect_postgres(&driver);

        assert!(driver.metadata().enforces_read_only());
        assert!(connection.metadata().enforces_read_only());
        assert!(connection.execute(&read_only_request()).is_ok());
        assert_eq!(
            driver.stats().executed_requests[0].read_only,
            ReadOnlyEnforcement::Required
        );
    }

    #[test]
    fn fake_with_read_only_refusal_declares_enforcement_but_refuses_at_execution() {
        let driver = FakeDriver::new(DbKind::Postgres).with_read_only_refusal();
        let connection = connect_postgres(&driver);

        assert!(connection.metadata().enforces_read_only());
        assert!(matches!(
            connection.execute(&read_only_request()),
            Err(DbError::NotSupported(_))
        ));
        assert!(connection.execute(&QueryRequest::new("SELECT 1")).is_ok());
    }
}

// ---------------------------------------------------------------------------
// Fake secret stores
// ---------------------------------------------------------------------------

/// The calls the fake secret stores received, as `(store_id, argument)`.
#[derive(Debug, Clone, Default)]
pub struct FakeSecretStoreCalls {
    pub list_requests: Vec<(String, SecretListRequest)>,
    pub described: Vec<(String, String)>,
    pub value_requests: Vec<(String, String)>,
}

/// Id of the fake flat store, shaped like AWS Secrets Manager.
pub const FAKE_SECRETS_STORE_ID: &str = "secrets-manager";

/// Id of the fake hierarchical store, shaped like SSM Parameter Store.
pub const FAKE_PARAMETER_STORE_ID: &str = "parameter-store";

/// Value the fake flat store returns for its key/value secret's `password`.
pub const FAKE_SECRET_PASSWORD: &str = "fake-password-7f3a";

/// Value the fake flat store returns for its plaintext secret.
pub const FAKE_SECRET_PLAINTEXT: &str = "fake plaintext token 91c2";

/// Bytes the fake flat store returns for its binary secret.
pub const FAKE_SECRET_BINARY: &[u8] = &[0xde, 0xad, 0xbe, 0xef, 0x00, 0x42];

/// Error the fake parameter store returns for its `SecureString`.
pub const FAKE_KMS_DENIED_MESSAGE: &str =
    "User is not authorized to perform kms:Decrypt on key alias/app-prod";

enum FakeSecretPayload {
    Text(String),
    Binary(Vec<u8>),
    Denied(&'static str),
    Unavailable(&'static str),
}

struct FakeSecret {
    details: SecretDetails,
    payload: FakeSecretPayload,
}

/// A deterministic in-memory secret store. A flat store matches its name
/// filter as a prefix, a hierarchical one as a substring of the full name.
struct FakeSecretStore {
    descriptor: SecretStoreDescriptor,
    secrets: Vec<FakeSecret>,
    default_page_size: usize,
    state: Arc<FakeDriverState>,
}

impl FakeSecretStore {
    fn record_list(&self, request: &SecretListRequest) {
        mutex_lock(&self.state.secret_store_calls)
            .list_requests
            .push((self.descriptor.id.clone(), request.clone()));
    }

    fn hierarchical(&self) -> bool {
        self.descriptor
            .supports(SecretStoreCapabilities::HIERARCHICAL)
    }

    fn matches_filter(&self, name: &str, filter: Option<&str>) -> bool {
        match filter.map(str::trim).filter(|filter| !filter.is_empty()) {
            None => true,
            Some(filter) if self.hierarchical() => name.contains(filter),
            Some(filter) => name.starts_with(filter),
        }
    }

    /// Entries of one listing before paging, and the sub-paths found under
    /// the requested path.
    fn listing(&self, request: &SecretListRequest) -> (Vec<&FakeSecret>, Vec<String>) {
        let filter = request.name_filter.as_deref();

        if !self.hierarchical() {
            let entries = self
                .secrets
                .iter()
                .filter(|secret| self.matches_filter(&secret.details.summary.name, filter))
                .collect();
            return (entries, Vec::new());
        }

        let path = request.path.clone().unwrap_or_else(|| "/".to_string());
        let mut entries = Vec::new();
        let mut child_paths = Vec::new();

        for secret in &self.secrets {
            let name = &secret.details.summary.name;
            let Some(rest) = name.strip_prefix(path.as_str()) else {
                continue;
            };

            match rest.split_once('/') {
                Some((segment, _)) if !request.recursive => {
                    let child = format!("{path}{segment}/");
                    if !child_paths.contains(&child) {
                        child_paths.push(child);
                    }
                }
                _ => {
                    if self.matches_filter(name, filter) {
                        entries.push(secret);
                    }
                }
            }
        }

        (entries, child_paths)
    }

    fn find(&self, name: &str) -> Result<&FakeSecret, DbError> {
        self.secrets
            .iter()
            .find(|secret| secret.details.summary.name == name)
            .ok_or_else(|| DbError::object_not_found(format!("Secret {name} not found")))
    }
}

impl SecretStoreSource for FakeSecretStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.descriptor
    }

    /// Pages by offset: the page token is the index of the first entry of
    /// the next page. Every page repeats the full set of sub-paths, which is
    /// what a real store may do, so callers must merge them.
    fn list(&self, request: &SecretListRequest) -> Result<SecretListPage, DbError> {
        self.record_list(request);

        let (entries, child_paths) = self.listing(request);
        let start = match request.page_token.as_deref() {
            Some(token) => token
                .parse::<usize>()
                .map_err(|_| DbError::query_failed(format!("Invalid page token {token}")))?,
            None => 0,
        };
        // `default_page_size` stands in for the service's own maximum, so a
        // request for a larger page is clamped instead of honored: that is
        // what a real store does with `max_results`.
        let page_size = request
            .page_size
            .map_or(self.default_page_size, |size| size as usize)
            .clamp(1, self.default_page_size);

        let end = (start + page_size).min(entries.len());
        let page: Vec<SecretSummary> = entries
            .get(start..end)
            .unwrap_or_default()
            .iter()
            .map(|secret| secret.details.summary.clone())
            .collect();

        Ok(SecretListPage {
            entries: page,
            child_paths,
            next_page_token: (end < entries.len()).then(|| end.to_string()),
        })
    }

    fn describe(&self, name: &str) -> Result<SecretDetails, DbError> {
        mutex_lock(&self.state.secret_store_calls)
            .described
            .push((self.descriptor.id.clone(), name.to_string()));

        self.find(name).map(|secret| secret.details.clone())
    }

    fn get_value(&self, name: &str, _version: Option<&str>) -> Result<SecretValue, DbError> {
        mutex_lock(&self.state.secret_store_calls)
            .value_requests
            .push((self.descriptor.id.clone(), name.to_string()));

        let secret = self.find(name)?;
        let version = secret.details.summary.version.clone();

        match &secret.payload {
            FakeSecretPayload::Text(text) => Ok(SecretValue::text(version, text.clone())),
            FakeSecretPayload::Binary(bytes) => Ok(SecretValue::binary(version, bytes.clone())),
            FakeSecretPayload::Denied(message) => Err(DbError::permission_denied(*message)),
            FakeSecretPayload::Unavailable(message) => Err(DbError::query_failed(*message)),
        }
    }
}

fn fake_time(month: u32, day: u32, hour: u32) -> Option<chrono::DateTime<chrono::Utc>> {
    use chrono::TimeZone as _;

    chrono::Utc
        .with_ymd_and_hms(2026, month, day, hour, 0, 0)
        .single()
}

fn fake_secret(
    summary: SecretSummary,
    details: impl FnOnce(SecretDetails) -> SecretDetails,
    payload: FakeSecretPayload,
) -> FakeSecret {
    let base = SecretDetails {
        resource_id: Some(format!("arn:fake:secret:{}", summary.name)),
        created: fake_time(1, 10, 8),
        summary,
        ..SecretDetails::default()
    };

    FakeSecret {
        details: details(base),
        payload,
    }
}

fn fake_secret_stores(state: &Arc<FakeDriverState>) -> Vec<FakeSecretStore> {
    vec![
        FakeSecretStore {
            descriptor: SecretStoreDescriptor::new(
                FAKE_SECRETS_STORE_ID,
                "Secrets Manager",
                SecretStoreCapabilities::VERSIONS
                    | SecretStoreCapabilities::VERSION_LABELS
                    | SecretStoreCapabilities::ROTATION
                    | SecretStoreCapabilities::SOFT_DELETE,
            ),
            secrets: fake_flat_secrets(),
            default_page_size: 4,
            state: state.clone(),
        },
        FakeSecretStore {
            descriptor: SecretStoreDescriptor::new(
                FAKE_PARAMETER_STORE_ID,
                "Parameter Store",
                SecretStoreCapabilities::VERSIONS
                    | SecretStoreCapabilities::HIERARCHICAL
                    | SecretStoreCapabilities::TYPED_VALUES,
            ),
            secrets: fake_parameters(),
            default_page_size: 2,
            state: state.clone(),
        },
    ]
}

/// Six secrets, so the default page of four leaves a second page: a
/// key/value secret with rotation, a secret managed by another service, a
/// plaintext one, a binary one, one pending deletion and one more key/value.
fn fake_flat_secrets() -> Vec<FakeSecret> {
    let rotation = SecretRotation {
        enabled: true,
        schedule: Some("rate(30 days)".to_string()),
        window: Some("4h".to_string()),
        function: Some("rotate-api-credentials".to_string()),
        last_rotated: fake_time(9, 20, 3),
        next_rotation: fake_time(10, 20, 3),
    };

    vec![
        fake_secret(
            SecretSummary {
                description: Some("Payments API credentials".to_string()),
                version: Some("v-current".to_string()),
                last_changed: fake_time(9, 20, 3),
                rotation: Some(rotation),
                ..SecretSummary::new("prod/api/credentials")
            },
            |details| SecretDetails {
                encryption_key: Some("alias/secrets".to_string()),
                last_accessed: fake_time(10, 1, 0),
                version_count: Some(4),
                version_labels: vec!["AWSCURRENT".to_string(), "AWSPREVIOUS".to_string()],
                tags: vec![
                    SecretTag::new("team", "payments"),
                    SecretTag::new("env", "prod"),
                ],
                ..details
            },
            FakeSecretPayload::Text(format!(
                r#"{{"username":"api-user","password":"{FAKE_SECRET_PASSWORD}","port":5432}}"#
            )),
        ),
        fake_secret(
            SecretSummary {
                description: Some("Master password of the orders database".to_string()),
                last_changed: fake_time(8, 2, 12),
                owning_service: Some("rds".to_string()),
                ..SecretSummary::new("prod/db/master")
            },
            |details| SecretDetails {
                version_count: Some(1),
                version_labels: vec!["AWSCURRENT".to_string()],
                ..details
            },
            FakeSecretPayload::Text(r#"{"username":"admin","password":"managed-pass"}"#.into()),
        ),
        fake_secret(
            SecretSummary {
                description: Some("Legacy deploy token".to_string()),
                last_changed: fake_time(7, 15, 9),
                deletion_scheduled_for: fake_time(11, 1, 0),
                ..SecretSummary::new("prod/deploy/legacy-token")
            },
            |details| SecretDetails {
                deleted_at: fake_time(10, 2, 0),
                ..details
            },
            FakeSecretPayload::Unavailable("The secret is scheduled for deletion"),
        ),
        fake_secret(
            SecretSummary {
                description: Some("Webhook signing token".to_string()),
                last_changed: fake_time(6, 5, 11),
                ..SecretSummary::new("prod/notify/webhook-token")
            },
            |details| details,
            FakeSecretPayload::Text(FAKE_SECRET_PLAINTEXT.to_string()),
        ),
        fake_secret(
            SecretSummary {
                description: Some("Signing key in DER form".to_string()),
                last_changed: fake_time(5, 3, 14),
                ..SecretSummary::new("prod/tls/signing-key")
            },
            |details| details,
            FakeSecretPayload::Binary(FAKE_SECRET_BINARY.to_vec()),
        ),
        fake_secret(
            SecretSummary {
                description: Some("Staging API credentials".to_string()),
                last_changed: fake_time(4, 8, 16),
                ..SecretSummary::new("staging/api/credentials")
            },
            |details| details,
            FakeSecretPayload::Text(r#"{"username":"staging","password":"staging-pass"}"#.into()),
        ),
    ]
}

fn fake_parameter(
    name: &str,
    value_type: &str,
    tier: &str,
    payload: FakeSecretPayload,
) -> FakeSecret {
    fake_secret(
        SecretSummary {
            value_type: Some(value_type.to_string()),
            tier: Some(tier.to_string()),
            version: Some("3".to_string()),
            last_changed: fake_time(9, 1, 10),
            ..SecretSummary::new(name)
        },
        |details| SecretDetails {
            data_type: Some("text".to_string()),
            last_changed_by: Some("arn:fake:user/deployer".to_string()),
            encryption_key: (value_type == "SecureString").then(|| "alias/app-prod".to_string()),
            tags: vec![SecretTag::new("app", "shop")],
            ..details
        },
        payload,
    )
}

/// A path tree `/app/prod/{db,feature-flags}/…`, `/app/staging/db/…` and
/// `/shared/…`, with a `SecureString` the caller may not decrypt.
fn fake_parameters() -> Vec<FakeSecret> {
    vec![
        fake_parameter(
            "/app/prod/db/host",
            "String",
            "Standard",
            FakeSecretPayload::Text("db.prod.internal".to_string()),
        ),
        fake_parameter(
            "/app/prod/db/password",
            "SecureString",
            "Standard",
            FakeSecretPayload::Denied(FAKE_KMS_DENIED_MESSAGE),
        ),
        fake_parameter(
            "/app/prod/db/port",
            "String",
            "Standard",
            FakeSecretPayload::Text("5432".to_string()),
        ),
        fake_parameter(
            "/app/prod/feature-flags/checkout",
            "String",
            "Advanced",
            FakeSecretPayload::Text("enabled".to_string()),
        ),
        fake_parameter(
            "/app/prod/region",
            "String",
            "Standard",
            FakeSecretPayload::Text("eu-west-1".to_string()),
        ),
        fake_parameter(
            "/app/staging/db/host",
            "String",
            "Standard",
            FakeSecretPayload::Text("db.staging.internal".to_string()),
        ),
        fake_parameter(
            "/shared/endpoint",
            "String",
            "Standard",
            FakeSecretPayload::Text("https://api.example.test".to_string()),
        ),
    ]
}
