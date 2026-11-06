use dbflux_core::chrono;
use dbflux_core::secrecy::{ExposeSecret, SecretSlice, SecretString};
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
    SecretDetails, SecretListPage, SecretListRequest, SecretPayload, SecretPutRequest,
    SecretRotation, SecretStoreCapabilities, SecretStoreDescriptor, SecretStoreSource,
    SecretSummary, SecretTag, SecretValue, SecretVersionStage,
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
    secret_write_errors: RwLock<Vec<(String, String)>>,
    region_switch_enabled: AtomicBool,
    available_regions: RwLock<Vec<String>>,
    region_list_error: RwLock<Option<String>>,
    available_regions_calls: AtomicUsize,
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

    /// Opts a fake into the live region-switch seam: it advertises
    /// `DriverCapabilities::REGION_SWITCH`, answers `available_regions` with
    /// the fixed list and rewrites the region of an `AwsSecrets` config. The
    /// current region comes from the profile each connection was built with,
    /// so a switch that reconnects with a rewritten config reports the new
    /// region. Without this opt-in the fake has no region hook and existing
    /// metadata assertions stay as they are.
    pub fn with_region_switch(mut self, regions: &[&str]) -> Self {
        let metadata = Arc::make_mut(&mut self.metadata);
        metadata.capabilities |= DriverCapabilities::REGION_SWITCH;
        *rwlock_write(&self.state.available_regions) =
            regions.iter().map(|region| region.to_string()).collect();
        self.state
            .region_switch_enabled
            .store(true, Ordering::Relaxed);
        self
    }

    /// Replaces the region-list answer: `Some` fails the next
    /// `available_regions` calls with that message, `None` restores the
    /// fixed list.
    pub fn set_region_list_error(&self, message: Option<String>) {
        *rwlock_write(&self.state.region_list_error) = message;
    }

    /// Replaces the connect outcome for later connects, the way a region
    /// switch fails when the reconnect cannot be established.
    pub fn set_connect_error(&self, message: Option<String>) {
        *rwlock_write(&self.state.connect_error) = message;
    }

    /// How many times a connection answered `available_regions`.
    pub fn available_regions_calls(&self) -> usize {
        self.state.available_regions_calls.load(Ordering::Relaxed)
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

    /// Fails `put_value` for the secret `name`, the way a denied IAM
    /// permission fails a write.
    pub fn with_secret_write_error(
        self,
        name: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        rwlock_write(&self.state.secret_write_errors).push((name.into(), message.into()));
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

    fn with_region(&self, config: &DbConfig, region: &str) -> Option<DbConfig> {
        if !self.state.region_switch_enabled.load(Ordering::Relaxed) {
            return None;
        }

        let DbConfig::AwsSecrets {
            profile,
            access_key_id,
            endpoint,
            ..
        } = config
        else {
            return None;
        };

        Some(DbConfig::AwsSecrets {
            region: region.to_string(),
            profile: profile.clone(),
            access_key_id: access_key_id.clone(),
            endpoint: endpoint.clone(),
        })
    }
}

struct FakeConnection {
    kind: DbKind,
    state: Arc<FakeDriverState>,
    metadata: Arc<DriverMetadata>,
    active_database: RwLock<Option<String>>,
    current_region: Option<String>,
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

        let current_region = match &profile.config {
            DbConfig::AwsSecrets { region, .. } => Some(region.clone()),
            _ => None,
        };

        Self {
            kind,
            state,
            metadata,
            active_database: RwLock::new(active_database_from_profile(profile)),
            current_region,
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

    fn current_region(&self) -> Option<String> {
        self.current_region.clone()
    }

    fn available_regions(&self) -> Result<Vec<String>, DbError> {
        if !self.state.region_switch_enabled.load(Ordering::Relaxed) {
            return Err(DbError::NotSupported(
                "region listing not supported".to_string(),
            ));
        }

        self.state
            .available_regions_calls
            .fetch_add(1, Ordering::Relaxed);

        if let Some(message) = rwlock_read(&self.state.region_list_error).clone() {
            return Err(DbError::PermissionDenied(message.into()));
        }

        Ok(rwlock_read(&self.state.available_regions).clone())
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
    use dbflux_core::secrecy::ExposeSecret;

    use super::*;

    fn connected() -> (FakeDriver, Box<dyn Connection>) {
        let driver = FakeDriver::new(DbKind::AwsSecrets);
        let profile = ConnectionProfile::new_with_driver(
            "fake-aws-secrets",
            DbKind::AwsSecrets,
            "builtin:fake-aws-secrets",
            DbConfig::default_aws_secrets(),
        );
        let connection = driver.connect(&profile).expect("the fake driver connects");
        (driver, connection)
    }

    fn exposed_text(value: &SecretValue) -> String {
        match &value.payload {
            SecretPayload::Text(text) => text.expose_secret().to_string(),
            SecretPayload::Binary(_) => panic!("expected a text payload"),
        }
    }

    fn exposed_bytes(value: &SecretValue) -> Vec<u8> {
        match &value.payload {
            SecretPayload::Binary(bytes) => bytes.expose_secret().to_vec(),
            SecretPayload::Text(_) => panic!("expected a binary payload"),
        }
    }

    /// A write becomes the current version and is recorded as the `secrecy`
    /// type it traveled in.
    #[test]
    fn a_secret_write_is_recorded_and_becomes_the_current_value() {
        let (driver, connection) = connected();
        let secrets = connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("the fake secrets store exists");

        let response = secrets
            .put_value(&SecretPutRequest::text(
                "prod/notify/webhook-token",
                "new webhook token",
            ))
            .expect("the fake secrets store advertises WRITE");

        assert_eq!(response.version.as_deref(), Some("v-written-1"));

        let calls = driver.secret_store_calls();
        assert_eq!(calls.put_requests.len(), 1);
        assert_eq!(calls.put_requests[0].0, FAKE_SECRETS_STORE_ID);
        assert_eq!(calls.put_requests[0].1.name, "prod/notify/webhook-token");
        assert_eq!(calls.put_requests[0].1.stage, None);
        assert!(!format!("{calls:?}").contains("new webhook token"));

        let current = secrets
            .get_value("prod/notify/webhook-token", None)
            .expect("the written value is readable");
        assert_eq!(exposed_text(&current), "new webhook token");
        assert_eq!(current.version.as_deref(), Some("v-written-1"));
    }

    #[test]
    fn a_staged_write_is_recorded_without_moving_the_current_value() {
        let (driver, connection) = connected();
        let secrets = connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("the fake secrets store exists");

        let response = secrets
            .put_value(
                &SecretPutRequest::text("prod/notify/webhook-token", "pending value")
                    .with_stage(SecretVersionStage::Pending),
            )
            .expect("the fake secrets store advertises WRITE");

        assert_eq!(response.version.as_deref(), Some("v-written-1"));
        assert_eq!(driver.secret_store_calls().put_requests.len(), 1);

        let current = secrets
            .get_value("prod/notify/webhook-token", None)
            .expect("the fake keeps the current value readable");
        assert_eq!(exposed_text(&current), FAKE_SECRET_PLAINTEXT);
    }

    #[test]
    fn a_managed_secret_refuses_the_write_and_keeps_its_value() {
        let (driver, connection) = connected();
        let secrets = connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("the fake secrets store exists");

        let error = secrets
            .put_value(&SecretPutRequest::text("prod/db/master", "replaced"))
            .expect_err("a managed secret is read only");
        let rendered = error.to_string();
        assert!(rendered.contains("rds"), "{rendered}");
        assert!(rendered.contains("prod/db/master"), "{rendered}");
        assert!(!rendered.contains("replaced"), "{rendered}");
        assert!(
            driver.secret_store_calls().put_requests.is_empty(),
            "the driver sends no put at all: the refusal precedes the write"
        );

        let current = secrets
            .get_value("prod/db/master", None)
            .expect("the value survived the refused write");
        assert_ne!(exposed_text(&current), "replaced");
    }

    #[test]
    fn a_scheduled_deletion_refuses_the_write() {
        let (_, connection) = connected();
        let secrets = connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("the fake secrets store exists");

        let error = secrets
            .put_value(&SecretPutRequest::text(
                "prod/deploy/legacy-token",
                "replaced",
            ))
            .expect_err("a deleted secret accepts no writes");

        let rendered = error.to_string();
        assert!(rendered.contains("deletion"), "{rendered}");
        assert!(rendered.contains("Secrets Manager"), "{rendered}");
        assert!(!rendered.contains("replaced"), "{rendered}");
    }

    #[test]
    fn a_write_to_a_missing_secret_is_not_found() {
        let (_, connection) = connected();
        let secrets = connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("the fake secrets store exists");

        let error = secrets
            .put_value(&SecretPutRequest::text("prod/missing", "value"))
            .expect_err("a missing secret accepts no writes");

        assert!(matches!(error, DbError::ObjectNotFound(_)), "{error}");
    }

    #[test]
    fn a_write_can_be_told_to_fail() {
        let driver = FakeDriver::new(DbKind::AwsSecrets)
            .with_secret_write_error("prod/notify/webhook-token", "put was denied by the fixture");
        let profile = ConnectionProfile::new_with_driver(
            "fake-aws-secrets",
            DbKind::AwsSecrets,
            "builtin:fake-aws-secrets",
            DbConfig::default_aws_secrets(),
        );
        let connection = driver.connect(&profile).expect("the fake driver connects");
        let secrets = connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("the fake secrets store exists");

        let error = secrets
            .put_value(&SecretPutRequest::text(
                "prod/notify/webhook-token",
                "replaced",
            ))
            .expect_err("the fixture refuses the write");

        assert!(
            matches!(error, DbError::PermissionDenied(ref formatted) if formatted.message.contains("denied by the fixture")),
            "{error}"
        );

        let current = secrets
            .get_value("prod/notify/webhook-token", None)
            .expect("the failed write did not change the value");
        assert_eq!(exposed_text(&current), FAKE_SECRET_PLAINTEXT);
    }

    #[test]
    fn a_binary_write_reaches_the_fake_store_as_bytes() {
        let (_, connection) = connected();
        let secrets = connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("the fake secrets store exists");

        let response = secrets
            .put_value(&SecretPutRequest::binary(
                "prod/tls/signing-key",
                vec![1, 2, 3],
            ))
            .expect("the fake secrets store advertises WRITE");

        assert_eq!(response.version.as_deref(), Some("v-written-1"));

        let current = secrets
            .get_value("prod/tls/signing-key", None)
            .expect("the written bytes are readable");
        assert_eq!(exposed_bytes(&current), vec![1, 2, 3]);
    }

    #[test]
    fn a_parameter_write_overwrites_and_bumps_the_version() {
        let (driver, connection) = connected();
        let parameters = connection
            .secret_store(FAKE_PARAMETER_STORE_ID)
            .expect("the fake parameter store exists");

        let response = parameters
            .put_value(&SecretPutRequest::text(
                "/app/prod/db/host",
                "db.new.internal",
            ))
            .expect("the fake parameter store advertises WRITE");

        assert_eq!(response.version.as_deref(), Some("4"));
        assert!(driver.secret_store_calls().put_requests.len() == 1);

        let current = parameters
            .get_value("/app/prod/db/host", None)
            .expect("the overwritten value is readable");
        assert_eq!(exposed_text(&current), "db.new.internal");
        assert_eq!(current.version.as_deref(), Some("4"));
    }

    #[test]
    fn a_parameter_store_refuses_binary_and_pending_writes() {
        let (_, connection) = connected();
        let parameters = connection
            .secret_store(FAKE_PARAMETER_STORE_ID)
            .expect("the fake parameter store exists");

        let binary = SecretPutRequest::binary("/app/prod/db/host", vec![0]);
        let Err(DbError::NotSupported(refusal)) = parameters.put_value(&binary) else {
            panic!("a binary value has no parameter-store shape");
        };
        assert!(refusal.contains("Parameter Store"), "{refusal}");
        assert!(refusal.contains("/app/prod/db/host"), "{refusal}");

        let staged = SecretPutRequest::text("/app/prod/db/host", "db")
            .with_stage(SecretVersionStage::Pending);
        let Err(DbError::NotSupported(refusal)) = parameters.put_value(&staged) else {
            panic!("a pending write has no parameter-store shape");
        };
        assert!(refusal.contains("Parameter Store"), "{refusal}");
        assert!(refusal.contains("/app/prod/db/host"), "{refusal}");
    }

    /// A pending write keeps its payload under its own version id: the
    /// version answers by id, the current value is untouched, and the
    /// summary still names the current version.
    #[test]
    fn a_pending_written_version_answers_by_its_version_id() {
        let (_, connection) = connected();
        let secrets = connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("the fake secrets store exists");

        let response = secrets
            .put_value(
                &SecretPutRequest::text("prod/notify/webhook-token", "pending value")
                    .with_stage(SecretVersionStage::Pending),
            )
            .expect("the fake secrets store advertises WRITE");
        let pending = response.version.expect("a put returns the version id");

        let staged = secrets
            .get_value("prod/notify/webhook-token", Some(&pending))
            .expect("the pending version is readable by its id");
        assert_eq!(exposed_text(&staged), "pending value");
        assert_eq!(staged.version.as_deref(), Some(pending.as_str()));

        let current = secrets
            .get_value("prod/notify/webhook-token", None)
            .expect("the current value is readable");
        assert_eq!(exposed_text(&current), FAKE_SECRET_PLAINTEXT);
    }

    /// Every written version keeps its payload: after two `Current` writes
    /// the first written version still answers by its id with its own
    /// value, and the current one with the latest.
    #[test]
    fn every_written_version_keeps_its_payload() {
        let (_, connection) = connected();
        let secrets = connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("the fake secrets store exists");

        let first = secrets
            .put_value(&SecretPutRequest::text(
                "prod/notify/webhook-token",
                "first write",
            ))
            .expect("the first write lands")
            .version
            .expect("a version id");
        let second = secrets
            .put_value(&SecretPutRequest::text(
                "prod/notify/webhook-token",
                "second write",
            ))
            .expect("the second write lands")
            .version
            .expect("a version id");

        let earlier = secrets
            .get_value("prod/notify/webhook-token", Some(&first))
            .expect("the earlier written version is readable");
        assert_eq!(exposed_text(&earlier), "first write");
        assert_eq!(earlier.version.as_deref(), Some(first.as_str()));

        let current = secrets
            .get_value("prod/notify/webhook-token", None)
            .expect("the current value is readable");
        assert_eq!(exposed_text(&current), "second write");
        assert_eq!(current.version.as_deref(), Some(second.as_str()));
    }

    /// A version the fake never stored is not found: `get_value` answers
    /// with the version asked for or reports it missing, never with
    /// whatever was written last.
    #[test]
    fn an_unknown_version_is_not_found() {
        let (_, connection) = connected();
        let secrets = connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("the fake secrets store exists");

        let error = secrets
            .get_value("prod/notify/webhook-token", Some("v-written-999"))
            .expect_err("an unknown version is not found");
        assert!(matches!(error, DbError::ObjectNotFound(_)), "{error}");
    }

    /// An overwrite does not clear a decrypt denial: read authorization is
    /// independent of the stored value, so the write succeeds and the read
    /// still fails the way it did before.
    #[test]
    fn an_overwrite_keeps_an_existing_decrypt_denial() {
        let (_, connection) = connected();
        let parameters = connection
            .secret_store(FAKE_PARAMETER_STORE_ID)
            .expect("the fake parameter store exists");

        let response = parameters
            .put_value(&SecretPutRequest::text("/app/prod/db/password", "rotated"))
            .expect("the overwrite lands");
        assert_eq!(response.version.as_deref(), Some("4"));

        let error = parameters
            .get_value("/app/prod/db/password", None)
            .expect_err("the denial survives the overwrite");
        match &error {
            DbError::PermissionDenied(formatted) => {
                assert_eq!(formatted.message, FAKE_KMS_DENIED_MESSAGE);
            }
            other => panic!("expected the decrypt denial, got {other:?}"),
        }
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
    /// Every `put_value` attempt, as `(store_id, request)`. The payload
    /// travels as the `secrecy` type `SecretPayload`, so it stays redacted in
    /// `Debug` output.
    pub put_requests: Vec<(String, SecretPutRequest)>,
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

#[derive(Clone)]
enum FakeSecretPayload {
    Text(String),
    Binary(Vec<u8>),
}

impl FakeSecretPayload {
    /// The stored form of a payload the seam delivered. Values live only in
    /// the fake's in-memory fixtures, never in a log.
    fn for_write(payload: &SecretPayload) -> Self {
        match payload {
            SecretPayload::Text(text) => Self::Text(text.expose_secret().to_owned()),
            SecretPayload::Binary(bytes) => Self::Binary(bytes.expose_secret().to_vec()),
        }
    }
}

/// Why a read of a secret fails, as its fixture models the service state or
/// a policy: independently of the value a write stores. A decrypt denial is
/// about the caller's authorization, so an overwrite does not clear it.
#[derive(Clone)]
enum FakeSecretReadFailure {
    Denied(&'static str),
    Unavailable(&'static str),
}

#[derive(Clone)]
struct FakeSecret {
    details: SecretDetails,
    /// The payload each version carries: the fixture seeds one entry keyed
    /// by the summary's version, and every accepted write adds its own, so
    /// a written version answers by its id for `Pending` and `Current`
    /// alike.
    versions: Vec<(String, FakeSecretPayload)>,
    /// Why a read of this secret fails, decided at fixture time and never
    /// changed by a write: read authorization is independent of the stored
    /// value.
    read_failure: Option<FakeSecretReadFailure>,
}

/// A deterministic in-memory secret store. A flat store matches its name
/// filter as a prefix, a hierarchical one as a substring of the full name.
/// Writes mutate the connection's own copy: a fake connection rebuilt later
/// sees its fixtures again, while reads and writes through one connection
/// agree.
struct FakeSecretStore {
    descriptor: SecretStoreDescriptor,
    secrets: RwLock<Vec<FakeSecret>>,
    default_page_size: usize,
    written_versions: AtomicUsize,
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
    fn listing(&self, request: &SecretListRequest) -> (Vec<FakeSecret>, Vec<String>) {
        let filter = request.name_filter.as_deref();
        let secrets = rwlock_read(&self.secrets);

        if !self.hierarchical() {
            let entries = secrets
                .iter()
                .filter(|secret| self.matches_filter(&secret.details.summary.name, filter))
                .cloned()
                .collect();
            return (entries, Vec::new());
        }

        let path = request.path.clone().unwrap_or_else(|| "/".to_string());
        let mut entries = Vec::new();
        let mut child_paths = Vec::new();

        for secret in secrets.iter() {
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
                        entries.push(secret.clone());
                    }
                }
            }
        }

        (entries, child_paths)
    }

    /// The stored copy of one secret, cloned so no read lock escapes this
    /// method.
    fn find(&self, name: &str) -> Result<FakeSecret, DbError> {
        rwlock_read(&self.secrets)
            .iter()
            .find(|secret| secret.details.summary.name == name)
            .cloned()
            .ok_or_else(|| DbError::object_not_found(format!("Secret {name} not found")))
    }

    /// Deterministic version id of the next flat-store write.
    fn next_written_version(&self) -> String {
        let count = self
            .written_versions
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        format!("v-written-{count}")
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

        self.find(name).map(|secret| secret.details)
    }

    fn get_value(&self, name: &str, version: Option<&str>) -> Result<SecretValue, DbError> {
        mutex_lock(&self.state.secret_store_calls)
            .value_requests
            .push((self.descriptor.id.clone(), name.to_string()));

        let secret = self.find(name)?;

        if let Some(failure) = &secret.read_failure {
            return match failure {
                FakeSecretReadFailure::Denied(message) => Err(DbError::permission_denied(*message)),
                FakeSecretReadFailure::Unavailable(message) => {
                    Err(DbError::query_failed(*message))
                }
            };
        }

        // The version asked for, or the current one when none was named.
        let requested = version
            .map(ToString::to_string)
            .or_else(|| secret.details.summary.version.clone());

        secret
            .versions
            .iter()
            .find(|(id, _)| Some(id) == requested.as_ref())
            .map(|(id, payload)| SecretValue {
                version: Some(id.clone()),
                payload: payload_value(id, payload),
            })
            .ok_or_else(|| {
                DbError::object_not_found(format!(
                    "Secret {name} has no version {}",
                    requested.as_deref().unwrap_or("<none>")
                ))
            })
    }

    /// One overwrite, following the rules the real stores keep: secrets
    /// another service owns are refused before the write (that is a
    /// DBFlux-side refusal the AWS driver makes too), and everything else is
    /// handed to the modeled service. The service answers a put on a secret
    /// scheduled for deletion with the rejection this fake returns, the way
    /// `Unavailable` answers a read of such a secret; a `Current` write
    /// becomes the current version, and a `Pending` write moves no current
    /// value (the fake keeps one version per secret). The write-failure knob
    /// makes the delivery to the store fail without touching the value.
    ///
    /// Only a write the modeled service accepted is recorded: a refused put
    /// never reaches the store, so `put_requests` lists delivered writes.
    fn put_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        self.descriptor.ensure_can_write(&request.name)?;

        let write_error = rwlock_read(&self.state.secret_write_errors)
            .iter()
            .find(|(name, _)| name == &request.name)
            .map(|(_, message)| message.clone());
        if let Some(message) = write_error {
            return Err(DbError::permission_denied(message));
        }

        let response = if self.hierarchical() {
            self.put_parameter_value(request)?
        } else {
            self.put_secret_value(request)?
        };

        mutex_lock(&self.state.secret_store_calls)
            .put_requests
            .push((self.descriptor.id.clone(), request.clone()));

        Ok(response)
    }
}

impl FakeSecretStore {
    /// The flat store's overwrite: a new version replaces the current value.
    fn put_secret_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        let mut secrets = rwlock_write(&self.secrets);
        let secret = secrets
            .iter_mut()
            .find(|secret| secret.details.summary.name == request.name)
            .ok_or_else(|| {
                DbError::object_not_found(format!("Secret {} not found", request.name))
            })?;

        if let Some(service) = &secret.details.summary.owning_service {
            return Err(DbError::permission_denied(format!(
                "Secrets Manager secret \"{}\" is managed by {service} and is read only",
                request.name
            )));
        }
        if secret.details.summary.deletion_scheduled_for.is_some() {
            // The service itself rejects a put on a secret pending deletion,
            // so this answer models the service state, not a local policy —
            // the same answer `Unavailable` gives a read of such a secret.
            return Err(DbError::query_failed(
                "Secrets Manager secret is scheduled for deletion; its value cannot be written",
            ));
        }

        let version = self.next_written_version();
        // A write keeps its payload under its own version id, whatever the
        // stage: a `Current` write moves the summary's current version to it,
        // a `Pending` write moves nothing. Earlier versions stay readable.
        secret
            .versions
            .push((version.clone(), FakeSecretPayload::for_write(&request.payload)));
        if request.stage != Some(SecretVersionStage::Pending) {
            secret.details.summary.version = Some(version.clone());
        }

        Ok(SecretValue {
            version: Some(version),
            payload: request.payload.clone(),
        })
    }

    /// The hierarchical store's overwrite, shaped like SSM `PutParameter`:
    /// no staging labels and text values only, and the numeric version moves.
    fn put_parameter_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        if let Some(refusal) = parameter_write_refusal(request) {
            return Err(refusal);
        }

        let mut secrets = rwlock_write(&self.secrets);
        let secret = secrets
            .iter_mut()
            .find(|secret| secret.details.summary.name == request.name)
            .ok_or_else(|| {
                DbError::object_not_found(format!("Parameter {} not found", request.name))
            })?;

        let current = secret
            .details
            .summary
            .version
            .as_deref()
            .and_then(|version| version.parse::<u64>().ok())
            .ok_or_else(|| {
                DbError::query_failed(format!(
                    "Parameter {} has no version to overwrite",
                    request.name
                ))
            })?;
        let version = (current.wrapping_add(1)).to_string();

        // The stored value changes; the read authorization does not. A
        // denial that was there before the write is there after it.
        secret.details.summary.version = Some(version.clone());
        secret
            .versions
            .push((version.clone(), FakeSecretPayload::for_write(&request.payload)));

        Ok(SecretValue {
            version: Some(version),
            payload: request.payload.clone(),
        })
    }
}

/// The `SecretValue` payload form of a stored version.
fn payload_value(version: &str, payload: &FakeSecretPayload) -> SecretPayload {
    match payload {
        FakeSecretPayload::Text(text) => SecretPayload::Text(SecretString::from(text.clone())),
        FakeSecretPayload::Binary(bytes) => SecretPayload::Binary(SecretSlice::from(bytes.clone())),
    }
}

/// Why this fake parameter store cannot take a write: no staging labels and
/// text values only, the refusals the AWS driver makes before its call,
/// worded like the driver's and naming the store and the parameter.
fn parameter_write_refusal(request: &SecretPutRequest) -> Option<DbError> {
    let name = &request.name;

    if request.stage == Some(SecretVersionStage::Pending) {
        return Some(DbError::NotSupported(format!(
            "Parameter Store has no staging labels; the write to \"{name}\" cannot be staged."
        )));
    }

    if matches!(request.payload, SecretPayload::Binary(_)) {
        return Some(DbError::NotSupported(format!(
            "Parameter Store stores values as text only; the write to \"{name}\" was refused."
        )));
    }

    None
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
    let mut summary = summary;
    let version = summary
        .version
        .clone()
        .unwrap_or_else(|| "v-current".to_string());
    // Every stored secret has a current version, so the summary and the
    // version store agree even when the fixture named no version.
    if summary.version.is_none() {
        summary.version = Some(version.clone());
    }

    let base = SecretDetails {
        resource_id: Some(format!("arn:fake:secret:{}", summary.name)),
        created: fake_time(1, 10, 8),
        summary,
        ..SecretDetails::default()
    };

    FakeSecret {
        details: details(base),
        versions: vec![(version, payload)],
        read_failure: None,
    }
}

/// Marks a secret whose reads fail the way its fixture models the service
/// state or a policy — independently of any value a write later stores.
fn failing_reads(secret: FakeSecret, failure: FakeSecretReadFailure) -> FakeSecret {
    FakeSecret {
        read_failure: Some(failure),
        ..secret
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
                    | SecretStoreCapabilities::SOFT_DELETE
                    | SecretStoreCapabilities::WRITE,
            ),
            secrets: RwLock::new(fake_flat_secrets()),
            default_page_size: 4,
            written_versions: AtomicUsize::new(0),
            state: state.clone(),
        },
        FakeSecretStore {
            descriptor: SecretStoreDescriptor::new(
                FAKE_PARAMETER_STORE_ID,
                "Parameter Store",
                SecretStoreCapabilities::VERSIONS
                    | SecretStoreCapabilities::HIERARCHICAL
                    | SecretStoreCapabilities::TYPED_VALUES
                    | SecretStoreCapabilities::WRITE,
            ),
            secrets: RwLock::new(fake_parameters()),
            default_page_size: 2,
            written_versions: AtomicUsize::new(0),
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
        failing_reads(
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
                FakeSecretPayload::Text("legacy-token-value".to_string()),
            ),
            FakeSecretReadFailure::Unavailable("The secret is scheduled for deletion"),
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
        failing_reads(
            fake_parameter(
                "/app/prod/db/password",
                "SecureString",
                "Standard",
                FakeSecretPayload::Text("unreadable-vault-value".to_string()),
            ),
            FakeSecretReadFailure::Denied(FAKE_KMS_DENIED_MESSAGE),
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
// ---------------------------------------------------------------------------
