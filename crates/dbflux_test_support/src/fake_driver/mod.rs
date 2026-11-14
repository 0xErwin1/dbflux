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
    MAX_TAGS, SecretCreateChoices, SecretCreateRequest, SecretCreated, SecretDeleteRequest,
    SecretDeletion, SecretDescriptionChange, SecretDescriptionUpdate, SecretDetails,
    SecretListPage, SecretListRequest, SecretPayload, SecretPutRequest, SecretRollback,
    SecretRollbackRequest, SecretRotation, SecretRotationChange, SecretRotationStart,
    SecretRotationUpdate, SecretStoreCapabilities, SecretStoreDescriptor, SecretStoreSource,
    SecretSummary, SecretTag, SecretTagsUpdate, SecretValue, SecretVersionInfo, SecretVersionStage,
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

mod connection;
mod metadata;
mod secret_store;

use connection::FakeConnection;
use metadata::*;
use secret_store::{FakeSecretStore, fake_secret_stores};

pub use secret_store::{
    FAKE_KMS_DENIED_MESSAGE, FAKE_PARAMETER_STORE_ID, FAKE_ROTATION_FUNCTION, FAKE_SECRET_BINARY,
    FAKE_SECRET_PASSWORD, FAKE_SECRET_PLAINTEXT, FAKE_SECRETS_STORE_ID, FakeSecretMutation,
    FakeSecretStoreCalls,
};

#[cfg(test)]
mod tests;

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
    secret_version_errors: RwLock<Vec<(String, String)>>,
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
    /// that the flag reached the connection. It clears an earlier
    /// `with_read_only_refusal`, so the last call of the two wins.
    pub fn with_read_only_enforcement(mut self) -> Self {
        self.set_read_only_capability(true);
        self.state.refuse_read_only.store(false, Ordering::Relaxed);
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

    /// Fails `list_versions` for the secret `name`, the way a missing IAM
    /// permission fails a history read. The secret's value reads are
    /// unaffected.
    pub fn with_secret_versions_error(
        self,
        name: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        rwlock_write(&self.state.secret_version_errors).push((name.into(), message.into()));
        self
    }

    /// [`Self::with_secret_versions_error`] for a driver that is already
    /// connected.
    pub fn set_secret_versions_error(&self, name: impl Into<String>, message: impl Into<String>) {
        rwlock_write(&self.state.secret_version_errors).push((name.into(), message.into()));
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
