//! Test-only fixtures for the shared object tree: a controllable fake
//! connection, a per-database bound connection + driver for slot-install
//! scenarios, and helpers to build a real `AppStateEntity` under the GPUI
//! test executor. Never compiled outside `cfg(test)`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use dbflux_core::{
    Connection, DatabaseCategory, DatabaseInfo, DbConfig, DbError, DbKind, DriverCapabilities,
    DriverFormDef, DriverMetadata, FormValues, Icon, QueryHandle, QueryRequest, QueryResult,
    RelationalSchema, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TableInfo, TransferFamily,
    WritePrivilege,
};
use gpui::{AppContext, Entity, TestAppContext};
use uuid::Uuid;

use crate::app_state_entity::AppStateEntity;

pub static TEST_FORM: std::sync::LazyLock<DriverFormDef> =
    std::sync::LazyLock::new(|| DriverFormDef { tabs: vec![] });

pub fn driver_metadata(id: &str) -> DriverMetadata {
    DriverMetadata {
        id: id.to_string(),
        display_name: "TestTree".to_string(),
        description: "test".to_string(),
        category: DatabaseCategory::Relational,
        transfer_family: TransferFamily::Sql,
        deployment_class: None,
        query_language: dbflux_core::QueryLanguage::Sql,
        capabilities: DriverCapabilities::empty(),
        default_port: Some(5432),
        uri_scheme: "test".to_string(),
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
    }
}

pub fn table(schema: Option<&str>, name: &str) -> TableInfo {
    TableInfo {
        name: name.to_string(),
        schema: schema.map(str::to_string),
        columns: None,
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
        presentation: dbflux_core::CollectionPresentation::default(),
        child_items: None,
        storage_hints: None,
    }
}

pub fn loaded_details(schema: Option<&str>, name: &str) -> TableInfo {
    TableInfo {
        columns: Some(Vec::new()),
        ..table(schema, name)
    }
}

pub fn db_schema(name: &str, tables: Vec<TableInfo>) -> dbflux_core::DbSchemaInfo {
    dbflux_core::DbSchemaInfo {
        name: name.to_string(),
        tables,
        views: Vec::new(),
        custom_types: None,
    }
}

pub fn relational_schema(
    databases: Vec<DatabaseInfo>,
    current: Option<&str>,
    schemas: Vec<dbflux_core::DbSchemaInfo>,
    tables: Vec<TableInfo>,
) -> SchemaSnapshot {
    SchemaSnapshot::relational(RelationalSchema {
        databases,
        current_database: current.map(str::to_string),
        schemas,
        tables,
        views: Vec::new(),
    })
}

/// A controllable fake connection backing the coordinator tests. All driver
// work is served from injected maps; call counters let tests assert
/// deduplicated execution.
pub struct FakeTreeConnection {
    pub metadata: DriverMetadata,
    pub kind: DbKind,
    pub strategy: SchemaLoadingStrategy,
    /// Result of `list_databases`.
    pub databases: Mutex<Vec<DatabaseInfo>>,
    /// Result of `schema_for_database`, keyed by database name.
    pub schemas: Mutex<HashMap<String, dbflux_core::DbSchemaInfo>>,
    /// Databases whose next `schema_for_database` call fails once.
    pub schema_failures: Mutex<HashMap<String, usize>>,
    /// Result of `table_details`, keyed by `(database, schema, table)`.
    pub details: Mutex<HashMap<(String, Option<String>, String), TableInfo>>,
    pub list_calls: AtomicUsize,
    pub schema_calls: Mutex<Vec<String>>,
    pub details_calls: AtomicUsize,
}

impl FakeTreeConnection {
    pub fn new(strategy: SchemaLoadingStrategy) -> Arc<Self> {
        Arc::new(Self {
            metadata: driver_metadata("test-tree-conn"),
            kind: DbKind::Postgres,
            strategy,
            databases: Mutex::new(Vec::new()),
            schemas: Mutex::new(HashMap::new()),
            schema_failures: Mutex::new(HashMap::new()),
            details: Mutex::new(HashMap::new()),
            list_calls: AtomicUsize::new(0),
            schema_calls: Mutex::new(Vec::new()),
            details_calls: AtomicUsize::new(0),
        })
    }

    pub fn list_calls(&self) -> usize {
        self.list_calls.load(Ordering::SeqCst)
    }

    pub fn schema_calls(&self) -> Vec<String> {
        self.schema_calls.lock().expect("schema calls").clone()
    }

    pub fn details_calls(&self) -> usize {
        self.details_calls.load(Ordering::SeqCst)
    }
}

impl Connection for FakeTreeConnection {
    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
    }

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
        Err(DbError::NotSupported("test connection".to_string()))
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Ok(SchemaSnapshot::default())
    }

    fn kind(&self) -> DbKind {
        self.kind
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        self.strategy
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.databases.lock().expect("databases").clone())
    }

    fn schema_for_database(&self, database: &str) -> Result<dbflux_core::DbSchemaInfo, DbError> {
        self.schema_calls
            .lock()
            .expect("schema calls")
            .push(database.to_string());

        let mut failures = self.schema_failures.lock().expect("schema failures");
        if let Some(remaining) = failures.get_mut(database)
            && *remaining > 0
        {
            *remaining -= 1;
            return Err(DbError::ConnectionFailed(dbflux_core::FormattedError::new(
                format!("introspection for '{database}' failed"),
            )));
        }
        drop(failures);

        self.schemas
            .lock()
            .expect("schemas")
            .get(database)
            .cloned()
            .ok_or_else(|| {
                DbError::ConnectionFailed(dbflux_core::FormattedError::new(format!(
                    "no fake schema for '{database}'"
                )))
            })
    }

    fn table_details(
        &self,
        database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        self.details_calls.fetch_add(1, Ordering::SeqCst);
        self.details
            .lock()
            .expect("details")
            .get(&(
                database.to_string(),
                schema.map(str::to_string),
                table.to_string(),
            ))
            .cloned()
            .ok_or_else(|| {
                DbError::ConnectionFailed(dbflux_core::FormattedError::new(format!(
                    "no fake details for '{table}'"
                )))
            })
    }
}

/// A per-database connection as real ConnectionPerDatabase drivers produce:
/// bound to one database and always labelled with it.
pub struct BoundTestConnection {
    metadata: DriverMetadata,
    bound_database: String,
}

impl BoundTestConnection {
    pub fn new(bound_database: &str) -> Box<Self> {
        Box::new(Self {
            metadata: driver_metadata("test-tree-conn"),
            bound_database: bound_database.to_string(),
        })
    }
}

impl Connection for BoundTestConnection {
    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
    }

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
        Err(DbError::NotSupported("test connection".to_string()))
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Ok(relational_schema(
            Vec::new(),
            Some(&self.bound_database),
            Vec::new(),
            Vec::new(),
        ))
    }

    fn kind(&self) -> DbKind {
        DbKind::Postgres
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::ConnectionPerDatabase
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }

    fn schema_for_database(&self, database: &str) -> Result<dbflux_core::DbSchemaInfo, DbError> {
        // Mirrors real per-database drivers: content always comes from the
        // bound database, whatever name was asked for.
        Ok(db_schema(
            database,
            vec![table(Some(&self.bound_database), "slot_table")],
        ))
    }

    fn table_details(
        &self,
        _database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        Ok(loaded_details(schema, table))
    }
}

/// Test driver producing [`BoundTestConnection`]s for the guarded
/// per-database install path.
pub struct InstallTestDriver {
    metadata: DriverMetadata,
    pub connect_calls: AtomicUsize,
    fail_connect: AtomicBool,
}

impl InstallTestDriver {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            metadata: driver_metadata("test-tree-conn"),
            connect_calls: AtomicUsize::new(0),
            fail_connect: AtomicBool::new(false),
        })
    }

    pub fn connect_calls(&self) -> usize {
        self.connect_calls.load(Ordering::SeqCst)
    }

    pub fn set_fail_connect(&self, fail: bool) {
        self.fail_connect.store(fail, Ordering::SeqCst);
    }
}

impl dbflux_core::DbDriver for InstallTestDriver {
    fn kind(&self) -> DbKind {
        DbKind::Postgres
    }

    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
    }

    fn form_definition(&self) -> &DriverFormDef {
        &TEST_FORM
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        DRIVER_KEY.to_string()
    }

    fn build_config(&self, _values: &FormValues) -> Result<DbConfig, DbError> {
        Ok(DbConfig::default_postgres())
    }

    fn extract_values(&self, _config: &DbConfig) -> FormValues {
        FormValues::new()
    }

    fn with_database(&self, config: &DbConfig, database: &str) -> Option<DbConfig> {
        let mut config = config.clone();
        match &mut config {
            DbConfig::Postgres {
                database: target, ..
            } => {
                *target = database.to_string();
                Some(config)
            }
            _ => None,
        }
    }

    fn connect_with_secrets(
        &self,
        profile: &dbflux_core::ConnectionProfile,
        _password: Option<&dbflux_core::secrecy::SecretString>,
        _ssh_secret: Option<&dbflux_core::secrecy::SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        self.connect_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_connect.load(Ordering::SeqCst) {
            return Err(DbError::ConnectionFailed(dbflux_core::FormattedError::new(
                "test driver refused the connection",
            )));
        }
        let bound = match &profile.config {
            DbConfig::Postgres { database, .. } => database.clone(),
            _ => "unknown".to_string(),
        };
        Ok(BoundTestConnection::new(&bound))
    }

    fn test_connection(&self, _profile: &dbflux_core::ConnectionProfile) -> Result<(), DbError> {
        Ok(())
    }
}

pub const DRIVER_KEY: &str = "builtin:test-tree-driver";

/// Builds a real `AppStateEntity` over in-memory storage.
pub fn test_app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
    cx.update(|cx| {
        cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("test storage runtime"),
            )
            .expect("test app state")
        })
    })
}

/// Connects `profile_id`'s profile through the same seam production uses,
/// returning the profile id.
pub fn connect_profile(
    state: &Entity<AppStateEntity>,
    cx: &mut TestAppContext,
    profile_id: Uuid,
    connection: Arc<dyn Connection>,
    schema: Option<SchemaSnapshot>,
) {
    let mut profile =
        dbflux_core::ConnectionProfile::new("test-profile", DbConfig::default_postgres());
    profile.id = profile_id;
    profile.set_driver_id(DRIVER_KEY);
    state.update(cx, |state, _| {
        state.apply_connect_profile(
            profile,
            connection,
            schema,
            None,
            false,
            WritePrivilege::Unknown,
        );
    });
}
