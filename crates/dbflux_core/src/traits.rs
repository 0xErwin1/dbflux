use bitflags::bitflags;

use crate::{
    CodeGenCapabilities, CodeGenerator, CollectionBrowseRequest, CollectionCountRequest,
    ConnectionProfile, CrudResult, CustomTypeInfo, DatabaseInfo, DbError, DbKind, DbSchemaInfo,
    DescribeRequest, DocumentDelete, DocumentInsert, DocumentUpdate, DriverCapabilities,
    DriverFormDef, DriverMetadata, ExplainRequest, FormValues, LanguageService, NoOpCodeGenerator,
    QueryHandle, QueryRequest, QueryResult, RowDelete, RowInsert, RowPatch, SchemaForeignKeyInfo,
    SchemaIndexInfo, SchemaSnapshot, SqlDialect, SqlGenerationRequest, SqlLanguageService,
    TableBrowseRequest, TableCountRequest, TableInfo, ViewInfo,
    key_value::{
        HashDeleteRequest, HashSetRequest, KeyBulkGetRequest, KeyDeleteRequest, KeyExistsRequest,
        KeyExpireRequest, KeyGetRequest, KeyGetResult, KeyPersistRequest, KeyRenameRequest,
        KeyScanPage, KeyScanRequest, KeySetRequest, KeyTtlRequest, KeyType, KeyTypeRequest,
        ListPushRequest, ListRemoveRequest, ListSetRequest, SetAddRequest, SetRemoveRequest,
        StreamAddRequest, StreamDeleteRequest, ZSetAddRequest, ZSetRemoveRequest,
    },
};

bitflags! {
    /// Schema features supported by a database driver.
    ///
    /// The UI uses this to determine which schema objects to display.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SchemaFeatures: u32 {
        const FOREIGN_KEYS = 1 << 0;
        const CHECK_CONSTRAINTS = 1 << 1;
        const UNIQUE_CONSTRAINTS = 1 << 2;
        const CUSTOM_TYPES = 1 << 3;
        const TRIGGERS = 1 << 4;
        const SEQUENCES = 1 << 5;
        const FUNCTIONS = 1 << 6;
    }
}

/// Describes how a database driver handles schema loading for multiple databases.
///
/// Different database systems have fundamentally different approaches:
/// - MySQL/MariaDB: Single connection can switch between databases with `USE`
/// - PostgreSQL: Each database requires a separate connection
/// - SQLite: Single database per file, no database switching
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaLoadingStrategy {
    /// Schema is loaded lazily per database on the same connection.
    /// Clicking a database loads its schema without reconnecting.
    /// Supports "closing" a database (unloading schema) without disconnecting.
    /// Used by: MySQL, MariaDB
    LazyPerDatabase,

    /// Each database requires a separate connection.
    /// Clicking a different database prompts to create a new connection.
    /// Used by: PostgreSQL
    ConnectionPerDatabase,

    /// Single database, no switching needed.
    /// Schema is loaded once at connection time.
    /// Used by: SQLite
    SingleDatabase,
}

/// Scope where a code generator can be applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeGenScope {
    Table,
    View,
    TableOrView,
    // Future: Schema, Database, Column
}

/// Metadata for a code generator available on a connection.
///
/// Drivers expose their available generators as a static slice, allowing
/// the UI to build context menus dynamically based on the selected item type.
#[derive(Debug, Clone, Copy)]
pub struct CodeGeneratorInfo {
    /// Unique identifier (e.g., "select_star", "create_table").
    pub id: &'static str,

    /// Human-readable label for the UI (e.g., "SELECT *", "CREATE TABLE").
    pub label: &'static str,

    /// Where this generator can be applied.
    pub scope: CodeGenScope,

    /// Display order in the menu (lower values appear first).
    pub order: u32,

    /// Whether this generator produces destructive SQL (e.g., DROP, TRUNCATE).
    pub destructive: bool,
}
use std::sync::Arc;

/// Handle for cancelling a running query.
///
/// Each database driver implements this trait to provide database-specific
/// cancellation logic. The handle is returned when starting a query and can
/// be used to cancel it from another thread.
pub trait QueryCancelHandle: Send + Sync {
    /// Attempt to cancel the query.
    ///
    /// This is a best-effort operation. The query may have already completed
    /// or the database may not support cancellation.
    ///
    /// Returns `Ok(())` if the cancel request was sent successfully.
    /// The actual query may still complete before the cancel takes effect.
    fn cancel(&self) -> Result<(), DbError>;

    /// Check if cancellation has been requested.
    fn is_cancelled(&self) -> bool;
}

/// A no-op cancel handle for databases that don't support cancellation.
#[derive(Clone)]
pub struct NoopCancelHandle;

impl QueryCancelHandle for NoopCancelHandle {
    fn cancel(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn is_cancelled(&self) -> bool {
        false
    }
}

/// Factory for creating database connections.
///
/// Implementations are registered in `AppState` by `DbKind` at startup.
/// Each database type (PostgreSQL, SQLite, etc.) provides its own driver.
pub trait DbDriver: Send + Sync {
    /// Returns the database kind this driver handles.
    fn kind(&self) -> DbKind;

    /// Returns the driver metadata including category, capabilities, and query language.
    ///
    /// This is the primary way for drivers to declare what they are and what they support.
    /// The UI uses this to adapt its behavior without driver-specific logic.
    fn metadata(&self) -> &'static DriverMetadata;

    /// Human-readable name for UI display (e.g., "PostgreSQL", "SQLite").
    ///
    /// Default implementation uses `metadata().display_name`.
    fn display_name(&self) -> &'static str {
        self.metadata().display_name
    }

    /// Optional description shown in the connection manager.
    ///
    /// Default implementation uses `metadata().description`.
    fn description(&self) -> &'static str {
        self.metadata().description
    }

    /// Returns the capabilities supported by this driver.
    ///
    /// Default implementation uses `metadata().capabilities`.
    fn capabilities(&self) -> DriverCapabilities {
        self.metadata().capabilities
    }

    /// Check if a specific capability is supported.
    fn supports(&self, capability: DriverCapabilities) -> bool {
        self.capabilities().contains(capability)
    }

    /// Returns the form field definitions for the connection manager UI.
    ///
    /// The UI uses this to render connection forms dynamically without
    /// hardcoding driver-specific logic.
    fn form_definition(&self) -> &'static DriverFormDef;

    /// Build a DbConfig from form values collected by the UI.
    ///
    /// The `values` map contains field IDs as keys and user input as values.
    /// Returns `DbError::InvalidProfile` if required fields are missing or invalid.
    fn build_config(&self, values: &FormValues) -> Result<crate::DbConfig, DbError>;

    /// Extract form values from an existing DbConfig for editing.
    ///
    /// Used when loading a saved connection profile into the form.
    fn extract_values(&self, config: &crate::DbConfig) -> FormValues;

    /// Build a connection URI from individual form field values and password.
    /// Returns `None` for drivers without URI support (e.g., SQLite).
    fn build_uri(&self, _values: &FormValues, _password: &str) -> Option<String> {
        None
    }

    /// Parse a connection URI into individual form field values.
    /// Returns `None` for drivers without URI support or if the URI is malformed.
    fn parse_uri(&self, _uri: &str) -> Option<FormValues> {
        None
    }

    /// Whether this database type requires authentication.
    ///
    /// Returns `false` for file-based databases like SQLite.
    /// Default implementation checks for the AUTHENTICATION capability.
    fn requires_password(&self) -> bool {
        self.supports(DriverCapabilities::AUTHENTICATION)
    }

    /// Create a connection without providing a password.
    ///
    /// Delegates to `connect_with_password(profile, None)`.
    fn connect(&self, profile: &ConnectionProfile) -> Result<Box<dyn Connection>, DbError> {
        self.connect_with_password(profile, None)
    }

    /// Create a connection with an optional password.
    ///
    /// The password is provided separately from the profile to support
    /// secure credential storage (keyring) without persisting passwords in config.
    fn connect_with_password(
        &self,
        profile: &ConnectionProfile,
        password: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError> {
        self.connect_with_secrets(profile, password, None)
    }

    /// Create a connection with optional password and SSH secret.
    ///
    /// The SSH secret is the passphrase for the private key or the SSH password,
    /// depending on the authentication method configured in the profile.
    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&str>,
        ssh_secret: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError>;

    /// Test if a connection can be established without keeping it open.
    ///
    /// Used by the "Test Connection" button in the connection manager.
    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError>;
}

/// Key-value operations exposed by drivers in `DatabaseCategory::KeyValue`.
///
/// The UI must rely on this contract plus `DriverCapabilities` rather than
/// driver-specific conditionals.
pub trait KeyValueApi: Send + Sync {
    /// Scan keys with cursor-based pagination.
    fn scan_keys(&self, request: &KeyScanRequest) -> Result<KeyScanPage, DbError>;

    /// Get key value plus metadata.
    fn get_key(&self, request: &KeyGetRequest) -> Result<KeyGetResult, DbError>;

    /// Set key value, optionally with TTL and conditional flags.
    fn set_key(&self, request: &KeySetRequest) -> Result<(), DbError>;

    /// Delete a single key. Returns `true` if a key was deleted.
    fn delete_key(&self, request: &KeyDeleteRequest) -> Result<bool, DbError>;

    /// Check whether a key exists.
    fn exists_key(&self, request: &KeyExistsRequest) -> Result<bool, DbError>;

    /// Get key type if supported by the driver.
    fn key_type(&self, _request: &KeyTypeRequest) -> Result<KeyType, DbError> {
        Err(DbError::NotSupported(
            "Key-value TYPE not supported by this driver".to_string(),
        ))
    }

    /// Get key TTL in seconds.
    ///
    /// `Ok(None)` means key exists and has no expiration.
    fn key_ttl(&self, _request: &KeyTtlRequest) -> Result<Option<i64>, DbError> {
        Err(DbError::NotSupported(
            "Key-value TTL not supported by this driver".to_string(),
        ))
    }

    /// Set or update key expiration. Returns `true` when expiration changed.
    fn expire_key(&self, _request: &KeyExpireRequest) -> Result<bool, DbError> {
        Err(DbError::NotSupported(
            "Key-value EXPIRE not supported by this driver".to_string(),
        ))
    }

    /// Remove key expiration. Returns `true` when expiration was removed.
    fn persist_key(&self, _request: &KeyPersistRequest) -> Result<bool, DbError> {
        Err(DbError::NotSupported(
            "Key-value PERSIST not supported by this driver".to_string(),
        ))
    }

    /// Rename a key.
    fn rename_key(&self, _request: &KeyRenameRequest) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Key-value RENAME not supported by this driver".to_string(),
        ))
    }

    /// Fetch multiple key values preserving request order.
    ///
    /// Missing keys are returned as `None`.
    fn bulk_get(&self, _request: &KeyBulkGetRequest) -> Result<Vec<Option<KeyGetResult>>, DbError> {
        Err(DbError::NotSupported(
            "Key-value bulk GET not supported by this driver".to_string(),
        ))
    }

    // -- Hash member operations --

    /// Set a field in a Hash key.
    fn hash_set(&self, _request: &HashSetRequest) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Hash SET not supported by this driver".to_string(),
        ))
    }

    /// Delete a field from a Hash key.
    fn hash_delete(&self, _request: &HashDeleteRequest) -> Result<bool, DbError> {
        Err(DbError::NotSupported(
            "Hash DELETE not supported by this driver".to_string(),
        ))
    }

    // -- List member operations --

    /// Overwrite a list element at the given index.
    fn list_set(&self, _request: &ListSetRequest) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "List SET not supported by this driver".to_string(),
        ))
    }

    /// Push a value to the head or tail of a list.
    fn list_push(&self, _request: &ListPushRequest) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "List PUSH not supported by this driver".to_string(),
        ))
    }

    /// Remove occurrences of a value from a list.
    fn list_remove(&self, _request: &ListRemoveRequest) -> Result<bool, DbError> {
        Err(DbError::NotSupported(
            "List REMOVE not supported by this driver".to_string(),
        ))
    }

    // -- Set member operations --

    /// Add a member to a Set key.
    fn set_add(&self, _request: &SetAddRequest) -> Result<bool, DbError> {
        Err(DbError::NotSupported(
            "Set ADD not supported by this driver".to_string(),
        ))
    }

    /// Remove a member from a Set key.
    fn set_remove(&self, _request: &SetRemoveRequest) -> Result<bool, DbError> {
        Err(DbError::NotSupported(
            "Set REMOVE not supported by this driver".to_string(),
        ))
    }

    // -- Sorted Set member operations --

    /// Add or update a member with a score in a Sorted Set key.
    fn zset_add(&self, _request: &ZSetAddRequest) -> Result<bool, DbError> {
        Err(DbError::NotSupported(
            "Sorted Set ADD not supported by this driver".to_string(),
        ))
    }

    /// Remove a member from a Sorted Set key.
    fn zset_remove(&self, _request: &ZSetRemoveRequest) -> Result<bool, DbError> {
        Err(DbError::NotSupported(
            "Sorted Set REMOVE not supported by this driver".to_string(),
        ))
    }

    // -- Stream operations --

    /// Append an entry to a Stream key. Returns the server-assigned entry ID.
    fn stream_add(&self, _request: &StreamAddRequest) -> Result<String, DbError> {
        Err(DbError::NotSupported(
            "Stream ADD not supported by this driver".to_string(),
        ))
    }

    /// Delete entries from a Stream key by their IDs.
    /// Returns the number of entries actually deleted.
    fn stream_delete(&self, _request: &StreamDeleteRequest) -> Result<u64, DbError> {
        Err(DbError::NotSupported(
            "Stream DELETE not supported by this driver".to_string(),
        ))
    }
}

/// Active database connection.
///
/// The UI interacts exclusively through this trait, never accessing driver internals.
/// Implementations must be thread-safe (`Send + Sync`) for background query execution.
pub trait Connection: Send + Sync {
    /// Returns the driver metadata for this connection.
    ///
    /// This provides access to the driver's capabilities, category, and query language
    /// without needing a reference to the driver itself.
    fn metadata(&self) -> &'static DriverMetadata;

    /// Returns the capabilities supported by this connection's driver.
    fn capabilities(&self) -> DriverCapabilities {
        self.metadata().capabilities
    }

    /// Check if a specific capability is supported.
    fn supports(&self, capability: DriverCapabilities) -> bool {
        self.capabilities().contains(capability)
    }

    /// Check if the connection is still alive.
    ///
    /// Typically sends a lightweight query like `SELECT 1`.
    fn ping(&self) -> Result<(), DbError>;

    /// Close the connection and release resources.
    fn close(&mut self) -> Result<(), DbError>;

    /// Execute a SQL query synchronously.
    ///
    /// For queries that may be long-running, prefer `execute_with_handle`
    /// to support cancellation.
    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError>;

    /// Execute a query and return a handle for cancellation.
    ///
    /// The default implementation delegates to `execute()` and returns
    /// an empty handle. Override this for databases that support cancellation.
    fn execute_with_handle(
        &self,
        req: &QueryRequest,
    ) -> Result<(QueryHandle, QueryResult), DbError> {
        let result = self.execute(req)?;
        Ok((QueryHandle::new(), result))
    }

    /// Cancel a running query using a previously returned handle.
    ///
    /// Behavior varies by database:
    /// - PostgreSQL: Sends `pg_cancel_backend()` to terminate the query
    /// - SQLite: Returns `DbError::NotSupported` (queries are typically fast)
    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError>;

    /// Cancel the currently active query on this connection.
    ///
    /// This is a convenience method that cancels whatever query is running
    /// without needing a handle. Returns `Ok(())` if no query is active.
    ///
    /// Behavior varies by database:
    /// - PostgreSQL: Sends cancel signal to the backend
    /// - SQLite: Calls sqlite3_interrupt()
    fn cancel_active(&self) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Query cancellation not supported".to_string(),
        ))
    }

    /// Get a cancel handle for this connection.
    ///
    /// The handle can be used from another thread to cancel an active query.
    /// Call this before starting a long-running query.
    fn cancel_handle(&self) -> Arc<dyn QueryCancelHandle> {
        Arc::new(NoopCancelHandle)
    }

    /// Clean up connection state after a cancelled query.
    ///
    /// This should be called after a query is cancelled to ensure
    /// the connection is in a clean state (e.g., rollback any open transaction).
    fn cleanup_after_cancel(&self) -> Result<(), DbError> {
        Ok(())
    }

    /// Retrieve the database schema (tables, views, columns, indexes).
    ///
    /// Called after connecting and when the user requests a schema refresh.
    fn schema(&self) -> Result<SchemaSnapshot, DbError>;

    /// List all databases available on the server.
    ///
    /// Returns database names with `is_current: true` for the active database.
    /// The default implementation returns an empty list (suitable for SQLite).
    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        Ok(Vec::new())
    }

    /// Fetch tables and views for a database (without column details).
    /// Returns empty `columns`/`indexes`; use `table_details()` for full info.
    fn schema_for_database(&self, _database: &str) -> Result<DbSchemaInfo, DbError> {
        Err(DbError::NotSupported(
            "schema_for_database not supported".to_string(),
        ))
    }

    /// Fetch columns and indexes for a table.
    fn table_details(
        &self,
        _database: &str,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<TableInfo, DbError> {
        Err(DbError::NotSupported(
            "table_details not supported".to_string(),
        ))
    }

    /// Fetch view metadata.
    fn view_details(
        &self,
        _database: &str,
        _schema: Option<&str>,
        _view: &str,
    ) -> Result<ViewInfo, DbError> {
        Err(DbError::NotSupported(
            "view_details not supported".to_string(),
        ))
    }

    /// Set active database for query execution (MySQL/MariaDB only).
    /// Issues `USE database` before queries. No-op for Postgres/SQLite.
    fn set_active_database(&self, _database: Option<&str>) -> Result<(), DbError> {
        Ok(())
    }

    /// Returns the currently active database, if any.
    fn active_database(&self) -> Option<String> {
        None
    }

    /// Returns the database kind for this connection.
    fn kind(&self) -> DbKind;

    /// Returns the schema loading strategy for this connection.
    ///
    /// This determines how the UI handles database clicks in the sidebar:
    /// - `LazyPerDatabase`: Load schema on click, support closing databases
    /// - `ConnectionPerDatabase`: Prompt to create new connection
    /// - `SingleDatabase`: No database switching needed
    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy;

    /// Returns the schema features supported by this connection.
    ///
    /// The UI uses this to decide which folders to show (FK, constraints, types, etc.).
    fn schema_features(&self) -> SchemaFeatures {
        SchemaFeatures::empty()
    }

    /// Fetch custom types for a schema (enums, domains, composites).
    fn schema_types(
        &self,
        _database: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<CustomTypeInfo>, DbError> {
        Ok(Vec::new())
    }

    /// Fetch all indexes in a schema.
    fn schema_indexes(
        &self,
        _database: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<SchemaIndexInfo>, DbError> {
        Ok(Vec::new())
    }

    /// Fetch all foreign keys in a schema.
    fn schema_foreign_keys(
        &self,
        _database: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        Ok(Vec::new())
    }

    // =========================================================================
    // Browse Operations (semantic queries, no raw SQL/JSON from UI)
    // =========================================================================

    /// Browse a table with pagination, ordering, and optional filter.
    ///
    /// The driver translates the request into its native query syntax.
    /// The default implementation builds SQL using `TableBrowseRequest::build_sql_with`.
    fn browse_table(&self, request: &TableBrowseRequest) -> Result<QueryResult, DbError> {
        let sql = request.build_sql_with(self.dialect());
        let mut query_request = QueryRequest::new(sql);

        if let Some(ref schema) = request.table.schema {
            query_request = query_request.with_database(Some(schema.clone()));
        }

        self.execute(&query_request)
    }

    /// Count rows in a table with an optional filter.
    ///
    /// The default implementation builds a `SELECT COUNT(*)` query.
    fn count_table(&self, request: &TableCountRequest) -> Result<u64, DbError> {
        let quoted_table = request.table.quoted_with(self.dialect());
        let sql = if let Some(ref f) = request.filter {
            let trimmed = f.trim();
            if trimmed.is_empty() {
                format!("SELECT COUNT(*) FROM {}", quoted_table)
            } else {
                format!("SELECT COUNT(*) FROM {} WHERE {}", quoted_table, trimmed)
            }
        } else {
            format!("SELECT COUNT(*) FROM {}", quoted_table)
        };

        let query_request = QueryRequest::new(sql);
        let result = self.execute(&query_request)?;

        let count = result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|val| match val {
                crate::Value::Int(i) => Some(*i as u64),
                _ => None,
            })
            .unwrap_or(0);

        Ok(count)
    }

    /// Browse a document collection with pagination and optional filter.
    ///
    /// The default implementation returns `NotSupported`. Document drivers
    /// override this to translate the request into their native query format.
    fn browse_collection(
        &self,
        _request: &CollectionBrowseRequest,
    ) -> Result<QueryResult, DbError> {
        Err(DbError::NotSupported(
            "Collection browsing not supported by this driver".to_string(),
        ))
    }

    /// Count documents in a collection with an optional filter.
    ///
    /// The default implementation returns `NotSupported`.
    fn count_collection(&self, _request: &CollectionCountRequest) -> Result<u64, DbError> {
        Err(DbError::NotSupported(
            "Collection counting not supported by this driver".to_string(),
        ))
    }

    /// Explain a query execution plan for a table or custom query.
    ///
    /// If `request.query` is `None`, explains a `SELECT * FROM table LIMIT 100`.
    /// Drivers override this with their native EXPLAIN syntax.
    fn explain(&self, _request: &ExplainRequest) -> Result<QueryResult, DbError> {
        Err(DbError::NotSupported(
            "EXPLAIN not supported by this driver".to_string(),
        ))
    }

    /// Describe a table's structure (columns, types, constraints).
    ///
    /// Returns the result as a query result set, similar to what `DESCRIBE table`
    /// would return in MySQL or `\d table` in psql.
    fn describe_table(&self, _request: &DescribeRequest) -> Result<QueryResult, DbError> {
        Err(DbError::NotSupported(
            "DESCRIBE not supported by this driver".to_string(),
        ))
    }

    /// Returns available code generators for this connection.
    fn code_generators(&self) -> &'static [CodeGeneratorInfo] {
        &[]
    }

    /// Generate code for a table. Returns `DbError::NotSupported` for unknown IDs.
    fn generate_code(&self, generator_id: &str, table: &TableInfo) -> Result<String, DbError> {
        let _ = table;
        Err(DbError::NotSupported(format!(
            "Code generator '{}' not supported",
            generator_id
        )))
    }

    /// Update a single row and return the updated row data.
    ///
    /// Uses `RETURNING *` on PostgreSQL for efficiency.
    /// Falls back to UPDATE + SELECT on MySQL/SQLite.
    fn update_row(&self, _patch: &RowPatch) -> Result<CrudResult, DbError> {
        Err(DbError::NotSupported(
            "Row updates not supported by this driver".to_string(),
        ))
    }

    /// Insert a new row and return the inserted row data.
    ///
    /// Uses `RETURNING *` on PostgreSQL for efficiency.
    /// Falls back to INSERT + SELECT on MySQL/SQLite.
    fn insert_row(&self, _insert: &RowInsert) -> Result<CrudResult, DbError> {
        Err(DbError::NotSupported(
            "Row inserts not supported by this driver".to_string(),
        ))
    }

    /// Delete a row and return the deleted row data.
    ///
    /// Uses `RETURNING *` on PostgreSQL for efficiency.
    /// Falls back to SELECT + DELETE on MySQL/SQLite.
    fn delete_row(&self, _delete: &RowDelete) -> Result<CrudResult, DbError> {
        Err(DbError::NotSupported(
            "Row deletes not supported by this driver".to_string(),
        ))
    }

    // =========================================================================
    // Document Operations (MongoDB-style)
    // =========================================================================

    /// Update documents matching a filter.
    fn update_document(&self, _update: &DocumentUpdate) -> Result<CrudResult, DbError> {
        Err(DbError::NotSupported(
            "Document updates not supported by this driver".to_string(),
        ))
    }

    /// Insert one or more documents.
    fn insert_document(&self, _insert: &DocumentInsert) -> Result<CrudResult, DbError> {
        Err(DbError::NotSupported(
            "Document inserts not supported by this driver".to_string(),
        ))
    }

    /// Delete documents matching a filter.
    fn delete_document(&self, _delete: &DocumentDelete) -> Result<CrudResult, DbError> {
        Err(DbError::NotSupported(
            "Document deletes not supported by this driver".to_string(),
        ))
    }

    /// Returns the key-value API implementation when available.
    ///
    /// Non-key-value drivers return `None`.
    fn key_value_api(&self) -> Option<&dyn KeyValueApi> {
        None
    }

    /// Returns the language service for this connection.
    ///
    /// Provides validation and dangerous-query detection for the connection's
    /// query language. The UI calls this instead of doing its own syntax checks.
    fn language_service(&self) -> &dyn LanguageService {
        &SqlLanguageService
    }

    /// Returns the SQL dialect for this connection.
    ///
    /// Used for generating database-specific SQL statements with proper
    /// quoting, escaping, and literal formatting.
    fn dialect(&self) -> &dyn SqlDialect;

    /// Returns the code generation capabilities of this connection.
    fn code_gen_capabilities(&self) -> CodeGenCapabilities {
        self.code_generator().capabilities()
    }

    /// Returns the code generator for this connection.
    fn code_generator(&self) -> &dyn CodeGenerator {
        &NoOpCodeGenerator
    }

    /// Generate SQL using this connection's dialect.
    ///
    /// Default implementation delegates to `crate::generate_sql()`.
    fn generate_sql(&self, request: &SqlGenerationRequest) -> Result<String, DbError> {
        Ok(crate::generate_sql(self.dialect(), request))
    }
}
