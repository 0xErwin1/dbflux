use bitflags::bitflags;

use crate::{
    ConnectionProfile, CrudResult, CustomTypeInfo, DatabaseInfo, DbError, DbKind, DbSchemaInfo,
    DriverFormDef, FormValues, QueryHandle, QueryRequest, QueryResult, RowDelete, RowInsert,
    RowPatch, SchemaForeignKeyInfo, SchemaIndexInfo, SchemaSnapshot, TableInfo, ViewInfo,
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

    /// Human-readable name for UI display (e.g., "PostgreSQL", "SQLite").
    fn display_name(&self) -> &'static str {
        self.kind().display_name()
    }

    /// Optional description shown in the connection manager.
    fn description(&self) -> &'static str {
        ""
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

    /// Whether this database type requires a password for connection.
    ///
    /// Returns `false` for file-based databases like SQLite.
    fn requires_password(&self) -> bool {
        true
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

/// Active database connection.
///
/// The UI interacts exclusively through this trait, never accessing driver internals.
/// Implementations must be thread-safe (`Send + Sync`) for background query execution.
pub trait Connection: Send + Sync {
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
}
