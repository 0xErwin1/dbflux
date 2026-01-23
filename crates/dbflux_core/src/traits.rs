use crate::{
    ConnectionProfile, DatabaseInfo, DbError, DbKind, QueryHandle, QueryRequest, QueryResult,
    SchemaSnapshot,
};

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

    /// Returns the database kind for this connection.
    fn kind(&self) -> DbKind;
}
