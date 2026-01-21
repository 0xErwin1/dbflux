use crate::{
    ConnectionProfile, DatabaseInfo, DbError, DbKind, QueryHandle, QueryRequest, QueryResult,
    SchemaSnapshot,
};

pub trait DbDriver: Send + Sync {
    fn kind(&self) -> DbKind;

    fn display_name(&self) -> &'static str {
        self.kind().display_name()
    }

    fn description(&self) -> &'static str {
        ""
    }

    fn requires_password(&self) -> bool {
        true
    }

    fn connect(&self, profile: &ConnectionProfile) -> Result<Box<dyn Connection>, DbError> {
        self.connect_with_password(profile, None)
    }

    fn connect_with_password(
        &self,
        profile: &ConnectionProfile,
        password: Option<&str>,
    ) -> Result<Box<dyn Connection>, DbError>;

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError>;
}

/// Active database connection.
///
/// UI interacts with this trait, never with driver internals.
pub trait Connection: Send + Sync {
    /// Check if the connection is alive.
    fn ping(&self) -> Result<(), DbError>;

    /// Close the connection.
    fn close(&mut self) -> Result<(), DbError>;

    /// Execute a query synchronously.
    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError>;

    /// Execute a query, returning a handle for cancellation.
    ///
    /// For simple/fast queries, the handle can be ignored.
    fn execute_with_handle(
        &self,
        req: &QueryRequest,
    ) -> Result<(QueryHandle, QueryResult), DbError> {
        let result = self.execute(req)?;
        Ok((QueryHandle::new(), result))
    }

    /// Cancel a running query.
    ///
    /// - PostgreSQL: uses `pg_cancel_backend()`
    /// - SQLite: returns `DbError::NotSupported`
    fn cancel(&self, handle: &QueryHandle) -> Result<(), DbError>;

    /// Get schema snapshot (tables, views, columns, indexes).
    fn schema(&self) -> Result<SchemaSnapshot, DbError>;

    /// List all databases on the server.
    ///
    /// Returns a list of database names. The current database is marked with `is_current: true`.
    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        Ok(Vec::new())
    }

    /// Get the database kind.
    fn kind(&self) -> DbKind;
}
