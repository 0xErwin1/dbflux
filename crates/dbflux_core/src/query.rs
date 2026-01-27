use crate::Value;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

/// Parameters for executing a SQL query.
#[derive(Debug, Clone, Default)]
pub struct QueryRequest {
    /// The SQL statement to execute.
    pub sql: String,

    /// Bind parameters for parameterized queries.
    pub params: Vec<Value>,

    /// Maximum number of rows to return (applied as SQL LIMIT).
    pub limit: Option<u32>,

    /// Number of rows to skip (applied as SQL OFFSET).
    pub offset: Option<u32>,

    /// Maximum time to wait for query completion.
    pub statement_timeout: Option<Duration>,

    /// Target database for query execution (MySQL/MariaDB).
    ///
    /// When set, the driver issues `USE database` before executing the query
    /// if the connection's current database differs. Ignored by PostgreSQL
    /// and SQLite (which use connection-level database selection).
    pub database: Option<String>,
}

impl QueryRequest {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            ..Default::default()
        }
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn with_database(mut self, database: Option<String>) -> Self {
        self.database = database;
        self
    }
}

/// A single row of query results.
pub type Row = Vec<Value>;

/// Metadata for a result column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    /// Column name as returned by the database.
    pub name: String,

    /// Database-specific type name (e.g., "varchar", "int4", "TEXT").
    pub type_name: String,

    /// Whether the column allows NULL values.
    pub nullable: bool,
}

/// Result of executing a SQL query.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Metadata for each column in the result set.
    pub columns: Vec<ColumnMeta>,

    /// Row data, where each row contains values matching `columns` order.
    pub rows: Vec<Row>,

    /// Number of rows affected by INSERT/UPDATE/DELETE statements.
    /// `None` for SELECT queries.
    pub affected_rows: Option<u64>,

    /// Wall-clock time taken to execute the query.
    pub execution_time: Duration,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: None,
            execution_time: Duration::ZERO,
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// Opaque handle for cancelling a running query.
///
/// Returned by `Connection::execute_with_handle()`. The internal data
/// is driver-specific (e.g., PostgreSQL backend PID) but hidden from the UI.
#[derive(Debug, Clone)]
pub struct QueryHandle {
    pub id: Uuid,
}

impl QueryHandle {
    pub fn new() -> Self {
        Self { id: Uuid::new_v4() }
    }
}

impl Default for QueryHandle {
    fn default() -> Self {
        Self::new()
    }
}
