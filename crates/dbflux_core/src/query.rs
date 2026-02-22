use crate::Value;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

// -- Query Result Shape --

/// Shape of data returned by a query. Set by the driver; the UI never sniffs content.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum QueryResultShape {
    /// Tabular data with columns and rows (SQL results, Redis arrays that
    /// fit a uniform structure).
    #[default]
    Table,

    /// Structured JSON (MongoDB documents, Redis hash results).
    Json,

    /// Plain text (Redis status replies, single-value results).
    Text,

    /// Raw binary data (Redis bulk strings that failed UTF-8 decode).
    Binary,
}

impl QueryResultShape {
    pub fn is_table(&self) -> bool {
        matches!(self, Self::Table)
    }

    pub fn is_json(&self) -> bool {
        matches!(self, Self::Json)
    }

    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text)
    }

    pub fn is_binary(&self) -> bool {
        matches!(self, Self::Binary)
    }
}

// -- Query Request --

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

// -- Query Result --

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub shape: QueryResultShape,
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Row>,
    pub affected_rows: Option<u64>,
    pub execution_time: Duration,
    pub text_body: Option<String>,
    pub raw_bytes: Option<Vec<u8>>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            shape: QueryResultShape::Table,
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: None,
            execution_time: Duration::ZERO,
            text_body: None,
            raw_bytes: None,
        }
    }

    pub fn table(
        columns: Vec<ColumnMeta>,
        rows: Vec<Row>,
        affected_rows: Option<u64>,
        execution_time: Duration,
    ) -> Self {
        Self {
            shape: QueryResultShape::Table,
            columns,
            rows,
            affected_rows,
            execution_time,
            text_body: None,
            raw_bytes: None,
        }
    }

    pub fn json(columns: Vec<ColumnMeta>, rows: Vec<Row>, execution_time: Duration) -> Self {
        Self {
            shape: QueryResultShape::Json,
            columns,
            rows,
            affected_rows: None,
            execution_time,
            text_body: None,
            raw_bytes: None,
        }
    }

    pub fn text(body: String, execution_time: Duration) -> Self {
        Self {
            shape: QueryResultShape::Text,
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: None,
            execution_time,
            text_body: Some(body),
            raw_bytes: None,
        }
    }

    pub fn binary(data: Vec<u8>, execution_time: Duration) -> Self {
        Self {
            shape: QueryResultShape::Binary,
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: None,
            execution_time,
            text_body: None,
            raw_bytes: Some(data),
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
