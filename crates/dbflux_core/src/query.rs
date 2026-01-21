use crate::Value;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct QueryRequest {
    pub sql: String,
    pub params: Vec<Value>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub statement_timeout: Option<Duration>,
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
}

pub type Row = Vec<Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Row>,
    pub affected_rows: Option<u64>,
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

/// Handle for cancelling a running query.
///
/// Returned by `execute_with_handle()`. The internal data is driver-specific
/// (e.g., PostgreSQL backend PID) but opaque to the UI.
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
