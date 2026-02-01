use crate::{Row, Value};

/// Unique identification of a row for WHERE clause in UPDATE/DELETE operations.
///
/// Uses primary key columns and their values to construct a stable WHERE clause.
/// If a table has no PK, the table is considered read-only for editing purposes.
#[derive(Debug, Clone)]
pub struct RowIdentity {
    /// Names of the primary key columns.
    pub columns: Vec<String>,

    /// Values of the primary key columns (same order as `columns`).
    pub values: Vec<Value>,
}

impl RowIdentity {
    pub fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
        debug_assert_eq!(
            columns.len(),
            values.len(),
            "RowIdentity: columns and values must have same length"
        );
        Self { columns, values }
    }

    pub fn is_valid(&self) -> bool {
        !self.columns.is_empty() && self.columns.len() == self.values.len()
    }
}

/// Changes to apply to a single row via UPDATE.
#[derive(Debug, Clone)]
pub struct RowPatch {
    /// Unique identification of the row to update.
    pub identity: RowIdentity,

    /// Table name.
    pub table: String,

    /// Schema name (PostgreSQL) or None (SQLite/MySQL).
    pub schema: Option<String>,

    /// Column changes: (column_name, new_value).
    pub changes: Vec<(String, Value)>,
}

impl RowPatch {
    pub fn new(
        identity: RowIdentity,
        table: String,
        schema: Option<String>,
        changes: Vec<(String, Value)>,
    ) -> Self {
        Self {
            identity,
            table,
            schema,
            changes,
        }
    }

    pub fn has_changes(&self) -> bool {
        !self.changes.is_empty()
    }
}

/// State of a row during editing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowState {
    /// No pending changes.
    Clean,

    /// Has unsaved local modifications.
    Dirty,

    /// Currently saving to database.
    Saving,

    /// Last save operation failed.
    Error(String),
}

impl Default for RowState {
    fn default() -> Self {
        Self::Clean
    }
}

impl RowState {
    pub fn is_clean(&self) -> bool {
        matches!(self, Self::Clean)
    }

    pub fn is_dirty(&self) -> bool {
        matches!(self, Self::Dirty)
    }

    pub fn is_saving(&self) -> bool {
        matches!(self, Self::Saving)
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }

    pub fn error_message(&self) -> Option<&str> {
        match self {
            Self::Error(msg) => Some(msg),
            _ => None,
        }
    }
}

/// Result of a CRUD operation.
#[derive(Debug, Clone)]
pub struct CrudResult {
    /// Number of rows affected by the operation.
    pub affected_rows: u64,

    /// The updated row data (from RETURNING clause or re-query).
    /// None if the operation doesn't return row data.
    pub returning_row: Option<Row>,
}

impl CrudResult {
    pub fn new(affected_rows: u64, returning_row: Option<Row>) -> Self {
        Self {
            affected_rows,
            returning_row,
        }
    }

    pub fn success(returning_row: Row) -> Self {
        Self {
            affected_rows: 1,
            returning_row: Some(returning_row),
        }
    }

    pub fn empty() -> Self {
        Self {
            affected_rows: 0,
            returning_row: None,
        }
    }
}
