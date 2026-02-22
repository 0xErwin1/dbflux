use crate::{
    Row, Value,
    key_value::{
        HashDeleteRequest, HashSetRequest, KeyDeleteRequest, KeySetRequest, ListPushRequest,
        ListRemoveRequest, ListSetRequest, SetAddRequest, SetRemoveRequest, StreamAddRequest,
        StreamDeleteRequest, ZSetAddRequest, ZSetRemoveRequest,
    },
};

/// Unique identification of a record for UPDATE/DELETE operations.
///
/// Different database types use different identification methods:
/// - SQL: composite primary key (one or more columns)
/// - Document DBs: ObjectId or similar unique identifier
/// - Key-Value: the key itself
#[derive(Debug, Clone)]
pub enum RecordIdentity {
    /// SQL-style composite primary key.
    /// Uses column names and values to construct a WHERE clause.
    Composite {
        columns: Vec<String>,
        values: Vec<Value>,
    },

    /// MongoDB-style ObjectId.
    /// Uses the `_id` field for identification.
    ObjectId(String),

    /// Key-value store key.
    /// The key string directly identifies the record.
    Key(String),
}

impl RecordIdentity {
    /// Create a composite identity from column names and values.
    pub fn composite(columns: Vec<String>, values: Vec<Value>) -> Self {
        debug_assert_eq!(
            columns.len(),
            values.len(),
            "RecordIdentity: columns and values must have same length"
        );
        Self::Composite { columns, values }
    }

    /// Alias for `composite` (backward compatibility).
    pub fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
        Self::composite(columns, values)
    }

    pub fn object_id(id: impl Into<String>) -> Self {
        Self::ObjectId(id.into())
    }

    pub fn key(key: impl Into<String>) -> Self {
        Self::Key(key.into())
    }

    pub fn is_valid(&self) -> bool {
        match self {
            Self::Composite { columns, values } => {
                !columns.is_empty() && columns.len() == values.len()
            }
            Self::ObjectId(id) => !id.is_empty(),
            Self::Key(key) => !key.is_empty(),
        }
    }

    /// Returns columns for composite identity, empty slice for others.
    pub fn columns(&self) -> &[String] {
        match self {
            Self::Composite { columns, .. } => columns,
            _ => &[],
        }
    }

    /// Returns values for composite identity, empty slice for others.
    pub fn values(&self) -> &[Value] {
        match self {
            Self::Composite { values, .. } => values,
            _ => &[],
        }
    }
}

/// Legacy alias for backward compatibility.
pub type RowIdentity = RecordIdentity;

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

/// Data for INSERT operation.
#[derive(Debug, Clone)]
pub struct RowInsert {
    /// Table name.
    pub table: String,

    /// Schema name (PostgreSQL) or None (SQLite/MySQL).
    pub schema: Option<String>,

    /// Column names for the values being inserted.
    pub columns: Vec<String>,

    /// Values to insert (same order as `columns`).
    pub values: Vec<Value>,
}

impl RowInsert {
    pub fn new(
        table: String,
        schema: Option<String>,
        columns: Vec<String>,
        values: Vec<Value>,
    ) -> Self {
        debug_assert_eq!(
            columns.len(),
            values.len(),
            "RowInsert: columns and values must have same length"
        );
        Self {
            table,
            schema,
            columns,
            values,
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.columns.is_empty() && self.columns.len() == self.values.len()
    }
}

/// Data for DELETE operation.
#[derive(Debug, Clone)]
pub struct RowDelete {
    /// Unique identification of the row to delete.
    pub identity: RowIdentity,

    /// Table name.
    pub table: String,

    /// Schema name (PostgreSQL) or None (SQLite/MySQL).
    pub schema: Option<String>,
}

impl RowDelete {
    pub fn new(identity: RowIdentity, table: String, schema: Option<String>) -> Self {
        Self {
            identity,
            table,
            schema,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.identity.is_valid()
    }
}

/// State of a row during editing.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RowState {
    /// No pending changes.
    #[default]
    Clean,

    /// Has unsaved local modifications.
    Dirty,

    /// Currently saving to database.
    Saving,

    /// Last save operation failed.
    Error(String),

    /// New row pending INSERT (not yet in database).
    PendingInsert,

    /// Existing row marked for DELETE (will be removed on save).
    PendingDelete,
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

    pub fn is_pending_insert(&self) -> bool {
        matches!(self, Self::PendingInsert)
    }

    pub fn is_pending_delete(&self) -> bool {
        matches!(self, Self::PendingDelete)
    }

    /// Check if the row has any pending changes (dirty, insert, or delete).
    pub fn has_pending_changes(&self) -> bool {
        matches!(
            self,
            Self::Dirty | Self::PendingInsert | Self::PendingDelete
        )
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

// =============================================================================
// Document Database Mutations (MongoDB-style)
// =============================================================================

/// Filter criteria for document operations.
#[derive(Debug, Clone)]
pub struct DocumentFilter {
    /// JSON-style filter document (e.g., `{"status": "active"}`).
    pub filter: serde_json::Value,
}

impl DocumentFilter {
    pub fn new(filter: serde_json::Value) -> Self {
        Self { filter }
    }

    pub fn by_id(id: &str) -> Self {
        Self {
            filter: serde_json::json!({"_id": id}),
        }
    }
}

/// Update operation for document databases.
#[derive(Debug, Clone)]
pub struct DocumentUpdate {
    /// Collection name.
    pub collection: String,

    /// Database name (optional, uses current if None).
    pub database: Option<String>,

    /// Filter to select documents to update.
    pub filter: DocumentFilter,

    /// Update operations (e.g., `{"$set": {"field": "value"}}`).
    pub update: serde_json::Value,

    /// Update all matching documents (updateMany) vs first match (updateOne).
    pub many: bool,

    /// Insert if no document matches (upsert).
    pub upsert: bool,
}

impl DocumentUpdate {
    pub fn new(collection: String, filter: DocumentFilter, update: serde_json::Value) -> Self {
        Self {
            collection,
            database: None,
            filter,
            update,
            many: false,
            upsert: false,
        }
    }

    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }

    pub fn many(mut self) -> Self {
        self.many = true;
        self
    }

    pub fn upsert(mut self) -> Self {
        self.upsert = true;
        self
    }
}

/// Insert operation for document databases.
#[derive(Debug, Clone)]
pub struct DocumentInsert {
    /// Collection name.
    pub collection: String,

    /// Database name (optional, uses current if None).
    pub database: Option<String>,

    /// Documents to insert.
    pub documents: Vec<serde_json::Value>,
}

impl DocumentInsert {
    pub fn one(collection: String, document: serde_json::Value) -> Self {
        Self {
            collection,
            database: None,
            documents: vec![document],
        }
    }

    pub fn many(collection: String, documents: Vec<serde_json::Value>) -> Self {
        Self {
            collection,
            database: None,
            documents,
        }
    }

    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }
}

/// Delete operation for document databases.
#[derive(Debug, Clone)]
pub struct DocumentDelete {
    /// Collection name.
    pub collection: String,

    /// Database name (optional, uses current if None).
    pub database: Option<String>,

    /// Filter to select documents to delete.
    pub filter: DocumentFilter,

    /// Delete all matching documents (deleteMany) vs first match (deleteOne).
    pub many: bool,
}

impl DocumentDelete {
    pub fn new(collection: String, filter: DocumentFilter) -> Self {
        Self {
            collection,
            database: None,
            filter,
            many: false,
        }
    }

    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }

    pub fn many(mut self) -> Self {
        self.many = true;
        self
    }
}

// =============================================================================
// Unified Mutation Request
// =============================================================================

/// Unified mutation request that can represent operations across database paradigms.
#[derive(Debug, Clone)]
pub enum MutationRequest {
    // SQL mutations (relational databases)
    SqlUpdate(RowPatch),
    SqlInsert(RowInsert),
    SqlDelete(RowDelete),

    // Document mutations (MongoDB-style)
    DocumentUpdate(DocumentUpdate),
    DocumentInsert(DocumentInsert),
    DocumentDelete(DocumentDelete),

    // Key-value mutations (Redis-style)
    KeyValueSet(KeySetRequest),
    KeyValueDelete(KeyDeleteRequest),
    KeyValueHashSet(HashSetRequest),
    KeyValueHashDelete(HashDeleteRequest),
    KeyValueListPush(ListPushRequest),
    KeyValueListSet(ListSetRequest),
    KeyValueListRemove(ListRemoveRequest),
    KeyValueSetAdd(SetAddRequest),
    KeyValueSetRemove(SetRemoveRequest),
    KeyValueZSetAdd(ZSetAddRequest),
    KeyValueZSetRemove(ZSetRemoveRequest),
    KeyValueStreamAdd(StreamAddRequest),
    KeyValueStreamDelete(StreamDeleteRequest),
}

impl MutationRequest {
    pub fn sql_update(patch: RowPatch) -> Self {
        Self::SqlUpdate(patch)
    }

    pub fn sql_insert(insert: RowInsert) -> Self {
        Self::SqlInsert(insert)
    }

    pub fn sql_delete(delete: RowDelete) -> Self {
        Self::SqlDelete(delete)
    }

    pub fn document_update(update: DocumentUpdate) -> Self {
        Self::DocumentUpdate(update)
    }

    pub fn document_insert(insert: DocumentInsert) -> Self {
        Self::DocumentInsert(insert)
    }

    pub fn document_delete(delete: DocumentDelete) -> Self {
        Self::DocumentDelete(delete)
    }

    /// Returns true if this is a SQL mutation.
    pub fn is_sql(&self) -> bool {
        matches!(
            self,
            Self::SqlUpdate(_) | Self::SqlInsert(_) | Self::SqlDelete(_)
        )
    }

    /// Returns true if this is a document mutation.
    pub fn is_document(&self) -> bool {
        matches!(
            self,
            Self::DocumentUpdate(_) | Self::DocumentInsert(_) | Self::DocumentDelete(_)
        )
    }

    pub fn is_key_value(&self) -> bool {
        matches!(
            self,
            Self::KeyValueSet(_)
                | Self::KeyValueDelete(_)
                | Self::KeyValueHashSet(_)
                | Self::KeyValueHashDelete(_)
                | Self::KeyValueListPush(_)
                | Self::KeyValueListSet(_)
                | Self::KeyValueListRemove(_)
                | Self::KeyValueSetAdd(_)
                | Self::KeyValueSetRemove(_)
                | Self::KeyValueZSetAdd(_)
                | Self::KeyValueZSetRemove(_)
                | Self::KeyValueStreamAdd(_)
                | Self::KeyValueStreamDelete(_)
        )
    }
}
