use serde::{Deserialize, Serialize};

use crate::{
    Row, SemanticFilter, Value,
    data::key_value::{
        HashDeleteRequest, HashSetRequest, KeyDeleteRequest, KeySetRequest, ListPushRequest,
        ListRemoveRequest, ListSetRequest, SetAddRequest, SetRemoveRequest, StreamAddRequest,
        StreamDeleteRequest, ZSetAddRequest, ZSetRemoveRequest,
    },
};

/// Unique identification of a record for UPDATE/DELETE operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecordIdentity {
    /// SQL-style composite primary key.
    /// Uses column names and values to construct a WHERE clause.
    Composite {
        columns: Vec<String>,
        values: Vec<Value>,
    },

    /// ObjectId for document databases.
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

/// A single column assignment: name, value, and optional driver-reported type.
///
/// The optional `type_name` is the raw database type as reported by the driver
/// (e.g. PostgreSQL's `_text` for `text[]`, `jsonb`, `int4`). Dialects use it
/// to choose the correct literal syntax — for instance, PostgreSQL needs
/// `ARRAY['a','b']::text[]` for array columns rather than the generic `::jsonb`
/// fallback used when no type is known.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnAssignment {
    pub name: String,
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
}

impl ColumnAssignment {
    pub fn new(name: impl Into<String>, value: Value) -> Self {
        Self {
            name: name.into(),
            value,
            type_name: None,
        }
    }

    pub fn typed(name: impl Into<String>, value: Value, type_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value,
            type_name: Some(type_name.into()),
        }
    }

    pub fn with_type_opt(mut self, type_name: Option<String>) -> Self {
        self.type_name = type_name;
        self
    }
}

impl From<(String, Value)> for ColumnAssignment {
    fn from((name, value): (String, Value)) -> Self {
        Self {
            name,
            value,
            type_name: None,
        }
    }
}

impl From<(&str, Value)> for ColumnAssignment {
    fn from((name, value): (&str, Value)) -> Self {
        Self {
            name: name.to_string(),
            value,
            type_name: None,
        }
    }
}

/// Wire shape for RowPatch — kept stable for IPC backward compatibility.
///
/// Old peers serialize/deserialize `changes` as `Vec<(String, Value)>` and
/// don't know about `change_types`. The `#[serde(default)]` on `change_types`
/// lets newer messages flow without breaking older peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RowPatchWire {
    identity: RowIdentity,
    table: String,
    schema: Option<String>,
    changes: Vec<(String, Value)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    change_types: Vec<Option<String>>,
}

/// Changes to apply to a single row via UPDATE.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "RowPatchWire", into = "RowPatchWire")]
pub struct RowPatch {
    /// Unique identification of the row to update.
    pub identity: RowIdentity,

    /// Table name.
    pub table: String,

    /// Schema name.
    pub schema: Option<String>,

    /// Column changes, each carrying name, value, and optional type metadata.
    pub changes: Vec<ColumnAssignment>,
}

impl RowPatch {
    /// Construct a RowPatch from `(column, value)` pairs without type info.
    ///
    /// Drivers that need type-aware literal emission (e.g. Postgres arrays)
    /// should construct via [`Self::with_typed_changes`] instead so the
    /// dialect can pick the right literal syntax.
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
            changes: changes.into_iter().map(ColumnAssignment::from).collect(),
        }
    }

    pub fn with_typed_changes(
        identity: RowIdentity,
        table: String,
        schema: Option<String>,
        changes: Vec<ColumnAssignment>,
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

impl From<RowPatchWire> for RowPatch {
    fn from(wire: RowPatchWire) -> Self {
        let RowPatchWire {
            identity,
            table,
            schema,
            changes,
            change_types,
        } = wire;

        let changes = changes
            .into_iter()
            .enumerate()
            .map(|(idx, (name, value))| ColumnAssignment {
                name,
                value,
                type_name: change_types.get(idx).cloned().flatten(),
            })
            .collect();

        Self {
            identity,
            table,
            schema,
            changes,
        }
    }
}

impl From<RowPatch> for RowPatchWire {
    fn from(patch: RowPatch) -> Self {
        let mut changes = Vec::with_capacity(patch.changes.len());
        let mut change_types = Vec::with_capacity(patch.changes.len());
        let mut any_typed = false;

        for assignment in patch.changes {
            if assignment.type_name.is_some() {
                any_typed = true;
            }
            change_types.push(assignment.type_name);
            changes.push((assignment.name, assignment.value));
        }

        if !any_typed {
            change_types.clear();
        }

        Self {
            identity: patch.identity,
            table: patch.table,
            schema: patch.schema,
            changes,
            change_types,
        }
    }
}

/// Wire shape for RowInsert — kept stable for IPC backward compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RowInsertWire {
    table: String,
    schema: Option<String>,
    columns: Vec<String>,
    values: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    column_types: Vec<Option<String>>,
}

/// Data for INSERT operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "RowInsertWire", into = "RowInsertWire")]
pub struct RowInsert {
    /// Table name.
    pub table: String,

    /// Schema name.
    pub schema: Option<String>,

    /// Columns being inserted, each carrying name, value, and optional type metadata.
    pub assignments: Vec<ColumnAssignment>,
}

impl RowInsert {
    /// Convenience constructor that takes parallel `columns`/`values` slices
    /// and produces assignments without type information.
    ///
    /// Drivers that need type-aware literal emission (e.g. Postgres arrays)
    /// should construct via [`Self::with_typed_assignments`] instead.
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
        let assignments = columns
            .into_iter()
            .zip(values)
            .map(|(name, value)| ColumnAssignment {
                name,
                value,
                type_name: None,
            })
            .collect();
        Self {
            table,
            schema,
            assignments,
        }
    }

    pub fn with_typed_assignments(
        table: String,
        schema: Option<String>,
        assignments: Vec<ColumnAssignment>,
    ) -> Self {
        Self {
            table,
            schema,
            assignments,
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.assignments.is_empty()
    }

    /// Column names in declared order. Allocates; prefer iterating `assignments` directly.
    pub fn column_names(&self) -> Vec<String> {
        self.assignments.iter().map(|a| a.name.clone()).collect()
    }

    /// Values in declared order. Allocates; prefer iterating `assignments` directly.
    pub fn values(&self) -> Vec<Value> {
        self.assignments.iter().map(|a| a.value.clone()).collect()
    }
}

impl From<RowInsertWire> for RowInsert {
    fn from(wire: RowInsertWire) -> Self {
        let RowInsertWire {
            table,
            schema,
            columns,
            values,
            column_types,
        } = wire;

        let assignments = columns
            .into_iter()
            .zip(values)
            .enumerate()
            .map(|(idx, (name, value))| ColumnAssignment {
                name,
                value,
                type_name: column_types.get(idx).cloned().flatten(),
            })
            .collect();

        Self {
            table,
            schema,
            assignments,
        }
    }
}

impl From<RowInsert> for RowInsertWire {
    fn from(insert: RowInsert) -> Self {
        let mut columns = Vec::with_capacity(insert.assignments.len());
        let mut values = Vec::with_capacity(insert.assignments.len());
        let mut column_types = Vec::with_capacity(insert.assignments.len());
        let mut any_typed = false;

        for assignment in insert.assignments {
            if assignment.type_name.is_some() {
                any_typed = true;
            }
            column_types.push(assignment.type_name);
            columns.push(assignment.name);
            values.push(assignment.value);
        }

        if !any_typed {
            column_types.clear();
        }

        Self {
            table: insert.table,
            schema: insert.schema,
            columns,
            values,
            column_types,
        }
    }
}

/// Data for DELETE operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowDelete {
    /// Unique identification of the row to delete.
    pub identity: RowIdentity,

    /// Table name.
    pub table: String,

    /// Schema name.
    pub schema: Option<String>,
}

/// Wire shape for SqlUpdateRequest — kept stable for IPC backward compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SqlUpdateRequestWire {
    table: String,
    schema: Option<String>,
    filter: SemanticFilter,
    changes: Vec<(String, Value)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    change_types: Vec<Option<String>>,
    returning: Option<Vec<String>>,
}

/// Changes to apply to rows selected by a semantic filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "SqlUpdateRequestWire", into = "SqlUpdateRequestWire")]
pub struct SqlUpdateRequest {
    /// Table name.
    pub table: String,

    /// Schema name.
    pub schema: Option<String>,

    /// Shared filter contract selecting the target rows.
    pub filter: SemanticFilter,

    /// Column changes, each carrying name, value, and optional type metadata.
    pub changes: Vec<ColumnAssignment>,

    /// Columns to return when the dialect supports RETURNING-style clauses.
    pub returning: Option<Vec<String>>,
}

/// Deletes rows selected by a semantic filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlDeleteRequest {
    /// Table name.
    pub table: String,

    /// Schema name.
    pub schema: Option<String>,

    /// Shared filter contract selecting the target rows.
    pub filter: SemanticFilter,

    /// Columns to return when the dialect supports RETURNING-style clauses.
    pub returning: Option<Vec<String>>,
}

/// Wire shape for SqlUpsertRequest — kept stable for IPC backward compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SqlUpsertRequestWire {
    table: String,
    schema: Option<String>,
    columns: Vec<String>,
    values: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    column_types: Vec<Option<String>>,
    conflict_columns: Vec<String>,
    update_assignments: Vec<(String, Value)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    update_assignment_types: Vec<Option<String>>,
}

/// Inserts a row and updates matching rows when a conflict occurs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "SqlUpsertRequestWire", into = "SqlUpsertRequestWire")]
pub struct SqlUpsertRequest {
    /// Table name.
    pub table: String,

    /// Schema name.
    pub schema: Option<String>,

    /// Columns being inserted, each carrying name, value, and optional type metadata.
    pub assignments: Vec<ColumnAssignment>,

    /// Columns that define the conflict target.
    pub conflict_columns: Vec<String>,

    /// Column changes to apply when a conflict occurs.
    pub update_assignments: Vec<ColumnAssignment>,
}

impl SqlUpdateRequest {
    /// Convenience constructor from `(name, value)` pairs without type info.
    ///
    /// Use [`Self::with_typed_changes`] when type metadata is available so
    /// dialects can emit type-aware literals (e.g. Postgres `ARRAY[...]::T[]`).
    pub fn new(
        table: String,
        schema: Option<String>,
        filter: SemanticFilter,
        changes: Vec<(String, Value)>,
    ) -> Self {
        Self {
            table,
            schema,
            filter,
            changes: changes.into_iter().map(ColumnAssignment::from).collect(),
            returning: None,
        }
    }

    pub fn with_typed_changes(
        table: String,
        schema: Option<String>,
        filter: SemanticFilter,
        changes: Vec<ColumnAssignment>,
    ) -> Self {
        Self {
            table,
            schema,
            filter,
            changes,
            returning: None,
        }
    }

    pub fn with_returning(mut self, returning: Vec<String>) -> Self {
        self.returning = Some(returning);
        self
    }

    pub fn has_changes(&self) -> bool {
        !self.changes.is_empty()
    }
}

impl From<SqlUpdateRequestWire> for SqlUpdateRequest {
    fn from(wire: SqlUpdateRequestWire) -> Self {
        let SqlUpdateRequestWire {
            table,
            schema,
            filter,
            changes,
            change_types,
            returning,
        } = wire;

        let changes = changes
            .into_iter()
            .enumerate()
            .map(|(idx, (name, value))| ColumnAssignment {
                name,
                value,
                type_name: change_types.get(idx).cloned().flatten(),
            })
            .collect();

        Self {
            table,
            schema,
            filter,
            changes,
            returning,
        }
    }
}

impl From<SqlUpdateRequest> for SqlUpdateRequestWire {
    fn from(req: SqlUpdateRequest) -> Self {
        let mut changes = Vec::with_capacity(req.changes.len());
        let mut change_types = Vec::with_capacity(req.changes.len());
        let mut any_typed = false;

        for assignment in req.changes {
            if assignment.type_name.is_some() {
                any_typed = true;
            }
            change_types.push(assignment.type_name);
            changes.push((assignment.name, assignment.value));
        }

        if !any_typed {
            change_types.clear();
        }

        Self {
            table: req.table,
            schema: req.schema,
            filter: req.filter,
            changes,
            change_types,
            returning: req.returning,
        }
    }
}

impl SqlDeleteRequest {
    pub fn new(table: String, schema: Option<String>, filter: SemanticFilter) -> Self {
        Self {
            table,
            schema,
            filter,
            returning: None,
        }
    }

    pub fn with_returning(mut self, returning: Vec<String>) -> Self {
        self.returning = Some(returning);
        self
    }
}

impl SqlUpsertRequest {
    /// Convenience constructor that takes parallel `columns`/`values` slices
    /// and `(name, value)` update assignments. Use
    /// [`Self::with_typed_assignments`] when type metadata is available.
    pub fn new(
        table: String,
        schema: Option<String>,
        columns: Vec<String>,
        values: Vec<Value>,
        conflict_columns: Vec<String>,
        update_assignments: Vec<(String, Value)>,
    ) -> Self {
        debug_assert_eq!(
            columns.len(),
            values.len(),
            "SqlUpsertRequest: columns and values must have same length"
        );

        let assignments = columns
            .into_iter()
            .zip(values)
            .map(|(name, value)| ColumnAssignment {
                name,
                value,
                type_name: None,
            })
            .collect();

        Self {
            table,
            schema,
            assignments,
            conflict_columns,
            update_assignments: update_assignments
                .into_iter()
                .map(ColumnAssignment::from)
                .collect(),
        }
    }

    pub fn with_typed_assignments(
        table: String,
        schema: Option<String>,
        assignments: Vec<ColumnAssignment>,
        conflict_columns: Vec<String>,
        update_assignments: Vec<ColumnAssignment>,
    ) -> Self {
        Self {
            table,
            schema,
            assignments,
            conflict_columns,
            update_assignments,
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.assignments.is_empty() && !self.conflict_columns.is_empty()
    }
}

impl From<SqlUpsertRequestWire> for SqlUpsertRequest {
    fn from(wire: SqlUpsertRequestWire) -> Self {
        let SqlUpsertRequestWire {
            table,
            schema,
            columns,
            values,
            column_types,
            conflict_columns,
            update_assignments,
            update_assignment_types,
        } = wire;

        let assignments = columns
            .into_iter()
            .zip(values)
            .enumerate()
            .map(|(idx, (name, value))| ColumnAssignment {
                name,
                value,
                type_name: column_types.get(idx).cloned().flatten(),
            })
            .collect();

        let update_assignments = update_assignments
            .into_iter()
            .enumerate()
            .map(|(idx, (name, value))| ColumnAssignment {
                name,
                value,
                type_name: update_assignment_types.get(idx).cloned().flatten(),
            })
            .collect();

        Self {
            table,
            schema,
            assignments,
            conflict_columns,
            update_assignments,
        }
    }
}

impl From<SqlUpsertRequest> for SqlUpsertRequestWire {
    fn from(req: SqlUpsertRequest) -> Self {
        fn split_assignments(
            assignments: Vec<ColumnAssignment>,
        ) -> (Vec<String>, Vec<Value>, Vec<Option<String>>) {
            let mut names = Vec::with_capacity(assignments.len());
            let mut values = Vec::with_capacity(assignments.len());
            let mut types = Vec::with_capacity(assignments.len());
            let mut any_typed = false;

            for a in assignments {
                if a.type_name.is_some() {
                    any_typed = true;
                }
                types.push(a.type_name);
                names.push(a.name);
                values.push(a.value);
            }

            if !any_typed {
                types.clear();
            }

            (names, values, types)
        }

        let (columns, values, column_types) = split_assignments(req.assignments);
        let (update_columns, update_values, update_assignment_types) =
            split_assignments(req.update_assignments);

        let update_assignments = update_columns
            .into_iter()
            .zip(update_values)
            .collect::<Vec<_>>();

        Self {
            table: req.table,
            schema: req.schema,
            columns,
            values,
            column_types,
            conflict_columns: req.conflict_columns,
            update_assignments,
            update_assignment_types,
        }
    }
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
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
// Document Database Mutations
// =============================================================================

/// Filter criteria for document operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MutationRequest {
    SqlUpdate(RowPatch),
    SqlUpdateMany(SqlUpdateRequest),
    SqlInsert(RowInsert),
    SqlUpsert(SqlUpsertRequest),
    SqlDelete(RowDelete),
    SqlDeleteMany(SqlDeleteRequest),

    DocumentUpdate(DocumentUpdate),
    DocumentInsert(DocumentInsert),
    DocumentDelete(DocumentDelete),

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

    pub fn sql_update_many(update: SqlUpdateRequest) -> Self {
        Self::SqlUpdateMany(update)
    }

    pub fn sql_insert(insert: RowInsert) -> Self {
        Self::SqlInsert(insert)
    }

    pub fn sql_upsert(upsert: SqlUpsertRequest) -> Self {
        Self::SqlUpsert(upsert)
    }

    pub fn sql_delete(delete: RowDelete) -> Self {
        Self::SqlDelete(delete)
    }

    pub fn sql_delete_many(delete: SqlDeleteRequest) -> Self {
        Self::SqlDeleteMany(delete)
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
            Self::SqlUpdate(_)
                | Self::SqlUpdateMany(_)
                | Self::SqlInsert(_)
                | Self::SqlUpsert(_)
                | Self::SqlDelete(_)
                | Self::SqlDeleteMany(_)
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
