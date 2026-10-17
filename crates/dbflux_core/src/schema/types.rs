use crate::data::key_value::KeyType;
use crate::{SchemaDropTarget, SchemaObjectKind};
use serde::{Deserialize, Serialize};

/// Information about a database on the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,

    /// True if this is the currently connected database.
    pub is_current: bool,
}

/// Schema within a database (PostgreSQL concept; SQLite has only "main").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbSchemaInfo {
    pub name: String,
    pub tables: Vec<TableInfo>,
    pub views: Vec<ViewInfo>,

    /// Custom types (enums, domains, composites). Lazy-loaded.
    #[serde(default)]
    pub custom_types: Option<Vec<CustomTypeInfo>>,
}

impl DbSchemaInfo {
    /// Removes the table, collection, or view named by `target`; database
    /// targets leave the schema unchanged.
    pub fn remove_object(&mut self, target: &SchemaDropTarget) {
        match target.kind {
            SchemaObjectKind::Table | SchemaObjectKind::Collection => {
                self.tables.retain(|entry| entry.name != target.name);
            }
            SchemaObjectKind::View => {
                self.views.retain(|entry| entry.name != target.name);
            }
            SchemaObjectKind::Database => {}
        }
    }
}

/// Unified schema structure for different database paradigms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataStructure {
    /// SQL databases (PostgreSQL, MySQL, SQLite, etc.)
    Relational(RelationalSchema),

    /// Document databases (MongoDB, CouchDB, etc.)
    Document(DocumentSchema),

    /// Key-value stores (Redis, Valkey, etc.)
    KeyValue(KeyValueSchema),

    /// Holds postcard wire index 3 (formerly the never-built graph schema) so
    /// that the variants after it keep their indices for driver RPC peers.
    #[doc(hidden)]
    ReservedGraph,

    /// Holds postcard wire index 4 (formerly the never-built wide-column schema)
    /// so that `TimeSeries` keeps index 5 for peers on the driver RPC protocol.
    #[doc(hidden)]
    ReservedWideColumn,

    /// Time-series databases (InfluxDB, TimescaleDB, QuestDB)
    TimeSeries(TimeSeriesSchema),
}

impl Default for DataStructure {
    fn default() -> Self {
        Self::Relational(RelationalSchema::default())
    }
}

/// What every schema with a database list exposes, so snapshot-wide
/// operations walk any paradigm the same way instead of matching each one.
trait DatabaseScoped {
    fn databases(&self) -> &[DatabaseInfo];

    fn current_database(&self) -> Option<&str>;

    fn databases_mut(&mut self) -> &mut Vec<DatabaseInfo>;

    /// Forgets the current database and the objects loaded for it.
    fn clear_current_database(&mut self);

    /// Removes the object `target` names from the current database. Schemas
    /// with no droppable objects keep everything.
    fn remove_current_object(&mut self, _target: &SchemaDropTarget) {}
}

impl DataStructure {
    /// The schema as a database-scoped one, the single place that knows which
    /// paradigms hold a database list. `None` for key-value stores, whose
    /// keyspaces are not databases, and the reserved wire slots.
    fn database_scoped(&self) -> Option<&dyn DatabaseScoped> {
        match self {
            DataStructure::Relational(schema) => Some(schema),
            DataStructure::Document(schema) => Some(schema),
            DataStructure::TimeSeries(schema) => Some(schema),
            DataStructure::KeyValue(_)
            | DataStructure::ReservedGraph
            | DataStructure::ReservedWideColumn => None,
        }
    }

    fn database_scoped_mut(&mut self) -> Option<&mut dyn DatabaseScoped> {
        match self {
            DataStructure::Relational(schema) => Some(schema),
            DataStructure::Document(schema) => Some(schema),
            DataStructure::TimeSeries(schema) => Some(schema),
            DataStructure::KeyValue(_)
            | DataStructure::ReservedGraph
            | DataStructure::ReservedWideColumn => None,
        }
    }
}

impl DatabaseScoped for RelationalSchema {
    fn databases(&self) -> &[DatabaseInfo] {
        &self.databases
    }

    fn current_database(&self) -> Option<&str> {
        self.current_database.as_deref()
    }

    fn databases_mut(&mut self) -> &mut Vec<DatabaseInfo> {
        &mut self.databases
    }

    fn clear_current_database(&mut self) {
        self.current_database = None;
        self.schemas.clear();
        self.tables.clear();
        self.views.clear();
    }

    fn remove_current_object(&mut self, target: &SchemaDropTarget) {
        if let Some(schema_name) = target.schema.as_deref() {
            if let Some(db_schema) = self
                .schemas
                .iter_mut()
                .find(|entry| entry.name == schema_name)
            {
                db_schema.remove_object(target);
            }
            return;
        }

        match target.kind {
            SchemaObjectKind::Table => {
                self.tables.retain(|entry| entry.name != target.name);
            }
            SchemaObjectKind::View => {
                self.views.retain(|entry| entry.name != target.name);
            }
            SchemaObjectKind::Collection | SchemaObjectKind::Database => {}
        }
    }
}

impl DatabaseScoped for DocumentSchema {
    fn databases(&self) -> &[DatabaseInfo] {
        &self.databases
    }

    fn current_database(&self) -> Option<&str> {
        self.current_database.as_deref()
    }

    fn databases_mut(&mut self) -> &mut Vec<DatabaseInfo> {
        &mut self.databases
    }

    fn clear_current_database(&mut self) {
        self.current_database = None;
        self.collections.clear();
    }

    fn remove_current_object(&mut self, target: &SchemaDropTarget) {
        if target.kind == SchemaObjectKind::Collection {
            self.collections.retain(|entry| entry.name != target.name);
        }
    }
}

impl DatabaseScoped for TimeSeriesSchema {
    fn databases(&self) -> &[DatabaseInfo] {
        &self.databases
    }

    fn current_database(&self) -> Option<&str> {
        self.current_database.as_deref()
    }

    fn databases_mut(&mut self) -> &mut Vec<DatabaseInfo> {
        &mut self.databases
    }

    fn clear_current_database(&mut self) {
        self.current_database = None;
        self.measurements.clear();
        self.retention_policies.clear();
    }
}

/// Schema for SQL/relational databases.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RelationalSchema {
    /// All databases on the server (PostgreSQL, MySQL) or empty (SQLite).
    pub databases: Vec<DatabaseInfo>,

    /// Name of the currently connected database.
    pub current_database: Option<String>,

    /// Schemas within the current database (PostgreSQL only).
    pub schemas: Vec<DbSchemaInfo>,

    /// Tables in the current schema (for databases without schema support).
    #[serde(default)]
    pub tables: Vec<TableInfo>,

    /// Views in the current schema (for databases without schema support).
    #[serde(default)]
    pub views: Vec<ViewInfo>,
}

/// Schema for document databases (MongoDB, CouchDB, etc.)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DocumentSchema {
    /// All databases on the server.
    pub databases: Vec<DatabaseInfo>,

    /// Name of the currently connected database.
    pub current_database: Option<String>,

    /// Collections in the current database.
    pub collections: Vec<CollectionInfo>,
}

/// Schema for key-value stores (Redis, Valkey, etc.)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KeyValueSchema {
    /// Key spaces (numbered databases in Redis).
    pub keyspaces: Vec<KeySpaceInfo>,

    /// Currently selected keyspace index.
    pub current_keyspace: Option<u32>,
}

/// Schema for time-series databases (InfluxDB, TimescaleDB, QuestDB)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimeSeriesSchema {
    pub databases: Vec<DatabaseInfo>,
    pub current_database: Option<String>,

    /// Measurements (InfluxDB) or hypertables (TimescaleDB).
    pub measurements: Vec<MeasurementInfo>,

    /// Retention policies.
    pub retention_policies: Vec<RetentionPolicyInfo>,
}

// =============================================================================
// Time-Series Schema Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementInfo {
    pub name: String,
    /// Tag keys (indexed dimensions).
    pub tags: Vec<String>,
    /// Field keys (values).
    pub fields: Vec<TimeSeriesFieldInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesFieldInfo {
    pub name: String,
    pub data_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicyInfo {
    pub name: String,
    pub duration: Option<String>,
    pub is_default: bool,
}

/// Complete schema snapshot returned by `Connection::schema()`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    /// The underlying schema structure, typed by database paradigm.
    pub structure: DataStructure,
}

impl SchemaSnapshot {
    /// Create a new relational schema snapshot.
    pub fn relational(schema: RelationalSchema) -> Self {
        Self {
            structure: DataStructure::Relational(schema),
        }
    }

    /// Create a new document schema snapshot.
    pub fn document(schema: DocumentSchema) -> Self {
        Self {
            structure: DataStructure::Document(schema),
        }
    }

    /// Create a new key-value schema snapshot.
    pub fn key_value(schema: KeyValueSchema) -> Self {
        Self {
            structure: DataStructure::KeyValue(schema),
        }
    }

    pub fn time_series(schema: TimeSeriesSchema) -> Self {
        Self {
            structure: DataStructure::TimeSeries(schema),
        }
    }

    // Type checking methods

    pub fn is_relational(&self) -> bool {
        matches!(self.structure, DataStructure::Relational(_))
    }

    pub fn is_document(&self) -> bool {
        matches!(self.structure, DataStructure::Document(_))
    }

    pub fn is_key_value(&self) -> bool {
        matches!(self.structure, DataStructure::KeyValue(_))
    }

    pub fn is_time_series(&self) -> bool {
        matches!(self.structure, DataStructure::TimeSeries(_))
    }

    // Schema accessors

    pub fn as_relational(&self) -> Option<&RelationalSchema> {
        match &self.structure {
            DataStructure::Relational(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_document(&self) -> Option<&DocumentSchema> {
        match &self.structure {
            DataStructure::Document(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_key_value(&self) -> Option<&KeyValueSchema> {
        match &self.structure {
            DataStructure::KeyValue(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_time_series(&self) -> Option<&TimeSeriesSchema> {
        match &self.structure {
            DataStructure::TimeSeries(s) => Some(s),
            _ => None,
        }
    }

    // Convenience accessors (backward compatibility)

    /// Get databases (for types that have them).
    pub fn databases(&self) -> &[DatabaseInfo] {
        self.structure
            .database_scoped()
            .map_or(&[], |schema| schema.databases())
    }

    /// Get current database name.
    pub fn current_database(&self) -> Option<&str> {
        self.structure
            .database_scoped()
            .and_then(|schema| schema.current_database())
    }

    /// Get schemas (relational only).
    pub fn schemas(&self) -> &[DbSchemaInfo] {
        match &self.structure {
            DataStructure::Relational(s) => &s.schemas,
            _ => &[],
        }
    }

    /// Get tables (relational only).
    pub fn tables(&self) -> &[TableInfo] {
        match &self.structure {
            DataStructure::Relational(s) => &s.tables,
            _ => &[],
        }
    }

    /// Get views (relational only).
    pub fn views(&self) -> &[ViewInfo] {
        match &self.structure {
            DataStructure::Relational(s) => &s.views,
            _ => &[],
        }
    }

    /// Get collections (document only).
    pub fn collections(&self) -> &[CollectionInfo] {
        match &self.structure {
            DataStructure::Document(s) => &s.collections,
            _ => &[],
        }
    }

    /// Get keyspaces (key-value only).
    pub fn keyspaces(&self) -> &[KeySpaceInfo] {
        match &self.structure {
            DataStructure::KeyValue(s) => &s.keyspaces,
            _ => &[],
        }
    }

    /// Get measurements (time-series only).
    pub fn measurements(&self) -> &[MeasurementInfo] {
        match &self.structure {
            DataStructure::TimeSeries(s) => &s.measurements,
            _ => &[],
        }
    }

    /// Drops `database` from the database list. When it is the current
    /// database, the objects loaded for it are cleared as well.
    pub fn remove_database(&mut self, database: &str) {
        let Some(schema) = self.structure.database_scoped_mut() else {
            return;
        };

        schema
            .databases_mut()
            .retain(|entry| entry.name != database);

        if schema.current_database() == Some(database) {
            schema.clear_current_database();
        }
    }

    /// Removes the object named by `target` when it belongs to the current
    /// database. Only relational and document snapshots hold droppable objects.
    pub fn remove_object(&mut self, target: &SchemaDropTarget) {
        let Some(schema) = self.structure.database_scoped_mut() else {
            return;
        };

        if Self::targets_current_database(target, schema.current_database()) {
            schema.remove_current_object(target);
        }
    }

    fn targets_current_database(target: &SchemaDropTarget, current_database: Option<&str>) -> bool {
        target
            .database
            .as_deref()
            .is_none_or(|database| current_database == Some(database))
    }
}

// =============================================================================
// Table Creation Metadata (faithful CREATE TABLE support)
// =============================================================================

/// Driver-reported details needed to regenerate a table's `CREATE TABLE`
/// statement faithfully — beyond what the legacy [`TableInfo`] shape carries.
///
/// This type is intentionally standalone: `TableInfo` is embedded in
/// postcard-encoded RPC payloads and persisted snapshot rows, so it cannot
/// gain fields without protocol and compatibility consequences. Creation
/// metadata travels beside it instead: in-process through the defaulted
/// `Connection::table_creation_metadata` seam, and persisted as a nullable
/// JSON column on deep snapshot table rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCreationMetadata {
    /// Schema that owns the table; `None` on engines without schemas.
    /// Together with `table` this forms the collision-safe identity used to
    /// associate metadata with its table — schema and name are kept as
    /// separate components, never concatenated into one key (a `"a.b"` name
    /// in schema `"c"` would be indistinguishable from `"b"` in `"c.a"`).
    #[serde(default)]
    pub schema: Option<String>,

    /// Table name this metadata describes.
    pub table: String,

    /// Whether the driver could observe every creation-relevant property.
    pub completeness: MetadataCompleteness,

    /// Identity (auto-increment) definition, when the table has one.
    #[serde(default)]
    pub identity: Option<IdentitySpec>,

    /// Explicit primary-key declaration, when present. Column order here is
    /// the declared key order, which the per-column `is_primary_key` flags on
    /// `TableInfo` cannot express.
    #[serde(default)]
    pub primary_key: Option<PrimaryKeySpec>,

    /// Conditions that currently make faithful `CREATE TABLE` generation
    /// impossible, even when the rest of the metadata is complete.
    #[serde(default)]
    pub blockers: Vec<CreationBlocker>,
}

/// How complete a [`TableCreationMetadata`] capture is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MetadataCompleteness {
    /// The driver observed every property faithful generation needs.
    Complete,

    /// The driver could not observe some properties; `missing` names them.
    Partial {
        /// Properties the driver could not observe.
        missing: Vec<MissingCreationMetadata>,
    },
}

/// A creation-relevant property a driver could not observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingCreationMetadata {
    /// Identity seed and/or increment were not observable.
    IdentitySeedOrIncrement,
    /// Primary-key column order was not observable.
    PrimaryKeyOrder,
    /// Column default expressions were not observable.
    ColumnDefaultExpressions,
    /// Engine storage options (filegroup, tablespace, and similar) were not
    /// observable.
    StorageOptions,
}

/// Identity (auto-increment) definition for a column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentitySpec {
    /// Column carrying the identity.
    pub column: String,

    /// Exact seed as a decimal string. SQL Server permits `numeric(38,0)`
    /// magnitudes that exceed 64-bit integers, so the value is kept as a
    /// string and must never be narrowed to a numeric type.
    pub seed: String,

    /// Exact increment as a decimal string, for the same reason as `seed`.
    pub increment: String,
}

/// Primary-key declaration with explicit column order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryKeySpec {
    /// Columns in declared key order; the order defines index/cluster
    /// position and must be preserved verbatim.
    pub columns: Vec<String>,
}

/// One condition that blocks faithful `CREATE TABLE` generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreationBlocker {
    /// Stable machine-readable reason (e.g. `"memory_optimized"`), so callers
    /// can branch on it without parsing `message`.
    pub code: String,

    /// Human-readable explanation of what blocks faithful generation.
    pub message: String,

    /// Column the blocker applies to, when it is column-specific.
    #[serde(default)]
    pub column: Option<String>,
}

/// Table metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,

    /// Schema name (PostgreSQL) or `None` (SQLite).
    pub schema: Option<String>,

    /// Column metadata. `None` = not yet loaded (lazy), `Some(vec)` = loaded.
    pub columns: Option<Vec<ColumnInfo>>,

    /// Names the engine resolves on this table in a query although `columns`
    /// does not list them, such as SQLite's `rowid` or PostgreSQL's `ctid`.
    ///
    /// A driver fills it in `table_details`, and only with names this table
    /// really has: a SQLite `WITHOUT ROWID` table gets no `rowid`. Empty when
    /// the driver declares none or the details are not loaded.
    ///
    /// Not serialized, so the driver RPC wire format is unchanged and tables
    /// of external RPC drivers always carry an empty list.
    #[serde(skip)]
    pub pseudo_columns: Box<[String]>,

    /// Index metadata. `None` = not yet loaded (lazy), `Some(data)` = loaded.
    pub indexes: Option<IndexData>,

    /// Foreign key metadata. `None` = not yet loaded (lazy), `Some(vec)` = loaded.
    #[serde(default)]
    pub foreign_keys: Option<Vec<ForeignKeyInfo>>,

    /// Constraint metadata (CHECK, UNIQUE). `None` = not yet loaded (lazy).
    #[serde(default)]
    pub constraints: Option<Vec<ConstraintInfo>>,

    /// Sampled document fields (document databases only).
    /// `None` = not loaded, `Some(vec)` = loaded via document sampling.
    #[serde(default)]
    pub sample_fields: Option<Vec<FieldInfo>>,

    /// Preferred UI presentation for opening this container.
    #[serde(default)]
    pub presentation: CollectionPresentation,

    /// Driver-provided child sources that should appear under this container.
    #[serde(default)]
    pub child_items: Option<Vec<CollectionChildInfo>>,

    /// Driver-specific storage metadata that has no dedicated core representation
    /// (e.g. Redshift distribution/sort keys, informational-only constraints).
    /// `None` = not applicable/not loaded; `Some(vec)` = loaded, possibly empty.
    #[serde(default)]
    pub storage_hints: Option<Vec<TableStorageHint>>,
}

/// A single driver-reported storage characteristic of a table that does not
/// map to an existing structured field (index, foreign key, constraint).
///
/// Used by drivers whose engine-specific storage concepts (distribution keys,
/// sort keys, advisory/non-enforced constraints) should still surface in the
/// UI without adding driver-specific branching to `dbflux_ui*` crates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStorageHint {
    /// Human-readable label, e.g. "Distribution Key", "Sort Key".
    pub label: String,

    /// Column names this hint applies to, if any.
    pub columns: Vec<String>,

    /// Free-form detail text, e.g. "KEY" or "compound".
    pub detail: Option<String>,
}

/// View metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewInfo {
    pub name: String,

    /// Schema name (PostgreSQL) or `None` (SQLite).
    pub schema: Option<String>,
}

/// Column metadata within a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,

    /// Database-specific type (e.g., "integer", "varchar(255)").
    pub type_name: String,

    pub nullable: bool,
    pub is_primary_key: bool,

    /// Default value expression, if any.
    pub default_value: Option<String>,

    /// For enum/set columns: the list of allowed values.
    /// Populated by drivers that support custom types (PostgreSQL enums,
    /// MySQL ENUM/SET).
    ///
    /// Always encoded. `postcard` has no representation for an absent struct
    /// field, so `skip_serializing_if` desynchronises the IPC frame even with
    /// `serde(default)` present.
    #[serde(default)]
    pub enum_values: Option<Vec<String>>,
}

/// Relational tables store [`IndexInfo`], document collections store
/// [`CollectionIndexInfo`] with direction, sparse, and TTL metadata.
// Externally tagged on purpose: an internally tagged enum cannot serialize a
// newtype variant holding a `Vec`, and postcard (driver RPC) cannot decode
// internally or adjacently tagged enums.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexData {
    Relational(Vec<IndexInfo>),
    Document(Vec<CollectionIndexInfo>),
}

/// Index metadata (relational databases).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,

    /// Column names included in the index.
    pub columns: Vec<String>,

    pub is_unique: bool,

    /// True if this is the primary key index.
    pub is_primary: bool,
}

/// Foreign key metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyInfo {
    pub name: String,

    /// Local column names.
    pub columns: Vec<String>,

    /// Referenced table name.
    pub referenced_table: String,

    /// Referenced schema (PostgreSQL).
    pub referenced_schema: Option<String>,

    /// Referenced column names.
    pub referenced_columns: Vec<String>,

    /// ON DELETE action (CASCADE, SET NULL, etc.).
    pub on_delete: Option<String>,

    /// ON UPDATE action.
    pub on_update: Option<String>,
}

/// Constraint type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintKind {
    Check,
    Unique,
    Exclusion,
}

/// Constraint metadata (CHECK, UNIQUE, EXCLUSION).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintInfo {
    pub name: String,
    pub kind: ConstraintKind,

    /// Columns involved (for UNIQUE/EXCLUSION).
    pub columns: Vec<String>,

    /// Check expression (for CHECK constraints).
    pub check_clause: Option<String>,
}

/// Custom type kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CustomTypeKind {
    Enum,
    Domain,
    Composite,
}

/// Custom type metadata (enum, domain, composite).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomTypeInfo {
    pub name: String,
    pub schema: Option<String>,
    pub kind: CustomTypeKind,

    /// Enum values (for Enum types).
    pub enum_values: Option<Vec<String>>,

    /// Base type name (for Domain types).
    pub base_type: Option<String>,
}

/// Schema-level index info (includes table name for display in schema tree).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaIndexInfo {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
}

/// Schema-level foreign key info (includes table name for display in schema tree).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaForeignKeyInfo {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub referenced_schema: Option<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

/// Schema-level column info (includes table name for bulk per-schema loads).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaColumnInfo {
    pub table_name: String,
    pub column: ColumnInfo,
}

/// Routine kind, mapped from engine metadata (e.g. PostgreSQL pg_proc.prokind).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutineKind {
    Function,
    Procedure,
    Aggregate,
    Window,
}

/// Schema-level routine info (stored procedures / user-defined functions).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutineInfo {
    pub name: String,
    pub kind: RoutineKind,
    /// Engine-specific unique identity within the schema; includes argument
    /// signature so overloads do not collide. PostgreSQL uses
    /// `pg_get_function_identity_arguments(oid)`.
    pub specific_name: String,
    pub parameter_types: Vec<String>,
    pub return_type_hint: Option<String>,
}

// =============================================================================
// Document Database Types (MongoDB, CouchDB, etc.)
// =============================================================================

/// Collection metadata for document databases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub name: String,

    /// Database name containing this collection.
    pub database: Option<String>,

    /// Estimated document count (may be approximate for performance).
    pub document_count: Option<u64>,

    /// Average document size in bytes.
    pub avg_document_size: Option<u64>,

    /// Sample fields discovered from documents. Document databases are schema-less,
    /// so this represents commonly occurring fields, not a fixed schema.
    #[serde(default)]
    pub sample_fields: Option<Vec<FieldInfo>>,

    /// Indexes on this collection.
    #[serde(default)]
    pub indexes: Option<Vec<CollectionIndexInfo>>,

    /// JSON Schema validator, if configured.
    #[serde(default)]
    pub validator: Option<String>,

    /// Whether the collection is capped (fixed-size).
    #[serde(default)]
    pub is_capped: bool,

    /// Preferred UI presentation for opening this collection.
    #[serde(default)]
    pub presentation: CollectionPresentation,

    /// Driver-provided child sources that should appear under this collection.
    #[serde(default)]
    pub child_items: Option<Vec<CollectionChildInfo>>,
}

/// Preferred UI presentation for document containers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CollectionPresentation {
    #[default]
    DataGrid,
    EventStream,
    ObjectBrowser,
}

/// Driver-provided child source metadata for a collection/container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionChildInfo {
    pub id: String,
    pub label: String,

    /// Optional driver-provided event timestamp used for generic child pickers.
    #[serde(default)]
    pub last_event_ts_ms: Option<i64>,

    #[serde(default)]
    pub presentation: CollectionPresentation,
}

/// Request for a page of driver-owned child sources under a collection/container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionChildrenRequest {
    pub collection: crate::CollectionRef,
    pub limit: u32,

    #[serde(default)]
    pub page_token: Option<String>,
}

/// One page of driver-owned child sources under a collection/container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionChildrenPage {
    pub items: Vec<CollectionChildInfo>,

    #[serde(default)]
    pub next_page_token: Option<String>,
}

/// Cached collection child list plus continuation token.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionChildrenCache {
    pub items: Vec<CollectionChildInfo>,

    #[serde(default)]
    pub next_page_token: Option<String>,
}

/// Field info discovered from document sampling.
///
/// Unlike SQL columns, document fields are dynamic. This represents
/// observed field patterns, not a guaranteed schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldInfo {
    /// Field name (can include dot notation for nested fields).
    pub name: String,

    /// Most common BSON/JSON type observed for this field.
    pub common_type: String,

    /// Percentage of documents containing this field (0.0-1.0).
    pub occurrence_rate: Option<f32>,

    /// Nested fields if this is an embedded document.
    #[serde(default)]
    pub nested_fields: Option<Vec<FieldInfo>>,
}

/// Index on a document collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionIndexInfo {
    pub name: String,

    /// Index key specification (field -> direction).
    /// Direction: 1 = ascending, -1 = descending, "text" = text index, etc.
    pub keys: Vec<(String, IndexDirection)>,

    pub is_unique: bool,

    /// Sparse index (only indexes documents that contain the field).
    #[serde(default)]
    pub is_sparse: bool,

    /// TTL index expiration in seconds.
    #[serde(default)]
    pub expire_after_seconds: Option<u64>,
}

/// Index direction for document database indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexDirection {
    Ascending,
    Descending,
    Text,
    Hashed,
    Geo2d,
    Geo2dSphere,
}

// =============================================================================
// Key-Value Database Types (Redis, Valkey, etc.)
// =============================================================================

/// Key space metadata for key-value databases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeySpaceInfo {
    /// Database index (Redis uses numbered databases 0-15 by default).
    pub db_index: u32,

    /// Number of keys in this database.
    pub key_count: Option<u64>,

    /// Memory usage in bytes.
    pub memory_bytes: Option<u64>,

    /// Average TTL in seconds (for keys with expiration).
    pub avg_ttl_seconds: Option<u64>,
}

/// Information about a specific key in a key-value store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyInfo {
    pub key: String,

    /// Value type (string, list, set, hash, zset, stream, etc.).
    pub value_type: KeyType,

    /// Time-to-live in seconds. None if no expiration.
    pub ttl_seconds: Option<i64>,

    /// Memory usage in bytes.
    pub memory_bytes: Option<u64>,

    /// Number of elements (for collections like list, set, hash).
    pub element_count: Option<u64>,
}

// =============================================================================
// Unified Container Abstraction
// =============================================================================

/// Unified container that can represent any database object type.
///
/// This allows the UI to work with tables, collections, and key spaces
/// through a common interface while preserving type-specific details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContainerInfo {
    /// SQL table with columns, indexes, and constraints.
    Table(TableInfo),

    /// SQL view.
    View(ViewInfo),

    /// Document collection (MongoDB, CouchDB).
    Collection(CollectionInfo),

    /// Key-value database info.
    KeySpace(KeySpaceInfo),
}

impl ContainerInfo {
    /// Returns the container name (owned for KeySpace since it's computed).
    pub fn name(&self) -> std::borrow::Cow<'_, str> {
        match self {
            Self::Table(t) => std::borrow::Cow::Borrowed(&t.name),
            Self::View(v) => std::borrow::Cow::Borrowed(&v.name),
            Self::Collection(c) => std::borrow::Cow::Borrowed(&c.name),
            Self::KeySpace(k) => std::borrow::Cow::Owned(format!("db{}", k.db_index)),
        }
    }

    pub fn is_table(&self) -> bool {
        matches!(self, Self::Table(_))
    }

    pub fn is_view(&self) -> bool {
        matches!(self, Self::View(_))
    }

    pub fn is_collection(&self) -> bool {
        matches!(self, Self::Collection(_))
    }

    pub fn is_key_space(&self) -> bool {
        matches!(self, Self::KeySpace(_))
    }

    pub fn as_table(&self) -> Option<&TableInfo> {
        match self {
            Self::Table(t) => Some(t),
            _ => None,
        }
    }

    pub fn as_collection(&self) -> Option<&CollectionInfo> {
        match self {
            Self::Collection(c) => Some(c),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measurements_returns_slice_for_time_series_schema() {
        let schema = SchemaSnapshot::time_series(TimeSeriesSchema {
            databases: Vec::new(),
            current_database: None,
            measurements: vec![
                MeasurementInfo {
                    name: "cpu".to_string(),
                    tags: vec!["host".to_string()],
                    fields: vec![],
                },
                MeasurementInfo {
                    name: "mem".to_string(),
                    tags: vec![],
                    fields: vec![],
                },
            ],
            retention_policies: Vec::new(),
        });

        let result = schema.measurements();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "cpu");
        assert_eq!(result[1].name, "mem");
    }

    #[test]
    fn measurements_returns_empty_for_non_time_series_schemas() {
        let relational = SchemaSnapshot::relational(RelationalSchema::default());
        assert!(relational.measurements().is_empty());

        let document = SchemaSnapshot::document(DocumentSchema::default());
        assert!(document.measurements().is_empty());

        let key_value = SchemaSnapshot::key_value(KeyValueSchema::default());
        assert!(key_value.measurements().is_empty());
    }

    #[test]
    fn routine_info_serde_roundtrip() {
        let original = RoutineInfo {
            name: "add".to_string(),
            kind: RoutineKind::Function,
            specific_name: "add(integer, integer)".to_string(),
            parameter_types: vec!["integer".to_string(), "integer".to_string()],
            return_type_hint: Some("integer".to_string()),
        };

        let json = serde_json::to_string(&original).expect("serialize");
        let decoded: RoutineInfo = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded.name, original.name);
        assert_eq!(decoded.kind, original.kind);
        assert_eq!(decoded.specific_name, original.specific_name);
        assert_eq!(decoded.parameter_types, original.parameter_types);
        assert_eq!(decoded.return_type_hint, original.return_type_hint);
    }

    #[test]
    fn routine_kind_serde_roundtrip_all_variants() {
        for kind in [
            RoutineKind::Function,
            RoutineKind::Procedure,
            RoutineKind::Aggregate,
            RoutineKind::Window,
        ] {
            let json = serde_json::to_string(&kind).expect("serialize");
            let decoded: RoutineKind = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(
                decoded, kind,
                "RoutineKind round-trip failed for {:?}",
                kind
            );
        }
    }

    #[test]
    fn collection_presentation_serde_roundtrip_all_variants() {
        for (variant, expected_json) in [
            (CollectionPresentation::DataGrid, "\"DataGrid\""),
            (CollectionPresentation::EventStream, "\"EventStream\""),
            (CollectionPresentation::ObjectBrowser, "\"ObjectBrowser\""),
        ] {
            let json = serde_json::to_string(&variant).expect("serialize");
            assert_eq!(json, expected_json);

            let decoded: CollectionPresentation = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(
                decoded, variant,
                "CollectionPresentation round-trip failed for {:?}",
                variant
            );
        }
    }

    #[test]
    fn collection_presentation_default_is_data_grid() {
        assert_eq!(
            CollectionPresentation::default(),
            CollectionPresentation::DataGrid
        );
    }

    fn minimal_table_info_json() -> serde_json::Value {
        serde_json::json!({
            "name": "orders",
            "schema": null,
            "columns": null,
            "indexes": null,
            "foreign_keys": null,
            "constraints": null,
            "sample_fields": null,
            "presentation": "DataGrid",
            "child_items": null,
        })
    }

    #[test]
    fn table_info_storage_hints_defaults_to_none_when_absent() {
        let json = minimal_table_info_json();
        let decoded: TableInfo = serde_json::from_value(json).expect("deserialize");
        assert!(decoded.storage_hints.is_none());
    }

    #[test]
    fn table_info_pseudo_columns_are_empty_unless_a_driver_sets_them() {
        let decoded: TableInfo =
            serde_json::from_value(minimal_table_info_json()).expect("deserialize");
        assert!(decoded.pseudo_columns.is_empty());

        let declared = TableInfo {
            pseudo_columns: Box::new(["rowid".to_string()]),
            ..decoded
        };
        let json = serde_json::to_value(&declared).expect("serialize");
        assert!(
            json.get("pseudo_columns").is_none(),
            "pseudo-columns are never serialized: {json}"
        );

        let redecoded: TableInfo = serde_json::from_value(json).expect("deserialize");
        assert!(redecoded.pseudo_columns.is_empty());
    }

    #[test]
    fn table_info_storage_hints_roundtrip_when_populated() {
        let mut json = minimal_table_info_json();
        json["storage_hints"] = serde_json::json!([
            { "label": "Distribution Key", "columns": ["customer_id"], "detail": "KEY" },
            { "label": "Sort Key", "columns": ["order_date"], "detail": "compound" },
        ]);

        let decoded: TableInfo = serde_json::from_value(json).expect("deserialize");
        let hints = decoded
            .storage_hints
            .as_ref()
            .expect("storage_hints populated");

        assert_eq!(hints.len(), 2);
        assert_eq!(hints[0].label, "Distribution Key");
        assert_eq!(hints[0].columns, vec!["customer_id".to_string()]);
        assert_eq!(hints[0].detail.as_deref(), Some("KEY"));
        assert_eq!(hints[1].label, "Sort Key");

        let json = serde_json::to_string(&decoded).expect("serialize");
        let redecoded: TableInfo = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(redecoded.storage_hints.expect("hints").len(), 2);
    }

    fn full_creation_metadata() -> TableCreationMetadata {
        TableCreationMetadata {
            schema: Some("sales".to_string()),
            table: "orders".to_string(),
            completeness: MetadataCompleteness::Complete,
            identity: Some(IdentitySpec {
                column: "id".to_string(),
                seed: "-99999999999999999999999999999999999999".to_string(),
                increment: "99999999999999999999999999999999999999".to_string(),
            }),
            primary_key: Some(PrimaryKeySpec {
                columns: vec!["tenant_id".to_string(), "id".to_string()],
            }),
            blockers: vec![CreationBlocker {
                code: "memory_optimized".to_string(),
                message: "MEMORY_OPTIMIZED tables have no faithful CREATE TABLE mapping"
                    .to_string(),
                column: None,
            }],
        }
    }

    #[test]
    fn table_creation_metadata_roundtrips_38_digit_identity_strings() {
        let original = full_creation_metadata();

        let json = serde_json::to_string(&original).expect("serialize");
        let decoded: TableCreationMetadata = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded, original);

        let identity = decoded.identity.expect("identity present");
        assert_eq!(
            identity.seed, "-99999999999999999999999999999999999999",
            "38-digit seed must survive as the exact decimal string"
        );
        assert_eq!(
            identity.increment, "99999999999999999999999999999999999999",
            "38-digit increment must survive as the exact decimal string"
        );
    }

    #[test]
    fn identity_seed_and_increment_serialize_as_json_strings() {
        let metadata = full_creation_metadata();

        let value = serde_json::to_value(&metadata).expect("to value");
        let seed = &value["identity"]["seed"];
        let increment = &value["identity"]["increment"];

        assert!(seed.is_string(), "seed must be a JSON string, got: {seed}");
        assert!(
            increment.is_string(),
            "increment must be a JSON string, got: {increment}"
        );
    }

    #[test]
    fn metadata_completeness_roundtrips_all_variants() {
        let complete = MetadataCompleteness::Complete;
        let partial = MetadataCompleteness::Partial {
            missing: vec![
                MissingCreationMetadata::IdentitySeedOrIncrement,
                MissingCreationMetadata::PrimaryKeyOrder,
                MissingCreationMetadata::ColumnDefaultExpressions,
                MissingCreationMetadata::StorageOptions,
            ],
        };

        for completeness in [complete, partial] {
            let json = serde_json::to_string(&completeness).expect("serialize");
            let decoded: MetadataCompleteness = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(decoded, completeness);
        }
    }

    #[test]
    fn table_creation_metadata_optional_fields_default_when_absent() {
        let json = serde_json::json!({
            "table": "plain",
            "completeness": { "state": "complete" },
        });

        let decoded: TableCreationMetadata = serde_json::from_value(json).expect("deserialize");
        assert_eq!(decoded.table, "plain");
        assert_eq!(decoded.schema, None);
        assert_eq!(decoded.identity, None);
        assert_eq!(decoded.primary_key, None);
        assert!(decoded.blockers.is_empty());
    }

    fn database(name: &str) -> DatabaseInfo {
        DatabaseInfo {
            name: name.to_string(),
            is_current: false,
        }
    }

    fn named_table(name: &str) -> TableInfo {
        let mut json = minimal_table_info_json();
        json["name"] = serde_json::Value::String(name.to_string());
        serde_json::from_value(json).expect("table info decodes")
    }

    fn named_view(name: &str) -> ViewInfo {
        ViewInfo {
            name: name.to_string(),
            schema: None,
        }
    }

    fn named_collection(name: &str) -> CollectionInfo {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "database": null,
            "document_count": null,
            "avg_document_size": null,
        }))
        .expect("collection info decodes")
    }

    fn database_names(snapshot: &SchemaSnapshot) -> Vec<&str> {
        snapshot
            .databases()
            .iter()
            .map(|entry| entry.name.as_str())
            .collect()
    }

    fn relational_snapshot() -> SchemaSnapshot {
        SchemaSnapshot::relational(RelationalSchema {
            databases: vec![database("app"), database("other")],
            current_database: Some("app".to_string()),
            schemas: vec![DbSchemaInfo {
                name: "public".to_string(),
                tables: vec![named_table("orders"), named_table("users")],
                views: vec![named_view("order_view"), named_view("user_view")],
                custom_types: None,
            }],
            tables: vec![named_table("orders"), named_table("users")],
            views: vec![named_view("order_view"), named_view("user_view")],
        })
    }

    fn document_snapshot() -> SchemaSnapshot {
        SchemaSnapshot::document(DocumentSchema {
            databases: vec![database("app"), database("other")],
            current_database: Some("app".to_string()),
            collections: vec![named_collection("orders"), named_collection("users")],
        })
    }

    fn time_series_snapshot() -> SchemaSnapshot {
        SchemaSnapshot::time_series(TimeSeriesSchema {
            databases: vec![database("app"), database("other")],
            current_database: Some("app".to_string()),
            measurements: vec![MeasurementInfo {
                name: "cpu".to_string(),
                tags: Vec::new(),
                fields: Vec::new(),
            }],
            retention_policies: vec![RetentionPolicyInfo {
                name: "autogen".to_string(),
                duration: None,
                is_default: true,
            }],
        })
    }

    fn key_value_snapshot() -> SchemaSnapshot {
        SchemaSnapshot::key_value(KeyValueSchema {
            keyspaces: vec![KeySpaceInfo {
                db_index: 0,
                key_count: Some(3),
                memory_bytes: None,
                avg_ttl_seconds: None,
            }],
            current_keyspace: Some(0),
        })
    }

    #[test]
    fn remove_current_database_clears_relational_lists() {
        let mut snapshot = relational_snapshot();
        snapshot.remove_database("app");

        assert_eq!(database_names(&snapshot), vec!["other"]);
        assert_eq!(snapshot.current_database(), None);
        assert!(snapshot.schemas().is_empty());
        assert!(snapshot.tables().is_empty());
        assert!(snapshot.views().is_empty());
    }

    #[test]
    fn remove_other_database_keeps_relational_lists() {
        let mut snapshot = relational_snapshot();
        snapshot.remove_database("other");

        assert_eq!(database_names(&snapshot), vec!["app"]);
        assert_eq!(snapshot.current_database(), Some("app"));
        assert_eq!(snapshot.schemas().len(), 1);
        assert_eq!(snapshot.tables().len(), 2);
        assert_eq!(snapshot.views().len(), 2);
    }

    #[test]
    fn remove_current_database_clears_document_collections() {
        let mut snapshot = document_snapshot();
        snapshot.remove_database("app");

        assert_eq!(database_names(&snapshot), vec!["other"]);
        assert_eq!(snapshot.current_database(), None);
        assert!(snapshot.collections().is_empty());
    }

    #[test]
    fn remove_other_database_keeps_document_collections() {
        let mut snapshot = document_snapshot();
        snapshot.remove_database("other");

        assert_eq!(database_names(&snapshot), vec!["app"]);
        assert_eq!(snapshot.current_database(), Some("app"));
        assert_eq!(snapshot.collections().len(), 2);
    }

    #[test]
    fn remove_current_database_clears_time_series_lists() {
        let mut snapshot = time_series_snapshot();
        snapshot.remove_database("app");

        assert_eq!(database_names(&snapshot), vec!["other"]);
        assert_eq!(snapshot.current_database(), None);
        let time_series = snapshot.as_time_series().expect("time-series snapshot");
        assert!(time_series.measurements.is_empty());
        assert!(time_series.retention_policies.is_empty());
    }

    #[test]
    fn remove_other_database_keeps_time_series_lists() {
        let mut snapshot = time_series_snapshot();
        snapshot.remove_database("other");

        assert_eq!(database_names(&snapshot), vec!["app"]);
        assert_eq!(snapshot.current_database(), Some("app"));
        let time_series = snapshot.as_time_series().expect("time-series snapshot");
        assert_eq!(time_series.measurements.len(), 1);
        assert_eq!(time_series.retention_policies.len(), 1);
    }

    #[test]
    fn remove_database_leaves_key_value_snapshot_unchanged() {
        let mut snapshot = key_value_snapshot();
        snapshot.remove_database("0");

        let key_value = snapshot.as_key_value().expect("key-value snapshot");
        assert_eq!(key_value.keyspaces.len(), 1);
        assert_eq!(key_value.current_keyspace, Some(0));
    }

    fn table_names(tables: &[TableInfo]) -> Vec<&str> {
        tables.iter().map(|entry| entry.name.as_str()).collect()
    }

    fn view_names(views: &[ViewInfo]) -> Vec<&str> {
        views.iter().map(|entry| entry.name.as_str()).collect()
    }

    #[test]
    fn remove_table_with_schema_removes_only_that_schema_table() {
        let mut snapshot = relational_snapshot();
        snapshot.remove_object(
            &SchemaDropTarget::new(SchemaObjectKind::Table, "orders").with_schema("public"),
        );

        assert_eq!(table_names(&snapshot.schemas()[0].tables), vec!["users"]);
        assert_eq!(snapshot.schemas()[0].views.len(), 2);
        assert_eq!(table_names(snapshot.tables()), vec!["orders", "users"]);
    }

    #[test]
    fn remove_view_with_schema_removes_only_that_schema_view() {
        let mut snapshot = relational_snapshot();
        snapshot.remove_object(
            &SchemaDropTarget::new(SchemaObjectKind::View, "order_view").with_schema("public"),
        );

        assert_eq!(view_names(&snapshot.schemas()[0].views), vec!["user_view"]);
        assert_eq!(snapshot.schemas()[0].tables.len(), 2);
        assert_eq!(snapshot.views().len(), 2);
    }

    #[test]
    fn remove_collection_with_schema_removes_relational_schema_table() {
        let mut snapshot = relational_snapshot();
        snapshot.remove_object(
            &SchemaDropTarget::new(SchemaObjectKind::Collection, "orders").with_schema("public"),
        );

        assert_eq!(table_names(&snapshot.schemas()[0].tables), vec!["users"]);
    }

    #[test]
    fn remove_table_and_view_without_schema_use_flat_lists() {
        let mut snapshot = relational_snapshot();
        snapshot.remove_object(&SchemaDropTarget::new(SchemaObjectKind::Table, "orders"));
        snapshot.remove_object(&SchemaDropTarget::new(SchemaObjectKind::View, "user_view"));
        snapshot.remove_object(&SchemaDropTarget::new(
            SchemaObjectKind::Collection,
            "users",
        ));

        assert_eq!(table_names(snapshot.tables()), vec!["users"]);
        assert_eq!(view_names(snapshot.views()), vec!["order_view"]);
        assert_eq!(snapshot.schemas()[0].tables.len(), 2);
    }

    #[test]
    fn remove_relational_object_of_other_database_is_ignored() {
        let mut snapshot = relational_snapshot();
        snapshot.remove_object(
            &SchemaDropTarget::new(SchemaObjectKind::Table, "orders").with_database("other"),
        );

        assert_eq!(snapshot.tables().len(), 2);
    }

    #[test]
    fn remove_collection_removes_only_named_document_collection() {
        let mut snapshot = document_snapshot();
        snapshot.remove_object(
            &SchemaDropTarget::new(SchemaObjectKind::Collection, "orders").with_database("app"),
        );

        let names: Vec<&str> = snapshot
            .collections()
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        assert_eq!(names, vec!["users"]);
    }

    #[test]
    fn remove_document_object_ignores_other_database_and_non_collection_kinds() {
        let mut snapshot = document_snapshot();
        snapshot.remove_object(
            &SchemaDropTarget::new(SchemaObjectKind::Collection, "orders").with_database("other"),
        );
        snapshot.remove_object(&SchemaDropTarget::new(SchemaObjectKind::Table, "orders"));

        assert_eq!(snapshot.collections().len(), 2);
    }

    #[test]
    fn remove_object_leaves_time_series_snapshots_unchanged() {
        let mut time_series = time_series_snapshot();
        time_series.remove_object(&SchemaDropTarget::new(SchemaObjectKind::Table, "cpu"));
        assert_eq!(time_series.measurements().len(), 1);
    }
}
