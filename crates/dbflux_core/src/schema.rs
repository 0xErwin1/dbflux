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

/// Unified schema structure for different database paradigms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataStructure {
    /// SQL databases (PostgreSQL, MySQL, SQLite, etc.)
    Relational(RelationalSchema),

    /// Document databases (MongoDB, CouchDB, etc.)
    Document(DocumentSchema),

    /// Key-value stores (Redis, Valkey, etc.)
    KeyValue(KeyValueSchema),

    /// Graph databases (Neo4j, Neptune, etc.)
    Graph(GraphSchema),

    /// Wide-column stores (Cassandra, HBase, ScyllaDB)
    WideColumn(WideColumnSchema),

    /// Time-series databases (InfluxDB, TimescaleDB, QuestDB)
    TimeSeries(TimeSeriesSchema),

    /// Search engines (Elasticsearch, OpenSearch, Meilisearch)
    Search(SearchSchema),

    /// Vector databases (Pinecone, Milvus, Qdrant, pgvector)
    Vector(VectorSchema),

    /// Multi-model databases (ArangoDB, SurrealDB, PostgreSQL+extensions)
    MultiModel(MultiModelSchema),
}

impl Default for DataStructure {
    fn default() -> Self {
        Self::Relational(RelationalSchema::default())
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

/// Schema for graph databases (Neo4j, Neptune, etc.)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphSchema {
    pub databases: Vec<DatabaseInfo>,
    pub current_database: Option<String>,

    /// Node labels in the graph.
    pub node_labels: Vec<NodeLabelInfo>,

    /// Relationship types in the graph.
    pub relationship_types: Vec<RelationshipTypeInfo>,

    /// Property keys used across nodes/relationships.
    pub property_keys: Vec<String>,
}

/// Schema for wide-column stores (Cassandra, HBase, ScyllaDB)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WideColumnSchema {
    /// Keyspaces (similar to databases).
    pub keyspaces: Vec<WideColumnKeyspaceInfo>,

    /// Currently selected keyspace.
    pub current_keyspace: Option<String>,
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

/// Schema for search engines (Elasticsearch, OpenSearch, Meilisearch)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchSchema {
    /// Search indices.
    pub indices: Vec<SearchIndexInfo>,

    /// Index templates.
    pub templates: Vec<String>,
}

/// Schema for vector databases (Pinecone, Milvus, Qdrant, pgvector)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VectorSchema {
    pub databases: Vec<DatabaseInfo>,
    pub current_database: Option<String>,

    /// Vector collections/indices.
    pub collections: Vec<VectorCollectionInfo>,
}

/// Schema for multi-model databases (ArangoDB, SurrealDB, PostgreSQL+extensions)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MultiModelSchema {
    pub databases: Vec<DatabaseInfo>,
    pub current_database: Option<String>,

    /// Supported paradigms in this multi-model database.
    pub capabilities: MultiModelCapabilities,

    /// Relational tables (if supported).
    pub tables: Vec<TableInfo>,

    /// Document collections (if supported).
    pub collections: Vec<CollectionInfo>,

    /// Graph structures (if supported).
    pub graphs: Vec<GraphInfo>,
}

// =============================================================================
// Graph Schema Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLabelInfo {
    pub name: String,
    pub count: Option<u64>,
    pub properties: Vec<PropertyInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipTypeInfo {
    pub name: String,
    pub count: Option<u64>,
    pub properties: Vec<PropertyInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyInfo {
    pub name: String,
    pub data_type: Option<String>,
}

// =============================================================================
// Wide-Column Schema Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WideColumnKeyspaceInfo {
    pub name: String,
    pub replication_strategy: Option<String>,
    pub column_families: Vec<ColumnFamilyInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnFamilyInfo {
    pub name: String,
    pub columns: Vec<WideColumnInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WideColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_partition_key: bool,
    pub is_clustering_key: bool,
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

// =============================================================================
// Search Schema Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchIndexInfo {
    pub name: String,
    pub doc_count: Option<u64>,
    pub mappings: Vec<SearchMappingInfo>,
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchMappingInfo {
    pub field: String,
    pub field_type: String,
    pub analyzer: Option<String>,
}

// =============================================================================
// Vector Schema Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorCollectionInfo {
    pub name: String,
    pub vector_count: Option<u64>,
    pub dimension: u32,
    pub metric: VectorMetric,
    pub metadata_fields: Vec<VectorMetadataField>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorMetric {
    #[default]
    Cosine,
    Euclidean,
    DotProduct,
    Manhattan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorMetadataField {
    pub name: String,
    pub data_type: String,
    pub indexed: bool,
}

// =============================================================================
// Multi-Model Schema Types
// =============================================================================

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MultiModelCapabilities {
    pub relational: bool,
    pub document: bool,
    pub graph: bool,
    pub key_value: bool,
    pub search: bool,
    pub vector: bool,
    pub time_series: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphInfo {
    pub name: String,
    pub vertex_collections: Vec<String>,
    pub edge_collections: Vec<String>,
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

    pub fn graph(schema: GraphSchema) -> Self {
        Self {
            structure: DataStructure::Graph(schema),
        }
    }

    pub fn wide_column(schema: WideColumnSchema) -> Self {
        Self {
            structure: DataStructure::WideColumn(schema),
        }
    }

    pub fn time_series(schema: TimeSeriesSchema) -> Self {
        Self {
            structure: DataStructure::TimeSeries(schema),
        }
    }

    pub fn search(schema: SearchSchema) -> Self {
        Self {
            structure: DataStructure::Search(schema),
        }
    }

    pub fn vector(schema: VectorSchema) -> Self {
        Self {
            structure: DataStructure::Vector(schema),
        }
    }

    pub fn multi_model(schema: MultiModelSchema) -> Self {
        Self {
            structure: DataStructure::MultiModel(schema),
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

    pub fn is_graph(&self) -> bool {
        matches!(self.structure, DataStructure::Graph(_))
    }

    pub fn is_wide_column(&self) -> bool {
        matches!(self.structure, DataStructure::WideColumn(_))
    }

    pub fn is_time_series(&self) -> bool {
        matches!(self.structure, DataStructure::TimeSeries(_))
    }

    pub fn is_search(&self) -> bool {
        matches!(self.structure, DataStructure::Search(_))
    }

    pub fn is_vector(&self) -> bool {
        matches!(self.structure, DataStructure::Vector(_))
    }

    pub fn is_multi_model(&self) -> bool {
        matches!(self.structure, DataStructure::MultiModel(_))
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

    pub fn as_graph(&self) -> Option<&GraphSchema> {
        match &self.structure {
            DataStructure::Graph(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_wide_column(&self) -> Option<&WideColumnSchema> {
        match &self.structure {
            DataStructure::WideColumn(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_time_series(&self) -> Option<&TimeSeriesSchema> {
        match &self.structure {
            DataStructure::TimeSeries(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_search(&self) -> Option<&SearchSchema> {
        match &self.structure {
            DataStructure::Search(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_vector(&self) -> Option<&VectorSchema> {
        match &self.structure {
            DataStructure::Vector(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_multi_model(&self) -> Option<&MultiModelSchema> {
        match &self.structure {
            DataStructure::MultiModel(s) => Some(s),
            _ => None,
        }
    }

    // Convenience accessors (backward compatibility)

    /// Get databases (for types that have them).
    pub fn databases(&self) -> &[DatabaseInfo] {
        match &self.structure {
            DataStructure::Relational(s) => &s.databases,
            DataStructure::Document(s) => &s.databases,
            DataStructure::Graph(s) => &s.databases,
            DataStructure::TimeSeries(s) => &s.databases,
            DataStructure::Vector(s) => &s.databases,
            DataStructure::MultiModel(s) => &s.databases,
            DataStructure::KeyValue(_)
            | DataStructure::WideColumn(_)
            | DataStructure::Search(_) => &[],
        }
    }

    /// Get current database name.
    pub fn current_database(&self) -> Option<&str> {
        match &self.structure {
            DataStructure::Relational(s) => s.current_database.as_deref(),
            DataStructure::Document(s) => s.current_database.as_deref(),
            DataStructure::Graph(s) => s.current_database.as_deref(),
            DataStructure::TimeSeries(s) => s.current_database.as_deref(),
            DataStructure::Vector(s) => s.current_database.as_deref(),
            DataStructure::MultiModel(s) => s.current_database.as_deref(),
            DataStructure::KeyValue(_)
            | DataStructure::WideColumn(_)
            | DataStructure::Search(_) => None,
        }
    }

    /// Get schemas (relational only).
    pub fn schemas(&self) -> &[DbSchemaInfo] {
        match &self.structure {
            DataStructure::Relational(s) => &s.schemas,
            _ => &[],
        }
    }

    /// Get tables (relational and multi-model).
    pub fn tables(&self) -> &[TableInfo] {
        match &self.structure {
            DataStructure::Relational(s) => &s.tables,
            DataStructure::MultiModel(s) => &s.tables,
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

    /// Get collections (document and multi-model).
    pub fn collections(&self) -> &[CollectionInfo] {
        match &self.structure {
            DataStructure::Document(s) => &s.collections,
            DataStructure::MultiModel(s) => &s.collections,
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
}

/// Table metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,

    /// Schema name (PostgreSQL) or `None` (SQLite).
    pub schema: Option<String>,

    /// Column metadata. `None` = not yet loaded (lazy), `Some(vec)` = loaded.
    pub columns: Option<Vec<ColumnInfo>>,

    /// Index metadata. `None` = not yet loaded (lazy), `Some(vec)` = loaded.
    pub indexes: Option<Vec<IndexInfo>>,

    /// Foreign key metadata. `None` = not yet loaded (lazy), `Some(vec)` = loaded.
    #[serde(default)]
    pub foreign_keys: Option<Vec<ForeignKeyInfo>>,

    /// Constraint metadata (CHECK, UNIQUE). `None` = not yet loaded (lazy).
    #[serde(default)]
    pub constraints: Option<Vec<ConstraintInfo>>,
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
}

/// Index metadata.
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
    pub value_type: KeyValueType,

    /// Time-to-live in seconds. None if no expiration.
    pub ttl_seconds: Option<i64>,

    /// Memory usage in bytes.
    pub memory_bytes: Option<u64>,

    /// Number of elements (for collections like list, set, hash).
    pub element_count: Option<u64>,
}

/// Redis/Valkey value types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyValueType {
    String,
    List,
    Set,
    SortedSet,
    Hash,
    Stream,
    Unknown,
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
