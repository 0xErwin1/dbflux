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

/// Complete schema snapshot returned by `Connection::schema()`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    /// All databases on the server (PostgreSQL) or empty (SQLite).
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
