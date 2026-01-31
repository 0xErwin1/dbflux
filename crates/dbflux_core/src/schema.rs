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
