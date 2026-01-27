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
