use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use crate::sql::dialect::PlaceholderStyle;

// ============================================================================
// Pagination Styles
// ============================================================================

/// Style of pagination supported by the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PaginationStyle {
    /// OFFSET/LIMIT style pagination.
    Offset,
    /// Cursor-based pagination using a bookmark/timestamp.
    Cursor,
    /// Page token style pagination (DynamoDB, MongoDB cursor).
    PageToken,
}

// ============================================================================
// WHERE Clause Operators
// ============================================================================

/// Operators supported in WHERE clauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WhereOperator {
    // Comparison operators
    Eq,  // =
    Ne,  // != or <>
    Gt,  // >
    Gte, // >=
    Lt,  // <
    Lte, // <=

    // Pattern matching
    Like,
    ILike,
    Regex,
    Null,

    // Collection operators
    In,
    NotIn,
    Contains,
    Overlap,

    // Array operators
    ContainsAll,
    ContainsAny,
    Size,

    // Logical operators (used in query builder, not stored)
    And,
    Or,
    Not,
}

impl WhereOperator {
    /// Returns the operator symbol/representation for SQL dialects.
    pub fn sql_symbol(&self) -> &'static str {
        match self {
            WhereOperator::Eq => "=",
            WhereOperator::Ne => "<>",
            WhereOperator::Gt => ">",
            WhereOperator::Gte => ">=",
            WhereOperator::Lt => "<",
            WhereOperator::Lte => "<=",
            WhereOperator::Like => "LIKE",
            WhereOperator::ILike => "ILIKE",
            WhereOperator::Null => "IS NULL",
            WhereOperator::In => "IN",
            WhereOperator::NotIn => "NOT IN",
            WhereOperator::Contains => "@>",
            WhereOperator::Overlap => "&&",
            WhereOperator::And => "AND",
            WhereOperator::Or => "OR",
            WhereOperator::Not => "NOT",
            WhereOperator::ContainsAll => "CONTAINS ALL",
            WhereOperator::ContainsAny => "CONTAINS ANY",
            WhereOperator::Size => "@>",
            WhereOperator::Regex => "~",
        }
    }
}

// ============================================================================
// Isolation Levels
// ============================================================================

/// Database transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// Lowest isolation - allows dirty reads, non-repeatable reads, phantom reads.
    ReadUncommitted,
    /// Allows dirty reads prevented - values read may be changed before commit.
    ReadCommitted,
    /// Values read remain consistent within a transaction but new values can appear.
    RepeatableRead,
    /// Full isolation - transaction sees only changes committed before it began.
    Serializable,
    /// Snapshot isolation (PostgreSQL, SQL Server).
    Snapshot,
    /// No transaction support.
    None,
}

impl IsolationLevel {
    /// Returns the SQL standard name for this isolation level.
    pub fn sql_name(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
            IsolationLevel::Snapshot => "SNAPSHOT",
            IsolationLevel::None => "NONE",
        }
    }
}

/// Icon identifier for database brands and UI elements.
///
/// The actual icon paths are resolved by the UI layer.
/// This allows the core to reference icons without depending on asset paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Icon {
    // Database brands
    Postgres,
    Mysql,
    Mariadb,
    Sqlite,
    Mongodb,
    Redis,
    Dynamodb,

    // Generic database icon (fallback)
    Database,
}

/// High-level category of database.
///
/// Each category has fundamentally different data models, query languages,
/// and UI requirements. The UI adapts its behavior based on this category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DatabaseCategory {
    /// Relational databases with tables, rows, columns, and SQL.
    /// Examples: PostgreSQL, MySQL, SQLite, SQL Server, Oracle
    Relational,

    /// Document stores with collections and JSON/BSON documents.
    /// Examples: MongoDB, CouchDB, Firestore
    Document,

    /// Key-value stores with simple key-value pairs.
    /// Examples: Redis, Valkey, DynamoDB, etcd
    KeyValue,

    /// Graph databases with nodes, edges, and graph queries.
    /// Examples: Neo4j, ArangoDB, Amazon Neptune
    Graph,

    /// Time-series databases optimized for temporal data.
    /// Examples: InfluxDB, TimescaleDB, QuestDB
    TimeSeries,

    /// Wide-column stores with keyspaces and column families.
    /// Examples: Cassandra, ScyllaDB, HBase
    WideColumn,
}

impl DatabaseCategory {
    pub fn display_name(&self) -> &'static str {
        match self {
            DatabaseCategory::Relational => "Relational",
            DatabaseCategory::Document => "Document",
            DatabaseCategory::KeyValue => "Key-Value",
            DatabaseCategory::Graph => "Graph",
            DatabaseCategory::TimeSeries => "Time Series",
            DatabaseCategory::WideColumn => "Wide Column",
        }
    }

    /// Returns the name used for the primary data container in this category.
    /// Used in UI labels like "Tables", "Collections", "Keys", etc.
    pub fn container_name(&self) -> &'static str {
        match self {
            DatabaseCategory::Relational => "Tables",
            DatabaseCategory::Document => "Collections",
            DatabaseCategory::KeyValue => "Keys",
            DatabaseCategory::Graph => "Nodes",
            DatabaseCategory::TimeSeries => "Measurements",
            DatabaseCategory::WideColumn => "Tables",
        }
    }

    /// Returns the singular form of the container name.
    pub fn container_name_singular(&self) -> &'static str {
        match self {
            DatabaseCategory::Relational => "Table",
            DatabaseCategory::Document => "Collection",
            DatabaseCategory::KeyValue => "Key",
            DatabaseCategory::Graph => "Node",
            DatabaseCategory::TimeSeries => "Measurement",
            DatabaseCategory::WideColumn => "Table",
        }
    }

    /// Returns the name used for individual records in this category.
    pub fn record_name(&self) -> &'static str {
        match self {
            DatabaseCategory::Relational => "Rows",
            DatabaseCategory::Document => "Documents",
            DatabaseCategory::KeyValue => "Values",
            DatabaseCategory::Graph => "Nodes",
            DatabaseCategory::TimeSeries => "Points",
            DatabaseCategory::WideColumn => "Rows",
        }
    }

    /// Returns the singular form of the record name.
    pub fn record_name_singular(&self) -> &'static str {
        match self {
            DatabaseCategory::Relational => "Row",
            DatabaseCategory::Document => "Document",
            DatabaseCategory::KeyValue => "Value",
            DatabaseCategory::Graph => "Node",
            DatabaseCategory::TimeSeries => "Point",
            DatabaseCategory::WideColumn => "Row",
        }
    }

    /// Bitmask of capabilities the UI should display for this category.
    /// Capabilities outside the mask are hidden (e.g. KV flags on a
    /// relational driver).
    pub fn relevant_capabilities(&self) -> DriverCapabilities {
        let common = DriverCapabilities::from_bits_truncate(
            DriverCapabilities::MULTIPLE_DATABASES.bits()
                | DriverCapabilities::SSH_TUNNEL.bits()
                | DriverCapabilities::SSL.bits()
                | DriverCapabilities::AUTHENTICATION.bits()
                | DriverCapabilities::QUERY_CANCELLATION.bits()
                | DriverCapabilities::QUERY_TIMEOUT.bits()
                | DriverCapabilities::TRANSACTIONS.bits()
                | DriverCapabilities::PREPARED_STATEMENTS.bits()
                | DriverCapabilities::INSERT.bits()
                | DriverCapabilities::UPDATE.bits()
                | DriverCapabilities::DELETE.bits()
                | DriverCapabilities::PAGINATION.bits()
                | DriverCapabilities::SORTING.bits()
                | DriverCapabilities::FILTERING.bits()
                | DriverCapabilities::EXPORT_CSV.bits()
                | DriverCapabilities::EXPORT_JSON.bits()
                | DriverCapabilities::PUBSUB.bits(),
        );

        let category_specific = match self {
            DatabaseCategory::Relational
            | DatabaseCategory::TimeSeries
            | DatabaseCategory::WideColumn => DriverCapabilities::from_bits_truncate(
                DriverCapabilities::SCHEMAS.bits()
                    | DriverCapabilities::VIEWS.bits()
                    | DriverCapabilities::FOREIGN_KEYS.bits()
                    | DriverCapabilities::INDEXES.bits()
                    | DriverCapabilities::CHECK_CONSTRAINTS.bits()
                    | DriverCapabilities::UNIQUE_CONSTRAINTS.bits()
                    | DriverCapabilities::CUSTOM_TYPES.bits()
                    | DriverCapabilities::TRIGGERS.bits()
                    | DriverCapabilities::STORED_PROCEDURES.bits()
                    | DriverCapabilities::SEQUENCES.bits()
                    | DriverCapabilities::RETURNING.bits(),
            ),

            DatabaseCategory::Document => DriverCapabilities::from_bits_truncate(
                DriverCapabilities::INDEXES.bits()
                    | DriverCapabilities::NESTED_DOCUMENTS.bits()
                    | DriverCapabilities::ARRAYS.bits()
                    | DriverCapabilities::AGGREGATION.bits(),
            ),

            DatabaseCategory::KeyValue => DriverCapabilities::from_bits_truncate(
                DriverCapabilities::KV_SCAN.bits()
                    | DriverCapabilities::KV_GET.bits()
                    | DriverCapabilities::KV_SET.bits()
                    | DriverCapabilities::KV_DELETE.bits()
                    | DriverCapabilities::KV_EXISTS.bits()
                    | DriverCapabilities::KV_TTL.bits()
                    | DriverCapabilities::KV_KEY_TYPES.bits()
                    | DriverCapabilities::KV_VALUE_SIZE.bits()
                    | DriverCapabilities::KV_RENAME.bits()
                    | DriverCapabilities::KV_BULK_GET.bits()
                    | DriverCapabilities::KV_STREAM_RANGE.bits()
                    | DriverCapabilities::KV_STREAM_ADD.bits()
                    | DriverCapabilities::KV_STREAM_DELETE.bits(),
            ),

            DatabaseCategory::Graph => DriverCapabilities::from_bits_truncate(
                DriverCapabilities::GRAPH_TRAVERSAL.bits()
                    | DriverCapabilities::EDGE_PROPERTIES.bits(),
            ),
        };

        common.union(category_specific)
    }
}

bitflags! {
    /// Capabilities that a database driver may support.
    ///
    /// The UI queries these flags to determine which features to enable.
    /// Drivers declare their capabilities at registration time.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DriverCapabilities: u64 {
        // === Connection features ===

        /// Driver supports multiple databases per server.
        /// UI shows database list in sidebar.
        const MULTIPLE_DATABASES = 1 << 0;

        /// Driver supports schemas within databases (e.g., PostgreSQL).
        /// UI shows schema level in sidebar.
        const SCHEMAS = 1 << 1;

        /// Driver supports SSH tunneling.
        const SSH_TUNNEL = 1 << 2;

        /// Driver supports SSL/TLS connections.
        const SSL = 1 << 3;

        /// Driver requires authentication (password, token, etc.).
        /// False for file-based databases like SQLite.
        const AUTHENTICATION = 1 << 4;

        // === Query features ===

        /// Driver supports cancelling running queries.
        const QUERY_CANCELLATION = 1 << 5;

        /// Driver supports query timeouts.
        const QUERY_TIMEOUT = 1 << 6;

        /// Driver supports transactions.
        const TRANSACTIONS = 1 << 7;

        /// Driver supports prepared statements / parameterized queries.
        const PREPARED_STATEMENTS = 1 << 8;

        // === Schema features ===

        /// Driver supports views.
        const VIEWS = 1 << 9;

        /// Driver supports foreign keys.
        const FOREIGN_KEYS = 1 << 10;

        /// Driver supports indexes.
        const INDEXES = 1 << 11;

        /// Driver supports CHECK constraints.
        const CHECK_CONSTRAINTS = 1 << 12;

        /// Driver supports UNIQUE constraints.
        const UNIQUE_CONSTRAINTS = 1 << 13;

        /// Driver supports custom types (enums, domains, etc.).
        const CUSTOM_TYPES = 1 << 14;

        /// Driver supports triggers.
        const TRIGGERS = 1 << 15;

        /// Driver supports stored procedures / functions.
        const STORED_PROCEDURES = 1 << 16;

        /// Driver supports sequences.
        const SEQUENCES = 1 << 17;

        // === CRUD features ===

        /// Driver supports inserting records.
        const INSERT = 1 << 18;

        /// Driver supports updating records.
        const UPDATE = 1 << 19;

        /// Driver supports deleting records.
        const DELETE = 1 << 20;

        /// Driver supports RETURNING clause (get affected rows back).
        const RETURNING = 1 << 21;

        // === Data features ===

        /// Driver supports server-side pagination (LIMIT/OFFSET or equivalent).
        const PAGINATION = 1 << 22;

        /// Driver supports server-side sorting (ORDER BY or equivalent).
        const SORTING = 1 << 23;

        /// Driver supports server-side filtering (WHERE or equivalent).
        const FILTERING = 1 << 24;

        // === Export features ===

        /// Driver supports exporting data to CSV.
        const EXPORT_CSV = 1 << 25;

        /// Driver supports exporting data to JSON.
        const EXPORT_JSON = 1 << 26;

        // === Document-specific features ===

        /// Driver supports nested documents / embedded objects.
        const NESTED_DOCUMENTS = 1 << 27;

        /// Driver supports array fields.
        const ARRAYS = 1 << 28;

        /// Driver supports aggregation pipelines.
        const AGGREGATION = 1 << 29;

        // === Key-value specific features ===

        /// Driver supports cursor-based key scanning.
        const KV_SCAN = 1 << 30;

        /// Driver supports reading single key values.
        const KV_GET = 1 << 31;

        /// Driver supports writing key values.
        const KV_SET = 1 << 32;

        /// Driver supports deleting keys.
        const KV_DELETE = 1 << 33;

        /// Driver supports key existence checks.
        const KV_EXISTS = 1 << 34;

        /// Driver supports key TTL read/write operations.
        const KV_TTL = 1 << 35;

        /// Driver can report concrete key types.
        const KV_KEY_TYPES = 1 << 36;

        /// Driver can report key value size.
        const KV_VALUE_SIZE = 1 << 37;

        /// Driver supports key rename operations.
        const KV_RENAME = 1 << 38;

        /// Driver supports bulk key reads.
        const KV_BULK_GET = 1 << 39;

        /// Driver supports reading stream entries (XRANGE or equivalent).
        const KV_STREAM_RANGE = 1 << 40;

        /// Driver supports appending stream entries (XADD or equivalent).
        const KV_STREAM_ADD = 1 << 41;

        /// Driver supports deleting stream entries by ID (XDEL or equivalent).
        const KV_STREAM_DELETE = 1 << 42;

        /// Driver supports pub/sub.
        const PUBSUB = 1 << 43;

        // === Graph-specific features ===

        /// Driver supports graph traversal queries.
        const GRAPH_TRAVERSAL = 1 << 44;

        /// Driver supports edge properties.
        const EDGE_PROPERTIES = 1 << 45;

        /// Driver supports transactional DDL (DDL inside transactions with rollback).
        /// When true, the driver can execute DDL statements within a transaction,
        /// allowing dry-run schema changes via transaction rollback.
        const TRANSACTIONAL_DDL = 1 << 46;
    }
}

impl Serialize for DriverCapabilities {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.bits().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DriverCapabilities {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bits = u64::deserialize(deserializer)?;
        Ok(Self::from_bits(bits).unwrap_or_else(Self::empty))
    }
}

impl DriverCapabilities {
    /// Common capabilities for relational databases.
    pub const RELATIONAL_BASE: Self = Self::from_bits_truncate(
        Self::MULTIPLE_DATABASES.bits()
            | Self::QUERY_CANCELLATION.bits()
            | Self::TRANSACTIONS.bits()
            | Self::PREPARED_STATEMENTS.bits()
            | Self::VIEWS.bits()
            | Self::INDEXES.bits()
            | Self::INSERT.bits()
            | Self::UPDATE.bits()
            | Self::DELETE.bits()
            | Self::PAGINATION.bits()
            | Self::SORTING.bits()
            | Self::FILTERING.bits()
            | Self::EXPORT_CSV.bits()
            | Self::EXPORT_JSON.bits(),
    );

    /// Common capabilities for document databases.
    pub const DOCUMENT_BASE: Self = Self::from_bits_truncate(
        Self::MULTIPLE_DATABASES.bits()
            | Self::AUTHENTICATION.bits()
            | Self::NESTED_DOCUMENTS.bits()
            | Self::ARRAYS.bits()
            | Self::INSERT.bits()
            | Self::UPDATE.bits()
            | Self::DELETE.bits()
            | Self::PAGINATION.bits()
            | Self::SORTING.bits()
            | Self::FILTERING.bits()
            | Self::EXPORT_JSON.bits(),
    );

    /// Common capabilities for key-value databases.
    pub const KEYVALUE_BASE: Self = Self::from_bits_truncate(
        Self::AUTHENTICATION.bits()
            | Self::KV_SCAN.bits()
            | Self::KV_GET.bits()
            | Self::KV_SET.bits()
            | Self::KV_DELETE.bits()
            | Self::KV_EXISTS.bits()
            | Self::EXPORT_JSON.bits(),
    );
}

/// Query language supported by the driver.
///
/// Determines which editor mode to use and how to parse/validate queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryLanguage {
    /// Standard SQL (with dialect variations).
    Sql,

    /// MongoDB Query Language (find, aggregate, etc.).
    MongoQuery,

    /// Redis commands (GET, SET, SCAN, etc.).
    RedisCommands,

    /// Cypher query language (Neo4j).
    Cypher,

    /// InfluxQL or Flux for time-series.
    InfluxQuery,

    /// CQL (Cassandra Query Language).
    Cql,

    /// Lua scripts.
    Lua,

    /// Python scripts.
    Python,

    /// Bash/shell scripts.
    Bash,

    /// Custom or proprietary query language.
    Custom(String),
}

impl QueryLanguage {
    /// Infer the query language from a file path's extension.
    ///
    /// Returns `None` for unrecognised extensions so the caller can fall back
    /// to a connection-derived language or refuse to open the file.
    pub fn from_path(path: &std::path::Path) -> Option<Self> {
        let ext = path.extension()?.to_str()?.to_lowercase();
        match ext.as_str() {
            "sql" => Some(Self::Sql),
            "js" | "mongodb" => Some(Self::MongoQuery),
            "redis" | "red" => Some(Self::RedisCommands),
            "cypher" | "cyp" => Some(Self::Cypher),
            "influxql" | "flux" => Some(Self::InfluxQuery),
            "cql" => Some(Self::Cql),
            "lua" => Some(Self::Lua),
            "py" => Some(Self::Python),
            "sh" | "bash" => Some(Self::Bash),
            _ => None,
        }
    }

    /// Default file extension for "Save As" dialogs.
    pub fn default_extension(&self) -> &'static str {
        match self {
            Self::Sql | Self::Cql => "sql",
            Self::MongoQuery => "js",
            Self::RedisCommands => "redis",
            Self::Cypher => "cypher",
            Self::InfluxQuery => "influxql",
            Self::Lua => "lua",
            Self::Python => "py",
            Self::Bash => "sh",
            Self::Custom(_) => "txt",
        }
    }

    /// All recognized file extensions for Open dialogs.
    pub fn file_dialog_extensions(&self) -> &[&'static str] {
        match self {
            Self::Sql => &["sql"],
            Self::MongoQuery => &["js", "mongodb"],
            Self::RedisCommands => &["redis", "red"],
            Self::Cypher => &["cypher", "cyp"],
            Self::InfluxQuery => &["influxql", "flux"],
            Self::Cql => &["cql"],
            Self::Lua => &["lua"],
            Self::Python => &["py"],
            Self::Bash => &["sh", "bash"],
            Self::Custom(_) => &["txt"],
        }
    }

    pub fn display_name(&self) -> &str {
        match self {
            QueryLanguage::Sql => "SQL",
            QueryLanguage::MongoQuery => "MongoDB Query",
            QueryLanguage::RedisCommands => "Redis Commands",
            QueryLanguage::Cypher => "Cypher",
            QueryLanguage::InfluxQuery => "InfluxQL",
            QueryLanguage::Cql => "CQL",
            QueryLanguage::Lua => "Lua",
            QueryLanguage::Python => "Python",
            QueryLanguage::Bash => "Bash",
            QueryLanguage::Custom(name) => name,
        }
    }

    /// Returns the file extension commonly used for this query language.
    pub fn file_extension(&self) -> &'static str {
        match self {
            QueryLanguage::Sql => "sql",
            QueryLanguage::MongoQuery => "mongodb",
            QueryLanguage::RedisCommands => "redis",
            QueryLanguage::Cypher => "cypher",
            QueryLanguage::InfluxQuery => "influxql",
            QueryLanguage::Cql => "cql",
            QueryLanguage::Lua => "lua",
            QueryLanguage::Python => "py",
            QueryLanguage::Bash => "sh",
            QueryLanguage::Custom(_) => "txt",
        }
    }

    /// Returns the syntax highlighting mode for code editors.
    pub fn editor_mode(&self) -> &'static str {
        match self {
            QueryLanguage::Sql | QueryLanguage::Cql => "sql",
            QueryLanguage::MongoQuery => "javascript",
            QueryLanguage::RedisCommands => "plaintext",
            QueryLanguage::Cypher => "cypher",
            QueryLanguage::InfluxQuery => "sql",
            QueryLanguage::Lua => "lua",
            QueryLanguage::Python => "python",
            QueryLanguage::Bash => "bash",
            QueryLanguage::Custom(_) => "plaintext",
        }
    }

    /// Returns the placeholder text for the query editor.
    pub fn placeholder(&self) -> &'static str {
        match self {
            QueryLanguage::Sql => "-- Enter SQL here...",
            QueryLanguage::MongoQuery => "// db.collection.find({})",
            QueryLanguage::RedisCommands => "# Enter Redis command...",
            QueryLanguage::Cypher => "// Enter Cypher query...",
            QueryLanguage::InfluxQuery => "-- Enter InfluxQL...",
            QueryLanguage::Cql => "-- Enter CQL...",
            QueryLanguage::Lua => "-- Enter Lua script...",
            QueryLanguage::Python => "# Enter Python script...",
            QueryLanguage::Bash => "# Enter Bash script...",
            QueryLanguage::Custom(_) => "Enter query...",
        }
    }

    /// Returns the comment prefix for this query language.
    pub fn comment_prefix(&self) -> &'static str {
        match self {
            QueryLanguage::Sql | QueryLanguage::InfluxQuery | QueryLanguage::Cql => "--",
            QueryLanguage::MongoQuery | QueryLanguage::Cypher => "//",
            QueryLanguage::RedisCommands | QueryLanguage::Python | QueryLanguage::Bash => "#",
            QueryLanguage::Lua => "--",
            QueryLanguage::Custom(_) => "#",
        }
    }

    pub fn supports_connection_context(&self) -> bool {
        matches!(
            self,
            QueryLanguage::Sql
                | QueryLanguage::MongoQuery
                | QueryLanguage::RedisCommands
                | QueryLanguage::Cypher
                | QueryLanguage::InfluxQuery
                | QueryLanguage::Cql
        )
    }
}

// ============================================================================
// Syntax Info
// ============================================================================

/// SQL dialect-specific syntax information for a driver.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyntaxInfo {
    /// Character used to quote identifiers (e.g., `"` for PostgreSQL, `` ` `` for MySQL).
    pub identifier_quote: char,

    /// Character used to quote string literals (usually `'`).
    pub string_quote: char,

    /// Style of placeholders in parameterized queries.
    pub placeholder_style: PlaceholderStyle,

    /// Whether the driver supports schemas (namespace within database).
    pub supports_schemas: bool,

    /// Default schema name when not specified (e.g., "public" for PostgreSQL).
    pub default_schema: Option<String>,

    /// Whether identifier names are case-sensitive.
    pub case_sensitive_identifiers: bool,
}

impl Default for SyntaxInfo {
    fn default() -> Self {
        Self {
            identifier_quote: '"',
            string_quote: '\'',
            placeholder_style: PlaceholderStyle::QuestionMark,
            supports_schemas: false,
            default_schema: None,
            case_sensitive_identifiers: true,
        }
    }
}

impl SyntaxInfo {
    /// Returns a SyntaxInfo for ANSI SQL (PostgreSQL-compatible defaults).
    pub fn ansi() -> Self {
        Self {
            identifier_quote: '"',
            string_quote: '\'',
            placeholder_style: PlaceholderStyle::DollarNumber,
            supports_schemas: true,
            default_schema: Some("public".to_string()),
            case_sensitive_identifiers: true,
        }
    }

    /// Returns a SyntaxInfo for MySQL.
    pub fn mysql() -> Self {
        Self {
            identifier_quote: '`',
            string_quote: '\'',
            placeholder_style: PlaceholderStyle::QuestionMark,
            supports_schemas: false,
            default_schema: None,
            case_sensitive_identifiers: false,
        }
    }

    /// Returns a SyntaxInfo for SQLite.
    pub fn sqlite() -> Self {
        Self {
            identifier_quote: '"',
            string_quote: '\'',
            placeholder_style: PlaceholderStyle::QuestionMark,
            supports_schemas: false,
            default_schema: None,
            case_sensitive_identifiers: true,
        }
    }
}

// ============================================================================
// Query Capabilities
// ============================================================================

/// Query-related capabilities supported by a driver.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCapabilities {
    /// Supported pagination styles.
    pub pagination: Vec<PaginationStyle>,

    /// Supported WHERE operators.
    pub where_operators: Vec<WhereOperator>,

    /// Maximum number of parameters in a query (0 = unlimited).
    pub max_query_parameters: u32,

    /// Whether the driver supports ORDER BY.
    pub supports_order_by: bool,

    /// Maximum number of ORDER BY columns (0 = unlimited).
    pub max_order_by_columns: u32,

    /// Whether the driver supports GROUP BY.
    pub supports_group_by: bool,

    /// Maximum number of GROUP BY columns (0 = unlimited).
    pub max_group_by_columns: u32,

    /// Whether the driver supports HAVING.
    pub supports_having: bool,

    /// Whether the driver supports DISTINCT.
    pub supports_distinct: bool,

    /// Whether the driver supports LIMIT.
    pub supports_limit: bool,

    /// Whether the driver supports OFFSET.
    pub supports_offset: bool,

    /// Whether the driver supports JOINs (INNER, LEFT, RIGHT, FULL).
    pub supports_joins: bool,

    /// Whether the driver supports subqueries.
    pub supports_subqueries: bool,

    /// Whether the driver supports UNION.
    pub supports_union: bool,

    /// Whether the driver supports INTERSECT.
    pub supports_intersect: bool,

    /// Whether the driver supports EXCEPT.
    pub supports_except: bool,

    /// Whether the driver supports CASE expressions.
    pub supports_case_expressions: bool,

    /// Whether the driver supports window functions.
    pub supports_window_functions: bool,

    /// Whether the driver supports CTEs (WITH clause).
    pub supports_ctes: bool,

    /// Whether the driver supports EXPLAIN (query planning).
    pub supports_explain: bool,
}

impl Default for QueryCapabilities {
    fn default() -> Self {
        Self {
            pagination: vec![PaginationStyle::Offset],
            where_operators: vec![
                WhereOperator::Eq,
                WhereOperator::Ne,
                WhereOperator::Gt,
                WhereOperator::Gte,
                WhereOperator::Lt,
                WhereOperator::Lte,
                WhereOperator::Like,
                WhereOperator::In,
                WhereOperator::And,
                WhereOperator::Or,
                WhereOperator::Not,
            ],
            max_query_parameters: 0,
            supports_order_by: true,
            max_order_by_columns: 0,
            supports_group_by: true,
            max_group_by_columns: 0,
            supports_having: true,
            supports_distinct: true,
            supports_limit: true,
            supports_offset: true,
            supports_joins: true,
            supports_subqueries: true,
            supports_union: true,
            supports_intersect: true,
            supports_except: true,
            supports_case_expressions: true,
            supports_window_functions: true,
            supports_ctes: true,
            supports_explain: true,
        }
    }
}

impl QueryCapabilities {
    /// Returns QueryCapabilities for relational databases.
    pub fn relational() -> Self {
        Self::default()
    }

    /// Returns QueryCapabilities for MongoDB.
    pub fn mongodb() -> Self {
        Self {
            pagination: vec![PaginationStyle::Cursor, PaginationStyle::PageToken],
            where_operators: vec![
                WhereOperator::Eq,
                WhereOperator::Ne,
                WhereOperator::Gt,
                WhereOperator::Gte,
                WhereOperator::Lt,
                WhereOperator::Lte,
                WhereOperator::In,
                WhereOperator::NotIn,
                WhereOperator::And,
                WhereOperator::Or,
                WhereOperator::Not,
            ],
            supports_joins: false,
            supports_union: false,
            supports_intersect: false,
            supports_except: false,
            supports_ctes: false,
            ..Self::default()
        }
    }

    /// Returns QueryCapabilities for Redis.
    pub fn redis() -> Self {
        Self {
            pagination: vec![PaginationStyle::Cursor],
            where_operators: vec![],
            supports_order_by: false,
            supports_group_by: false,
            supports_having: false,
            supports_distinct: false,
            supports_limit: false,
            supports_offset: false,
            supports_joins: false,
            supports_subqueries: false,
            supports_union: false,
            supports_intersect: false,
            supports_except: false,
            supports_case_expressions: false,
            supports_window_functions: false,
            supports_ctes: false,
            supports_explain: false,
            ..Self::default()
        }
    }
}

// ============================================================================
// Mutation Capabilities
// ============================================================================

/// Mutation (INSERT/UPDATE/DELETE) capabilities supported by a driver.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationCapabilities {
    /// Whether INSERT is supported.
    pub supports_insert: bool,

    /// Whether UPDATE is supported.
    pub supports_update: bool,

    /// Whether DELETE is supported.
    pub supports_delete: bool,

    /// Whether UPSERT (INSERT ON CONFLICT) is supported.
    pub supports_upsert: bool,

    /// Whether RETURNING clause is supported in mutations.
    pub supports_returning: bool,

    /// Maximum number of VALUES in a single INSERT (0 = unlimited).
    pub max_insert_values: u32,

    /// Whether the driver supports batch operations.
    pub supports_batch: bool,

    /// Whether the driver supports bulk update.
    pub supports_bulk_update: bool,

    /// Whether the driver supports bulk delete.
    pub supports_bulk_delete: bool,
}

impl Default for MutationCapabilities {
    fn default() -> Self {
        Self {
            supports_insert: true,
            supports_update: true,
            supports_delete: true,
            supports_upsert: false,
            supports_returning: false,
            max_insert_values: 0,
            supports_batch: true,
            supports_bulk_update: true,
            supports_bulk_delete: true,
        }
    }
}

impl MutationCapabilities {
    /// Returns MutationCapabilities for PostgreSQL.
    pub fn postgresql() -> Self {
        Self {
            supports_upsert: true,
            supports_returning: true,
            ..Self::default()
        }
    }

    /// Returns MutationCapabilities for SQLite.
    pub fn sqlite() -> Self {
        Self {
            supports_upsert: true,
            supports_returning: true,
            ..Self::default()
        }
    }
}

// ============================================================================
// DDL Capabilities
// ============================================================================

/// DDL (Data Definition Language) capabilities supported by a driver.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdlCapabilities {
    /// Whether CREATE DATABASE is supported.
    pub supports_create_database: bool,

    /// Whether DROP DATABASE is supported.
    pub supports_drop_database: bool,

    /// Whether CREATE TABLE is supported.
    pub supports_create_table: bool,

    /// Whether DROP TABLE is supported.
    pub supports_drop_table: bool,

    /// Whether ALTER TABLE is supported.
    pub supports_alter_table: bool,

    /// Whether CREATE INDEX is supported.
    pub supports_create_index: bool,

    /// Whether DROP INDEX is supported.
    pub supports_drop_index: bool,

    /// Whether CREATE VIEW is supported.
    pub supports_create_view: bool,

    /// Whether DROP VIEW is supported.
    pub supports_drop_view: bool,

    /// Whether CREATE TRIGGER is supported.
    pub supports_create_trigger: bool,

    /// Whether DROP TRIGGER is supported.
    pub supports_drop_trigger: bool,

    /// Whether DDL statements can run inside a transaction.
    /// When false, DDL commits automatically and cannot be rolled back.
    pub transactional_ddl: bool,

    /// Whether ADD COLUMN is supported in ALTER TABLE.
    pub supports_add_column: bool,

    /// Whether DROP COLUMN is supported in ALTER TABLE.
    pub supports_drop_column: bool,

    /// Whether RENAME COLUMN is supported in ALTER TABLE.
    pub supports_rename_column: bool,

    /// Whether ALTER COLUMN is supported in ALTER TABLE.
    pub supports_alter_column: bool,

    /// Whether ADD CONSTRAINT is supported in ALTER TABLE.
    pub supports_add_constraint: bool,

    /// Whether DROP CONSTRAINT is supported in ALTER TABLE.
    pub supports_drop_constraint: bool,
}

impl Default for DdlCapabilities {
    fn default() -> Self {
        Self {
            supports_create_database: true,
            supports_drop_database: true,
            supports_create_table: true,
            supports_drop_table: true,
            supports_alter_table: true,
            supports_create_index: true,
            supports_drop_index: true,
            supports_create_view: true,
            supports_drop_view: true,
            supports_create_trigger: false,
            supports_drop_trigger: false,
            transactional_ddl: true,
            supports_add_column: true,
            supports_drop_column: true,
            supports_rename_column: true,
            supports_alter_column: true,
            supports_add_constraint: true,
            supports_drop_constraint: true,
        }
    }
}

impl DdlCapabilities {
    /// Returns DdlCapabilities for MySQL (non-transactional DDL).
    pub fn mysql() -> Self {
        Self {
            transactional_ddl: false,
            supports_rename_column: false,
            supports_drop_column: false,
            ..Self::default()
        }
    }

    /// Returns DdlCapabilities for SQLite (limited ALTER TABLE).
    pub fn sqlite() -> Self {
        Self {
            supports_alter_table: false,
            supports_add_column: true,
            supports_rename_column: true,
            supports_drop_column: false,
            supports_alter_column: false,
            supports_add_constraint: false,
            supports_drop_constraint: false,
            transactional_ddl: false,
            ..Self::default()
        }
    }
}

// ============================================================================
// Transaction Capabilities
// ============================================================================

/// Transaction capabilities supported by a driver.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionCapabilities {
    /// Whether transactions are supported.
    pub supports_transactions: bool,

    /// Supported isolation levels.
    pub supported_isolation_levels: Vec<IsolationLevel>,

    /// Default isolation level.
    pub default_isolation_level: Option<IsolationLevel>,

    /// Whether savepoints are supported within a transaction.
    pub supports_savepoints: bool,

    /// Whether nested transactions (savepoints) are supported.
    pub supports_nested_transactions: bool,

    /// Whether the driver supports READ ONLY transactions.
    pub supports_read_only: bool,

    /// Whether the driver supports deferrable transactions.
    pub supports_deferrable: bool,
}

impl Default for TransactionCapabilities {
    fn default() -> Self {
        Self {
            supports_transactions: true,
            supported_isolation_levels: vec![
                IsolationLevel::ReadCommitted,
                IsolationLevel::Serializable,
            ],
            default_isolation_level: Some(IsolationLevel::ReadCommitted),
            supports_savepoints: true,
            supports_nested_transactions: true,
            supports_read_only: true,
            supports_deferrable: false,
        }
    }
}

impl TransactionCapabilities {
    /// Returns TransactionCapabilities for SQLite.
    pub fn sqlite() -> Self {
        Self {
            supported_isolation_levels: vec![IsolationLevel::ReadCommitted],
            default_isolation_level: Some(IsolationLevel::ReadCommitted),
            supports_nested_transactions: false,
            supports_deferrable: true,
            ..Self::default()
        }
    }

    /// Returns TransactionCapabilities for drivers without transaction support.
    pub fn none() -> Self {
        Self {
            supports_transactions: false,
            supported_isolation_levels: vec![],
            default_isolation_level: None,
            supports_savepoints: false,
            supports_nested_transactions: false,
            supports_read_only: false,
            supports_deferrable: false,
        }
    }
}

// ============================================================================
// Driver Limits
// ============================================================================

/// Operational limits for a driver.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverLimits {
    /// Maximum length of a query string in bytes (0 = unlimited).
    pub max_query_length: u64,

    /// Maximum number of parameters in a query (0 = unlimited).
    pub max_parameters: u32,

    /// Maximum number of rows in a result set (0 = unlimited).
    pub max_result_rows: u64,

    /// Maximum number of connections in a pool (0 = use driver default).
    pub max_connections: u32,

    /// Maximum depth of nested subqueries (0 = unlimited).
    pub max_nested_subqueries: u32,

    /// Maximum length of an identifier name in bytes.
    pub max_identifier_length: u32,

    /// Maximum number of columns in a table (0 = unlimited).
    pub max_columns: u32,

    /// Maximum number of indexes per table (0 = unlimited).
    pub max_indexes_per_table: u32,
}

impl Default for DriverLimits {
    fn default() -> Self {
        Self {
            max_query_length: 0,
            max_parameters: 0,
            max_result_rows: 0,
            max_connections: 0,
            max_nested_subqueries: 16,
            max_identifier_length: 63,
            max_columns: 0,
            max_indexes_per_table: 0,
        }
    }
}

impl DriverLimits {
    /// Returns DriverLimits for PostgreSQL.
    pub fn postgresql() -> Self {
        Self {
            max_parameters: 32767,
            max_identifier_length: 63,
            max_columns: 250,
            max_indexes_per_table: 32,
            ..Self::default()
        }
    }

    /// Returns DriverLimits for MySQL.
    pub fn mysql() -> Self {
        Self {
            max_parameters: 65535,
            max_identifier_length: 64,
            max_columns: 4096,
            max_indexes_per_table: 64,
            ..Self::default()
        }
    }

    /// Returns DriverLimits for SQLite.
    pub fn sqlite() -> Self {
        Self {
            max_query_length: 1_000_000_000,
            max_parameters: 32766,
            max_identifier_length: 100_000,
            max_columns: 32766,
            max_indexes_per_table: 64,
            ..Self::default()
        }
    }
}

// ============================================================================
// Operation Classifier
// ============================================================================

/// Trait for classifying database operations by their impact level.
///
/// Used by the governance system to determine if an operation requires
/// approval before execution. Drivers can provide custom classifiers
/// via `DriverMetadata::classification_override`.
pub trait OperationClassifier: Send + Sync {
    /// Classify a query string by its likely impact.
    ///
    /// Returns `ExecutionClassification` based on the query content.
    fn classify(&self, query: &str) -> ExecutionClassification;

    /// Get classification for a specific tool/operation.
    fn classify_tool(&self, tool_id: &str) -> ExecutionClassification;
}

// We import ExecutionClassification from the policy crate
// This is defined here to avoid circular dependencies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutionClassification {
    /// Metadata operations (schema introspection).
    Metadata,
    /// Read operations (SELECT queries).
    Read,
    /// Write operations (INSERT, UPDATE).
    Write,
    /// Destructive operations (DELETE without WHERE).
    Destructive,
    /// Safe DDL operations (CREATE TABLE, CREATE INDEX).
    AdminSafe,
    /// DDL operations requiring caution (DROP COLUMN, ALTER TABLE).
    Admin,
    /// Irreversible DDL operations (DROP TABLE, DROP DATABASE, TRUNCATE).
    AdminDestructive,
}

/// Metadata that a driver provides about itself.
///
/// This is returned by `DbDriver::metadata()` and used by the UI
/// to configure behavior without knowing driver-specific details.
#[derive(Serialize, Deserialize)]
pub struct DriverMetadata {
    /// Unique identifier for this driver (e.g., "postgres", "mongodb").
    pub id: String,

    /// Human-readable name (e.g., "PostgreSQL", "MongoDB").
    pub display_name: String,

    /// Short description shown in the connection manager.
    pub description: String,

    /// Database category (Relational, Document, etc.).
    pub category: DatabaseCategory,

    /// Query language used by this database.
    pub query_language: QueryLanguage,

    /// Capabilities supported by this driver.
    pub capabilities: DriverCapabilities,

    /// Default port for network connections (None for file-based).
    pub default_port: Option<u16>,

    /// URI scheme for connection strings (e.g., "postgresql", "mongodb").
    pub uri_scheme: String,

    /// Icon identifier for this driver.
    /// The UI resolves this to the actual asset path.
    pub icon: Icon,

    // === New capability fields (Phase 1) ===
    /// SQL syntax information (quoting, placeholders, etc.).
    pub syntax: Option<SyntaxInfo>,

    /// Query capabilities (pagination, operators, etc.).
    pub query: Option<QueryCapabilities>,

    /// Mutation capabilities (INSERT, UPDATE, DELETE, RETURNING).
    pub mutation: Option<MutationCapabilities>,

    /// DDL capabilities (CREATE, ALTER, DROP, transactional DDL).
    pub ddl: Option<DdlCapabilities>,

    /// Transaction capabilities (isolation levels, savepoints).
    pub transactions: Option<TransactionCapabilities>,

    /// Operational limits (max params, max rows, etc.).
    pub limits: Option<DriverLimits>,

    /// Custom operation classifier override.
    /// When None, uses the default classifier from the governance service.
    /// Note: Not serialized - must be re-established on deserialization.
    #[serde(skip)]
    pub classification_override: Option<Box<dyn OperationClassifier>>,
}

impl Debug for DriverMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverMetadata")
            .field("id", &self.id)
            .field("display_name", &self.display_name)
            .field("description", &self.description)
            .field("category", &self.category)
            .field("query_language", &self.query_language)
            .field("capabilities", &self.capabilities)
            .field("default_port", &self.default_port)
            .field("uri_scheme", &self.uri_scheme)
            .field("icon", &self.icon)
            .field("syntax", &self.syntax)
            .field("query", &self.query)
            .field("mutation", &self.mutation)
            .field("ddl", &self.ddl)
            .field("transactions", &self.transactions)
            .field("limits", &self.limits)
            .field("classification_override", &"...")
            .finish()
    }
}

impl Clone for DriverMetadata {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            display_name: self.display_name.clone(),
            description: self.description.clone(),
            category: self.category,
            query_language: self.query_language.clone(),
            capabilities: self.capabilities,
            default_port: self.default_port,
            uri_scheme: self.uri_scheme.clone(),
            icon: self.icon,
            syntax: self.syntax.clone(),
            query: self.query.clone(),
            mutation: self.mutation.clone(),
            ddl: self.ddl.clone(),
            transactions: self.transactions.clone(),
            limits: self.limits.clone(),
            // Note: classification_override is not cloned as it requires
            // concrete type knowledge. Use None after clone.
            classification_override: None,
        }
    }
}

impl DriverMetadata {
    /// Check if a capability is supported.
    pub fn supports(&self, capability: DriverCapabilities) -> bool {
        self.capabilities.contains(capability)
    }

    /// Check if this is a relational database.
    pub fn is_relational(&self) -> bool {
        self.category == DatabaseCategory::Relational
    }

    /// Check if this is a document database.
    pub fn is_document(&self) -> bool {
        self.category == DatabaseCategory::Document
    }

    /// Check if this is a key-value database.
    pub fn is_key_value(&self) -> bool {
        self.category == DatabaseCategory::KeyValue
    }
}

// ============================================================================
// Driver Metadata Builder
// ============================================================================

/// Builder for constructing DriverMetadata with a fluent API.
pub struct DriverMetadataBuilder {
    id: String,
    display_name: String,
    description: String,
    category: DatabaseCategory,
    query_language: QueryLanguage,
    capabilities: DriverCapabilities,
    default_port: Option<u16>,
    uri_scheme: String,
    icon: Icon,
    syntax: Option<SyntaxInfo>,
    query_caps: Option<QueryCapabilities>,
    mutation_caps: Option<MutationCapabilities>,
    ddl_caps: Option<DdlCapabilities>,
    transaction_caps: Option<TransactionCapabilities>,
    limits: Option<DriverLimits>,
    classification_override: Option<Box<dyn OperationClassifier>>,
}

impl DriverMetadataBuilder {
    /// Create a new builder with required fields.
    pub fn new(
        id: impl Into<String>,
        display_name: impl Into<String>,
        category: DatabaseCategory,
        query_language: QueryLanguage,
    ) -> Self {
        Self {
            id: id.into(),
            display_name: display_name.into(),
            description: String::new(),
            category,
            query_language,
            capabilities: DriverCapabilities::empty(),
            default_port: None,
            uri_scheme: String::new(),
            icon: Icon::Database,
            syntax: None,
            query_caps: None,
            mutation_caps: None,
            ddl_caps: None,
            transaction_caps: None,
            limits: None,
            classification_override: None,
        }
    }

    /// Set the description.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Set the capabilities.
    pub fn capabilities(mut self, capabilities: DriverCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Set the default port.
    pub fn default_port(mut self, port: u16) -> Self {
        self.default_port = Some(port);
        self
    }

    /// Set the URI scheme.
    pub fn uri_scheme(mut self, scheme: impl Into<String>) -> Self {
        self.uri_scheme = scheme.into();
        self
    }

    /// Set the icon.
    pub fn icon(mut self, icon: Icon) -> Self {
        self.icon = icon;
        self
    }

    /// Set the syntax info.
    pub fn syntax(mut self, syntax: SyntaxInfo) -> Self {
        self.syntax = Some(syntax);
        self
    }

    /// Set the query capabilities.
    pub fn query(mut self, query: QueryCapabilities) -> Self {
        self.query_caps = Some(query);
        self
    }

    /// Set the mutation capabilities.
    pub fn mutation(mut self, mutation: MutationCapabilities) -> Self {
        self.mutation_caps = Some(mutation);
        self
    }

    /// Set the DDL capabilities.
    pub fn ddl(mut self, ddl: DdlCapabilities) -> Self {
        self.ddl_caps = Some(ddl);
        self
    }

    /// Set the transaction capabilities.
    pub fn transactions(mut self, transactions: TransactionCapabilities) -> Self {
        self.transaction_caps = Some(transactions);
        self
    }

    /// Set the driver limits.
    pub fn limits(mut self, limits: DriverLimits) -> Self {
        self.limits = Some(limits);
        self
    }

    /// Set the classification override.
    pub fn classification_override(
        mut self,
        classifier: impl OperationClassifier + 'static,
    ) -> Self {
        self.classification_override = Some(Box::new(classifier));
        self
    }

    /// Build the DriverMetadata.
    pub fn build(self) -> DriverMetadata {
        DriverMetadata {
            id: self.id,
            display_name: self.display_name,
            description: self.description,
            category: self.category,
            query_language: self.query_language,
            capabilities: self.capabilities,
            default_port: self.default_port,
            uri_scheme: self.uri_scheme,
            icon: self.icon,
            syntax: self.syntax,
            query: self.query_caps,
            mutation: self.mutation_caps,
            ddl: self.ddl_caps,
            transactions: self.transaction_caps,
            limits: self.limits,
            classification_override: self.classification_override,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_category_names() {
        assert_eq!(DatabaseCategory::Relational.container_name(), "Tables");
        assert_eq!(DatabaseCategory::Document.container_name(), "Collections");
        assert_eq!(DatabaseCategory::KeyValue.container_name(), "Keys");

        assert_eq!(DatabaseCategory::Relational.record_name(), "Rows");
        assert_eq!(DatabaseCategory::Document.record_name(), "Documents");
    }

    #[test]
    fn test_relational_base_capabilities() {
        let caps = DriverCapabilities::RELATIONAL_BASE;

        assert!(caps.contains(DriverCapabilities::MULTIPLE_DATABASES));
        assert!(caps.contains(DriverCapabilities::TRANSACTIONS));
        assert!(caps.contains(DriverCapabilities::PAGINATION));
        assert!(!caps.contains(DriverCapabilities::KV_TTL));
    }

    #[test]
    fn test_document_base_capabilities() {
        let caps = DriverCapabilities::DOCUMENT_BASE;

        assert!(caps.contains(DriverCapabilities::NESTED_DOCUMENTS));
        assert!(caps.contains(DriverCapabilities::ARRAYS));
        assert!(!caps.contains(DriverCapabilities::SCHEMAS));
    }

    #[test]
    fn test_keyvalue_base_capabilities() {
        let caps = DriverCapabilities::KEYVALUE_BASE;

        assert!(caps.contains(DriverCapabilities::KV_SCAN));
        assert!(caps.contains(DriverCapabilities::KV_GET));
        assert!(caps.contains(DriverCapabilities::KV_SET));
        assert!(caps.contains(DriverCapabilities::KV_DELETE));
        assert!(caps.contains(DriverCapabilities::KV_EXISTS));
        assert!(!caps.contains(DriverCapabilities::TRANSACTIONS));
    }

    #[test]
    fn test_query_language_display() {
        assert_eq!(QueryLanguage::Sql.display_name(), "SQL");
        assert_eq!(QueryLanguage::MongoQuery.display_name(), "MongoDB Query");
        assert_eq!(
            QueryLanguage::RedisCommands.display_name(),
            "Redis Commands"
        );
    }

    #[test]
    fn test_relevant_capabilities_excludes_cross_category() {
        let relational = DatabaseCategory::Relational.relevant_capabilities();
        assert!(relational.contains(DriverCapabilities::SCHEMAS));
        assert!(relational.contains(DriverCapabilities::FOREIGN_KEYS));
        assert!(!relational.contains(DriverCapabilities::KV_SCAN));
        assert!(!relational.contains(DriverCapabilities::NESTED_DOCUMENTS));
        assert!(!relational.contains(DriverCapabilities::GRAPH_TRAVERSAL));

        let document = DatabaseCategory::Document.relevant_capabilities();
        assert!(document.contains(DriverCapabilities::NESTED_DOCUMENTS));
        assert!(document.contains(DriverCapabilities::AGGREGATION));
        assert!(!document.contains(DriverCapabilities::KV_SCAN));
        assert!(!document.contains(DriverCapabilities::SCHEMAS));

        let kv = DatabaseCategory::KeyValue.relevant_capabilities();
        assert!(kv.contains(DriverCapabilities::KV_SCAN));
        assert!(kv.contains(DriverCapabilities::KV_TTL));
        assert!(!kv.contains(DriverCapabilities::SCHEMAS));
        assert!(!kv.contains(DriverCapabilities::NESTED_DOCUMENTS));

        let graph = DatabaseCategory::Graph.relevant_capabilities();
        assert!(graph.contains(DriverCapabilities::GRAPH_TRAVERSAL));
        assert!(!graph.contains(DriverCapabilities::KV_SCAN));
        assert!(!graph.contains(DriverCapabilities::SCHEMAS));
    }

    #[test]
    fn test_relevant_capabilities_includes_common() {
        for category in [
            DatabaseCategory::Relational,
            DatabaseCategory::Document,
            DatabaseCategory::KeyValue,
            DatabaseCategory::Graph,
        ] {
            let relevant = category.relevant_capabilities();
            assert!(
                relevant.contains(DriverCapabilities::SSH_TUNNEL),
                "{:?} should include SSH_TUNNEL",
                category
            );
            assert!(
                relevant.contains(DriverCapabilities::AUTHENTICATION),
                "{:?} should include AUTHENTICATION",
                category
            );
            assert!(
                relevant.contains(DriverCapabilities::EXPORT_JSON),
                "{:?} should include EXPORT_JSON",
                category
            );
        }
    }

    // =========================================================================
    // Phase 1: Core Types Tests
    // =========================================================================

    #[test]
    fn test_pagination_style_variants() {
        assert!(matches!(PaginationStyle::Offset, PaginationStyle::Offset));
        assert!(matches!(PaginationStyle::Cursor, PaginationStyle::Cursor));
        assert!(matches!(
            PaginationStyle::PageToken,
            PaginationStyle::PageToken
        ));
    }

    #[test]
    fn test_where_operator_sql_symbol() {
        assert_eq!(WhereOperator::Eq.sql_symbol(), "=");
        assert_eq!(WhereOperator::Ne.sql_symbol(), "<>");
        assert_eq!(WhereOperator::Like.sql_symbol(), "LIKE");
        assert_eq!(WhereOperator::Null.sql_symbol(), "IS NULL");
        assert_eq!(WhereOperator::In.sql_symbol(), "IN");
        assert_eq!(WhereOperator::And.sql_symbol(), "AND");
        assert_eq!(WhereOperator::Or.sql_symbol(), "OR");
    }

    #[test]
    fn test_isolation_level_sql_name() {
        assert_eq!(IsolationLevel::ReadCommitted.sql_name(), "READ COMMITTED");
        assert_eq!(IsolationLevel::Serializable.sql_name(), "SERIALIZABLE");
        assert_eq!(IsolationLevel::Snapshot.sql_name(), "SNAPSHOT");
        assert_eq!(IsolationLevel::None.sql_name(), "NONE");
    }

    #[test]
    fn test_syntax_info_default() {
        let syntax = SyntaxInfo::default();
        assert_eq!(syntax.identifier_quote, '"');
        assert_eq!(syntax.string_quote, '\'');
        assert!(!syntax.supports_schemas);
        assert!(syntax.case_sensitive_identifiers);
    }

    #[test]
    fn test_syntax_info_presets() {
        let pg = SyntaxInfo::ansi();
        assert_eq!(pg.identifier_quote, '"');
        assert!(pg.supports_schemas);
        assert_eq!(pg.default_schema, Some("public".to_string()));

        let mysql = SyntaxInfo::mysql();
        assert_eq!(mysql.identifier_quote, '`');
        assert!(!mysql.supports_schemas);
    }

    #[test]
    fn test_query_capabilities_default() {
        let qc = QueryCapabilities::default();
        assert!(qc.supports_order_by);
        assert!(qc.supports_group_by);
        assert!(qc.supports_limit);
        assert!(qc.supports_offset);
        assert!(qc.supports_joins);
        assert!(qc.supports_subqueries);
        assert!(qc.supports_ctes);
    }

    #[test]
    fn test_query_capabilities_mongodb() {
        let mongodb = QueryCapabilities::mongodb();
        assert!(!mongodb.supports_joins);
        assert!(!mongodb.supports_union);
        assert!(!mongodb.supports_ctes);
        assert!(mongodb.pagination.contains(&PaginationStyle::Cursor));
    }

    #[test]
    fn test_mutation_capabilities_default() {
        let mc = MutationCapabilities::default();
        assert!(mc.supports_insert);
        assert!(mc.supports_update);
        assert!(mc.supports_delete);
        assert!(!mc.supports_upsert);
    }

    #[test]
    fn test_mutation_capabilities_postgresql() {
        let pg = MutationCapabilities::postgresql();
        assert!(pg.supports_upsert);
        assert!(pg.supports_returning);
    }

    #[test]
    fn test_ddl_capabilities_default() {
        let dc = DdlCapabilities::default();
        assert!(dc.supports_create_table);
        assert!(dc.supports_drop_table);
        assert!(dc.supports_alter_table);
        assert!(dc.transactional_ddl);
    }

    #[test]
    fn test_ddl_capabilities_mysql() {
        let mysql = DdlCapabilities::mysql();
        assert!(!mysql.transactional_ddl);
        assert!(!mysql.supports_drop_column);
    }

    #[test]
    fn test_transaction_capabilities_default() {
        let tc = TransactionCapabilities::default();
        assert!(tc.supports_transactions);
        assert!(tc.supports_savepoints);
        assert!(tc.supports_read_only);
        assert!(tc.default_isolation_level.is_some());
    }

    #[test]
    fn test_driver_limits_default() {
        let limits = DriverLimits::default();
        assert_eq!(limits.max_identifier_length, 63);
        assert_eq!(limits.max_nested_subqueries, 16);
    }

    #[test]
    fn test_driver_limits_postgresql() {
        let pg = DriverLimits::postgresql();
        assert_eq!(pg.max_parameters, 32767);
        assert_eq!(pg.max_columns, 250);
    }

    #[test]
    fn test_driver_metadata_builder() {
        let metadata = DriverMetadataBuilder::new(
            "postgres",
            "PostgreSQL",
            DatabaseCategory::Relational,
            QueryLanguage::Sql,
        )
        .description("Test PostgreSQL driver")
        .default_port(5432)
        .uri_scheme("postgresql")
        .syntax(SyntaxInfo::ansi())
        .query(QueryCapabilities::relational())
        .mutation(MutationCapabilities::postgresql())
        .ddl(DdlCapabilities::default())
        .transactions(TransactionCapabilities::default())
        .limits(DriverLimits::postgresql())
        .build();

        assert_eq!(metadata.id, "postgres");
        assert_eq!(metadata.display_name, "PostgreSQL");
        assert_eq!(metadata.description, "Test PostgreSQL driver");
        assert_eq!(metadata.default_port, Some(5432));
        assert!(metadata.syntax.is_some());
        assert!(metadata.query.is_some());
        assert!(metadata.mutation.is_some());
        assert!(metadata.ddl.is_some());
        assert!(metadata.transactions.is_some());
        assert!(metadata.limits.is_some());
    }

    #[test]
    fn test_driver_metadata_clone_preserves_basic_fields() {
        let metadata = DriverMetadataBuilder::new(
            "test",
            "Test",
            DatabaseCategory::Relational,
            QueryLanguage::Sql,
        )
        .build();

        let cloned = metadata.clone();
        assert_eq!(cloned.id, metadata.id);
        assert_eq!(cloned.display_name, metadata.display_name);
        // Note: classification_override is None after clone
        assert!(cloned.classification_override.is_none());
    }

    #[test]
    fn test_execution_classification_variants() {
        assert!(matches!(
            ExecutionClassification::Metadata,
            ExecutionClassification::Metadata
        ));
        assert!(matches!(
            ExecutionClassification::Read,
            ExecutionClassification::Read
        ));
        assert!(matches!(
            ExecutionClassification::Write,
            ExecutionClassification::Write
        ));
        assert!(matches!(
            ExecutionClassification::AdminSafe,
            ExecutionClassification::AdminSafe
        ));
        assert!(matches!(
            ExecutionClassification::Admin,
            ExecutionClassification::Admin
        ));
        assert!(matches!(
            ExecutionClassification::AdminDestructive,
            ExecutionClassification::AdminDestructive
        ));
    }
}
