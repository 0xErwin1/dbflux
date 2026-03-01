use bitflags::bitflags;
use serde::{Deserialize, Serialize};

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
            QueryLanguage::Custom(_) => "Enter query...",
        }
    }

    /// Returns the comment prefix for this query language.
    pub fn comment_prefix(&self) -> &'static str {
        match self {
            QueryLanguage::Sql | QueryLanguage::InfluxQuery | QueryLanguage::Cql => "--",
            QueryLanguage::MongoQuery | QueryLanguage::Cypher => "//",
            QueryLanguage::RedisCommands => "#",
            QueryLanguage::Custom(_) => "#",
        }
    }
}

/// Metadata that a driver provides about itself.
///
/// This is returned by `DbDriver::metadata()` and used by the UI
/// to configure behavior without knowing driver-specific details.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}
