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

        /// Driver supports key expiration (TTL).
        const KEY_EXPIRATION = 1 << 30;

        /// Driver supports key patterns / wildcards.
        const KEY_PATTERNS = 1 << 31;

        /// Driver supports pub/sub.
        const PUBSUB = 1 << 32;

        // === Graph-specific features ===

        /// Driver supports graph traversal queries.
        const GRAPH_TRAVERSAL = 1 << 33;

        /// Driver supports edge properties.
        const EDGE_PROPERTIES = 1 << 34;
    }
}

impl DriverCapabilities {
    /// Common capabilities for relational databases.
    pub const RELATIONAL_BASE: Self = Self::from_bits_truncate(
        Self::MULTIPLE_DATABASES.bits()
            | Self::QUERY_CANCELLATION.bits()
            | Self::QUERY_TIMEOUT.bits()
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
            | Self::KEY_EXPIRATION.bits()
            | Self::KEY_PATTERNS.bits()
            | Self::INSERT.bits()
            | Self::UPDATE.bits()
            | Self::DELETE.bits()
            | Self::EXPORT_JSON.bits(),
    );
}

/// Query language supported by the driver.
///
/// Determines which editor mode to use and how to parse/validate queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    Custom(&'static str),
}

impl QueryLanguage {
    pub fn display_name(&self) -> &'static str {
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
}

/// Static metadata that a driver provides about itself.
///
/// This is returned by `DbDriver::metadata()` and used by the UI
/// to configure behavior without knowing driver-specific details.
#[derive(Debug, Clone)]
pub struct DriverMetadata {
    /// Unique identifier for this driver (e.g., "postgres", "mongodb").
    pub id: &'static str,

    /// Human-readable name (e.g., "PostgreSQL", "MongoDB").
    pub display_name: &'static str,

    /// Short description shown in the connection manager.
    pub description: &'static str,

    /// Database category (Relational, Document, etc.).
    pub category: DatabaseCategory,

    /// Query language used by this database.
    pub query_language: QueryLanguage,

    /// Capabilities supported by this driver.
    pub capabilities: DriverCapabilities,

    /// Default port for network connections (None for file-based).
    pub default_port: Option<u16>,

    /// URI scheme for connection strings (e.g., "postgresql", "mongodb").
    pub uri_scheme: &'static str,

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
        assert!(!caps.contains(DriverCapabilities::KEY_EXPIRATION));
    }

    #[test]
    fn test_document_base_capabilities() {
        let caps = DriverCapabilities::DOCUMENT_BASE;

        assert!(caps.contains(DriverCapabilities::NESTED_DOCUMENTS));
        assert!(caps.contains(DriverCapabilities::ARRAYS));
        assert!(!caps.contains(DriverCapabilities::SCHEMAS));
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
}
