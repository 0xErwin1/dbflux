use serde::{Deserialize, Serialize};

use crate::DataStructure;

/// Kind of data view to use for displaying query results.
///
/// Mirrors the database paradigm from `DataStructure` but simplified
/// for UI rendering decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DataViewKind {
    /// Tabular data with rows and columns (SQL, wide-column, time-series).
    #[default]
    Tabular,

    /// Document/JSON tree view (MongoDB, CouchDB, Elasticsearch).
    Document,

    /// Key-value list view (Redis, Valkey).
    KeyValue,

    /// Graph visualization with nodes and edges (Neo4j, Neptune).
    Graph,

    /// Time-series chart view (InfluxDB, TimescaleDB).
    TimeSeries,

    /// Vector/embedding visualization (Pinecone, Milvus).
    Vector,
}

impl DataViewKind {
    pub fn is_tabular(&self) -> bool {
        matches!(self, Self::Tabular)
    }

    pub fn is_document(&self) -> bool {
        matches!(self, Self::Document)
    }

    pub fn is_key_value(&self) -> bool {
        matches!(self, Self::KeyValue)
    }

    pub fn is_graph(&self) -> bool {
        matches!(self, Self::Graph)
    }

    pub fn is_time_series(&self) -> bool {
        matches!(self, Self::TimeSeries)
    }

    pub fn is_vector(&self) -> bool {
        matches!(self, Self::Vector)
    }
}

impl From<&DataStructure> for DataViewKind {
    fn from(structure: &DataStructure) -> Self {
        match structure {
            DataStructure::Relational(_) => Self::Tabular,
            DataStructure::Document(_) => Self::Document,
            DataStructure::KeyValue(_) => Self::KeyValue,
            DataStructure::Graph(_) => Self::Graph,
            DataStructure::WideColumn(_) => Self::Tabular,
            DataStructure::TimeSeries(_) => Self::TimeSeries,
            DataStructure::Search(_) => Self::Document,
            DataStructure::Vector(_) => Self::Vector,
            DataStructure::MultiModel(_) => Self::Tabular, // Default, can be overridden
        }
    }
}
