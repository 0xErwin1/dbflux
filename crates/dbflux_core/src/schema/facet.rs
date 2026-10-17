//! Paradigm facets declared per schema object.
//!
//! A facet names one way an object can be viewed. One table can carry
//! several: a PostgreSQL table with a `vector` column is both tabular and a
//! vector set. Facets are computed on demand by
//! [`Connection::object_facets`](crate::Connection::object_facets) for tables
//! and [`Connection::collection_facets`](crate::Connection::collection_facets)
//! for collections, never stored in `TableInfo`, so the RPC wire and persisted
//! snapshots keep their shape.

use crate::{DatabaseCategory, TableRef, Value};

/// One paradigm an object exposes. Every variant has a view in the UI; a new
/// variant lands together with the driver that emits it and that view.
///
/// Deliberately exhaustive: it is never on a wire, and every match on it in
/// the UI must stop compiling until a new variant has a view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectFacet {
    /// Rows and columns in the data grid.
    Tabular,
    /// Nested documents in the document tree.
    Document,
    /// A column of embeddings that supports nearest-neighbor search.
    Vector(VectorFacet),
    /// Rows indexed by time: the data document opens the chart over the time
    /// column next to the rows.
    TimeSeries(TimeSeriesFacet),
}

/// The time axis of a time-series object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeSeriesFacet {
    /// The column rows are indexed by. `None` when the driver does not know
    /// it; the chart then takes the first timestamp column of the result.
    pub time_column: Option<String>,
}

/// A vector column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorFacet {
    pub column: String,
    /// Declared dimension, when the column type fixes one.
    pub dimension: Option<u32>,
}

/// Collection facets derived from a connection's declared category.
///
/// Only for drivers that cannot declare facets per collection: external
/// drivers on the RPC protocol, whose wire carries no facets. Every in-process
/// driver declares its own through
/// [`Connection::collection_facets`](crate::Connection::collection_facets).
pub fn category_collection_facets(category: DatabaseCategory) -> Vec<ObjectFacet> {
    match category {
        DatabaseCategory::Document => vec![ObjectFacet::Document, ObjectFacet::Tabular],
        DatabaseCategory::TimeSeries => vec![
            ObjectFacet::TimeSeries(TimeSeriesFacet { time_column: None }),
            ObjectFacet::Tabular,
        ],
        _ => vec![ObjectFacet::Tabular],
    }
}

/// Distance function of a nearest-neighbor search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorDistance {
    #[default]
    L2,
    Cosine,
    /// Negative inner product, so ascending order puts the closest first.
    InnerProduct,
}

impl VectorDistance {
    pub const ALL: [VectorDistance; 3] = [
        VectorDistance::L2,
        VectorDistance::Cosine,
        VectorDistance::InnerProduct,
    ];
}

/// Name of the column a nearest-neighbor query adds to every row, holding its
/// distance to the anchor. Drivers alias the computed distance to it so the
/// grid can find it without knowing the driver.
pub const NEAREST_DISTANCE_COLUMN: &str = "nearest_distance";

/// Rows of a table closest to an anchor vector, rendered by
/// [`QueryGenerator::generate_nearest_neighbors`](crate::QueryGenerator::generate_nearest_neighbors).
#[derive(Debug, Clone, PartialEq)]
pub struct NearestNeighborRequest {
    pub table: TableRef,
    pub database: Option<String>,
    pub column: String,
    /// The anchor vector, as the driver returned it.
    pub anchor: Value,
    pub distance: VectorDistance,
    pub limit: u32,
    /// Primary-key columns and values of the anchor row, excluded from the
    /// result. Empty when the anchor row should not be excluded.
    pub exclude_primary_key: Vec<(String, Value)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_document_category_declares_documents_and_a_grid() {
        assert_eq!(
            category_collection_facets(DatabaseCategory::Document),
            vec![ObjectFacet::Document, ObjectFacet::Tabular]
        );
    }

    #[test]
    fn a_time_series_category_declares_a_time_axis_it_does_not_know() {
        assert_eq!(
            category_collection_facets(DatabaseCategory::TimeSeries),
            vec![
                ObjectFacet::TimeSeries(TimeSeriesFacet { time_column: None }),
                ObjectFacet::Tabular
            ]
        );
    }

    #[test]
    fn other_categories_declare_a_grid() {
        for category in [
            DatabaseCategory::Relational,
            DatabaseCategory::KeyValue,
            DatabaseCategory::LogStream,
            DatabaseCategory::ObjectStorage,
        ] {
            assert_eq!(
                category_collection_facets(category),
                vec![ObjectFacet::Tabular],
                "{category:?}"
            );
        }
    }
}
