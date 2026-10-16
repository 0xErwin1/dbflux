//! Paradigm facets declared per schema object.
//!
//! A facet names one way an object can be viewed. One table can carry
//! several: a PostgreSQL table with a `vector` column is both tabular and a
//! vector set. Facets are computed on demand by
//! [`Connection::object_facets`](crate::Connection::object_facets), never
//! stored in `TableInfo`, so the RPC wire and persisted snapshots keep their
//! shape.

use crate::{DatabaseCategory, TableRef, Value};

/// One paradigm an object exposes. Every variant has a view in the UI; a new
/// variant lands together with the driver that emits it and that view.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ObjectFacet {
    /// Rows and columns in the data grid.
    Tabular,
    /// Nested documents in the document tree.
    Document,
    /// A column of embeddings that supports nearest-neighbor search.
    Vector(VectorFacet),
}

/// A vector column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorFacet {
    pub column: String,
    /// Declared dimension, when the column type fixes one.
    pub dimension: Option<u32>,
}

/// Facets of a collection, derived from the category of its connection.
///
/// A document database's collection opens in the document tree and can also
/// be shown as a grid; every other collection holds flat rows.
pub fn default_collection_facets(category: Option<DatabaseCategory>) -> Vec<ObjectFacet> {
    match category {
        Some(DatabaseCategory::Document) => vec![ObjectFacet::Document, ObjectFacet::Tabular],
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
    fn document_collections_open_as_documents_and_can_show_a_grid() {
        assert_eq!(
            default_collection_facets(Some(DatabaseCategory::Document)),
            vec![ObjectFacet::Document, ObjectFacet::Tabular]
        );
    }

    #[test]
    fn other_collections_are_tabular() {
        for category in [
            None,
            Some(DatabaseCategory::Relational),
            Some(DatabaseCategory::KeyValue),
            Some(DatabaseCategory::Graph),
            Some(DatabaseCategory::TimeSeries),
            Some(DatabaseCategory::WideColumn),
        ] {
            assert_eq!(
                default_collection_facets(category),
                vec![ObjectFacet::Tabular],
                "{category:?}"
            );
        }
    }
}
