use super::data_grid_panel::DataSource;
use dbflux_core::{ObjectFacet, QueryResultShape};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DataViewMode {
    #[default]
    Table,
    /// Collapsible document tree.
    Document,
    /// The page as editable JSON (document collections only).
    Json,
}

impl DataViewMode {
    pub fn recommended_for(source: &DataSource) -> Self {
        match source {
            DataSource::Table { .. } => DataViewMode::Table,
            DataSource::Collection { .. } => DataViewMode::Table,
            DataSource::QueryResult { result, .. } => {
                if result.shape == QueryResultShape::Json {
                    DataViewMode::Document
                } else {
                    DataViewMode::Table
                }
            }
        }
    }

    /// The view a facet opens in, or `None` for a facet that is an action on
    /// the grid rather than a view of its own (a vector column).
    pub fn for_facet(facet: &ObjectFacet) -> Option<Self> {
        match facet {
            ObjectFacet::Tabular => Some(DataViewMode::Table),
            ObjectFacet::Document => Some(DataViewMode::Document),
            ObjectFacet::Vector(_) => None,
            // The time axis opens the chart next to the rows (a result view,
            // see `result_view::result_view_mode_for_fresh_result`), not a data view.
            ObjectFacet::TimeSeries(_) => None,
        }
    }

    /// Views of an object with `facets`, in facet order, without repeats.
    pub fn for_facets(facets: &[ObjectFacet]) -> Vec<Self> {
        let mut modes = Vec::new();
        for mode in facets.iter().filter_map(Self::for_facet) {
            if !modes.contains(&mode) {
                modes.push(mode);
            }
        }
        modes
    }

    pub fn available_for(source: &DataSource) -> Vec<Self> {
        match source {
            DataSource::Table { .. } => Self::for_facets(&[ObjectFacet::Tabular]),
            // Any collection can be toggled to the document tree; the category
            // only decides which view it opens in (`view_config_for`).
            DataSource::Collection { .. } => {
                Self::for_facets(&[ObjectFacet::Document, ObjectFacet::Tabular])
            }
            DataSource::QueryResult { result, .. } => {
                if result.shape == QueryResultShape::Json {
                    vec![DataViewMode::Table, DataViewMode::Document]
                } else {
                    vec![DataViewMode::Table]
                }
            }
        }
    }

    pub fn label(&self) -> String {
        match self {
            DataViewMode::Table => dbflux_i18n::t!("document.data.grid.mode.table"),
            DataViewMode::Document => dbflux_i18n::t!("document.data.grid.mode.document"),
            DataViewMode::Json => dbflux_i18n::t!("document.collection.view.json"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataViewConfig {
    pub mode: DataViewMode,
}

impl Default for DataViewConfig {
    fn default() -> Self {
        Self {
            mode: DataViewMode::Table,
        }
    }
}

impl DataViewConfig {
    pub fn for_source(source: &DataSource) -> Self {
        Self {
            mode: DataViewMode::recommended_for(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DataViewMode;
    use dbflux_core::{ObjectFacet, VectorFacet};

    #[test]
    fn facets_map_to_views_in_order_and_vectors_add_none() {
        assert_eq!(
            DataViewMode::for_facets(&[
                ObjectFacet::Tabular,
                ObjectFacet::Vector(VectorFacet {
                    column: "embedding".to_string(),
                    dimension: Some(3),
                }),
            ]),
            vec![DataViewMode::Table]
        );
        assert_eq!(
            DataViewMode::for_facets(&[ObjectFacet::Document, ObjectFacet::Tabular]),
            vec![DataViewMode::Document, DataViewMode::Table]
        );
    }
    use crate::data_grid_panel::DataSource;
    use dbflux_core::{CollectionRef, Pagination, QueryResult, TableRef};
    use std::sync::Arc;
    use uuid::Uuid;

    fn table_source() -> DataSource {
        DataSource::Table {
            profile_id: Uuid::new_v4(),
            database: Some("app".to_string()),
            table: TableRef::with_schema("public", "users"),
            pagination: Pagination::Offset {
                limit: 100,
                offset: 0,
            },
            order_by: Vec::new(),
            total_rows: Some(42),
        }
    }

    fn collection_source() -> DataSource {
        DataSource::Collection {
            profile_id: Uuid::new_v4(),
            collection: CollectionRef::new("app", "users"),
            pagination: Pagination::Offset {
                limit: 100,
                offset: 0,
            },
            total_docs: Some(42),
        }
    }

    fn query_source(result: QueryResult) -> DataSource {
        DataSource::QueryResult {
            result: Arc::new(result),
            original_query: "SELECT 1".to_string(),
            profile_id: None,
        }
    }

    #[test]
    fn recommends_expected_mode_for_each_source() {
        assert_eq!(
            DataViewMode::recommended_for(&table_source()),
            DataViewMode::Table
        );
        assert_eq!(
            DataViewMode::recommended_for(&collection_source()),
            DataViewMode::Table
        );

        assert_eq!(
            DataViewMode::recommended_for(&query_source(QueryResult::json(
                vec![],
                vec![],
                std::time::Duration::ZERO,
            ))),
            DataViewMode::Document
        );

        assert_eq!(
            DataViewMode::recommended_for(&query_source(QueryResult::table(
                vec![],
                vec![],
                None,
                std::time::Duration::ZERO,
            ))),
            DataViewMode::Table
        );
    }

    #[test]
    fn available_modes_match_source_capabilities() {
        assert_eq!(
            DataViewMode::available_for(&table_source()),
            vec![DataViewMode::Table]
        );

        assert_eq!(
            DataViewMode::available_for(&collection_source()),
            vec![DataViewMode::Document, DataViewMode::Table]
        );

        assert_eq!(
            DataViewMode::available_for(&query_source(QueryResult::json(
                vec![],
                vec![],
                std::time::Duration::ZERO,
            ))),
            vec![DataViewMode::Table, DataViewMode::Document]
        );

        assert_eq!(
            DataViewMode::available_for(&query_source(QueryResult::text(
                "ok".to_string(),
                std::time::Duration::ZERO,
            ))),
            vec![DataViewMode::Table]
        );
    }
}
