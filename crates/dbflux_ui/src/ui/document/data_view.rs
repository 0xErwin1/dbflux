use super::data_grid_panel::DataSource;
use dbflux_core::QueryResultShape;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DataViewMode {
    #[default]
    Table,
    Document,
}

impl DataViewMode {
    pub fn recommended_for(source: &DataSource) -> Self {
        match source {
            DataSource::Table { .. } => DataViewMode::Table,
            DataSource::Collection { .. } => DataViewMode::Document,
            DataSource::QueryResult { result, .. } => {
                if result.shape == QueryResultShape::Json {
                    DataViewMode::Document
                } else {
                    DataViewMode::Table
                }
            }
        }
    }

    pub fn available_for(source: &DataSource) -> Vec<Self> {
        match source {
            DataSource::Table { .. } => vec![DataViewMode::Table],
            DataSource::Collection { .. } => vec![DataViewMode::Table, DataViewMode::Document],
            DataSource::QueryResult { result, .. } => {
                if result.shape == QueryResultShape::Json {
                    vec![DataViewMode::Table, DataViewMode::Document]
                } else {
                    vec![DataViewMode::Table]
                }
            }
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            DataViewMode::Table => "Table",
            DataViewMode::Document => "Document",
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
    use crate::ui::document::data_grid_panel::DataSource;
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
            DataViewMode::Document
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
            vec![DataViewMode::Table, DataViewMode::Document]
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
