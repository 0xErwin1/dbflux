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
