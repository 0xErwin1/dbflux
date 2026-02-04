use super::data_grid_panel::DataSource;

/// How data should be rendered in the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DataViewMode {
    /// Tabular grid view (rows and columns).
    #[default]
    Table,

    /// Document/JSON tree view with expandable nested structures.
    Document,
}

impl DataViewMode {
    /// Get the recommended view mode for a data source.
    pub fn recommended_for(source: &DataSource) -> Self {
        match source {
            DataSource::Table { .. } => DataViewMode::Table,
            DataSource::Collection { .. } => DataViewMode::Document,
            DataSource::QueryResult { result, .. } => {
                if result.is_document_result {
                    DataViewMode::Document
                } else {
                    DataViewMode::Table
                }
            }
        }
    }

    /// Get all available view modes for a data source.
    pub fn available_for(source: &DataSource) -> Vec<Self> {
        match source {
            DataSource::Table { .. } => vec![DataViewMode::Table],
            DataSource::Collection { .. } => vec![DataViewMode::Table, DataViewMode::Document],
            DataSource::QueryResult { result, .. } => {
                if result.is_document_result {
                    vec![DataViewMode::Table, DataViewMode::Document]
                } else {
                    vec![DataViewMode::Table]
                }
            }
        }
    }

    /// Get the display label for this view mode.
    pub fn label(&self) -> &'static str {
        match self {
            DataViewMode::Table => "Table",
            DataViewMode::Document => "Document",
        }
    }
}

/// Configuration for how data should be displayed.
#[derive(Debug, Clone)]
pub struct DataViewConfig {
    /// Current view mode.
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
