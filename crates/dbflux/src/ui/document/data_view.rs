use super::data_grid_panel::DataSource;

/// How data should be rendered in the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DataViewMode {
    /// Tabular grid view (rows and columns).
    /// Best for: SQL tables, CSV data, structured results.
    #[default]
    Table,

    /// Document/JSON tree view with expandable nested structures.
    /// Best for: MongoDB collections, JSON documents.
    Document,

    /// Key-value list view.
    /// Best for: Redis keys, simple mappings.
    KeyValue,
}

impl DataViewMode {
    /// Get the recommended view mode for a data source.
    pub fn recommended_for(source: &DataSource) -> Self {
        match source {
            DataSource::Table { .. } => DataViewMode::Table,
            DataSource::Collection { .. } => DataViewMode::Document,
            DataSource::QueryResult { .. } => DataViewMode::Table,
        }
    }

    /// Get all available view modes for a data source.
    pub fn available_for(source: &DataSource) -> Vec<Self> {
        match source {
            DataSource::Table { .. } => vec![DataViewMode::Table],
            DataSource::Collection { .. } => vec![DataViewMode::Table, DataViewMode::Document],
            DataSource::QueryResult { .. } => vec![DataViewMode::Table],
        }
    }

    /// Check if this view mode supports the given data source.
    pub fn supports(&self, source: &DataSource) -> bool {
        Self::available_for(source).contains(self)
    }

    /// Get the display label for this view mode.
    pub fn label(&self) -> &'static str {
        match self {
            DataViewMode::Table => "Table",
            DataViewMode::Document => "Document",
            DataViewMode::KeyValue => "Key-Value",
        }
    }

    /// Get the icon name for this view mode.
    pub fn icon(&self) -> &'static str {
        match self {
            DataViewMode::Table => "table",
            DataViewMode::Document => "file-json",
            DataViewMode::KeyValue => "list",
        }
    }
}

/// Configuration for how data should be displayed.
#[derive(Debug, Clone)]
pub struct DataViewConfig {
    /// Current view mode.
    pub mode: DataViewMode,

    /// For Document mode: whether to expand all nested objects by default.
    pub expand_all: bool,

    /// For Document mode: maximum nesting depth to show inline.
    pub inline_depth: usize,

    /// For Table mode: whether to show row numbers.
    pub show_row_numbers: bool,
}

impl Default for DataViewConfig {
    fn default() -> Self {
        Self {
            mode: DataViewMode::Table,
            expand_all: false,
            inline_depth: 1,
            show_row_numbers: true,
        }
    }
}

impl DataViewConfig {
    pub fn for_source(source: &DataSource) -> Self {
        Self {
            mode: DataViewMode::recommended_for(source),
            ..Default::default()
        }
    }
}
