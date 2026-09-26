use dbflux_core::QueryResultShape;

/// Controls how query results are rendered.
///
/// `Table` defers to the grid's internal `DataViewMode` (table or document tree).
/// The other variants are text-based renderers selectable from the mode bar.
/// `Chart` is only available for `Table`-shaped results that have at least one
/// `Timestamp` column and one numeric column (detected by `ChartDetection`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultViewMode {
    Table,
    Chart,
    /// The chart above the data grid, as a time-series measurement opens.
    Both,
    Json,
    Text,
    Raw,
}

impl ResultViewMode {
    pub fn default_for_shape(shape: &QueryResultShape) -> Self {
        match shape {
            QueryResultShape::Table | QueryResultShape::Json => Self::Table,
            QueryResultShape::Text => Self::Text,
            QueryResultShape::Binary => Self::Raw,
        }
    }

    /// All view modes available for a given result shape.
    pub fn available_for_shape(shape: &QueryResultShape) -> Vec<Self> {
        match shape {
            QueryResultShape::Table => vec![Self::Table, Self::Json],
            QueryResultShape::Json => vec![Self::Table, Self::Text, Self::Raw],
            QueryResultShape::Text => vec![Self::Text, Self::Json, Self::Raw],
            QueryResultShape::Binary => vec![Self::Raw],
        }
    }

    /// All view modes available for a `Table`-shaped result that passed chart
    /// auto-detection. The `Chart` button is appended after `Table`.
    pub fn available_for_chartable_result() -> Vec<Self> {
        vec![Self::Table, Self::Chart, Self::Json]
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Table => "Data",
            Self::Chart => "Chart",
            Self::Both => "Both",
            Self::Json => "JSON",
            Self::Text => "Text",
            Self::Raw => "Raw",
        }
    }

    pub fn is_table(&self) -> bool {
        matches!(self, Self::Table)
    }

    /// Whether the mode draws the chart (alone or above the grid).
    pub fn shows_chart(&self) -> bool {
        matches!(self, Self::Chart | Self::Both)
    }
}
