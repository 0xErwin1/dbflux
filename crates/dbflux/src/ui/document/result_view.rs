use dbflux_core::QueryResultShape;

/// Controls how query results are rendered.
///
/// `Table` defers to `DataViewMode` (grid or document tree). The other
/// variants are text-based renderers selectable from the status bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultViewMode {
    Table,
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

    pub fn label(&self) -> &'static str {
        match self {
            Self::Table => "Data",
            Self::Json => "JSON",
            Self::Text => "Text",
            Self::Raw => "Raw",
        }
    }

    pub fn is_table(&self) -> bool {
        matches!(self, Self::Table)
    }
}
