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

#[cfg(test)]
mod tests {
    use super::ResultViewMode;
    use dbflux_core::QueryResultShape;

    #[test]
    fn default_for_shape_matches_expected_mode() {
        assert_eq!(
            ResultViewMode::default_for_shape(&QueryResultShape::Table),
            ResultViewMode::Table
        );
        assert_eq!(
            ResultViewMode::default_for_shape(&QueryResultShape::Json),
            ResultViewMode::Table
        );
        assert_eq!(
            ResultViewMode::default_for_shape(&QueryResultShape::Text),
            ResultViewMode::Text
        );
        assert_eq!(
            ResultViewMode::default_for_shape(&QueryResultShape::Binary),
            ResultViewMode::Raw
        );
    }

    #[test]
    fn available_modes_for_each_shape_are_stable() {
        assert_eq!(
            ResultViewMode::available_for_shape(&QueryResultShape::Table),
            vec![ResultViewMode::Table, ResultViewMode::Json]
        );

        assert_eq!(
            ResultViewMode::available_for_shape(&QueryResultShape::Json),
            vec![
                ResultViewMode::Table,
                ResultViewMode::Text,
                ResultViewMode::Raw
            ]
        );

        assert_eq!(
            ResultViewMode::available_for_shape(&QueryResultShape::Text),
            vec![
                ResultViewMode::Text,
                ResultViewMode::Json,
                ResultViewMode::Raw
            ]
        );

        assert_eq!(
            ResultViewMode::available_for_shape(&QueryResultShape::Binary),
            vec![ResultViewMode::Raw]
        );
    }
}
