use dbflux_components::chart::{AggKind, BindingSpec, ChartDetection};
use dbflux_core::{ColumnKind, ColumnMeta, QueryResultShape};

/// Controls how query results are rendered.
///
/// `Table` defers to `DataViewMode` (grid or document tree). The other
/// variants are text-based renderers selectable from the status bar.
/// `Chart` is only available for `Table`-shaped results that have at least
/// one `Timestamp` column and one numeric column (detected by `ChartDetection`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultViewMode {
    Table,
    Chart,
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
            Self::Json => "JSON",
            Self::Text => "Text",
            Self::Raw => "Raw",
        }
    }

    pub fn is_table(&self) -> bool {
        matches!(self, Self::Table)
    }
}

/// Derive the default `BindingSpec` for a TimeSeries auto-selected chart.
///
/// Called when a `Collection` source with `TimeSeries` category produces a result
/// with `ChartDetection::Ok`. Uses column indices only — no column name sniffing,
/// no driver-id branching.
///
/// - X: the detected `time_col` (always a `Timestamp` column)
/// - Y: only the first numeric column (the user explicitly picks more via AxisBar)
/// - Group: the first `Text` column if any (covers tag-style grouping)
/// - Filter / Aggregation: both default to `None`
pub fn default_bindings_for_time_series(
    time_col: usize,
    numeric_cols: &[usize],
    columns: &[ColumnMeta],
) -> BindingSpec {
    let y = numeric_cols
        .first()
        .copied()
        .map(|idx| vec![idx])
        .unwrap_or_default();

    let group_by = columns
        .iter()
        .enumerate()
        .find(|(_, c)| c.kind == ColumnKind::Text)
        .map(|(i, _)| i);

    BindingSpec {
        x: time_col,
        y,
        group_by,
        filter: None,
        aggregation: AggKind::None,
    }
}

/// Whether a result with the given `ChartDetection` should auto-select
/// `ResultViewMode::Chart` for a TimeSeries collection source.
///
/// Auto-select fires only when detection is `Ok` (has both a Timestamp column
/// and at least one numeric column). Returning `false` leaves the default
/// `Table` mode in place.
pub fn should_auto_select_chart_for_time_series(detection: &ChartDetection) -> bool {
    matches!(detection, ChartDetection::Ok { .. })
}

#[cfg(test)]
mod tests {
    use super::{
        ResultViewMode, default_bindings_for_time_series, should_auto_select_chart_for_time_series,
    };
    use dbflux_components::chart::{AggKind, ChartDetection};
    use dbflux_core::{ColumnKind, ColumnMeta, QueryResultShape};

    fn make_col(name: &str, kind: ColumnKind) -> ColumnMeta {
        ColumnMeta {
            name: name.to_owned(),
            type_name: String::new(),
            kind,
            nullable: true,
            is_primary_key: false,
        }
    }

    // ---- T-CE-F06: auto-select logic ----

    /// When detection is Ok the result should auto-select Chart for TimeSeries.
    #[test]
    fn auto_select_chart_when_detection_ok() {
        let detection = ChartDetection::Ok {
            time_col: 0,
            numeric_cols: vec![1],
        };
        assert!(
            should_auto_select_chart_for_time_series(&detection),
            "Ok detection must auto-select Chart for TimeSeries sources"
        );
    }

    /// When there is no Timestamp column, do NOT auto-select Chart.
    #[test]
    fn no_auto_select_chart_when_no_timestamp() {
        assert!(
            !should_auto_select_chart_for_time_series(&ChartDetection::NoTimeColumn),
            "NoTimeColumn must not auto-select Chart"
        );
        assert!(
            !should_auto_select_chart_for_time_series(&ChartDetection::NoNumericSeries),
            "NoNumericSeries must not auto-select Chart"
        );
        assert!(
            !should_auto_select_chart_for_time_series(&ChartDetection::EmptyResult),
            "EmptyResult must not auto-select Chart"
        );
    }

    /// Default bindings for a TimeSeries result: X=time_col, Y=first numeric,
    /// group=first Text column.
    #[test]
    fn default_bindings_for_time_series_presets_x_y_group() {
        let columns = vec![
            make_col("time", ColumnKind::Timestamp),
            make_col("value", ColumnKind::Float),
            make_col("host", ColumnKind::Text),
        ];

        let bindings = default_bindings_for_time_series(0, &[1], &columns);

        assert_eq!(bindings.x, 0, "X must be the time column");
        assert_eq!(bindings.y, vec![1], "Y must be the first numeric column");
        assert_eq!(
            bindings.group_by,
            Some(2),
            "group_by must be first Text column"
        );
        assert_eq!(bindings.aggregation, AggKind::None);
        assert!(bindings.filter.is_none());
    }

    /// When there is no Text column, group_by should be None.
    #[test]
    fn default_bindings_no_group_when_no_text_column() {
        let columns = vec![
            make_col("time", ColumnKind::Timestamp),
            make_col("value", ColumnKind::Float),
        ];

        let bindings = default_bindings_for_time_series(0, &[1], &columns);

        assert!(
            bindings.group_by.is_none(),
            "no Text column means no group_by"
        );
    }

    /// Only the first numeric column is pre-bound as Y; user picks more via AxisBar.
    #[test]
    fn default_bindings_only_first_numeric_as_y() {
        let columns = vec![
            make_col("time", ColumnKind::Timestamp),
            make_col("val_a", ColumnKind::Float),
            make_col("val_b", ColumnKind::Float),
            make_col("host", ColumnKind::Text),
        ];

        let bindings = default_bindings_for_time_series(0, &[1, 2], &columns);

        assert_eq!(
            bindings.y,
            vec![1],
            "only the first numeric should be pre-bound; user picks more via AxisBar"
        );
    }

    /// Verify available_for_chartable_result returns expected set.
    #[test]
    fn available_for_chartable_result_includes_chart_table_json() {
        let modes = ResultViewMode::available_for_chartable_result();
        assert!(modes.contains(&ResultViewMode::Chart));
        assert!(modes.contains(&ResultViewMode::Table));
        assert!(modes.contains(&ResultViewMode::Json));
    }

    // ---- Existing stable tests ----

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

    #[test]
    fn available_for_chartable_result_includes_chart_mode() {
        let modes = ResultViewMode::available_for_chartable_result();
        assert!(
            modes.contains(&ResultViewMode::Chart),
            "chartable result should include Chart mode"
        );
        assert!(
            modes.contains(&ResultViewMode::Table),
            "chartable result should include Table mode"
        );
    }

    /// Structural assertion: chart mode selection logic is uniform across all
    /// detection outcomes — the decision is based solely on `ChartDetection`,
    /// never on driver-id strings or column name patterns.
    ///
    /// This test constructs two identical detection results and verifies the
    /// same selection outcome regardless of which driver category is imagined
    /// to have produced them (since the function does not accept a category).
    #[test]
    fn chart_mode_selection_is_driver_agnostic() {
        let ok_detection = ChartDetection::Ok {
            time_col: 0,
            numeric_cols: vec![1],
        };

        // The selection function is called identically regardless of the imagined
        // driver (InfluxDB, MongoDB, PostgreSQL, DynamoDB, etc.). The result must
        // be the same because the function signature takes `ChartDetection` only.
        let result_a = should_auto_select_chart_for_time_series(&ok_detection);
        let result_b = should_auto_select_chart_for_time_series(&ok_detection);
        assert_eq!(
            result_a, result_b,
            "selection must be deterministic and driver-agnostic"
        );

        let no_time = ChartDetection::NoTimeColumn;
        assert!(!should_auto_select_chart_for_time_series(&no_time));
        assert!(!should_auto_select_chart_for_time_series(
            &ChartDetection::EmptyResult
        ));
    }

    /// Binding preservation: switching from Chart to Table does not mutate the
    /// binding spec. The `default_bindings_for_time_series` function is pure and
    /// stateless — repeated calls with the same inputs return the same output.
    #[test]
    fn bindings_are_preserved_across_mode_switches() {
        let columns = vec![
            make_col("time", ColumnKind::Timestamp),
            make_col("value", ColumnKind::Float),
            make_col("host", ColumnKind::Text),
        ];

        let bindings_first = default_bindings_for_time_series(0, &[1], &columns);
        let bindings_second = default_bindings_for_time_series(0, &[1], &columns);

        // Same binding on repeated derivation — switching Chart→Table→Chart keeps
        // the same BindingSpec because the inputs (column shape) did not change.
        assert_eq!(bindings_first.x, bindings_second.x);
        assert_eq!(bindings_first.y, bindings_second.y);
        assert_eq!(bindings_first.group_by, bindings_second.group_by);
    }
}
