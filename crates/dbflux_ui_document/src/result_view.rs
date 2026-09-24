use dbflux_components::chart::spec::group_key;
use dbflux_components::chart::{AggKind, BindingSpec, ChartDetection};
use dbflux_core::{ColumnKind, QueryResult, QueryResultShape};

// `ResultViewMode` now lives in `dbflux_components` so it can be used by
// `ResultPanel` without a circular dependency. Re-exported here for all
// callers in `dbflux_ui` that import from this module.
pub use dbflux_components::result_view::ResultViewMode;

/// Most distinct values the first `Text` column may hold in a result and
/// still become the default group. Past this the chart starts ungrouped, so a
/// high-cardinality text field does not open as a wall of lines. A group picked
/// in the axis bar has no such limit.
pub const DEFAULT_GROUP_MAX_VALUES: usize = 12;

/// Derive the default `BindingSpec` for a TimeSeries auto-selected chart.
///
/// Called when a `Collection` source with `TimeSeries` category produces a result
/// with `ChartDetection::Ok`. Uses column kinds and cell values only — no column
/// name sniffing, no driver-id branching.
///
/// - X: the detected `time_col` (always a `Timestamp` column)
/// - Y: only the first numeric column (the user explicitly picks more via AxisBar)
/// - Group: the first `Text` column (covers tag-style grouping), when it holds
///   at most `DEFAULT_GROUP_MAX_VALUES` distinct values in `result`
/// - Filter / Aggregation: both default to `None`
pub fn default_bindings_for_time_series(
    time_col: usize,
    numeric_cols: &[usize],
    result: &QueryResult,
) -> BindingSpec {
    let y = numeric_cols
        .first()
        .copied()
        .map(|idx| vec![idx])
        .unwrap_or_default();

    let group_by = result
        .columns
        .iter()
        .position(|c| c.kind == ColumnKind::Text)
        .filter(|&column| distinct_values_within(result, column, DEFAULT_GROUP_MAX_VALUES));

    BindingSpec {
        x: time_col,
        y,
        group_by,
        filter: None,
        aggregation: AggKind::None,
    }
}

/// Whether `column` holds at most `limit` distinct values across the rows.
/// Stops scanning as soon as the limit is passed.
fn distinct_values_within(result: &QueryResult, column: usize, limit: usize) -> bool {
    let mut seen: Vec<String> = Vec::new();

    for row in &result.rows {
        let Some(cell) = row.get(column) else {
            continue;
        };

        let key = group_key(cell);
        if !seen.contains(&key) {
            seen.push(key);

            if seen.len() > limit {
                return false;
            }
        }
    }

    true
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

/// Result view a fresh result opens in.
///
/// - The first result of a time-series collection opens as a chart when chart
///   detection passed.
/// - A chart stays a chart while the new result is still chartable.
/// - Later results of a time-series collection (refresh, paging) keep the view
///   the user picked, so an auto-refresh never flips Data back to Chart.
/// - Everything else falls back to the default view for the result shape.
pub fn result_view_mode_for_fresh_result(
    current: ResultViewMode,
    shape: &QueryResultShape,
    detection: &ChartDetection,
    time_series_collection: bool,
    first_result: bool,
) -> ResultViewMode {
    let chartable = should_auto_select_chart_for_time_series(detection);

    if time_series_collection && first_result && chartable {
        return ResultViewMode::Chart;
    }

    if current == ResultViewMode::Chart && chartable {
        return ResultViewMode::Chart;
    }

    if time_series_collection && !first_result && current != ResultViewMode::Chart {
        return current;
    }

    ResultViewMode::default_for_shape(shape)
}

#[cfg(test)]
mod tests {
    use super::{
        ResultViewMode, default_bindings_for_time_series, result_view_mode_for_fresh_result,
        should_auto_select_chart_for_time_series,
    };
    use dbflux_components::chart::{AggKind, ChartDetection};
    use dbflux_core::{ColumnKind, ColumnMeta, QueryResult, QueryResultShape, Value};

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

    fn empty_result(columns: Vec<ColumnMeta>) -> QueryResult {
        QueryResult::table(columns, Vec::new(), None, std::time::Duration::ZERO)
    }

    /// Time, load and host columns with one row per host name given.
    fn hosts_result(hosts: &[String]) -> QueryResult {
        QueryResult::table(
            vec![
                make_col("time", ColumnKind::Timestamp),
                make_col("load", ColumnKind::Float),
                make_col("host", ColumnKind::Text),
            ],
            hosts
                .iter()
                .enumerate()
                .map(|(i, host)| {
                    vec![
                        Value::Int(i as i64),
                        Value::Float(1.0),
                        Value::Text(host.clone()),
                    ]
                })
                .collect(),
            None,
            std::time::Duration::ZERO,
        )
    }

    fn host_names(count: usize) -> Vec<String> {
        (0..count).map(|i| format!("host-{i}")).collect()
    }

    #[test]
    fn default_group_applies_to_a_low_cardinality_tag() {
        let mut hosts = host_names(3);
        hosts.extend(host_names(3));

        let bindings = default_bindings_for_time_series(0, &[1], &hosts_result(&hosts));

        assert_eq!(
            bindings.group_by,
            Some(2),
            "3 distinct hosts group the chart"
        );
    }

    #[test]
    fn default_group_applies_up_to_the_limit() {
        let bindings = default_bindings_for_time_series(
            0,
            &[1],
            &hosts_result(&host_names(super::DEFAULT_GROUP_MAX_VALUES)),
        );

        assert_eq!(bindings.group_by, Some(2));
    }

    #[test]
    fn default_group_is_skipped_past_the_limit() {
        let bindings = default_bindings_for_time_series(
            0,
            &[1],
            &hosts_result(&host_names(super::DEFAULT_GROUP_MAX_VALUES + 1)),
        );

        assert_eq!(
            bindings.group_by, None,
            "13 distinct values start the chart ungrouped"
        );
        assert_eq!(bindings.y, vec![1], "X and Y defaults still apply");
    }

    #[test]
    fn a_group_picked_by_hand_is_not_capped() {
        use dbflux_components::chart::spec::{ChartSpec, ManualChartSelection};

        let result = hosts_result(&host_names(super::DEFAULT_GROUP_MAX_VALUES + 1));
        let selection = ManualChartSelection {
            x_col: 0,
            y_cols: vec![1],
            group_by: Some(2),
        };

        let spec = ChartSpec::from_manual_selection(&selection, &result.columns, 10_000)
            .expect("spec")
            .with_group_series(&result);

        assert_eq!(spec.series.len(), 13, "every host gets its own line");
    }

    fn chartable() -> ChartDetection {
        ChartDetection::Ok {
            time_col: 0,
            numeric_cols: vec![1],
        }
    }

    #[test]
    fn first_time_series_collection_result_opens_as_chart() {
        let mode = result_view_mode_for_fresh_result(
            ResultViewMode::Table,
            &QueryResultShape::Table,
            &chartable(),
            true,
            true,
        );

        assert_eq!(mode, ResultViewMode::Chart);
    }

    #[test]
    fn unchartable_time_series_collection_result_opens_as_data() {
        let mode = result_view_mode_for_fresh_result(
            ResultViewMode::Table,
            &QueryResultShape::Table,
            &ChartDetection::NoNumericSeries,
            true,
            true,
        );

        assert_eq!(mode, ResultViewMode::Table);
    }

    #[test]
    fn refreshed_time_series_collection_keeps_the_view_the_user_picked() {
        for picked in [ResultViewMode::Table, ResultViewMode::Json] {
            let mode = result_view_mode_for_fresh_result(
                picked,
                &QueryResultShape::Table,
                &chartable(),
                true,
                false,
            );

            assert_eq!(mode, picked, "a refresh must not replace {picked:?}");
        }
    }

    #[test]
    fn chart_falls_back_to_data_when_the_new_result_is_not_chartable() {
        let mode = result_view_mode_for_fresh_result(
            ResultViewMode::Chart,
            &QueryResultShape::Table,
            &ChartDetection::EmptyResult,
            true,
            false,
        );

        assert_eq!(mode, ResultViewMode::Table);
    }

    #[test]
    fn other_sources_only_stay_in_chart_when_already_there() {
        let from_table = result_view_mode_for_fresh_result(
            ResultViewMode::Table,
            &QueryResultShape::Table,
            &chartable(),
            false,
            true,
        );
        let from_chart = result_view_mode_for_fresh_result(
            ResultViewMode::Chart,
            &QueryResultShape::Table,
            &chartable(),
            false,
            false,
        );

        assert_eq!(from_table, ResultViewMode::Table);
        assert_eq!(from_chart, ResultViewMode::Chart);
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

        let bindings = default_bindings_for_time_series(0, &[1], &empty_result(columns.clone()));

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

        let bindings = default_bindings_for_time_series(0, &[1], &empty_result(columns.clone()));

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

        let bindings = default_bindings_for_time_series(0, &[1, 2], &empty_result(columns.clone()));

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

        let bindings_first =
            default_bindings_for_time_series(0, &[1], &empty_result(columns.clone()));
        let bindings_second =
            default_bindings_for_time_series(0, &[1], &empty_result(columns.clone()));

        // Same binding on repeated derivation — switching Chart→Table→Chart keeps
        // the same BindingSpec because the inputs (column shape) did not change.
        assert_eq!(bindings_first.x, bindings_second.x);
        assert_eq!(bindings_first.y, bindings_second.y);
        assert_eq!(bindings_first.group_by, bindings_second.group_by);
    }
}
