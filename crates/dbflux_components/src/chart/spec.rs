//! The persisted chart specification composite and its constructors.
//!
//! The specification value types (chart kinds, axis and series specs, the
//! binding) live in `bolt::chart::spec` and are re-exported here. What stays
//! product-side is the composite and everything that binds it to a
//! `dbflux_core::ColumnMeta` column list: the constructors and the
//! group-series split, which read a `QueryResult`'s cells.

use dbflux_core::{ColumnKind, QueryResult, Value};
use serde::{Deserialize, Serialize};

pub use bolt::chart::spec::{
    AggKind, AxisKind, AxisSpec, BindingSpec, ChartKind, ManualChartSelection, SeriesSpec, YScale,
};

/// Full specification describing what and how to render a chart.
///
/// No `Default` impl: a chart always requires an explicit column selection.
/// Use `ChartSpec::from_detection` or `ChartSpec::from_manual_selection` as constructors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChartSpec {
    /// Chart rendering kind. Defaults to `Line` when absent in JSON.
    #[serde(default)]
    pub kind: ChartKind,
    pub x_axis: AxisSpec,
    pub series: Vec<SeriesSpec>,
    /// Whether the legend is visible. Follows the rule: visible by default when
    /// `series.len() > 1`; per-panel toggle stored separately on `DataGridPanel`.
    #[serde(default)]
    pub legend_visible: bool,
    /// Point count threshold before LTTB decimation is applied. Default: 10 000.
    #[serde(default = "default_decimation_threshold")]
    pub decimation_threshold: usize,
    /// Column binding for the AxisBar. Added in v0.6; absent in older JSON → default.
    #[serde(default)]
    pub binding: BindingSpec,
    /// When `true`, the engine records the original `QueryResult.rows` index for
    /// each decimated point in `RenderModel.source_indices`. Only enabled by
    /// DataDocument hosts that implement `source_for_point`; CodeDocument-backed
    /// charts leave this `false` to avoid the memory overhead.
    #[serde(default)]
    pub track_source_indices: bool,
    /// Y-axis scale mode. Defaults to `Linear` when absent in older JSON.
    ///
    /// When set to `Log`, the engine applies `ln(y + 1)` before mapping data
    /// values to screen coordinates and generates log-spaced Y ticks with
    /// original-scale labels.
    #[serde(default)]
    pub y_scale: YScale,
}

fn default_decimation_threshold() -> usize {
    10_000
}

/// Key a row's group-by cell is matched on.
pub fn group_key(value: &Value) -> String {
    value.as_display_string()
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

impl ChartSpec {
    /// Build a `ChartSpec` from an explicit `BindingSpec` and column metadata.
    ///
    /// Returns `None` when `bindings.x` is out of bounds or when all `bindings.y`
    /// indices are out of bounds (producing an empty series list).
    ///
    /// The `bindings` value is preserved verbatim in the returned `ChartSpec`
    /// so that the AxisBar can round-trip it without loss.
    ///
    /// # Axis kind inference
    ///
    /// The X axis kind is inferred from the column's `ColumnKind`:
    /// - `Timestamp` → `AxisKind::Time`
    /// - anything else → `AxisKind::Numeric`
    pub fn from_bindings(
        bindings: &BindingSpec,
        columns: &[dbflux_core::ColumnMeta],
        decimation_threshold: usize,
    ) -> Option<Self> {
        let x_col_meta = columns.get(bindings.x)?;

        let axis_kind = if x_col_meta.kind == dbflux_core::ColumnKind::Timestamp {
            AxisKind::Time
        } else {
            AxisKind::Numeric
        };

        let x_axis = AxisSpec {
            column_index: bindings.x,
            label: x_col_meta.name.clone(),
            kind: axis_kind,
            unit: None,
        };

        let series: Vec<SeriesSpec> = bindings
            .y
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(slot, col_idx)| {
                let meta = columns.get(col_idx)?;
                Some(SeriesSpec {
                    column_index: col_idx,
                    label: meta.name.clone(),
                    color_slot: slot as u8,
                    group_value: None,
                })
            })
            .collect();

        if series.is_empty() {
            return None;
        }

        let legend_visible = series.len() > 1;

        Some(ChartSpec {
            kind: ChartKind::Line,
            x_axis,
            series,
            legend_visible,
            decimation_threshold,
            binding: bindings.clone(),
            track_source_indices: false,
            y_scale: YScale::Linear,
        })
    }

    /// Build a `ChartSpec` from successful auto-detection.
    ///
    /// The caller must ensure that `detection` is `ChartDetection::Ok`; passing
    /// any other variant returns `None`.
    pub fn from_detection(
        time_col: usize,
        numeric_cols: Vec<usize>,
        columns: &[dbflux_core::ColumnMeta],
        decimation_threshold: usize,
    ) -> Option<Self> {
        let x_col_meta = columns.get(time_col)?;
        let x_axis = AxisSpec {
            column_index: time_col,
            label: x_col_meta.name.clone(),
            kind: AxisKind::Time,
            unit: None,
        };

        let series: Vec<SeriesSpec> = numeric_cols
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(slot, col_idx)| {
                let meta = columns.get(col_idx)?;
                Some(SeriesSpec {
                    column_index: col_idx,
                    label: meta.name.clone(),
                    color_slot: slot as u8,
                    group_value: None,
                })
            })
            .collect();

        if series.is_empty() {
            return None;
        }

        let legend_visible = series.len() > 1;

        let binding = BindingSpec {
            x: time_col,
            y: numeric_cols.clone(),
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        Some(ChartSpec {
            kind: ChartKind::Line,
            x_axis,
            series,
            legend_visible,
            decimation_threshold,
            binding,
            track_source_indices: false,
            y_scale: YScale::Linear,
        })
    }

    /// Build a `ChartSpec` from a manual column selection supplied by the user.
    pub fn from_manual_selection(
        selection: &ManualChartSelection,
        columns: &[dbflux_core::ColumnMeta],
        decimation_threshold: usize,
    ) -> Option<Self> {
        let x_col_meta = columns.get(selection.x_col)?;

        let axis_kind = if x_col_meta.kind == ColumnKind::Timestamp {
            AxisKind::Time
        } else {
            AxisKind::Numeric
        };

        let x_axis = AxisSpec {
            column_index: selection.x_col,
            label: x_col_meta.name.clone(),
            kind: axis_kind,
            unit: None,
        };

        let y_cols: Vec<usize> = selection
            .y_cols
            .iter()
            .copied()
            .filter(|&i| i != selection.x_col && i < columns.len())
            .collect();

        let series: Vec<SeriesSpec> = y_cols
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(slot, col_idx)| {
                let meta = columns.get(col_idx)?;
                Some(SeriesSpec {
                    column_index: col_idx,
                    label: meta.name.clone(),
                    color_slot: slot as u8,
                    group_value: None,
                })
            })
            .collect();

        if series.is_empty() {
            return None;
        }

        let legend_visible = series.len() > 1;

        let group_by = selection
            .group_by
            .filter(|&column| column != selection.x_col && column < columns.len());

        let binding = BindingSpec {
            x: selection.x_col,
            y: y_cols,
            group_by,
            filter: None,
            aggregation: AggKind::None,
        };

        Some(ChartSpec {
            kind: ChartKind::Line,
            x_axis,
            series,
            legend_visible,
            decimation_threshold,
            binding,
            track_source_indices: false,
            y_scale: YScale::Linear,
        })
    }

    /// Splits each series into one series per distinct value of the
    /// `binding.group_by` column, in the order the values first appear.
    ///
    /// A single Y column is labelled by the group value alone. Several Y
    /// columns are labelled `"<column> · <value>"`. The spec is returned
    /// unchanged when no group is bound, the group column is out of range,
    /// the result has no rows, or the series are already split.
    pub fn with_group_series(mut self, result: &QueryResult) -> Self {
        let Some(group_column) = self.binding.group_by else {
            return self;
        };

        if group_column >= result.columns.len()
            || self
                .series
                .iter()
                .any(|series| series.group_value.is_some())
        {
            return self;
        }

        let mut group_values: Vec<String> = Vec::new();
        for row in &result.rows {
            let Some(cell) = row.get(group_column) else {
                continue;
            };

            let key = group_key(cell);
            if !group_values.contains(&key) {
                group_values.push(key);
            }
        }

        if group_values.is_empty() {
            return self;
        }

        let label_with_column = self.series.len() > 1;

        let grouped: Vec<SeriesSpec> = self
            .series
            .iter()
            .flat_map(|series| {
                group_values.iter().map(move |value| {
                    let label = if label_with_column {
                        format!("{} · {}", series.label, value)
                    } else {
                        value.clone()
                    };

                    (series.column_index, label, value.clone())
                })
            })
            .enumerate()
            .map(|(slot, (column_index, label, value))| SeriesSpec {
                column_index,
                label,
                color_slot: u8::try_from(slot).unwrap_or(u8::MAX),
                group_value: Some(value),
            })
            .collect();

        self.series = grouped;
        self
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Old JSON without a `y_scale` field must deserialize to `YScale::Linear`
    /// so that existing persisted chart specs are not broken.
    #[test]
    fn chart_spec_without_y_scale_field_deserializes_as_linear() {
        let json = r#"{
            "x_axis": {"column_index": 0, "label": "t", "kind": "Time", "unit": null},
            "series": [],
            "legend_visible": false,
            "decimation_threshold": 10000,
            "binding": {"x": 0, "y": [], "aggregation": "None"}
        }"#;

        let spec: ChartSpec = serde_json::from_str(json).expect("deserialize");
        assert_eq!(
            spec.y_scale,
            YScale::Linear,
            "missing 'y_scale' should default to Linear"
        );
    }

    #[test]
    fn chart_spec_without_kind_field_deserializes_as_line() {
        // Simulates old JSON that has no "kind" key.
        let json = r#"{
            "x_axis": {"column_index": 0, "label": "t", "kind": "Time", "unit": null},
            "series": [],
            "legend_visible": false,
            "decimation_threshold": 10000,
            "binding": {"x": 0, "y": [], "aggregation": "None"}
        }"#;

        let spec: ChartSpec = serde_json::from_str(json).expect("deserialize");
        assert_eq!(
            spec.kind,
            ChartKind::Line,
            "missing 'kind' should default to Line"
        );
    }

    fn grouped_result() -> dbflux_core::QueryResult {
        let column = |name: &str, kind: ColumnKind| dbflux_core::ColumnMeta {
            name: name.to_string(),
            type_name: String::new(),
            kind,
            nullable: true,
            is_primary_key: false,
        };

        dbflux_core::QueryResult::table(
            vec![
                column("time", ColumnKind::Timestamp),
                column("load", ColumnKind::Float),
                column("host", ColumnKind::Text),
                column("mem", ColumnKind::Float),
            ],
            ["a", "b", "a", "b"]
                .iter()
                .enumerate()
                .map(|(i, host)| {
                    vec![
                        Value::Int(i as i64),
                        Value::Float(1.0),
                        Value::Text((*host).to_string()),
                        Value::Float(2.0),
                    ]
                })
                .collect(),
            None,
            std::time::Duration::ZERO,
        )
    }

    fn grouped_selection(y_cols: Vec<usize>) -> ManualChartSelection {
        ManualChartSelection {
            x_col: 0,
            y_cols,
            group_by: Some(2),
        }
    }

    #[test]
    fn manual_selection_keeps_its_group_in_the_binding() {
        let result = grouped_result();
        let spec =
            ChartSpec::from_manual_selection(&grouped_selection(vec![1]), &result.columns, 100)
                .expect("spec");

        assert_eq!(spec.binding.group_by, Some(2));
    }

    #[test]
    fn with_group_series_splits_one_column_by_group_value() {
        let result = grouped_result();
        let spec =
            ChartSpec::from_manual_selection(&grouped_selection(vec![1]), &result.columns, 100)
                .expect("spec")
                .with_group_series(&result);

        let series: Vec<(usize, &str, Option<&str>)> = spec
            .series
            .iter()
            .map(|s| (s.column_index, s.label.as_str(), s.group_value.as_deref()))
            .collect();
        assert_eq!(series, vec![(1, "a", Some("a")), (1, "b", Some("b"))]);
        assert_ne!(spec.series[0].color_slot, spec.series[1].color_slot);
    }

    #[test]
    fn with_group_series_names_each_column_and_group_pair() {
        let result = grouped_result();
        let spec =
            ChartSpec::from_manual_selection(&grouped_selection(vec![1, 3]), &result.columns, 100)
                .expect("spec")
                .with_group_series(&result);

        let labels: Vec<&str> = spec.series.iter().map(|s| s.label.as_str()).collect();
        assert_eq!(labels, vec!["load · a", "load · b", "mem · a", "mem · b"]);
    }

    #[test]
    fn with_group_series_is_idempotent_and_ignores_missing_groups() {
        let result = grouped_result();
        let grouped =
            ChartSpec::from_manual_selection(&grouped_selection(vec![1]), &result.columns, 100)
                .expect("spec")
                .with_group_series(&result)
                .with_group_series(&result);
        assert_eq!(
            grouped.series.len(),
            2,
            "splitting twice must not split again"
        );

        let ungrouped = ChartSpec::from_detection(0, vec![1], &result.columns, 100)
            .expect("spec")
            .with_group_series(&result);
        assert_eq!(ungrouped.series.len(), 1);
        assert!(ungrouped.series[0].group_value.is_none());
    }

    // ---------------------------------------------------------------------------
    // T-CE-E01: ChartSpec::from_bindings tests (RED → GREEN)
    // ---------------------------------------------------------------------------

    fn make_col_meta(name: &str, kind: dbflux_core::ColumnKind) -> dbflux_core::ColumnMeta {
        dbflux_core::ColumnMeta {
            name: name.to_owned(),
            type_name: String::new(),
            kind,
            nullable: true,
            is_primary_key: false,
        }
    }

    fn three_col_meta() -> Vec<dbflux_core::ColumnMeta> {
        vec![
            make_col_meta("ts", dbflux_core::ColumnKind::Timestamp),
            make_col_meta("cpu", dbflux_core::ColumnKind::Float),
            make_col_meta("mem", dbflux_core::ColumnKind::Float),
        ]
    }

    #[test]
    fn from_bindings_basic_produces_correct_x_axis_and_series() {
        let cols = three_col_meta();
        let bindings = BindingSpec {
            x: 0,
            y: vec![1, 2],
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        let spec = ChartSpec::from_bindings(&bindings, &cols, 10_000)
            .expect("from_bindings should succeed");

        assert_eq!(spec.x_axis.column_index, 0);
        assert_eq!(spec.x_axis.label, "ts");
        assert_eq!(spec.x_axis.kind, AxisKind::Time);
        assert_eq!(spec.series.len(), 2);
        assert_eq!(spec.series[0].column_index, 1);
        assert_eq!(spec.series[0].label, "cpu");
        assert_eq!(spec.series[1].column_index, 2);
        assert_eq!(spec.series[1].label, "mem");
        assert_eq!(spec.decimation_threshold, 10_000);
        assert!(spec.legend_visible, "two series → legend visible");
    }

    #[test]
    fn from_bindings_single_y_legend_hidden() {
        let cols = three_col_meta();
        let bindings = BindingSpec {
            x: 0,
            y: vec![1],
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        let spec = ChartSpec::from_bindings(&bindings, &cols, 10_000)
            .expect("from_bindings should succeed");

        assert_eq!(spec.series.len(), 1);
        assert!(!spec.legend_visible, "one series → legend hidden");
    }

    #[test]
    fn from_bindings_empty_y_returns_none() {
        let cols = three_col_meta();
        let bindings = BindingSpec {
            x: 0,
            y: vec![],
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        let result = ChartSpec::from_bindings(&bindings, &cols, 10_000);
        assert!(result.is_none(), "empty y cols must return None");
    }

    #[test]
    fn from_bindings_x_out_of_bounds_returns_none() {
        let cols = three_col_meta();
        let bindings = BindingSpec {
            x: 99,
            y: vec![1],
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        let result = ChartSpec::from_bindings(&bindings, &cols, 10_000);
        assert!(result.is_none(), "out-of-bounds x must return None");
    }

    #[test]
    fn from_bindings_numeric_x_uses_numeric_axis_kind() {
        let cols = vec![
            make_col_meta("seq", dbflux_core::ColumnKind::Integer),
            make_col_meta("val", dbflux_core::ColumnKind::Float),
        ];
        let bindings = BindingSpec {
            x: 0,
            y: vec![1],
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        let spec = ChartSpec::from_bindings(&bindings, &cols, 10_000)
            .expect("from_bindings should succeed");

        assert_eq!(spec.x_axis.kind, AxisKind::Numeric);
    }

    #[test]
    fn from_bindings_binding_field_mirrors_input() {
        let cols = three_col_meta();
        let bindings = BindingSpec {
            x: 0,
            y: vec![1, 2],
            group_by: Some(2),
            filter: Some("cpu > 0".to_string()),
            aggregation: AggKind::Avg,
        };

        let spec = ChartSpec::from_bindings(&bindings, &cols, 10_000)
            .expect("from_bindings should succeed");

        assert_eq!(spec.binding.x, 0);
        assert_eq!(spec.binding.y, vec![1, 2]);
        assert_eq!(spec.binding.group_by, Some(2));
        assert_eq!(spec.binding.filter, Some("cpu > 0".to_string()));
        assert_eq!(spec.binding.aggregation, AggKind::Avg);
    }

    #[test]
    fn from_bindings_out_of_bounds_y_columns_are_skipped() {
        let cols = three_col_meta();
        let bindings = BindingSpec {
            x: 0,
            y: vec![1, 99],
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        let spec = ChartSpec::from_bindings(&bindings, &cols, 10_000)
            .expect("valid y entries produce a spec");

        // Column index 99 is out of bounds; only column 1 is valid.
        assert_eq!(spec.series.len(), 1);
        assert_eq!(spec.series[0].column_index, 1);
    }

    #[test]
    fn chart_spec_serde_round_trip_keeps_every_field() {
        let spec = ChartSpec::from_bindings(
            &BindingSpec {
                x: 0,
                y: vec![1],
                group_by: None,
                filter: None,
                aggregation: AggKind::None,
            },
            &three_col_meta(),
            10_000,
        )
        .expect("spec");

        let json = serde_json::to_string(&spec).expect("serialize");
        let back: ChartSpec = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.kind, spec.kind);
        assert_eq!(back.x_axis.column_index, spec.x_axis.column_index);
        let (back_series, spec_series): (Vec<_>, Vec<_>) = (
            back.series
                .iter()
                .map(|s| {
                    (
                        s.column_index,
                        s.label.as_str(),
                        s.color_slot,
                        s.group_value.clone(),
                    )
                })
                .collect(),
            spec.series
                .iter()
                .map(|s| {
                    (
                        s.column_index,
                        s.label.as_str(),
                        s.color_slot,
                        s.group_value.clone(),
                    )
                })
                .collect(),
        );
        assert_eq!(back_series, spec_series);
        assert_eq!(back.legend_visible, spec.legend_visible);
        assert_eq!(back.decimation_threshold, spec.decimation_threshold);
        assert_eq!(back.binding, spec.binding);
        assert_eq!(back.track_source_indices, spec.track_source_indices);
        assert_eq!(back.y_scale, spec.y_scale);
    }
}
