//! Chart specification types that define what to render.

use dbflux_core::ColumnKind;
use serde::{Deserialize, Serialize};

/// Extension seam for chart kinds.
///
/// Only `Line` is fully implemented in v0.6. `Bar` and `Scatter` are declared
/// here so the next change is purely additive.
///
/// `#[serde(default)]` on the containing `ChartSpec.kind` field ensures that
/// existing serialized `ChartSpec` JSON without a `kind` key deserializes to
/// `Line` — preserving forward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ChartKind {
    #[default]
    Line,
    Bar,
    Scatter,
}

/// Axis classification used to pick the appropriate tick and label format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AxisKind {
    /// Time axis; ticks are formatted as human-readable dates/times.
    Time,
    /// Numeric axis; ticks are formatted as decimal numbers.
    Numeric,
}

/// Specification for a single axis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AxisSpec {
    /// Index into `QueryResult.columns` for the data source.
    pub column_index: usize,
    /// Human-readable label (typically the column name).
    pub label: String,
    /// Determines tick-formatting strategy.
    pub kind: AxisKind,
    /// Optional unit label rendered near the axis (e.g. "ms", "req/s").
    ///
    /// `None` in v0.6 — drivers do not yet supply unit metadata.
    /// This field is a forward-compatibility seam for v0.7 driver metadata.
    pub unit: Option<String>,
}

/// Specification for one Y series.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesSpec {
    /// Index into `QueryResult.columns` for the data source.
    pub column_index: usize,
    /// Human-readable label (typically the column name).
    pub label: String,
    /// Index into the panel palette; wraps modulo palette length.
    pub color_slot: u8,
}

/// Aggregation kind for the AxisBar binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum AggKind {
    /// No aggregation — raw values are passed through.
    #[default]
    None,
    Sum,
    Avg,
    Min,
    Max,
}

/// Column binding specification for the AxisBar.
///
/// Maps logical roles (X, Y, group, filter, aggregation) to column indices
/// in the current `QueryResult`. Uses `Vec<usize>` for Y rather than
/// `SmallVec` to keep the dependency footprint minimal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindingSpec {
    /// Column index for the X axis.
    pub x: usize,
    /// Column indices for Y series (up to 4 in v0.6).
    #[serde(default)]
    pub y: Vec<usize>,
    /// Optional column index for the group-by dimension.
    #[serde(default)]
    pub group_by: Option<usize>,
    /// Optional simple column-equality filter expression.
    #[serde(default)]
    pub filter: Option<String>,
    /// Aggregation applied to each Y series.
    #[serde(default)]
    pub aggregation: AggKind,
}

impl Default for BindingSpec {
    fn default() -> Self {
        Self {
            x: 0,
            y: Vec::new(),
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        }
    }
}

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
}

fn default_decimation_threshold() -> usize {
    10_000
}

/// A manual column selection entered by the user via the picker UI.
#[derive(Debug, Clone)]
pub struct ManualChartSelection {
    pub x_col: usize,
    pub y_cols: Vec<usize>,
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
                })
            })
            .collect();

        if series.is_empty() {
            return None;
        }

        let legend_visible = series.len() > 1;

        let binding = BindingSpec {
            x: selection.x_col,
            y: y_cols,
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
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chart_kind_defaults_to_line() {
        assert_eq!(ChartKind::default(), ChartKind::Line);
    }

    #[test]
    fn chart_kind_serde_round_trip() {
        let kinds = [ChartKind::Line, ChartKind::Bar, ChartKind::Scatter];
        for kind in kinds {
            let json = serde_json::to_string(&kind).expect("serialize");
            let back: ChartKind = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(kind, back, "round-trip failed for {:?}", kind);
        }
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

    #[test]
    fn binding_spec_defaults_are_sensible() {
        let b = BindingSpec::default();
        assert_eq!(b.x, 0);
        assert!(b.y.is_empty());
        assert!(b.group_by.is_none());
        assert!(b.filter.is_none());
        assert_eq!(b.aggregation, AggKind::None);
    }

    // Seam preservation: these references ensure ChartKind::Bar, ChartKind::Scatter,
    // and AggKind remain reachable and cannot silently disappear.
    #[test]
    fn seam_chart_kind_bar_and_scatter_are_reachable() {
        // This test exists solely so the compiler will catch removal of these variants.
        let _bar = ChartKind::Bar;
        let _scatter = ChartKind::Scatter;
        assert_ne!(ChartKind::Bar, ChartKind::Line);
        assert_ne!(ChartKind::Scatter, ChartKind::Line);
        assert_ne!(ChartKind::Bar, ChartKind::Scatter);
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
}
