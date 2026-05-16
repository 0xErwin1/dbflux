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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChartKind {
    Line,
    Bar,
    Scatter,
}

impl Default for ChartKind {
    fn default() -> Self {
        Self::Line
    }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggKind {
    /// No aggregation — raw values are passed through.
    None,
    Sum,
    Avg,
    Min,
    Max,
}

impl Default for AggKind {
    fn default() -> Self {
        Self::None
    }
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
        assert_eq!(spec.kind, ChartKind::Line, "missing 'kind' should default to Line");
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
}
