//! Chart specification value types: what a chart draws, as persisted data.
//!
//! These are the value types a chart specification is built from. The
//! composite specification type and its constructors live with the data
//! owner — it is the owner's column list a spec binds to — while the value
//! vocabulary itself is product-neutral.

use serde::{Deserialize, Serialize};

/// Y-axis scale mode.
///
/// `Linear` is the default and preserves existing behavior byte-for-byte.
/// `Log` applies `ln(y + 1)` (log1p) before projecting data values to screen
/// coordinates.  Using `y + 1` instead of `y` avoids `ln(0) = -∞` for
/// zero-count buckets, the common case in count-per-bucket series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum YScale {
    #[default]
    Linear,
    Log,
}

/// Chart rendering kinds. `Line`, `Bar`, and `Scatter` are all fully
/// implemented and share the same kind-agnostic render model.
///
/// `#[serde(default)]` on the containing chart specification's `kind` field
/// ensures that existing serialized JSON without a `kind` key deserializes to
/// `Line` — preserving forward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ChartKind {
    #[default]
    Line,
    Bar,
    Scatter,
    /// Filled line chart: the area between the series line and the baseline is
    /// shaded. Shares all of Line's geometry and hover behaviour.
    Area,
    /// Stacked vertical bars: each X position shows one bar per series,
    /// stacked cumulatively rather than grouped side-by-side. The Y axis is
    /// re-scaled at render time to the maximum stack sum, since the precomputed
    /// render model's Y maximum reflects individual-series maxima only.
    StackedBar,
    /// Pie chart: no X/Y axes; each visible series becomes one wedge sized by
    /// the sum of that series' Y values. Focus by angle from the pie centre.
    Pie,
    /// Single-stat / number chart: shows the latest value per visible series
    /// as large text, with no axes or gridlines. A single-value widget
    /// imports as `Number`.
    Number,
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
    /// Index of the bound column in the data owner's column list.
    pub column_index: usize,
    /// Human-readable label (typically the column name).
    pub label: String,
    /// Determines tick-formatting strategy.
    pub kind: AxisKind,
    /// Optional unit label rendered near the axis (e.g. "ms", "req/s").
    ///
    /// `None` in v0.6 — data owners do not yet supply unit metadata.
    /// This field is a forward-compatibility seam for later versions.
    pub unit: Option<String>,
}

/// Specification for one Y series.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesSpec {
    /// Index of the bound column in the data owner's column list.
    pub column_index: usize,
    /// Human-readable label (typically the column name).
    pub label: String,
    /// Index into the panel palette; wraps modulo palette length.
    pub color_slot: u8,
    /// Value of the binding's group-by column this series is restricted to.
    /// `None` plots every row. Set when the spec is split by group.
    #[serde(default)]
    pub group_value: Option<String>,
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
/// in the data owner's table. Uses `Vec<usize>` for Y rather than
/// `SmallVec` to keep the dependency footprint minimal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// A manual column selection entered by the user via the picker UI.
#[derive(Debug, Clone)]
pub struct ManualChartSelection {
    pub x_col: usize,
    pub y_cols: Vec<usize>,
    /// Column whose distinct values split every Y column into one series each.
    pub group_by: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn y_scale_defaults_to_linear() {
        assert_eq!(YScale::default(), YScale::Linear);
    }

    #[test]
    fn y_scale_serde_round_trip() {
        for scale in [YScale::Linear, YScale::Log] {
            let json = serde_json::to_string(&scale).expect("serialize");
            let back: YScale = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(scale, back, "round-trip failed for {:?}", scale);
        }
    }

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

    // Seam preservation: these references ensure ChartKind variants remain
    // reachable and cannot silently disappear across refactors.
    #[test]
    fn seam_chart_kind_all_variants_are_reachable() {
        // This test exists solely so the compiler will catch removal of variants.
        let _bar = ChartKind::Bar;
        let _scatter = ChartKind::Scatter;
        let _area = ChartKind::Area;
        let _stacked = ChartKind::StackedBar;
        let _pie = ChartKind::Pie;
        assert_ne!(ChartKind::Bar, ChartKind::Line);
        assert_ne!(ChartKind::Scatter, ChartKind::Line);
        assert_ne!(ChartKind::Area, ChartKind::Line);
        assert_ne!(ChartKind::StackedBar, ChartKind::Line);
        assert_ne!(ChartKind::Pie, ChartKind::Line);
        assert_ne!(ChartKind::Bar, ChartKind::Scatter);
    }

    #[test]
    fn chart_kind_serde_round_trip_new_variants() {
        // Verify that StackedBar and Pie survive a JSON round-trip so persisted
        // chart specs can be read back after these variants are added.
        let kinds = [ChartKind::Area, ChartKind::StackedBar, ChartKind::Pie];
        for kind in kinds {
            let json = serde_json::to_string(&kind).expect("serialize");
            let back: ChartKind = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(kind, back, "round-trip failed for {:?}", kind);
        }
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

    #[test]
    fn binding_spec_serde_round_trip() {
        let bindings = BindingSpec {
            x: 1,
            y: vec![2, 3],
            group_by: Some(4),
            filter: Some("cpu > 0".to_string()),
            aggregation: AggKind::Avg,
        };

        let json = serde_json::to_string(&bindings).expect("serialize");
        let back: BindingSpec = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(bindings, back);
    }
}
