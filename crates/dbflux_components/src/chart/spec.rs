//! Chart specification types that define what to render.

use dbflux_core::ColumnKind;

/// Extension seam for chart kinds. Only `Line` ships in v0.6.
///
/// Adding a new variant here is a non-breaking change thanks to `#[non_exhaustive]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChartKind {
    Line,
}

/// Axis classification used to pick the appropriate tick and label format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AxisKind {
    /// Time axis; ticks are formatted as human-readable dates/times.
    Time,
    /// Numeric axis; ticks are formatted as decimal numbers.
    Numeric,
}

/// Specification for a single axis.
#[derive(Debug, Clone)]
pub struct AxisSpec {
    /// Index into `QueryResult.columns` for the data source.
    pub column_index: usize,
    /// Human-readable label (typically the column name).
    pub label: String,
    /// Determines tick-formatting strategy.
    pub kind: AxisKind,
}

/// Specification for one Y series.
#[derive(Debug, Clone)]
pub struct SeriesSpec {
    /// Index into `QueryResult.columns` for the data source.
    pub column_index: usize,
    /// Human-readable label (typically the column name).
    pub label: String,
    /// Index into the panel palette; wraps modulo palette length.
    pub color_slot: u8,
}

/// Full specification describing what and how to render a chart.
///
/// No `Default` impl: a chart always requires an explicit column selection.
/// Use `ChartSpec::from_detection` or `ChartSpec::from_manual_selection` as constructors.
#[derive(Debug, Clone)]
pub struct ChartSpec {
    pub kind: ChartKind,
    pub x_axis: AxisSpec,
    pub series: Vec<SeriesSpec>,
    /// Whether the legend is visible. Follows the rule: visible by default when
    /// `series.len() > 1`; per-panel toggle stored separately on `DataGridPanel`.
    pub legend_visible: bool,
    /// Point count threshold before LTTB decimation is applied. Default: 10 000.
    pub decimation_threshold: usize,
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
        };

        let series: Vec<SeriesSpec> = numeric_cols
            .into_iter()
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
        };

        let y_cols: Vec<usize> = selection
            .y_cols
            .iter()
            .copied()
            .filter(|&i| i != selection.x_col && i < columns.len())
            .collect();

        let series: Vec<SeriesSpec> = y_cols
            .into_iter()
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
        })
    }
}
