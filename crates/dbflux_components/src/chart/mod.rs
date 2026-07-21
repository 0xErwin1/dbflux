//! Driver-agnostic line-chart engine for DBFlux.
//!
//! The chart module owns the full pipeline from query-result introspection to
//! GPUI rendering:
//!
//! 1. **`detect`** — auto-detects suitable columns from a `QueryResult`
//!    using `ColumnKind` semantics only (never type names or driver IDs).
//! 2. **`spec`** — chart and series specification types; constructors for
//!    detection-driven and manual column selection.
//! 3. **`decimate`** — LTTB downsampling to keep paint fast on large datasets.
//! 4. **`axis`** — tick generation and label formatting for numeric and time axes.
//! 5. **`legend`** — pure element factory for the legend pill row.
//! 6. **`engine`** — `ChartView`, the GPUI entity that owns state and renders
//!    the canvas.

pub mod axis;
pub mod axis_bar;
pub mod data_source;
pub mod decimate;
pub mod detect;
pub mod engine;
pub mod legend;
pub mod point_inspector;
pub mod spec;
pub mod stats;

pub use axis_bar::{AxisPill, axis_bar_element};
pub use data_source::{
    AuditAggregateSpec, AuditGroupBy, AuditSource, ChartDataPlan, ChartDataSource,
    ChartSourceDescription, ChartSourceError, InstanceMetricSource, MetricSource, TimeWindow,
    resolve_source,
};
pub use detect::{ChartDetection, detect_chart_columns};
pub use engine::{ChartBuildError, ChartView, format_x_value, format_y_value};
pub use legend::legend_element;
pub use point_inspector::{DataPointRef, SourceRowRef, point_inspector_element};
pub use spec::{
    AggKind, AxisKind, AxisSpec, BindingSpec, ChartKind, ChartSpec, ManualChartSelection,
    SeriesSpec, YScale,
};
pub use stats::{
    SeriesStats, compute_series_stats, count_columns_for_why, format_resolution, format_span,
};
