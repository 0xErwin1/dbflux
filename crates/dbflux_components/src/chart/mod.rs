//! Driver-agnostic line-chart engine for DBFlux.
//!
//! The chart module owns the full pipeline from query-result introspection to
//! GPUI rendering. The product-neutral half lives in `bolt::chart` — the
//! column contract, decimation, axis ticks, the legend, the axis bar, the
//! point inspector, the spec value types and the readout formatting — and is
//! re-exported here at its historical paths. What stays product-side is what
//! reads a `dbflux_core` type or carries a product name:
//!
//! - **`data_source`** — the chart data-source seam: the concrete sources
//!   (query, collection, metric, instance metric, audit) and the plans that
//!   carry `QueryRequest`s.
//! - **`spec`** — the persisted `ChartSpec` composite and its constructors,
//!   which bind to `dbflux_core::ColumnMeta` columns and split series from a
//!   `QueryResult`'s group cells.
//! - **`engine`** — `ChartView`, the GPUI entity, whose `build` interprets
//!   `Value` cells (series construction) and owns the render model, the
//!   hover/keyboard interaction and the canvas painting.
//! - the adapters (`axis_bar`, `point_inspector`, `detect`, `stats`) — thin
//!   wrappers that convert `dbflux_core` columns to `bolt::chart::ChartColumn`
//!   and answer the product's translated labels, so call sites keep compiling
//!   untouched.

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

mod compat;

pub use axis_bar::{AxisPickerOption, AxisPill, axis_bar_element, axis_picker_options};
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
