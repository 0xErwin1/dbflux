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
pub mod decimate;
pub mod detect;
pub mod engine;
pub mod legend;
pub mod spec;

pub use detect::{ChartDetection, detect_chart_columns};
pub use engine::{CHART_PALETTE, ChartBuildError, ChartView};
pub use spec::{AxisKind, AxisSpec, ChartKind, ChartSpec, ManualChartSelection, SeriesSpec};
