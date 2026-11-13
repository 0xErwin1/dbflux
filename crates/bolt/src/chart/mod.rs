//! The chart: decimation, axis ticks, and the chart chrome elements —
//! legend, axis bar, point inspector — over a product-supplied column contract.
//!
//! A chart renderer must not know where its data came from: a file, a feed,
//! a poll, a log. What the chart reads is a list of
//! [`column::ChartColumn`]s — a display name and a semantic
//! [`bolt::data::ColumnKind`] — plus resolved numeric points once the data
//! owner has interpreted its own values. Interpreting raw cell values
//! (parsing decimals, reading timestamps) stays with the data owner; what a
//! series, a tick and a pill draw lives here.
//!
//! The translated labels of the chrome elements are product text: the legend
//! asks the installed [`bolt::strings`] translator; the axis bar and the point
//! inspector take their labels as supplied values, because they translate at
//! element-build time where no `App` is at hand. The product fills both, the
//! way it fills every [`bolt::strings`] lookup.

pub mod axis;
pub mod axis_bar;
pub mod column;
pub mod decimate;
pub mod detect;
pub mod format;
pub mod legend;
pub mod point_inspector;
pub mod spec;
pub mod stats;

pub use axis::{TickLabel, ticks_categorical, ticks_log, ticks_numeric, ticks_time};
pub use axis_bar::{
    AxisBarChrome, AxisPickerOption, AxisPill, axis_bar_element, axis_picker_options,
};
pub use column::ChartColumn;
pub use decimate::{lttb, lttb_with_indices};
pub use detect::{ChartDetection, detect_chart_columns};
pub use format::{format_x_value, format_y_value};
pub use legend::legend_element;
pub use point_inspector::{
    DataPointRef, PointInspectorText, SourceRowRef, point_inspector_element,
};
pub use spec::{
    AggKind, AxisKind, AxisSpec, BindingSpec, ChartKind, ManualChartSelection, SeriesSpec, YScale,
};
pub use stats::{
    SeriesStats, compute_series_stats, count_chart_columns, format_resolution, format_span,
    hit_test_focused_series, interpolate_y_at_x,
};
