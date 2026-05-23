//! Chart shell and host trait for the DBFlux chart subsystem.
//!
//! This module provides the `ChartHost` trait seam and the `ChartShell` entity
//! that absorbs chart state from concrete host surfaces such as `DataGridPanel`.
//! Any surface that can mount a chart implements `ChartHost`; the shell owns
//! `ChartView`, hidden-series state, the rail, and toolbar rendering.

pub mod host;
pub mod metric_picker;
mod metric_picker_render;
pub mod shell;
pub mod toolbar;

pub use host::{ChartHost, HostAdapter};
pub use metric_picker::{MetricPickerState, host_supports_metric_catalog};
pub use shell::{ChartRailTab, ChartShell, ChartShellEvent};
pub use toolbar::{ActionHandler, ChartToolbarContext, ChartToolbarHandlers, render_chart_toolbar};
