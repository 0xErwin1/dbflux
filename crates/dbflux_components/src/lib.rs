//! Pure UI components and design tokens for DBFlux.
//!
//! This crate contains UI primitives, controls, and composites that depend
//! only on `gpui` and `gpui_component`. It has zero domain dependencies.

#![allow(clippy::type_complexity)]

pub mod actions;
pub mod density;
pub mod helpers;
pub mod icon;
pub mod semantic;
pub mod tokens;
pub mod typography;

pub mod chart;
pub mod composites;
pub mod controls;
pub mod primitives;
pub mod result_panel;
pub mod saved_chart;

pub use saved_chart::{
    SavedChart, SavedChartManager, SavedChartRefreshPolicy, SavedChartStore, TimeRangePreset,
    open_saved_charts_store,
};
