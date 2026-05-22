//! Pure UI components and design tokens for DBFlux.
//!
//! This crate contains UI primitives, controls, and composites that depend
//! only on `gpui` and `gpui_component`. It has zero domain dependencies.

#![allow(clippy::type_complexity)]
// Required by the `#[gpui::test]` attribute macro expansion in test modules.
#![recursion_limit = "512"]

pub mod actions;
pub mod density;
pub mod helpers;
pub mod icon;
pub mod semantic;
pub mod tokens;
pub mod typography;

pub mod chart;
pub mod common;
pub mod components;
pub mod composites;
pub mod controls;
pub mod icons;
pub mod modals;
pub mod primitives;
pub mod result_panel;
pub mod result_view;
pub mod saved_chart;
pub mod theme;

pub mod sql_preview;

#[cfg(test)]
mod style_guardrails;

pub use composites::refresh_split_button;
pub use saved_chart::{
    SavedChart, SavedChartManager, SavedChartRefreshPolicy, SavedChartStore, TimeRangePreset,
    open_saved_charts_store,
};
pub use sql_preview::{SqlGenerationType, SqlPreviewContext};
