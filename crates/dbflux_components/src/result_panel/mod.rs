//! `ResultPanel` — shared chrome host for all result-rendering views.
//!
//! This module provides:
//! - `ResultPanel`: a GPUI entity that owns the mode bar, refresh dropdown,
//!   focus root, and footer chrome shared across all document result views.
//! - `ResultPanelView`: enum that selects the active child view.
//!
//! # Current state (Arc 0 — scaffolding)
//!
//! The structs and variants are declared but no real rendering or wiring is
//! implemented. `ResultPanel::render` returns an empty `div()` placeholder.
//! Actual chrome extraction from documents begins in Arc 2 (DataDocument).
//!
//! All variants and unused fields are annotated with `#[allow(dead_code)]`
//! so that the scaffolding compiles cleanly without triggering warnings.

#![allow(dead_code)]

use gpui::{AnyView, Context, IntoElement, Render, Window, div};

/// Selects which child view `ResultPanel` hosts.
///
/// Each variant holds an `AnyView` for the corresponding view kind. This keeps
/// `ResultPanelView` non-generic so it can be stored in `ResultPanel` without
/// infecting container documents with a type parameter.
///
/// Concrete entity types (e.g. `Entity<TableView>`) are wrapped via
/// `Entity::into_any()` when setting the active view. New view kinds are added
/// here in later arcs.
#[allow(dead_code)]
pub enum ResultPanelView {
    /// Relational table grid.
    Table(AnyView),
    /// Document-tree renderer for document-DB collections.
    DocumentTree(AnyView),
    /// JSON text renderer.
    Json(AnyView),
    /// Plain text renderer.
    Text(AnyView),
    /// Raw/binary renderer.
    Raw(AnyView),
    /// Chart renderer (reuses `dbflux_components::chart::ChartView`).
    Chart(AnyView),
    /// Redis-style key-value browser.
    KeyValue(AnyView),
    /// Audit / log-stream event viewer.
    LogStream(AnyView),
}

/// Shared chrome host for all result-rendering views.
///
/// Owns: mode bar, refresh dropdown, refresh timer, focus root, footer/status
/// bar. Delegates actual cell rendering to the `ResultPanelView` child.
///
/// In Arc 0 this is an empty placeholder that compiles but renders nothing.
/// Chrome wiring is added in Arc 2 when the Data document slice begins.
pub struct ResultPanel {
    // Placeholder — real fields added in Arc 2.
    _placeholder: (),
}

impl ResultPanel {
    pub fn new(_cx: &mut Context<Self>) -> Self {
        Self { _placeholder: () }
    }
}

impl Render for ResultPanel {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        // Arc 0 placeholder: actual chrome rendering is wired in Arc 2.
        div()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::{AppContext, TestAppContext};

    /// Compile-time structural test: `ResultPanel` can be constructed inside a
    /// GPUI test context and the `Render` impl compiles.
    ///
    /// This is the TDD compile-time guard for the scaffolding. Chrome behavior
    /// tests are added in Arc 2 when the Data document slice begins.
    #[gpui::test]
    fn result_panel_constructs_in_test_context(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let _panel = cx.new(ResultPanel::new);
        });
    }
}
