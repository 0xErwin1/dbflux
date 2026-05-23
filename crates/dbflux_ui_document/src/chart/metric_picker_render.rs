//! `MetricPickerView` — boundary struct for rendering the metric picker rail.
//!
//! This file contains the render helpers for `MetricPickerState`. Following the
//! `KeyValueView`/`LogStreamView` boundary-struct pattern: state lives on
//! `ChartShell` (a GPUI entity), render code lives here (a plain struct with
//! borrowed references), never a GPUI entity itself.
//!
//! Phase 7 of metrics-chart-picker (#96) fills in the full render implementation.
//! This stub makes Phase 6 compile so the state machine can be tested.

use super::metric_picker::MetricPickerState;
use super::shell::ChartShell;
use dbflux_app::MetricCatalogCache;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_ui_base::AppStateEntity;
use gpui::prelude::*;
use gpui::{AnyElement, App, Context, Entity, Window};
use std::sync::Arc;

/// Boundary struct for rendering the metric picker rail.
///
/// Holds borrowed references into the `ChartShell`'s state. `render` is
/// called from the rail render dispatch inside `shell.rs`/`data_grid_panel`.
#[allow(dead_code)] // used in Phase 7 render dispatch
pub struct MetricPickerView<'a> {
    pub state: &'a mut MetricPickerState,
    pub shell: &'a mut ChartShell,
    pub app_state: &'a Entity<AppStateEntity>,
    pub cache: &'a Arc<MetricCatalogCache>,
}

impl<'a> MetricPickerView<'a> {
    /// Render the metric picker rail content.
    #[allow(dead_code)] // used in Phase 7 rail render dispatch
    ///
    /// Layout (Phase 7 target):
    ///   ┌─────────────────────────────────────┐
    ///   │ Namespace column (left, ~120px)     │
    ///   │ Metrics column   (center, flex)     │
    ///   │ Config section   (right, ~160px)    │
    ///   └─────────────────────────────────────┘
    ///
    /// This stub renders a placeholder message until Phase 7 is complete.
    pub fn render(
        &mut self,
        window: &mut Window,
        cx: &mut Context<ChartShell>,
    ) -> impl IntoElement {
        // Ensure filter inputs are created now that we have a Window.
        self.state.ensure_inputs_created(window, cx);

        // Kick namespace fetch if not started.
        let shell_weak = cx.weak_entity();
        let cache = self.cache.clone();
        self.state.ensure_namespaces_loaded(shell_weak, cache, cx);

        // TODO(Phase 7): implement full three-column layout.
        gpui::div()
            .size_full()
            .flex()
            .flex_col()
            .p(Spacing::SM)
            .child(Text::muted("Metric picker — full UI coming in Phase 7"))
    }
}

/// Compile-time check: `MetricPickerView` must not be a GPUI entity.
///
/// If this ever accidentally becomes an Entity, this assertion would need to
/// be removed — do NOT do that without updating the boundary-struct docs.
#[cfg(test)]
mod tests {
    use super::*;

    /// T-MP-BND-01: `MetricPickerView` must remain a plain boundary struct.
    ///
    /// Asserts that its type name does not start with "Entity" (which would
    /// indicate it was accidentally converted to a GPUI entity).
    #[test]
    fn metric_picker_view_is_not_a_gpui_entity() {
        let name = std::any::type_name::<MetricPickerView>();
        assert!(
            !name.starts_with("Entity"),
            "MetricPickerView must be a plain boundary struct, not a GPUI entity; got: {name}"
        );
    }
}
