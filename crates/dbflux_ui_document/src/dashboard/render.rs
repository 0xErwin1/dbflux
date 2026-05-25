//! `Render` implementation for `DashboardDocument`.
//!
//! The render logic iterates `panel_slots`, rendering each loaded panel inline
//! and each orphan slot as a visible broken-placeholder element. The shared
//! `TimeRangePanel`'s time-range dropdown is rendered at the top of the layout,
//! above the panel grid.
//!
//! `TimeRangePanel` does not implement `Render` directly; its UI surface is
//! the `dropdown_time_range: Entity<Dropdown>` field, which does implement
//! `Render` and can be rendered inline.

use super::{DashboardDocument, DashboardPanelSlot};
use gpui::prelude::*;
use gpui::{Context, IntoElement, Window, div};

impl Render for DashboardDocument {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Borrow the shared time-range dropdown from the panel entity.
        let time_range_dropdown = self
            .shared_time_range
            .read(cx)
            .dropdown_time_range
            .clone()
            .into_any_element();

        let mut panel_children: Vec<gpui::AnyElement> = Vec::new();

        if self.panel_slots.is_empty() {
            // Empty-state placeholder — provides a stable DOM anchor for acceptance tests.
            panel_children.push(
                div()
                    .id("dashboard-empty-state")
                    .child("No panels — add charts to this dashboard.")
                    .into_any_element(),
            );
        } else {
            for slot in &self.panel_slots {
                match slot {
                    DashboardPanelSlot::Loaded { panel } => {
                        panel_children.push(panel.clone().into_any_element());
                    }
                    DashboardPanelSlot::Orphan { .. } => {
                        panel_children.push(
                            div()
                                .id("dashboard-orphan-panel")
                                .child("Chart not found — saved chart was deleted")
                                .into_any_element(),
                        );
                    }
                }
            }
        }

        div()
            .flex()
            .flex_col()
            .size_full()
            .child(div().flex().flex_row().child(time_range_dropdown))
            .children(panel_children)
    }
}

#[cfg(test)]
mod tests {
    use super::super::PANEL_REEXEC_CAP;

    /// Render-level invariant: `PANEL_REEXEC_CAP` is visible from render.rs
    /// (same crate, `pub(crate)` const). Compile-only assertion.
    #[test]
    fn render_can_reference_panel_reexec_cap() {
        assert!(PANEL_REEXEC_CAP > 0);
    }
}
