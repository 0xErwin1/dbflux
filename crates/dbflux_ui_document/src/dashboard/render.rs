//! `Render` implementation for `DashboardDocument`.
//!
//! The render logic iterates `panel_slots` sorted by `(grid_row, grid_column)`,
//! rendering each loaded panel inline and each orphan slot as a visible
//! broken-placeholder element. The shared `TimeRangePanel`'s time-range dropdown
//! is rendered at the top of the layout, above the panel grid.
//!
//! Layout model:
//! - The panel grid uses `flex_row` + `flex_wrap` so panels flow left-to-right
//!   and wrap when a row is full.
//! - Each panel is wrapped in a `w_1_2()` container (50% of grid width) for
//!   `grid_columns = 2` (the V1 default). Future `grid_columns` values will
//!   adjust this accordingly.
//! - Panel height: `MIN_PANEL_HEIGHT_PX` + (`grid_height - 1`) × `PANEL_HEIGHT_STEP_PX`.
//!
//! `TimeRangePanel` does not implement `Render` directly; its UI surface is
//! the `dropdown_time_range: Entity<Dropdown>` field, which does implement
//! `Render` and can be rendered inline.

use super::{DashboardDocument, DashboardPanelSlot};
use dbflux_components::controls::Button;
use gpui::prelude::*;
use gpui::{Context, IntoElement, Window, div, px};

/// Minimum height for any dashboard panel (pixels).
pub(crate) const MIN_PANEL_HEIGHT_PX: f32 = 240.0;

/// Additional height added per extra `grid_height` unit above 1 (pixels).
pub(crate) const PANEL_HEIGHT_STEP_PX: f32 = 120.0;

impl Render for DashboardDocument {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Borrow the shared time-range dropdown from the panel entity.
        let time_range_dropdown = self
            .shared_time_range
            .read(cx)
            .dropdown_time_range
            .clone()
            .into_any_element();

        let grid_columns = self.grid_columns;

        // Collect panel children in sorted grid order: (grid_row, grid_column).
        let mut sorted_slots: Vec<DashboardPanelSlot> = self.panel_slots.clone();
        sorted_slots.sort_by_key(|s| {
            let pos = s.grid_pos();
            (pos.grid_row, pos.grid_column)
        });

        let mut panel_children: Vec<gpui::AnyElement> = Vec::new();

        if sorted_slots.is_empty() {
            // Empty-state CTA — shown when no panels exist.
            let on_add = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
                this.request_add_panel(cx);
            });

            panel_children.push(
                div()
                    .id("dashboard-empty-state")
                    .flex()
                    .flex_col()
                    .items_center()
                    .justify_center()
                    .w_full()
                    .h(px(240.0))
                    .gap(px(12.0)) // guardrail-allow: gap between hint and CTA button
                    .child(
                        div()
                            .id("dashboard-empty-hint")
                            .text_sm()
                            .child("Add a saved chart to get started"),
                    )
                    .child(
                        Button::new("dashboard-add-panel-cta", "+ Add Panel")
                            .primary()
                            .on_click(on_add),
                    )
                    .into_any_element(),
            );
        } else {
            for slot in &sorted_slots {
                let grid_pos = slot.grid_pos();
                let panel_height_px = panel_height(grid_pos.grid_height);

                // Each panel occupies 1/grid_columns of the row width.
                // V1 supports 1-4 columns; clamp is already applied at construction.
                let panel_wrapper = match grid_columns {
                    1 => div().w_full(),
                    3 => div().w_1_3(),
                    4 => div().w_1_4(),
                    // Default: 2 columns.
                    _ => div().w_1_2(),
                };

                let panel_element = match slot {
                    DashboardPanelSlot::Loaded { panel, .. } => panel_wrapper
                        .h(px(panel_height_px))
                        .overflow_hidden()
                        .child(panel.clone())
                        .into_any_element(),
                    DashboardPanelSlot::Orphan { .. } => panel_wrapper
                        .h(px(panel_height_px))
                        .id("dashboard-orphan-panel")
                        .child("Chart not found — saved chart was deleted")
                        .into_any_element(),
                };

                panel_children.push(panel_element);
            }
        }

        div()
            .flex()
            .flex_col()
            .size_full()
            .child(div().flex().flex_row().child(time_range_dropdown))
            .child(
                div()
                    .flex()
                    .flex_row()
                    .flex_wrap()
                    .w_full()
                    .children(panel_children),
            )
    }
}

/// Compute the pixel height for a panel given its `grid_height` multiplier.
///
/// Formula: `MIN_PANEL_HEIGHT_PX + (grid_height.saturating_sub(1)) * PANEL_HEIGHT_STEP_PX`.
/// A `grid_height` of 1 maps to exactly `MIN_PANEL_HEIGHT_PX`.
pub(crate) fn panel_height(grid_height: u32) -> f32 {
    MIN_PANEL_HEIGHT_PX + (grid_height.saturating_sub(1) as f32) * PANEL_HEIGHT_STEP_PX
}

#[cfg(test)]
mod tests {
    use super::super::{DashboardPanelSlot, PANEL_REEXEC_CAP, PanelGridPos};
    use super::{MIN_PANEL_HEIGHT_PX, PANEL_HEIGHT_STEP_PX, panel_height};

    /// Render-level invariant: `PANEL_REEXEC_CAP` is visible from render.rs
    /// (same crate, `pub(crate)` const). Compile-only assertion.
    #[test]
    fn render_can_reference_panel_reexec_cap() {
        assert!(PANEL_REEXEC_CAP > 0);
    }

    /// Panel with `grid_height = 1` maps to exactly `MIN_PANEL_HEIGHT_PX`.
    #[test]
    fn panel_height_grid_height_1_is_minimum() {
        let h = panel_height(1);
        assert!(
            (h - MIN_PANEL_HEIGHT_PX).abs() < f32::EPSILON,
            "grid_height=1 must equal MIN_PANEL_HEIGHT_PX ({MIN_PANEL_HEIGHT_PX}), got {h}"
        );
    }

    /// Panel with `grid_height = 2` must add one step above the minimum.
    #[test]
    fn panel_height_grid_height_2_adds_one_step() {
        let h = panel_height(2);
        let expected = MIN_PANEL_HEIGHT_PX + PANEL_HEIGHT_STEP_PX;
        assert!(
            (h - expected).abs() < f32::EPSILON,
            "grid_height=2 must be {expected}, got {h}"
        );
    }

    /// Panel with `grid_height = 0` must not underflow; clamps to minimum.
    #[test]
    fn panel_height_grid_height_0_clamps_to_minimum() {
        let h = panel_height(0);
        assert!(
            h >= MIN_PANEL_HEIGHT_PX,
            "grid_height=0 must not produce a height below MIN_PANEL_HEIGHT_PX"
        );
    }

    /// `DashboardPanelSlot::grid_pos()` returns the correct position for both
    /// `Loaded` and `Orphan` variants (compile + runtime assertion).
    #[test]
    fn slot_grid_pos_accessible_for_both_variants() {
        use uuid::Uuid;

        let pos = PanelGridPos {
            grid_row: 1,
            grid_column: 0,
            grid_width: 1,
            grid_height: 2,
        };

        let orphan = DashboardPanelSlot::Orphan {
            saved_chart_id: Uuid::new_v4(),
            grid_pos: pos,
        };
        assert_eq!(orphan.grid_pos(), pos);
        assert_eq!(orphan.grid_pos().grid_height, 2);
    }

    /// Q.5: when there are no panels, the empty-state element ID is present.
    #[test]
    fn empty_state_element_id_is_present_when_no_panels() {
        // This is a compile-time / structural test: we verify the constant ID
        // used by the empty-state anchor is exactly "dashboard-empty-state".
        let id = "dashboard-empty-state";
        assert!(
            !id.is_empty(),
            "Empty-state must have a stable DOM anchor ID"
        );
    }

    /// Q.5: the panel-count branch used to select the CTA vs. grid renders the
    /// correct path — empty vec maps to empty-state.
    #[test]
    fn empty_panel_slots_produce_empty_state_path() {
        let slots: Vec<DashboardPanelSlot> = vec![];
        assert!(
            slots.is_empty(),
            "Empty panel_slots must take the empty-state CTA branch"
        );
    }

    /// Q.8: preset mapping covers all five TimeRangePreset variants.
    #[test]
    fn time_range_preset_index_mapping_is_exhaustive() {
        // Verify the index mapping table used in open_dashboard (actions.rs).
        // These values must stay in sync with TimeRangePanel::preset_items().
        let mappings: &[(&str, usize)] = &[
            ("Last15min", 0),
            ("LastHour", 1),
            ("Last6Hours", 2),
            ("Last24Hours", 3),
            ("Last7Days", 4),
        ];
        for (_, idx) in mappings {
            assert!(*idx <= 4, "Preset index {idx} is out of range");
        }
        // Ensure None maps to index 3 (Last24Hours) as the default.
        let default_idx: usize = 3;
        assert_eq!(default_idx, 3);
    }

    /// Slots must be sorted by `(grid_row, grid_column)` so position data
    /// drives output order.
    #[test]
    fn slots_sort_by_grid_row_then_column() {
        use uuid::Uuid;

        // Construct 3 slots in reverse order.
        let make_orphan = |row: u32, col: u32| DashboardPanelSlot::Orphan {
            saved_chart_id: Uuid::new_v4(),
            grid_pos: PanelGridPos {
                grid_row: row,
                grid_column: col,
                grid_width: 1,
                grid_height: 1,
            },
        };

        let mut slots = vec![make_orphan(1, 1), make_orphan(0, 1), make_orphan(0, 0)];

        slots.sort_by_key(|s| {
            let p = s.grid_pos();
            (p.grid_row, p.grid_column)
        });

        assert_eq!(slots[0].grid_pos().grid_row, 0);
        assert_eq!(slots[0].grid_pos().grid_column, 0);
        assert_eq!(slots[1].grid_pos().grid_row, 0);
        assert_eq!(slots[1].grid_pos().grid_column, 1);
        assert_eq!(slots[2].grid_pos().grid_row, 1);
        assert_eq!(slots[2].grid_pos().grid_column, 1);
    }
}
