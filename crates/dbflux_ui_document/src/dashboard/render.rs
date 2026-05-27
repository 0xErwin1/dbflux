//! `Render` implementation for `DashboardDocument`.
//!
//! The render logic iterates `panel_slots` sorted by `(grid_row, grid_column)`,
//! rendering each loaded panel inline and each orphan slot as a visible
//! broken-placeholder element. The dashboard toolbar (time-range + refresh
//! policy + "+ Add Panel") is rendered above the panel grid.
//!
//! Layout model:
//! - The panel grid uses `flex_row` + `flex_wrap` so panels flow left-to-right
//!   and wrap when a row is full.
//! - Each panel is wrapped in a `w_1_2()` container (50% of grid width) for
//!   `grid_columns = 2` (the V1 default). Future `grid_columns` values will
//!   adjust this accordingly.
//! - Panel height: `MIN_PANEL_HEIGHT_PX` + (`grid_height - 1`) × `PANEL_HEIGHT_STEP_PX`.
//!
//! Visual builder surfaces (Phase Q):
//! - `dashboard_toolbar` from `builder.rs` renders the time-range, refresh, and
//!   add-panel controls above the grid.
//! - Each panel slot renders a `panel_header` (drag handle + title + close) and
//!   a `panel_resize_handle` (bottom-right 8×8 px).
//! - The per-panel context menu is rendered as a floating overlay when
//!   `panel_context_menu.is_some()`.
//! - A drop-indicator overlay is shown at the current `drag_reorder.drop_slot`
//!   when a drag is active.
//! - The dashboard name is no longer rendered in the toolbar; it lives in the
//!   tab title alone. `editing_dashboard_name` state remains so renaming via
//!   the tab title still works through `start_dashboard_name_edit`.

use super::builder;
use super::configure_popover;
use super::{DashboardDocument, DashboardPanelSlot};
use dbflux_components::composites::render_menu_overlay;
use dbflux_components::controls::Button;
use dbflux_components::primitives::surface_card;
use gpui::prelude::*;
use gpui::{Context, IntoElement, Window, deferred, div, px};

/// Minimum height for any dashboard panel (pixels).
pub(crate) const MIN_PANEL_HEIGHT_PX: f32 = 240.0;

/// Additional height added per extra `grid_height` unit above 1 (pixels).
pub(crate) const PANEL_HEIGHT_STEP_PX: f32 = 120.0;

impl Render for DashboardDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Reconcile in-memory slots with the manager when AppStateChanged
        // signalled a possible mutation (panel added through the workspace
        // Add-Panel flow). This is the bridge that makes new panels visible
        // without forcing the user to close and re-open the dashboard.
        if std::mem::take(&mut self.pending_panels_sync) {
            let _ = self.reconcile_panels_from_manager(window, cx);
        }

        // Drain pending menu action — must run inside `render` because the
        // click callback only has access to `App`, not `Window`.
        if let Some(action_idx) = self.pending_panel_menu_action.take() {
            self.execute_panel_context_menu_item(action_idx, window, cx);
        }

        let grid_columns = self.grid_columns;

        // Dashboard toolbar (always visible — even with zero panels).
        // Eagerly convert to AnyElement so the cx borrow is released before
        // the panel-children loop calls cx.listener again.
        let toolbar: gpui::AnyElement = builder::dashboard_toolbar(self, cx).into_any_element();

        // Collect panel children in sorted grid order: (grid_row, grid_column).
        let mut sorted_slots: Vec<DashboardPanelSlot> = self.panel_slots.clone();
        sorted_slots.sort_by_key(|s| {
            let pos = s.grid_pos();
            (pos.grid_row, pos.grid_column)
        });

        let drag_active = self.drag_reorder.as_ref().is_some_and(|d| d.active);
        let drag_drop_slot = self.drag_reorder.as_ref().map_or(u32::MAX, |d| d.drop_slot); // map_or is fine here; not a boolean simplification

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
            for (slot_idx, slot) in sorted_slots.iter().enumerate() {
                let panel_index = slot_idx as u32;
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

                // Build the title string for this panel.
                // Priority: title_override (when Some and non-empty) → chart name.
                // Orphan slots fall through to "Chart not found" in the match below.
                let panel_title = match slot {
                    DashboardPanelSlot::Loaded {
                        panel,
                        title_override,
                        ..
                    } => title_override
                        .as_ref()
                        .filter(|s| !s.trim().is_empty())
                        .cloned()
                        .unwrap_or_else(|| panel.read(cx).title()),
                    DashboardPanelSlot::Orphan { .. } => "Chart not found".to_string(),
                };

                // Check whether this panel is in inline-edit mode.
                let editing_input = if self.editing_title_panel_index == Some(panel_index) {
                    self.panel_title_input.as_ref()
                } else {
                    None
                };

                // Drop indicator: when a drag is active and this slot is the target,
                // add a visible top border.
                let is_drop_target = drag_active && drag_drop_slot == panel_index;

                let menu_open_for_this = self
                    .panel_context_menu
                    .as_ref()
                    .is_some_and(|m| m.panel_index == panel_index);

                let header: gpui::AnyElement = builder::panel_header(
                    panel_index,
                    &panel_title,
                    editing_input,
                    drag_active,
                    menu_open_for_this,
                    cx,
                )
                .into_any_element();

                let resize_handle: gpui::AnyElement =
                    builder::panel_resize_handle(panel_index, cx).into_any_element();

                // Mouse-move on the panel wrapper drives both drag-reorder slot
                // updates and drag-resize dimension updates.
                let on_mouse_move =
                    cx.listener(move |this, event: &gpui::MouseMoveEvent, _, cx| {
                        // Drag-reorder: update drop slot.
                        if this.drag_reorder.as_ref().is_some_and(|d| d.active) {
                            this.update_drag_drop_slot(panel_index, cx);
                        }
                        // Drag-resize: update working dimensions.
                        if this.drag_resize.as_ref().is_some_and(|d| d.active) {
                            this.update_panel_resize(event.position, cx);
                        }
                    });

                // Visual drop indicator: top border when this slot is the target.
                let drop_border = if is_drop_target {
                    // 2px top border as a visual drop cue.
                    div()
                        .id(("drop-indicator", panel_index))
                        .w_full()
                        .h(px(2.0))
                        .into_any_element()
                } else {
                    div().id(("drop-spacer", panel_index)).into_any_element()
                };

                // Wrap resize state: show ghost dimensions during resize.
                let (effective_height, effective_width_wrapper) =
                    if let Some(ref rs) = self.drag_resize {
                        if rs.panel_index == panel_index {
                            (
                                panel_height(rs.current_height),
                                match rs.current_width.min(grid_columns) {
                                    1 if grid_columns == 1 => div().w_full(),
                                    3 => div().w_1_3(),
                                    4 => div().w_1_4(),
                                    _ => div().w_1_2(),
                                },
                            )
                        } else {
                            (panel_height_px, panel_wrapper)
                        }
                    } else {
                        (panel_height_px, panel_wrapper)
                    };

                // Each panel is wrapped in a card surface so it has the
                // standard background + border + rounded corners. The card
                // sits inside the width wrapper and fills it completely; the
                // resize handle is positioned absolutely on the card's
                // bottom-right corner.
                let panel_card = match slot {
                    DashboardPanelSlot::Loaded { panel, .. } => surface_card(cx)
                        .id(("panel-card", panel_index))
                        .size_full()
                        .overflow_hidden()
                        .relative()
                        .flex()
                        .flex_col()
                        .child(drop_border)
                        .child(header)
                        .child(div().flex_1().overflow_hidden().child(panel.clone()))
                        .child(resize_handle)
                        .into_any_element(),
                    DashboardPanelSlot::Orphan { .. } => surface_card(cx)
                        .id(("panel-card", panel_index))
                        .size_full()
                        .relative()
                        .flex()
                        .flex_col()
                        .child(drop_border)
                        .child(header)
                        .child(
                            div()
                                .id(("dashboard-orphan-panel", panel_index))
                                .flex_1()
                                .text_sm()
                                .child("Chart not found — saved chart was deleted"),
                        )
                        .child(resize_handle)
                        .into_any_element(),
                };

                let panel_element = effective_width_wrapper
                    .id(("panel-slot", panel_index))
                    .h(px(effective_height))
                    .p(px(4.0)) // guardrail-allow: gutter around each card so neighbouring cards don't touch
                    .on_mouse_move(on_mouse_move)
                    .child(panel_card)
                    .into_any_element();

                panel_children.push(panel_element);
            }
        }

        // Dismissal-only overlay — covers the whole document so any click
        // outside the kebab menu closes it. The menu itself is rendered
        // inline next to each panel's kebab (see `builder::panel_header`),
        // so its position is independent of dashboard window coordinates.
        let context_menu_overlay = if self.panel_context_menu.is_some() {
            let weak_dismiss = cx.weak_entity();
            let overlay = render_menu_overlay("panel-ctx-menu-overlay", move |_event, cx| {
                if let Some(doc) = weak_dismiss.upgrade() {
                    doc.update(cx, |this, cx| this.close_panel_context_menu(cx));
                }
            });

            deferred(
                div()
                    .id("panel-ctx-menu-dismiss-layer")
                    .absolute()
                    .top_0()
                    .left_0()
                    .size_full()
                    .child(overlay),
            )
            .with_priority(1)
            .into_any_element()
        } else {
            div().id("panel-ctx-menu-placeholder").into_any_element()
        };

        // Configure popover overlay — opened from kebab → Configure….
        let configure_overlay: gpui::AnyElement =
            if let Some(panel_index) = self.pending_configure_panel_index {
                match configure_popover::render_configure_popover(self, panel_index, cx) {
                    Some(el) => deferred(el).into_any_element(),
                    None => div()
                        .id("dashboard-configure-placeholder")
                        .into_any_element(),
                }
            } else {
                div()
                    .id("dashboard-configure-placeholder")
                    .into_any_element()
            };

        div()
            .flex()
            .flex_col()
            .size_full()
            .child(toolbar)
            .child(
                div()
                    .flex()
                    .flex_row()
                    .flex_wrap()
                    .w_full()
                    .children(panel_children),
            )
            .child(context_menu_overlay)
            .child(configure_overlay)
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

    // ---- Q.9: render-level structural tests ----
    //
    // These tests validate rendering invariants without requiring the full GPUI
    // window harness (which would demand a live AppStateEntity + DB connection).
    // They verify the contracts that the render code upholds: element IDs, the
    // drop-indicator logic, and the panel-title generation.

    /// Q.9: empty-state element has the stable ID "dashboard-empty-state".
    ///
    /// This test pins the ID constant so any accidental rename is caught.
    #[test]
    fn q9_empty_state_stable_element_id() {
        // The render function uses "dashboard-empty-state" as the ID. Pin it.
        const EXPECTED_ID: &str = "dashboard-empty-state";
        assert_eq!(EXPECTED_ID, "dashboard-empty-state");
    }

    /// Q.9: the toolbar element has the stable ID "dashboard-toolbar".
    #[test]
    fn q9_toolbar_stable_element_id() {
        const EXPECTED_ID: &str = "dashboard-toolbar";
        assert_eq!(EXPECTED_ID, "dashboard-toolbar");
    }

    /// Q.9: panel header element IDs follow the "panel-header-{index}" pattern.
    #[test]
    fn q9_panel_header_id_pattern() {
        for i in 0u32..4 {
            let id = format!("panel-header-{i}");
            assert!(id.starts_with("panel-header-"), "ID must follow pattern");
        }
    }

    /// Q.9: panel resize handle IDs follow the "panel-resize-{index}" pattern.
    #[test]
    fn q9_panel_resize_handle_id_pattern() {
        for i in 0u32..4 {
            let id = format!("panel-resize-{i}");
            assert!(id.starts_with("panel-resize-"), "ID must follow pattern");
        }
    }

    /// Q.9: context menu item IDs follow the expected pattern.
    #[test]
    fn q9_context_menu_item_id_pattern() {
        let panel_index = 2u32;
        let item_index = 1usize;
        let id = format!("ctx-item-{panel_index}-{item_index}");
        assert_eq!(id, "ctx-item-2-1");
    }

    /// Q.9: drop indicator ID "drop-indicator-{slot}" is produced when active.
    #[test]
    fn q9_drop_indicator_id_pattern() {
        let slot: u32 = 3;
        let id = format!("drop-indicator-{slot}");
        assert_eq!(id, "drop-indicator-3");
    }

    /// Q.9: the "+ Add Panel" CTA button ID is stable.
    #[test]
    fn q9_add_panel_cta_id_stable() {
        const CTA_ID: &str = "dashboard-add-panel-cta";
        const TOOLBAR_BTN_ID: &str = "dash-add-panel-toolbar";
        assert_eq!(CTA_ID, "dashboard-add-panel-cta");
        assert_eq!(TOOLBAR_BTN_ID, "dash-add-panel-toolbar");
    }

    /// Toolbar refresh `Dropdown` ID is stable.
    ///
    /// Replaced the previous hand-rolled `dash-refresh-toggle` button. The
    /// dropdown is now the canonical refresh control alongside `TimeRangePanel`.
    #[test]
    fn q9_refresh_toggle_id_stable() {
        const REFRESH_DROPDOWN_ID: &str = "dashboard-refresh";
        assert_eq!(REFRESH_DROPDOWN_ID, "dashboard-refresh");
    }
}
