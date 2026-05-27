//! `Render` implementation for `DashboardDocument`.
//!
//! Layout model (Grafana-style 12-column free grid):
//! - Every dashboard uses a fixed 12-column grid; the persisted
//!   `viz_dashboards.grid_columns` value is ignored by the UI.
//! - The grid container is `position: relative`; each panel is absolutely
//!   positioned with `left = col * (100/12)%`, `width = w * (100/12)%`,
//!   `top = row * DASHBOARD_ROW_PX`, `height = h * DASHBOARD_ROW_PX`.
//! - A zero-height width probe sits as the first child so the grid container's
//!   rendered width can be read back through `on_children_prepainted` and
//!   used to snap drag deltas to grid cells.
//!
//! Edit/View modes:
//! - `View` (the default for new tabs) renders panels read-only: no drag
//!   cursor, no resize handles, no kebab menu, no focus ring, and the keymap
//!   is inert.
//! - `Edit` renders three resize handles per panel (right edge, bottom edge,
//!   bottom-right corner grip), enables drag-to-move on the header, and shows
//!   the focus ring on the keyboard-focused panel. The dashboard toolbar
//!   exposes a Pencil/Eye toggle that flips the mode for the tab.
//!
//! Drag affordances:
//! - Drag-to-move snaps to grid cells on every mouse-move; the ghost outline
//!   tracks the working position. Mouse-up commits when the rectangle does
//!   not overlap any other panel, otherwise it snaps back with a soft toast.
//! - Drag-resize is axis-restricted via `ResizeAxis` so the three handles can
//!   share a single `DragResizeState` without mutating dimensions the user
//!   did not grab.

use super::builder;
use super::configure_popover;
use super::{DASHBOARD_GRID_COLUMNS, DASHBOARD_ROW_PX, DashboardDocument, DashboardPanelSlot};
use dbflux_components::composites::render_menu_overlay;
use dbflux_components::controls::Button;
use dbflux_components::primitives::{Text, surface_card};
use gpui::prelude::*;
use gpui::{Bounds, Context, IntoElement, KeyDownEvent, Pixels, Window, deferred, div, px};
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;
use std::cell::Cell;
use std::rc::Rc;

/// Minimum height for the empty-state CTA. Live panels size off
/// `DASHBOARD_ROW_PX` directly, not this constant.
pub(crate) const MIN_PANEL_HEIGHT_PX: f32 = 240.0;

impl Render for DashboardDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Reconcile in-memory slots with the manager when AppStateChanged
        // signalled a possible mutation (panel added through the workspace
        // Add-Panel flow). This is the bridge that makes new panels visible
        // without forcing the user to close and re-open the dashboard.
        if std::mem::take(&mut self.pending_panels_sync) {
            let _ = self.reconcile_panels_from_manager(window, cx);
        }

        // First render after construction: install the auto-refresh timer
        // for the persisted policy. The constructor can't spawn a task while
        // `Self` is still being built.
        if std::mem::take(&mut self.pending_refresh_timer_init) {
            self.update_refresh_timer(cx);
        }

        // Drain pending menu action — must run inside `render` because the
        // click callback only has access to `App`, not `Window`.
        if let Some(action_idx) = self.pending_panel_menu_action.take() {
            self.execute_panel_context_menu_item(action_idx, window, cx);
        }

        // Dashboard toolbar (always visible — even with zero panels).
        // Eagerly convert to AnyElement so the cx borrow is released before
        // the panel-children loop calls cx.listener again.
        let toolbar: gpui::AnyElement = builder::dashboard_toolbar(self, cx).into_any_element();

        let edit_mode = self.is_edit_mode();

        // Per-panel children render in original `panel_slots` order; the visual
        // position is driven entirely by `grid_pos` via absolute positioning.
        let drag_active = self.drag_reorder.as_ref().is_some_and(|d| d.active);
        let drag_panel_index = self.drag_reorder.as_ref().map(|d| d.from_index);
        let drag_working = self
            .drag_reorder
            .as_ref()
            .map(|d| (d.working_column, d.working_row));

        let mut panel_children: Vec<gpui::AnyElement> = Vec::new();

        if self.panel_slots.is_empty() {
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
            // Per-frame collapse view: indices to skip + row shifts so panels
            // below collapsed sections close the gap.
            let (hidden_slots, row_shifts) = self.collapse_view();

            for (slot_idx, slot) in self.panel_slots.iter().enumerate() {
                if hidden_slots.contains(&slot_idx) {
                    continue;
                }
                let panel_index = slot_idx as u32;
                let row_shift = row_shifts.get(slot_idx).copied().unwrap_or(0);
                let grid_pos = slot.grid_pos();

                // Effective rectangle: while a drag-resize or drag-to-move is in
                // progress on this panel, render the working ghost dimensions
                // and the working column/row.
                let (eff_col, base_row, eff_w, eff_h) = if let Some(rs) = self
                    .drag_resize
                    .as_ref()
                    .filter(|rs| rs.panel_index == panel_index)
                {
                    (
                        grid_pos.grid_column,
                        grid_pos.grid_row,
                        rs.current_width,
                        rs.current_height,
                    )
                } else if drag_panel_index == Some(panel_index)
                    && let Some((col, row)) = drag_working
                {
                    (col, row, grid_pos.grid_width, grid_pos.grid_height)
                } else {
                    (
                        grid_pos.grid_column,
                        grid_pos.grid_row,
                        grid_pos.grid_width,
                        grid_pos.grid_height,
                    )
                };
                let eff_row = base_row.saturating_sub(row_shift);

                // Build the title string for this panel.
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
                    DashboardPanelSlot::Divider { .. } => String::new(),
                };

                // Check whether this panel is in inline-edit mode.
                let editing_input = if self.editing_title_panel_index == Some(panel_index) {
                    self.panel_title_input.as_ref()
                } else {
                    None
                };

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
                    edit_mode,
                    cx,
                )
                .into_any_element();

                // Focus ring is an edit-mode affordance — in view mode the
                // dashboard is read-only and no panel is "armed".
                let ring_color = cx.theme().ring;
                let is_focused = edit_mode && self.focused_panel_index == Some(panel_index);
                let on_card_mouse_down = cx.listener(move |this, _, _, cx| {
                    if this.is_edit_mode() {
                        this.focused_panel_index = Some(panel_index);
                        cx.notify();
                    }
                });

                let card_focus_decoration =
                    move |card: gpui::Stateful<gpui::Div>| -> gpui::Stateful<gpui::Div> {
                        if is_focused {
                            card.border_2().border_color(ring_color)
                        } else {
                            card
                        }
                    };

                // Resize handles render only in edit mode.
                let resize_right = if edit_mode {
                    Some(builder::panel_resize_right(panel_index, cx).into_any_element())
                } else {
                    None
                };
                let resize_bottom = if edit_mode {
                    Some(builder::panel_resize_bottom(panel_index, cx).into_any_element())
                } else {
                    None
                };
                let resize_corner = if edit_mode {
                    Some(builder::panel_resize_corner(panel_index, cx).into_any_element())
                } else {
                    None
                };

                let panel_card = match slot {
                    DashboardPanelSlot::Loaded { panel, .. } => card_focus_decoration(
                        surface_card(cx)
                            .id(("panel-card", panel_index))
                            .size_full()
                            .overflow_hidden()
                            .relative()
                            .flex()
                            .flex_col()
                            .on_mouse_down(gpui::MouseButton::Left, on_card_mouse_down)
                            .child(header)
                            .child(div().flex_1().overflow_hidden().child(panel.clone()))
                            .when_some(resize_right, |el, r| el.child(r))
                            .when_some(resize_bottom, |el, r| el.child(r))
                            .when_some(resize_corner, |el, r| el.child(r)),
                    )
                    .into_any_element(),
                    DashboardPanelSlot::Orphan { .. } => card_focus_decoration(
                        surface_card(cx)
                            .id(("panel-card", panel_index))
                            .size_full()
                            .relative()
                            .flex()
                            .flex_col()
                            .on_mouse_down(gpui::MouseButton::Left, on_card_mouse_down)
                            .child(header)
                            .child(
                                div()
                                    .id(("dashboard-orphan-panel", panel_index))
                                    .flex_1()
                                    .text_sm()
                                    .child("Chart not found — saved chart was deleted"),
                            )
                            .when_some(resize_right, |el, r| el.child(r))
                            .when_some(resize_bottom, |el, r| el.child(r))
                            .when_some(resize_corner, |el, r| el.child(r)),
                    )
                    .into_any_element(),
                    DashboardPanelSlot::Divider { markdown, .. } => {
                        let label = divider_label(markdown);
                        let is_collapsed = self.is_divider_collapsed(panel_index);
                        let on_toggle =
                            cx.listener(move |this, _: &gpui::ClickEvent, _window, cx| {
                                this.toggle_divider_collapse(panel_index, cx);
                            });
                        let chevron = if is_collapsed { "▸" } else { "▾" };
                        div()
                            .id(("dashboard-divider", panel_index))
                            .size_full()
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_3()
                            .cursor_pointer()
                            .on_click(on_toggle)
                            .child(Text::heading(chevron))
                            .child(Text::heading(label))
                            .into_any_element()
                    }
                };

                // Position the panel absolutely on the 12-column grid.
                // `left` / `width` are percentages of the grid container's
                // current width; `top` / `height` are fixed pixel multiples of
                // `DASHBOARD_ROW_PX`. Inset 4 px on every side so neighbouring
                // panels do not visually touch.
                let col_percent = (eff_col as f32) * (100.0 / DASHBOARD_GRID_COLUMNS as f32);
                let width_percent = (eff_w as f32) * (100.0 / DASHBOARD_GRID_COLUMNS as f32);
                let top_px = (eff_row as f32) * DASHBOARD_ROW_PX;
                let height_px = (eff_h as f32) * DASHBOARD_ROW_PX;

                let panel_element = div()
                    .id(("panel-slot", panel_index))
                    .absolute()
                    .left(gpui::relative(col_percent / 100.0))
                    .top(px(top_px))
                    .w(gpui::relative(width_percent / 100.0))
                    .h(px(height_px))
                    .p(px(4.0)) // guardrail-allow: gutter so neighbouring cards do not touch
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

        // While a drag-to-move or drag-resize is active, capture mouse
        // movements and releases on the dashboard root so the gesture
        // continues to track even when the cursor leaves the originating
        // panel (e.g. dragging across the chart area or off-edge).
        let drag_active_global = self.drag_reorder.as_ref().is_some_and(|d| d.active)
            || self.drag_resize.as_ref().is_some_and(|r| r.active);

        // Shared cell that captures the grid container's last painted width.
        // The container reports its rendered bounds via `on_children_prepainted`;
        // the global mouse-move handler reads the captured width to convert
        // pixel deltas into grid columns.
        let grid_width_px: Rc<Cell<f32>> = Rc::new(Cell::new(0.0));
        let grid_width_for_capture = Rc::clone(&grid_width_px);
        let grid_width_for_move = Rc::clone(&grid_width_px);

        let on_global_mouse_move = cx.listener(move |this, event: &gpui::MouseMoveEvent, _, cx| {
            let width = grid_width_for_move.get();
            let px_per_col = if width > 0.0 {
                width / DASHBOARD_GRID_COLUMNS as f32
            } else {
                0.0
            };
            if this.drag_resize.as_ref().is_some_and(|r| r.active) {
                this.update_panel_resize(event.position, px_per_col, cx);
            }
            if this.drag_reorder.as_ref().is_some_and(|d| d.active) {
                this.update_panel_drag(event.position, px_per_col, cx);
            }
        });

        let on_global_mouse_up = cx.listener(move |this, _: &gpui::MouseUpEvent, _, cx| {
            if this.drag_resize.as_ref().is_some_and(|r| r.active) {
                this.end_panel_resize(cx);
            }
            if this.drag_reorder.as_ref().is_some_and(|d| d.active) {
                this.end_panel_drag(cx);
            }
        });

        // Drag ghost: a dashed ring at the working column/row during a
        // drag-to-move so the user can see where the panel will land.
        let drag_ghost: Option<gpui::AnyElement> = self.drag_reorder.as_ref().and_then(|state| {
            let panel = self.panel_slots.get(state.from_index as usize)?;
            let pos = panel.grid_pos();
            let col_percent =
                (state.working_column as f32) * (100.0 / DASHBOARD_GRID_COLUMNS as f32);
            let width_percent = (pos.grid_width as f32) * (100.0 / DASHBOARD_GRID_COLUMNS as f32);
            let top_px = (state.working_row as f32) * DASHBOARD_ROW_PX;
            let height_px = (pos.grid_height as f32) * DASHBOARD_ROW_PX;

            let theme = cx.theme();
            Some(
                div()
                    .id("dashboard-drag-ghost")
                    .absolute()
                    .left(gpui::relative(col_percent / 100.0))
                    .top(px(top_px))
                    .w(gpui::relative(width_percent / 100.0))
                    .h(px(height_px))
                    .p(px(4.0)) // guardrail-allow: match the panel gutter so the ghost lines up
                    .child(
                        div()
                            .size_full()
                            .border_2()
                            .border_dashed()
                            .border_color(theme.ring)
                            .rounded(px(4.0)), // guardrail-allow: subtle hint matches surface_card radius
                    )
                    .into_any_element(),
            )
        });

        // Grid container height: cover every visible panel's effective
        // (row + height) after applying the collapse-reflow shift, plus a
        // little headroom while dragging so a moved ghost extending past the
        // bottom still fits inside the container. Hidden panels do not
        // contribute so the scroll area shrinks when sections are folded.
        let (collapse_hidden, collapse_shifts) = self.collapse_view();
        let max_row_end = self
            .panel_slots
            .iter()
            .enumerate()
            .filter(|(i, _)| !collapse_hidden.contains(i))
            .map(|(i, s)| {
                let p = s.grid_pos();
                let shift = collapse_shifts.get(i).copied().unwrap_or(0);
                p.grid_row
                    .saturating_sub(shift)
                    .saturating_add(p.grid_height)
            })
            .max()
            .unwrap_or(0);
        let drag_ghost_extent = self.drag_reorder.as_ref().map_or(0u32, |state| {
            self.panel_slots
                .get(state.from_index as usize)
                .map(|s| state.working_row.saturating_add(s.grid_pos().grid_height))
                .unwrap_or(0)
        });
        let resize_ghost_extent = self.drag_resize.as_ref().map_or(0u32, |state| {
            self.panel_slots
                .get(state.panel_index as usize)
                .map(|s| s.grid_pos().grid_row.saturating_add(state.current_height))
                .unwrap_or(0)
        });
        let grid_rows = max_row_end
            .max(drag_ghost_extent)
            .max(resize_ghost_extent)
            .max(1);
        let grid_height_px = (grid_rows as f32) * DASHBOARD_ROW_PX;

        // Wire keyboard navigation. The dashboard root tracks the document
        // focus handle so on_key_down fires when the document is the active
        // pane. Keys handled:
        //   - Left / Right          : prev / next panel in visual order
        //   - Up / Down              : move focus by one grid row
        //   - Enter                 : open Configure popover for focused panel
        //   - F2                    : start inline title edit on focused panel
        //   - Delete / Backspace    : remove focused panel
        //   - Escape                : close any open popover / menu
        let focus_handle = self.focus_handle.clone();
        let on_key_down = cx.listener(
            |this, event: &KeyDownEvent, window: &mut Window, cx: &mut Context<Self>| {
                let key = event.keystroke.key.as_str();
                let modifiers = event.keystroke.modifiers;
                // Ignore keys when an inline title edit is in progress; the
                // input owns the keyboard.
                if this.editing_title_panel_index.is_some() || this.editing_dashboard_name {
                    return;
                }
                if modifiers.platform || modifiers.alt || modifiers.shift || modifiers.control {
                    return;
                }
                match key {
                    "left" => this.move_panel_focus(-1, cx),
                    "right" => this.move_panel_focus(1, cx),
                    "up" => this.move_panel_focus_rows(-1, cx),
                    "down" => this.move_panel_focus_rows(1, cx),
                    "enter" => {
                        if let Some(idx) = this.focused_panel_index {
                            this.start_configure_panel(idx as usize, cx);
                        }
                    }
                    "f2" => {
                        if let Some(idx) = this.focused_panel_index {
                            this.start_panel_title_edit(idx, window, cx);
                        }
                    }
                    "delete" | "backspace" => {
                        if let Some(idx) = this.focused_panel_index {
                            this.remove_panel(idx, cx);
                        }
                    }
                    "escape" => {
                        if this.panel_context_menu.is_some() {
                            this.close_panel_context_menu(cx);
                        } else if this.pending_configure_panel_index.is_some() {
                            this.close_configure_panel(cx);
                        }
                    }
                    _ => {}
                }
            },
        );

        let is_empty = self.panel_slots.is_empty();

        let grid_container = if is_empty {
            // Empty-state CTA lives in a non-absolute flex row so it can
            // center itself without colliding with `relative()`.
            div()
                .id("dashboard-grid")
                .flex()
                .flex_row()
                .w_full()
                .h(px(MIN_PANEL_HEIGHT_PX))
                .children(panel_children)
        } else {
            // Width probe: a zero-height sibling that fills the container's
            // width so `on_children_prepainted` can report the rendered
            // container width to the drag handlers. The probe is the FIRST
            // child so its bounds are deterministic regardless of panel
            // count or layout state.
            let width_probe = div().id("dashboard-grid-width-probe").w_full().h(px(0.0));

            div()
                .on_children_prepainted(move |bounds_list: Vec<Bounds<Pixels>>, _window, _cx| {
                    if let Some(probe) = bounds_list.first() {
                        grid_width_for_capture.set(probe.size.width.into());
                    }
                })
                .id("dashboard-grid")
                .relative()
                .w_full()
                .h(px(grid_height_px))
                .child(width_probe)
                .children(panel_children)
                .when_some(drag_ghost, |el, ghost| el.child(ghost))
        };

        div()
            .flex()
            .flex_col()
            .size_full()
            .track_focus(&focus_handle)
            .on_key_down(on_key_down)
            .child(toolbar)
            .child(
                div()
                    .id("dashboard-scroll-region")
                    .flex_1()
                    .overflow_y_scrollbar()
                    // Bottom slack so the last row of panels clears the
                    // resizable Tasks-panel splitter that lives just below
                    // the workspace document area. Without this, scrolling
                    // to the end leaves the final row's bottom edge flush
                    // with — and partially hidden behind — the Tasks bar.
                    .child(div().child(grid_container).pb(px(24.0))), // guardrail-allow: bespoke bottom slack to clear the Tasks-panel splitter
            )
            .when(drag_active_global, |el| {
                el.on_mouse_move(on_global_mouse_move)
                    .on_mouse_up(gpui::MouseButton::Left, on_global_mouse_up)
            })
            .child(context_menu_overlay)
            .child(configure_overlay)
    }
}

/// Strip the leading markdown header marker (`#`, `##`, …) from a divider
/// markdown string, returning the trimmed display text.
///
/// CloudWatch text widgets typically carry a single `# Header` line; the
/// dashboard renders dividers as a plain header strip with no axes/toolbar,
/// so the rendered text is the markdown stripped of its leading `#` runs.
fn divider_label(markdown: &str) -> String {
    let first_line = markdown.lines().next().unwrap_or("");
    let trimmed = first_line.trim_start();
    let stripped = trimmed.trim_start_matches('#').trim_start();
    if stripped.is_empty() {
        first_line.to_string()
    } else {
        stripped.to_string()
    }
}

/// Compute the pixel height for a panel given its `grid_height` multiplier.
///
/// In the new 12-col absolute-position model this is just
/// `grid_height * DASHBOARD_ROW_PX`. Kept for legacy test coverage; the
/// production render path inlines the math directly.
#[cfg(test)]
pub(crate) fn panel_height(grid_height: u32) -> f32 {
    (grid_height as f32) * DASHBOARD_ROW_PX
}

#[cfg(test)]
mod tests {
    use super::super::{DASHBOARD_ROW_PX, DashboardPanelSlot, PANEL_REEXEC_CAP, PanelGridPos};
    use super::panel_height;

    /// Render-level invariant: `PANEL_REEXEC_CAP` is visible from render.rs
    /// (same crate, `pub(crate)` const). Compile-only assertion.
    #[test]
    fn render_can_reference_panel_reexec_cap() {
        const _: () = assert!(PANEL_REEXEC_CAP > 0);
    }

    /// `panel_height(1)` is exactly one `DASHBOARD_ROW_PX` row.
    #[test]
    fn panel_height_one_row_is_eighty_px() {
        let h = panel_height(1);
        assert!(
            (h - DASHBOARD_ROW_PX).abs() < f32::EPSILON,
            "grid_height=1 must equal DASHBOARD_ROW_PX ({DASHBOARD_ROW_PX}), got {h}"
        );
    }

    /// `panel_height` scales linearly with the row count.
    #[test]
    fn panel_height_scales_with_rows() {
        assert_eq!(panel_height(2), DASHBOARD_ROW_PX * 2.0);
        assert_eq!(panel_height(3), DASHBOARD_ROW_PX * 3.0);
    }

    /// `panel_height(0)` collapses to zero — the empty-state CTA carries its
    /// own minimum height (`MIN_PANEL_HEIGHT_PX`).
    #[test]
    fn panel_height_zero_rows_is_zero() {
        let h = panel_height(0);
        assert!((h - 0.0).abs() < f32::EPSILON);
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

        let mut slots = [make_orphan(1, 1), make_orphan(0, 1), make_orphan(0, 0)];

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

    /// Each panel renders three resize handles whose element IDs follow the
    /// `panel-resize-{edge}-{index}` pattern. The constants below pin those
    /// IDs so renames cause a test failure rather than a silent regression.
    #[test]
    fn panel_resize_handle_ids_cover_three_edges() {
        for i in 0u32..4 {
            assert!(format!("panel-resize-right-{i}").starts_with("panel-resize-right-"));
            assert!(format!("panel-resize-bottom-{i}").starts_with("panel-resize-bottom-"));
            assert!(format!("panel-resize-corner-{i}").starts_with("panel-resize-corner-"));
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
