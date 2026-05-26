//! In-document drag-reorder, drag-resize, and inline-title-edit machinery.
//!
//! This module is a render-helper companion to `DashboardDocument`. It
//! exposes:
//!
//! - `DragReorderState` / `DragResizeState` — drag-operation state machines
//!   stored inside `DashboardDocument`.
//! - Render helpers (`panel_header`, `panel_resize_handle`, `dashboard_toolbar`)
//!   used by `render.rs`. These are `pub(super)` — they do not cross the crate
//!   boundary.
//! - A `PanelContextMenu` struct for the per-panel right-click menu.
//!
//! Design notes (§6.7 / §6.1 / §6.2):
//! - Drag-reorder uses insert-at-position semantics.
//! - Drag-resize snaps on drag-end; no mid-drag persistence.
//! - Inline title edit stores `editing_title_panel_index: Option<u32>` on the
//!   document; the `Input` entity is lazily created when editing starts.
//! - The toolbar always renders (even when there are zero panels).

use dbflux_components::controls::{Button, Dropdown, InputState};
use dbflux_components::saved_chart::TimeRangePreset;
use gpui::prelude::*;
use gpui::{Context, CursorStyle, Entity, IntoElement, MouseButton, Pixels, Window, div, px};
use gpui_component::InteractiveElementExt;

use super::DashboardDocument;

// ---------------------------------------------------------------------------
// Drag-reorder state
// ---------------------------------------------------------------------------

/// Drag-reorder state for the panel grid.
///
/// A drag starts when the user presses the mouse button on a panel header.
/// `drop_slot` is updated on every mouse-move to reflect the current target
/// slot. The drag commits on mouse-up by calling
/// `DashboardDocument::reorder_panels`.
#[derive(Debug, Clone)]
pub(crate) struct DragReorderState {
    /// Slot index of the panel being dragged.
    pub from_index: u32,
    /// Current drop target slot index (updated on mouse-move).
    pub drop_slot: u32,
    /// True while the mouse button is held down.
    pub active: bool,
}

// ---------------------------------------------------------------------------
// Drag-resize state
// ---------------------------------------------------------------------------

/// Drag-resize state for a single panel.
///
/// A resize drag starts when the user presses the mouse button on the
/// bottom-right resize handle. `delta_cols` and `delta_rows` accumulate the
/// change in grid units as the mouse moves. On mouse-up the accumulated delta
/// is applied via `DashboardDocument::resize_panel`.
#[derive(Debug, Clone)]
pub(crate) struct DragResizeState {
    /// Slot index of the panel being resized.
    pub panel_index: u32,
    /// Grid width at the start of the drag.
    pub original_width: u32,
    /// Grid height at the start of the drag.
    pub original_height: u32,
    /// Screen X position at drag start.
    pub start_x: Pixels,
    /// Screen Y position at drag start.
    pub start_y: Pixels,
    /// Working new width (updated on mouse-move; persisted on mouse-up).
    pub current_width: u32,
    /// Working new height (updated on mouse-move; persisted on mouse-up).
    pub current_height: u32,
    /// True while the mouse button is held down.
    pub active: bool,
}

/// Pixels per grid unit for width drag.
pub(super) const DRAG_RESIZE_PX_PER_COL: f32 = 120.0;
/// Pixels per grid unit for height drag.
pub(super) const DRAG_RESIZE_PX_PER_ROW: f32 = 80.0;

// ---------------------------------------------------------------------------
// Per-panel context menu
// ---------------------------------------------------------------------------

/// Per-panel right-click context menu.
#[derive(Debug, Clone)]
pub(crate) struct PanelContextMenu {
    /// Which panel the menu belongs to.
    pub panel_index: u32,
    /// Approximate screen position where the menu was opened.
    pub position: gpui::Point<Pixels>,
    /// The available menu items.
    pub items: Vec<PanelMenuAction>,
    /// Keyboard-navigation cursor (0-based into `items`).
    #[allow(dead_code)]
    pub selected_index: usize,
}

/// Actions available in the per-panel context menu.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PanelMenuAction {
    /// Opens the Configure popover for this panel.
    Configure,
    /// Opens inline title-edit for this panel.
    EditTitle,
    /// Removes the panel from the dashboard.
    RemovePanel,
}

impl PanelContextMenu {
    pub(super) fn new(panel_index: u32, position: gpui::Point<Pixels>) -> Self {
        Self {
            panel_index,
            position,
            items: vec![
                PanelMenuAction::Configure,
                PanelMenuAction::EditTitle,
                PanelMenuAction::RemovePanel,
            ],
            selected_index: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Time-range preset helpers
// ---------------------------------------------------------------------------

/// All five time-range preset variants in display order.
pub(super) const TIME_RANGE_PRESETS: &[(TimeRangePreset, &str)] = &[
    (TimeRangePreset::Last15min, "Last 15 min"),
    (TimeRangePreset::LastHour, "Last 1 hour"),
    (TimeRangePreset::Last6Hours, "Last 6 hours"),
    (TimeRangePreset::Last24Hours, "Last 24 hours"),
    (TimeRangePreset::Last7Days, "Last 7 days"),
];

/// Returns the display label for a `TimeRangePreset`.
#[allow(dead_code)]
pub(super) fn preset_label(preset: TimeRangePreset) -> &'static str {
    TIME_RANGE_PRESETS
        .iter()
        .find(|(p, _)| *p == preset)
        .map(|(_, l)| *l)
        .unwrap_or("Last 24 hours")
}

// ---------------------------------------------------------------------------
// Render helpers (pub(super) — used only by render.rs)
// ---------------------------------------------------------------------------

/// Returns the dashboard toolbar element.
///
/// Renders (left to right):
/// - Dashboard name: static label (double-click to start inline rename) or an
///   inline `Input` when `editing_dashboard_name` is true (Q.2).
/// - `TimeRangePanel` view (preset dropdown + Custom picker) — the canonical
///   time-range chrome shared with `ChartDocument` and `AuditDocument`.
/// - Refresh-policy `Dropdown` (Off / 10s / 30s / 1m / 5m).
/// - "+ Add Panel" primary button anchored to the right edge.
pub(super) fn dashboard_toolbar(
    dashboard: &DashboardDocument,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    let editing_name = dashboard.editing_dashboard_name;
    let name_input = dashboard.dashboard_name_input.as_ref().cloned();
    let dashboard_title = dashboard.title().to_string();
    let time_range_panel = dashboard.shared_time_range().clone();
    let refresh_dropdown = dashboard.refresh_dropdown.clone();

    // Q.2: Dashboard name area — inline input when editing, double-click label otherwise.
    let name_area: gpui::AnyElement = if editing_name {
        debug_assert!(
            name_input.is_some(),
            "dashboard_name_input must be Some when editing_dashboard_name is true"
        );
        let input_state = name_input.expect("InputState must be present when editing");
        div()
            .id("dashboard-name-edit")
            .flex_shrink_0()
            .child(dbflux_components::controls::Input::new(&input_state).small())
            .into_any_element()
    } else {
        let on_double_click = cx.listener(|this, _: &gpui::ClickEvent, window, cx| {
            this.start_dashboard_name_edit(window, cx);
        });
        div()
            .id("dashboard-name-label")
            .flex_shrink_0()
            .text_sm()
            .cursor(CursorStyle::PointingHand)
            .child(dashboard_title)
            .on_double_click(on_double_click)
            .into_any_element()
    };

    // Pull the preset dropdown out of the TimeRangePanel so the toolbar can
    // host it inline rather than embedding the full panel (whose default
    // render layout is vertically stacked). Custom-range pickers are rendered
    // below the toolbar when the user picks "Custom…".
    let preset_dropdown: Entity<Dropdown> = time_range_panel.read(cx).dropdown_time_range.clone();

    // "+ Add Panel" toolbar button (anchored right).
    let on_add_panel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
        this.request_add_panel(cx);
    });

    div()
        .id("dashboard-toolbar")
        .flex()
        .flex_row()
        .items_center()
        .gap(px(8.0)) // guardrail-allow: toolbar item spacing
        .p(px(8.0)) // guardrail-allow: toolbar padding
        .w_full()
        .child(name_area)
        .child(preset_dropdown)
        .child(refresh_dropdown)
        .child(div().flex_1())
        .child(
            Button::new("dash-add-panel-toolbar", "+ Add Panel")
                .primary()
                .on_click(on_add_panel),
        )
}

/// Returns the panel-header element for a single panel slot.
///
/// Renders: drag handle (title area) + optional inline title input + close
/// button + right-click context-menu hook.
///
/// When `is_editing_title` is true, an `Input` entity is rendered inline for
/// title editing; when false, the title is a clickable span that starts inline
/// edit on single-click, and a drag handle on mouse-down.
pub(super) fn panel_header(
    panel_index: u32,
    title: &str,
    editing_input: Option<&Entity<InputState>>,
    _drag_active: bool,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    let is_editing = editing_input.is_some();
    let title_owned = title.to_string();

    // Start inline title edit on single click (only when not editing).
    let on_title_click = if !is_editing {
        let on_click = cx.listener(move |this, _: &gpui::ClickEvent, window, cx| {
            this.start_panel_title_edit(panel_index, window, cx);
        });
        Some(on_click)
    } else {
        None
    };

    // Context menu on right-click.
    let on_right_click = cx.listener(move |this, event: &gpui::MouseDownEvent, _, cx| {
        this.open_panel_context_menu(panel_index, event.position, cx);
    });

    // Drag start on header mouse-down (only when not editing).
    let on_drag_start = if !is_editing {
        let drag_start = cx.listener(move |this, _: &gpui::MouseDownEvent, _, cx| {
            this.start_panel_drag(panel_index, cx);
        });
        Some(drag_start)
    } else {
        None
    };

    // Drag end on header mouse-up.
    let on_drag_end = if !is_editing {
        let drag_end = cx.listener(move |this, _: &gpui::MouseUpEvent, _, cx| {
            this.end_panel_drag(cx);
        });
        Some(drag_end)
    } else {
        None
    };

    // Kebab menu button — opens the same context menu as right-click, but
    // gives keyboard/mouse users a discoverable affordance. Replaces the
    // previous standalone "×" close button.
    let on_kebab_click = cx.listener(move |this, event: &gpui::ClickEvent, _, cx| {
        this.open_panel_context_menu(panel_index, event.position(), cx);
    });

    let mut header = div()
        .id(("panel-header", panel_index))
        .flex()
        .flex_row()
        .items_center()
        .w_full()
        .gap(px(4.0)) // guardrail-allow: header item spacing
        .p(px(4.0)) // guardrail-allow: header padding
        .cursor(CursorStyle::OpenHand)
        .on_mouse_down(MouseButton::Right, on_right_click);

    if let Some(on_start) = on_drag_start {
        header = header.on_mouse_down(MouseButton::Left, on_start);
    }
    if let Some(on_end) = on_drag_end {
        header = header.on_mouse_up(MouseButton::Left, on_end);
    }

    if let Some(input_state) = editing_input {
        // Render the input inline. Commit and cancel are handled entirely by
        // the InputEvent subscription established in `start_panel_title_edit`.
        debug_assert!(
            editing_input.is_some(),
            "editing_input must be Some when editing_title_panel_index is set"
        );
        header = header.child(
            dbflux_components::controls::Input::new(input_state)
                .w_full()
                .small(),
        );
    } else {
        let title_elem = if let Some(on_click) = on_title_click {
            div()
                .id(("panel-title", panel_index))
                .flex_1()
                .text_sm()
                .cursor(CursorStyle::OpenHand)
                .child(title_owned)
                .on_click(on_click)
                .into_any_element()
        } else {
            div()
                .id(("panel-title", panel_index))
                .flex_1()
                .text_sm()
                .child(title_owned)
                .into_any_element()
        };

        // Kebab menu trigger — replaces the previous standalone "×" close
        // button. Opens the same context menu as right-click on the header,
        // so destructive actions (Remove panel) sit behind one extra click.
        let kebab_btn = Button::new(("panel-kebab", panel_index), "⋯")
            .ghost()
            .on_click(on_kebab_click);

        header = header.child(title_elem).child(kebab_btn);
    }

    header
}

/// Returns the bottom-right resize handle element for a panel slot.
///
/// The handle is a small 8×8 px hit area with `nwse-resize` cursor. On
/// mouse-down it starts the resize drag; mouse-move (on the panel wrapper)
/// updates the working dimensions; mouse-up commits via
/// `DashboardDocument::resize_panel`.
pub(super) fn panel_resize_handle(
    panel_index: u32,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    let on_resize_start = cx.listener(move |this, event: &gpui::MouseDownEvent, _, cx| {
        this.start_panel_resize(panel_index, event.position, cx);
    });

    let on_resize_end = cx.listener(move |this, _: &gpui::MouseUpEvent, _, cx| {
        this.end_panel_resize(cx);
    });

    div()
        .id(("panel-resize", panel_index))
        .w(px(8.0)) // guardrail-allow: resize handle hit area (8px is below normal token range)
        .h(px(8.0)) // guardrail-allow: resize handle hit area
        .absolute()
        .bottom(px(0.0))
        .right(px(0.0))
        .cursor(CursorStyle::ResizeUpLeftDownRight)
        .on_mouse_down(MouseButton::Left, on_resize_start)
        .on_mouse_up(MouseButton::Left, on_resize_end)
}

// ---------------------------------------------------------------------------
// AppState inline call helpers (pure logic, no GPUI)
// ---------------------------------------------------------------------------

/// Compute the new grid dimensions after a resize drag delta.
///
/// `delta_x` / `delta_y` are pixel deltas from the drag start position.
/// Returns `(new_width, new_height)` clamped to `[1, max_width]` × `[1, 4]`.
pub(super) fn compute_resize_dimensions(
    original_width: u32,
    original_height: u32,
    delta_x: f32,
    delta_y: f32,
    max_width: u32,
) -> (u32, u32) {
    let col_delta = (delta_x / DRAG_RESIZE_PX_PER_COL).round() as i32;
    let row_delta = (delta_y / DRAG_RESIZE_PX_PER_ROW).round() as i32;

    let new_width = (original_width as i32 + col_delta).clamp(1, max_width as i32) as u32;
    let new_height = (original_height as i32 + row_delta).clamp(1, 4) as u32;

    (new_width, new_height)
}

/// Compute the drop-target slot index for an in-progress drag.
///
/// Given the current mouse Y position and the panel heights, returns the slot
/// index where the dragged panel would be inserted.
#[allow(dead_code)]
pub(super) fn compute_drop_slot(mouse_y: f32, panel_heights: &[f32], panel_count: u32) -> u32 {
    if panel_count == 0 || panel_heights.is_empty() {
        return 0;
    }

    let mut cumulative_y = 0.0f32;
    for (i, &h) in panel_heights.iter().enumerate() {
        cumulative_y += h;
        if mouse_y < cumulative_y - h / 2.0 {
            return i as u32;
        }
    }
    panel_count.saturating_sub(1)
}

// ---------------------------------------------------------------------------
// Helper: `InputState` factory for inline title editing
// ---------------------------------------------------------------------------

/// Creates a new `InputState` with the given initial text.
///
/// Must be called from within `cx.new(|cx| make_title_input(text, window, cx))`
/// where `cx: &mut Context<InputState>`.
#[allow(dead_code)]
pub(super) fn make_title_input(
    initial_text: String,
    window: &mut Window,
    cx: &mut gpui::Context<InputState>,
) -> InputState {
    let mut state = InputState::new(window, cx);
    state.set_value(&initial_text, window, cx);
    state
}

// ---------------------------------------------------------------------------
// Tests (Q.9 state-machine and helper logic, no GPUI runtime required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The `TIME_RANGE_PRESETS` table must cover all five canonical presets.
    #[test]
    fn time_range_presets_table_has_five_entries() {
        assert_eq!(TIME_RANGE_PRESETS.len(), 5);
    }

    /// `preset_label` returns the correct human-readable string for each variant.
    #[test]
    fn preset_label_returns_correct_string() {
        assert_eq!(preset_label(TimeRangePreset::Last15min), "Last 15 min");
        assert_eq!(preset_label(TimeRangePreset::LastHour), "Last 1 hour");
        assert_eq!(preset_label(TimeRangePreset::Last6Hours), "Last 6 hours");
        assert_eq!(preset_label(TimeRangePreset::Last24Hours), "Last 24 hours");
        assert_eq!(preset_label(TimeRangePreset::Last7Days), "Last 7 days");
    }

    /// `compute_resize_dimensions` with no delta returns the original dimensions.
    #[test]
    fn compute_resize_dimensions_zero_delta_is_identity() {
        let (w, h) = compute_resize_dimensions(2, 2, 0.0, 0.0, 4);
        assert_eq!(w, 2);
        assert_eq!(h, 2);
    }

    /// `compute_resize_dimensions` clamps width to the max_width parameter.
    #[test]
    fn compute_resize_dimensions_clamps_width_to_max() {
        let max_w = 2u32;
        let (w, _h) = compute_resize_dimensions(1, 1, 999.0, 0.0, max_w);
        assert_eq!(w, max_w, "width must be clamped to grid_columns");
    }

    /// `compute_resize_dimensions` clamps height to 4 regardless of delta.
    #[test]
    fn compute_resize_dimensions_clamps_height_to_four() {
        let (_w, h) = compute_resize_dimensions(1, 1, 0.0, 999.0, 4);
        assert_eq!(h, 4, "height must be clamped to 4");
    }

    /// `compute_resize_dimensions` clamps both dimensions to minimum 1.
    #[test]
    fn compute_resize_dimensions_clamps_to_minimum_one() {
        let (w, h) = compute_resize_dimensions(2, 2, -999.0, -999.0, 4);
        assert_eq!(w, 1, "width must not go below 1");
        assert_eq!(h, 1, "height must not go below 1");
    }

    /// `compute_drop_slot` returns 0 when there are no panels.
    #[test]
    fn compute_drop_slot_returns_zero_for_empty() {
        let slot = compute_drop_slot(500.0, &[], 0);
        assert_eq!(slot, 0);
    }

    /// `compute_drop_slot` returns the last slot when the cursor is below all panels.
    #[test]
    fn compute_drop_slot_returns_last_when_below_all() {
        let heights = [100.0f32, 100.0, 100.0];
        let slot = compute_drop_slot(400.0, &heights, 3);
        assert_eq!(slot, 2);
    }

    /// `compute_drop_slot` returns 0 when the cursor is above the first panel midpoint.
    #[test]
    fn compute_drop_slot_returns_zero_when_above_first_midpoint() {
        let heights = [100.0f32, 100.0, 100.0];
        // Midpoint of first panel = 50px; cursor at 30px → slot 0.
        let slot = compute_drop_slot(30.0, &heights, 3);
        assert_eq!(slot, 0);
    }

    /// `PanelContextMenu` is constructed with the correct panel_index and the
    /// canonical action set in order: Configure, EditTitle, RemovePanel.
    #[test]
    fn panel_context_menu_has_canonical_items() {
        let menu = PanelContextMenu::new(3, gpui::Point::default());
        assert_eq!(menu.panel_index, 3);
        assert_eq!(menu.items.len(), 3);
        assert_eq!(menu.items[0], PanelMenuAction::Configure);
        assert_eq!(menu.items[1], PanelMenuAction::EditTitle);
        assert_eq!(menu.items[2], PanelMenuAction::RemovePanel);
    }

    /// `DragReorderState` starts as active and preserves the from/drop indices.
    #[test]
    fn drag_reorder_state_construction() {
        let state = DragReorderState {
            from_index: 2,
            drop_slot: 2,
            active: true,
        };
        assert_eq!(state.from_index, 2);
        assert!(state.active);
    }

    /// `DragResizeState` starts with original dimensions preserved.
    #[test]
    fn drag_resize_state_construction() {
        let state = DragResizeState {
            panel_index: 1,
            original_width: 2,
            original_height: 3,
            start_x: px(100.0),
            start_y: px(200.0),
            current_width: 2,
            current_height: 3,
            active: true,
        };
        assert_eq!(state.original_width, 2);
        assert_eq!(state.original_height, 3);
        assert_eq!(state.current_width, 2);
    }
}
