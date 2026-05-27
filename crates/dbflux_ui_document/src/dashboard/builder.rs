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

use crate::chrome::{ToolbarButton, ToolbarButtonVariant, compact_top_bar};
use dbflux_components::composites::refresh_split_button;
use dbflux_components::controls::{Dropdown, InputState};
use dbflux_components::saved_chart::TimeRangePreset;
use dbflux_components::tokens::{Radii, Spacing};
use gpui::prelude::*;
use gpui::{App, Context, CursorStyle, Entity, IntoElement, MouseButton, Pixels, Window, div, px};
use gpui_component::ActiveTheme;

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
    ///
    /// Position is no longer tracked: the kebab menu anchors inline next to
    /// its panel's `⋯` button via `.relative()` + `.absolute().top()`. See
    /// `builder::panel_header` for the wrapper that hosts the floating menu.
    pub panel_index: u32,
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
    pub(super) fn new(panel_index: u32) -> Self {
        Self {
            panel_index,
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
/// - `TimeRangePanel` preset dropdown (content-sized) — the canonical
///   time-range chrome shared with `ChartDocument` and `AuditDocument`.
/// - Refresh-policy `Dropdown` (content-sized).
/// - "+ Add Panel" primary button anchored to the right edge.
///
/// The dashboard name is intentionally omitted; the tab title already shows it
/// and `start_dashboard_name_edit` is still reachable through other affordances.
/// Layout matches `AuditDocument` via `compact_top_bar` so the dashboard
/// inherits the same flex-wrap + shrink rules and the dropdowns size to their
/// content instead of stretching across the row.
pub(super) fn dashboard_toolbar(
    dashboard: &DashboardDocument,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    use dbflux_components::common::time_range::TimeRange;
    use dbflux_components::common::time_range::view::TimeRangePanel;

    let theme = cx.theme().clone();
    let time_range_panel = dashboard.shared_time_range().clone();
    let refresh_dropdown = dashboard.refresh_dropdown.clone();

    // Preset dropdown lifted out of the TimeRangePanel so the toolbar embeds
    // the control inline. The TimeRangePanel itself stays the owner of state;
    // we only render its child widgets.
    let preset_dropdown: Entity<Dropdown> = time_range_panel.read(cx).dropdown_time_range.clone();
    let selected_time_range = time_range_panel.read(cx).selected_time_range;
    let custom_range_visible = selected_time_range == Some(TimeRange::Custom);

    // Content-sized wrapper — `Dropdown::render` applies `w_full()` internally,
    // which stretches as a direct flex child. The wrapper acts as an
    // intrinsic-width flex item so the control collapses to content.
    let time_control = div()
        .flex_shrink_0()
        .rounded(Radii::SM)
        .child(preset_dropdown);

    // Refresh split-button — same helper AuditDocument uses, so the visual
    // language matches the rest of the app. Manual click re-executes every
    // loaded panel; the dropdown segment sets the auto-refresh interval.
    let weak = cx.weak_entity();
    let refresh_btn = refresh_split_button(
        "dashboard-refresh-split",
        dashboard.shared_refresh_policy_as_core(),
        false,
        false,
        refresh_dropdown,
        move |_window, cx| {
            if let Some(doc) = weak.upgrade() {
                doc.update(cx, |this, cx| this.refresh_all_loaded_panels(cx));
            }
        },
        &theme,
    );

    let refresh_control = div().flex_shrink_0().child(refresh_btn);

    // "+ Add Panel" toolbar button — `ToolbarButton` keeps the 28 px row
    // height that matches every other DBFlux toolbar (data grid, audit, code).
    let on_add_panel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
        this.request_add_panel(cx);
    });

    let add_btn = div().flex_shrink_0().ml_auto().child(
        ToolbarButton::new("dash-add-panel-toolbar")
            .label("+ Add Panel")
            .variant(ToolbarButtonVariant::Primary)
            .on_click(move |event, window, app| on_add_panel(event, window, app)),
    );

    // Items pushed in order. When Custom is selected, the picker slots are
    // inserted between the preset dropdown and the refresh control, mirroring
    // AuditDocument exactly so users see a familiar custom-range row.
    let mut items: Vec<gpui::AnyElement> = vec![time_control.into_any_element()];

    if custom_range_visible {
        let custom_controls = build_custom_time_controls(&time_range_panel, cx);
        items.push(custom_controls.into_any_element());
    }

    items.push(refresh_control.into_any_element());
    items.push(add_btn.into_any_element());

    let _ = TimeRangePanel::preset_items; // touch import to keep linter happy
    compact_top_bar(&theme, items)
        .id("dashboard-toolbar")
        .gap(Spacing::SM)
}

/// Build the custom-range row (date picker + start/end hour/minute + Apply)
/// using the shared `TimeRangePanel::custom_picker_slots` API.
///
/// Returns a flex row containing each picker so it appears inline in the
/// toolbar exactly the way `AuditDocument` renders the same controls.
fn build_custom_time_controls(
    panel: &Entity<dbflux_components::common::time_range::view::TimeRangePanel>,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    let slots = panel.read(cx).custom_picker_slots(px(220.0), cx);
    let weak_panel = panel.downgrade();

    let can_apply = panel.read(cx).can_apply_custom_range(cx);
    let on_apply = move |_event: &gpui::ClickEvent, _w: &mut Window, app: &mut App| {
        if let Some(panel) = weak_panel.upgrade() {
            panel.update(app, |panel, cx| {
                let _ = panel.apply_custom_range(cx);
            });
        }
    };

    div()
        .flex_shrink_0()
        .flex()
        .items_center()
        .gap_1()
        .child(slots.date_picker)
        .child(slots.from_label)
        .child(slots.start_hour)
        .child(slots.start_minute)
        .child(slots.to_label)
        .child(slots.end_hour)
        .child(slots.end_minute)
        .child(
            ToolbarButton::new("dashboard-custom-time-apply")
                .label("Apply")
                .variant(ToolbarButtonVariant::Default)
                .disabled(!can_apply)
                .on_click(on_apply),
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
    menu_open: bool,
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

    // Context menu on right-click — anchors inline next to this panel's
    // kebab, so no event position is captured.
    let on_right_click = cx.listener(move |this, _: &gpui::MouseDownEvent, _, cx| {
        this.open_panel_context_menu(panel_index, cx);
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
    // gives keyboard/mouse users a discoverable affordance. The menu floats
    // inline next to the trigger via the `.relative()` wrapper built below,
    // so the click position is irrelevant.
    let on_kebab_click = cx.listener(move |this, _: &gpui::ClickEvent, _, cx| {
        this.open_panel_context_menu(panel_index, cx);
    });
    // Prevent the header's left-mouse-down handler (which starts a panel drag)
    // from also firing when the user presses the kebab button.
    let on_kebab_mouse_down = |_: &gpui::MouseDownEvent, _: &mut Window, cx: &mut App| {
        cx.stop_propagation();
    };

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

        // Kebab menu trigger — matches the sidebar pattern: a borderless
        // square div with content-only sizing and a background-only hover
        // effect. Adding a border on hover would reflow the header (the user
        // reported this as a layout shift); leaving the box dimensions static
        // and only changing `bg` avoids any reflow.
        //
        // The menu items are rendered as an absolute sibling inside this
        // `.relative()` wrapper so the floating panel anchors *directly* next
        // to the kebab regardless of the dashboard's window offset. This
        // avoids the window-vs-local coordinate mismatch the previous
        // click-position implementation suffered from.
        let theme = cx.theme();
        let hover_bg = theme.secondary;

        let kebab_trigger = div()
            .id(("panel-kebab", panel_index))
            .flex_shrink_0()
            .px_1()
            .rounded(Radii::SM)
            .cursor_pointer()
            .hover(move |d| d.bg(hover_bg))
            .text_sm()
            .child("\u{22EF}") // ⋯
            .on_mouse_down(MouseButton::Left, on_kebab_mouse_down)
            .on_click(on_kebab_click);

        let menu_panel = if menu_open {
            Some(panel_kebab_menu(panel_index, cx))
        } else {
            None
        };

        let kebab_wrapper = div()
            .relative()
            .flex_shrink_0()
            .child(kebab_trigger)
            .when_some(menu_panel, |el, panel| {
                el.child(
                    gpui::deferred(
                        div()
                            .absolute()
                            .top(px(20.0)) // sit just below the kebab glyph
                            .right(px(0.0))
                            .child(panel),
                    )
                    .with_priority(2),
                )
            });

        header = header.child(title_elem).child(kebab_wrapper);
    }

    header
}

/// Build the floating menu panel for the panel at `panel_index`.
///
/// Renders the same `MenuItem` chain used by the sidebar (icons, separator,
/// danger color for `Remove panel`). Click handlers stash the chosen action
/// in `pending_panel_menu_action`; the action is consumed at the start of the
/// next `render` pass where a real `Window` is available.
fn panel_kebab_menu(panel_index: u32, cx: &mut Context<DashboardDocument>) -> gpui::AnyElement {
    use dbflux_components::composites::{MenuItem, render_menu_items};
    use dbflux_components::icons::AppIcon;

    // Items mirror the sidebar's two-section layout: actions, then a
    // separator, then the destructive `Remove panel`.
    let menu_items: Vec<MenuItem> = vec![
        MenuItem::new("Configure…").icon(AppIcon::Settings),
        MenuItem::new("Edit title…").icon(AppIcon::Pencil),
        MenuItem::separator(),
        MenuItem::new("Remove panel").icon(AppIcon::Delete).danger(),
    ];

    // The visible items list contains a separator, so map visual index back
    // to the domain `PanelMenuAction` order (Configure=0, EditTitle=1,
    // RemovePanel=2).
    let visual_to_action: Vec<Option<usize>> = vec![Some(0), Some(1), None, Some(2)];

    let weak = cx.weak_entity();
    let on_click = move |visual_idx: usize, app: &mut gpui::App| {
        let Some(Some(action_idx)) = visual_to_action.get(visual_idx).copied() else {
            return;
        };
        if let Some(doc) = weak.upgrade() {
            doc.update(app, |this, cx| {
                this.pending_panel_menu_action = Some(action_idx);
                cx.notify();
            });
        }
    };
    let on_hover = move |_: usize, _: &mut gpui::App| {};

    let panel_id = format!("panel-ctx-menu-{}", panel_index);
    render_menu_items(&panel_id, &menu_items, None, on_click, on_hover, cx).into_any_element()
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
        let menu = PanelContextMenu::new(3);
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
