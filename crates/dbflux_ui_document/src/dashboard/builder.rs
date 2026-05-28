//! In-document drag-reorder, drag-resize, and inline-title-edit machinery.
//!
//! This module is a render-helper companion to `DashboardDocument`. It
//! exposes:
//!
//! - `DragReorderState` / `DragResizeState` — drag-operation state machines
//!   stored inside `DashboardDocument`.
//! - Render helpers (`panel_header`, `panel_resize_right`, `panel_resize_bottom`,
//!   `panel_resize_corner`, `dashboard_toolbar`) used by `render.rs`. These are
//!   `pub(super)` — they do not cross the crate boundary.
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

/// Drag-to-move state for a single panel.
///
/// A drag starts when the user presses the left mouse button on a panel header
/// while the dashboard is in edit mode. The render-root mouse-move handler
/// snaps `working_column` / `working_row` to the nearest 12-col grid cell.
/// On mouse-up the move commits if the target rectangle does not overlap
/// another panel; otherwise the panel snaps back to its original position
/// and a toast informs the user.
#[derive(Debug, Clone)]
pub(crate) struct DragReorderState {
    /// Slot index of the panel being dragged.
    pub from_index: u32,
    /// Original `grid_column` at drag start (for snap-back on overlap).
    pub original_column: u32,
    /// Original `grid_row` at drag start.
    pub original_row: u32,
    /// Window-space X of the cursor when the drag started.
    pub start_x: Pixels,
    /// Window-space Y of the cursor when the drag started.
    pub start_y: Pixels,
    /// Working target column, snapped to grid units on every mouse-move.
    pub working_column: u32,
    /// Working target row, snapped to grid units on every mouse-move.
    pub working_row: u32,
    /// True while the mouse button is held down.
    pub active: bool,
}

// ---------------------------------------------------------------------------
// Drag-resize state
// ---------------------------------------------------------------------------

/// Axis a resize drag is allowed to mutate.
///
/// The right edge handle resizes width only (`X`); the bottom edge handle
/// resizes height only (`Y`); the corner grip resizes both (`Both`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResizeAxis {
    X,
    Y,
    Both,
}

/// Drag-resize state for a single panel.
///
/// A resize drag starts when the user presses the mouse button on one of the
/// three resize handles (right edge, bottom edge, or bottom-right corner). The
/// `axis` field constrains which dimensions the global mouse-move handler will
/// mutate. On mouse-up the new dimensions commit via
/// `DashboardDocument::end_panel_resize`, which performs the collision check
/// against the other panels and snaps back on overlap.
#[derive(Debug, Clone)]
pub(crate) struct DragResizeState {
    /// Slot index of the panel being resized.
    pub panel_index: u32,
    /// Axis the drag is allowed to mutate.
    pub axis: ResizeAxis,
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

    let add_btn = ToolbarButton::new("dash-add-panel-toolbar")
        .label("+ Add Panel")
        .variant(ToolbarButtonVariant::Primary)
        .on_click(move |event, window, app| on_add_panel(event, window, app));

    // Edit/View toggle. Pencil icon = "enter edit"; Eye icon = "back to view".
    use dbflux_components::icons::AppIcon;
    let in_edit_mode = dashboard.is_edit_mode();
    let on_toggle_mode = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
        this.toggle_mode(cx);
    });
    let (mode_icon, mode_tooltip) = if in_edit_mode {
        (AppIcon::Eye, "Exit edit mode")
    } else {
        (AppIcon::Pencil, "Edit dashboard")
    };
    let mode_btn = ToolbarButton::new("dash-mode-toggle")
        .icon(mode_icon)
        .variant(ToolbarButtonVariant::Default)
        .focused(in_edit_mode)
        .tooltip(mode_tooltip)
        .on_click(move |event, window, app| on_toggle_mode(event, window, app));

    // Group both right-anchored controls in one wrapper with its own gap so
    // they don't visually collide with each other or the toolbar edge.
    let right_group = div()
        .flex_shrink_0()
        .ml_auto()
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .child(add_btn)
        .child(mode_btn);

    // Items pushed in order. When Custom is selected, the picker slots are
    // inserted between the preset dropdown and the refresh control, mirroring
    // AuditDocument exactly so users see a familiar custom-range row.
    let mut items: Vec<gpui::AnyElement> = vec![time_control.into_any_element()];

    if custom_range_visible {
        let custom_controls = build_custom_time_controls(&time_range_panel, cx);
        items.push(custom_controls.into_any_element());
    }

    items.push(refresh_control.into_any_element());
    items.push(right_group.into_any_element());

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
    edit_mode: bool,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    let is_editing = editing_input.is_some();
    let title_owned = title.to_string();

    // Inline title edit is reachable only through the kebab menu's
    // "Edit title…" entry. Single-clicking the title text used to start the
    // edit, but the user found it noisy (every accidental click became an
    // edit), so the click handler is intentionally not wired here.

    // Context menu on right-click — anchors inline next to this panel's
    // kebab, so no event position is captured. Only available in edit mode.
    let on_right_click = if edit_mode {
        Some(cx.listener(move |this, _: &gpui::MouseDownEvent, _, cx| {
            this.open_panel_context_menu(panel_index, cx);
        }))
    } else {
        None
    };

    // Drag start on header mouse-down — only in edit mode and only when not
    // editing the title. The drag captures the cursor position so the global
    // mouse-move handler can snap to grid cells.
    let on_drag_start = if edit_mode && !is_editing {
        let drag_start = cx.listener(
            move |this, event: &gpui::MouseDownEvent, _, cx: &mut Context<DashboardDocument>| {
                this.start_panel_drag(panel_index, event.position, cx);
            },
        );
        Some(drag_start)
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

    // Header gets a move-cursor when it can be dragged. The previous
    // OpenHand cursor also appeared while just hovering the title text,
    // which the user read as an unwanted "hover effect" on the panel; now
    // the cursor only changes on the header (drag region) and only when
    // the panel isn't being edited.
    let mut header = div()
        .id(("panel-header", panel_index))
        .flex()
        .flex_row()
        .items_center()
        .w_full()
        .gap(px(4.0)) // guardrail-allow: header item spacing
        .p(px(4.0)); // guardrail-allow: header padding

    if let Some(handler) = on_right_click {
        header = header.on_mouse_down(MouseButton::Right, handler);
    }

    // The header only becomes a drag handle in edit mode. In view mode the
    // header is just a static label — no cursor change, no drag start.
    if edit_mode && !is_editing {
        header = header.cursor(CursorStyle::OpenHand);
    }

    if let Some(on_start) = on_drag_start {
        header = header.on_mouse_down(MouseButton::Left, on_start);
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
        // Title is a static label. Clicking it used to open inline edit,
        // but that was too easy to trigger by accident; the kebab menu's
        // "Edit title…" entry is now the only way to start the edit.
        let title_elem = div()
            .id(("panel-title", panel_index))
            .flex_1()
            .text_sm()
            .child(title_owned)
            .into_any_element();

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

        // The kebab is an edit-mode affordance only. In view mode the title
        // sits alone — no menu, no rename, no remove.
        if edit_mode {
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
        } else {
            // Silence the unused-variable warnings for the listeners we built
            // unconditionally; in view mode they are intentionally dropped.
            let _ = (on_kebab_click, on_kebab_mouse_down, menu_open, hover_bg);
            header = header.child(title_elem);
        }
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

/// Returns the right-edge resize handle for a panel slot.
///
/// The handle is an 8 px wide full-height strip aligned to the panel's right
/// edge. On mouse-down it starts a width-only resize drag.
pub(super) fn panel_resize_right(
    panel_index: u32,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    let on_resize_start = cx.listener(
        move |this, event: &gpui::MouseDownEvent, _, cx: &mut Context<DashboardDocument>| {
            this.start_panel_resize(panel_index, ResizeAxis::X, event.position, cx);
        },
    );

    div()
        .id(("panel-resize-right", panel_index))
        .absolute()
        .top(px(0.0))
        .right(px(0.0))
        .h_full()
        .w(px(8.0)) // guardrail-allow: resize-strip hit width
        .cursor(CursorStyle::ResizeLeftRight)
        .on_mouse_down(MouseButton::Left, on_resize_start)
}

/// Returns the bottom-edge resize handle for a panel slot.
///
/// The handle is an 8 px tall full-width strip aligned to the panel's bottom
/// edge. On mouse-down it starts a height-only resize drag.
pub(super) fn panel_resize_bottom(
    panel_index: u32,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    let on_resize_start = cx.listener(
        move |this, event: &gpui::MouseDownEvent, _, cx: &mut Context<DashboardDocument>| {
            this.start_panel_resize(panel_index, ResizeAxis::Y, event.position, cx);
        },
    );

    div()
        .id(("panel-resize-bottom", panel_index))
        .absolute()
        .left(px(0.0))
        .bottom(px(0.0))
        .w_full()
        .h(px(8.0)) // guardrail-allow: resize-strip hit height
        .cursor(CursorStyle::ResizeUpDown)
        .on_mouse_down(MouseButton::Left, on_resize_start)
}

/// Returns the bottom-right corner resize grip for a panel slot.
///
/// The grip is a 16×16 px square with two short diagonal strokes. Dragging it
/// resizes the panel on both axes simultaneously.
pub(super) fn panel_resize_corner(
    panel_index: u32,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    let on_resize_start = cx.listener(
        move |this, event: &gpui::MouseDownEvent, _, cx: &mut Context<DashboardDocument>| {
            this.start_panel_resize(panel_index, ResizeAxis::Both, event.position, cx);
        },
    );

    let theme = cx.theme();
    let grip_color = theme.muted_foreground;
    let hover_bg = theme.secondary;

    div()
        .id(("panel-resize-corner", panel_index))
        .w(px(16.0)) // guardrail-allow: corner grip hit area
        .h(px(16.0)) // guardrail-allow: corner grip hit area
        .absolute()
        .bottom(px(0.0))
        .right(px(0.0))
        .flex()
        .items_end()
        .justify_end()
        .cursor(CursorStyle::ResizeUpLeftDownRight)
        .hover(move |d| d.bg(hover_bg))
        .on_mouse_down(MouseButton::Left, on_resize_start)
        .child(
            div()
                .w(px(10.0))
                .h(px(10.0))
                .border_b_2()
                .border_r_2()
                .border_color(grip_color)
                .mr(px(2.0))
                .mb(px(2.0)),
        )
}

// ---------------------------------------------------------------------------
// Grid-snap helpers (pure logic, no GPUI)
// ---------------------------------------------------------------------------

use super::{DASHBOARD_GRID_COLUMNS, DASHBOARD_ROW_PX};

/// Snap a pixel delta on the column axis to whole grid units.
///
/// `px_per_col` is the rendered width of one column in pixels. Returns the
/// signed delta in grid columns.
pub(super) fn snap_columns(delta_x: f32, px_per_col: f32) -> i32 {
    if px_per_col <= 0.0 {
        return 0;
    }
    (delta_x / px_per_col).round() as i32
}

/// Snap a pixel delta on the row axis to whole grid units.
pub(super) fn snap_rows(delta_y: f32) -> i32 {
    (delta_y / DASHBOARD_ROW_PX).round() as i32
}

/// Apply a column delta to `original_width`, clamping to `[1, 12]`.
pub(super) fn apply_width_delta(original_width: u32, col_delta: i32) -> u32 {
    (original_width as i32 + col_delta).clamp(1, DASHBOARD_GRID_COLUMNS as i32) as u32
}

/// Apply a row delta to `original_height`, clamping to `[1, 12]`.
pub(super) fn apply_height_delta(original_height: u32, row_delta: i32) -> u32 {
    (original_height as i32 + row_delta).clamp(1, 12) as u32
}

/// Apply a column delta to `original_column`, clamping to `[0, 11]`.
///
/// `width` is the current panel width; the column is also clamped so the
/// panel's right edge stays within the 12-column grid.
pub(super) fn apply_column_delta(original_column: u32, col_delta: i32, width: u32) -> u32 {
    let raw = (original_column as i32 + col_delta).max(0) as u32;
    let max_column = DASHBOARD_GRID_COLUMNS.saturating_sub(width.max(1));
    raw.min(max_column)
}

/// Apply a row delta to `original_row`, clamping to non-negative integers.
pub(super) fn apply_row_delta(original_row: u32, row_delta: i32) -> u32 {
    (original_row as i32 + row_delta).max(0) as u32
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

    /// `snap_columns` rounds half-cell deltas to the nearest grid unit.
    #[test]
    fn snap_columns_rounds_to_nearest() {
        assert_eq!(snap_columns(0.0, 100.0), 0);
        assert_eq!(snap_columns(49.0, 100.0), 0);
        assert_eq!(snap_columns(51.0, 100.0), 1);
        assert_eq!(snap_columns(-149.0, 100.0), -1);
        assert_eq!(snap_columns(-151.0, 100.0), -2);
    }

    /// `snap_columns` is safe against zero / negative pixel-per-col values.
    #[test]
    fn snap_columns_handles_zero_pixels_per_col() {
        assert_eq!(snap_columns(500.0, 0.0), 0);
        assert_eq!(snap_columns(500.0, -10.0), 0);
    }

    /// `snap_rows` uses `DASHBOARD_ROW_PX` (80) as the unit.
    #[test]
    fn snap_rows_rounds_to_nearest_row() {
        assert_eq!(snap_rows(0.0), 0);
        assert_eq!(snap_rows(39.0), 0);
        assert_eq!(snap_rows(41.0), 1);
        assert_eq!(snap_rows(-81.0), -1);
    }

    /// `apply_width_delta` clamps to `[1, 12]`.
    #[test]
    fn apply_width_delta_clamps_to_grid() {
        assert_eq!(apply_width_delta(6, 0), 6);
        assert_eq!(apply_width_delta(6, 10), 12);
        assert_eq!(apply_width_delta(6, -10), 1);
        assert_eq!(apply_width_delta(1, -5), 1);
    }

    /// `apply_height_delta` clamps to `[1, 12]`.
    #[test]
    fn apply_height_delta_clamps_to_one_and_twelve() {
        assert_eq!(apply_height_delta(2, 0), 2);
        assert_eq!(apply_height_delta(2, 20), 12);
        assert_eq!(apply_height_delta(2, -20), 1);
    }

    /// `apply_column_delta` keeps the panel's right edge within the grid.
    #[test]
    fn apply_column_delta_keeps_panel_inside_grid() {
        // Panel of width 4 cannot start past column 8 (8 + 4 = 12).
        assert_eq!(apply_column_delta(0, 20, 4), 8);
        // A wider panel of width 12 must stay at column 0.
        assert_eq!(apply_column_delta(0, 20, 12), 0);
        // Negative deltas clamp to 0.
        assert_eq!(apply_column_delta(2, -5, 4), 0);
    }

    /// `apply_row_delta` clamps to non-negative rows.
    #[test]
    fn apply_row_delta_clamps_to_zero() {
        assert_eq!(apply_row_delta(3, 0), 3);
        assert_eq!(apply_row_delta(3, -5), 0);
        assert_eq!(apply_row_delta(3, 4), 7);
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

    /// `DragReorderState` starts as active and preserves the original column/row.
    #[test]
    fn drag_reorder_state_construction() {
        let state = DragReorderState {
            from_index: 2,
            original_column: 4,
            original_row: 1,
            start_x: px(50.0),
            start_y: px(60.0),
            working_column: 4,
            working_row: 1,
            active: true,
        };
        assert_eq!(state.from_index, 2);
        assert_eq!(state.original_column, 4);
        assert_eq!(state.working_column, 4);
        assert!(state.active);
    }

    /// `DragResizeState` carries the resize axis along with dimensions.
    #[test]
    fn drag_resize_state_construction() {
        let state = DragResizeState {
            panel_index: 1,
            axis: ResizeAxis::Both,
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
        assert_eq!(state.axis, ResizeAxis::Both);
    }
}
