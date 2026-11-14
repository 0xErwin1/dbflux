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

use crate::chrome::{document_bar, document_title, time_preset_control};
use dbflux_components::composites::refresh_split_button;
use dbflux_components::controls::Button;
use dbflux_components::controls::InputState;
use dbflux_components::primitives::{Badge, BadgeTone, Icon, SegmentedControl, SegmentedItem};
use dbflux_components::saved_chart::TimeRangePreset;
use dbflux_components::tokens::{ChromeColors, DashboardMetrics, DocumentMetrics};
use gpui::prelude::*;
use gpui::{
    App, Context, CursorStyle, Entity, IntoElement, MouseButton, PathBuilder, Pixels, Window,
    canvas, div, point, px,
};
use gpui_component::ActiveTheme;

use super::{DashboardDocument, DashboardMode};

/// Width of the date range picker in the custom range row.
const DASHBOARD_DATE_PICKER_WIDTH: Pixels = px(220.0);

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

/// All five time-range preset variants in display order. Display labels come
/// from `preset_label`, which resolves them through the catalog.
#[allow(dead_code)]
pub(super) const TIME_RANGE_PRESETS: &[TimeRangePreset] = &[
    TimeRangePreset::Last15min,
    TimeRangePreset::LastHour,
    TimeRangePreset::Last6Hours,
    TimeRangePreset::Last24Hours,
    TimeRangePreset::Last7Days,
];

/// Returns the translated display label for a `TimeRangePreset`.
#[allow(dead_code)]
pub(super) fn preset_label(preset: TimeRangePreset) -> String {
    match preset {
        TimeRangePreset::Last15min => {
            dbflux_i18n::t!("document.dashboard.builder.preset.last_15_min")
        }
        TimeRangePreset::LastHour => dbflux_i18n::t!("document.dashboard.builder.preset.last_hour"),
        TimeRangePreset::Last6Hours => {
            dbflux_i18n::t!("document.dashboard.builder.preset.last_6_hours")
        }
        TimeRangePreset::Last24Hours => {
            dbflux_i18n::t!("document.dashboard.builder.preset.last_24_hours")
        }
        TimeRangePreset::Last7Days => {
            dbflux_i18n::t!("document.dashboard.builder.preset.last_7_days")
        }
    }
}

// ---------------------------------------------------------------------------
// Render helpers (pub(super) — used only by render.rs)
// ---------------------------------------------------------------------------

/// Returns the dashboard header row (P1Dashboard).
///
/// Renders (left to right): the dashboard icon and title with its
/// connection badge, then the time presets, the refresh split, "Add panel"
/// and the View/Edit switch. Read-only dashboards trade the last two for
/// "Save as editable". While the Custom preset is selected, a second row
/// carries the date and time pickers.
pub(super) fn dashboard_toolbar(
    dashboard: &DashboardDocument,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    use dbflux_components::common::time_range::TimeRange;
    use dbflux_components::icons::AppIcon;

    let theme = cx.theme().clone();
    let tint = ChromeColors::tint(&theme);
    let time_range_panel = dashboard.shared_time_range().clone();
    let refresh_dropdown = dashboard.refresh_dropdown.clone();
    let selected_time_range = time_range_panel.read(cx).selected_time_range;
    let custom_range_visible = selected_time_range == Some(TimeRange::Custom);

    let presets = {
        let panel = time_range_panel.clone();

        time_preset_control(selected_time_range, true, move |index, _, cx| {
            panel.update(cx, |panel, cx| panel.select_preset(index, cx));
        })
    };

    // Refresh split-button — same helper AuditDocument uses. Manual click
    // re-executes every loaded panel; the dropdown segment sets the
    // auto-refresh interval.
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
    );

    let connection_badge = dashboard.profile_id.and_then(|profile_id| {
        dashboard
            .app_state
            .read(cx)
            .profiles()
            .iter()
            .find(|profile| profile.id == profile_id)
            .map(|profile| Badge::new(profile.name.clone(), BadgeTone::Neutral))
    });

    // Session-only notice: instance-metric panels accumulate samples only
    // while DBFlux runs, so a wide shared range (e.g. Last 7 days) must not
    // imply history the panels do not have.
    let has_accumulating_panels = dashboard.panel_slots.iter().any(|slot| match slot {
        crate::dashboard::DashboardPanelSlot::Loaded { panel, .. } => {
            panel.read(cx).source_is_accumulating()
        }
        _ => false,
    });
    let session_samples_note = has_accumulating_panels.then(|| {
        div()
            .flex_shrink_0()
            .text_size(DocumentMetrics::TABLE_META_FONT)
            .text_color(theme.muted_foreground)
            .child(dbflux_i18n::t!("document.chart.session_samples_only"))
    });

    let title = div()
        .flex()
        .min_w_0()
        .items_center()
        .gap(DocumentMetrics::GAP)
        .child(document_title(
            AppIcon::ChartColumnBig,
            tint,
            dashboard.title(),
            cx,
        ))
        .children(connection_badge);

    let is_read_only = dashboard.is_read_only();

    let actions: Vec<gpui::AnyElement> = if is_read_only {
        let on_save_as = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.request_save_as_editable(cx);
        });

        vec![
            Button::new(
                "dash-save-as-editable",
                dbflux_i18n::t!("document.dashboard.toolbar.save_as_editable"),
            )
            .icon(AppIcon::Save)
            .tooltip(dbflux_i18n::t!(
                "document.dashboard.toolbar.save_as_editable_tooltip"
            ))
            .on_click(move |event, window, app| on_save_as(event, window, app))
            .into_any_element(),
        ]
    } else {
        let on_add_panel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.request_add_panel(cx);
        });

        let weak = cx.weak_entity();
        let mode_switch = SegmentedControl::new(
            vec![
                SegmentedItem::new(
                    "dash-mode-view",
                    dbflux_i18n::t!("document.dashboard.toolbar.mode_view"),
                )
                .icon(AppIcon::Eye),
                SegmentedItem::new(
                    "dash-mode-edit",
                    dbflux_i18n::t!("document.dashboard.toolbar.mode_edit"),
                )
                .icon(AppIcon::Pencil),
            ],
            if dashboard.is_edit_mode() {
                "dash-mode-edit"
            } else {
                "dash-mode-view"
            },
            move |id, _, cx| {
                let mode = if id.as_ref() == "dash-mode-edit" {
                    DashboardMode::Edit
                } else {
                    DashboardMode::View
                };

                if let Some(doc) = weak.upgrade() {
                    doc.update(cx, |this, cx| this.set_mode(mode, cx));
                }
            },
        );

        vec![
            Button::new(
                "dash-add-panel-toolbar",
                dbflux_i18n::t!("document.dashboard.toolbar.add_panel"),
            )
            .icon(AppIcon::Plus)
            .on_click(move |event, window, app| on_add_panel(event, window, app))
            .into_any_element(),
            mode_switch.into_any_element(),
        ]
    };

    let header = document_bar(DocumentMetrics::HEADER_HEIGHT_TALL, cx)
        .id("dashboard-toolbar")
        .child(title)
        .child(div().flex_1())
        .children(session_samples_note)
        .child(presets)
        .child(div().flex_shrink_0().child(refresh_btn))
        .children(actions);

    div()
        .flex()
        .flex_col()
        .flex_shrink_0()
        .child(header)
        .when(custom_range_visible, |toolbar| {
            toolbar.child(build_custom_time_controls(&time_range_panel, cx))
        })
}

/// Build the custom-range row (date picker + start/end hour/minute + Apply)
/// using the shared `TimeRangePanel::custom_picker_slots` API.
fn build_custom_time_controls(
    panel: &Entity<dbflux_components::common::time_range::view::TimeRangePanel>,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    use dbflux_components::icons::AppIcon;

    let slots = panel
        .read(cx)
        .custom_picker_slots(DASHBOARD_DATE_PICKER_WIDTH, cx);
    let weak_panel = panel.downgrade();

    let can_apply = panel.read(cx).can_apply_custom_range(cx);
    let on_apply = move |_event: &gpui::ClickEvent, _w: &mut Window, app: &mut App| {
        if let Some(panel) = weak_panel.upgrade() {
            panel.update(app, |panel, cx| {
                if let Err(error) = panel.apply_custom_range(cx) {
                    log::debug!("dashboard custom range not applied: {error}");
                }
            });
        }
    };

    document_bar(DocumentMetrics::TOOLBAR_HEIGHT, cx)
        .child(slots.date_picker)
        .child(slots.from_label)
        .child(slots.start_hour)
        .child(slots.start_minute)
        .child(slots.to_label)
        .child(slots.end_hour)
        .child(slots.end_minute)
        .child(
            Button::new(
                "dashboard-custom-time-apply",
                dbflux_i18n::t!("document.dashboard.toolbar.apply"),
            )
            .icon(AppIcon::Check)
            .disabled(!can_apply)
            .on_click(on_apply),
        )
}

/// Returns the panel-header element for a single panel slot (P1Dashboard):
/// 36 px, a drag grip in edit mode, the panel's kind icon in the tint, the
/// title, and in edit mode a settings button that opens the panel menu.
///
/// When `is_editing_title` is true, an `Input` entity is rendered inline for
/// title editing. In edit mode the header is the drag handle.
#[allow(clippy::too_many_arguments)]
pub(super) fn panel_header(
    panel_index: u32,
    title: &str,
    icon: dbflux_components::icons::AppIcon,
    editing_input: Option<&Entity<InputState>>,
    _drag_active: bool,
    menu_open: bool,
    edit_mode: bool,
    has_editable_title: bool,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    use dbflux_components::icons::AppIcon;

    let is_editing = editing_input.is_some();
    let theme = cx.theme().clone();
    let tint = ChromeColors::tint(&theme);

    let mut header = div()
        .id(("panel-header", panel_index))
        .flex()
        .flex_row()
        .flex_shrink_0()
        .items_center()
        .w_full()
        .gap(DocumentMetrics::GAP)
        .h(DashboardMetrics::PANEL_HEADER_HEIGHT)
        .px(DashboardMetrics::PANEL_PADDING)
        .border_b_1()
        .border_color(theme.border);

    // Context menu on right-click — anchors inline next to this panel's
    // settings button. Only available in edit mode.
    if edit_mode {
        header = header.on_mouse_down(
            MouseButton::Right,
            cx.listener(move |this, _: &gpui::MouseDownEvent, _, cx| {
                this.open_panel_context_menu(panel_index, cx);
            }),
        );
    }

    // The header only becomes a drag handle in edit mode, and not while its
    // title is being edited.
    if edit_mode && !is_editing {
        header = header.cursor(CursorStyle::OpenHand).on_mouse_down(
            MouseButton::Left,
            cx.listener(
                move |this,
                      event: &gpui::MouseDownEvent,
                      _,
                      cx: &mut Context<DashboardDocument>| {
                    this.start_panel_drag(panel_index, event.position, cx);
                },
            ),
        );
    }

    if edit_mode {
        header = header.child(
            Icon::new(AppIcon::Grid3x3)
                .size(DashboardMetrics::PANEL_GRIP)
                .color(theme.input),
        );
    }

    header = header.child(
        Icon::new(icon)
            .size(DashboardMetrics::PANEL_ICON)
            .color(tint),
    );

    if let Some(input_state) = editing_input {
        // Commit and cancel are handled by the InputEvent subscription
        // established in `start_panel_title_edit`.
        return header.child(
            div().flex_1().child(
                dbflux_components::controls::Input::new(input_state)
                    .w_full()
                    .small(),
            ),
        );
    }

    header = header.child(
        div()
            .id(("panel-title", panel_index))
            .flex_1()
            .min_w_0()
            .truncate()
            .text_size(DashboardMetrics::PANEL_TITLE_FONT)
            .font_weight(gpui::FontWeight::SEMIBOLD)
            .text_color(ChromeColors::strong(&theme))
            .child(title.to_string()),
    );

    // The settings menu is an edit-mode affordance only. It floats inline
    // next to its trigger through the `.relative()` wrapper below, so its
    // position is independent of the dashboard's window offset.
    if !edit_mode {
        return header;
    }

    let on_settings_click = cx.listener(move |this, _: &gpui::ClickEvent, _, cx| {
        this.open_panel_context_menu(panel_index, cx);
    });

    let settings = div()
        .id(("panel-kebab", panel_index))
        .flex()
        .flex_shrink_0()
        .items_center()
        .cursor_pointer()
        // Keep the header's drag start from firing on the button.
        .on_mouse_down(MouseButton::Left, |_, _, cx| {
            cx.stop_propagation();
        })
        .on_click(on_settings_click)
        .child(
            Icon::new(AppIcon::Settings)
                .size(DashboardMetrics::PANEL_ICON)
                .color(if menu_open {
                    tint
                } else {
                    theme.muted_foreground
                }),
        );

    let menu_panel = menu_open.then(|| panel_kebab_menu(panel_index, has_editable_title, cx));

    header.child(div().relative().flex_shrink_0().child(settings).when_some(
        menu_panel,
        |el, panel| {
            el.child(
                gpui::deferred(
                    div()
                        .absolute()
                        .top(DashboardMetrics::PANEL_ICON)
                        .right(px(0.0))
                        .child(panel),
                )
                .with_priority(2),
            )
        },
    ))
}

/// Build the floating menu panel for the panel at `panel_index`.
///
/// Renders the same `MenuItem` chain used by the sidebar (icons, separator,
/// danger color for `Remove panel`). Click handlers stash the chosen action
/// in `pending_panel_menu_action`; the action is consumed at the start of the
/// next `render` pass where a real `Window` is available.
fn panel_kebab_menu(
    panel_index: u32,
    has_editable_title: bool,
    cx: &mut Context<DashboardDocument>,
) -> gpui::AnyElement {
    use dbflux_components::composites::{MenuItem, render_menu_items};
    use dbflux_components::icons::AppIcon;

    // Build the action list. "Edit title…" is omitted for slot types that
    // carry no user-editable title (Inspector, Divider).
    let mut menu_items: Vec<MenuItem> = vec![
        MenuItem::new(dbflux_i18n::t!("document.dashboard.panel.menu.configure"))
            .icon(AppIcon::Settings),
    ];
    if has_editable_title {
        menu_items.push(
            MenuItem::new(dbflux_i18n::t!("document.dashboard.panel.menu.edit_title"))
                .icon(AppIcon::Pencil),
        );
    }
    menu_items.push(MenuItem::separator());
    menu_items.push(
        MenuItem::new(dbflux_i18n::t!("document.dashboard.panel.menu.remove"))
            .icon(AppIcon::Delete)
            .danger(),
    );

    // Map visual index → domain PanelMenuAction index, skipping the separator
    // and the conditionally absent "Edit title…" item.
    let mut visual_to_action: Vec<Option<usize>> = vec![Some(0)]; // Configure
    if has_editable_title {
        visual_to_action.push(Some(1)); // EditTitle
    }
    visual_to_action.push(None); // separator
    visual_to_action.push(Some(2)); // RemovePanel

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
        .w(DashboardMetrics::RESIZE_STRIP)
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
        .h(DashboardMetrics::RESIZE_STRIP)
        .cursor(CursorStyle::ResizeUpDown)
        .on_mouse_down(MouseButton::Left, on_resize_start)
}

/// Returns the bottom-right corner resize grip for a panel slot: a 14 px
/// triangle filling the card's cut corner, in the tint on the focused
/// panel and the strong line otherwise. Dragging it resizes the panel on
/// both axes simultaneously.
pub(super) fn panel_resize_corner(
    panel_index: u32,
    focused: bool,
    cx: &mut Context<DashboardDocument>,
) -> impl IntoElement {
    let on_resize_start = cx.listener(
        move |this, event: &gpui::MouseDownEvent, _, cx: &mut Context<DashboardDocument>| {
            this.start_panel_resize(panel_index, ResizeAxis::Both, event.position, cx);
        },
    );

    let theme = cx.theme();
    let color = if focused {
        ChromeColors::tint(theme)
    } else {
        theme.input
    };

    div()
        .id(("panel-resize-corner", panel_index))
        .size(DashboardMetrics::RESIZE_CORNER)
        .absolute()
        .bottom(px(0.0))
        .right(px(0.0))
        .cursor(CursorStyle::ResizeUpLeftDownRight)
        .on_mouse_down(MouseButton::Left, on_resize_start)
        .child(
            canvas(
                |_, _, _| {},
                move |bounds, _, window, _| {
                    let mut builder = PathBuilder::fill();
                    builder.move_to(point(bounds.right(), bounds.top()));
                    builder.line_to(point(bounds.right(), bounds.bottom()));
                    builder.line_to(point(bounds.left(), bounds.bottom()));
                    builder.close();

                    match builder.build() {
                        Ok(path) => window.paint_path(path, color),
                        Err(error) => log::warn!("Failed to build resize grip path: {error}"),
                    }
                },
            )
            .size_full(),
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

    /// Every preset in the table is labeled through the translated catalog,
    /// in display order, with one distinct label per preset.
    #[test]
    fn time_range_presets_table_labels_come_from_the_catalog() {
        let expected_keys = [
            "document.dashboard.builder.preset.last_15_min",
            "document.dashboard.builder.preset.last_hour",
            "document.dashboard.builder.preset.last_6_hours",
            "document.dashboard.builder.preset.last_24_hours",
            "document.dashboard.builder.preset.last_7_days",
        ];

        let labels: Vec<String> = TIME_RANGE_PRESETS
            .iter()
            .map(|preset| preset_label(*preset))
            .collect();
        let expected: Vec<String> = expected_keys
            .iter()
            .map(|key| dbflux_i18n::t!(key))
            .collect();

        assert_eq!(labels, expected);

        let mut distinct = labels.clone();
        distinct.sort();
        distinct.dedup();
        assert_eq!(distinct.len(), TIME_RANGE_PRESETS.len());
    }

    /// `preset_label` routes every `TimeRangePreset` variant through the
    /// `document.dashboard.builder.preset.*` catalog instead of returning a
    /// hardcoded English string.
    #[test]
    fn preset_label_returns_correct_string() {
        assert_eq!(
            preset_label(TimeRangePreset::Last15min),
            dbflux_i18n::t!("document.dashboard.builder.preset.last_15_min")
        );
        assert_eq!(
            preset_label(TimeRangePreset::LastHour),
            dbflux_i18n::t!("document.dashboard.builder.preset.last_hour")
        );
        assert_eq!(
            preset_label(TimeRangePreset::Last6Hours),
            dbflux_i18n::t!("document.dashboard.builder.preset.last_6_hours")
        );
        assert_eq!(
            preset_label(TimeRangePreset::Last24Hours),
            dbflux_i18n::t!("document.dashboard.builder.preset.last_24_hours")
        );
        assert_eq!(
            preset_label(TimeRangePreset::Last7Days),
            dbflux_i18n::t!("document.dashboard.builder.preset.last_7_days")
        );
    }

    /// `preset_label` keys resolve in both locales and diverge between them,
    /// so the parity loop cannot pass on English fallbacks copied verbatim
    /// into `es.yml`.
    #[test]
    fn preset_label_keys_resolve_and_differ_between_locales() {
        let keys = [
            "document.dashboard.builder.preset.last_15_min",
            "document.dashboard.builder.preset.last_hour",
            "document.dashboard.builder.preset.last_6_hours",
            "document.dashboard.builder.preset.last_24_hours",
            "document.dashboard.builder.preset.last_7_days",
        ];
        for key in keys {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);
                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }

        let en = dbflux_i18n::t!("document.dashboard.builder.preset.last_hour", locale = "en");
        let es = dbflux_i18n::t!("document.dashboard.builder.preset.last_hour", locale = "es");
        assert_ne!(en, es);
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

    /// The dashboard toolbar and per-panel kebab-menu keys resolve in both
    /// locales and are not accidentally left off the catalog.
    #[test]
    fn dashboard_toolbar_and_panel_menu_keys_resolve_in_both_locales() {
        let keys = [
            "document.dashboard.toolbar.add_panel",
            "document.dashboard.toolbar.apply",
            "document.dashboard.toolbar.mode_view",
            "document.dashboard.toolbar.mode_edit",
            "document.dashboard.toolbar.save_as_editable",
            "document.dashboard.toolbar.save_as_editable_tooltip",
            "document.dashboard.panel.menu.configure",
            "document.dashboard.panel.menu.edit_title",
            "document.dashboard.panel.menu.remove",
        ];
        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);
                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    /// At least one toolbar key must actually diverge between locales.
    #[test]
    fn dashboard_toolbar_add_panel_differs_between_locales() {
        let en = dbflux_i18n::t!("document.dashboard.toolbar.add_panel", locale = "en");
        let es = dbflux_i18n::t!("document.dashboard.toolbar.add_panel", locale = "es");
        assert_ne!(en, es);
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
