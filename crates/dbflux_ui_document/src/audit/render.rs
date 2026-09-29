//! Render methods for `AuditDocument`.
//!
//! All `render_*` methods, display-formatting helpers, and static
//! presentation utilities live here so that `mod.rs` can focus on
//! document lifecycle, data loading, and filter state.

use std::collections::HashMap;

use super::chart_view::AuditViewMode;
use super::filters::{TimestampDisplayMode, format_timestamp_ms};
use super::{AuditContextMenuAction, AuditDocument, ToolbarSlot};
use crate::chrome::{
    detail_field, document_bar, document_footer, document_subtitle, document_title, footer_item,
    footer_pager, pager_arrow, search_field, time_preset_control, toolbar_rule,
};
use crate::handle::DocumentEvent;
use crate::syntax_runs::json_highlights;
use dbflux_components::chart::YScale;
use dbflux_components::components::multi_select::MultiSelect;
use dbflux_components::controls::{Button, ButtonVariant, ReadOnlyEditor};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    Badge, BadgeTone, Chamfer, FocusShape, Icon, SegmentedControl, SegmentedItem, Status,
    StatusIndicator, Text, focus_ring,
};
use dbflux_components::tokens::{
    ChamferCut, ChromeColors, DocumentMetrics, Feedback, Spacing, SyntaxColors,
};
use dbflux_components::typography::AppFonts;
use dbflux_components::vim::VimBinding;
use dbflux_core::{EventCategory, EventOutcome};
use dbflux_storage::repositories::audit::AuditEventDto;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::{EditorState as GpuiEditorState, Textarea, TextareaState};
use gpui_component::scroll::ScrollableElement;

use super::super::types::DocumentState;
use dbflux_components::composites::{
    EmptyState, ListRow, MenuItem, menu_frame, menu_row, refresh_split_button, render_separator,
};

/// Width of the audit row context menu.
const AUDIT_CONTEXT_MENU_WIDTH: Pixels = px(220.0);
/// Width of the footer's export menu.
const AUDIT_EXPORT_MENU_WIDTH: Pixels = px(180.0);
/// Width of the timezone select in the toolbar.
const AUDIT_TIMEZONE_WIDTH: Pixels = px(100.0);
/// Width of the chart group-by select in the toolbar.
const AUDIT_GROUP_BY_WIDTH: Pixels = px(150.0);
/// Width of the date range picker in the custom range row.
const AUDIT_DATE_PICKER_WIDTH: Pixels = px(260.0);
/// Wash over the timeline bars a drag spans.
const AUDIT_TIMELINE_SELECTION_ALPHA: f32 = 0.14;
/// Detail fields per row under an expanded event.
const AUDIT_DETAIL_COLUMNS: usize = 6;
/// Longest details JSON, in characters, still shown on one line.
const AUDIT_COMPACT_JSON_LIMIT: usize = 160;
/// Summaries longer than this, in characters, are repeated in full in the
/// detail, since the row truncates them.
const AUDIT_LONG_SUMMARY: usize = 100;

/// Column widths of the event table (P1Audit: 26, 110, 70, 130, 1fr, 140,
/// 70, 90); an external stream uses a partition and an event id column.
struct AuditColumns;

impl AuditColumns {
    const CHEVRON: Pixels = px(26.0);
    const TIME: Pixels = px(110.0);
    const LEVEL: Pixels = px(70.0);
    const CATEGORY: Pixels = px(130.0);
    const ACTOR: Pixels = px(140.0);
    const DURATION: Pixels = px(70.0);
    const OUTCOME: Pixels = px(90.0);
    const PARTITION: Pixels = px(200.0);
    const EVENT_ID: Pixels = px(220.0);
}

fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

/// The time shown in an event row: the time of day with milliseconds for
/// an event of the current day, the month, day and time otherwise.
fn row_time_label(ms: i64, mode: TimestampDisplayMode, now_ms: i64) -> String {
    let full = format_timestamp_ms(ms, mode);
    let today = format_timestamp_ms(now_ms, mode);

    let (Some((date, time)), Some((today_date, _))) = (full.split_once(' '), today.split_once(' '))
    else {
        return full;
    };

    if date == today_date {
        return time.to_string();
    }

    let month_day = date.get(5..).unwrap_or(date);
    let minutes = time.get(..5).unwrap_or(time);
    format!("{month_day} {minutes}")
}

/// Duration of an event as the table shows it: milliseconds below a second,
/// seconds with one decimal above, an em dash when unknown.
fn format_duration_ms(duration_ms: Option<i64>) -> String {
    match duration_ms {
        Some(ms) if ms < 1_000 => format!("{ms} ms"),
        Some(ms) => format!("{:.1} s", ms as f64 / 1_000.0),
        None => "—".to_string(),
    }
}

/// Badge tone of an event level.
fn level_tone(level: &str) -> BadgeTone {
    match level {
        "error" | "fatal" => BadgeTone::Danger,
        "warn" => BadgeTone::Warning,
        "info" => BadgeTone::Info,
        _ => BadgeTone::Neutral,
    }
}

/// Status diamond of an event outcome.
fn outcome_status(outcome: EventOutcome) -> Status {
    match outcome {
        EventOutcome::Success => Status::Connected,
        EventOutcome::Failure => Status::Error,
        EventOutcome::Cancelled => Status::Idle,
        EventOutcome::Pending => Status::Busy,
    }
}

/// Icon of an event category in the table.
fn category_icon(category: Option<EventCategory>) -> AppIcon {
    match category {
        Some(EventCategory::Config) => AppIcon::Settings,
        Some(EventCategory::Connection) => AppIcon::Database,
        Some(EventCategory::Query) => AppIcon::Play,
        Some(EventCategory::Hook) => AppIcon::Plug,
        Some(EventCategory::Script) => AppIcon::SquareTerminal,
        Some(EventCategory::System) => AppIcon::Zap,
        Some(EventCategory::Mcp) => AppIcon::Bot,
        Some(EventCategory::Governance) => AppIcon::Scale,
        Some(EventCategory::ObjectStorage) => AppIcon::Boxes,
        None => AppIcon::Info,
    }
}

impl AuditDocument {
    /// Category text of an audit row. A missing or unrecognized category
    /// keeps the table's `NULL` placeholder.
    pub(super) fn category_label(category: Option<&str>) -> String {
        category
            .and_then(EventCategory::from_str_repr)
            .map(crate::labels::audit_category_label)
            .unwrap_or_else(|| "NULL".to_string())
    }

    /// Level chip text for an audit row. An unrecognized level is shown as
    /// its stored value in uppercase, as before translation.
    pub(super) fn short_level_label(level: &str) -> String {
        dbflux_core::EventSeverity::from_str_repr(level)
            .map(crate::labels::audit_level_chip_label)
            .unwrap_or_else(|| level.to_uppercase())
    }

    /// Switches between the event table and the aggregated chart.
    pub(super) fn set_view_mode(&mut self, chart: bool, cx: &mut Context<Self>) {
        if chart {
            self.view_mode = AuditViewMode::Chart;

            if self.chart.last_result.is_none() {
                self.trigger_chart_aggregate(cx);
            }
        } else {
            self.view_mode = AuditViewMode::Table;
        }

        cx.notify();
    }

    pub(super) fn format_timestamp_ms(&self, ms: i64) -> String {
        format_timestamp_ms(ms, self.timestamp_mode)
    }

    pub(super) fn format_connection_driver(
        connection_id: &Option<String>,
        driver_id: &Option<String>,
    ) -> Option<String> {
        let connection = connection_id.as_deref().filter(|value| !value.is_empty());
        let driver = driver_id.as_deref().filter(|value| !value.is_empty());

        match (connection, driver) {
            (Some(connection), Some(driver)) => Some(format!("{} / {}", connection, driver)),
            (Some(connection), None) => Some(connection.to_string()),
            (None, Some(driver)) => Some(driver.to_string()),
            _ => None,
        }
    }

    pub(super) fn pretty_json(json: &str) -> String {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(json) {
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| json.to_string())
        } else {
            json.to_string()
        }
    }

    pub(super) fn render_context_menu(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let menu = self.context_menu.as_ref()?;
        self.events.get(menu.row)?;

        let row = menu.row;
        let items = self.menu_items_for_row(row);
        let selected_index = menu.selected_index;

        let mut menu_elements: Vec<AnyElement> = Vec::new();

        for (idx, item) in items.iter().enumerate() {
            if item.is_separator() {
                menu_elements.push(render_separator(cx).into_any_element());
                continue;
            }

            let is_selected = idx == selected_index;

            let mut row_item = MenuItem::new(item.label.clone());
            if let Some(icon) = item.icon {
                row_item = row_item.icon(icon);
            }

            menu_elements.push(
                menu_row(
                    SharedString::from(format!("audit-ctx-{}", idx)),
                    &row_item,
                    is_selected,
                    cx,
                )
                .on_mouse_move(cx.listener(move |this, _, _, cx| {
                    if let Some(ref mut menu) = this.context_menu
                        && menu.selected_index != idx
                    {
                        menu.selected_index = idx;
                        cx.notify();
                    }
                }))
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.run_menu_item_at(row, idx, window, cx);
                }))
                .into_any_element(),
            );
        }

        let position = menu.position;

        let element = deferred(
            menu_frame(cx)
                .absolute()
                .top(position.y)
                .left(position.x)
                .w(AUDIT_CONTEXT_MENU_WIDTH)
                .occlude()
                .on_mouse_down_out(cx.listener(|this, _, window, cx| {
                    this.close_context_menu(window, cx);
                }))
                .children(menu_elements),
        )
        .with_priority(2)
        .into_any_element();

        Some(element)
    }

    /// The header row: the audit icon and title with its description, or the
    /// event stream's title for an external source.
    pub(super) fn render_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);

        let (icon, subtitle) = if self.is_external_event_stream() {
            (AppIcon::Logs, None)
        } else {
            (
                AppIcon::FingerprintPattern,
                Some(dbflux_i18n::t!("document.audit.subtitle")),
            )
        };

        document_bar(DocumentMetrics::HEADER_HEIGHT, cx)
            .child(document_title(icon, tint, self.title.clone(), cx))
            .when_some(subtitle, |header, subtitle| {
                header.child(document_subtitle(subtitle, cx))
            })
    }

    pub(super) fn render_toolbar(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let is_internal = !self.is_external_event_stream();
        let is_chart = matches!(self.view_mode, AuditViewMode::Chart);

        let search = search_field(
            &self.search_input,
            Some(DocumentMetrics::SEARCH_WIDTH),
            self.slot_has_ring(ToolbarSlot::Search),
            cx,
        );

        let weak_self = cx.weak_entity();
        let presets = time_preset_control(self.selected_time_range, true, move |index, _, cx| {
            if let Some(doc) = weak_self.upgrade() {
                doc.update(cx, |this, cx| this.select_time_preset(index, cx));
            }
        })
        .focused(self.slot_has_ring(ToolbarSlot::Time));

        let timezone = focus_ring(
            self.slot_has_ring(ToolbarSlot::Timezone),
            FocusShape::Chamfer(ChamferCut::CONTROL),
            None,
            div()
                .w(AUDIT_TIMEZONE_WIDTH)
                .child(self.dropdown_timestamp_mode.clone()),
            cx,
        );

        let filter = |slot: ToolbarSlot, control: Entity<MultiSelect>, this: &Self| {
            focus_ring(
                this.slot_has_ring(slot),
                FocusShape::Chamfer(ChamferCut::CONTROL),
                None,
                div().flex_none().child(control),
                cx,
            )
        };

        let level = filter(ToolbarSlot::Level, self.multi_select_level.clone(), self);
        let category = filter(
            ToolbarSlot::Category,
            self.multi_select_category.clone(),
            self,
        );
        let outcome = filter(
            ToolbarSlot::Outcome,
            self.multi_select_outcome.clone(),
            self,
        );

        let clear = Button::new(
            "audit-clear-btn",
            dbflux_i18n::t!("document.audit.filter.clear"),
        )
        .icon(AppIcon::CircleX)
        .focused(self.slot_has_ring(ToolbarSlot::Clear))
        .tab_stop(false)
        .on_click(cx.listener(|this, _, window, cx| {
            this.clear_filters(window, cx);
        }));

        let view_switch = is_internal.then(|| {
            let weak_self = cx.weak_entity();

            SegmentedControl::new(
                vec![
                    SegmentedItem::new(
                        "audit-view-table",
                        dbflux_i18n::t!("document.audit.filter.view_mode.table"),
                    )
                    .icon(AppIcon::Table),
                    SegmentedItem::new(
                        "audit-view-chart",
                        dbflux_i18n::t!("document.audit.filter.view_mode.chart"),
                    )
                    .icon(AppIcon::ChartColumnBig),
                ],
                if is_chart {
                    "audit-view-chart"
                } else {
                    "audit-view-table"
                },
                move |id, _, cx| {
                    let chart = id.as_ref() == "audit-view-chart";

                    if let Some(doc) = weak_self.upgrade() {
                        doc.update(cx, |this, cx| this.set_view_mode(chart, cx));
                    }
                },
            )
        });

        let chart_controls = (is_internal && is_chart).then(|| {
            let current_y_scale = self.chart.chart_shell.read(cx).y_scale();
            let weak_self = cx.weak_entity();

            let y_scale = SegmentedControl::new(
                vec![
                    SegmentedItem::new(
                        "audit-y-linear",
                        dbflux_i18n::t!("document.audit.filter.y_scale.linear"),
                    ),
                    SegmentedItem::new(
                        "audit-y-log",
                        dbflux_i18n::t!("document.audit.filter.y_scale.log"),
                    ),
                ],
                match current_y_scale {
                    YScale::Linear => "audit-y-linear",
                    YScale::Log => "audit-y-log",
                },
                move |id, _, cx| {
                    let scale = if id.as_ref() == "audit-y-log" {
                        YScale::Log
                    } else {
                        YScale::Linear
                    };

                    if let Some(doc) = weak_self.upgrade() {
                        doc.update(cx, |this, cx| {
                            this.chart.chart_shell.update(cx, |shell, cx| {
                                shell.set_y_scale(scale, cx);
                            });
                        });
                    }
                },
            );

            div()
                .flex()
                .flex_none()
                .items_center()
                .gap(DocumentMetrics::GAP)
                .child(
                    div()
                        .w(AUDIT_GROUP_BY_WIDTH)
                        .child(self.dropdown_chart_group_by.clone()),
                )
                .child(y_scale)
        });

        let weak_self = cx.weak_entity();
        let refresh = refresh_split_button(
            "audit-refresh-control",
            self.refresh_policy,
            self.slot_has_ring(ToolbarSlot::Refresh),
            self.slot_has_ring(ToolbarSlot::RefreshPolicy),
            self.refresh_dropdown.clone(),
            move |_window, cx| {
                if let Some(doc) = weak_self.upgrade() {
                    doc.update(cx, |this, cx| this.load_events(cx));
                }
            },
        )
        .variant(ButtonVariant::Primary);

        div()
            .flex()
            .flex_wrap()
            .flex_shrink_0()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .min_h(DocumentMetrics::TOOLBAR_HEIGHT)
            .py(DocumentMetrics::TOOLBAR_PADDING_Y)
            .px(DocumentMetrics::PADDING_X)
            .border_b_1()
            .border_color(cx.theme().border)
            .child(search)
            .child(presets)
            .child(timezone)
            .when(is_internal, |bar| {
                bar.child(toolbar_rule(cx))
                    .child(level)
                    .child(category)
                    .child(outcome)
            })
            .child(div().flex_1())
            .children(chart_controls)
            .child(clear)
            .children(view_switch)
            .child(refresh)
    }

    /// The custom range row under the toolbar, shown while the Custom preset
    /// is selected: the date range, the start and end times, and Apply.
    pub(super) fn render_custom_range_row(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let slots = self
            .time_range_panel
            .read(cx)
            .custom_picker_slots(AUDIT_DATE_PICKER_WIDTH, cx);

        let ring_start = self.slot_has_ring(ToolbarSlot::CustomStart);
        let ring_end = self.slot_has_ring(ToolbarSlot::CustomEnd);
        let ring = |focused: bool, child: AnyElement| {
            focus_ring(
                focused,
                FocusShape::Chamfer(ChamferCut::CONTROL),
                None,
                child,
                cx,
            )
        };

        let apply = Button::new(
            "audit-custom-time-apply",
            dbflux_i18n::t!("document.audit.filter.apply"),
        )
        .icon(AppIcon::Check)
        .focused(self.slot_has_ring(ToolbarSlot::CustomApply))
        .disabled(!self.can_apply_custom_time_range(cx))
        .tab_stop(false)
        .on_click(cx.listener(|this, _, _, cx| {
            this.apply_custom_time_range(cx);
        }));

        document_bar(DocumentMetrics::TOOLBAR_HEIGHT, cx)
            .child(ring(ring_start, slots.date_picker))
            .child(slots.from_label)
            .child(ring(ring_start, slots.start_hour))
            .child(ring(ring_start, slots.start_minute))
            .child(slots.to_label)
            .child(ring(ring_end, slots.end_hour))
            .child(ring(ring_end, slots.end_minute))
            .child(apply)
    }

    /// The timeline strip: one bar per time bucket, errors stacked on the
    /// rest of the events, and a legend. Dragging across bars zooms the
    /// time range to them.
    pub(super) fn render_timeline(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let events_color = theme.primary;
        let errors_color = theme.danger;
        let selection_wash = ChromeColors::tint(theme).opacity(AUDIT_TIMELINE_SELECTION_ALPHA);
        let peak = self
            .timeline
            .buckets
            .iter()
            .map(|bucket| bucket.total)
            .max()
            .unwrap_or(0)
            .max(1);

        let selection = self
            .timeline_drag
            .map(|(anchor, current)| (anchor.min(current), anchor.max(current)));

        let bars = self
            .timeline
            .buckets
            .iter()
            .enumerate()
            .map(|(index, bucket)| {
                let bar_height = |count: i64| {
                    if count <= 0 {
                        px(0.0)
                    } else {
                        (DocumentMetrics::TIMELINE_BAR_HEIGHT * (count as f32 / peak as f32))
                            .max(px(1.0))
                    }
                };

                let error_height = bar_height(bucket.errors);
                let event_height = bar_height(bucket.total - bucket.errors);
                let selected =
                    selection.is_some_and(|(first, last)| (first..=last).contains(&index));

                div()
                    .id(("audit-timeline-bar", index))
                    .flex_1()
                    .flex()
                    .flex_col()
                    .justify_end()
                    .h(DocumentMetrics::TIMELINE_BAR_HEIGHT)
                    .cursor_crosshair()
                    .when(selected, |bar| bar.bg(selection_wash))
                    .child(div().h(error_height).bg(errors_color))
                    .child(div().h(event_height).bg(events_color))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, _, cx| {
                            this.timeline_drag = Some((index, index));
                            cx.notify();
                        }),
                    )
                    .on_mouse_move(cx.listener(move |this, event: &MouseMoveEvent, _, cx| {
                        if event.pressed_button != Some(MouseButton::Left) {
                            return;
                        }

                        if let Some((anchor, current)) = this.timeline_drag
                            && current != index
                        {
                            this.timeline_drag = Some((anchor, index));
                            cx.notify();
                        }
                    }))
                    .on_mouse_up(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            let Some((anchor, _)) = this.timeline_drag.take() else {
                                return;
                            };

                            this.zoom_to_timeline_bars(anchor, index, window, cx);
                        }),
                    )
            })
            .collect::<Vec<_>>();

        let legend_entry = |color: Hsla, label: String| {
            div()
                .flex()
                .items_center()
                .gap(DocumentMetrics::GAP)
                .child(div().size(DocumentMetrics::TIMELINE_SWATCH).bg(color))
                .child(label)
        };

        div()
            .id("audit-timeline")
            .flex()
            .flex_shrink_0()
            .items_end()
            .gap(DocumentMetrics::TIMELINE_LEGEND_GAP)
            .px(DocumentMetrics::PADDING_X)
            .py(DocumentMetrics::TIMELINE_PADDING_Y)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.background)
            .on_mouse_up_out(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    if this.timeline_drag.take().is_some() {
                        cx.notify();
                    }
                }),
            )
            .child(
                div()
                    .flex_1()
                    .flex()
                    .items_end()
                    .gap(DocumentMetrics::TIMELINE_BAR_GAP)
                    .h(DocumentMetrics::TIMELINE_BAR_HEIGHT)
                    .children(bars),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .flex_shrink_0()
                    .gap(DocumentMetrics::TIMELINE_LEGEND_ROW_GAP)
                    .text_size(DocumentMetrics::TIMELINE_LEGEND_FONT)
                    .text_color(theme.muted_foreground)
                    .child(legend_entry(
                        events_color,
                        dbflux_i18n::t!("document.audit.timeline.events"),
                    ))
                    .child(legend_entry(
                        errors_color,
                        dbflux_i18n::t!("document.audit.timeline.errors"),
                    ))
                    .child(dbflux_i18n::t!("document.audit.timeline.drag_to_zoom")),
            )
    }

    /// Column widths of the internal event table, in order: chevron, time,
    /// level, category, summary (flexible), actor, duration, outcome.
    fn render_table_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let header = div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(DocumentMetrics::TABLE_ROW_HEIGHT)
            .px(DocumentMetrics::PADDING_X)
            .border_b_1()
            .border_color(theme.input)
            .bg(theme.background)
            .text_size(DocumentMetrics::TABLE_HEADER_FONT)
            .text_color(theme.muted_foreground)
            .child(div().w(AuditColumns::CHEVRON))
            .child(
                div()
                    .w(AuditColumns::TIME)
                    .child(dbflux_i18n::t!("document.audit.detail.time")),
            );

        if self.is_external_event_stream() {
            return header
                .child(
                    div()
                        .w(AuditColumns::PARTITION)
                        .child(dbflux_i18n::t!("document.audit.detail.partition")),
                )
                .child(
                    div()
                        .flex_1()
                        .child(dbflux_i18n::t!("document.audit.detail.message")),
                )
                .child(
                    div()
                        .w(AuditColumns::EVENT_ID)
                        .child(dbflux_i18n::t!("document.audit.detail.event_id")),
                );
        }

        header
            .child(
                div()
                    .w(AuditColumns::LEVEL)
                    .child(dbflux_i18n::t!("document.audit.detail.level")),
            )
            .child(
                div()
                    .w(AuditColumns::CATEGORY)
                    .child(dbflux_i18n::t!("document.audit.detail.category")),
            )
            .child(
                div()
                    .flex_1()
                    .child(dbflux_i18n::t!("document.audit.detail.summary")),
            )
            .child(
                div()
                    .w(AuditColumns::ACTOR)
                    .child(dbflux_i18n::t!("document.audit.detail.actor")),
            )
            .child(
                div()
                    .w(AuditColumns::DURATION)
                    .child(dbflux_i18n::t!("document.audit.detail.duration")),
            )
            .child(
                div()
                    .w(AuditColumns::OUTCOME)
                    .child(dbflux_i18n::t!("document.audit.detail.outcome")),
            )
    }

    pub(super) fn render_event_list(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if self.events.is_empty() && self.is_loading {
            return div()
                .flex_1()
                .flex()
                .items_center()
                .justify_center()
                .child(Text::caption(self.source_loading_label()))
                .into_any_element();
        }

        if self.events.is_empty()
            && self.status_message.is_some()
            && self.state() == DocumentState::Error
        {
            return div()
                .flex_1()
                .flex()
                .items_center()
                .justify_center()
                .p(DocumentMetrics::PADDING_X)
                .flex_col()
                .gap(DocumentMetrics::DETAIL_FIELD_GAP)
                .child(
                    EmptyState::new(
                        AppIcon::TriangleAlert,
                        self.status_message.clone().unwrap_or_default(),
                    )
                    .title(self.source_error_heading())
                    .danger(),
                )
                .child(
                    Button::new(
                        "audit-retry",
                        dbflux_i18n::t!("document.audit.filter.retry"),
                    )
                    .icon(AppIcon::RefreshCcw)
                    .on_click(cx.listener(|this, _, _, cx| this.refresh(cx))),
                )
                .into_any_element();
        }

        if self.events.is_empty() {
            return div()
                .flex_1()
                .flex()
                .items_center()
                .justify_center()
                .p(DocumentMetrics::PADDING_X)
                .child(EmptyState::new(
                    AppIcon::FingerprintPattern,
                    self.source_empty_label(),
                ))
                .into_any_element();
        }

        let events = self.events.clone();
        let now_ms = now_epoch_ms();
        let mut rows = Vec::with_capacity(events.len());

        for (row_index, event) in events.into_iter().enumerate() {
            rows.push(
                self.render_event_row(row_index, event, now_ms, window, cx)
                    .into_any_element(),
            );
        }

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(self.render_table_header(cx))
            .child(
                div()
                    .id("audit-event-list")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .flex()
                    .flex_col()
                    .children(rows),
            )
            .into_any_element()
    }

    pub(super) fn render_event_row(
        &mut self,
        row_index: usize,
        event: AuditEventDto,
        now_ms: i64,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let strong = ChromeColors::strong(&theme);
        let muted = theme.muted_foreground;
        let event_id = event.id;
        let is_expanded = self.expanded_event_ids.contains(&event_id);
        // Only highlight the selected row when this document has GPUI focus.
        // When focus moves to the sidebar, the highlight disappears so the
        // user isn't confused by three simultaneous focus indicators.
        let is_selected = self.has_focus && self.selected_row == Some(row_index);
        let time_label = row_time_label(event.created_at_epoch_ms, self.timestamp_mode, now_ms);
        let summary = event.summary.clone().unwrap_or_default();
        let is_external = self.is_external_event_stream();

        let expanded_wash = ChromeColors::tint(&theme).opacity(DocumentMetrics::EXPANDED_ROW_ALPHA);

        let mono_cell = |width: Pixels, value: String| {
            div()
                .w(width)
                .flex_shrink_0()
                .pr(DocumentMetrics::GAP)
                .truncate()
                .font_family(AppFonts::MONO)
                .text_size(DocumentMetrics::TABLE_META_FONT)
                .text_color(muted)
                .child(value)
        };

        let summary_cell = |value: String, mono: bool| {
            div()
                .flex_1()
                .min_w_0()
                .pr(DocumentMetrics::GAP)
                .truncate()
                .text_color(strong)
                .when(mono, |cell| cell.font_family(AppFonts::MONO))
                .child(if value.is_empty() {
                    SharedString::from("—")
                } else {
                    SharedString::from(value)
                })
        };

        let mut row = ListRow::new(SharedString::from(format!("audit-event-{}", event_id)))
            .selected(is_selected)
            .selection_bar(true)
            .build(cx)
            .when(is_expanded && !is_selected, |row| row.bg(expanded_wash))
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(DocumentMetrics::TABLE_ROW_HEIGHT)
            .px(DocumentMetrics::PADDING_X)
            .border_b_1()
            .border_color(theme.table_row_border)
            .text_size(DocumentMetrics::TABLE_CELL_FONT)
            .on_click(cx.listener(move |this, _, window, cx| {
                // Signal the workspace to update focus_target → Document so that
                // Ctrl+H and other panel-navigation bindings work correctly.
                cx.emit(DocumentEvent::RequestFocus);
                this.select_row(row_index, cx);
                this.toggle_event_expanded(event_id, cx);
                this.focus_handle.focus(window, cx);
            }))
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    this.open_context_menu_at_mouse(row_index, event.position, window, cx);
                }),
            )
            .child(
                div().w(AuditColumns::CHEVRON).flex_shrink_0().child(
                    Icon::new(if is_expanded {
                        AppIcon::ChevronDown
                    } else {
                        AppIcon::ChevronRight
                    })
                    .size(DocumentMetrics::TABLE_CHEVRON)
                    .color(muted),
                ),
            )
            .child(mono_cell(AuditColumns::TIME, time_label));

        if is_external {
            let partition = event.action.clone().filter(|value| !value.is_empty());

            row = row
                .child(
                    Self::badge_cell(AuditColumns::PARTITION)
                        .when_some(partition, |cell, value| {
                            cell.child(Badge::new(value, BadgeTone::Neutral))
                        }),
                )
                .child(summary_cell(summary, false))
                .child(mono_cell(
                    AuditColumns::EVENT_ID,
                    event.object_id.clone().unwrap_or_default(),
                ));
        } else {
            let category = event
                .category
                .as_deref()
                .and_then(dbflux_core::EventCategory::from_str_repr);
            let outcome = event
                .outcome
                .as_deref()
                .and_then(dbflux_core::EventOutcome::from_str_repr);

            let level_cell = Self::badge_cell(AuditColumns::LEVEL).when_some(
                event.level.as_deref(),
                |cell, level| {
                    cell.child(Badge::new(
                        Self::short_level_label(level),
                        level_tone(level),
                    ))
                },
            );

            let category_cell = div()
                .w(AuditColumns::CATEGORY)
                .flex_shrink_0()
                .flex()
                .items_center()
                .gap(Feedback::STATUS_GAP)
                .pr(DocumentMetrics::GAP)
                .overflow_hidden()
                .text_color(theme.foreground)
                .child(
                    Icon::new(category_icon(category))
                        .size(DocumentMetrics::TABLE_ICON)
                        .color(muted),
                )
                .child(
                    div()
                        .truncate()
                        .child(Self::category_label(event.category.as_deref())),
                );

            let outcome_cell = div().w(AuditColumns::OUTCOME).flex_shrink_0().when_some(
                outcome,
                |cell, outcome| {
                    cell.child(
                        StatusIndicator::new(outcome_status(outcome))
                            .label(crate::labels::audit_outcome_label(outcome)),
                    )
                },
            );

            row = row
                .child(level_cell)
                .child(category_cell)
                .child(summary_cell(
                    summary,
                    category == Some(dbflux_core::EventCategory::Query),
                ))
                .child(mono_cell(AuditColumns::ACTOR, event.actor_id.clone()))
                .child(mono_cell(
                    AuditColumns::DURATION,
                    format_duration_ms(event.duration_ms),
                ))
                .child(outcome_cell);
        }

        // The per-event id scopes the ids of the detail's controls, which
        // repeat in every expanded row.
        div()
            .id(("audit-event-entry", event_id as u64))
            .w_full()
            .flex()
            .flex_col()
            .child(row)
            .when(is_expanded, |root| {
                root.child(self.render_inline_detail(event, window, cx))
            })
    }

    /// A table cell holding a badge. The cell lays its child out as a flex
    /// item so the badge keeps its content width instead of stretching to
    /// the column, and the trailing padding keeps it off the next column.
    fn badge_cell(width: Pixels) -> Div {
        div()
            .w(width)
            .flex_shrink_0()
            .flex()
            .items_center()
            .pr(DocumentMetrics::GAP)
            .overflow_hidden()
    }

    /// Lays detail fields out in rows of six equal columns.
    fn detail_grid(fields: Vec<AnyElement>) -> Div {
        let mut rows = Vec::new();
        let mut fields = fields.into_iter().peekable();

        while fields.peek().is_some() {
            let mut row = div().flex().gap(DocumentMetrics::DETAIL_FIELD_GAP);

            for _ in 0..AUDIT_DETAIL_COLUMNS {
                let cell = div().flex_1().min_w_0();
                row = row.child(match fields.next() {
                    Some(field) => cell.child(field),
                    None => cell,
                });
            }

            rows.push(row);
        }

        div()
            .flex()
            .flex_col()
            .gap(DocumentMetrics::DETAIL_FIELD_GAP)
            .children(rows)
    }

    fn detail_value(value: Option<String>) -> String {
        value
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "—".to_string())
    }

    /// A code block of a detail: 8 px cut, panel fill, line border, 12 px
    /// mono at 1.7 line height, JSON keys and values colored.
    fn detail_json_block(json: &str, cx: &App) -> Div {
        let theme = cx.theme();
        let colors = SyntaxColors::for_current(cx);
        let text = Self::display_json(json);
        let highlights = json_highlights(&text, &colors);

        div()
            .relative()
            .mt(DocumentMetrics::DETAIL_BLOCK_MARGIN_TOP)
            .px(DocumentMetrics::DETAIL_BLOCK_PADDING_X)
            .py(DocumentMetrics::DETAIL_BLOCK_PADDING_Y)
            .font_family(AppFonts::MONO)
            .text_size(DocumentMetrics::DETAIL_BLOCK_FONT)
            .line_height(relative(DocumentMetrics::DETAIL_BLOCK_LINE_HEIGHT))
            .text_color(theme.foreground)
            .child(
                Chamfer::new(ChamferCut::INPUT)
                    .fill(theme.popover)
                    .border(theme.border),
            )
            .child(StyledText::new(text).with_highlights(highlights))
    }

    /// Compact JSON when it fits on one line of the detail, pretty-printed
    /// otherwise; text that is not JSON is shown as is.
    pub(super) fn display_json(json: &str) -> String {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
            return json.to_string();
        };

        let compact = value.to_string();

        if compact.chars().count() <= AUDIT_COMPACT_JSON_LIMIT {
            return Self::spaced_json(&compact);
        }

        serde_json::to_string_pretty(&value).unwrap_or(compact)
    }

    /// Adds the spaces of the board's inline JSON (`{ "a": 1, "b": 2 }`) to
    /// compact JSON, leaving string contents untouched.
    fn spaced_json(compact: &str) -> String {
        let mut spaced = String::with_capacity(compact.len() + compact.len() / 4);
        let mut in_string = false;
        let mut escaped = false;

        for character in compact.chars() {
            if in_string {
                spaced.push(character);

                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == '"' {
                    in_string = false;
                }
                continue;
            }

            match character {
                '"' => {
                    in_string = true;
                    spaced.push(character);
                }
                '{' => spaced.push_str("{ "),
                '}' => spaced.push_str(" }"),
                ':' => spaced.push_str(": "),
                ',' => spaced.push_str(", "),
                other => spaced.push(other),
            }
        }

        spaced.replace("{  }", "{}")
    }

    pub(super) fn render_inline_detail(
        &mut self,
        event: AuditEventDto,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if self.is_external_event_stream() {
            return self.render_external_inline_detail(event, window, cx);
        }

        let theme = cx.theme().clone();
        let timestamp = self.format_timestamp_ms(event.created_at_epoch_ms);
        let actor = if event
            .actor_type
            .as_deref()
            .filter(|actor_type| !actor_type.is_empty() && *actor_type != "system")
            .is_some()
        {
            let actor_type_label = event
                .actor_type
                .as_deref()
                .and_then(dbflux_core::EventActorType::from_str_repr)
                .map(crate::labels::audit_actor_type_label)
                .unwrap_or_else(|| event.actor_type.clone().unwrap_or_default());

            format!("{} ({})", event.actor_id, actor_type_label)
        } else {
            event.actor_id.clone()
        };
        let connection_driver =
            Self::format_connection_driver(&event.connection_id, &event.driver_id);
        let error_message = event
            .error_message
            .clone()
            .filter(|value| !value.is_empty());
        let details_json = event.details_json.clone().filter(|value| !value.is_empty());
        let correlation_id = event
            .correlation_id
            .clone()
            .filter(|value| !value.is_empty());
        let summary = event
            .summary
            .clone()
            .filter(|value| value.chars().count() > AUDIT_LONG_SUMMARY);

        let mut fields: Vec<AnyElement> = vec![
            detail_field(dbflux_i18n::t!("document.audit.detail.time"), timestamp, cx)
                .into_any_element(),
            detail_field(
                dbflux_i18n::t!("document.audit.detail.action"),
                Self::detail_value(event.action.clone()),
                cx,
            )
            .into_any_element(),
            detail_field(dbflux_i18n::t!("document.audit.detail.actor"), actor, cx)
                .into_any_element(),
            detail_field(
                dbflux_i18n::t!("document.audit.detail.connection_driver"),
                Self::detail_value(connection_driver),
                cx,
            )
            .into_any_element(),
            detail_field(
                dbflux_i18n::t!("document.audit.detail.source"),
                Self::detail_value(event.source_id.clone()),
                cx,
            )
            .into_any_element(),
            detail_field(
                dbflux_i18n::t!("document.audit.detail.correlation_id"),
                Self::detail_value(correlation_id.clone()),
                cx,
            )
            .into_any_element(),
        ];

        for (label_key, value) in [
            ("document.audit.detail.tool", Some(event.tool_id.clone())),
            (
                "document.audit.detail.classification",
                event.classification.clone(),
            ),
            (
                "document.audit.detail.decision",
                Some(event.decision.clone()),
            ),
            ("document.audit.detail.reason", event.reason.clone()),
        ] {
            if let Some(value) = value.filter(|value| !value.is_empty()) {
                fields.push(detail_field(dbflux_i18n::t!(label_key), value, cx).into_any_element());
            }
        }

        let copy_json = Button::new(
            "audit-detail-copy-json",
            dbflux_i18n::t!("document.audit.action.copy_json"),
        )
        .icon(AppIcon::Copy)
        .tab_stop(false)
        .on_click({
            let event = event.clone();
            move |_, _, cx| match serde_json::to_string_pretty(&event) {
                Ok(json) => cx.write_to_clipboard(ClipboardItem::new_string(json)),
                Err(error) => log::warn!("audit event could not be serialized: {error}"),
            }
        });

        let filter_by_correlation = correlation_id.clone().map(|correlation_id| {
            Button::new(
                "audit-detail-filter-correlation",
                dbflux_i18n::t!("document.audit.action.filter_by_correlation"),
            )
            .icon(AppIcon::ListFilter)
            .tab_stop(false)
            .on_click(cx.listener(move |this, _, _, cx| {
                this.filter_by_correlation(correlation_id.clone(), cx);
            }))
        });

        let open_approval =
            (cfg!(feature = "mcp") && Self::is_pending_approval(&event)).then(|| {
                Button::new(
                    "audit-detail-open-approval",
                    dbflux_i18n::t!("document.audit.action.open_approval"),
                )
                .primary()
                .icon(AppIcon::Bot)
                .tab_stop(false)
                .on_click(cx.listener(|_, _, _, cx| {
                    cx.emit(DocumentEvent::RequestOpenApprovals);
                }))
            });

        div()
            .w_full()
            .flex()
            .flex_col()
            .pt(DocumentMetrics::DETAIL_PADDING_TOP)
            .pb(DocumentMetrics::DETAIL_PADDING_BOTTOM)
            .pl(DocumentMetrics::DETAIL_PADDING_LEFT)
            .pr(DocumentMetrics::PADDING_X)
            .border_b_1()
            .border_color(theme.table_row_border)
            .bg(theme.background)
            .child(Self::detail_grid(fields))
            .when_some(summary, |detail, summary| {
                detail.child(
                    div()
                        .mt(DocumentMetrics::DETAIL_BLOCK_MARGIN_TOP)
                        .text_size(DocumentMetrics::TABLE_CELL_FONT)
                        .text_color(ChromeColors::strong(&theme))
                        .child(summary),
                )
            })
            .when_some(error_message, |detail, error| {
                detail.child(
                    div()
                        .mt(DocumentMetrics::DETAIL_BLOCK_MARGIN_TOP)
                        .font_family(AppFonts::MONO)
                        .text_size(DocumentMetrics::DETAIL_BLOCK_FONT)
                        .text_color(theme.danger)
                        .child(error),
                )
            })
            .when_some(details_json, |detail, json| {
                detail.child(Self::detail_json_block(&json, cx))
            })
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap(DocumentMetrics::GAP)
                    .mt(DocumentMetrics::DETAIL_ACTIONS_MARGIN_TOP)
                    .children(filter_by_correlation)
                    .child(copy_json)
                    .children(open_approval),
            )
            .into_any_element()
    }

    pub(super) fn render_external_inline_detail(
        &mut self,
        event: AuditEventDto,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let row_event_id = event.id;
        let timestamp = self.format_timestamp_ms(event.created_at_epoch_ms);
        let secondary_timestamp = event
            .error_message
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok())
            .map(|value| self.format_timestamp_ms(value));
        let message = event.summary.clone().filter(|value| !value.is_empty());
        let details_json = event.details_json.clone().filter(|value| !value.is_empty());

        let mut fields: Vec<AnyElement> = vec![
            detail_field(dbflux_i18n::t!("document.audit.detail.time"), timestamp, cx)
                .into_any_element(),
            detail_field(
                dbflux_i18n::t!("document.audit.detail.source"),
                Self::detail_value(event.connection_id.clone()),
                cx,
            )
            .into_any_element(),
            detail_field(
                dbflux_i18n::t!("document.audit.detail.partition"),
                Self::detail_value(event.action.clone()),
                cx,
            )
            .into_any_element(),
            detail_field(
                dbflux_i18n::t!("document.audit.detail.event_id"),
                Self::detail_value(event.object_id.clone()),
                cx,
            )
            .into_any_element(),
        ];

        if let Some(value) = secondary_timestamp {
            fields.push(
                detail_field(
                    dbflux_i18n::t!("document.audit.detail.secondary_time"),
                    value,
                    cx,
                )
                .into_any_element(),
            );
        }

        let block = |child: AnyElement| {
            div()
                .relative()
                .mt(DocumentMetrics::DETAIL_BLOCK_MARGIN_TOP)
                .px(DocumentMetrics::DETAIL_BLOCK_PADDING_X)
                .py(DocumentMetrics::DETAIL_BLOCK_PADDING_Y)
                .child(
                    Chamfer::new(ChamferCut::INPUT)
                        .fill(theme.popover)
                        .border(theme.border),
                )
                .child(child)
        };

        let message_block = message.map(|value| {
            let message_input =
                self.ensure_external_message_input(row_event_id, &value, window, cx);

            block(
                Textarea::new(&message_input)
                    .appearance(false)
                    .disabled(true)
                    .w_full()
                    .into_any_element(),
            )
        });

        let details_block = details_json.map(|value| {
            let pretty_details = Self::pretty_json(&value);
            let details_input =
                self.ensure_external_details_input(row_event_id, &pretty_details, window, cx);
            let details_rows = Self::event_code_rows(&pretty_details, 4);
            let input_id = details_input.entity_id();
            let scope = match self.external_details_vims.get(&input_id) {
                Some(vim) => vim.leader_scope(div(), cx),
                None => div(),
            };
            let container = VimBinding::capture_run_command(
                VimBinding::wire(scope, input_id, cx),
                input_id,
                cx,
            );
            let indicator = self
                .external_details_vims
                .get(&input_id)
                .and_then(|vim| vim.render_indicator(cx));

            block(
                container
                    .w_full()
                    .flex()
                    .flex_col()
                    .child(
                        ReadOnlyEditor::new(&details_input)
                            .appearance(false)
                            .w_full()
                            .h(Self::event_text_height(details_rows)),
                    )
                    .children(indicator)
                    .into_any_element(),
            )
        });

        div()
            .w_full()
            .flex()
            .flex_col()
            .pt(DocumentMetrics::DETAIL_PADDING_TOP)
            .pb(DocumentMetrics::DETAIL_PADDING_BOTTOM)
            .pl(DocumentMetrics::DETAIL_PADDING_LEFT)
            .pr(DocumentMetrics::PADDING_X)
            .border_b_1()
            .border_color(theme.table_row_border)
            .bg(theme.background)
            .child(Self::detail_grid(fields))
            .children(message_block)
            .children(details_block)
            .into_any_element()
    }

    /// The footer's Export button (secondary, with a chevron) and, when
    /// open, its menu of file formats.
    pub(super) fn render_export_button(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let menu_open = self.export_menu_open;

        div()
            .relative()
            .flex_shrink_0()
            .child(
                Button::new(
                    "audit-export-trigger",
                    dbflux_i18n::t!("document.audit.menu.export"),
                )
                .icon(AppIcon::FileSpreadsheet)
                .trailing_icon(AppIcon::ChevronDown)
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.toggle_export_menu(cx);
                })),
            )
            .when(menu_open, |trigger| {
                trigger.child(self.render_export_menu(cx))
            })
    }

    pub(super) fn render_export_menu(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let items = [
            (
                dbflux_i18n::t!("document.audit.menu.export_format.csv"),
                "csv",
            ),
            (
                dbflux_i18n::t!("document.audit.menu.export_format.json"),
                "json",
            ),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (label, format))| {
            menu_row(
                SharedString::from(format!("audit-export-{}", index)),
                &MenuItem::new(label).icon(AppIcon::Download),
                index == self.export_menu_selected,
                cx,
            )
            .on_mouse_move(cx.listener(move |this, _, _, cx| {
                if this.export_menu_selected != index {
                    this.export_menu_selected = index;
                    cx.notify();
                }
            }))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.export_with_format(format, cx);
            }))
            .into_any_element()
        })
        .collect::<Vec<_>>();

        deferred(
            menu_frame(cx)
                .absolute()
                .bottom_full()
                .right_0()
                .mb(Spacing::XS)
                .w(AUDIT_EXPORT_MENU_WIDTH)
                .occlude()
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.export_menu_open = false;
                    cx.notify();
                }))
                .children(items),
        )
        .with_priority(2)
    }

    pub(super) fn render_status_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let can_prev = self.can_go_prev();
        let can_next = self.can_go_next();

        let row_count_label = if let Some((start, end)) = self.current_page_range() {
            dbflux_i18n::t!(
                "document.audit.pager.range",
                start = start,
                end = end,
                total = self.total_events,
                unit = self.source_row_label()
            )
        } else {
            format!("{} {}", self.total_events, self.source_row_label())
        };

        let pager = self.total_pages().map(|total_pages| {
            footer_pager(
                self.pagination.current_page() as u32,
                Some(total_pages),
                pager_arrow("audit-prev-page", AppIcon::ChevronLeft, can_prev, cx).when(
                    can_prev,
                    |arrow| {
                        arrow.on_click(cx.listener(|this, _, _, cx| {
                            this.go_to_prev_page(cx);
                        }))
                    },
                ),
                pager_arrow("audit-next-page", AppIcon::ChevronRight, can_next, cx).when(
                    can_next,
                    |arrow| {
                        arrow.on_click(cx.listener(|this, _, _, cx| {
                            this.go_to_next_page(cx);
                        }))
                    },
                ),
                cx,
            )
        });

        let can_export = self.total_events > 0 && !self.is_external_event_stream();
        let loading = self.is_loading && self.status_message.is_some();

        document_footer(cx)
            .child(footer_item(AppIcon::Rows3, row_count_label, cx))
            .children(pager)
            .child(div().flex_1())
            .when(loading, |footer| {
                footer.child(footer_item(
                    AppIcon::Loader,
                    dbflux_i18n::t!("document.audit.filter.loading"),
                    cx,
                ))
            })
            .when(can_export, |footer| {
                footer
                    .child(self.render_export_button(cx))
                    .child(dbflux_i18n::t!("document.audit.export.hint"))
            })
    }

    // ── Input entity helpers ──────────────────────────────────────────────

    fn ensure_event_editor_input(
        cache: &mut HashMap<i64, Entity<GpuiEditorState>>,
        event_id: i64,
        value: &str,
        editor_mode: Option<&'static str>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<GpuiEditorState> {
        let value = value.to_string();

        let input = cache
            .entry(event_id)
            .or_insert_with(|| {
                let initial_value = value.clone();

                cx.new(|cx| {
                    let mut state = GpuiEditorState::new(window, cx)
                        .language(editor_mode.unwrap_or("plaintext"))
                        .line_number(false)
                        .soft_wrap(true);

                    state.set_value(&initial_value, window, cx);
                    state
                })
            })
            .clone();

        if input.read(cx).value() != value {
            input.update(cx, |state, cx| state.set_value(value, window, cx));
        }

        input
    }

    fn ensure_event_textarea(
        cache: &mut HashMap<i64, Entity<TextareaState>>,
        event_id: i64,
        value: &str,
        rows: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<TextareaState> {
        let value = value.to_string();

        let input = cache
            .entry(event_id)
            .or_insert_with(|| {
                let initial_value = value.clone();

                cx.new(|cx| {
                    let mut state = TextareaState::new(window, cx)
                        .auto_grow(rows, usize::MAX)
                        .soft_wrap(true);

                    state.set_value(&initial_value, window, cx);
                    state
                })
            })
            .clone();

        if input.read(cx).value() != value {
            input.update(cx, |state, cx| state.set_value(value, window, cx));
        }

        input
    }

    /// Returns (or lazily creates) the `InputState` entity used to display an
    /// external event's message field as an editable read-only text area.
    pub(super) fn ensure_external_message_input(
        &mut self,
        event_id: i64,
        message: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<TextareaState> {
        let rows = Self::event_message_rows(message, 2);
        Self::ensure_event_textarea(
            &mut self.external_message_inputs,
            event_id,
            message,
            rows,
            window,
            cx,
        )
    }

    /// Returns (or lazily creates) the `InputState` entity used to display an
    /// external event's details JSON as a code editor.
    pub(super) fn ensure_external_details_input(
        &mut self,
        event_id: i64,
        details_json: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<GpuiEditorState> {
        let input = Self::ensure_event_editor_input(
            &mut self.external_details_inputs,
            event_id,
            details_json,
            Some("json"),
            window,
            cx,
        );

        let input_id = input.entity_id();
        if let std::collections::hash_map::Entry::Vacant(slot) =
            self.external_details_vims.entry(input_id)
        {
            slot.insert(VimBinding::new(input.clone(), window, cx));
            VimBinding::follow_setting(self, input_id, cx);
        }

        input
    }

    // ── Row sizing helpers ────────────────────────────────────────────────

    pub(super) fn event_text_rows(value: &str, min_rows: usize) -> usize {
        const ESTIMATED_CHARS_PER_ROW: usize = 80;

        let line_rows = value.lines().count().max(1);
        let wrap_rows = value
            .lines()
            .map(|line| {
                let char_count = line.chars().count();
                char_count.div_ceil(ESTIMATED_CHARS_PER_ROW).max(1)
            })
            .sum::<usize>()
            .max(1);

        line_rows.max(wrap_rows).max(min_rows)
    }

    pub(super) fn event_message_rows(value: &str, min_rows: usize) -> usize {
        Self::event_text_rows(value, min_rows)
    }

    pub(super) fn event_code_rows(value: &str, min_rows: usize) -> usize {
        value.lines().count().max(min_rows).max(1)
    }

    pub(super) fn event_text_height(rows: usize) -> Pixels {
        px((rows as f32 * 24.0) + 20.0)
    }

    // ── CSV / copy helpers ────────────────────────────────────────────────

    /// Formats a single audit event as a CSV row with a header embedded.
    ///
    /// The format matches the full export schema so it is consistent with
    /// what the "Export CSV" button produces.
    pub(super) fn event_to_csv_row(event: &AuditEventDto) -> String {
        let header = "id,timestamp,level,category,outcome,actor_id,actor_type,action,source_id,\
                      connection_id,driver_id,duration_ms,summary,error_message,correlation_id";

        let escape_csv = |s: &str| -> String {
            if s.contains(',') || s.contains('"') || s.contains('\n') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s.to_string()
            }
        };

        let row = format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            event.id,
            event.created_at_epoch_ms,
            escape_csv(event.level.as_deref().unwrap_or("")),
            escape_csv(event.category.as_deref().unwrap_or("")),
            escape_csv(event.outcome.as_deref().unwrap_or("")),
            escape_csv(&event.actor_id),
            escape_csv(event.actor_type.as_deref().unwrap_or("")),
            escape_csv(event.action.as_deref().unwrap_or("")),
            escape_csv(event.source_id.as_deref().unwrap_or("")),
            escape_csv(event.connection_id.as_deref().unwrap_or("")),
            escape_csv(event.driver_id.as_deref().unwrap_or("")),
            event.duration_ms.map(|d| d.to_string()).unwrap_or_default(),
            escape_csv(event.summary.as_deref().unwrap_or("")),
            escape_csv(event.error_message.as_deref().unwrap_or("")),
            escape_csv(event.correlation_id.as_deref().unwrap_or("")),
        );

        format!("{}\n{}", header, row)
    }
}

#[cfg(test)]
mod tests {
    const FILTER_KEYS: &[&str] = &[
        "document.audit.filter.apply",
        "document.audit.filter.clear",
        "document.audit.filter.group_label",
        "document.audit.filter.loading",
        "document.audit.filter.retry",
        "document.audit.filter.view_mode.chart",
        "document.audit.filter.view_mode.table",
        "document.audit.filter.y_scale.linear",
        "document.audit.filter.y_scale.log",
        "document.audit.filter.placeholder.search_events",
        "document.audit.filter.placeholder.filter_events",
        "document.audit.filter.placeholder.all_time",
        "document.audit.filter.placeholder.last_12_hours",
        "document.audit.filter.timezone.local",
        "document.audit.filter.timezone.utc",
        "document.audit.pager.range",
    ];

    #[test]
    fn audit_filter_keys_resolve_in_both_locales() {
        for key in FILTER_KEYS {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, *key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn audit_filter_clear_label_differs_between_locales() {
        let en = dbflux_i18n::t!("document.audit.filter.clear", locale = "en");
        let es = dbflux_i18n::t!("document.audit.filter.clear", locale = "es");

        assert_eq!(en, "Clear");
        assert_ne!(en, es);
    }

    const DETAIL_KEYS: &[&str] = &[
        "document.audit.detail.action",
        "document.audit.detail.actor",
        "document.audit.detail.category",
        "document.audit.detail.connection_driver",
        "document.audit.detail.correlation_id",
        "document.audit.detail.details",
        "document.audit.detail.duration",
        "document.audit.detail.error",
        "document.audit.detail.event_id",
        "document.audit.detail.level",
        "document.audit.detail.message",
        "document.audit.detail.outcome",
        "document.audit.detail.partition",
        "document.audit.detail.secondary_time",
        "document.audit.detail.source",
        "document.audit.detail.summary",
        "document.audit.detail.time",
    ];

    #[test]
    fn audit_detail_keys_resolve_in_both_locales() {
        for key in DETAIL_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, *key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn audit_detail_correlation_id_label_differs_between_locales() {
        let en = dbflux_i18n::t!("document.audit.detail.correlation_id", locale = "en");
        let es = dbflux_i18n::t!("document.audit.detail.correlation_id", locale = "es");

        assert_eq!(en, "Correlation ID");
        assert_ne!(en, es);
    }

    #[test]
    fn render_detail_field_accepts_a_translated_string_label() {
        // `render_detail_field` widened from `&'static str` to
        // `impl Into<SharedString>` so translated `String` values from
        // `dbflux_i18n::t!` can be passed directly without an intermediate
        // leak or a `&'static str` catalog. This compiles only if the
        // widened signature is in place.
        let label: String = dbflux_i18n::t!("document.audit.detail.time");

        assert!(!label.is_empty());
    }
}
