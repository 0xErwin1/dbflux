//! Render methods for `AuditDocument`.
//!
//! All `render_*` methods, display-formatting helpers, and static
//! presentation utilities live here so that `mod.rs` can focus on
//! document lifecycle, data loading, and filter state.

use std::collections::HashMap;

use super::chart_view::AuditViewMode;
use super::filters::{TimeRange, format_timestamp_ms};
use super::{AuditContextMenuAction, AuditDocument, AuditDocumentSource, ToolbarSlot};
use crate::ui::document::handle::DocumentEvent;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_components::chart::YScale;
use dbflux_components::controls::{
    GpuiInput as Input, InputState, ReadonlyTextView, SelectableText,
};
use dbflux_components::primitives::{Icon, Label, Text, surface_raised};
use dbflux_components::tokens::BannerColors;
use dbflux_storage::repositories::audit::AuditEventDto;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::ButtonVariants;
use gpui_component::scroll::ScrollableElement;

use super::super::chrome::{compact_top_bar, workspace_footer_bar};
use super::super::types::DocumentState;

impl AuditDocument {
    /// Renders a null placeholder matching the DataTable convention: italic muted "NULL".
    pub(super) fn null_display(_theme: &gpui_component::Theme) -> Div {
        div()
            .italic()
            .child(Text::caption("NULL").muted_foreground())
    }

    pub(super) fn short_category_label(category: Option<&str>) -> &'static str {
        match category {
            Some("config") => "CONFIG",
            Some("connection") => "CONN",
            Some("query") => "QUERY",
            Some("hook") => "HOOK",
            Some("script") => "SCRIPT",
            Some("system") => "SYS",
            Some("mcp") => "MCP",
            Some("governance") => "GOV",
            _ => "NULL",
        }
    }

    /// Foreground color for a level chip, tinted via `BannerColors`.
    ///
    /// - error → Danger (red)
    /// - warn  → Warning (amber)
    /// - info  → Info (blue)
    /// - debug/trace/other → Neutral (muted)
    pub(super) fn level_color(level: Option<&str>, theme: &gpui_component::Theme) -> Hsla {
        match level {
            Some("error") => BannerColors::danger_fg(theme),
            Some("warn") => BannerColors::warning_fg(theme),
            Some("info") => BannerColors::info_fg(theme),
            _ => theme.muted_foreground,
        }
    }

    /// Background tint for a level chip, sourced from `BannerColors`.
    pub(super) fn level_bg_color(level: Option<&str>, theme: &gpui_component::Theme) -> Hsla {
        match level {
            Some("error") => BannerColors::danger_bg(theme),
            Some("warn") => BannerColors::warning_bg(theme),
            Some("info") => BannerColors::info_bg(theme),
            _ => {
                let mut neutral = theme.muted_foreground;
                neutral.a = 0.15;
                neutral
            }
        }
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
        let theme = cx.theme().clone();

        let event = self.events.get(menu.row)?;
        let has_correlation = event
            .correlation_id
            .as_deref()
            .map(|c| !c.is_empty())
            .unwrap_or(false);

        let items = Self::context_menu_items(has_correlation);
        let selected_index = menu.selected_index;

        let mut menu_elements: Vec<AnyElement> = Vec::new();

        for (idx, item) in items.iter().enumerate() {
            if item.is_separator() {
                menu_elements.push(
                    div()
                        .h(px(1.0))
                        .mx(Spacing::SM)
                        .my(Spacing::XS)
                        .bg(theme.border)
                        .into_any_element(),
                );
                continue;
            }

            let Some(action) = item.action else {
                continue;
            };

            let is_selected = idx == selected_index;
            let label = item.label;
            let icon = item.icon;

            // Icon color follows the DataGridPanel context menu convention.
            let icon_color = if is_selected {
                theme.accent_foreground
            } else {
                theme.muted_foreground
            };

            menu_elements.push(
                div()
                    .id(SharedString::from(format!("audit-ctx-{}", idx)))
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .mx(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .when(is_selected, |d| d.bg(theme.accent))
                    .when(!is_selected, |d| d.hover(|d| d.bg(theme.secondary)))
                    // Icon or indent to keep label alignment consistent.
                    .when_some(icon, |d, icon| {
                        d.child(Icon::new(icon).size(px(16.0)).color(icon_color))
                    })
                    .when(icon.is_none(), |d| d.pl(px(20.0)))
                    .on_mouse_move(cx.listener(move |this, _, _, cx| {
                        if let Some(ref mut menu) = this.context_menu
                            && menu.selected_index != idx
                        {
                            menu.selected_index = idx;
                            cx.notify();
                        }
                    }))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        // Resolve the action again — the menu may have changed.
                        let has_corr = this
                            .context_menu
                            .as_ref()
                            .and_then(|m| this.events.get(m.row))
                            .and_then(|e| e.correlation_id.as_deref())
                            .map(|c| !c.is_empty())
                            .unwrap_or(false);
                        let items = Self::context_menu_items(has_corr);
                        if let Some(item) = items.get(idx)
                            && item.action == Some(action)
                            && let Some(menu) = this.context_menu.clone()
                        {
                            let event = this.events.get(menu.row).cloned();
                            this.close_context_menu(window, cx);
                            match action {
                                AuditContextMenuAction::CopyRowAsCsv => {
                                    if let Some(event) = event {
                                        let csv = Self::event_to_csv_row(&event);
                                        cx.write_to_clipboard(ClipboardItem::new_string(csv));
                                    }
                                }
                                AuditContextMenuAction::CopySummary => {
                                    if let Some(event) = event {
                                        let summary = event.summary.clone().unwrap_or_default();
                                        cx.write_to_clipboard(ClipboardItem::new_string(summary));
                                    }
                                }
                                AuditContextMenuAction::FilterByCorrelation => {
                                    if let Some(event) = event
                                        && let Some(correlation_id) =
                                            event.correlation_id.clone().filter(|c| !c.is_empty())
                                    {
                                        this.filter_by_correlation(correlation_id, cx);
                                    }
                                }
                            }
                        }
                    }))
                    .child(Text::caption(label).color(if is_selected {
                        theme.accent_foreground
                    } else {
                        theme.foreground
                    }))
                    .into_any_element(),
            );
        }

        let position = menu.position;

        let element = deferred(
            surface_raised(cx)
                .absolute()
                .top(position.y)
                .left(position.x)
                .w(px(200.0))
                .shadow_lg()
                .py(Spacing::XS)
                .occlude()
                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                    cx.stop_propagation();
                })
                .on_mouse_down_out(cx.listener(|this, _, window, cx| {
                    this.close_context_menu(window, cx);
                }))
                .children(menu_elements),
        )
        .with_priority(2)
        .into_any_element();

        Some(element)
    }

    pub(super) fn render_toolbar(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();

        // Search input.
        let custom_range_visible = self.selected_time_range == Some(TimeRange::Custom);

        let search_control = div()
            .flex()
            .items_center()
            .w(px(360.0))
            .rounded(Radii::SM)
            .when(self.slot_has_ring(ToolbarSlot::Search), |d| {
                d.border_1().border_color(theme.ring)
            })
            .child(
                div().flex_1().child(
                    dbflux_components::controls::GpuiInput::new(&self.search_input)
                        .small()
                        .h(Heights::BUTTON),
                ),
            );

        // Dropdown wrappers — ring goes around the whole labeled control.
        let time_control = div()
            .rounded(Radii::SM)
            .when(self.slot_has_ring(ToolbarSlot::Time), |d| {
                d.border_1().border_color(theme.ring)
            })
            .child(self.dropdown_time_range.clone());

        let timestamp_mode_control = div()
            .rounded(Radii::SM)
            .when(self.slot_has_ring(ToolbarSlot::Timezone), |d| {
                d.border_1().border_color(theme.ring)
            })
            .child(self.dropdown_timestamp_mode.clone());

        let can_apply_custom_time_range = self.can_apply_custom_time_range(cx);
        let custom_apply_button = div()
            .id("audit-custom-time-apply")
            .h(Heights::BUTTON)
            .flex()
            .items_center()
            .px(Spacing::SM)
            .rounded(Radii::SM)
            .border_1()
            .border_color(if self.slot_has_ring(ToolbarSlot::CustomApply) {
                theme.ring
            } else {
                theme.input
            })
            .when(can_apply_custom_time_range, |d| {
                d.cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.apply_custom_time_range(cx);
                    }))
            })
            .when(!can_apply_custom_time_range, |d| d.opacity(0.45))
            .child(Text::caption("Apply"));

        let custom_time_controls = div()
            .flex()
            .items_center()
            .gap_1()
            .child(
                div()
                    .w(px(260.0))
                    .rounded(Radii::SM)
                    .when(self.slot_has_ring(ToolbarSlot::CustomStart), |d| {
                        d.border_1().border_color(theme.ring)
                    })
                    .child(
                        gpui_component::date_picker::DatePicker::new(
                            &self.custom_date_range_picker,
                        )
                        .small()
                        .placeholder("Select date range")
                        .number_of_months(2),
                    ),
            )
            .child(Text::caption("from"))
            .child(
                div()
                    .w(px(72.0))
                    .rounded(Radii::SM)
                    .when(self.slot_has_ring(ToolbarSlot::CustomStart), |d| {
                        d.border_1().border_color(theme.ring)
                    })
                    .child(self.custom_start_hour_dropdown.clone()),
            )
            .child(
                div()
                    .w(px(72.0))
                    .rounded(Radii::SM)
                    .when(self.slot_has_ring(ToolbarSlot::CustomStart), |d| {
                        d.border_1().border_color(theme.ring)
                    })
                    .child(self.custom_start_minute_dropdown.clone()),
            )
            .child(Text::caption("to"))
            .child(
                div()
                    .w(px(72.0))
                    .rounded(Radii::SM)
                    .when(self.slot_has_ring(ToolbarSlot::CustomEnd), |d| {
                        d.border_1().border_color(theme.ring)
                    })
                    .child(self.custom_end_hour_dropdown.clone()),
            )
            .child(
                div()
                    .w(px(72.0))
                    .rounded(Radii::SM)
                    .when(self.slot_has_ring(ToolbarSlot::CustomEnd), |d| {
                        d.border_1().border_color(theme.ring)
                    })
                    .child(self.custom_end_minute_dropdown.clone()),
            )
            .child(custom_apply_button);

        let level_control = div()
            .rounded(Radii::SM)
            .when(self.slot_has_ring(ToolbarSlot::Level), |d| {
                d.border_1().border_color(theme.ring)
            })
            .child(self.multi_select_level.clone());

        let category_control = div()
            .rounded(Radii::SM)
            .when(self.slot_has_ring(ToolbarSlot::Category), |d| {
                d.border_1().border_color(theme.ring)
            })
            .child(self.multi_select_category.clone());

        let outcome_control = div()
            .rounded(Radii::SM)
            .when(self.slot_has_ring(ToolbarSlot::Outcome), |d| {
                d.border_1().border_color(theme.ring)
            })
            .child(self.multi_select_outcome.clone());

        // Refresh split button.
        let refresh_label = if self.refresh_policy.is_auto() {
            self.refresh_policy.label()
        } else {
            "Refresh"
        };
        let refresh_icon = if self.refresh_policy.is_auto() {
            AppIcon::Clock
        } else {
            AppIcon::RefreshCcw
        };

        let refresh_btn = div()
            .id("audit-refresh-control")
            .h(Heights::BUTTON)
            .flex()
            .items_center()
            .gap_0()
            .rounded(Radii::SM)
            .bg(theme.background)
            .border_1()
            .border_color(if self.slot_has_ring(ToolbarSlot::Refresh) {
                theme.ring
            } else {
                theme.input
            })
            .child(
                div()
                    .id("audit-refresh-action")
                    .h_full()
                    .px(Spacing::SM)
                    .flex()
                    .items_center()
                    .gap_1()
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.accent.opacity(0.08)))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.load_events(cx);
                    }))
                    .child(
                        Icon::new(refresh_icon)
                            .size(px(16.0))
                            .color(theme.foreground),
                    )
                    .child(Text::caption(refresh_label)),
            )
            .child(div().w(px(1.0)).h_full().bg(theme.input))
            .child(
                div()
                    .w(px(28.0))
                    .h_full()
                    .rounded_r(Radii::SM)
                    .when(self.slot_has_ring(ToolbarSlot::RefreshPolicy), |d| {
                        d.border_1().border_color(theme.ring)
                    })
                    .child(self.refresh_dropdown.clone()),
            );

        // Clear button.
        let clear_btn = div()
            .id("audit-clear-btn")
            .h(Heights::BUTTON)
            .flex()
            .items_center()
            .px(Spacing::SM)
            .rounded(Radii::SM)
            .border_1()
            .border_color(if self.slot_has_ring(ToolbarSlot::Clear) {
                theme.ring
            } else {
                gpui::transparent_black()
            })
            .cursor_pointer()
            .hover(|d| d.bg(theme.secondary))
            .on_click(cx.listener(|this, _, window, cx| {
                this.clear_filters(window, cx);
            }))
            .child(Text::caption("Clear"));

        let _ = window;

        compact_top_bar(&theme, {
            let mut items = vec![
                search_control.into_any_element(),
                time_control.into_any_element(),
                timestamp_mode_control.into_any_element(),
            ];

            if custom_range_visible {
                items.push(custom_time_controls.into_any_element());
            }

            if !self.is_external_event_stream() {
                items.extend([
                    level_control.into_any_element(),
                    category_control.into_any_element(),
                    outcome_control.into_any_element(),
                ]);
            }

            // View-mode toggle and chart group-by selector (Internal source only).
            if !self.is_external_event_stream() {
                let is_chart = matches!(self.view_mode, AuditViewMode::Chart);

                let toggle_label = if is_chart { "Table" } else { "Chart" };
                let view_toggle = div()
                    .id("audit-view-toggle")
                    .h(Heights::BUTTON)
                    .flex()
                    .items_center()
                    .px(Spacing::SM)
                    .rounded(Radii::SM)
                    .border_1()
                    .border_color(theme.input)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        if is_chart {
                            this.view_mode = AuditViewMode::Table;
                        } else {
                            this.view_mode = AuditViewMode::Chart;
                            if this.chart.last_result.is_none() {
                                this.trigger_chart_aggregate(cx);
                            }
                        }
                        cx.notify();
                    }))
                    .child(Text::caption(toggle_label));

                items.push(view_toggle.into_any_element());

                if is_chart {
                    // Fixed-width container prevents the dropdown from consuming
                    // the full toolbar row width.  The "Group:" label distinguishes
                    // this selector from the Level/Category/Outcome filter chips.
                    let group_by_control = div()
                        .flex()
                        .items_center()
                        .gap_1()
                        .w(px(148.0))
                        .child(Text::caption("Group:"))
                        .child(div().flex_1().child(self.dropdown_chart_group_by.clone()));
                    items.push(group_by_control.into_any_element());

                    // Y-scale toggle: "Y: Linear" / "Y: Log".
                    // Fixed-width so it does not stretch the toolbar row.
                    let current_y_scale = self.chart.chart_shell.read(cx).y_scale();
                    let y_scale_label = match current_y_scale {
                        YScale::Linear => "Y: Linear",
                        YScale::Log => "Y: Log",
                    };
                    let y_scale_toggle = div()
                        .id("audit-y-scale-toggle")
                        .h(Heights::BUTTON)
                        .w(px(80.0))
                        .flex()
                        .items_center()
                        .justify_center()
                        .px(Spacing::SM)
                        .rounded(Radii::SM)
                        .border_1()
                        .border_color(theme.input)
                        .cursor_pointer()
                        .hover(|d| d.bg(theme.secondary))
                        .on_click(cx.listener(move |this, _, _, cx| {
                            let next_scale = match current_y_scale {
                                YScale::Linear => YScale::Log,
                                YScale::Log => YScale::Linear,
                            };
                            this.chart.chart_shell.update(cx, |shell, cx| {
                                shell.set_y_scale(next_scale, cx);
                            });
                        }))
                        .child(Text::caption(y_scale_label));
                    items.push(y_scale_toggle.into_any_element());
                }
            }

            items.extend([
                div().flex_1().into_any_element(),
                refresh_btn.into_any_element(),
                clear_btn.into_any_element(),
            ]);

            items
        })
    }

    pub(super) fn render_event_list(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if self.events.is_empty() && self.is_loading {
            return div()
                .flex_1()
                .items_center()
                .justify_center()
                .child(Text::muted(self.source_loading_label()))
                .into_any_element();
        }

        if self.events.is_empty()
            && self.status_message.is_some()
            && self.state() == DocumentState::Error
        {
            return div()
                .flex_1()
                .flex()
                .flex_col()
                .items_center()
                .justify_center()
                .gap_3()
                .child(Text::heading(self.source_error_heading()).danger())
                .child(Text::muted(self.status_message.clone().unwrap_or_default()))
                .child(
                    gpui_component::button::Button::new("audit-retry")
                        .label("Retry")
                        .small()
                        .ghost()
                        .on_click(cx.listener(|this, _, _, cx| this.refresh(cx))),
                )
                .into_any_element();
        }

        if self.events.is_empty() {
            return div()
                .flex_1()
                .flex()
                .flex_col()
                .items_center()
                .justify_center()
                .gap_3()
                .child(Text::muted(self.source_empty_label()))
                .into_any_element();
        }

        let events = self.events.clone();
        let mut rows = Vec::with_capacity(events.len());

        for (row_index, event) in events.into_iter().enumerate() {
            rows.push(
                self.render_event_row(row_index, event, window, cx)
                    .into_any_element(),
            );
        }

        div()
            .id("audit-event-list")
            .flex_1()
            .overflow_y_scrollbar()
            .flex()
            .flex_col()
            .children(rows)
            .into_any_element()
    }

    pub(super) fn render_event_row(
        &mut self,
        row_index: usize,
        event: AuditEventDto,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let event_id = event.id;
        let is_expanded = self.expanded_event_ids.contains(&event_id);
        // Only highlight the selected row when this document has GPUI focus.
        // When focus moves to the sidebar, the highlight disappears so the
        // user isn't confused by three simultaneous focus indicators.
        let is_selected = self.has_focus && self.selected_row == Some(row_index);
        let timestamp = self.format_timestamp_ms(event.created_at_epoch_ms);
        let summary = event.summary.clone().unwrap_or_default();
        let summary_display: AnyElement = if summary.is_empty() {
            Self::null_display(&theme).into_any_element()
        } else {
            Text::body(summary).into_any_element()
        };
        let connection_driver =
            Self::format_connection_driver(&event.connection_id, &event.driver_id);
        let event_action = event.action.clone();
        let external_event_id = event.object_id.clone();

        // Background priority: selected (keyboard cursor) > expanded > default.
        // Use theme.list_active for the selected row — same token as key_value and sidebar.
        let row_bg = if is_selected {
            theme.list_active
        } else if is_expanded {
            theme.primary.opacity(0.08)
        } else {
            gpui::transparent_black()
        };

        div()
            .w_full()
            .border_b_1()
            .border_color(theme.border.opacity(0.5))
            .child(
                div()
                    .id(SharedString::from(format!("audit-event-{}", event_id)))
                    .flex()
                    .items_center()
                    .gap_3()
                    .px_3()
                    .py_1p5()
                    .cursor_pointer()
                    .bg(row_bg)
                    // Selected rows get a left-border accent to match other list views.
                    .when(is_selected, |d| d.border_l_2().border_color(theme.accent))
                    .hover(|style| style.bg(theme.list_hover))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            // Signal the workspace to update focus_target → Document so that
                            // Ctrl+H and other panel-navigation bindings work correctly.
                            cx.emit(DocumentEvent::RequestFocus);
                            this.select_row(row_index, cx);
                            this.toggle_event_expanded(event_id, cx);
                            this.focus_handle.focus(window);
                        }),
                    )
                    .on_mouse_down(
                        MouseButton::Right,
                        cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                            // Right-click: open at the actual mouse position.
                            this.open_context_menu_at_mouse(row_index, event.position, window, cx);
                        }),
                    )
                    .child(
                        Icon::new(if is_expanded {
                            AppIcon::ChevronDown
                        } else {
                            AppIcon::ChevronRight
                        })
                        .size(px(12.0))
                        .muted(),
                    )
                    .child(Text::code(timestamp))
                    .when(self.is_external_event_stream(), |row| {
                        row.when_some(event_action.clone(), |row, value| {
                            row.child(
                                div()
                                    .px_1p5()
                                    .py_px()
                                    .rounded(px(3.0))
                                    .bg(theme.primary.opacity(0.15))
                                    .max_w(px(240.0))
                                    .child(
                                        div()
                                            .truncate()
                                            .child(Text::label_sm(value).font_size(FontSizes::XS)),
                                    ),
                            )
                        })
                    })
                    .when(!self.is_external_event_stream(), |row| {
                        let level = event.level.as_deref();
                        let level_display: AnyElement = match level {
                            Some(l) => div()
                                .px_1p5()
                                .py_px()
                                .rounded(px(3.0))
                                .bg(Self::level_bg_color(Some(l), &theme))
                                .flex_shrink_0()
                                .child(
                                    Text::label_sm(l.to_uppercase())
                                        .font_size(FontSizes::XS)
                                        .color(Self::level_color(Some(l), &theme)),
                                )
                                .into_any_element(),
                            None => Self::null_display(&theme)
                                .flex_shrink_0()
                                .into_any_element(),
                        };
                        let category = Self::short_category_label(event.category.as_deref());

                        // Neutral CAT chip — categories share a single muted tint
                        // because the LVL chip already carries severity color.
                        let mut neutral_bg = theme.muted_foreground;
                        neutral_bg.a = 0.15;
                        let category_chip = div()
                            .px_1p5()
                            .py_px()
                            .rounded(px(3.0))
                            .bg(neutral_bg)
                            .flex_shrink_0()
                            .child(
                                Text::label_sm(category.to_string())
                                    .font_size(FontSizes::XS)
                                    .color(theme.muted_foreground),
                            );

                        row.child(level_display).child(category_chip)
                    })
                    .child(div().text_sm().flex_1().truncate().child(summary_display))
                    .when_some(
                        external_event_id.filter(|_| self.is_external_event_stream()),
                        |row, value| row.child(Text::caption(value)),
                    )
                    .when_some(
                        connection_driver.filter(|value| !value.is_empty()),
                        |row, value| row.child(Text::caption(value)),
                    ),
            )
            .when(is_expanded, |root| {
                root.child(self.render_inline_detail(event, window, cx))
            })
    }

    pub(super) fn render_detail_field(
        &self,
        label: &'static str,
        value: Option<String>,
        theme: &gpui_component::Theme,
    ) -> Div {
        let value_element: AnyElement = match value {
            Some(ref v) if !v.is_empty() => Text::body(v.clone()).into_any_element(),
            _ => Self::null_display(theme).into_any_element(),
        };
        div()
            .flex_col()
            .gap_1p5()
            .min_w(px(120.0))
            .child(Label::new(label))
            .child(value_element)
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
        let level = event.level.clone();
        let category = match Self::short_category_label(event.category.as_deref()) {
            "NULL" => None,
            label => Some(label.to_string()),
        };
        let outcome = event.outcome.clone();
        let actor = if event
            .actor_type
            .as_deref()
            .filter(|actor_type| !actor_type.is_empty() && *actor_type != "system")
            .is_some()
        {
            format!(
                "{} ({})",
                event.actor_id,
                event.actor_type.as_deref().unwrap_or("")
            )
        } else {
            event.actor_id.clone()
        };
        let action = event.action.clone();
        let source = event.source_id.clone();
        let connection_driver =
            Self::format_connection_driver(&event.connection_id, &event.driver_id);
        let duration = event
            .duration_ms
            .map(|duration_ms| format!("{} ms", duration_ms));
        let summary = event.summary.clone().filter(|value| !value.is_empty());
        let error_message = event
            .error_message
            .clone()
            .filter(|value| !value.is_empty());
        let details_json = event.details_json.clone().filter(|value| !value.is_empty());
        let correlation_id = event
            .correlation_id
            .clone()
            .filter(|value| !value.is_empty());

        div()
            .px_4()
            .pb_3()
            .pt_1()
            .flex()
            .flex_col()
            .gap_3()
            .bg(theme.secondary.opacity(0.35))
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_4()
                    .children(vec![
                        self.render_detail_field("Time", Some(timestamp), &theme)
                            .into_any_element(),
                        self.render_detail_field("Level", level, &theme)
                            .into_any_element(),
                        self.render_detail_field("Category", category, &theme)
                            .into_any_element(),
                        self.render_detail_field("Outcome", outcome, &theme)
                            .into_any_element(),
                        self.render_detail_field("Actor", Some(actor), &theme)
                            .into_any_element(),
                        self.render_detail_field("Action", action, &theme)
                            .into_any_element(),
                        self.render_detail_field("Source", source, &theme)
                            .into_any_element(),
                    ])
                    .when_some(connection_driver, |row, value| {
                        row.child(self.render_detail_field(
                            "Connection/Driver",
                            Some(value),
                            &theme,
                        ))
                    })
                    .when_some(duration, |row, value| {
                        row.child(self.render_detail_field("Duration", Some(value), &theme))
                    }),
            )
            .when_some(summary, |root, value| {
                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(Label::new("Summary"))
                        .child(Text::body(value)),
                )
            })
            .when_some(error_message, |root, value| {
                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(Label::new("Error").text_color(theme.danger))
                        .child(Text::body(value).danger()),
                )
            })
            .when_some(details_json, |root, value| {
                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(Label::new("Details"))
                        .child(
                            div()
                                .bg(theme.secondary)
                                .p_2()
                                .rounded(Radii::SM)
                                .child(Text::code(Self::pretty_json(&value))),
                        ),
                )
            })
            .when_some(correlation_id, |root, value| {
                let correlation_id_for_click = value.clone();

                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(Label::new("Correlation ID"))
                        .child(
                            div()
                                .cursor_pointer()
                                .hover(|style| style.underline())
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(move |this, _, _, cx| {
                                        this.filter_by_correlation(
                                            correlation_id_for_click.clone(),
                                            cx,
                                        );
                                    }),
                                )
                                .child(Text::body(value.clone()).primary()),
                        ),
                )
            })
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
        let source_name = event.connection_id.clone();
        let source_partition = event.action.clone();
        let event_id = event.object_id.clone();
        let secondary_timestamp = event
            .error_message
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok())
            .map(|value| self.format_timestamp_ms(value));
        let message = event.summary.clone().filter(|value| !value.is_empty());
        let details_json = event.details_json.clone().filter(|value| !value.is_empty());

        div()
            .px_4()
            .pb_3()
            .pt_1()
            .flex()
            .flex_col()
            .gap_3()
            .bg(theme.secondary.opacity(0.35))
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_4()
                    .children(vec![
                        self.render_detail_field("Time", Some(timestamp), &theme)
                            .into_any_element(),
                        self.render_detail_field("Source", source_name, &theme)
                            .into_any_element(),
                        self.render_detail_field("Partition", source_partition, &theme)
                            .into_any_element(),
                        self.render_detail_field("Event ID", event_id, &theme)
                            .into_any_element(),
                    ])
                    .when_some(secondary_timestamp, |row, value| {
                        row.child(self.render_detail_field("Secondary Time", Some(value), &theme))
                    }),
            )
            .when_some(message, |root, value| {
                let message_input =
                    self.ensure_external_message_input(row_event_id, &value, window, cx);

                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(Label::new("Message"))
                        .child(SelectableText::new(&message_input).w_full()),
                )
            })
            .when_some(details_json, |root, value| {
                let pretty_details = Self::pretty_json(&value);
                let details_input =
                    self.ensure_external_details_input(row_event_id, &pretty_details, window, cx);
                let details_rows = Self::event_code_rows(&pretty_details, 4);

                root.child(
                    div()
                        .flex_col()
                        .gap_1p5()
                        .child(Label::new("Details"))
                        .child(
                            div().bg(theme.secondary).p_2().rounded(Radii::SM).child(
                                ReadonlyTextView::new(&details_input)
                                    .w_full()
                                    .h(Self::event_text_height(details_rows)),
                            ),
                        ),
                )
            })
            .into_any_element()
    }

    pub(super) fn render_export_button(
        &self,
        theme: &gpui_component::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let menu_open = self.export_menu_open;

        // Identical to DataGridPanel::render_export_button.
        div()
            .id("audit-export-trigger")
            .relative()
            .flex()
            .items_center()
            .gap_1()
            .px(Spacing::XS)
            .rounded(Radii::SM)
            .cursor_pointer()
            .hover(|d| d.bg(theme.secondary))
            .on_click(cx.listener(|this, _, _, cx| {
                this.toggle_export_menu(cx);
            }))
            .child(Icon::new(AppIcon::FileSpreadsheet).size(px(16.0)).muted())
            .child(Text::caption("Export"))
            .child(Icon::new(AppIcon::ChevronDown).size(px(12.0)).muted())
            .when(menu_open, |trigger| {
                trigger.child(self.render_export_menu(theme, cx))
            })
    }

    pub(super) fn render_export_menu(
        &self,
        theme: &gpui_component::Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let items = [("CSV", "csv"), ("JSON", "json")]
            .into_iter()
            .enumerate()
            .map(|(index, (label, format))| {
                // Identical to DataGridPanel::render_export_menu items.
                div()
                    .id(SharedString::from(format!("audit-export-{}", index)))
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .mx(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.export_with_format(format, cx);
                    }))
                    .child(Text::body(label))
                    .into_any_element()
            })
            .collect::<Vec<_>>();

        // Identical to DataGridPanel::render_export_menu container.
        deferred(
            surface_raised(cx)
                .absolute()
                .bottom_full()
                .right_0()
                .mb(Spacing::XS)
                .w(px(160.0))
                .shadow_lg()
                .py(Spacing::XS)
                .occlude()
                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                    cx.stop_propagation();
                })
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.export_menu_open = false;
                    cx.notify();
                }))
                .children(items),
        )
        .with_priority(1)
    }

    pub(super) fn render_status_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let can_prev = self.can_go_prev();
        let can_next = self.can_go_next();

        // Left: row count with icon — same as DataGridPanel.
        let left = {
            let row_count_label = if let Some((start, end)) = self.current_page_range() {
                format!(
                    "{}-{} of {} {}",
                    start,
                    end,
                    self.total_events,
                    self.source_row_label()
                )
            } else {
                format!("{} {}", self.total_events, self.source_row_label())
            };

            div()
                .flex()
                .items_center()
                .gap_1()
                .child(Icon::new(AppIcon::Rows3).size(px(12.0)).muted())
                .child(Text::caption(row_count_label))
        };

        // Center: pagination — matches the DataGridPanel `‹ N / Total ›`
        // pattern using Unicode single-chevrons (see S5.5).
        let center = div().flex().items_center().gap(Spacing::XS).when_some(
            self.total_pages(),
            |pagination, total_pages| {
                let page = self.pagination.current_page();
                let page_label = if total_pages > 1 {
                    format!("{} / {}", page, total_pages)
                } else {
                    format!("{}", page)
                };

                pagination
                    .child(
                        div()
                            .id("audit-prev-page")
                            .flex()
                            .items_center()
                            .justify_center()
                            .w(px(20.0))
                            .h(px(20.0))
                            .rounded(Radii::SM)
                            .text_size(FontSizes::SM)
                            .when(can_prev, |d| {
                                d.cursor_pointer()
                                    .text_color(theme.foreground)
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.go_to_prev_page(cx);
                                    }))
                            })
                            .when(!can_prev, |d| {
                                d.text_color(theme.muted_foreground).opacity(0.5)
                            })
                            .child("\u{2039}"),
                    )
                    .child(
                        Text::caption(page_label)
                            .font_size(FontSizes::XS)
                            .color(theme.muted_foreground),
                    )
                    .child(
                        div()
                            .id("audit-next-page")
                            .flex()
                            .items_center()
                            .justify_center()
                            .w(px(20.0))
                            .h(px(20.0))
                            .rounded(Radii::SM)
                            .text_size(FontSizes::SM)
                            .when(can_next, |d| {
                                d.cursor_pointer()
                                    .text_color(theme.foreground)
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.go_to_next_page(cx);
                                    }))
                            })
                            .when(!can_next, |d| {
                                d.text_color(theme.muted_foreground).opacity(0.5)
                            })
                            .child("\u{203a}"),
                    )
            },
        );

        // Right: export + loading indicator — same as DataGridPanel.
        let right = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .when(
                self.total_events > 0 && !self.is_external_event_stream(),
                |d| d.child(self.render_export_button(&theme, cx)),
            )
            .when_some(
                self.status_message.clone().filter(|_| self.is_loading),
                |d, _| {
                    d.child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(
                                Icon::new(AppIcon::Loader)
                                    .size(px(12.0))
                                    .color(theme.muted_foreground),
                            )
                            .child(Text::dim("Loading…")),
                    )
                },
            );

        workspace_footer_bar(&theme, left, center, right)
    }

    // ── Input entity helpers ──────────────────────────────────────────────

    /// Returns (or lazily creates) the `InputState` entity used to display an
    /// external event's message field as an editable read-only text area.
    pub(super) fn ensure_external_message_input(
        &mut self,
        event_id: i64,
        message: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<InputState> {
        Self::ensure_event_text_input(
            &mut self.external_message_inputs,
            event_id,
            message,
            None,
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
    ) -> Entity<InputState> {
        Self::ensure_event_text_input(
            &mut self.external_details_inputs,
            event_id,
            details_json,
            Some("json"),
            window,
            cx,
        )
    }

    fn ensure_event_text_input(
        cache: &mut HashMap<i64, Entity<InputState>>,
        event_id: i64,
        value: &str,
        editor_mode: Option<&'static str>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<InputState> {
        let value = value.to_string();
        let rows = if editor_mode.is_some() {
            Self::event_code_rows(&value, 4)
        } else {
            Self::event_message_rows(&value, 2)
        };

        let input = cache
            .entry(event_id)
            .or_insert_with(|| {
                let initial_value = value.clone();
                let initial_rows = rows;

                cx.new(|cx| {
                    let mut state = if let Some(editor_mode) = editor_mode {
                        InputState::new(window, cx)
                            .code_editor(editor_mode)
                            .line_number(false)
                            .rows(initial_rows)
                            .soft_wrap(true)
                    } else {
                        InputState::new(window, cx)
                            .auto_grow(initial_rows, usize::MAX)
                            .soft_wrap(true)
                    };

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
