//! Render implementation for `ChartDocument`.
//!
//! Layout (as seen inside `ResultPanel`):
//!
//!   ┌──────────────────────────────────────────────┐
//!   │ chrome row: title · Run · Save               │  ← ToolbarSegments
//!   ├──────────────────────────────────────────────┤
//!   │ chart toolbar (RANGE/REFRESH/...)            │  ┐
//!   ├──────────────────────────────────────────────┤  │ render_chart_content
//!   │ axis bar (bindings)                          │  │
//!   ├──────────────────────────────────────────────┤  │
//!   │ chart area (fills remaining space)           │  ┘
//!   └──────────────────────────────────────────────┘
//!
//! The chrome row is owned by `ResultPanel`; its content comes from the
//! `ToolbarSegment`s returned by `ChartDocument::header_segments`.
//! The chart content area is rendered by `render_chart_content`, called from
//! the `ViewHandle::render` closure built by `into_view_handle`.

use super::{ChartDocument, ExecState, should_render_stats_rail, toggle_stats_rail};
use crate::chart::ChartRailTab;
use crate::chart::metric_picker_render::MetricPickerView;
use crate::chart::toolbar::{ChartToolbarContext, ChartToolbarHandlers, render_chart_toolbar};
use dbflux_components::chart::{
    ChartDetection, ChartView, axis_bar_element, format_span, format_x_value, format_y_value,
};
use dbflux_components::common::time_range::state::TimeRange;
use dbflux_components::common::time_range::view::{TimeRangeChanged, TimeRangePanel};
use dbflux_components::controls::DropdownSelectionChanged;
use dbflux_components::controls::Input;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::result_panel::ResultPanel;
use dbflux_components::semantic::ChartColors;
use dbflux_components::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_ui_base::toast::{PendingToast, flush_pending_toast, now_hms};
use gpui::prelude::*;
use gpui::*;
use gpui_component::button::{Button, ButtonVariant, ButtonVariants};
use gpui_component::{ActiveTheme, Disableable, Sizable};
use std::sync::Arc;

// Mirrors DataGridPanel::dock_* — flagged for future shared module.

fn dock_section(
    content: impl IntoElement,
    theme: &gpui_component::theme::Theme,
) -> impl IntoElement {
    div()
        .px(px(14.0))
        .py(Spacing::MD)
        .border_b_1()
        .border_color(theme.border)
        .child(content)
}

fn dock_header(label: &str, chart_colors: &ChartColors) -> impl IntoElement {
    div()
        .text_size(px(10.0))
        .text_color(chart_colors.muted_fg)
        .font_weight(FontWeight::BOLD)
        .mb(Spacing::XXS)
        .child(SharedString::from(label.to_uppercase()))
}

fn dock_kv_row(k: &str, v: impl IntoElement, chart_colors: &ChartColors) -> impl IntoElement {
    div()
        .flex()
        .items_start()
        .gap(Spacing::SM)
        .py(px(2.0))
        .child(
            div()
                .w(px(96.0))
                .flex_shrink_0()
                .text_size(px(10.0))
                .text_color(chart_colors.muted_fg)
                .child(SharedString::from(k.to_string())),
        )
        .child(div().flex_1().text_size(px(11.0)).child(v))
}

impl Render for ChartDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // -- Lazily create the TimeRangePanel on first render.
        // Panel creation requires a Window reference (for DatePickerState), so
        // it must be deferred here rather than done in the constructor.
        // Index 3 = Last24Hours — same default as CodeDocument's source panel.
        if self.time_range_panel.is_none() {
            let panel = cx.new(|cx| TimeRangePanel::new("24h", Some(3), window, cx));

            let time_range_sub = cx.subscribe(
                &panel,
                |this: &mut Self, _panel, event: &TimeRangeChanged, cx| {
                    this.on_time_range_changed(event.start_ms, event.end_ms, cx);
                },
            );

            // Subscribe directly to the preset dropdown so that selecting
            // "Custom…" makes the custom picker row visible immediately —
            // before the user clicks Apply. The panel's TimeRangeChanged is
            // only emitted on Apply, not on preset selection.
            let preset_dropdown = panel.read(cx).dropdown_time_range.clone();
            let preset_sub = cx.subscribe(
                &preset_dropdown,
                |this: &mut Self, _, event: &DropdownSelectionChanged, cx| {
                    this.selected_time_range = TimeRangePanel::time_range_for_index(event.index);
                    cx.notify();
                },
            );

            self.time_range_panel = Some(panel.clone());
            self._time_range_sub = Some(time_range_sub);
            self._subscriptions.push(preset_sub);

            // Seed the initial window synchronously rather than relying on
            // emit_initial's event delivery, which GPUI defers until after the
            // current render pass. Sources that require a window (MetricSource,
            // CollectionSource) would otherwise call build_plan(None) → WindowRequired
            // toast on the very first auto-run triggered by pending_run_on_first_render.
            //
            // Index 3 = Last24Hours. Mirror the logic from
            // TimeRangePanel::resolved_window_for_preset: fill end = now when start
            // is Some so the window is always closed (required by MetricQuery).
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let start_ms = now_ms - 24 * 60 * 60_000; // 24h lookback
            self.pending_time_window = Some((start_ms, now_ms));
            // Seed selected_time_range to match the initial panel preset.
            self.selected_time_range = panel.read(cx).selected_time_range;

            // Also trigger emit_initial so that the subscription fires on the
            // next render pass, keeping the dropdown and panel state consistent.
            panel.update(cx, |panel, cx| panel.emit_initial(cx));
        }

        // -- Consume pending data-source swap from MetricPickerApplied event.
        // Must run before the reexecute drain so the new source is in place
        // when the immediate re-execution request is issued.
        if let Some(source) = self.pending_data_source.take() {
            self.set_data_source(source, window, cx);
        }

        // -- Drain pending chart re-execute triggered by time-range changes.
        if std::mem::take(&mut self.pending_chart_reexecute) {
            self.request_reexecute(window, cx);
        }

        // -- Flush pending toasts --
        flush_pending_toast(self.pending_toast.take(), window, cx);

        // -- Apply pending query result --
        if let Some(pending) = self.pending_result.take() {
            self.apply_result(pending, cx);
        }

        // -- Auto-run on first render --
        if self.pending_run_on_first_render {
            self.pending_run_on_first_render = false;
            self.request_reexecute(window, cx);
        }

        // -- Ensure chart view is built for the current result --
        // Must happen before ViewHandle::render is called so ensure_chart_view
        // has a chance to construct the ChartView entity.
        if let Some(result) = self.last_result.clone() {
            self.chart_shell.update(cx, |shell, cx| {
                shell.ensure_chart_view(&result, cx);
            });
        }

        // -- Lazily build ResultPanel on first render --
        // Self-referential construction requires a live entity handle, which
        // is available from within the render closure via cx.entity().
        if self.result_panel.is_none() {
            let entity = cx.entity();
            let view_handle = ChartDocument::into_view_handle(entity, cx);
            let panel = cx.new(|cx| ResultPanel::new(view_handle, cx));
            self.result_panel = Some(panel);
        }

        let focus_handle = self.focus_handle.clone();
        let result_panel = self.result_panel.as_ref().unwrap().clone();

        // -- Name prompt modal overlay --
        let show_name_prompt = self.name_prompt.is_some();
        let name_prompt_element = show_name_prompt.then(|| {
            let theme = cx.theme().clone();
            let input = self.name_prompt.as_ref().unwrap().input.clone();

            div()
                .absolute()
                .inset_0()
                .flex()
                .items_center()
                .justify_center()
                .bg(theme.background.opacity(0.6))
                .child(
                    div()
                        .bg(theme.secondary)
                        .border_1()
                        .border_color(theme.border)
                        .p(Spacing::LG)
                        .w(px(360.0))
                        .flex()
                        .flex_col()
                        .gap(Spacing::MD)
                        .child(Text::label("Save chart"))
                        .child(Input::new(&input).placeholder("Chart name"))
                        .child(
                            div()
                                .flex()
                                .flex_row()
                                .gap(Spacing::SM)
                                .justify_end()
                                .child(Button::new("cancel-save").label("Cancel").small().on_click(
                                    cx.listener(|this, _, _window, cx| {
                                        this.cancel_save(cx);
                                    }),
                                ))
                                .child(
                                    Button::new("confirm-save")
                                        .label("Save")
                                        .small()
                                        .with_variant(ButtonVariant::Primary)
                                        .on_click(cx.listener(|this, _, _window, cx| {
                                            this.confirm_save(cx);
                                        })),
                                ),
                        ),
                )
        });

        // Outer container: tracks focus, hosts ResultPanel and the name-prompt
        // overlay as a sibling (not inside the chrome row).
        div()
            .size_full()
            .relative()
            .track_focus(&focus_handle)
            .child(result_panel)
            .when_some(name_prompt_element, |el, modal| el.child(modal))
    }
}

impl ChartDocument {
    /// Render the chart content area: chart toolbar row + axis bar + chart area.
    ///
    /// Called from the `ViewHandle::render` closure produced by
    /// `into_view_handle`. Pixel-equivalent to the former standalone render body
    /// minus the header row (which is now projected as chrome-row segments).
    ///
    /// When the Metric rail is open (i.e. `ChartRailTab::Metric` is active),
    /// an absolute-positioned 320px panel is overlaid on the right edge showing
    /// the `MetricPickerView` — same layout as the Stats rail in `DataGridPanel`.
    pub(super) fn render_chart_content(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();

        // -- Read chart view entity from shell --
        let chart_view_entity = self.chart_shell.read(cx).chart_view().cloned();
        let chart_detection = self.chart_shell.read(cx).chart_detection.clone();

        // -- Chart area content --
        let chart_area: AnyElement = if let Some(chart_entity) = chart_view_entity {
            div().size_full().child(chart_entity).into_any_element()
        } else {
            // Degraded state: show a placeholder based on detection result.
            // For self-executing sources (MetricSource) the copy is tailored to
            // metric charts; for query/empty sources the generic copy is shown.
            let is_metric = self.data_source.is_self_executing();
            let msg = match &chart_detection {
                Some(ChartDetection::EmptyResult) | None => {
                    if is_metric {
                        if self.exec_state == ExecState::Running {
                            "Loading metric data…"
                        } else {
                            "No data points for the selected window."
                        }
                    } else {
                        "Run the query to populate the chart."
                    }
                }
                Some(ChartDetection::NoTimeColumn) => "No time column detected in result.",
                Some(ChartDetection::NoNumericSeries) => "No numeric series detected in result.",
                Some(ChartDetection::Ok { .. }) => "Chart build failed.",
            };
            div()
                .size_full()
                .flex()
                .items_center()
                .justify_center()
                .child(Text::muted(msg))
                .into_any_element()
        };

        // When embedded inside another document (e.g. a DashboardDocument
        // panel) the host owns the chrome — skip the chart-internal toolbar
        // and axis rows entirely so the chart canvas fills the panel card.
        let embedded = self.embedded;

        // -- Chart toolbar row: RANGE / REFRESH / window / points / Stats / PNG / Save --
        let chart_toolbar_row = {
            let resolved_window = self
                .last_result
                .as_ref()
                .and_then(|r| r.resolved_window.as_ref())
                .map(|rw| (rw.start_ms, rw.end_ms));
            let row_count = self
                .last_result
                .as_ref()
                .map(|r| r.row_count())
                .unwrap_or(0);

            let shell_for_stats = self.chart_shell.clone();
            let shell_for_kind = self.chart_shell.clone();
            let weak_self_for_png = cx.weak_entity();
            let weak_self_for_save = cx.weak_entity();
            let weak_self_for_refresh = cx.weak_entity();

            let dropdown_time_range = self
                .time_range_panel
                .as_ref()
                .map(|p| p.read(cx).dropdown_time_range.clone());

            let ctx = ChartToolbarContext {
                theme: &theme,
                chart_shell: self.chart_shell.clone(),
                refresh_policy: self.refresh_policy,
                refresh_dropdown: self.refresh_dropdown.clone(),
                dropdown_time_range,
                row_count,
                resolved_window,
                source_supports_save: true,
            };

            let handlers = ChartToolbarHandlers {
                on_refresh: Arc::new(move |window, cx| {
                    if let Some(doc) = weak_self_for_refresh.upgrade() {
                        doc.update(cx, |this, cx| this.request_reexecute(window, cx));
                    }
                }),
                on_toggle_stats_rail: Arc::new(move |_window, cx| {
                    shell_for_stats.update(cx, |s, cx| {
                        (s.chart_rail_open, s.chart_rail_tab) =
                            toggle_stats_rail(s.chart_rail_open, s.chart_rail_tab);
                        cx.notify();
                    });
                }),
                on_png_export: Arc::new(move |_window, cx| {
                    if let Some(doc) = weak_self_for_png.upgrade() {
                        doc.update(cx, |this, _cx| {
                            this.pending_toast = Some(PendingToast {
                                message: format!("PNG export coming in v0.7 — {}", now_hms()),
                                is_error: false,
                            });
                        });
                    }
                }),
                on_save_chart: Arc::new(move |window, cx| {
                    if let Some(doc) = weak_self_for_save.upgrade() {
                        doc.update(cx, |this, cx| {
                            this.open_name_prompt(window, cx);
                        });
                    }
                }),
                on_select_chart_kind: Arc::new(move |kind, _window, cx| {
                    shell_for_kind.update(cx, |s, cx| s.set_chart_kind(kind, cx));
                }),
            };

            render_chart_toolbar(ctx, handlers, cx)
        };

        // -- AxisBar row: shown when result is available --
        let (bindings, open_pill, columns) = {
            let shell = self.chart_shell.read(cx);
            (
                shell.active_bindings(),
                shell.axis_open_pill,
                self.last_result
                    .as_ref()
                    .map(|r| r.columns.clone())
                    .unwrap_or_default(),
            )
        };

        let chart_shell_for_pill = self.chart_shell.clone();
        let chart_shell_for_x = self.chart_shell.clone();
        let chart_shell_for_y = self.chart_shell.clone();
        let chart_shell_for_group = self.chart_shell.clone();
        let chart_shell_for_agg = self.chart_shell.clone();

        let chart_colors = ChartColors::for_current(cx);

        let axis_bar = axis_bar_element(
            &bindings,
            &columns,
            open_pill,
            &chart_colors,
            move |pill, _window, cx| {
                chart_shell_for_pill.update(cx, |s, cx| s.toggle_axis_pill(pill, cx));
            },
            move |col_idx, _window, cx| {
                chart_shell_for_x.update(cx, |s, cx| {
                    let mut b = s.active_bindings();
                    b.x = col_idx;
                    s.apply_bindings(b, cx);
                });
            },
            move |col_idx, checked, _window, cx| {
                chart_shell_for_y.update(cx, |s, cx| {
                    let mut b = s.active_bindings();
                    if checked {
                        if !b.y.contains(&col_idx) {
                            b.y.push(col_idx);
                        }
                    } else {
                        b.y.retain(|&i| i != col_idx);
                    }
                    s.apply_bindings(b, cx);
                });
            },
            move |group_col, _window, cx| {
                chart_shell_for_group.update(cx, |s, cx| {
                    let mut b = s.active_bindings();
                    b.group_by = group_col;
                    s.apply_bindings(b, cx);
                });
            },
            move |agg, _window, cx| {
                chart_shell_for_agg.update(cx, |s, cx| {
                    let mut b = s.active_bindings();
                    b.aggregation = agg;
                    s.apply_bindings(b, cx);
                });
            },
        );

        let axis_row = div()
            .flex()
            .flex_row()
            .items_center()
            .h(Heights::ROW)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.secondary)
            .child(axis_bar);

        // -- Custom date/time picker row --
        // Rendered below the chart toolbar when the user has selected "Custom…"
        // in the range preset dropdown. Mirrors the audit document's custom
        // picker row exactly: same sub-entities from the panel, same spacing,
        // same Apply button with enabled/disabled logic.
        let custom_picker_row: Option<AnyElement> =
            if self.selected_time_range == Some(TimeRange::Custom) {
                if let Some(panel_entity) = &self.time_range_panel {
                    // Read can_apply (and weak_self for the Apply click closure)
                    // before the helper call so there are no concurrent borrows.
                    let can_apply = panel_entity.read(cx).can_apply_custom_range(cx);
                    let weak_self = cx.weak_entity();

                    let apply_btn = div()
                        .id("chart-custom-time-apply")
                        .h(Heights::BUTTON)
                        .flex()
                        .items_center()
                        .px(Spacing::SM)
                        .rounded(Radii::SM)
                        .border_1()
                        .border_color(theme.input)
                        .when(can_apply, |d| {
                            let ws = weak_self.clone();
                            d.cursor_pointer()
                                .hover(|d| d.bg(theme.secondary))
                                .on_click(move |_, _, cx| {
                                    if let Some(doc) = ws.upgrade() {
                                        doc.update(cx, |this, cx| {
                                            this.apply_custom_range(cx);
                                        });
                                    }
                                })
                        })
                        .when(!can_apply, |d| d.opacity(0.45))
                        .child(Text::caption("Apply"));

                    // Outer band: full-width chrome (border, bg, padding) plus
                    // flex_wrap so the picker row + Apply can wrap on narrow
                    // viewports. The picker row itself is one opaque unit.
                    let row = div()
                        .flex()
                        .flex_row()
                        .items_center()
                        .flex_wrap()
                        .gap_1()
                        .py(Spacing::XS)
                        .px(Spacing::SM)
                        .border_b_1()
                        .border_color(theme.border)
                        .bg(theme.tab_bar)
                        .child(
                            panel_entity
                                .read(cx)
                                .render_custom_picker_row(px(260.0), cx),
                        )
                        .child(apply_btn);

                    Some(row.into_any_element())
                } else {
                    None
                }
            } else {
                None
            };

        // -- Metric picker rail (absolute overlay, right edge) --
        // Rendered when the Metric tab is active and the shell has picker state.
        // Uses the same absolute-right-panel layout as the Stats rail in DataGridPanel.
        let metric_rail: Option<AnyElement> = {
            let (rail_open, rail_tab) = {
                let shell = self.chart_shell.read(cx);
                (shell.chart_rail_open, shell.chart_rail_tab)
            };

            if rail_open && rail_tab == ChartRailTab::Metric {
                // Single read+update path: render the picker inside one
                // `update` closure so a concurrent clear of `metric_picker`
                // (subscription, pending action) cannot turn the previously
                // observed `Some` into a `None` between the read and the update.
                let cache = self.app_state.read(cx).metric_catalog_cache().clone();
                let rail_element: Option<AnyElement> = self.chart_shell.update(cx, |shell, cx| {
                    shell.metric_picker.as_mut().map(|picker| {
                        MetricPickerView {
                            state: picker,
                            cache: &cache,
                        }
                        .render(window, cx)
                        .into_any_element()
                    })
                });

                rail_element.map(|element| {
                    div()
                        .absolute()
                        .top_0()
                        .right_0()
                        .bottom_0()
                        .w(px(320.0))
                        .flex()
                        .flex_col()
                        .border_l_1()
                        .border_color(theme.border)
                        .bg(theme.popover)
                        .occlude()
                        .child(div().flex_grow().min_h_0().overflow_hidden().child(element))
                        .into_any_element()
                })
            } else {
                None
            }
        };

        // -- Stats rail (absolute overlay, right edge) --
        // Rendered when the Stats tab is active and the shell's rail is open.
        let stats_rail: Option<AnyElement> = {
            let (rail_open, rail_tab) = {
                let shell = self.chart_shell.read(cx);
                (shell.chart_rail_open, shell.chart_rail_tab)
            };
            if should_render_stats_rail(rail_open, rail_tab) {
                self.render_stats_rail(&theme, cx)
            } else {
                None
            }
        };

        div()
            .flex()
            .flex_col()
            .size_full()
            .relative() // needed so the absolute rail positions relative to this container
            .when(!embedded, |el| el.child(chart_toolbar_row))
            .when_some(
                if embedded { None } else { custom_picker_row },
                |el, row| el.child(row),
            )
            .when(!embedded, |el| el.child(axis_row))
            .child(div().flex_1().min_h_0().child(chart_area))
            .when_some(metric_rail, |el, rail| el.child(rail))
            .when_some(stats_rail, |el, rail| el.child(rail))
            .into_any_element()
    }

    /// Render the 320 px Stats rail for the right-edge overlay.
    ///
    /// Returns `None` when the chart view is still being built or when no stats
    /// are available for the focused series. Mirrors the layout produced by
    /// `DataGridPanel::render_rail_stats_tab` with snapshot-style borrows to
    /// satisfy GPUI's single-context borrow rules.
    fn render_stats_rail(
        &self,
        theme: &gpui_component::theme::Theme,
        cx: &mut App,
    ) -> Option<AnyElement> {
        let chart_colors = ChartColors::for_current(cx);

        // Scope 1: read chart_shell to capture chart_view and focused_idx.
        let (chart_view_opt, focused_idx) = {
            let shell = self.chart_shell.read(cx);
            let cv = shell.chart_view().cloned();
            let fi = cv
                .as_ref()
                .map(|cv| cv.read(cx).focused_series_idx())
                .unwrap_or(shell.chart_focused_series_idx);
            (cv, fi)
        };

        let placeholder = |msg: &'static str| -> AnyElement {
            div()
                .p_2()
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child(msg)
                .into_any_element()
        };

        let Some(chart_view) = chart_view_opt else {
            return Some(self.wrap_stats_rail_chrome(
                placeholder("Rebuilding chart…"),
                theme,
                &chart_colors,
            ));
        };

        // Scope 2: read chart_view entity to capture all primitive values before
        // building the element tree. Borrows must be dropped before constructing
        // elements — mirrors DataGridPanel::render_rail_stats_tab exactly.
        let (stats_opt, label, color, x_min, x_max, x_is_time) = {
            let view = chart_view.read(cx);
            let stats = view.series_stats().get(focused_idx).copied().flatten();
            let label = view.series_label(focused_idx).to_string();
            let color = view.series_color(focused_idx, cx);
            let (x_min, x_max) = view.data_x_bounds();
            let x_is_time = view.x_is_time();
            (stats, label, color, x_min, x_max, x_is_time)
        };

        let Some(stats) = stats_opt else {
            return Some(self.wrap_stats_rail_chrome(
                placeholder("No stats available for this series."),
                theme,
                &chart_colors,
            ));
        };

        let start_label = format_x_value(x_min, x_is_time);
        let end_label = format_x_value(x_max, x_is_time);
        let span_label = format_span(x_max - x_min);
        let points_count = self
            .last_result
            .as_ref()
            .map(|r| r.row_count())
            .unwrap_or(0);

        let cyan_color = theme.cyan;
        let primary_color = theme.primary;

        let cyan_val = |v: f64| -> AnyElement {
            div()
                .text_size(px(11.0))
                .text_color(cyan_color)
                .child(SharedString::from(format_y_value(v)))
                .into_any_element()
        };
        let primary_val = |v: f64| -> AnyElement {
            div()
                .text_size(px(11.0))
                .text_color(primary_color)
                .child(SharedString::from(format_y_value(v)))
                .into_any_element()
        };
        let fg_val = |v: f64| -> AnyElement {
            div()
                .text_size(px(11.0))
                .text_color(theme.foreground)
                .child(SharedString::from(format_y_value(v)))
                .into_any_element()
        };
        let str_val = |s: String| -> AnyElement {
            div()
                .text_size(px(11.0))
                .text_color(theme.foreground)
                .child(SharedString::from(s))
                .into_any_element()
        };
        let unavail_val = || -> AnyElement {
            div()
                .text_size(px(11.0))
                .text_color(theme.muted_foreground)
                .italic()
                .child("unavailable")
                .into_any_element()
        };

        let body = div()
            .id("chart-doc-rail-stats-scroll")
            .size_full()
            .flex()
            .flex_col()
            .overflow_y_scroll()
            // SERIES header
            .child(dock_section(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(div().w(px(10.0)).h(px(10.0)).rounded_sm().bg(color))
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .font_weight(FontWeight::BOLD)
                            .text_color(theme.foreground)
                            .child(SharedString::from(label)),
                    ),
                theme,
            ))
            // STATS section
            .child(dock_section(
                div()
                    .flex()
                    .flex_col()
                    .gap(px(2.0))
                    .child(dock_header("Stats", &chart_colors))
                    .child(dock_kv_row("min", cyan_val(stats.min), &chart_colors))
                    .child(dock_kv_row("max", cyan_val(stats.max), &chart_colors))
                    .child(dock_kv_row("avg", cyan_val(stats.avg), &chart_colors))
                    .child(dock_kv_row("p50", fg_val(stats.p50), &chart_colors))
                    .child(dock_kv_row("p95", fg_val(stats.p95), &chart_colors))
                    .child(dock_kv_row("p99", primary_val(stats.p99), &chart_colors))
                    .child(dock_kv_row("last", fg_val(stats.last), &chart_colors)),
                theme,
            ))
            // WINDOW section
            .child(dock_section(
                div()
                    .flex()
                    .flex_col()
                    .gap(px(2.0))
                    .child(dock_header("Window", &chart_colors))
                    .child(dock_kv_row("start", str_val(start_label), &chart_colors))
                    .child(dock_kv_row("end", str_val(end_label), &chart_colors))
                    .child(dock_kv_row("span", str_val(span_label), &chart_colors))
                    .child(dock_kv_row(
                        "points",
                        str_val(format!("{}", points_count)),
                        &chart_colors,
                    )),
                theme,
            ))
            // SOURCE section — placeholder until drivers populate QueryResult.metadata
            .child(dock_section(
                div()
                    .flex()
                    .flex_col()
                    .gap(px(2.0))
                    .child(dock_header("Source", &chart_colors))
                    .child(dock_kv_row("measurement", unavail_val(), &chart_colors))
                    .child(dock_kv_row("field", unavail_val(), &chart_colors))
                    .child(dock_kv_row("host", unavail_val(), &chart_colors))
                    .child(dock_kv_row("region", unavail_val(), &chart_colors)),
                theme,
            ))
            .into_any_element();

        Some(self.wrap_stats_rail_chrome(body, theme, &chart_colors))
    }

    /// Wraps a stats rail body in the absolute-right 320 px chrome, prefixed by
    /// a header bar containing the "STATS" title and a close button that
    /// dismisses the rail by setting `chart_rail_open = false` on the shell.
    fn wrap_stats_rail_chrome(
        &self,
        body: AnyElement,
        theme: &gpui_component::theme::Theme,
        chart_colors: &ChartColors,
    ) -> AnyElement {
        let shell_for_close = self.chart_shell.clone();
        let muted_fg = chart_colors.muted_fg;

        let header = div()
            .flex()
            .flex_row()
            .items_center()
            .justify_between()
            .px(px(14.0))
            .py(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .text_size(px(10.0))
                    .text_color(muted_fg)
                    .font_weight(FontWeight::BOLD)
                    .child("STATS"),
            )
            .child(
                div()
                    .id("chart-doc-stats-rail-close")
                    .w(px(20.0))
                    .h(px(20.0))
                    .flex()
                    .items_center()
                    .justify_center()
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|h| h.bg(theme.muted))
                    .on_mouse_down(MouseButton::Left, move |_, _window, cx| {
                        shell_for_close.update(cx, |s, cx| {
                            s.chart_rail_open = false;
                            cx.notify();
                        });
                    })
                    .child(Icon::new(AppIcon::X).size(px(11.0)).color(muted_fg)),
            );

        div()
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .w(px(320.0))
            .flex()
            .flex_col()
            .border_l_1()
            .border_color(theme.border)
            .bg(theme.popover)
            .occlude()
            .child(header)
            .child(div().flex_grow().min_h_0().overflow_hidden().child(body))
            .into_any_element()
    }
}
