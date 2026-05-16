//! Render implementation for `ChartDocument`.
//!
//! Layout:
//!   ┌──────────────────────────────────────────────┐
//!   │ header: title · Save button                  │
//!   ├──────────────────────────────────────────────┤
//!   │ collapsible editor drawer                    │
//!   │   [chevron] Query  [Run]                     │
//!   │   ┌───────────────────────────────────────┐  │
//!   │   │ SQL editor (when expanded)            │  │
//!   │   └───────────────────────────────────────┘  │
//!   ├──────────────────────────────────────────────┤
//!   │ chart area (fills remaining space)           │
//!   └──────────────────────────────────────────────┘

use super::ChartDocument;
use crate::ui::components::toast::{PendingToast, flush_pending_toast, now_hms};
use crate::ui::document::chart::ChartRailTab;
use crate::ui::document::chart::toolbar::{
    ChartToolbarContext, ChartToolbarHandlers, render_chart_toolbar,
};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{Heights, Spacing};
use dbflux_components::chart::{ChartDetection, axis_bar_element};
use dbflux_components::controls::{GpuiInput, Input};
use dbflux_components::primitives::{Icon, Text};
use gpui::prelude::*;
use gpui::*;
use gpui_component::button::{Button, ButtonVariant, ButtonVariants};
use gpui_component::{ActiveTheme, Disableable, Sizable};
use std::sync::Arc;

impl Render for ChartDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
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

        let theme = cx.theme().clone();

        let is_executing = self.exec_state == super::ExecState::Running;
        let drawer_open = self.editor_drawer_open;
        let editor_input = self.editor_input.clone();
        let focus_handle = self.focus_handle.clone();
        let title = self.title.clone();
        let show_name_prompt = self.name_prompt.is_some();

        // -- Ensure chart view is built for the current result --
        // This must happen before reading shell state so ensure_chart_view
        // has a chance to construct the ChartView entity.
        if let Some(result) = self.last_result.clone() {
            self.chart_shell.update(cx, |shell, cx| {
                shell.ensure_chart_view(&result, cx);
            });
        }

        // -- Read chart view entity from shell --
        let chart_view_entity = self.chart_shell.read(cx).chart_view().cloned();

        let chart_detection = self.chart_shell.read(cx).chart_detection.clone();

        // -- Chart area content --
        let chart_area: AnyElement = if let Some(chart_entity) = chart_view_entity {
            div().size_full().child(chart_entity).into_any_element()
        } else {
            // Degraded state: show a placeholder based on detection result.
            let msg = match &chart_detection {
                Some(ChartDetection::EmptyResult) | None => "Run the query to populate the chart.",
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

        // -- Name prompt modal --
        let name_prompt_element = show_name_prompt.then(|| {
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

        let chevron_icon = if drawer_open {
            AppIcon::ChevronDown
        } else {
            AppIcon::ChevronRight
        };

        let header =
            div()
                .flex()
                .flex_col()
                .border_b_1()
                .border_color(theme.border)
                .child(
                    div()
                        .flex()
                        .flex_row()
                        .items_center()
                        .h(Heights::TOOLBAR)
                        .px(Spacing::MD)
                        .gap(Spacing::SM)
                        .child(
                            div()
                                .id("chart-doc-drawer-toggle")
                                .flex()
                                .flex_row()
                                .items_center()
                                .gap(Spacing::XS)
                                .cursor_pointer()
                                .on_click(cx.listener(|this, _, _window, cx| {
                                    this.toggle_editor_drawer(cx);
                                }))
                                .child(Icon::new(chevron_icon).small())
                                .child(Text::label(title)),
                        )
                        .child(
                            Button::new("run-query")
                                .label(if is_executing { "Running…" } else { "Run" })
                                .small()
                                .with_variant(ButtonVariant::Primary)
                                .disabled(is_executing)
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.request_reexecute(window, cx);
                                })),
                        )
                        .child(div().flex_grow())
                        .child(Button::new("save-chart").label("Save").small().on_click(
                            cx.listener(|this, _, window, cx| {
                                this.open_name_prompt(window, cx);
                            }),
                        )),
                )
                .when(drawer_open, |el| {
                    el.child(
                        div()
                            .h(px(180.0))
                            .border_t_1()
                            .border_color(theme.border)
                            .bg(theme.background)
                            .flex()
                            .flex_col()
                            .min_h_0()
                            .child(
                                div().flex_1().min_h_0().overflow_hidden().child(
                                    GpuiInput::new(&editor_input)
                                        .appearance(false)
                                        .w_full()
                                        .h_full(),
                                ),
                            ),
                    )
                });

        // -- Chart toolbar row: RANGE / REFRESH / window / points / Stats / PNG / Save --
        //
        // `time_range_panel` is `None` for ChartDocument until a future change
        // wires a TimeRangePanel to standalone documents. When `None`, the shared
        // toolbar hides the RANGE chip section automatically.
        //
        // TODO(chart-everywhere): wire `time_range_panel` for ChartDocument so
        // standalone charts gain RANGE chip support. Requires a TimeRangePanel
        // entity in ChartDocument::new and a subscription that re-executes the
        // query on range change (same pattern as DataGridPanel).
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
            let weak_self_for_png = cx.weak_entity();
            let weak_self_for_save = cx.weak_entity();

            let ctx = ChartToolbarContext {
                theme: &theme,
                chart_shell: self.chart_shell.clone(),
                refresh_dropdown: self.refresh_dropdown.clone(),
                time_range_panel: self.time_range_panel.clone(),
                row_count,
                resolved_window,
                source_supports_save: true,
            };

            let handlers = ChartToolbarHandlers {
                on_select_range_preset: Arc::new(|_i, _window, _cx| {
                    // No TimeRangePanel wired yet — this handler is never called
                    // because the RANGE section is hidden when time_range_panel is None.
                }),
                on_toggle_stats_rail: Arc::new(move |_window, cx| {
                    shell_for_stats.update(cx, |s, cx| {
                        if s.chart_rail_open && s.chart_rail_tab == ChartRailTab::Stats {
                            s.chart_rail_open = false;
                        } else {
                            s.chart_rail_open = true;
                            s.chart_rail_tab = ChartRailTab::Stats;
                        }
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

        let axis_bar = axis_bar_element(
            &bindings,
            &columns,
            open_pill,
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
            .h(px(28.0))
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.secondary)
            .child(axis_bar);

        div()
            .flex()
            .flex_col()
            .size_full()
            .track_focus(&focus_handle)
            .child(header)
            .child(chart_toolbar_row)
            .child(axis_row)
            .child(div().flex_grow().min_h_0().child(chart_area))
            .when_some(name_prompt_element, |el, modal| el.child(modal))
    }
}
