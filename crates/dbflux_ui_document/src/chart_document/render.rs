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

use super::ChartDocument;
use crate::chart::ChartRailTab;
use crate::chart::toolbar::{ChartToolbarContext, ChartToolbarHandlers, render_chart_toolbar};
use dbflux_components::chart::{ChartDetection, axis_bar_element};
use dbflux_components::common::time_range::state::TimeRange;
use dbflux_components::common::time_range::view::{TimeRangeChanged, TimeRangePanel};
use dbflux_components::controls::DropdownSelectionChanged;
use dbflux_components::controls::Input;
use dbflux_components::primitives::Text;
use dbflux_components::result_panel::ResultPanel;
use dbflux_components::semantic::ChartColors;
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_ui_base::toast::{PendingToast, flush_pending_toast, now_hms};
use gpui::prelude::*;
use gpui::*;
use gpui_component::button::{Button, ButtonVariant, ButtonVariants};
use gpui_component::{ActiveTheme, Disableable, Sizable};
use std::sync::Arc;

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
    pub(super) fn render_chart_content(
        &mut self,
        _window: &mut Window,
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

        div()
            .flex()
            .flex_col()
            .size_full()
            .child(chart_toolbar_row)
            .when_some(custom_picker_row, |el, row| el.child(row))
            .child(axis_row)
            .child(div().flex_1().min_h_0().child(chart_area))
            .into_any_element()
    }
}
