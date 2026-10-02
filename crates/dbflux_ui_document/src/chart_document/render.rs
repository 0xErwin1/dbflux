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

use super::{
    ChartDocument, ExecState, seed_initial_window, should_render_stats_rail, toggle_stats_rail,
};
use crate::chart::ChartRailTab;
use crate::chart::metric_picker_render::MetricPickerView;
use crate::chart::toolbar::chart_window_label;
use crate::chart::toolbar::{ChartToolbarContext, ChartToolbarHandlers, render_chart_toolbar};
use crate::chrome::{document_bar, document_title};
use crate::pane::DocumentSidePanel;
use dbflux_components::chart::{
    ChartDetection, ChartView, MetricSource, axis_bar_element, format_span, format_x_value,
    format_y_value, legend_element,
};
use dbflux_components::common::time_range::state::TimeRange;
use dbflux_components::common::time_range::view::{TimeRangeChanged, TimeRangePanel};
use dbflux_components::composites::{Island, docked_island_frame};
use dbflux_components::controls::DropdownSelectionChanged;
use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::Modal;
use dbflux_components::primitives::{Badge, BadgeTone, Icon, Text};
use dbflux_components::result_panel::ResultPanel;
use dbflux_components::semantic::ChartColors;
use dbflux_components::tokens::{
    ChartDocumentMetrics, ChromeColors, DocumentMetrics, Fields, IslandMetrics, Spacing,
};
use dbflux_components::typography::AppFonts;
use dbflux_core::LogErr;
use dbflux_ui_base::toast::flush_pending_toast;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use std::sync::Arc;

/// Width of the save-chart name prompt.
const CHART_SAVE_PROMPT_WIDTH: Pixels = px(360.0);
/// Width of the date range picker in the custom range row.
const CHART_DATE_PICKER_WIDTH: Pixels = px(260.0);

impl Render for ChartDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // -- Lazily create the TimeRangePanel on first render.
        // Panel creation requires a Window reference (for DatePickerState), so
        // it must be deferred here rather than done in the constructor.
        //
        // The initial preset index comes from `self.initial_time_range_index`:
        //   Index 0 = Last15min   (InstanceMetric sources)
        //   Index 3 = Last24Hours (all other sources — default)
        if self.time_range_panel.is_none() {
            let preset_index = self.initial_time_range_index;
            let label = TimeRangePanel::label_for_index(preset_index)
                .unwrap_or_else(|| TimeRangePanel::preset_label(TimeRange::Last24Hours));
            let panel = cx.new(|cx| TimeRangePanel::new(label, Some(preset_index), window, cx));

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
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let lookback_ms =
                TimeRangePanel::lookback_ms_for_index(preset_index).unwrap_or(24 * 60 * 60_000);

            // A dashboard may have staged a window before this first render;
            // the staged bounds and their provenance win over the child's own
            // default preset so the seed cannot overwrite the externally
            // applied window.
            let staged = self
                .external_window_staged
                .then(|| {
                    let (start_ms, end_ms) = self.pending_time_window?;
                    let display_window = self.applied_display_window?;
                    Some((start_ms, end_ms, display_window))
                })
                .flatten();
            let (seed_start, seed_end, display_window) =
                seed_initial_window(staged, lookback_ms, now_ms);
            self.pending_time_window = Some((seed_start, seed_end));
            self.applied_display_window = Some(display_window);
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
        let name_prompt_element = self.name_prompt.as_ref().map(|prompt| {
            let input = prompt.input.clone();

            // Enter in the name field saves and Escape cancels, like the
            // footer buttons.
            Modal::new(dbflux_i18n::t!("document.chart.toolbar.save_chart"))
                .id("chart-save-prompt")
                .icon(AppIcon::Save)
                .width(CHART_SAVE_PROMPT_WIDTH)
                .focus_handle(prompt.focus.handle())
                .on_close({
                    let weak_self = cx.weak_entity();
                    move |_window, cx| {
                        weak_self
                            .update(cx, |this, cx| this.cancel_save(cx))
                            .log_err();
                    }
                })
                .on_confirm({
                    let weak_self = cx.weak_entity();
                    move |_window, cx| {
                        weak_self
                            .update(cx, |this, cx| this.confirm_save(cx))
                            .log_err();
                    }
                })
                .body(
                    Input::new(&input)
                        .placeholder(dbflux_i18n::t!("document.chart.shell.name_placeholder")),
                )
                .footer(
                    div()
                        .flex()
                        .gap(DocumentMetrics::GAP)
                        .child(
                            Button::new(
                                "cancel-save",
                                dbflux_i18n::t!("document.chart.shell.cancel"),
                            )
                            .on_click(cx.listener(
                                |this, _, _window, cx| {
                                    this.cancel_save(cx);
                                },
                            )),
                        )
                        .child(
                            Button::new(
                                "confirm-save",
                                dbflux_i18n::t!("document.chart.shell.save"),
                            )
                            .primary()
                            .icon(AppIcon::Save)
                            .on_click(cx.listener(
                                |this, _, _window, cx| {
                                    this.confirm_save(cx);
                                },
                            )),
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
            let metric_unconfigured = self
                .data_source
                .as_any()
                .and_then(|any| any.downcast_ref::<MetricSource>())
                .is_some_and(|source| source.series.is_empty());
            let msg = if metric_unconfigured {
                "This chart has no metric series configured. Edit it to add one.".into()
            } else {
                match &chart_detection {
                    Some(ChartDetection::EmptyResult) | None => {
                        if is_metric {
                            if self.exec_state == ExecState::Running {
                                dbflux_i18n::t!("document.chart.shell.degraded.loading_metric")
                            } else {
                                dbflux_i18n::t!("document.chart.shell.degraded.no_data_points")
                            }
                        } else {
                            dbflux_i18n::t!("document.chart.shell.degraded.run_query")
                        }
                    }
                    Some(ChartDetection::NoTimeColumn) => {
                        dbflux_i18n::t!("document.chart.shell.degraded.no_time_column")
                    }
                    Some(ChartDetection::NoNumericSeries) => {
                        dbflux_i18n::t!("document.chart.shell.degraded.no_numeric_series")
                    }
                    Some(ChartDetection::Ok { .. }) => {
                        dbflux_i18n::t!("document.chart.shell.degraded.build_failed")
                    }
                }
            };
            div()
                .size_full()
                .flex()
                .items_center()
                .justify_center()
                .child(Text::caption(msg))
                .into_any_element()
        };

        // When embedded inside another document (e.g. a DashboardDocument
        // panel) the host owns the chrome — skip the chart-internal toolbar
        // and axis rows entirely so the chart canvas fills the panel card.
        let embedded = self.embedded;

        // -- Chart toolbar row: RANGE / REFRESH / window / points / Stats / Save --
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
            let weak_self_for_save = cx.weak_entity();
            let weak_self_for_refresh = cx.weak_entity();

            let tint = ChromeColors::tint(&theme);
            let leading = div()
                .flex()
                .min_w_0()
                .items_center()
                .gap(DocumentMetrics::GAP)
                .child(document_title(
                    AppIcon::ChartSpline,
                    tint,
                    self.title.clone(),
                    cx,
                ))
                .when(self.saved_chart_id.is_some(), |title| {
                    title.child(Badge::new(
                        dbflux_i18n::t!("document.chart.shell.saved_badge"),
                        BadgeTone::Success,
                    ))
                })
                .into_any_element();

            let ctx = ChartToolbarContext {
                theme: &theme,
                chart_shell: self.chart_shell.clone(),
                refresh_policy: self.refresh_policy,
                refresh_dropdown: self.refresh_dropdown.clone(),
                time_range_panel: self.time_range_panel.clone(),
                row_count,
                resolved_window,
                source_supports_save: true,
                refresh_variant: dbflux_components::controls::ButtonVariant::Secondary,
                leading: Some(leading),
                show_window: false,
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
        let doc_for_x = cx.entity();
        let doc_for_y = cx.entity();
        let doc_for_group = cx.entity();
        let doc_for_agg = cx.entity();

        let chart_colors = ChartColors::for_current(cx);

        let picker_cursor = self.chart_shell.read(cx).axis_picker_cursor();

        let axis_bar = axis_bar_element(
            &bindings,
            &columns,
            open_pill,
            picker_cursor,
            &chart_colors,
            move |pill, _window, cx| {
                chart_shell_for_pill.update(cx, |s, cx| s.toggle_axis_pill(pill, cx));
            },
            move |col_idx, _window, cx| {
                doc_for_x.update(cx, |this, cx| {
                    this.chart_shell.update(cx, |s, cx| {
                        let mut b = s.active_bindings();
                        b.x = col_idx;
                        s.apply_bindings(b, cx);
                    });
                    this.rebuild_chart_view(cx);
                });
            },
            move |col_idx, checked, _window, cx| {
                doc_for_y.update(cx, |this, cx| {
                    this.chart_shell.update(cx, |s, cx| {
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
                    this.rebuild_chart_view(cx);
                });
            },
            move |group_col, _window, cx| {
                doc_for_group.update(cx, |this, cx| {
                    this.chart_shell.update(cx, |s, cx| {
                        let mut b = s.active_bindings();
                        b.group_by = group_col;
                        s.apply_bindings(b, cx);
                    });
                    this.rebuild_chart_view(cx);
                });
            },
            move |agg, _window, cx| {
                doc_for_agg.update(cx, |this, cx| {
                    this.chart_shell.update(cx, |s, cx| {
                        let mut b = s.active_bindings();
                        b.aggregation = agg;
                        s.apply_bindings(b, cx);
                    });
                    this.rebuild_chart_view(cx);
                });
            },
        );

        let window_label = {
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

            chart_window_label(&self.chart_shell, resolved_window, row_count, cx)
        };

        // Session-only notice for accumulating sources: the visible series is
        // limited to samples collected while this chart is open, whatever the
        // selected time range claims. Without it a "Last 7 days" preset would
        // imply history the session does not have.
        let session_note = self.source_is_accumulating().then(|| {
            div()
                .flex_shrink_0()
                .font_family(AppFonts::MONO)
                .text_size(DocumentMetrics::TABLE_META_FONT)
                .text_color(theme.muted_foreground)
                .child(dbflux_i18n::t!("document.chart.session_samples_only"))
        });

        let axis_row = document_bar(ChartDocumentMetrics::AXIS_ROW_HEIGHT, cx)
            .child(axis_bar)
            .child(div().flex_1())
            .child(
                div()
                    .flex_shrink_0()
                    .font_family(AppFonts::MONO)
                    .text_size(DocumentMetrics::TABLE_META_FONT)
                    .text_color(theme.muted_foreground)
                    .child(window_label),
            )
            .children(session_note);

        // -- Custom date/time picker row --
        // Rendered below the chart toolbar when the user has selected "Custom…"
        // in the range preset dropdown. Mirrors the audit document's custom
        // picker row exactly: same sub-entities from the panel, same spacing,
        // same Apply button with enabled/disabled logic.
        let selected_time_range = self
            .time_range_panel
            .as_ref()
            .and_then(|panel| panel.read(cx).selected_time_range);
        self.selected_time_range = selected_time_range;

        let custom_picker_row: Option<AnyElement> =
            if selected_time_range == Some(TimeRange::Custom) {
                self.time_range_panel.as_ref().map(|panel_entity| {
                    let can_apply = panel_entity.read(cx).can_apply_custom_range(cx);
                    let weak_self = cx.weak_entity();

                    document_bar(DocumentMetrics::TOOLBAR_HEIGHT, cx)
                        .child(
                            panel_entity
                                .read(cx)
                                .render_custom_picker_row(CHART_DATE_PICKER_WIDTH, cx),
                        )
                        .child(
                            Button::new(
                                "chart-custom-time-apply",
                                dbflux_i18n::t!("document.chart.shell.custom_range.apply"),
                            )
                            .icon(AppIcon::Check)
                            .disabled(!can_apply)
                            .tab_stop(false)
                            .on_click(move |_, _, cx| {
                                if let Some(doc) = weak_self.upgrade() {
                                    doc.update(cx, |this, cx| {
                                        this.apply_custom_range(cx);
                                    });
                                }
                            }),
                        )
                        .into_any_element()
                })
            } else {
                None
            };

        // -- Side rails (metric picker, stats) --
        // A chart tab hands its rails to the workspace, which draws them as
        // islands beside the document island (`side_panels`). Embedded in a
        // dashboard panel there is no workspace slot, so they stay docked
        // inside the chart.
        let docked_rails: Vec<AnyElement> = if embedded {
            self.rail_panels(window, cx)
                .into_iter()
                .map(|(_, width, content)| docked_rail(&theme, width, content))
                .collect()
        } else {
            Vec::new()
        };

        // When embedded inside a dashboard panel, surface the legend as a
        // sibling strip below the chart canvas. The standalone ChartDocument
        // exposes its legend through `DataGridPanel::render_chart_legend_row`,
        // but dashboard panels skip the data-grid chrome entirely, so the
        // legend would otherwise be invisible — series identity is then only
        // surfaced in the hover readout, which doesn't match what users see
        // in CloudWatch / Grafana.
        let embedded_legend: Option<AnyElement> = if embedded {
            self.build_embedded_legend(cx)
        } else {
            None
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
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .when(!embedded, |area| {
                        area.pt(ChartDocumentMetrics::AREA_PADDING_TOP)
                            .px(ChartDocumentMetrics::AREA_PADDING)
                            .pb(ChartDocumentMetrics::AREA_PADDING)
                    })
                    .child(chart_area),
            )
            .when_some(embedded_legend, |el, legend| el.child(legend))
            .children(docked_rails)
            .into_any_element()
    }

    /// The open rail, if any, as `(id, width, content)`: the metric picker
    /// while the Metric tab is open, the stats rail while the Stats tab is.
    fn rail_panels(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Vec<(&'static str, Pixels, AnyElement)> {
        let (rail_open, rail_tab) = {
            let shell = self.chart_shell.read(cx);
            (shell.chart_rail_open, shell.chart_rail_tab)
        };

        if rail_open && rail_tab == ChartRailTab::Metric {
            // Single read+update path: render the picker inside one `update`
            // closure so a concurrent clear of `metric_picker` (subscription,
            // pending action) cannot turn the previously observed `Some` into
            // a `None` between the read and the update.
            let cache = self.app_state.read(cx).metric_catalog_cache().clone();
            let picker: Option<AnyElement> = self.chart_shell.update(cx, |shell, cx| {
                shell.metric_picker.as_mut().map(|picker| {
                    MetricPickerView {
                        state: picker,
                        cache: &cache,
                    }
                    .render(window, cx)
                    .into_any_element()
                })
            });

            return picker
                .map(|element| {
                    let content = div()
                        .flex()
                        .flex_col()
                        .size_full()
                        .min_h_0()
                        .overflow_hidden()
                        .child(element)
                        .into_any_element();

                    (
                        "chart-metric-picker",
                        ChartDocumentMetrics::PICKER_WIDTH,
                        content,
                    )
                })
                .into_iter()
                .collect();
        }

        if should_render_stats_rail(rail_open, rail_tab) {
            let theme = cx.theme().clone();

            return self
                .render_stats_rail(&theme, cx)
                .map(|content| ("chart-stats", ChartDocumentMetrics::RAIL_WIDTH, content))
                .into_iter()
                .collect();
        }

        Vec::new()
    }

    /// The rails of a chart tab, for the workspace to draw as islands beside
    /// the document island. An embedded chart keeps its rails docked inside,
    /// so it hands over none.
    pub(super) fn side_panels(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Vec<DocumentSidePanel> {
        if self.embedded {
            return Vec::new();
        }

        self.rail_panels(window, cx)
            .into_iter()
            .map(|(id, width, content)| DocumentSidePanel {
                id: id.into(),
                width,
                content,
            })
            .collect()
    }

    /// Build the always-visible legend row used when this chart is embedded in
    /// a dashboard panel. Returns `None` when the chart view has not been
    /// built yet (e.g. while data is still loading) or when the chart has no
    /// series to label (single-column charts, raw query results before binding).
    fn build_embedded_legend(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let chart_entity = self.chart_shell.read(cx).chart_view().cloned()?;

        let (series, palette, stats, focused_idx) = {
            let cv = chart_entity.read(cx);
            (
                cv.spec_series().to_vec(),
                cv.resolved_palette(cx),
                cv.series_stats().to_vec(),
                cv.focused_series_idx(),
            )
        };

        if series.is_empty() {
            return None;
        }

        let shell = self.chart_shell.clone();
        let hidden = shell.read(cx).chart_hidden_series.clone();
        let chart_colors = ChartColors::for_current(cx);

        let on_toggle = move |idx: usize, _window: &mut Window, cx: &mut App| {
            shell.update(cx, |s, cx| {
                s.toggle_chart_series_hidden(idx, cx);
            });
        };

        let legend = legend_element(
            &series,
            &palette,
            &stats,
            &hidden,
            focused_idx,
            &chart_colors,
            Some(on_toggle),
        );

        Some(
            div()
                .id("embedded-chart-legend")
                .flex_none()
                .px(DocumentMetrics::PADDING_X)
                .py(Spacing::XS)
                .child(legend)
                .into_any_element(),
        )
    }

    /// Render the Stats rail for the right-edge overlay (P1Chart): the
    /// window and the focused series' statistics as label and value rows,
    /// then every series with its swatch and a visibility toggle.
    ///
    /// Returns `None` when the chart view is still being built.
    fn render_stats_rail(
        &self,
        theme: &gpui_component::theme::Theme,
        cx: &mut App,
    ) -> Option<AnyElement> {
        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;

        let chart_view = self.chart_shell.read(cx).chart_view().cloned();

        let Some(chart_view) = chart_view else {
            return Some(
                self.wrap_stats_rail_chrome(
                    div()
                        .text_size(DocumentMetrics::TABLE_META_FONT)
                        .text_color(muted)
                        .child(dbflux_i18n::t!(
                            "document.chart.shell.stats_rail.rebuilding"
                        ))
                        .into_any_element(),
                    theme,
                ),
            );
        };

        let (stats_opt, focused_label, x_min, x_max, x_is_time, series, palette, focused_idx) = {
            let view = chart_view.read(cx);
            let focused_idx = view.focused_series_idx();
            (
                view.series_stats().get(focused_idx).copied().flatten(),
                view.series_label(focused_idx).to_string(),
                view.data_x_bounds().0,
                view.data_x_bounds().1,
                view.x_is_time(),
                view.spec_series().to_vec(),
                view.resolved_palette(cx),
                focused_idx,
            )
        };

        let hidden = self.chart_shell.read(cx).chart_hidden_series.clone();
        let points_count = self
            .last_result
            .as_ref()
            .map(|r| r.row_count())
            .unwrap_or(0);

        let source = self.profile_id.and_then(|profile_id| {
            self.app_state
                .read(cx)
                .profiles()
                .iter()
                .find(|profile| profile.id == profile_id)
                .map(|profile| profile.name.clone())
        });

        let row = |label: String, value: String| {
            div()
                .flex()
                .items_center()
                .justify_between()
                .gap(DocumentMetrics::GAP)
                .py(ChartDocumentMetrics::RAIL_ROW_PADDING_Y)
                .text_size(DocumentMetrics::TABLE_CELL_FONT)
                .child(div().text_color(muted).child(label))
                .child(
                    div()
                        .min_w_0()
                        .truncate()
                        .font_family(AppFonts::MONO)
                        .text_color(strong)
                        .child(value),
                )
        };

        let mut rows = vec![
            row(
                dbflux_i18n::t!("document.chart.shell.stats_rail.window.start"),
                format_x_value(x_min, x_is_time),
            ),
            row(
                dbflux_i18n::t!("document.chart.shell.stats_rail.window.end"),
                format_x_value(x_max, x_is_time),
            ),
            row(
                dbflux_i18n::t!("document.chart.shell.stats_rail.window.span"),
                format_span(x_max - x_min),
            ),
            row(
                dbflux_i18n::t!("document.chart.shell.stats_rail.window.points"),
                points_count.to_string(),
            ),
        ];

        match stats_opt {
            Some(stats) => {
                let with_series =
                    |value: f64| format!("{} ({focused_label})", format_y_value(value));

                rows.extend([
                    row(
                        dbflux_i18n::t!("document.chart.shell.stats_rail.max"),
                        with_series(stats.max),
                    ),
                    row(
                        dbflux_i18n::t!("document.chart.shell.stats_rail.min"),
                        with_series(stats.min),
                    ),
                    row(
                        dbflux_i18n::t!("document.chart.shell.stats_rail.mean"),
                        format_y_value(stats.avg),
                    ),
                    row("p50".to_string(), format_y_value(stats.p50)),
                    row("p95".to_string(), format_y_value(stats.p95)),
                    row("p99".to_string(), format_y_value(stats.p99)),
                    row(
                        dbflux_i18n::t!("document.chart.shell.stats_rail.last"),
                        format_y_value(stats.last),
                    ),
                ]);
            }
            None => rows.push(row(
                dbflux_i18n::t!("document.chart.shell.stats_rail.series"),
                dbflux_i18n::t!("document.chart.shell.stats_rail.no_stats"),
            )),
        }

        if let Some(source) = source {
            rows.push(row(
                dbflux_i18n::t!("document.chart.shell.stats_rail.source_title"),
                source,
            ));
        }

        let series_rows = series
            .iter()
            .enumerate()
            .map(|(index, spec)| {
                let color = palette
                    .get(spec.color_slot as usize % palette.len().max(1))
                    .copied()
                    .unwrap_or(ChromeColors::tint(theme));
                let is_hidden = hidden.contains(&index);
                let shell = self.chart_shell.clone();

                div()
                    .id(("chart-rail-series", index))
                    .flex()
                    .items_center()
                    .gap(DocumentMetrics::GAP)
                    .h(ChartDocumentMetrics::RAIL_SERIES_ROW_HEIGHT)
                    .text_size(Fields::TEXT)
                    .text_color(if is_hidden { muted } else { theme.foreground })
                    .when(index == focused_idx, |row| {
                        row.font_weight(FontWeight::BOLD)
                    })
                    .cursor_pointer()
                    .on_click(move |_, _, cx| {
                        shell.update(cx, |shell, cx| shell.toggle_chart_series_hidden(index, cx));
                    })
                    .child(
                        div()
                            .size(ChartDocumentMetrics::RAIL_SWATCH)
                            .flex_shrink_0()
                            .bg(if is_hidden {
                                color.opacity(0.35)
                            } else {
                                color
                            }),
                    )
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .truncate()
                            .child(spec.label.clone()),
                    )
                    .child(
                        Icon::new(if is_hidden {
                            AppIcon::EyeOff
                        } else {
                            AppIcon::Eye
                        })
                        .size(ChartDocumentMetrics::RAIL_ICON)
                        .color(muted),
                    )
                    .into_any_element()
            })
            .collect::<Vec<_>>();

        let body = div()
            .id("chart-doc-rail-stats-scroll")
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_y_scroll()
            .children(rows)
            .when(!series_rows.is_empty(), |body| {
                body.child(
                    div().pt(ChartDocumentMetrics::RAIL_SECTION_GAP).child(
                        Text::label(dbflux_i18n::t!("document.chart.shell.stats_rail.series"))
                            .font_size(ChartDocumentMetrics::RAIL_LABEL_FONT),
                    ),
                )
                .children(series_rows)
            })
            .into_any_element();

        Some(self.wrap_stats_rail_chrome(body, theme))
    }

    /// Wraps a stats rail body in the rail's padding, headed by
    /// the "STATS" label and a close button that dismisses the rail by
    /// setting `chart_rail_open = false` on the shell.
    fn wrap_stats_rail_chrome(
        &self,
        body: AnyElement,
        theme: &gpui_component::theme::Theme,
    ) -> AnyElement {
        let shell_for_close = self.chart_shell.clone();
        let muted = theme.muted_foreground;

        let header = div()
            .flex()
            .flex_row()
            .items_center()
            .justify_between()
            .pb(ChartDocumentMetrics::RAIL_LABEL_GAP)
            .child(
                Text::label(dbflux_i18n::t!("document.chart.toolbar.stats"))
                    .font_size(ChartDocumentMetrics::RAIL_LABEL_FONT),
            )
            .child(
                div()
                    .id("chart-doc-stats-rail-close")
                    .flex()
                    .items_center()
                    .cursor_pointer()
                    .on_click(move |_, _window, cx| {
                        shell_for_close.update(cx, |s, cx| {
                            s.chart_rail_open = false;
                            cx.notify();
                        });
                    })
                    .child(
                        Icon::new(AppIcon::CircleX)
                            .size(ChartDocumentMetrics::RAIL_ICON)
                            .color(muted),
                    ),
            );

        div()
            .flex()
            .flex_col()
            .size_full()
            .min_h_0()
            .px(ChartDocumentMetrics::RAIL_PADDING_X)
            .py(ChartDocumentMetrics::RAIL_PADDING_Y)
            .child(header)
            .child(body)
            .into_any_element()
    }
}

/// A rail docked inside the chart, for a chart embedded where no workspace
/// island can take it: an island on the right edge, over the chart area.
fn docked_rail(
    theme: &gpui_component::theme::Theme,
    width: Pixels,
    content: AnyElement,
) -> AnyElement {
    docked_island_frame(theme)
        .absolute()
        .top_0()
        .right_0()
        .bottom_0()
        .w(width + IslandMetrics::GAP)
        .occlude()
        .child(Island::new().flex_1().min_h_0().child(content))
        .into_any_element()
}
