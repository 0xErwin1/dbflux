//! What surrounds the plan views: the view switch row with the totals and
//! the zoom controls, the warnings band, and the footer.

use std::time::Duration;

use dbflux_components::controls::Button;
use dbflux_components::diagram as canvas;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, SegmentedControl, SegmentedItem};
use dbflux_components::tokens::{ChromeColors, FontSizes, ResultMetrics, Spacing, ui};
use dbflux_core::PlanFormat;
use gpui::prelude::*;
use gpui::{AnyElement, Context, FontWeight, Role, SharedString, Window, div};
use gpui_component::ActiveTheme;

use super::ids::tagged;
use super::model::{format_cost, format_rows, format_time};
use super::tooltip::tip;
use super::{AnalyzeOffer, PlanRunMode, PlanState, PlanView, PlanViewMode};

impl PlanView {
    pub(super) fn render_body(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        match &self.state {
            PlanState::Ready(_) => match self.mode {
                PlanViewMode::Diagram => self.render_diagram(window, cx),
                PlanViewMode::Tree => self.render_tree(cx),
                PlanViewMode::Raw => self.render_raw(cx),
            },
            _ => self.render_state(cx),
        }
    }

    /// The row under the result tabs: the view switch, what the plan covers
    /// with its totals, and (in the diagram) the zoom controls.
    pub(super) fn render_switch_row(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let ready = self.model().is_some();
        let in_diagram = ready && self.mode == PlanViewMode::Diagram;

        tagged("plan-switch-row")
            .flex()
            .flex_none()
            .items_center()
            .gap(ResultMetrics::VIEW_ROW_GAP)
            .w_full()
            .min_h(ResultMetrics::VIEW_ROW_HEIGHT)
            .px(ResultMetrics::FOOTER_PADDING_X)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(theme.border)
            .child(self.render_view_switch(ready, cx))
            .children(
                self.offer
                    .as_ref()
                    .map(|offer| self.render_run_mode_switch(offer, cx)),
            )
            .child(self.render_totals(cx))
            .child(div().flex_1())
            .when(in_diagram, |row| row.child(self.render_zoom_controls(cx)))
            .into_any_element()
    }

    fn render_view_switch(&self, ready: bool, cx: &mut Context<Self>) -> AnyElement {
        let view = cx.entity().downgrade();
        let items: Vec<SegmentedItem> = PlanViewMode::ALL
            .iter()
            .map(|mode| SegmentedItem::new(mode.key(), mode_label(*mode)).icon(mode_icon(*mode)))
            .collect();

        let control = SegmentedControl::new(items, self.mode.key(), move |selected, _, cx| {
            let Some(mode) = PlanViewMode::ALL
                .iter()
                .copied()
                .find(|mode| mode.key() == selected.as_ref())
            else {
                return;
            };

            if let Err(error) = view.update(cx, |this, cx| this.set_mode(mode, cx)) {
                log::debug!("plan view released before its view switch: {error}");
            }
        })
        .group("plan-view")
        .tabs(true)
        .aria_label(dbflux_i18n::t!("document.query_plan.view.switch"))
        .disabled(!ready)
        .segment_bounds(&self.switch_bounds);

        let entity = cx.entity();
        let origin_probe = canvas::measure_bounds(move |bounds, cx| {
            entity.update(cx, |this, _| this.switch_origin = bounds.origin);
        });

        let tooltips = PlanViewMode::ALL.iter().filter_map(|mode| {
            let bounds = self.switch_bounds.get(mode.key())?;
            let (title, body) = mode_tooltip(*mode);

            Some(
                tagged(SharedString::from(format!("plan-view-tip-{}", mode.key())))
                    .absolute()
                    .left(bounds.origin.x - self.switch_origin.x)
                    .top(bounds.origin.y - self.switch_origin.y)
                    .w(bounds.size.width)
                    .h(bounds.size.height)
                    .tooltip(tip(title, Some(body.into()))),
            )
        });

        tagged("plan-view-switch")
            .relative()
            .flex_none()
            .child(origin_probe)
            .child(control)
            .children(tooltips.collect::<Vec<_>>())
            .into_any_element()
    }

    /// Estimate | Analyze. Analyze is dimmed, with the reason on hover, when
    /// the statement is not one that may run.
    fn render_run_mode_switch(&self, offer: &AnalyzeOffer, cx: &mut Context<Self>) -> AnyElement {
        let view = cx.entity().downgrade();
        let items: Vec<SegmentedItem> = PlanRunMode::ALL
            .iter()
            .map(|mode| {
                SegmentedItem::new(mode.key(), run_mode_label(*mode))
                    .icon(match mode {
                        PlanRunMode::Estimate => AppIcon::Eye,
                        PlanRunMode::Analyze => AppIcon::Activity,
                    })
                    .disabled(*mode == PlanRunMode::Analyze && offer.refusal.is_some())
            })
            .collect();

        let control = SegmentedControl::new(items, self.run_mode.key(), move |selected, _, cx| {
            let Some(mode) = PlanRunMode::ALL
                .iter()
                .copied()
                .find(|mode| mode.key() == selected.as_ref())
            else {
                return;
            };

            if let Err(error) = view.update(cx, |this, cx| this.choose_run_mode(mode, cx)) {
                log::debug!("plan view released before its run mode switch: {error}");
            }
        })
        .group("plan-run-mode")
        .tabs(true)
        .aria_label(dbflux_i18n::t!("document.query_plan.run_mode.switch"))
        .disabled(self.is_loading())
        .segment_bounds(&self.run_mode_bounds);

        let entity = cx.entity();
        let origin_probe = canvas::measure_bounds(move |bounds, cx| {
            entity.update(cx, |this, _| this.run_mode_origin = bounds.origin);
        });

        let tooltips = PlanRunMode::ALL.iter().filter_map(|mode| {
            let bounds = self.run_mode_bounds.get(mode.key())?;
            let body = match (mode, offer.refusal) {
                (PlanRunMode::Analyze, Some(refusal)) => refusal.reason(),
                (PlanRunMode::Analyze, None) => {
                    dbflux_i18n::t!("document.query_plan.tooltip.analyze_body")
                }
                (PlanRunMode::Estimate, _) => {
                    dbflux_i18n::t!("document.query_plan.tooltip.estimate_body")
                }
            };

            Some(
                tagged(SharedString::from(format!(
                    "plan-run-mode-tip-{}",
                    mode.key()
                )))
                .absolute()
                .left(bounds.origin.x - self.run_mode_origin.x)
                .top(bounds.origin.y - self.run_mode_origin.y)
                .w(bounds.size.width)
                .h(bounds.size.height)
                .tooltip(tip(run_mode_label(*mode), Some(body.into()))),
            )
        });

        tagged("plan-run-mode-switch")
            .relative()
            .flex_none()
            .child(origin_probe)
            .child(control)
            .children(tooltips.collect::<Vec<_>>())
            .into_any_element()
    }

    /// "Statement L7–16 · total cost 9,639.43 · rows 50": what the plan
    /// covers, then the totals the engine reports for it.
    fn render_totals(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let strong = ChromeColors::strong(theme);
        let model = self.model();

        let analyzed = model.is_some_and(|model| model.analyzed);
        let total_cost = model
            .and_then(|model| model.total_cost)
            .filter(|_| !analyzed);
        let total_rows = model.and_then(|model| model.total_rows);
        let planning = model
            .and_then(|model| model.planning_time)
            .filter(|_| analyzed);
        let execution = model
            .and_then(|model| model.execution_time)
            .filter(|_| analyzed);
        let has_totals = total_cost.is_some()
            || total_rows.is_some()
            || planning.is_some()
            || execution.is_some();

        let strong_value = move |value: String| {
            div()
                .text_color(strong)
                .font_weight(FontWeight::MEDIUM)
                .child(value)
        };
        let separator = || div().child("·");
        let tooltip_body = if analyzed {
            dbflux_i18n::t!("document.query_plan.tooltip.totals_analyzed_body")
        } else {
            dbflux_i18n::t!("document.query_plan.tooltip.totals_body")
        };

        tagged("plan-totals")
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .min_w_0()
            .font_family(dbflux_components::fonts::editor_family(cx))
            .text_size(ResultMetrics::STATEMENT_FONT)
            .text_color(theme.muted_foreground)
            .child(div().child(self.scope.clone()))
            .when_some(total_cost, |row, cost| {
                row.child(separator())
                    .child(div().child(dbflux_i18n::t!("document.query_plan.totals.cost")))
                    .child(strong_value(format_cost(cost)))
            })
            .when_some(planning, |row, time| {
                row.child(separator())
                    .child(div().child(dbflux_i18n::t!("document.query_plan.totals.planning")))
                    .child(strong_value(format_time(time)))
            })
            .when_some(execution, |row, time| {
                row.child(separator())
                    .child(div().child(dbflux_i18n::t!("document.query_plan.totals.execution")))
                    .child(strong_value(format_time(time)))
            })
            .when_some(total_rows, |row, rows| {
                row.child(separator())
                    .child(div().child(dbflux_i18n::t!("document.query_plan.totals.rows")))
                    .child(strong_value(format_rows(rows)))
            })
            .when(has_totals, |row| {
                row.tooltip(tip(
                    dbflux_i18n::t!("document.query_plan.tooltip.totals_title"),
                    Some(tooltip_body.clone().into()),
                ))
            })
            .into_any_element()
    }

    /// Zoom out, the readout (a click resets the view), zoom in and Fit.
    fn render_zoom_controls(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let zoom = self.camera.zoom;

        div()
            .flex()
            .flex_none()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new(
                    "plan-zoom-out",
                    dbflux_i18n::t!("document.query_plan.controls.zoom_out"),
                )
                .icon(AppIcon::ZoomOut)
                .icon_only()
                .disabled(!canvas::can_zoom_out(zoom))
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| this.zoom_out(cx))),
            )
            .child(
                tagged("plan-zoom-reset")
                    .flex()
                    .justify_center()
                    .min_w(dbflux_components::tokens::SchemaMetrics::ZOOM_WIDTH)
                    .cursor_pointer()
                    .role(Role::Button)
                    .aria_label(dbflux_i18n::t!("document.query_plan.controls.zoom_reset"))
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_size(dbflux_components::tokens::SchemaMetrics::ZOOM_FONT)
                    .text_color(theme.foreground)
                    .tooltip(tip(
                        dbflux_i18n::t!("document.query_plan.controls.zoom_reset"),
                        None,
                    ))
                    .on_click(cx.listener(|this, _, _, cx| this.reset_view(cx)))
                    .child(format!("{:.0}%", zoom * 100.0)),
            )
            .child(
                Button::new(
                    "plan-zoom-in",
                    dbflux_i18n::t!("document.query_plan.controls.zoom_in"),
                )
                .icon(AppIcon::ZoomIn)
                .icon_only()
                .disabled(!canvas::can_zoom_in(zoom))
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| this.zoom_in(cx))),
            )
            .child(
                Button::new(
                    "plan-fit",
                    dbflux_i18n::t!("document.query_plan.controls.fit"),
                )
                .icon(AppIcon::Maximize2)
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| this.fit(cx))),
            )
            .into_any_element()
    }

    /// The band under the switch row: how many warnings the plan carries and
    /// what they say.
    pub(super) fn render_warnings(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let model = self.model()?;
        if model.warnings.is_empty() {
            return None;
        }

        let theme = cx.theme();
        let count = model.warnings.len();
        let heading = if count == 1 {
            dbflux_i18n::t!("document.query_plan.warnings.one")
        } else {
            dbflux_i18n::t!("document.query_plan.warnings.many", count = count)
        };

        Some(
            tagged("plan-warnings")
                .flex()
                .flex_none()
                .items_start()
                .gap(Spacing::SM)
                .px(ResultMetrics::FOOTER_PADDING_X)
                .py(Spacing::SM)
                .text_size(FontSizes::BASE)
                .border_b_1()
                .border_color(theme.border)
                .bg(theme.warning.opacity(0.08))
                .child(
                    Icon::new(AppIcon::TriangleAlert)
                        .size(ResultMetrics::FOOTER_ICON)
                        .color(theme.warning),
                )
                .child(
                    div()
                        .font_weight(FontWeight::BOLD)
                        .text_color(theme.warning)
                        .child(heading),
                )
                .child(
                    tagged("plan-warnings-text")
                        .flex_1()
                        .min_w_0()
                        .max_h(ui(96.0))
                        .overflow_y_scroll()
                        .text_color(theme.muted_foreground)
                        .child(model.warnings.join(" · ")),
                )
                .into_any_element(),
        )
    }

    /// The strip under the view: what the plan holds, that the query was not
    /// run, how long the plan took and, on the raw plan, Copy.
    pub(super) fn render_footer(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let model = self.model();
        let in_raw = model.is_some() && self.mode == PlanViewMode::Raw;

        let left: Vec<AnyElement> = match (&self.state, model) {
            (PlanState::Loading, _) => vec![footer_item(
                AppIcon::Loader,
                if self.run_mode == PlanRunMode::Analyze {
                    dbflux_i18n::t!("document.query_plan.loading.analyzing")
                } else {
                    dbflux_i18n::t!("document.query_plan.loading.running")
                },
                cx,
            )],
            (_, Some(model)) => {
                let summary = if in_raw {
                    format_label(model.raw_format)
                } else {
                    node_count_label(model.nodes.len())
                };
                let run_state = if model.analyzed {
                    tagged("plan-footer-analyzed")
                        .child(footer_item(
                            AppIcon::Play,
                            dbflux_i18n::t!("document.query_plan.footer.analyzed"),
                            cx,
                        ))
                        .into_any_element()
                } else {
                    footer_item(
                        AppIcon::Lock,
                        dbflux_i18n::t!("document.query_plan.footer.not_run"),
                        cx,
                    )
                };
                vec![footer_item(AppIcon::Rows3, summary, cx), run_state]
            }
            _ => {
                // An analyzed run executes the statement, so a cancel or an
                // error after dispatch cannot claim that nothing ran.
                let (id, icon, text) = match (&self.state, self.run_mode) {
                    (PlanState::Cancelled, PlanRunMode::Analyze) => (
                        "plan-footer-analyze-cancelled",
                        AppIcon::X,
                        dbflux_i18n::t!("document.query_plan.footer.analyze_cancelled"),
                    ),
                    (PlanState::Failed(_), PlanRunMode::Analyze) => (
                        "plan-footer-analyze-failed",
                        AppIcon::TriangleAlert,
                        dbflux_i18n::t!("document.query_plan.footer.analyze_failed"),
                    ),
                    _ => (
                        "plan-footer-not-run",
                        AppIcon::Lock,
                        dbflux_i18n::t!("document.query_plan.footer.not_run"),
                    ),
                };
                vec![
                    tagged(id)
                        .child(footer_item(icon, text, cx))
                        .into_any_element(),
                ]
            }
        };

        tagged("plan-footer")
            .flex()
            .flex_none()
            .items_center()
            .gap(ResultMetrics::FOOTER_GAP)
            .h(ResultMetrics::FOOTER_HEIGHT)
            .px(ResultMetrics::FOOTER_PADDING_X)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.background)
            .text_size(ResultMetrics::FOOTER_FONT)
            .text_color(theme.muted_foreground)
            .children(left)
            .child(div().flex_1())
            .when(self.is_loading(), |footer| {
                footer.child(
                    Button::new(
                        "plan-cancel",
                        dbflux_i18n::t!("document.query_plan.loading.cancel"),
                    )
                    .kbd("Esc")
                    .tab_stop(false)
                    .on_click(cx.listener(|_, _, _, cx| {
                        cx.emit(super::PlanViewEvent::CancelRequested);
                    })),
                )
            })
            .when(in_raw, |footer| {
                footer.child(
                    Button::new(
                        "plan-raw-copy",
                        dbflux_i18n::t!("document.query_plan.raw.copy"),
                    )
                    .icon(AppIcon::Copy)
                    .tab_stop(false)
                    .on_click(cx.listener(|this, _, _, cx| this.copy_raw(cx))),
                )
            })
            .when_some(
                self.elapsed.filter(|_| model.is_some()),
                |footer, elapsed| {
                    footer.child(
                        div()
                            .flex_shrink_0()
                            .font_family(dbflux_components::fonts::editor_family(cx))
                            .child(format_elapsed(elapsed)),
                    )
                },
            )
            .into_any_element()
    }
}

fn footer_item(icon: AppIcon, text: String, cx: &Context<PlanView>) -> AnyElement {
    div()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(ResultMetrics::FOOTER_ITEM_GAP)
        .child(
            Icon::new(icon)
                .size(ResultMetrics::FOOTER_ICON)
                .color(cx.theme().muted_foreground),
        )
        .child(text)
        .into_any_element()
}

fn mode_label(mode: PlanViewMode) -> String {
    match mode {
        PlanViewMode::Diagram => dbflux_i18n::t!("document.query_plan.view.diagram"),
        PlanViewMode::Tree => dbflux_i18n::t!("document.query_plan.view.tree"),
        PlanViewMode::Raw => dbflux_i18n::t!("document.query_plan.view.raw"),
    }
}

fn run_mode_label(mode: PlanRunMode) -> String {
    match mode {
        PlanRunMode::Estimate => dbflux_i18n::t!("document.query_plan.run_mode.estimate"),
        PlanRunMode::Analyze => dbflux_i18n::t!("document.query_plan.run_mode.analyze"),
    }
}

fn mode_icon(mode: PlanViewMode) -> AppIcon {
    match mode {
        PlanViewMode::Diagram => AppIcon::ChartNetwork,
        PlanViewMode::Tree => AppIcon::Rows3,
        PlanViewMode::Raw => AppIcon::Code,
    }
}

fn mode_tooltip(mode: PlanViewMode) -> (String, String) {
    match mode {
        PlanViewMode::Diagram => (
            dbflux_i18n::t!("document.query_plan.view.diagram"),
            dbflux_i18n::t!("document.query_plan.tooltip.diagram_body"),
        ),
        PlanViewMode::Tree => (
            dbflux_i18n::t!("document.query_plan.view.tree"),
            dbflux_i18n::t!("document.query_plan.tooltip.tree_body"),
        ),
        PlanViewMode::Raw => (
            dbflux_i18n::t!("document.query_plan.view.raw"),
            dbflux_i18n::t!("document.query_plan.tooltip.raw_body"),
        ),
    }
}

pub(super) fn node_count_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("document.query_plan.footer.nodes.one", count = count)
    } else {
        dbflux_i18n::t!("document.query_plan.footer.nodes.many", count = count)
    }
}

fn format_label(format: PlanFormat) -> String {
    match format {
        PlanFormat::Json => dbflux_i18n::t!("document.query_plan.footer.format.json"),
        PlanFormat::Xml => dbflux_i18n::t!("document.query_plan.footer.format.xml"),
        PlanFormat::Text => dbflux_i18n::t!("document.query_plan.footer.format.text"),
    }
}

/// How long the plan took: milliseconds under a second, seconds above.
pub(super) fn format_elapsed(elapsed: Duration) -> String {
    let millis = elapsed.as_millis();
    if millis == 0 {
        "<1 ms".to_string()
    } else if millis < 1000 {
        format!("{millis} ms")
    } else {
        format!("{:.1} s", elapsed.as_secs_f64())
    }
}
