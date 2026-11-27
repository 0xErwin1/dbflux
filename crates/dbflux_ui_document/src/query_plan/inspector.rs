//! The node inspector: what the workspace rail shows for the selected plan
//! node. It repeats the operation's description, so a keyboard user reads the
//! same text a mouse user gets in the tooltip, then the estimate, what the
//! node reads and the engine's remaining attributes.

use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{ChromeColors, SchemaInspectorMetrics, ui};
use gpui::prelude::*;
use gpui::{
    AnyElement, App, Context, ElementId, EventEmitter, FocusHandle, Focusable, FontWeight, Rems,
    SharedString, Window, div,
};
use gpui_component::ActiveTheme;

use super::ids::tagged;
use super::model::{
    PlanNodeView, RowDrift, format_cost, format_loops, format_ratio, format_rows, format_share,
    format_time,
};
use super::tooltip::tip;

/// Width of the label column of a field row. (112 px)
const LABEL_WIDTH: Rems = ui(112.0);

/// A request from the inspector's own controls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PlanInspectorEvent {
    /// The close button: dismiss the rail.
    Close,
}

pub(crate) struct PlanInspector {
    node: PlanNodeView,
    focus_handle: FocusHandle,
}

impl EventEmitter<PlanInspectorEvent> for PlanInspector {}

impl PlanInspector {
    pub(super) fn new(node: PlanNodeView, cx: &mut Context<Self>) -> Self {
        Self {
            node,
            focus_handle: cx.focus_handle(),
        }
    }

    /// Shows another node in the same rail.
    pub(super) fn show(&mut self, node: PlanNodeView, cx: &mut Context<Self>) {
        self.node = node;
        cx.notify();
    }

    #[cfg(test)]
    pub(crate) fn node(&self) -> &PlanNodeView {
        &self.node
    }

    fn render_header(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();

        tagged("plan-inspector-header")
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(SchemaInspectorMetrics::HEADER_GAP)
            .h(SchemaInspectorMetrics::HEADER_HEIGHT)
            .px(SchemaInspectorMetrics::HEADER_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Icon::new(super::diagram::node_icon(&self.node))
                    .size(SchemaInspectorMetrics::HEADER_ICON)
                    .color(ChromeColors::tint(theme)),
            )
            .child(
                div()
                    .flex()
                    .flex_1()
                    .min_w_0()
                    .items_center()
                    .gap(SchemaInspectorMetrics::HEADER_GAP)
                    .child(
                        div()
                            .truncate()
                            .font_weight(FontWeight::BOLD)
                            .text_color(ChromeColors::strong(theme))
                            .child(self.node.operation.clone()),
                    )
                    .child(
                        div()
                            .flex_shrink_0()
                            .font_family(dbflux_components::fonts::editor_family(cx))
                            .text_size(
                                dbflux_components::tokens::DocumentInspectorMetrics::SIZE_FONT,
                            )
                            .text_color(theme.muted_foreground)
                            .child(dbflux_i18n::t!(
                                "document.query_plan.inspector.node",
                                id = self.node.id
                            )),
                    ),
            )
            .child(
                Button::new(
                    "plan-inspector-close",
                    dbflux_i18n::t!("document.data.row_inspector.action.close"),
                )
                .icon(AppIcon::CircleX)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(|_, _, _, cx| cx.emit(PlanInspectorEvent::Close))),
            )
            .into_any_element()
    }

    /// One `label  value` row; the label carries the tooltip that explains
    /// the field when there is one.
    fn field(
        &self,
        id: &'static str,
        label: String,
        value: String,
        tooltip: Option<(String, String)>,
        cx: &App,
    ) -> AnyElement {
        let theme = cx.theme();

        tagged(ElementId::Name(format!("plan-inspector-field-{id}").into()))
            .flex()
            .items_start()
            .gap(SchemaInspectorMetrics::GAP)
            .text_size(SchemaInspectorMetrics::ROW_FONT)
            .when_some(tooltip, |row, (title, body)| {
                row.tooltip(tip(title, Some(SharedString::from(body))))
            })
            .child(
                div()
                    .flex_shrink_0()
                    .w(LABEL_WIDTH)
                    .text_color(theme.muted_foreground)
                    .child(label),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_color(ChromeColors::strong(theme))
                    .child(value),
            )
            .into_any_element()
    }

    fn estimate_rows(&self, cx: &App) -> Vec<AnyElement> {
        let node = &self.node;
        let mut rows = Vec::new();

        if let Some(cost) = node.startup_cost {
            rows.push(self.field(
                "startup-cost",
                dbflux_i18n::t!("document.query_plan.inspector.startup_cost"),
                format_cost(cost),
                Some((
                    dbflux_i18n::t!("document.query_plan.tooltip.startup_cost_title"),
                    dbflux_i18n::t!("document.query_plan.tooltip.startup_cost_body"),
                )),
                cx,
            ));
        }
        if let Some(cost) = node.total_cost {
            rows.push(self.field(
                "total-cost",
                dbflux_i18n::t!("document.query_plan.inspector.total_cost"),
                format_cost(cost),
                Some((
                    dbflux_i18n::t!("document.query_plan.tooltip.cost_title"),
                    dbflux_i18n::t!("document.query_plan.tooltip.cost_body"),
                )),
                cx,
            ));
        }
        if let Some(cost) = node.self_cost {
            let value = match node.self_share {
                Some(share) => format!("{} · {}", format_cost(cost), format_share(share)),
                None => format_cost(cost),
            };
            rows.push(self.field(
                "self-cost",
                dbflux_i18n::t!("document.query_plan.inspector.self_cost"),
                value,
                Some((
                    dbflux_i18n::t!("document.query_plan.tooltip.self_cost_title"),
                    dbflux_i18n::t!("document.query_plan.tooltip.self_cost_node_body"),
                )),
                cx,
            ));
        }
        if let Some(count) = node.rows {
            let label = if node.has_actuals() {
                dbflux_i18n::t!("document.query_plan.inspector.rows_per_loop")
            } else {
                dbflux_i18n::t!("document.query_plan.inspector.rows")
            };
            rows.push(self.field(
                "rows",
                label,
                format_rows(count),
                Some((
                    dbflux_i18n::t!("document.query_plan.tooltip.rows_title"),
                    dbflux_i18n::t!("document.query_plan.tooltip.rows_body"),
                )),
                cx,
            ));
        }

        rows
    }

    /// What the engine measured, for a node of an analyzed plan: times across
    /// all loops, rows per loop, and how far those rows are from the estimate.
    fn actual_rows(&self, cx: &App) -> Vec<AnyElement> {
        let node = &self.node;
        let mut rows = Vec::new();

        if let Some(time) = node.actual_startup_time {
            rows.push(self.field(
                "startup-time",
                dbflux_i18n::t!("document.query_plan.inspector.startup_time"),
                format_time(time),
                None,
                cx,
            ));
        }
        if let Some(time) = node.actual_time {
            rows.push(self.field(
                "total-time",
                dbflux_i18n::t!("document.query_plan.inspector.total_time"),
                format_time(time),
                Some((
                    dbflux_i18n::t!("document.query_plan.tooltip.time_title"),
                    dbflux_i18n::t!("document.query_plan.tooltip.time_body"),
                )),
                cx,
            ));
        }
        if let Some(time) = node.self_time {
            let value = match node.self_time_share {
                Some(share) => format!("{} · {}", format_time(time), format_share(share)),
                None => format_time(time),
            };
            rows.push(self.field(
                "self-time",
                dbflux_i18n::t!("document.query_plan.inspector.self_time"),
                value,
                Some((
                    dbflux_i18n::t!("document.query_plan.tooltip.self_time_title"),
                    dbflux_i18n::t!("document.query_plan.tooltip.self_time_body"),
                )),
                cx,
            ));
        }
        if let Some(count) = node.actual_rows {
            rows.push(self.field(
                "actual-rows",
                dbflux_i18n::t!("document.query_plan.inspector.rows_per_loop"),
                format_rows(count),
                Some((
                    dbflux_i18n::t!("document.query_plan.tooltip.actual_rows_title"),
                    dbflux_i18n::t!("document.query_plan.tooltip.actual_rows_body"),
                )),
                cx,
            ));
        }
        if let Some(loops) = node.loops {
            rows.push(self.field(
                "loops",
                dbflux_i18n::t!("document.query_plan.inspector.loops"),
                format_loops(loops),
                Some((
                    dbflux_i18n::t!("document.query_plan.tooltip.loops_title"),
                    dbflux_i18n::t!("document.query_plan.tooltip.loops_body"),
                )),
                cx,
            ));
        }
        if let Some(drift) = node.row_drift {
            rows.push(self.field(
                "versus",
                dbflux_i18n::t!("document.query_plan.inspector.versus"),
                versus_label(drift),
                None,
                cx,
            ));
        }

        rows
    }

    fn reads_rows(&self, cx: &App) -> Vec<AnyElement> {
        let node = &self.node;
        let mut rows = Vec::new();

        if let Some(relation) = node.relation.clone() {
            rows.push(self.field(
                "relation",
                dbflux_i18n::t!("document.query_plan.inspector.relation"),
                relation,
                Some((
                    dbflux_i18n::t!("document.query_plan.tooltip.relation_title"),
                    dbflux_i18n::t!("document.query_plan.tooltip.relation_body"),
                )),
                cx,
            ));
        }
        if let Some(index) = node.index.clone() {
            rows.push(self.field(
                "index",
                dbflux_i18n::t!("document.query_plan.inspector.index"),
                index,
                Some((
                    dbflux_i18n::t!("document.query_plan.tooltip.index_title"),
                    dbflux_i18n::t!("document.query_plan.tooltip.index_body"),
                )),
                cx,
            ));
        }

        rows
    }

    /// The key condition as its own labelled row, then the driver's details.
    /// A detail that repeats the key condition is left out so the condition is
    /// not listed twice; drivers that keep it out of `details` still show it.
    fn detail_rows(&self, cx: &App) -> Vec<AnyElement> {
        let condition = self.node.key_condition.as_ref();
        let mut rows = Vec::new();

        if let Some(condition) = condition {
            rows.push(self.field(
                "key-condition",
                condition.label.clone(),
                condition.value.clone(),
                Some((
                    condition.label.clone(),
                    dbflux_i18n::t!("document.query_plan.tooltip.condition_body"),
                )),
                cx,
            ));
        }

        rows.extend(
            self.node
                .details
                .iter()
                .enumerate()
                .filter(|(_, (key, value))| {
                    !condition.is_some_and(|condition| {
                        &condition.label == key && &condition.value == value
                    })
                })
                .map(|(position, (key, value))| {
                    self.field_by_position(position, key.clone(), value.clone(), None, cx)
                }),
        );

        rows
    }

    /// A detail row, whose id is its position since engine keys can repeat.
    fn field_by_position(
        &self,
        position: usize,
        label: String,
        value: String,
        tooltip: Option<(String, String)>,
        cx: &App,
    ) -> AnyElement {
        let theme = cx.theme();

        tagged(ElementId::Name(
            format!("plan-inspector-detail-{position}").into(),
        ))
        .flex()
        .items_start()
        .gap(SchemaInspectorMetrics::GAP)
        .text_size(SchemaInspectorMetrics::ROW_FONT)
        .when_some(tooltip, |row, (title, body)| {
            row.tooltip(tip(title, Some(SharedString::from(body))))
        })
        .child(
            div()
                .flex_shrink_0()
                .w(LABEL_WIDTH)
                .text_color(theme.muted_foreground)
                .child(label),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .font_family(dbflux_components::fonts::editor_family(cx))
                .text_color(ChromeColors::strong(theme))
                .child(value),
        )
        .into_any_element()
    }
}

/// "15.5× more rows": the actual rows against the estimate.
pub(super) fn versus_label(drift: RowDrift) -> String {
    match drift {
        RowDrift::More(factor) => dbflux_i18n::t!(
            "document.query_plan.inspector.versus_more",
            ratio = format_ratio(factor)
        ),
        RowDrift::Fewer(factor) => dbflux_i18n::t!(
            "document.query_plan.inspector.versus_fewer",
            ratio = format_ratio(factor)
        ),
        RowDrift::Same => dbflux_i18n::t!("document.query_plan.inspector.versus_same"),
    }
}

fn section(id: &'static str, label: String, rows: Vec<AnyElement>) -> Option<AnyElement> {
    if rows.is_empty() {
        return None;
    }

    Some(
        tagged(ElementId::Name(
            format!("plan-inspector-section-{id}").into(),
        ))
        .flex()
        .flex_col()
        .gap(SchemaInspectorMetrics::ROW_GAP)
        .child(Text::label(label).font_size(SchemaInspectorMetrics::LABEL_FONT))
        .children(rows)
        .into_any_element(),
    )
}

impl Focusable for PlanInspector {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl gpui::Render for PlanInspector {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let header = self.render_header(cx);
        let theme = cx.theme();

        let sections: Vec<AnyElement> = [
            section(
                "estimate",
                dbflux_i18n::t!("document.query_plan.inspector.estimate"),
                self.estimate_rows(cx),
            ),
            section(
                "actual",
                dbflux_i18n::t!("document.query_plan.inspector.actual"),
                self.actual_rows(cx),
            ),
            section(
                "reads",
                dbflux_i18n::t!("document.query_plan.inspector.reads"),
                self.reads_rows(cx),
            ),
            section(
                "details",
                dbflux_i18n::t!("document.query_plan.inspector.details"),
                self.detail_rows(cx),
            ),
        ]
        .into_iter()
        .flatten()
        .collect();

        tagged("plan-inspector")
            .size_full()
            .flex()
            .flex_col()
            .track_focus(&self.focus_handle)
            .child(header)
            .child(
                tagged("plan-inspector-body")
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .flex_col()
                    .gap(SchemaInspectorMetrics::GAP)
                    .px(SchemaInspectorMetrics::PADDING_X)
                    .py(SchemaInspectorMetrics::PADDING_Y)
                    .overflow_y_scroll()
                    .when_some(self.node.description.clone(), |body, description| {
                        body.child(
                            tagged("plan-inspector-description")
                                .text_size(SchemaInspectorMetrics::SUMMARY_FONT)
                                .text_color(theme.muted_foreground)
                                .child(description),
                        )
                    })
                    .children(sections),
            )
    }
}
