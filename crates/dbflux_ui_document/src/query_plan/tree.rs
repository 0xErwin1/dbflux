//! The Tree view: the plan as an indented list with its numbers in columns.
//!
//! A column with no data for this plan is left out, so a plan without costs
//! is a short list rather than a table of dashes. The key condition of a node
//! sits under it, since that is the line that says why the node is slow.

use std::collections::HashSet;

use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Badge, BadgeTone, Icon};
use dbflux_components::tokens::{ChromeColors, Radii, Spacing, ui};
use gpui::prelude::*;
use gpui::{
    AnyElement, App, Context, ElementId, FontWeight, Rems, Role, SharedString, div, px, relative,
};
use gpui_component::ActiveTheme;

use super::diagram::node_icon;
use super::ids::tagged;
use super::model::{
    NodeId, PlanColumns, PlanModel, PlanNodeView, format_cost, format_loops, format_ratio,
    format_rows, format_share, format_time,
};
use super::tooltip::tip;
use super::{PlanView, Step};

const INDEX_WIDTH: Rems = ui(200.0);
const COST_WIDTH: Rems = ui(110.0);
const SELF_COST_WIDTH: Rems = ui(180.0);
const ROWS_WIDTH: Rems = ui(90.0);
const ACTUAL_ROWS_WIDTH: Rems = ui(100.0);
const TIME_WIDTH: Rems = ui(90.0);
const LOOPS_WIDTH: Rems = ui(80.0);
const BAR_WIDTH: Rems = ui(56.0);
/// Indent of one tree level. (16 px)
const INDENT: Rems = ui(16.0);
const ROW_HEIGHT: Rems = ui(30.0);
const FONT: Rems = ui(12.5);
const CONDITION_FONT: Rems = ui(11.0);

/// Left padding of a row `levels` deep.
fn indent(levels: usize) -> Rems {
    ui(16.0 * levels as f32)
}

/// A move inside the Tree view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RowMove {
    Select(NodeId),
    Collapse(NodeId),
    Expand(NodeId),
}

/// The rows on screen, in order: every node except the descendants of a
/// collapsed one.
pub(super) fn visible_rows(model: &PlanModel, collapsed: &HashSet<NodeId>) -> Vec<NodeId> {
    let mut rows = Vec::with_capacity(model.nodes.len());
    let mut hidden_below: Option<usize> = None;

    for node in &model.nodes {
        if let Some(depth) = hidden_below {
            if node.depth > depth {
                continue;
            }
            hidden_below = None;
        }

        rows.push(node.id);
        if collapsed.contains(&node.id) {
            hidden_below = Some(node.depth);
        }
    }

    rows
}

/// What an arrow key does with `current` selected: Up and Down step between
/// rows, Left collapses the row (or goes to its parent when it is closed or a
/// leaf), Right opens it (or goes to its first child).
pub(super) fn row_move(
    model: &PlanModel,
    collapsed: &HashSet<NodeId>,
    current: NodeId,
    step: Step,
) -> Option<RowMove> {
    let rows = visible_rows(model, collapsed);
    let position = rows.iter().position(|id| *id == current)?;
    let node = model.node(current)?;
    let has_children = !node.children.is_empty();

    match step {
        Step::Down => rows.get(position + 1).copied().map(RowMove::Select),
        Step::Up => position
            .checked_sub(1)
            .and_then(|previous| rows.get(previous))
            .copied()
            .map(RowMove::Select),
        Step::Left if has_children && !collapsed.contains(&current) => {
            Some(RowMove::Collapse(current))
        }
        Step::Left => node.parent.map(RowMove::Select),
        Step::Right if has_children && collapsed.contains(&current) => {
            Some(RowMove::Expand(current))
        }
        Step::Right => node.children.first().copied().map(RowMove::Select),
    }
}

impl PlanView {
    pub(super) fn render_tree(&self, cx: &mut Context<Self>) -> AnyElement {
        let Some(model) = self.model() else {
            return div().into_any_element();
        };
        let border = cx.theme().border;

        let rows: Vec<AnyElement> = visible_rows(model, &self.collapsed)
            .into_iter()
            .filter_map(|id| model.node(id))
            .map(|node| self.render_tree_row(node, model.columns, cx))
            .collect();

        tagged("plan-tree")
            .size_full()
            .flex()
            .flex_col()
            .child(self.render_tree_header(model.columns, model.analyzed, cx))
            .child(
                tagged("plan-tree-rows")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .track_scroll(&self.tree_scroll)
                    .border_color(border)
                    .children(rows),
            )
            .into_any_element()
    }

    fn render_tree_header(
        &self,
        columns: PlanColumns,
        analyzed: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let cell = |id: &'static str,
                    label: String,
                    width: Rems,
                    right: bool,
                    tooltip: Option<(String, String)>|
         -> AnyElement {
            tagged(id)
                .flex_shrink_0()
                .w(width)
                .when(right, |cell| cell.flex().justify_end())
                .font_weight(FontWeight::BOLD)
                .when_some(tooltip, |cell, (title, body)| {
                    cell.tooltip(tip(title, Some(body.into())))
                })
                .child(label)
                .into_any_element()
        };

        tagged("plan-tree-header")
            .flex()
            .flex_none()
            .items_center()
            .h(ROW_HEIGHT)
            .px(Spacing::MD)
            .gap(Spacing::MD)
            .border_b_1()
            .border_color(theme.border)
            .font_family(dbflux_components::fonts::editor_family(cx))
            .text_size(CONDITION_FONT)
            .text_color(ChromeColors::strong(theme))
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .font_weight(FontWeight::BOLD)
                    .child(dbflux_i18n::t!("document.query_plan.tree.node")),
            )
            .when(columns.index, |header| {
                header.child(cell(
                    "plan-tree-header-index",
                    dbflux_i18n::t!("document.query_plan.tree.index"),
                    INDEX_WIDTH,
                    false,
                    Some((
                        dbflux_i18n::t!("document.query_plan.tooltip.index_title"),
                        dbflux_i18n::t!("document.query_plan.tooltip.index_body"),
                    )),
                ))
            })
            .when(columns.cost, |header| {
                header.child(cell(
                    "plan-tree-header-cost",
                    dbflux_i18n::t!("document.query_plan.tree.cost"),
                    COST_WIDTH,
                    true,
                    Some((
                        dbflux_i18n::t!("document.query_plan.tooltip.cost_title"),
                        dbflux_i18n::t!("document.query_plan.tooltip.cost_body"),
                    )),
                ))
            })
            .when(columns.self_cost, |header| {
                header.child(cell(
                    "plan-tree-header-self-cost",
                    dbflux_i18n::t!("document.query_plan.tree.self_cost"),
                    SELF_COST_WIDTH,
                    true,
                    Some((
                        dbflux_i18n::t!("document.query_plan.tooltip.self_cost_title"),
                        dbflux_i18n::t!("document.query_plan.tooltip.self_cost_column_body"),
                    )),
                ))
            })
            .when(columns.rows, |header| {
                header.child(cell(
                    "plan-tree-header-rows",
                    if analyzed {
                        dbflux_i18n::t!("document.query_plan.tree.est_rows")
                    } else {
                        dbflux_i18n::t!("document.query_plan.tree.rows")
                    },
                    ROWS_WIDTH,
                    true,
                    Some((
                        dbflux_i18n::t!("document.query_plan.tooltip.rows_title"),
                        dbflux_i18n::t!("document.query_plan.tooltip.rows_body"),
                    )),
                ))
            })
            .when(columns.actual_rows, |header| {
                header.child(cell(
                    "plan-tree-header-actual-rows",
                    dbflux_i18n::t!("document.query_plan.tree.actual_rows"),
                    ACTUAL_ROWS_WIDTH,
                    true,
                    Some((
                        dbflux_i18n::t!("document.query_plan.tooltip.actual_rows_title"),
                        dbflux_i18n::t!("document.query_plan.tooltip.actual_rows_body"),
                    )),
                ))
            })
            .when(columns.time, |header| {
                header.child(cell(
                    "plan-tree-header-time",
                    dbflux_i18n::t!("document.query_plan.tree.time"),
                    TIME_WIDTH,
                    true,
                    Some((
                        dbflux_i18n::t!("document.query_plan.tooltip.time_title"),
                        dbflux_i18n::t!("document.query_plan.tooltip.time_body"),
                    )),
                ))
            })
            .when(columns.loops, |header| {
                header.child(cell(
                    "plan-tree-header-loops",
                    dbflux_i18n::t!("document.query_plan.tree.loops"),
                    LOOPS_WIDTH,
                    true,
                    Some((
                        dbflux_i18n::t!("document.query_plan.tooltip.loops_title"),
                        dbflux_i18n::t!("document.query_plan.tooltip.loops_body"),
                    )),
                ))
            })
            .when(columns.self_time, |header| {
                header.child(cell(
                    "plan-tree-header-self-time",
                    dbflux_i18n::t!("document.query_plan.tree.self_time"),
                    SELF_COST_WIDTH,
                    true,
                    Some((
                        dbflux_i18n::t!("document.query_plan.tooltip.self_time_title"),
                        dbflux_i18n::t!("document.query_plan.tooltip.self_time_body"),
                    )),
                ))
            })
            .into_any_element()
    }

    fn render_tree_row(
        &self,
        node: &PlanNodeView,
        columns: PlanColumns,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let id = node.id;
        let theme = cx.theme();
        let strong = ChromeColors::strong(theme);
        let tint = ChromeColors::tint(theme);
        let hover = theme.secondary_hover;
        let selected = self.selected == Some(id);
        let figure_color = if node.is_marked() {
            theme.warning
        } else {
            strong
        };
        let font = dbflux_components::fonts::editor_family(cx);

        let numeric = |width: Rems, text: Option<String>, color| -> AnyElement {
            div()
                .flex_shrink_0()
                .w(width)
                .flex()
                .justify_end()
                .font_family(font.clone())
                .text_color(color)
                .child(text.unwrap_or_default())
                .into_any_element()
        };

        tagged(ElementId::Name(format!("plan-tree-row-{id}").into()))
            .flex()
            .items_center()
            .min_h(ROW_HEIGHT)
            .px(Spacing::MD)
            .gap(Spacing::MD)
            .text_size(FONT)
            .cursor_pointer()
            .border_l_2()
            .border_color(if selected {
                tint
            } else {
                gpui::transparent_black()
            })
            .when(selected, |row| row.bg(tint.opacity(0.10)))
            .hover(|row| row.bg(hover))
            .role(Role::Button)
            .aria_label(SharedString::from(node.operation.clone()))
            .on_click(cx.listener(move |this, _, window, cx| {
                window.focus(&this.focus_handle, cx);
                this.select(id, cx);
            }))
            .child(self.tree_node_cell(node, cx))
            .when(columns.index, |row| {
                row.child(
                    div()
                        .flex_shrink_0()
                        .w(INDEX_WIDTH)
                        .truncate()
                        .font_family(font.clone())
                        .text_color(strong)
                        .child(node.index.clone().unwrap_or_else(|| "–".to_string())),
                )
            })
            .when(columns.cost, |row| {
                row.child(numeric(
                    COST_WIDTH,
                    node.total_cost.map(format_cost),
                    figure_color,
                ))
            })
            .when(columns.self_cost, |row| {
                row.child(tree_self_cost_cell(node, cx))
            })
            .when(columns.rows, |row| {
                row.child(numeric(ROWS_WIDTH, node.rows.map(format_rows), strong))
            })
            .when(columns.actual_rows, |row| {
                row.child(numeric(
                    ACTUAL_ROWS_WIDTH,
                    node.actual_rows.map(format_rows),
                    strong,
                ))
            })
            .when(columns.time, |row| {
                row.child(numeric(
                    TIME_WIDTH,
                    node.actual_time.map(format_time),
                    figure_color,
                ))
            })
            .when(columns.loops, |row| {
                row.child(numeric(LOOPS_WIDTH, node.loops.map(format_loops), strong))
            })
            .when(columns.self_time, |row| {
                row.child(tree_self_cost_cell(node, cx))
            })
            .into_any_element()
    }

    /// The first cell of a row: the fold button, the operation and, under
    /// them, the key condition.
    fn tree_node_cell(&self, node: &PlanNodeView, cx: &mut Context<Self>) -> AnyElement {
        let muted = cx.theme().muted_foreground;
        let font = dbflux_components::fonts::editor_family(cx);

        div()
            .flex_1()
            .min_w_0()
            .flex()
            .flex_col()
            .py(Spacing::XS)
            .child(
                div()
                    .flex()
                    .items_center()
                    .pl(indent(node.depth))
                    .gap(Spacing::XS)
                    .child(self.tree_fold_button(node, cx))
                    .child(tree_title(node, cx)),
            )
            .when_some(node.key_condition.clone(), |cell, condition| {
                cell.child(
                    div()
                        .pl(indent(node.depth + 1))
                        .min_w_0()
                        .truncate()
                        .font_family(font)
                        .text_size(CONDITION_FONT)
                        .text_color(muted)
                        .child(format!("{} {}", condition.label, condition.value)),
                )
            })
            .into_any_element()
    }

    /// The chevron that folds a branch; a leaf has a blank of the same width.
    fn tree_fold_button(&self, node: &PlanNodeView, cx: &mut Context<Self>) -> AnyElement {
        let id = node.id;
        if node.children.is_empty() {
            return div().flex_shrink_0().w(INDENT).into_any_element();
        }

        let is_open = !self.collapsed.contains(&id);
        tagged(ElementId::Name(format!("plan-tree-toggle-{id}").into()))
            .flex()
            .flex_shrink_0()
            .items_center()
            .justify_center()
            .w(INDENT)
            .cursor_pointer()
            .role(Role::Button)
            .aria_label(SharedString::from(node.operation.clone()))
            .on_click(cx.listener(move |this, _, _, cx| {
                if !this.collapsed.remove(&id) {
                    this.collapsed.insert(id);
                }
                cx.notify();
            }))
            .child(
                Icon::new(if is_open {
                    AppIcon::ChevronDown
                } else {
                    AppIcon::ChevronRight
                })
                .size(ui(12.0))
                .color(cx.theme().muted_foreground),
            )
            .into_any_element()
    }
}

/// The operation with its icon, the relation it reads and the costliest
/// marker. The operation's tooltip is the driver's description.
fn tree_title(node: &PlanNodeView, cx: &App) -> AnyElement {
    let id = node.id;
    let theme = cx.theme();
    let (strong, muted) = (ChromeColors::strong(theme), theme.muted_foreground);
    let font = dbflux_components::fonts::editor_family(cx);

    tagged(ElementId::Name(
        format!("plan-tree-row-{id}-operation").into(),
    ))
    .flex()
    .items_center()
    .gap(Spacing::SM)
    .min_w_0()
    .child(Icon::new(node_icon(node)).size(ui(13.0)).color(muted))
    .child(div().text_color(strong).child(node.operation.clone()))
    .when_some(node.relation.clone(), |title, relation| {
        title.child(
            div()
                .font_family(font)
                .text_size(CONDITION_FONT)
                .text_color(muted)
                .child(format!(
                    "{} {relation}",
                    dbflux_i18n::t!("document.query_plan.tree.on_relation")
                )),
        )
    })
    .when_some(node.estimate_gap, |title, gap| {
        title.child(
            tagged(ElementId::Name(format!("plan-tree-row-{id}-gap").into()))
                .tooltip(tip(
                    dbflux_i18n::t!("document.query_plan.tooltip.gap_title"),
                    Some(super::diagram::gap_tooltip_body(node).into()),
                ))
                .child(Badge::new(format_ratio(gap), BadgeTone::Warning)),
        )
    })
    .when(node.is_slowest, |title| {
        title.child(
            tagged(ElementId::Name(
                format!("plan-tree-row-{id}-slowest").into(),
            ))
            .tooltip(tip(
                dbflux_i18n::t!("document.query_plan.tooltip.slowest_title"),
                Some(dbflux_i18n::t!("document.query_plan.tooltip.slowest_body").into()),
            ))
            .child(Badge::new(
                dbflux_i18n::t!("document.query_plan.node.slowest"),
                BadgeTone::Warning,
            )),
        )
    })
    .when(node.is_costliest, |title| {
        title.child(
            tagged(ElementId::Name(
                format!("plan-tree-row-{id}-costliest").into(),
            ))
            .tooltip(tip(
                dbflux_i18n::t!("document.query_plan.tooltip.costliest_title"),
                Some(dbflux_i18n::t!("document.query_plan.tooltip.costliest_body").into()),
            ))
            .child(Badge::new(
                dbflux_i18n::t!("document.query_plan.node.costliest"),
                BadgeTone::Warning,
            )),
        )
    })
    .when_some(node.description.clone(), |title, description| {
        title.tooltip(tip(node.operation.clone(), Some(description.into())))
    })
    .into_any_element()
}

/// The self cost cell: the share as a bar and a percentage.
fn tree_self_cost_cell(node: &PlanNodeView, cx: &App) -> AnyElement {
    let theme = cx.theme();
    let color = if node.is_marked() {
        theme.warning
    } else {
        ChromeColors::strong(theme)
    };
    let bar_color = if node.is_marked() {
        theme.warning
    } else {
        ChromeColors::tint(theme)
    };
    let font = dbflux_components::fonts::editor_family(cx);

    div()
        .flex_shrink_0()
        .w(SELF_COST_WIDTH)
        .flex()
        .items_center()
        .justify_end()
        .gap(Spacing::SM)
        .when_some(node.share, |cell, share| {
            cell.child(
                div()
                    .w(BAR_WIDTH)
                    .h(Spacing::XS)
                    .rounded(Radii::SM)
                    .bg(theme.border)
                    .child(
                        div()
                            .h_full()
                            .w(relative(share as f32))
                            .min_w(px(1.0))
                            .rounded(Radii::SM)
                            .bg(bar_color),
                    ),
            )
            .child(
                div()
                    .w(ui(48.0))
                    .flex()
                    .justify_end()
                    .font_family(font)
                    .text_color(color)
                    .child(format_share(share)),
            )
        })
        .into_any_element()
}
