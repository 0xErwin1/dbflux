use gpui::{AnyElement, Context, ElementId, Entity, IntoElement, SharedString, div};

use crate::query_builder::panel::{FILTER_DEPTH_CAP, QueryBuilderPanel};
use dbflux_components::controls::InputState;

/// Renders the Filters section of the Query Builder.
///
/// Displays a recursive AND/OR group tree. Each group node shows:
/// - an AND/OR toggle button
/// - "+Filter" and "+Group" buttons (disabled at the depth cap)
/// - each child predicate with a comparator cycle button, a value input, and a
///   remove button
/// - each child sub-group rendered recursively
///
/// The root container exposes the same controls so the user can add predicates
/// to the top-level when no filter exists yet.
pub fn render_filters(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::Button;
    use gpui::SharedString;
    use gpui::prelude::*;

    let filter_depth = panel.current_spec.filter.as_ref().map_or(0, |f| f.depth());

    let source_alias = panel.current_spec.source.alias.clone();
    let source_alias_for_group = source_alias.clone();

    let mut container = div()
        .flex()
        .flex_col()
        .p_2()
        .gap_1()
        .child(div().text_sm().child(SharedString::from("Filters")))
        .when(filter_depth >= FILTER_DEPTH_CAP, |this| {
            this.child(div().text_sm().child(SharedString::from(
                "Maximum filter nesting depth reached (6 levels)",
            )))
        });

    match panel.current_spec.filter.clone() {
        None => {
            container = container.child(
                div()
                    .flex()
                    .flex_row()
                    .gap_1()
                    .items_center()
                    .child(
                        div()
                            .flex_1()
                            .text_sm()
                            .child(SharedString::from("No filters")),
                    )
                    .child(
                        Button::new("qb-add-first-pred", "+ Filter")
                            .ghost()
                            .small()
                            .on_click(cx.listener(move |this, _event, _window, cx| {
                                this.add_predicate(vec![], &source_alias.clone(), "column", cx);
                            })),
                    )
                    .child(
                        Button::new("qb-add-first-group", "+ Group")
                            .ghost()
                            .small()
                            .on_click(cx.listener(|this, _event, _window, cx| {
                                this.add_group(vec![], cx);
                            })),
                    ),
            );
        }

        Some(root) => {
            let input_states = panel.predicate_input_states.clone();
            let root_element =
                render_filter_node(root, vec![], &source_alias_for_group, &input_states, cx);
            container = container.child(root_element);
        }
    }

    container
}

fn render_filter_node(
    node: dbflux_core::FilterNode,
    path: Vec<usize>,
    source_alias: &str,
    input_states: &std::collections::HashMap<u64, Entity<InputState>>,
    cx: &mut Context<QueryBuilderPanel>,
) -> AnyElement {
    use dbflux_core::FilterNode;
    use gpui::prelude::*;

    match node {
        FilterNode::Group { op, children } => {
            render_filter_group(op, children, path, source_alias, input_states, cx)
                .into_any_element()
        }

        FilterNode::Predicate(pred) => {
            let input_state = input_states.get(&pred.node_id).cloned();
            render_filter_predicate(pred, path, input_state, cx).into_any_element()
        }
    }
}

fn render_filter_group(
    op: dbflux_core::BoolOp,
    children: Vec<dbflux_core::FilterNode>,
    path: Vec<usize>,
    source_alias: &str,
    input_states: &std::collections::HashMap<u64, Entity<InputState>>,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::Button;
    use gpui::SharedString;
    use gpui::prelude::*;

    let op_label = match op {
        dbflux_core::BoolOp::And => "AND",
        dbflux_core::BoolOp::Or => "OR",
    };

    let at_depth_cap = path.len() >= FILTER_DEPTH_CAP;
    let path_for_toggle = path.clone();
    let path_for_add_pred = path.clone();
    let path_for_add_group = path.clone();
    let path_for_remove = path.clone();
    let source_alias_for_pred = source_alias.to_string();

    let mut group_div = div().flex().flex_col().gap_1().pl_2().child(
        div()
            .flex()
            .flex_row()
            .gap_1()
            .items_center()
            .child(
                Button::new(path_id("qb-grp-op", &path_for_toggle), op_label)
                    .ghost()
                    .small()
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        this.toggle_group_op(path_for_toggle.clone(), cx);
                    })),
            )
            .child(
                Button::new(path_id("qb-grp-add-pred", &path_for_add_pred), "+ Filter")
                    .ghost()
                    .small()
                    .disabled(at_depth_cap)
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        this.add_predicate(
                            path_for_add_pred.clone(),
                            &source_alias_for_pred.clone(),
                            "column",
                            cx,
                        );
                    })),
            )
            .child(
                Button::new(path_id("qb-grp-add-grp", &path_for_add_group), "+ Group")
                    .ghost()
                    .small()
                    .disabled(at_depth_cap)
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        this.add_group(path_for_add_group.clone(), cx);
                    })),
            )
            .when(!path.is_empty(), |this| {
                this.child(
                    Button::new(path_id("qb-grp-rm", &path_for_remove), "✕")
                        .ghost()
                        .small()
                        .on_click(cx.listener(move |this, _event, _window, cx| {
                            this.remove_filter_node(path_for_remove.clone(), cx);
                        })),
                )
            }),
    );

    for (i, child) in children.into_iter().enumerate() {
        let mut child_path = path.clone();
        child_path.push(i);
        let child_element = render_filter_node(child, child_path, source_alias, input_states, cx);
        group_div = group_div.child(child_element);
    }

    group_div
}

fn render_filter_predicate(
    pred: dbflux_core::Predicate,
    path: Vec<usize>,
    input_state: Option<Entity<InputState>>,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::{Button, Input};
    use gpui::SharedString;
    use gpui::prelude::*;

    let comparator_label = comparator_label(pred.comparator);
    let col_label = format!("{}.{}", pred.source_alias, pred.column);
    let path_for_cmp = path.clone();
    let path_for_rm = path.clone();

    let needs_value = !matches!(
        pred.comparator,
        dbflux_core::Comparator::IsNull | dbflux_core::Comparator::IsNotNull
    );

    let mut row = div()
        .flex()
        .flex_row()
        .gap_1()
        .items_center()
        .child(
            div()
                .flex_shrink_0()
                .text_sm()
                .child(SharedString::from(col_label)),
        )
        .child(
            Button::new(path_id("qb-pred-cmp", &path_for_cmp), comparator_label)
                .ghost()
                .small()
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    this.cycle_predicate_comparator(path_for_cmp.clone(), cx);
                })),
        );

    if needs_value {
        if let Some(state) = input_state {
            row = row.child(Input::new(&state).small().w_full());
        } else {
            row = row.child(div().text_sm().child(SharedString::from("<value>")));
        }
    }

    row.child(
        Button::new(path_id("qb-pred-rm", &path_for_rm), "✕")
            .ghost()
            .small()
            .on_click(cx.listener(move |this, _event, _window, cx| {
                this.remove_filter_node(path_for_rm.clone(), cx);
            })),
    )
}

fn path_id(prefix: &str, path: &[usize]) -> ElementId {
    let key: String = std::iter::once(prefix.to_string())
        .chain(path.iter().map(|i| i.to_string()))
        .collect::<Vec<_>>()
        .join("-");
    ElementId::Name(SharedString::from(key))
}

fn comparator_label(cmp: dbflux_core::Comparator) -> &'static str {
    use dbflux_core::Comparator;
    match cmp {
        Comparator::Eq => "=",
        Comparator::Neq => "≠",
        Comparator::Gt => ">",
        Comparator::Lt => "<",
        Comparator::Gte => "≥",
        Comparator::Lte => "≤",
        Comparator::Like => "LIKE",
        Comparator::ILike => "ILIKE",
        Comparator::In => "IN",
        Comparator::IsNull => "IS NULL",
        Comparator::IsNotNull => "IS NOT NULL",
    }
}
