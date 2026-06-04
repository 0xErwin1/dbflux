use gpui::{AnyElement, Context, ElementId, Entity, IntoElement, SharedString, div};

use crate::query_builder::panel::{FILTER_DEPTH_CAP, FilterTarget, QueryBuilderPanel};
use dbflux_components::controls::{Dropdown, InputState};

/// Renders the Filters section of the Query Builder (WHERE target).
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
    render_filters_for_target(panel, FilterTarget::Where, cx)
}

/// Parameterized filter renderer — renders either the WHERE or HAVING tree.
///
/// Routes all mutations through `add_predicate_for`, `remove_filter_node_for`,
/// etc., so the same predicate-tree UI serves both sections.
pub fn render_filters_for_target(
    panel: &mut QueryBuilderPanel,
    target: FilterTarget,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::Button;
    use gpui::SharedString;
    use gpui::prelude::*;

    let tree = match target {
        FilterTarget::Where => panel.current_spec.filter.clone(),
        FilterTarget::Having => panel.current_spec.having.clone(),
    };

    let filter_depth = tree.as_ref().map_or(0, |f| f.depth());

    let source_alias = match target {
        FilterTarget::Having => String::new(),
        FilterTarget::Where => panel.current_spec.source.alias.clone(),
    };
    let source_alias_for_group = source_alias.clone();

    let (add_pred_id, add_group_id): (ElementId, ElementId) = match target {
        FilterTarget::Where => (
            ElementId::Name(SharedString::from("qb-add-first-pred")),
            ElementId::Name(SharedString::from("qb-add-first-group")),
        ),
        FilterTarget::Having => (
            ElementId::Name(SharedString::from("qb-having-add-first-pred")),
            ElementId::Name(SharedString::from("qb-having-add-first-group")),
        ),
    };

    let (input_states, column_input_states, comparator_dropdowns) = match target {
        FilterTarget::Where => (
            panel.predicate_input_states.clone(),
            panel.predicate_column_input_states.clone(),
            panel.predicate_comparator_dropdowns.clone(),
        ),
        FilterTarget::Having => (
            panel.having_predicate_input_states.clone(),
            panel.having_predicate_column_input_states.clone(),
            panel.having_predicate_comparator_dropdowns.clone(),
        ),
    };

    let mut container =
        div()
            .flex()
            .flex_col()
            .gap_1()
            .when(filter_depth >= FILTER_DEPTH_CAP, |this| {
                this.child(div().text_sm().child(SharedString::from(
                    "Maximum filter nesting depth reached (6 levels)",
                )))
            });

    match tree {
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
                        Button::new(add_pred_id, "+ Filter")
                            .ghost()
                            .small()
                            .on_click(cx.listener(move |this, _event, _window, cx| {
                                this.add_predicate_for(
                                    target,
                                    vec![],
                                    &source_alias.clone(),
                                    "",
                                    cx,
                                );
                            })),
                    )
                    .child(
                        Button::new(add_group_id, "+ Sub-group")
                            .ghost()
                            .small()
                            .on_click(cx.listener(move |this, _event, _window, cx| {
                                this.add_group_for(target, vec![], cx);
                            })),
                    ),
            );
        }

        Some(root) => {
            let root_element = render_filter_node(
                root,
                vec![],
                target,
                &source_alias_for_group,
                &input_states,
                &column_input_states,
                &comparator_dropdowns,
                cx,
            );
            container = container.child(root_element);
        }
    }

    container
}

#[allow(clippy::too_many_arguments)]
fn render_filter_node(
    node: dbflux_core::FilterNode,
    path: Vec<usize>,
    target: FilterTarget,
    source_alias: &str,
    input_states: &std::collections::HashMap<u64, Entity<InputState>>,
    column_input_states: &std::collections::HashMap<u64, Entity<InputState>>,
    comparator_dropdowns: &std::collections::HashMap<u64, Entity<Dropdown>>,
    cx: &mut Context<QueryBuilderPanel>,
) -> AnyElement {
    use dbflux_core::FilterNode;
    use gpui::prelude::*;

    match node {
        FilterNode::Group { op, children } => render_filter_group(
            op,
            children,
            path,
            target,
            source_alias,
            input_states,
            column_input_states,
            comparator_dropdowns,
            cx,
        )
        .into_any_element(),

        FilterNode::Predicate(pred) => {
            let input_state = input_states.get(&pred.node_id).cloned();
            let column_input = column_input_states.get(&pred.node_id).cloned();
            let comparator_dropdown = comparator_dropdowns.get(&pred.node_id).cloned();
            render_filter_predicate(
                pred,
                path,
                target,
                input_state,
                column_input,
                comparator_dropdown,
                cx,
            )
            .into_any_element()
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn render_filter_group(
    op: dbflux_core::BoolOp,
    children: Vec<dbflux_core::FilterNode>,
    path: Vec<usize>,
    target: FilterTarget,
    source_alias: &str,
    input_states: &std::collections::HashMap<u64, Entity<InputState>>,
    column_input_states: &std::collections::HashMap<u64, Entity<InputState>>,
    comparator_dropdowns: &std::collections::HashMap<u64, Entity<Dropdown>>,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::Button;
    use gpui::SharedString;
    use gpui::prelude::*;

    let op_label = match op {
        dbflux_core::BoolOp::And => "AND",
        dbflux_core::BoolOp::Or => "OR",
    };

    let prefix = match target {
        FilterTarget::Where => "qb-grp",
        FilterTarget::Having => "qb-hav-grp",
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
                Button::new(
                    path_id(&format!("{}-op", prefix), &path_for_toggle),
                    op_label,
                )
                .ghost()
                .small()
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    this.toggle_group_op_for(target, path_for_toggle.clone(), cx);
                })),
            )
            .child(
                Button::new(
                    path_id(&format!("{}-add-pred", prefix), &path_for_add_pred),
                    "+ Filter",
                )
                .ghost()
                .small()
                .disabled(at_depth_cap)
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    this.add_predicate_for(
                        target,
                        path_for_add_pred.clone(),
                        &source_alias_for_pred.clone(),
                        "",
                        cx,
                    );
                })),
            )
            .child(
                Button::new(
                    path_id(&format!("{}-add-grp", prefix), &path_for_add_group),
                    "+ Sub-group",
                )
                .ghost()
                .small()
                .disabled(at_depth_cap)
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    this.add_group_for(target, path_for_add_group.clone(), cx);
                })),
            )
            .when(!path.is_empty(), |this| {
                this.child(
                    Button::new(path_id(&format!("{}-rm", prefix), &path_for_remove), "✕")
                        .ghost()
                        .small()
                        .on_click(cx.listener(move |this, _event, _window, cx| {
                            this.remove_filter_node_for(target, path_for_remove.clone(), cx);
                        })),
                )
            }),
    );

    for (i, child) in children.into_iter().enumerate() {
        let mut child_path = path.clone();
        child_path.push(i);
        let child_element = render_filter_node(
            child,
            child_path,
            target,
            source_alias,
            input_states,
            column_input_states,
            comparator_dropdowns,
            cx,
        );
        group_div = group_div.child(child_element);
    }

    group_div
}

fn render_filter_predicate(
    pred: dbflux_core::Predicate,
    path: Vec<usize>,
    target: FilterTarget,
    input_state: Option<Entity<InputState>>,
    column_input_state: Option<Entity<InputState>>,
    comparator_dropdown: Option<Entity<Dropdown>>,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::{Button, Input, completion_input_keys_wrapper};
    use gpui::SharedString;
    use gpui::prelude::*;

    let rm_prefix = match target {
        FilterTarget::Where => "qb-pred-rm",
        FilterTarget::Having => "qb-hav-pred-rm",
    };

    let path_for_rm = path.clone();

    let needs_value = !matches!(
        pred.comparator,
        dbflux_core::Comparator::IsNull | dbflux_core::Comparator::IsNotNull
    );

    let mut row = div().flex().flex_row().gap_1().items_center();

    if let Some(col_state) = column_input_state {
        row = row.child(
            completion_input_keys_wrapper(&col_state)
                .flex_1()
                .child(Input::new(&col_state).small().w_full()),
        );
    } else {
        let fallback = format!("{}.{}", pred.source_alias, pred.column);
        row = row.child(
            div()
                .flex_shrink_0()
                .text_sm()
                .child(SharedString::from(fallback)),
        );
    }

    if let Some(dropdown) = comparator_dropdown {
        row = row.child(comparator_chip(dropdown, cx));
    } else {
        row = row.child(
            div()
                .text_sm()
                .child(SharedString::from(comparator_label(pred.comparator))),
        );
    }

    if needs_value {
        if let Some(state) = input_state {
            row = row.child(div().flex_1().child(Input::new(&state).small().w_full()));
        } else {
            row = row.child(div().text_sm().child(SharedString::from("<value>")));
        }
    }

    row.child(
        Button::new(path_id(rm_prefix, &path_for_rm), "✕")
            .ghost()
            .small()
            .on_click(cx.listener(move |this, _event, _window, cx| {
                this.remove_filter_node_for(target, path_for_rm.clone(), cx);
            })),
    )
}

/// Wraps a dropdown trigger in a bordered, themed chip so the selected
/// label and the chevron read as a single discrete control.
fn comparator_chip(
    dropdown: Entity<Dropdown>,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::tokens::{Heights, Radii};
    use gpui::prelude::*;
    use gpui_component::ActiveTheme;

    let theme = cx.theme();
    div()
        .w(gpui::px(76.0))
        .h(Heights::BUTTON)
        .flex_shrink_0()
        .rounded(Radii::SM)
        .border_1()
        .border_color(theme.input)
        .bg(theme.background)
        .child(dropdown)
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
