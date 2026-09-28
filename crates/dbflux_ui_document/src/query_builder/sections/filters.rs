use dbflux_components::primitives::{SegmentedControl, SegmentedItem};
use dbflux_components::tokens::{BuilderMetrics, ChromeColors, Fields};
use gpui::{AnyElement, Context, ElementId, Entity, IntoElement, SharedString, div};
use gpui_component::ActiveTheme;

use crate::labels::{bool_op_label, comparator_label};
use crate::query_builder::keyboard::row_id;
use crate::query_builder::panel::{FILTER_DEPTH_CAP, FilterTarget, QueryBuilderPanel};
use dbflux_components::composites::RailMark;
use dbflux_components::controls::{Dropdown, InputState};
use gpui_component::input::EditorState;

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

    let mark = panel.rail_mark.clone();
    let empty_row = row_id::filters_empty(target);

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
                this.child(div().text_sm().child(SharedString::from(dbflux_i18n::t!(
                    "document.query_builder.filters.max_depth",
                    depth = FILTER_DEPTH_CAP
                ))))
            });

    match tree {
        None => {
            container = container.child(
                mark.row(
                    &empty_row,
                    div()
                        .flex()
                        .flex_row()
                        .gap_1()
                        .items_center()
                        .child(
                            div()
                                .flex_1()
                                .text_sm()
                                .child(SharedString::from(dbflux_i18n::t!(
                                    "document.query_builder.filters.no_filters"
                                ))),
                        )
                        .child(
                            mark.ring_element(
                                &empty_row,
                                "add-filter",
                                Button::new(
                                    add_pred_id,
                                    dbflux_i18n::t!("document.query_builder.filters.add_filter"),
                                )
                                .ghost()
                                .inline()
                                .on_click(cx.listener(
                                    move |this, _event, _window, cx| {
                                        this.add_predicate_for(
                                            target,
                                            vec![],
                                            &source_alias.clone(),
                                            "",
                                            cx,
                                        );
                                    },
                                )),
                            ),
                        )
                        .child(
                            mark.ring_element(
                                &empty_row,
                                "add-group",
                                Button::new(
                                    add_group_id,
                                    dbflux_i18n::t!("document.query_builder.filters.add_subgroup"),
                                )
                                .ghost()
                                .inline()
                                .on_click(cx.listener(
                                    move |this, _event, _window, cx| {
                                        this.add_group_for(target, vec![], cx);
                                    },
                                )),
                            ),
                        ),
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
                &mark,
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
    column_input_states: &std::collections::HashMap<u64, Entity<EditorState>>,
    comparator_dropdowns: &std::collections::HashMap<u64, Entity<Dropdown>>,
    mark: &RailMark,
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
            mark,
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
                mark,
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
    column_input_states: &std::collections::HashMap<u64, Entity<EditorState>>,
    comparator_dropdowns: &std::collections::HashMap<u64, Entity<Dropdown>>,
    mark: &RailMark,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::Button;
    use gpui::SharedString;
    use gpui::prelude::*;

    let group_row = row_id::filter_group(target, &path);

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

    let theme = cx.theme().clone();
    let tint = ChromeColors::tint(&theme);

    let op_key = path.iter().fold(format!("{prefix}-op"), |key, index| {
        format!("{key}-{index}")
    });
    let op_id =
        move |key: &str, op: dbflux_core::BoolOp| SharedString::from(format!("{key}-{op:?}"));
    let weak = cx.weak_entity();
    let current_id = op_id(&op_key, op);
    let op_switch = SegmentedControl::new(
        [dbflux_core::BoolOp::And, dbflux_core::BoolOp::Or]
            .into_iter()
            .map(|op| SegmentedItem::new(op_id(&op_key, op), bool_op_label(op)))
            .collect(),
        current_id.clone(),
        move |id, _, cx| {
            if *id == current_id {
                return;
            }

            if let Some(builder) = weak.upgrade() {
                builder.update(cx, |this, cx| {
                    this.toggle_group_op_for(target, path_for_toggle.clone(), cx);
                });
            }
        },
    );

    let link = |id: ElementId, label: String, enabled: bool| {
        div()
            .id(id)
            .text_size(BuilderMetrics::LINK_FONT)
            .text_color(tint)
            .when(!enabled, |link| link.opacity(Fields::DISABLED_OPACITY))
            .when(enabled, |link| link.cursor_pointer())
            .child(label)
    };

    let add_links = div()
        .flex()
        .items_center()
        .gap(BuilderMetrics::CHIP_GAP)
        .child(
            mark.ring(
                &group_row,
                "add-filter",
                link(
                    path_id(&format!("{}-add-pred", prefix), &path_for_add_pred),
                    dbflux_i18n::t!("document.query_builder.filters.add_filter_link"),
                    !at_depth_cap,
                )
                .when(!at_depth_cap, |link| {
                    link.on_click(cx.listener(move |this, _event, _window, cx| {
                        this.add_predicate_for(
                            target,
                            path_for_add_pred.clone(),
                            &source_alias_for_pred.clone(),
                            "",
                            cx,
                        );
                    }))
                }),
            ),
        )
        .child(div().text_color(theme.muted_foreground).child("·"))
        .child(
            mark.ring(
                &group_row,
                "add-group",
                link(
                    path_id(&format!("{}-add-grp", prefix), &path_for_add_group),
                    dbflux_i18n::t!("document.query_builder.filters.add_group_link"),
                    !at_depth_cap,
                )
                .when(!at_depth_cap, |link| {
                    link.on_click(cx.listener(move |this, _event, _window, cx| {
                        this.add_group_for(target, path_for_add_group.clone(), cx);
                    }))
                }),
            ),
        );

    let mut group_div = div()
        .flex()
        .flex_col()
        .gap(BuilderMetrics::ROW_GAP)
        .when(!path.is_empty(), |group| {
            group.pl(BuilderMetrics::CARD_PADDING)
        })
        .child(
            mark.row(
                &group_row,
                div()
                    .flex()
                    .flex_row()
                    .gap(BuilderMetrics::ROW_GAP)
                    .items_center()
                    .child(mark.ring_element(&group_row, "op", op_switch))
                    .child(div().flex_1())
                    .when(!path.is_empty(), |this| {
                        this.child(remove_button(
                            path_id(&format!("{}-rm", prefix), &path_for_remove),
                            cx.listener(move |this, _event, _window, cx| {
                                this.remove_filter_node_for(target, path_for_remove.clone(), cx);
                            }),
                        ))
                    }),
            ),
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
            mark,
            cx,
        );
        group_div = group_div.child(child_element);
    }

    group_div.child(add_links)
}

#[allow(clippy::too_many_arguments)]
fn render_filter_predicate(
    pred: dbflux_core::Predicate,
    path: Vec<usize>,
    target: FilterTarget,
    input_state: Option<Entity<InputState>>,
    column_input_state: Option<Entity<EditorState>>,
    comparator_dropdown: Option<Entity<Dropdown>>,
    mark: &RailMark,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::{Button, Input};

    let predicate_row = row_id::filter_predicate(target, pred.node_id);
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

    let mut row = div()
        .flex()
        .flex_row()
        .gap(BuilderMetrics::ROW_GAP)
        .items_center();

    if let Some(col_state) = column_input_state {
        row = row.child(
            mark.ring(
                &predicate_row,
                "column",
                div().w(BuilderMetrics::FILTER_COLUMN_WIDTH).flex_shrink_0(),
            )
            .child(crate::completion_support::single_line_completion_editor(&col_state).w_full()),
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
        row = row.child(mark.ring(&predicate_row, "op", comparator_chip(dropdown, cx)));
    } else {
        row = row.child(
            div()
                .text_sm()
                .child(SharedString::from(comparator_label(pred.comparator))),
        );
    }

    if needs_value {
        if let Some(state) = input_state {
            row = row.child(
                mark.ring(&predicate_row, "value", div().flex_1())
                    .child(Input::new(&state).small().w_full()),
            );
        } else {
            row = row.child(div().text_sm().child(SharedString::from("<value>")));
        }
    }

    mark.row(
        &predicate_row,
        row.child(remove_button(
            path_id(rm_prefix, &path_for_rm),
            cx.listener(move |this, _event, _window, cx| {
                this.remove_filter_node_for(target, path_for_rm.clone(), cx);
            }),
        )),
    )
}

/// The remove control of a filter row or group: a muted circled X.
fn remove_button(
    id: ElementId,
    on_click: impl Fn(&gpui::ClickEvent, &mut gpui::Window, &mut gpui::App) + 'static,
) -> impl IntoElement {
    use dbflux_components::controls::Button;
    use dbflux_components::icons::AppIcon;

    Button::new(id, dbflux_i18n::t!("document.query_builder.filters.remove"))
        .ghost()
        .inline()
        .icon(AppIcon::CircleX)
        .icon_only()
        .tab_stop(false)
        .on_click(on_click)
}

/// Wraps a dropdown trigger in a bordered, themed chip so the selected
/// label and the chevron read as a single discrete control.
fn comparator_chip(dropdown: Entity<Dropdown>, _cx: &mut Context<QueryBuilderPanel>) -> gpui::Div {
    use gpui::prelude::*;

    div()
        .w(BuilderMetrics::FILTER_COMPARATOR_WIDTH)
        .flex_shrink_0()
        .child(dropdown)
}

fn path_id(prefix: &str, path: &[usize]) -> ElementId {
    let key: String = std::iter::once(prefix.to_string())
        .chain(path.iter().map(|i| i.to_string()))
        .collect::<Vec<_>>()
        .join("-");
    ElementId::Name(SharedString::from(key))
}
