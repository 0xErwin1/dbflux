use gpui::{Context, ElementId, IntoElement, SharedString, div};
use gpui_component::ActiveTheme;

use crate::query_builder::panel::{AggregateRow, GroupByRow, QueryBuilderPanel};

/// Renders the "Group By / Aggregates" section body.
///
/// Two sub-sections:
/// 1. Group-by rows: column text input + remove button, "+" button at bottom.
/// 2. Aggregate rows: function dropdown + column input + alias input + remove button.
pub fn render_group_by(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use crate::query_builder::panel::{AGG_FN_ORDER, agg_fn_display};
    use dbflux_components::controls::{Button, Input, completion_input_keys_wrapper};
    use dbflux_components::tokens::{Heights, Radii};
    use dbflux_core::AggFn;
    use gpui::prelude::*;
    use gpui_component::ActiveTheme;

    let group_by_rows: Vec<GroupByRow> = panel.group_by_rows.clone();
    let aggregate_rows: Vec<AggregateRow> = panel.aggregate_rows.clone();
    let gb_col_inputs = panel.group_by_col_inputs.clone();
    let agg_fn_dropdowns = panel.agg_fn_dropdowns.clone();
    let agg_col_inputs = panel.agg_col_inputs.clone();
    let agg_alias_inputs = panel.agg_alias_inputs.clone();

    let mut container = div().flex().flex_col().gap_1();

    // --- Group-by sub-section ---
    container = container.child(
        div()
            .text_sm()
            .text_color(cx.theme().muted_foreground)
            .child(SharedString::from("Group by columns")),
    );

    for (i, _row) in group_by_rows.iter().enumerate() {
        let mut row_div = div().flex().flex_row().gap_1().items_center();

        if let Some(col_input) = gb_col_inputs.get(i).cloned() {
            row_div = row_div.child(
                completion_input_keys_wrapper(&col_input)
                    .flex_1()
                    .min_w(gpui::px(0.0))
                    .child(
                        Input::new(&col_input)
                            .small()
                            .w_full()
                            .placeholder("alias.column"),
                    ),
            );
        }

        row_div = row_div.child(
            Button::new(element_id("qb-gb-rm", i), "✕")
                .ghost()
                .small()
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    this.remove_group_by_row(i, cx);
                })),
        );

        container = container.child(row_div);
    }

    let source_alias = panel.current_spec.source.alias.clone();
    container = container.child(
        Button::new("qb-gb-add", "+ Group-by column")
            .ghost()
            .small()
            .on_click(cx.listener(move |this, _event, _window, cx| {
                this.add_group_by_column(source_alias.clone(), String::new(), cx);
            })),
    );

    // --- Aggregate sub-section ---
    container = container.child(
        div()
            .text_sm()
            .text_color(cx.theme().muted_foreground)
            .child(SharedString::from("Aggregates")),
    );

    for (i, row) in aggregate_rows.iter().enumerate() {
        let is_count_star = row.function == AggFn::CountStar;
        let theme = cx.theme().clone();

        let mut row_div = div().flex().flex_row().gap_1().items_center();

        if let Some(fn_dd) = agg_fn_dropdowns.get(i).cloned() {
            row_div = row_div.child(
                div()
                    .w(gpui::px(110.0))
                    .h(Heights::BUTTON)
                    .flex_shrink_0()
                    .rounded(Radii::SM)
                    .border_1()
                    .border_color(theme.input)
                    .bg(theme.background)
                    .child(fn_dd),
            );
        }

        if let Some(col_input) = agg_col_inputs.get(i).cloned() {
            if is_count_star {
                row_div = row_div.child(
                    div()
                        .flex_1()
                        .min_w(gpui::px(0.0))
                        .text_sm()
                        .opacity(0.4)
                        .child(SharedString::from("*")),
                );
                let _ = col_input;
            } else {
                row_div = row_div.child(
                    completion_input_keys_wrapper(&col_input)
                        .flex_1()
                        .min_w(gpui::px(0.0))
                        .child(
                            Input::new(&col_input)
                                .small()
                                .w_full()
                                .placeholder("alias.column"),
                        ),
                );
            }
        }

        if let Some(alias_input) = agg_alias_inputs.get(i).cloned() {
            row_div = row_div.child(
                div().w(gpui::px(100.0)).flex_shrink_0().child(
                    Input::new(&alias_input)
                        .small()
                        .w_full()
                        .placeholder("alias"),
                ),
            );
        }

        row_div = row_div.child(
            Button::new(element_id("qb-agg-rm", i), "✕")
                .ghost()
                .small()
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    this.remove_aggregate_row(i, cx);
                })),
        );

        container = container.child(row_div);
    }

    let agg_add_items: Vec<(&'static str, AggFn)> = AGG_FN_ORDER
        .iter()
        .map(|f| (agg_fn_display(*f), *f))
        .collect();

    let mut add_row = div().flex().flex_row().gap_1().flex_wrap();
    for (label, function) in agg_add_items {
        add_row = add_row.child(
            Button::new(
                ElementId::Name(SharedString::from(format!("qb-agg-add-{}", label))),
                format!("+ {label}"),
            )
            .ghost()
            .small()
            .on_click(cx.listener(move |this, _event, _window, cx| {
                this.add_aggregate(function, cx);
            })),
        );
    }
    container = container.child(add_row);

    container
}

/// Renders the HAVING section body.
///
/// Delegates to the same filter-node renderer used by the WHERE section,
/// but bound to `FilterTarget::Having` so mutations land on `spec.having`.
pub fn render_having(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use super::filters::render_filters_for_target;
    use crate::query_builder::panel::FilterTarget;

    render_filters_for_target(panel, FilterTarget::Having, cx)
}

fn element_id(prefix: &str, index: usize) -> ElementId {
    ElementId::Name(SharedString::from(format!("{}-{}", prefix, index)))
}
