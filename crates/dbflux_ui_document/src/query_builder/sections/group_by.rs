use gpui::{Context, ElementId, IntoElement, SharedString, div};
use gpui_component::ActiveTheme;

use crate::query_builder::panel::{AggregateRow, GroupByRow, QueryBuilderPanel};

/// Renders the "Group By / Aggregates" section body.
///
/// Two sub-sections:
/// 1. Group-by rows: column label + remove button, "+" button at bottom.
/// 2. Aggregate rows: function label + column label + alias label + remove button.
///
/// Mutation actions route to the panel's group-by and aggregate mutation
/// methods. No GPUI entities are created here; all dropdowns and inputs for
/// this section are expected to be added in a future polish pass. For now,
/// the section renders the current state as labeled rows so the spec is
/// always reflected in the UI.
pub fn render_group_by(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::Button;
    use dbflux_core::AggFn;
    use gpui::prelude::*;

    let group_by_rows: Vec<GroupByRow> = panel.group_by_rows.clone();
    let aggregate_rows: Vec<AggregateRow> = panel.aggregate_rows.clone();

    let mut container = div().flex().flex_col().gap_1();

    // --- Group-by sub-section ---
    container = container.child(
        div()
            .text_sm()
            .text_color(cx.theme().muted_foreground)
            .child(SharedString::from("Group by columns")),
    );

    for (i, row) in group_by_rows.iter().enumerate() {
        let label = if row.source_alias.is_empty() || row.source_alias == row.column {
            row.column.clone()
        } else {
            format!("{}.{}", row.source_alias, row.column)
        };

        container = container.child(
            div()
                .flex()
                .flex_row()
                .gap_1()
                .items_center()
                .child(div().flex_1().text_sm().child(SharedString::from(label)))
                .child(
                    Button::new(element_id("qb-gb-rm", i), "✕")
                        .ghost()
                        .small()
                        .on_click(cx.listener(move |this, _event, _window, cx| {
                            this.remove_group_by_row(i, cx);
                        })),
                ),
        );
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
        let fn_label = agg_fn_label(row.function);
        let col_label = if row.function == AggFn::CountStar {
            "*".to_string()
        } else if row.column.is_empty() {
            "(no column)".to_string()
        } else {
            format!("{}.{}", row.source_alias, row.column)
        };
        let display = format!("{}({}) AS {}", fn_label, col_label, row.alias);

        container = container.child(
            div()
                .flex()
                .flex_row()
                .gap_1()
                .items_center()
                .child(div().flex_1().text_sm().child(SharedString::from(display)))
                .child(
                    Button::new(element_id("qb-agg-rm", i), "✕")
                        .ghost()
                        .small()
                        .on_click(cx.listener(move |this, _event, _window, cx| {
                            this.remove_aggregate_row(i, cx);
                        })),
                ),
        );
    }

    container = container.child(
        Button::new("qb-agg-add-count-star", "+ Count(*)")
            .ghost()
            .small()
            .on_click(cx.listener(|this, _event, _window, cx| {
                this.add_aggregate(AggFn::CountStar, cx);
            })),
    );

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

fn agg_fn_label(function: dbflux_core::AggFn) -> &'static str {
    use dbflux_core::AggFn;
    match function {
        AggFn::Count => "COUNT",
        AggFn::CountStar => "COUNT",
        AggFn::CountDistinct => "COUNT DISTINCT",
        AggFn::Sum => "SUM",
        AggFn::Avg => "AVG",
        AggFn::Min => "MIN",
        AggFn::Max => "MAX",
    }
}

fn element_id(prefix: &str, index: usize) -> ElementId {
    ElementId::Name(SharedString::from(format!("{}-{}", prefix, index)))
}
