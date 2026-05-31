use gpui::{Context, IntoElement, div};

use crate::query_builder::panel::{FILTER_DEPTH_CAP, QueryBuilderPanel};

/// Renders the Filters section of the Query Builder.
///
/// Displays the recursive AND/OR group tree. The root group is rendered at
/// depth 1. Nesting is blocked when depth reaches `FILTER_DEPTH_CAP` (6).
pub fn render_filters(
    panel: &mut QueryBuilderPanel,
    _cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use gpui::SharedString;
    use gpui::prelude::*;

    let filter_depth = panel.current_spec.filter.as_ref().map_or(0, |f| f.depth());

    div()
        .flex()
        .flex_col()
        .p_2()
        .child(div().text_sm().child(SharedString::from("Filters")))
        .when(filter_depth >= FILTER_DEPTH_CAP, |this| {
            this.child(div().text_sm().child(SharedString::from(
                "Maximum filter nesting depth reached (6 levels)",
            )))
        })
        .child(match &panel.current_spec.filter {
            None => div().child(div().text_sm().child(SharedString::from("No filters"))),
            Some(node) => render_filter_node_preview(node),
        })
}

fn render_filter_node_preview(node: &dbflux_core::FilterNode) -> gpui::Div {
    use dbflux_core::FilterNode;
    use gpui::SharedString;
    use gpui::prelude::*;

    match node {
        FilterNode::Group { op, children } => {
            let label = match op {
                dbflux_core::BoolOp::And => "AND",
                dbflux_core::BoolOp::Or => "OR",
            };
            div()
                .flex()
                .flex_col()
                .child(div().text_sm().child(SharedString::from(label)))
                .children(
                    children
                        .iter()
                        .map(render_filter_node_preview)
                        .collect::<Vec<_>>(),
                )
        }
        FilterNode::Predicate(p) => {
            div()
                .flex()
                .flex_row()
                .child(div().text_sm().child(SharedString::from(format!(
                    "{}.{} {:?}",
                    p.source_alias, p.column, p.comparator
                ))))
        }
    }
}
