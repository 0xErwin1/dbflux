use gpui::{Context, IntoElement, div};

use crate::query_builder::panel::QueryBuilderPanel;

/// Renders the Sort section of the Query Builder.
///
/// Shows an ordered list of sort entries, each with column name, direction
/// toggle, and reorder controls.
pub fn render_sort(
    panel: &mut QueryBuilderPanel,
    _cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_core::VisualSortDirection;
    use gpui::SharedString;
    use gpui::prelude::*;

    div()
        .flex()
        .flex_col()
        .p_2()
        .child(div().text_sm().child(SharedString::from("Sort")))
        .children(
            panel
                .sort_rows
                .iter()
                .map(|row| {
                    let dir_label = match row.direction {
                        VisualSortDirection::Asc => "ASC",
                        VisualSortDirection::Desc => "DESC",
                    };

                    div()
                        .flex()
                        .flex_row()
                        .gap_1()
                        .p_1()
                        .child(div().text_sm().child(SharedString::from(format!(
                            "{}.{} {}",
                            row.source_alias, row.column, dir_label
                        ))))
                })
                .collect::<Vec<_>>(),
        )
}
