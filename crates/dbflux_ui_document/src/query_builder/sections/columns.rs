use gpui::{Context, IntoElement, div};

use crate::query_builder::panel::{ProjectionMode, QueryBuilderPanel};

/// Renders the Columns section of the Query Builder.
///
/// Shows an "All columns (*)" toggle and, when in Explicit mode, a list of
/// selected columns with add/remove/reorder controls.
pub fn render_columns(
    panel: &mut QueryBuilderPanel,
    _cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use gpui::SharedString;
    use gpui::prelude::*;

    let all_active = panel.projection_mode == ProjectionMode::All;

    div()
        .flex()
        .flex_col()
        .p_2()
        .child(
            div()
                .flex()
                .flex_row()
                .gap_2()
                .child(div().text_sm().child(SharedString::from("Columns")))
                .child(div().text_sm().child(SharedString::from(if all_active {
                    "All (*)"
                } else {
                    "Explicit"
                }))),
        )
        .when(!all_active, |this| {
            this.child(
                div().flex().flex_col().children(
                    panel
                        .projection_rows
                        .iter()
                        .map(|row| {
                            div().flex().flex_row().gap_1().child(div().text_sm().child(
                                SharedString::from(format!("{}.{}", row.source_alias, row.column)),
                            ))
                        })
                        .collect::<Vec<_>>(),
                ),
            )
        })
}
