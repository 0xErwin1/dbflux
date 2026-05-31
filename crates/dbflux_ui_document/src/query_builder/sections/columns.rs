use gpui::{Context, IntoElement, div};

use crate::query_builder::panel::{ProjectionMode, QueryBuilderPanel};

/// Renders the Columns section of the Query Builder.
///
/// Shows an "All columns (*)" checkbox and, when in Explicit mode, a list of
/// selected columns each with a remove button, plus an "add column" input.
pub fn render_columns(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::{Button, Checkbox, Input};
    use gpui::SharedString;
    use gpui::prelude::*;

    let all_active = panel.projection_mode == ProjectionMode::All;

    let mut container = div().flex().flex_col().gap_1().child(
        Checkbox::new("qb-all-columns")
            .checked(all_active)
            .label("All columns (*)")
            .on_click(cx.listener(|this, checked, _window, cx| {
                this.set_all_columns(*checked, cx);
            })),
    );

    if !all_active {
        let projection_rows = panel.projection_rows.clone();

        for (i, row) in projection_rows.iter().enumerate() {
            let label = format!("{}.{}", row.source_alias, row.column);
            let row_div =
                div()
                    .flex()
                    .flex_row()
                    .gap_1()
                    .items_center()
                    .child(div().flex_1().text_sm().child(SharedString::from(label)))
                    .child(Button::new(("qb-rm-col", i), "✕").ghost().small().on_click(
                        cx.listener(move |this, _event, _window, cx| {
                            this.remove_column(i, cx);
                        }),
                    ));

            container = container.child(row_div);
        }

        if let Some(add_state) = panel.add_column_input_state.as_ref() {
            container = container.child(
                div()
                    .flex()
                    .flex_row()
                    .gap_1()
                    .items_center()
                    .child(
                        Input::new(add_state)
                            .small()
                            .w_full()
                            .placeholder("alias.column"),
                    )
                    .child(
                        Button::new("qb-add-col", "Add")
                            .small()
                            .on_click(cx.listener(|this, _event, _window, cx| {
                                if let Some(state) = this.add_column_input_state.clone() {
                                    let text = state.read(cx).value().to_string();
                                    let parts: Vec<&str> = text.splitn(2, '.').collect();
                                    if parts.len() == 2 {
                                        this.add_column(parts[0], parts[1], cx);
                                    }
                                }
                            })),
                    ),
            );
        }
    }

    container
}
