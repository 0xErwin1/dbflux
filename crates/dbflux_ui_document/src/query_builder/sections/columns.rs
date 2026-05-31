use gpui::{Context, IntoElement, div};

use crate::query_builder::panel::{ProjectionMode, QueryBuilderPanel};

/// Renders the Columns section of the Query Builder.
///
/// Shows an "All columns (*)" checkbox; when unchecked, lists each available
/// source-table column with its own checkbox so the user can toggle individual
/// projections. A free-text "alias.column" + Add row remains below for
/// columns from joined tables that are not in the source's column list.
pub fn render_columns(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::{Button, Checkbox, Input};
    use gpui::SharedString;
    use gpui::prelude::*;

    let all_active = panel.projection_mode == ProjectionMode::All;
    let source_alias = panel.current_spec.source.alias.clone();
    let available_columns = panel.available_columns.clone();
    let selected_extras: Vec<(String, String)> = panel
        .projection_rows
        .iter()
        .filter(|r| !(r.source_alias == source_alias && available_columns.contains(&r.column)))
        .map(|r| (r.source_alias.clone(), r.column.clone()))
        .collect();

    let mut container = div().flex().flex_col().gap_1().child(
        Checkbox::new("qb-all-columns")
            .checked(all_active)
            .label("All columns (*)")
            .on_click(cx.listener(|this, checked, _window, cx| {
                this.set_all_columns(*checked, cx);
            })),
    );

    if all_active {
        return container;
    }

    let columns_grid = available_columns.chunks(2).enumerate().fold(
        div().flex().flex_col().gap_1(),
        |grid, (chunk_ix, chunk)| {
            let mut row = div().flex().flex_row().gap_2();
            for (within_ix, col_name) in chunk.iter().enumerate() {
                let i = chunk_ix * 2 + within_ix;
                let alias_for_listener = source_alias.clone();
                let column_for_listener = col_name.clone();
                let checked = panel.is_column_selected(&source_alias, col_name);

                row = row.child(
                    div().flex_1().min_w(gpui::px(0.0)).child(
                        Checkbox::new(("qb-col-toggle", i))
                            .checked(checked)
                            .label(col_name.clone())
                            .on_click(cx.listener(move |this, _checked, _window, cx| {
                                this.toggle_column(&alias_for_listener, &column_for_listener, cx);
                            })),
                    ),
                );
            }
            // Pad single-item row so layout stays balanced.
            if chunk.len() == 1 {
                row = row.child(div().flex_1());
            }
            grid.child(row)
        },
    );
    container = container.child(columns_grid);

    for (i, (alias, column)) in selected_extras.iter().enumerate() {
        let label = format!("{}.{}", alias, column);
        let alias_for_listener = alias.clone();
        let column_for_listener = column.clone();
        container = container.child(
            div()
                .flex()
                .flex_row()
                .gap_1()
                .items_center()
                .child(div().flex_1().text_sm().child(SharedString::from(label)))
                .child(
                    Button::new(("qb-rm-extra-col", i), "✕")
                        .ghost()
                        .small()
                        .on_click(cx.listener(move |this, _event, _window, cx| {
                            this.toggle_column(&alias_for_listener, &column_for_listener, cx);
                        })),
                ),
        );
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
                                let text = state.read(cx).value().trim().to_string();
                                if text.is_empty() {
                                    return;
                                }
                                let (alias, column) = match text.split_once('.') {
                                    Some((a, c)) => (a.trim().to_string(), c.trim().to_string()),
                                    None => (this.current_spec.source.alias.clone(), text.clone()),
                                };
                                this.add_column(&alias, &column, cx);
                                state.update(cx, |s, cx| {
                                    s.set_value("", _window, cx);
                                });
                            }
                        })),
                ),
        );
    }

    container
}
