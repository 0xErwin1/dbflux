use gpui::{Context, IntoElement, div};

use crate::query_builder::panel::QueryBuilderPanel;

/// Renders the Sort section of the Query Builder.
///
/// Shows an ordered list of sort entries. Each row has a direction toggle
/// button (ASC/DESC), up/down reorder buttons, and a remove button.
/// A footer row contains an "add sort" input and Add button.
pub fn render_sort(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::{Button, Input, completion_input_keys_wrapper};
    use dbflux_core::{OrderByMode, VisualSortDirection};
    use gpui::SharedString;
    use gpui::prelude::*;

    if panel.order_by_mode(cx) == OrderByMode::SortKeyOnly {
        return render_sort_key_only(panel, cx).into_any_element();
    }

    let sort_count = panel.sort_rows.len();
    let sort_rows = panel.sort_rows.clone();

    let mut container = div().flex().flex_col().gap_1();

    for (i, row) in sort_rows.iter().enumerate() {
        let dir_label = match row.direction {
            VisualSortDirection::Asc => "ASC",
            VisualSortDirection::Desc => "DESC",
        };

        let label = format!("{}.{}", row.source_alias, row.column);
        let can_move_up = i > 0;
        let can_move_down = i + 1 < sort_count;

        let row_div = div()
            .flex()
            .flex_row()
            .gap_1()
            .items_center()
            .child(div().flex_1().text_sm().child(SharedString::from(label)))
            .child(
                Button::new(("qb-sort-dir", i), dir_label)
                    .ghost()
                    .small()
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        this.toggle_sort_direction(i, cx);
                    })),
            )
            .child(
                Button::new(("qb-sort-up", i), "↑")
                    .ghost()
                    .small()
                    .disabled(!can_move_up)
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        if i > 0 {
                            this.reorder_sort(i, i - 1, cx);
                        }
                    })),
            )
            .child(
                Button::new(("qb-sort-dn", i), "↓")
                    .ghost()
                    .small()
                    .disabled(!can_move_down)
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        this.reorder_sort(i, i + 1, cx);
                    })),
            )
            .child(
                Button::new(("qb-rm-sort", i), "✕")
                    .ghost()
                    .small()
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        this.remove_sort(i, cx);
                    })),
            );

        container = container.child(row_div);
    }

    if let Some(add_state) = panel.add_sort_input_state.as_ref() {
        container = container.child(
            div()
                .flex()
                .flex_row()
                .gap_1()
                .items_center()
                .child(
                    completion_input_keys_wrapper(add_state).flex_1().child(
                        Input::new(add_state)
                            .small()
                            .w_full()
                            .placeholder("alias.column"),
                    ),
                )
                .child(
                    Button::new("qb-add-sort", "Add")
                        .small()
                        .on_click(cx.listener(|this, _event, _window, cx| {
                            if let Some(state) = this.add_sort_input_state.clone() {
                                let text = state.read(cx).value().trim().to_string();
                                if text.is_empty() {
                                    return;
                                }
                                let (alias, column) = match text.split_once('.') {
                                    Some((a, c)) => (a.trim().to_string(), c.trim().to_string()),
                                    None => (this.current_spec.source.alias.clone(), text.clone()),
                                };
                                this.add_sort(&alias, &column, cx);
                                state.update(cx, |s, cx| {
                                    s.set_value("", _window, cx);
                                });
                            }
                        })),
                ),
        );
    }

    container.into_any_element()
}

/// Renders the sort section for drivers that can order only on the sort key
/// (`OrderByMode::SortKeyOnly`, e.g. DynamoDB): a single ascending/descending
/// toggle with an inline hint, instead of the multi-column sort list.
fn render_sort_key_only(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::Button;
    use dbflux_core::VisualSortDirection;
    use gpui::SharedString;
    use gpui::prelude::*;
    use gpui_component::ActiveTheme;

    let direction = panel.sort_key_direction();
    let dir_label = match direction {
        VisualSortDirection::Asc => "ASC",
        VisualSortDirection::Desc => "DESC",
    };

    let sort_key_label = panel
        .sort_key_column(cx)
        .filter(|name| !name.is_empty())
        .map(|name| format!("Sort key order ({name})"))
        .unwrap_or_else(|| "Sort key order".to_string());

    div()
        .flex()
        .flex_col()
        .gap_1()
        .child(
            div()
                .flex()
                .flex_row()
                .gap_1()
                .items_center()
                .child(
                    div()
                        .flex_1()
                        .text_sm()
                        .child(SharedString::from(sort_key_label)),
                )
                .child(
                    Button::new("qb-sortkey-dir", dir_label)
                        .ghost()
                        .small()
                        .on_click(cx.listener(move |this, _event, _window, cx| {
                            let next = match this.sort_key_direction() {
                                VisualSortDirection::Asc => VisualSortDirection::Desc,
                                VisualSortDirection::Desc => VisualSortDirection::Asc,
                            };
                            this.set_sort_key_direction(next, cx);
                        })),
                ),
        )
        .child(
            div()
                .text_xs()
                .text_color(cx.theme().muted_foreground)
                .child("Ordering is limited to the sort key for this driver."),
        )
}
