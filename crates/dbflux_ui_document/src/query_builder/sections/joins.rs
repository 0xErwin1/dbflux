use gpui::{Context, IntoElement, div};

use crate::query_builder::panel::{FkLoadState, JoinRow, QueryBuilderPanel};

/// Renders the Joins section of the Query Builder.
///
/// Each join row shows the join kind toggle (INNER/LEFT), a to-table input,
/// an ON expression input (or FK info when available), and a remove button.
/// A "+Join" button appends a new row.
///
/// A banner appears when FK metadata is unavailable. While loading, a spinner
/// label is shown next to existing rows.
pub fn render_joins(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use dbflux_components::controls::{Button, Input};
    use dbflux_core::JoinOn;
    use gpui::SharedString;
    use gpui::prelude::*;

    let show_banner =
        matches!(panel.fk_state, FkLoadState::Unavailable) && !panel.fk_banner_dismissed;
    let fk_loading = matches!(panel.fk_state, FkLoadState::Loading);

    let source_alias = panel.current_spec.source.alias.clone();
    let join_rows = panel.join_rows.clone();
    let join_states = panel.join_input_states.clone();
    let kind_dropdowns = panel.join_kind_dropdowns.clone();

    let mut container = div().flex().flex_col().gap_1();

    if show_banner {
        container = container.child(
            div()
                .flex()
                .flex_row()
                .gap_1()
                .items_start()
                .child(
                    div()
                        .flex_1()
                        .min_w(gpui::px(0.0))
                        .text_sm()
                        .child(SharedString::from(
                            "No foreign key metadata available. Enter conditions as raw expressions.",
                        )),
                )
                .child(
                    Button::new("qb-dismiss-fk-banner", "✕")
                        .ghost()
                        .small()
                        .on_click(cx.listener(|this, _event, _window, cx| {
                            this.dismiss_fk_banner(cx);
                        })),
                ),
        );
    }

    if fk_loading && !join_rows.is_empty() {
        container = container.child(
            div()
                .text_sm()
                .child(SharedString::from("Loading foreign keys…")),
        );
    }

    for (i, row) in join_rows.iter().enumerate() {
        let mut row_div = div().flex().flex_row().gap_1().items_center();

        if let Some(dropdown) = kind_dropdowns.get(i).cloned() {
            row_div = row_div.child(div().w(gpui::px(80.0)).flex_shrink_0().child(dropdown));
        } else {
            let kind_label = match row.kind {
                dbflux_core::JoinKind::Inner => "INNER",
                dbflux_core::JoinKind::Left => "LEFT",
                dbflux_core::JoinKind::Right => "RIGHT",
                dbflux_core::JoinKind::Full => "FULL",
            };
            row_div = row_div.child(div().text_sm().child(SharedString::from(kind_label)));
        }

        if let Some((to_table_state, on_expr_state)) = join_states.get(i) {
            let on_expr_is_fk = matches!(row.on, JoinOn::FkPath { .. });

            row_div = row_div
                .child(
                    div().flex_1().min_w(gpui::px(0.0)).child(
                        Input::new(to_table_state)
                            .small()
                            .w_full()
                            .placeholder("table"),
                    ),
                )
                .child(if on_expr_is_fk {
                    let on_text = match &row.on {
                        JoinOn::FkPath {
                            from_column,
                            to_column,
                        } => format!(
                            "{}.{} = {}.{}",
                            row.from_alias, from_column, row.to_alias, to_column
                        ),
                        JoinOn::RawExpression(expr) => expr.clone(),
                    };
                    div()
                        .flex_1()
                        .min_w(gpui::px(0.0))
                        .text_sm()
                        .child(SharedString::from(on_text))
                        .into_any_element()
                } else {
                    div()
                        .flex_1()
                        .min_w(gpui::px(0.0))
                        .child(
                            Input::new(on_expr_state)
                                .small()
                                .w_full()
                                .placeholder("a.id = b.a_id"),
                        )
                        .into_any_element()
                });
        } else {
            let on_text = match &row.on {
                JoinOn::FkPath {
                    from_column,
                    to_column,
                } => format!(
                    "{}.{} = {}.{}",
                    row.from_alias, from_column, row.to_alias, to_column
                ),
                JoinOn::RawExpression(expr) => {
                    if expr.is_empty() {
                        "ON <expression>".to_string()
                    } else {
                        expr.clone()
                    }
                }
            };
            row_div = row_div
                .child(
                    div()
                        .text_sm()
                        .child(SharedString::from(row.to_table.clone())),
                )
                .child(div().text_sm().child(SharedString::from(on_text)));
        }

        row_div = row_div.child(
            Button::new(("qb-rm-join", i), "✕")
                .ghost()
                .small()
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    this.remove_join(i, cx);
                })),
        );

        container = container.child(row_div);
    }

    container = container.child(
        Button::new("qb-add-join", "+ Join")
            .ghost()
            .small()
            .on_click(cx.listener(move |this, _event, _window, cx| {
                this.add_join(&source_alias.clone(), cx);
            })),
    );

    container
}
