use gpui::{Context, IntoElement, div};

use crate::query_builder::panel::{FkLoadState, QueryBuilderPanel};

/// Renders the Joins section of the Query Builder.
///
/// Each join row shows the join kind dropdown (INNER/LEFT/RIGHT/FULL), the
/// target table input, and a remove button. The ON clause uses structured
/// conditions by default (a list of `left <op> right` rows joined with AND),
/// or shows raw FK / free-text when the row is in a non-structured mode.
///
/// A banner appears when FK metadata is unavailable.
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
    let cond_lefts = panel.join_cond_left_inputs.clone();
    let cond_rights = panel.join_cond_right_inputs.clone();
    let cond_ops = panel.join_cond_op_dropdowns.clone();

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
        let mut join_block = div().flex().flex_col().gap_1();

        // Header row: kind dropdown + to_table input + × remove.
        let mut header = div().flex().flex_row().gap_1().items_center();

        if let Some(dropdown) = kind_dropdowns.get(i).cloned() {
            use dbflux_components::tokens::{Heights, Radii};
            use gpui_component::ActiveTheme;
            let theme = cx.theme();
            header = header.child(
                div()
                    .w(gpui::px(80.0))
                    .h(Heights::BUTTON)
                    .flex_shrink_0()
                    .rounded(Radii::SM)
                    .border_1()
                    .border_color(theme.input)
                    .bg(theme.background)
                    .child(dropdown),
            );
        }

        if let Some((to_table_state, _on_expr_state)) = join_states.get(i) {
            header = header.child(
                div().flex_1().min_w(gpui::px(0.0)).child(
                    Input::new(to_table_state)
                        .small()
                        .w_full()
                        .placeholder("table"),
                ),
            );
        } else {
            header = header.child(
                div()
                    .flex_1()
                    .text_sm()
                    .child(SharedString::from(row.to_table.clone())),
            );
        }

        header = header.child(
            Button::new(("qb-rm-join", i), "✕")
                .ghost()
                .small()
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    this.remove_join(i, cx);
                })),
        );

        join_block = join_block.child(header);

        // ON body — switches on JoinOn variant.
        match &row.on {
            JoinOn::Conditions(preds) => {
                for (cond_ix, pred) in preds.iter().enumerate() {
                    let id = pred.node_id;
                    let left = cond_lefts.get(&id).cloned();
                    let right = cond_rights.get(&id).cloned();
                    let op_dd = cond_ops.get(&id).cloned();

                    let prefix = if cond_ix == 0 { "ON" } else { "AND" };
                    let mut cond_row = div().flex().flex_row().gap_1().items_center().pl_2().child(
                        div()
                            .w(gpui::px(32.0))
                            .flex_shrink_0()
                            .text_sm()
                            .child(SharedString::from(prefix)),
                    );

                    if let Some(state) = left {
                        cond_row = cond_row.child(
                            div().flex_1().min_w(gpui::px(0.0)).child(
                                Input::new(&state)
                                    .small()
                                    .w_full()
                                    .placeholder("alias.column"),
                            ),
                        );
                    }

                    if let Some(dd) = op_dd {
                        use dbflux_components::tokens::{Heights, Radii};
                        use gpui_component::ActiveTheme;
                        let theme = cx.theme();
                        cond_row = cond_row.child(
                            div()
                                .w(gpui::px(76.0))
                                .h(Heights::BUTTON)
                                .flex_shrink_0()
                                .rounded(Radii::SM)
                                .border_1()
                                .border_color(theme.input)
                                .bg(theme.background)
                                .child(dd),
                        );
                    }

                    if let Some(state) = right {
                        cond_row = cond_row.child(
                            div().flex_1().min_w(gpui::px(0.0)).child(
                                Input::new(&state)
                                    .small()
                                    .w_full()
                                    .placeholder("alias.column"),
                            ),
                        );
                    }

                    let node_id_for_rm = id;
                    cond_row = cond_row.child(
                        Button::new(("qb-rm-cond", id as usize), "✕")
                            .ghost()
                            .small()
                            .on_click(cx.listener(move |this, _event, _window, cx| {
                                this.remove_join_condition(node_id_for_rm, cx);
                            })),
                    );

                    join_block = join_block.child(cond_row);
                }

                let row_idx = i;
                join_block = join_block.child(
                    div().flex().flex_row().pl_2().child(
                        Button::new(("qb-add-cond", i), "+ Condition")
                            .ghost()
                            .small()
                            .on_click(cx.listener(move |this, _event, _window, cx| {
                                this.add_join_condition(row_idx, cx);
                            })),
                    ),
                );
            }

            JoinOn::FkPath {
                from_column,
                to_column,
            } => {
                let on_text = format!(
                    "{}.{} = {}.{}",
                    row.from_alias, from_column, row.to_alias, to_column
                );
                join_block = join_block.child(
                    div()
                        .pl_2()
                        .text_sm()
                        .child(SharedString::from(format!("ON {on_text}"))),
                );
            }

            JoinOn::RawExpression(expr) => {
                if let Some((_to_table_state, on_expr_state)) = join_states.get(i) {
                    let mut raw_row = div().flex().flex_row().gap_1().items_center().pl_2();
                    raw_row = raw_row.child(
                        div()
                            .w(gpui::px(32.0))
                            .flex_shrink_0()
                            .text_sm()
                            .child(SharedString::from("ON")),
                    );
                    raw_row = raw_row.child(
                        div().flex_1().min_w(gpui::px(0.0)).child(
                            Input::new(on_expr_state)
                                .small()
                                .w_full()
                                .placeholder("a.id = b.a_id"),
                        ),
                    );
                    join_block = join_block.child(raw_row);
                    let _ = expr;
                }
            }
        }

        container = container.child(join_block);
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
