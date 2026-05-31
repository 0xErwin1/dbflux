use gpui::{Context, IntoElement, div};

use crate::query_builder::panel::{FkLoadState, QueryBuilderPanel};

/// Renders the Joins section of the Query Builder.
///
/// Shows a banner when FK metadata is unavailable, and a list of join rows
/// each with from-alias, to-table, join kind, and ON condition controls.
///
/// While FK metadata is loading (`FkLoadState::Loading`), join rows show a
/// raw-expression input with a loading indicator. Once metadata arrives
/// (`FkLoadState::Ready`), FK-matching columns are offered in a dropdown.
pub fn render_joins(
    panel: &mut QueryBuilderPanel,
    _cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use gpui::SharedString;
    use gpui::prelude::*;

    let show_banner =
        matches!(panel.fk_state, FkLoadState::Unavailable) && !panel.fk_banner_dismissed;

    let fk_loading = matches!(panel.fk_state, FkLoadState::Loading);

    div()
        .flex()
        .flex_col()
        .p_2()
        .child(div().text_sm().child(SharedString::from("Joins")))
        .when(show_banner, |this| {
            this.child(div().flex().flex_row().gap_1().child(div().text_sm().child(
                SharedString::from(
                    "No foreign key metadata available. Add join conditions as raw expressions.",
                ),
            )))
        })
        .when(fk_loading && !panel.join_rows.is_empty(), |this| {
            this.child(
                div()
                    .text_sm()
                    .child(SharedString::from("Loading foreign keys…")),
            )
        })
        .children(
            panel
                .join_rows
                .iter()
                .map(|row| {
                    use dbflux_core::JoinOn;
                    use gpui::SharedString;

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

                    div()
                        .flex()
                        .flex_row()
                        .gap_1()
                        .p_1()
                        .child(div().text_sm().child(SharedString::from(format!(
                            "{:?} JOIN {} AS {} {}",
                            row.kind, row.to_table, row.to_alias, on_text
                        ))))
                })
                .collect::<Vec<_>>(),
        )
}
