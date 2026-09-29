use gpui::{Context, IntoElement, SharedString, div};
use gpui_component::ActiveTheme;

use dbflux_components::controls::{Button, ButtonVariant, Input};
use dbflux_components::tokens::{FontSizes, Spacing};
use dbflux_core::{AssignmentValue, ScalarLiteral};

use crate::labels::assignment_value_kind_label;
use crate::query_builder::keyboard::row_id;
use crate::query_builder::panel::QueryBuilderPanel;

/// Returns `true` when `value` is the `Expression` variant.
fn is_expression(value: &AssignmentValue) -> bool {
    matches!(value, AssignmentValue::Expression(_))
}

/// Cycle through `AssignmentValue` kinds: Literal → Expression → Null → Default → Literal.
pub(crate) fn cycle_value_kind(current: &AssignmentValue, current_text: &str) -> AssignmentValue {
    match current {
        AssignmentValue::Literal(_) => AssignmentValue::Expression(current_text.to_string()),
        AssignmentValue::Expression(_) => AssignmentValue::Null,
        AssignmentValue::Null => AssignmentValue::Default,
        AssignmentValue::Default => {
            AssignmentValue::Literal(ScalarLiteral::Text(current_text.to_string()))
        }
    }
}

/// Renders the SET assignments section.
///
/// Each row shows:
/// - A column name input
/// - A value input (shown for Literal and Expression kinds)
/// - A kind-cycle button (Literal / Raw SQL / NULL / DEFAULT)
/// - A remove button
///
/// `Raw SQL` mode renders a warning badge to surface the injection risk per
/// design §14.
///
/// The `InputState` objects for each row are stored in `QueryBuilderPanel.assign_col_inputs`
/// and `.assign_val_inputs`, keyed by row index. The panel rebuilds them when assignment
/// count changes via `rebuild_assign_inputs`.
pub fn render_assignments(
    panel: &mut QueryBuilderPanel,
    cx: &mut Context<QueryBuilderPanel>,
) -> impl IntoElement {
    use gpui::prelude::*;

    let theme = cx.theme().clone();

    let row_count = panel
        .mutation_state
        .as_ref()
        .map(|s| s.assignments.len())
        .unwrap_or(0);

    let mark = panel.rail_mark.clone();
    let mut container = div().flex().flex_col().gap_1();

    for row_ix in 0..row_count {
        let assignment_row = row_id::assignment(row_ix);
        let value = panel
            .mutation_state
            .as_ref()
            .and_then(|s| s.assignments.get(row_ix))
            .map(|r| r.assignment.value.clone())
            .unwrap_or(AssignmentValue::Null);

        let kind_label = assignment_value_kind_label(&value);
        let expr_mode = is_expression(&value);
        let kind_variant = if expr_mode {
            ButtonVariant::Danger
        } else {
            ButtonVariant::Secondary
        };

        let show_value_input = matches!(
            value,
            AssignmentValue::Literal(_) | AssignmentValue::Expression(_)
        );

        let mut row_div = mark
            .row(&assignment_row, div())
            .flex()
            .flex_row()
            .gap_1()
            .items_center();

        // Column name input
        if let Some(col_state) = panel.assign_col_inputs.get(&row_ix).cloned() {
            row_div = row_div.child(
                mark.ring(&assignment_row, "column", div())
                    .w(gpui::px(140.0))
                    .child(Input::new(&col_state).placeholder(dbflux_i18n::t!(
                        "document.query_builder.assignments.column_placeholder"
                    ))),
            );
        }

        // Value input (Literal / Expression only)
        if show_value_input {
            if let Some(val_state) = panel.assign_val_inputs.get(&row_ix).cloned() {
                row_div = row_div.child(mark.ring(&assignment_row, "value", div()).flex_1().child(
                    Input::new(&val_state).placeholder(dbflux_i18n::t!(
                        "document.query_builder.assignments.value_placeholder"
                    )),
                ));
            }
        } else {
            row_div = row_div.child(
                div()
                    .flex_1()
                    .text_size(FontSizes::SM)
                    .text_color(theme.muted_foreground)
                    .child(SharedString::from(kind_label.clone())),
            );
        }

        // Kind-cycle button
        row_div = row_div.child(
            mark.ring_element(
                &assignment_row,
                "kind",
                Button::new(("qb-assign-kind", row_ix), kind_label)
                    .inline()
                    .variant(kind_variant)
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        this.cycle_assignment_kind(row_ix, cx);
                    })),
            ),
        );

        // Remove button
        row_div = row_div.child(
            Button::new(("qb-assign-rm", row_ix), "×")
                .inline()
                .variant(ButtonVariant::Ghost)
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    this.remove_assignment(row_ix, cx);
                })),
        );

        container = container.child(row_div);

        // Expression-mode warning badge
        if expr_mode {
            container = container.child(
                div().flex().flex_row().gap_1().items_center().child(
                    div()
                        .px(Spacing::XS)
                        .py(gpui::px(2.0)) // guardrail-allow: 2px padding is not a standard spacing token
                        .rounded_sm()
                        .bg(theme.secondary)
                        .border_1()
                        .border_color(theme.border)
                        .text_size(FontSizes::XS)
                        .text_color(theme.danger)
                        .child(SharedString::from(dbflux_i18n::t!(
                            "document.query_builder.assignments.raw_sql_warning"
                        ))),
                ),
            );
        }
    }

    // "Add assignment" button
    container = container.child(
        mark.row(
            &row_id::assignment_add(),
            div().flex().child(
                Button::new(
                    "qb-assign-add",
                    dbflux_i18n::t!("document.query_builder.assignments.add_button"),
                )
                .variant(ButtonVariant::Ghost)
                .on_click(cx.listener(|this, _event, _window, cx| this.add_assignment(cx))),
            ),
        ),
    );

    container.into_any_element()
}
