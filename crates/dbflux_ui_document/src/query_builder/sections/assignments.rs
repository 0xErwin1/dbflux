use gpui::{Context, IntoElement, SharedString, div};
use gpui_component::ActiveTheme;

use dbflux_components::controls::{Button, ButtonVariant, Input};
use dbflux_components::tokens::{FontSizes, Spacing};
use dbflux_core::{Assignment, AssignmentValue, ScalarLiteral};

use crate::query_builder::mutation_state::AssignmentRow;
use crate::query_builder::panel::QueryBuilderPanel;

/// Returns the display label for an `AssignmentValue` kind selector button.
fn value_kind_label(value: &AssignmentValue) -> &'static str {
    match value {
        AssignmentValue::Literal(_) => "Literal",
        AssignmentValue::Expression(_) => "Raw SQL",
        AssignmentValue::Null => "NULL",
        AssignmentValue::Default => "DEFAULT",
    }
}

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

    let mut container = div().flex().flex_col().gap_1();

    for row_ix in 0..row_count {
        let value = panel
            .mutation_state
            .as_ref()
            .and_then(|s| s.assignments.get(row_ix))
            .map(|r| r.assignment.value.clone())
            .unwrap_or(AssignmentValue::Null);

        let kind_label = value_kind_label(&value);
        let expr_mode = is_expression(&value);
        let kind_variant = if expr_mode {
            ButtonVariant::Danger
        } else {
            ButtonVariant::Default
        };

        let show_value_input = matches!(
            value,
            AssignmentValue::Literal(_) | AssignmentValue::Expression(_)
        );

        let mut row_div = div().flex().flex_row().gap_1().items_center();

        // Column name input
        if let Some(col_state) = panel.assign_col_inputs.get(&row_ix).cloned() {
            row_div = row_div.child(
                div()
                    .w(gpui::px(140.0))
                    .child(Input::new(&col_state).placeholder("column")),
            );
        }

        // Value input (Literal / Expression only)
        if show_value_input {
            if let Some(val_state) = panel.assign_val_inputs.get(&row_ix).cloned() {
                row_div = row_div.child(
                    div()
                        .flex_1()
                        .child(Input::new(&val_state).placeholder("value")),
                );
            }
        } else {
            row_div = row_div.child(
                div()
                    .flex_1()
                    .text_size(FontSizes::SM)
                    .text_color(theme.muted_foreground)
                    .child(SharedString::from(kind_label)),
            );
        }

        // Kind-cycle button
        row_div = row_div.child(
            Button::new(("qb-assign-kind", row_ix), kind_label)
                .variant(kind_variant)
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    if let Some(state) = this.mutation_state.as_mut()
                        && let Some(row) = state.assignments.get_mut(row_ix)
                    {
                        let new_value = cycle_value_kind(&row.assignment.value, &row.raw_text);
                        row.assignment.value = new_value;
                    }
                    this.refresh_mutation_preview_pure();
                    cx.notify();
                })),
        );

        // Remove button
        row_div = row_div.child(
            Button::new(("qb-assign-rm", row_ix), "×")
                .variant(ButtonVariant::Ghost)
                .on_click(cx.listener(move |this, _event, _window, cx| {
                    if let Some(state) = this.mutation_state.as_mut()
                        && row_ix < state.assignments.len()
                    {
                        state.assignments.remove(row_ix);
                        this.pending_assign_rebuild = true;
                    }
                    this.refresh_mutation_preview_pure();
                    cx.notify();
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
                        .child(
                            "Raw SQL — expression is interpolated verbatim. Verify before running.",
                        ),
                ),
            );
        }
    }

    // "Add assignment" button
    container = container.child(
        Button::new("qb-assign-add", "+ Add assignment")
            .variant(ButtonVariant::Ghost)
            .on_click(cx.listener(|this, _event, _window, cx| {
                if let Some(state) = this.mutation_state.as_mut() {
                    state.assignments.push(AssignmentRow {
                        assignment: Assignment {
                            column: String::new(),
                            value: AssignmentValue::Literal(ScalarLiteral::Text(String::new())),
                        },
                        raw_text: String::new(),
                    });
                    this.pending_assign_rebuild = true;
                }
                this.refresh_mutation_preview_pure();
                cx.notify();
            })),
    );

    container.into_any_element()
}
