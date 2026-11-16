//! The inline cell editor in expression mode: the text input behind an `ƒx`
//! marker, with the expressions the dialect offers for the column's type
//! listed under it.
//!
//! Up and Down step through the suggestions and put the chosen one in the
//! input, which stays editable; Enter commits whatever the input holds. The
//! list is drawn in a deferred layer so the cell's clipping does not cut it.

use gpui::prelude::FluentBuilder;
use gpui::{
    Anchor, AnyElement, Entity, InteractiveElement, IntoElement, KeyDownEvent, ParentElement,
    Pixels, SharedString, Styled, Window, anchored, deferred, div, point, px,
};
use gpui_component::Sizable;

use super::state::DataTableState;
use crate::controls::{GpuiInput as Input, InputState};
use crate::tokens::Spacing;
use bolt::components::grid::GridColors;

/// The marker drawn before the input while the editor is in expression mode.
pub(super) const EXPRESSION_MARKER: &str = "\u{192}x";

/// Placeholder of the input while it is in expression mode.
pub(super) fn expression_placeholder() -> SharedString {
    dbflux_i18n::t!("document.data.grid.expression.placeholder").into()
}

/// The element the editing cell draws in expression mode.
/// How far a key press moves the suggestion: plain Down and Up only, so a
/// shifted arrow stays a selection gesture of the input.
fn suggestion_step(keystroke: &gpui::Keystroke) -> Option<isize> {
    if keystroke.modifiers.shift {
        return None;
    }

    match keystroke.key.as_str() {
        "down" => Some(1),
        "up" => Some(-1),
        _ => None,
    }
}

pub(super) fn expression_editor(
    input: &Entity<InputState>,
    state: &Entity<DataTableState>,
    suggestions: &[String],
    selected: Option<usize>,
    colors: &GridColors,
    font_size: Pixels,
) -> AnyElement {
    let has_suggestions = !suggestions.is_empty();
    let state = state.clone();

    div()
        .id("cell-expression-editor")
        .relative()
        .flex()
        .items_center()
        .size_full()
        .gap(Spacing::XS)
        .when(has_suggestions, |editor| {
            editor.capture_key_down(move |event: &KeyDownEvent, window: &mut Window, cx| {
                let Some(step) = suggestion_step(&event.keystroke) else {
                    return;
                };

                cx.stop_propagation();
                state.update(cx, |state, cx| state.step_suggestion(step, window, cx));
            })
        })
        .child(
            div()
                .id("cell-expression-marker")
                .flex_shrink_0()
                .px(Spacing::XS)
                .ml(Spacing::XS)
                .italic()
                .text_size(font_size)
                .text_color(colors.tint)
                .bg(colors.tint.opacity(0.18))
                .child(EXPRESSION_MARKER),
        )
        .child(div().flex_1().min_w_0().child(Input::new(input).small()))
        .when(has_suggestions, |editor| {
            editor.child(suggestion_list(suggestions, selected, colors, font_size))
        })
        .into_any_element()
}

fn suggestion_list(
    suggestions: &[String],
    selected: Option<usize>,
    colors: &GridColors,
    font_size: Pixels,
) -> AnyElement {
    let rows = suggestions.iter().enumerate().map(|(index, expression)| {
        let is_selected = selected == Some(index);

        div()
            .id(("cell-expression-suggestion", index))
            .px(Spacing::SM)
            .py(px(2.0))
            .text_size(font_size)
            .text_color(if is_selected {
                colors.strong
            } else {
                colors.tint
            })
            .when(is_selected, |row| row.bg(colors.tint.opacity(0.18)))
            .child(expression.clone())
    });

    div()
        .absolute()
        .left_0()
        .top_full()
        .child(
            deferred(
                anchored()
                    .anchor(Anchor::TopLeft)
                    .offset(point(px(0.0), Spacing::XS))
                    .snap_to_window()
                    .child(
                        div()
                            .id("cell-expression-suggestions")
                            .min_w(px(220.0))
                            .flex()
                            .flex_col()
                            .py(Spacing::XS)
                            .bg(colors.background)
                            .border_1()
                            .border_color(colors.ring)
                            .occlude()
                            .children(rows),
                    ),
            )
            .with_priority(1),
        )
        .into_any_element()
}

#[cfg(test)]
mod tests;
