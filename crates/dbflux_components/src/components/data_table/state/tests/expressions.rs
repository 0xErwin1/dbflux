use super::*;

/// The cell at `coord` as the edit buffer holds it, if staged.
fn staged_cell(
    window: &mut gpui::VisualTestContext,
    state: &gpui::Entity<crate::components::data_table::state::DataTableState>,
    col: usize,
) -> Option<crate::components::data_table::model::CellValue> {
    window.update(|_, app| {
        state
            .read(app)
            .edit_buffer()
            .row_changes(0)
            .into_iter()
            .find(|(staged_col, _)| *staged_col == col)
            .map(|(_, value)| value.clone())
    })
}

/// S2: text typed in the default (literal) mode stays a literal even
/// when it looks like a function call.
#[gpui::test]
fn function_text_typed_in_literal_mode_stays_a_literal(cx: &mut gpui::TestAppContext) {
    use crate::components::data_table::selection::CellCoord;

    let (state, input, window) = editing_state(cx, CellCoord::new(0, 1));

    window.update(|window, app| {
        input.update(app, |input, cx| input.set_value("now()", window, cx));
    });
    window.update(|_, app| state.update(app, |s, cx| s.stop_editing(true, cx)));

    let staged = staged_cell(window, &state, 1).expect("the typed value is staged");
    assert_eq!(staged.as_expression(), None);
    assert_eq!(staged.to_value(), dbflux_core::Value::Text("now()".into()));
}

/// S1: with the editor flipped to expression mode, the typed text is
/// staged as an expression.
#[gpui::test]
fn text_typed_in_expression_mode_is_staged_as_an_expression(cx: &mut gpui::TestAppContext) {
    use crate::components::data_table::selection::CellCoord;

    let (state, input, window) = editing_state(cx, CellCoord::new(0, 1));

    assert!(!window.update(|_, app| state.read(app).is_expression_mode()));
    window.update(|window, app| {
        state.update(app, |s, cx| assert!(s.toggle_expression_mode(window, cx)));
    });
    assert!(window.update(|_, app| state.read(app).is_expression_mode()));

    window.update(|window, app| {
        input.update(app, |input, cx| {
            input.set_value("  gen_random_uuid() ", window, cx)
        });
    });
    window.update(|_, app| state.update(app, |s, cx| s.stop_editing(true, cx)));

    let staged = staged_cell(window, &state, 1).expect("the expression is staged");
    assert_eq!(staged.as_expression(), Some("gen_random_uuid()"));
    assert!(
        !window.update(|_, app| state.read(app).is_expression_mode()),
        "closing the editor leaves expression mode"
    );
}

/// Flipping back to literal before committing stages the text as a
/// literal again.
#[gpui::test]
fn toggling_twice_returns_to_a_literal(cx: &mut gpui::TestAppContext) {
    use crate::components::data_table::selection::CellCoord;

    let (state, input, window) = editing_state(cx, CellCoord::new(0, 1));

    window.update(|window, app| {
        state.update(app, |s, cx| {
            s.toggle_expression_mode(window, cx);
            s.toggle_expression_mode(window, cx);
        });
        input.update(app, |input, cx| input.set_value("now()", window, cx));
    });
    window.update(|_, app| state.update(app, |s, cx| s.stop_editing(true, cx)));

    let staged = staged_cell(window, &state, 1).expect("staged");
    assert_eq!(staged.as_expression(), None);
}

/// An empty expression is no value: committing it stages nothing.
#[gpui::test]
fn an_empty_expression_stages_nothing(cx: &mut gpui::TestAppContext) {
    use crate::components::data_table::selection::CellCoord;

    let (state, input, window) = editing_state(cx, CellCoord::new(0, 1));

    window.update(|window, app| {
        state.update(app, |s, cx| {
            s.toggle_expression_mode(window, cx);
        });
        input.update(app, |input, cx| input.set_value("   ", window, cx));
    });
    window.update(|_, app| state.update(app, |s, cx| s.stop_editing(true, cx)));

    assert!(staged_cell(window, &state, 1).is_none());
}

/// With no editor open, the toggle opens one on the active cell in
/// expression mode, and a cell that already holds an expression opens
/// there on its own.
#[gpui::test]
fn the_toggle_opens_an_expression_editor_and_expression_cells_reopen_in_it(
    cx: &mut gpui::TestAppContext,
) {
    use crate::components::data_table::selection::CellCoord;

    let (state, input, window) = editing_state(cx, CellCoord::new(0, 1));
    window.update(|window, app| {
        state.update(app, |s, cx| {
            s.toggle_expression_mode(window, cx);
        });
        input.update(app, |input, cx| input.set_value("now()", window, cx));
    });
    window.update(|_, app| state.update(app, |s, cx| s.stop_editing(true, cx)));

    window.update(|window, app| {
        state.update(app, |s, cx| {
            assert!(s.start_editing(CellCoord::new(0, 1), window, cx));
            assert!(
                s.is_expression_mode(),
                "an expression cell opens in expression mode"
            );
            let text = s.cell_input().expect("input").read(cx).value().to_string();
            assert_eq!(text, "now()");
            s.stop_editing(false, cx);
        });
    });

    window.update(|window, app| {
        state.update(app, |s, cx| {
            s.select_cell(CellCoord::new(1, 1), cx);
            assert!(s.toggle_expression_mode(window, cx));
            assert!(s.is_editing() && s.is_expression_mode());
        });
    });
}

/// Up and Down step through the suggestions of the column, wrapping, and
/// put the chosen expression in the input.
#[gpui::test]
fn suggestions_step_into_the_input(cx: &mut gpui::TestAppContext) {
    use crate::components::data_table::selection::CellCoord;

    let (state, input, window) = editing_state(cx, CellCoord::new(0, 1));
    window.update(|window, app| {
        state.update(app, |s, cx| {
            s.set_expression_suggestions(1, vec!["now()".into(), "CURRENT_DATE".into()]);
            assert!(
                s.editor_suggestions().0.is_empty(),
                "literal mode lists none"
            );
            s.toggle_expression_mode(window, cx);
            assert_eq!(s.editor_suggestions().0.len(), 2);

            s.step_suggestion(1, window, cx);
            assert_eq!(s.editor_suggestions().1, Some(0));
            s.step_suggestion(1, window, cx);
            s.step_suggestion(1, window, cx);
            assert_eq!(s.editor_suggestions().1, Some(0), "stepping wraps");
            s.step_suggestion(-1, window, cx);
        });
    });

    let text = window.update(|_, app| input.read(app).value().to_string());
    assert_eq!(text, "CURRENT_DATE");
}

/// A source that cannot write an expression never enters expression mode,
/// whether the editor is open, closed or started as an expression.
#[gpui::test]
fn a_source_without_expressions_never_enters_expression_mode(cx: &mut gpui::TestAppContext) {
    use crate::components::data_table::selection::CellCoord;

    let (state, _input, window) = editing_state(cx, CellCoord::new(0, 1));
    window.update(|window, app| {
        state.update(app, |s, cx| {
            s.set_expressions_allowed(false);
            assert!(!s.toggle_expression_mode(window, cx), "open editor");
            assert!(!s.is_expression_mode());
            s.stop_editing(false, cx);

            assert!(!s.toggle_expression_mode(window, cx), "no editor");
            assert!(!s.is_editing());
            assert!(!s.start_editing_as_expression(CellCoord::new(0, 1), window, cx));
            assert!(!s.is_editing());
        });
    });
}

/// An open enum dropdown becomes a text editor in expression mode.
#[gpui::test]
fn the_toggle_turns_an_open_enum_dropdown_into_an_expression_editor(cx: &mut gpui::TestAppContext) {
    use crate::components::data_table::selection::CellCoord;

    let (state, _input, window) = editing_state(cx, CellCoord::new(0, 1));
    window.update(|window, app| {
        state.update(app, |s, cx| {
            s.stop_editing(false, cx);
            s.set_enum_options(1, vec!["a".to_string(), "b".to_string()]);
            assert!(s.start_editing(CellCoord::new(0, 1), window, cx));
            assert!(s.is_editing_enum(), "the enum column opens a dropdown");

            assert!(s.toggle_expression_mode(window, cx));
            assert!(!s.is_editing_enum());
            assert!(s.is_editing() && s.is_expression_mode());
            assert!(s.cell_input().is_some());
        });
    });
}
