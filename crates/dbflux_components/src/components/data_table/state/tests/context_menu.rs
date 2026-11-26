use super::*;
use crate::components::data_table::selection::CellCoord;

/// A right-click inside the selected range keeps the range, but it must still
/// commit the value typed into the cell being edited rather than drop it.
#[gpui::test]
fn right_clicking_inside_the_selection_commits_the_edit_in_progress(cx: &mut gpui::TestAppContext) {
    let (state, input, window) = editing_state(cx, CellCoord::new(0, 1));

    window.update(|window, app| {
        input.update(app, |input, cx| input.set_value("typed", window, cx));
        state.update(app, |state, _| {
            state.selection.select_cell(CellCoord::new(1, 0));
            state.selection.extend_to(CellCoord::new(0, 1));
        });
    });

    window.update(|window, app| {
        state.update(app, |state, cx| {
            state.request_cell_context_menu(
                CellCoord::new(1, 1),
                gpui::point(gpui::px(0.0), gpui::px(0.0)),
                window,
                cx,
            );
        });
    });

    window.update(|_, app| {
        let state = state.read(app);
        assert!(state.editing_cell.is_none(), "the editor closes");
        assert!(
            !state.pending_refocus,
            "the context menu keeps the focus; the table must not take it back on the next render"
        );
        assert_eq!(
            state.selection.anchor,
            Some(CellCoord::new(1, 0)),
            "the range is kept"
        );
        assert_eq!(state.selection.active, Some(CellCoord::new(0, 1)));
        let staged: Vec<_> = state
            .edit_buffer()
            .row_changes(0)
            .into_iter()
            .map(|(col, value)| (col, value.to_value()))
            .collect();
        assert_eq!(staged, vec![(1, dbflux_core::Value::Text("typed".into()))]);
    });
}
