//! The column header menu: what it lists and what its entries do.

use super::*;
use crate::keyboard_test_support::init_keyboard_runtime;
use dbflux_components::components::data_table::{ContextMenuAction, DataTableEvent};

/// Opens the column header menu of column `col` the way a right click on
/// the header does.
fn open_header_menu(
    panel: &gpui::Entity<DataGridPanel>,
    col: usize,
    window: &mut VisualTestContext,
) {
    window.update(|_, cx| {
        let table_state = panel
            .read(cx)
            .grid_table
            .table_state
            .clone()
            .expect("the result builds a table");
        table_state.update(cx, |_, cx| {
            cx.emit(DataTableEvent::ContextMenuRequested {
                row: 0,
                col,
                position: gpui::point(gpui::px(0.0), gpui::px(0.0)),
                is_column_header: true,
            });
        });
    });
    window.run_until_parked();
}

/// Copying the column name heads the menu, ahead of the ordering entries,
/// and the keyboard walks the same list the menu draws.
#[test]
fn copy_column_name_heads_the_column_header_menu() {
    let actions = DataGridPanel::column_header_menu_fixed_actions();

    assert_eq!(
        actions,
        [
            ContextMenuAction::CopyColumnName,
            ContextMenuAction::Order(dbflux_core::SortDirection::Ascending),
            ContextMenuAction::Order(dbflux_core::SortDirection::Descending),
            ContextMenuAction::RemoveOrdering,
        ]
    );
}

/// Selecting the first entry copies the bare name of the column the menu
/// was opened on and closes the menu.
#[gpui::test]
fn selecting_copy_column_name_copies_the_header_name(cx: &mut TestAppContext) {
    init_keyboard_runtime(cx);
    let app_state = isolated_test_app_state(cx);
    let (_host, panel, window) = host_result_grid(cx, app_state);

    open_header_menu(&panel, 1, window);

    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            assert!(panel.dispatch_command(Command::MenuSelect, window, cx));
        });
    });
    window.run_until_parked();

    let copied = window
        .update(|_, cx| cx.read_from_clipboard())
        .and_then(|item| item.text());
    assert_eq!(copied.as_deref(), Some("name"));
    assert!(window.update(|_, cx| panel.read(cx).context_menu.is_none()));
}

/// The table menu (`m`) carries Copy column name for the active cell's
/// column, right after Copy, so the keyboard reaches it without the header.
#[gpui::test]
fn table_menu_copies_the_active_column_name_from_the_keyboard(cx: &mut TestAppContext) {
    use dbflux_app::keymap::ContextId;

    init_keyboard_runtime(cx);
    let app_state = isolated_test_app_state(cx);
    let (_host, panel, window) = host_result_grid(cx, app_state);

    let active_column = |window: &mut VisualTestContext| {
        window.update(|_, cx| {
            panel
                .read(cx)
                .grid_table
                .table_state
                .as_ref()
                .and_then(|state| state.read(cx).selection().active)
                .map(|coord| coord.col)
        })
    };

    window.update(|_, cx| {
        let table_state = panel
            .read(cx)
            .grid_table
            .table_state
            .clone()
            .expect("the result builds a table");
        table_state.update(cx, |state, cx| {
            state.select_cell(CellCoord::new(0, 1), cx);
        });
    });
    window.run_until_parked();
    assert_eq!(active_column(window), Some(1), "the name column is active");

    window.simulate_keystrokes("m");
    window.run_until_parked();
    assert_eq!(
        window.update(|_, cx| panel.read(cx).active_context(cx)),
        ContextId::ContextMenu,
        "`m` opens the table menu"
    );

    for key in ["j", "enter"] {
        window.simulate_keystrokes(key);
        window.run_until_parked();
    }

    let copied = window
        .update(|_, cx| cx.read_from_clipboard())
        .and_then(|item| item.text());
    assert_eq!(copied.as_deref(), Some("name"));
    assert!(window.update(|_, cx| panel.read(cx).context_menu.is_none()));
}
