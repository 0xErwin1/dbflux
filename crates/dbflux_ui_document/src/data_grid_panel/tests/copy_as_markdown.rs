//! The "Copy as Markdown" row of the table cell menu and the
//! `ResultsCopyAsMarkdown` command both copy the selected range (or the whole
//! loaded result for one cell) as a Markdown table.

use super::*;
use crate::data_grid_panel::context_menu::build_menu_tree;
use dbflux_components::components::data_table::{ContextMenuAction, DataTableState};
use dbflux_components::composites::MenuNode;

const SELECTED_TABLE: &str = "| id | name |\n| --- | --- |\n| 1 | row 1 |\n| 2 | row 2 |\n";

fn result_grid(
    cx: &mut TestAppContext,
) -> (
    gpui::Entity<DataGridPanel>,
    gpui::Entity<DataTableState>,
    &mut VisualTestContext,
) {
    init_test_runtime(cx);

    let app_state = isolated_test_app_state(cx);
    let (_host, panel, window) = host_result_grid(cx, app_state);
    let table_state = window.update(|_, cx| {
        panel
            .read(cx)
            .grid_table
            .table_state
            .clone()
            .expect("the result grid builds its table")
    });

    (panel, table_state, window)
}

fn select_two_rows(window: &mut VisualTestContext, table_state: &gpui::Entity<DataTableState>) {
    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.select_cell(CellCoord::new(0, 0), cx);
            state.extend_selection(CellCoord::new(1, 1), cx);
        });
    });
}

fn clipboard_text(window: &mut VisualTestContext) -> String {
    window.update(|_, cx| {
        cx.read_from_clipboard()
            .and_then(|item| item.text())
            .unwrap_or_default()
    })
}

fn open_cell_menu_tree(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> Vec<MenuNode> {
    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.open_context_menu_at_selection(window, cx);
            let menu = panel.context_menu.as_ref().expect("the cell menu opens");
            build_menu_tree(panel, menu, cx)
        })
    })
}

#[gpui::test]
fn the_cell_menu_lists_copy_as_markdown_right_after_copy(cx: &mut TestAppContext) {
    let (panel, table_state, window) = result_grid(cx);
    select_two_rows(window, &table_state);

    let tree = open_cell_menu_tree(&panel, window);

    let ids: Vec<&str> = tree.iter().filter_map(|node| node.id.as_deref()).collect();
    assert_eq!(ids[..2], ["copy", "copy-as-markdown"]);
    let row = tree
        .iter()
        .find(|node| node.id.as_deref() == Some("copy-as-markdown"))
        .expect("the row exists");
    assert_eq!(
        row.item.label.to_string(),
        dbflux_i18n::t!("document.data.context_menu.item.copy_as_markdown")
    );
}

#[gpui::test]
fn activating_the_menu_row_copies_the_selected_range_as_markdown(cx: &mut TestAppContext) {
    let (panel, table_state, window) = result_grid(cx);
    select_two_rows(window, &table_state);
    open_cell_menu_tree(&panel, window);

    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.handle_context_menu_action(ContextMenuAction::CopyAsMarkdown, window, cx);
        });
    });

    assert_eq!(clipboard_text(window), SELECTED_TABLE);
    assert!(window.update(|_, cx| panel.read(cx).context_menu.is_none()));
}

#[gpui::test]
fn the_command_copies_the_selected_range_as_markdown(cx: &mut TestAppContext) {
    let (panel, table_state, window) = result_grid(cx);
    select_two_rows(window, &table_state);

    let handled = window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.dispatch_command(Command::ResultsCopyAsMarkdown, window, cx)
        })
    });

    assert!(handled);
    assert_eq!(clipboard_text(window), SELECTED_TABLE);
}

#[gpui::test]
fn the_command_copies_the_whole_result_for_a_single_cell(cx: &mut TestAppContext) {
    let (panel, table_state, window) = result_grid(cx);
    window.update(|_, cx| {
        table_state.update(cx, |state, cx| state.select_cell(CellCoord::new(1, 1), cx));
    });

    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.dispatch_command(Command::ResultsCopyAsMarkdown, window, cx)
        })
    });

    assert_eq!(
        clipboard_text(window),
        "| id | name |\n| --- | --- |\n| 1 | row 1 |\n| 2 | row 2 |\n| 3 | row 3 |\n"
    );
}
