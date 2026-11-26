//! The export menu's "Copy" rows copy the selected cells when the selection
//! spans more than one cell, and the whole loaded result otherwise.

use super::*;
use dbflux_components::components::data_table::DataTableState;
use dbflux_components::components::data_table::model::CellValue;
use dbflux_export::ExportFormat;

fn result_grid(
    cx: &mut TestAppContext,
) -> (
    gpui::Entity<DataGridPanel>,
    gpui::Entity<DataTableState>,
    &mut VisualTestContext,
) {
    init_test_runtime(cx);

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("isolated storage runtime"),
            )
            .expect("test storage setup")
        })
    });
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

fn select(
    window: &mut VisualTestContext,
    table_state: &gpui::Entity<DataTableState>,
    from: CellCoord,
    to: CellCoord,
) {
    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.select_cell(from, cx);
            state.extend_selection(to, cx);
        });
    });
}

fn copy_as(
    window: &mut VisualTestContext,
    panel: &gpui::Entity<DataGridPanel>,
    format: ExportFormat,
) -> String {
    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.copy_to_clipboard_with_format(format, window, cx)
        });
        cx.read_from_clipboard()
            .and_then(|item| item.text())
            .unwrap_or_default()
    })
}

#[gpui::test]
fn a_multi_row_selection_copies_only_the_selected_rows(cx: &mut TestAppContext) {
    let (panel, table_state, window) = result_grid(cx);
    select(
        window,
        &table_state,
        CellCoord::new(0, 0),
        CellCoord::new(1, 1),
    );

    let copied = copy_as(window, &panel, ExportFormat::Csv);

    assert_eq!(copied, "id,name\n1,row 1\n2,row 2\n");
    let expected_toast =
        crate::labels::context_menu_clipboard_copied_rows_toast(2, "CSV", copied.len());
    window.run_until_parked();
    assert_eq!(last_toast_title(window), Some(expected_toast));
}

#[gpui::test]
fn a_cell_range_copies_only_the_selected_columns(cx: &mut TestAppContext) {
    let (panel, table_state, window) = result_grid(cx);
    select(
        window,
        &table_state,
        CellCoord::new(2, 1),
        CellCoord::new(1, 1),
    );

    let copied = copy_as(window, &panel, ExportFormat::JsonCompact);

    assert_eq!(copied, r#"[{"name":"row 2"},{"name":"row 3"}]"#);
}

#[gpui::test]
fn a_single_selected_cell_copies_the_whole_loaded_result(cx: &mut TestAppContext) {
    let (panel, table_state, window) = result_grid(cx);
    select(
        window,
        &table_state,
        CellCoord::new(1, 1),
        CellCoord::new(1, 1),
    );

    let copied = copy_as(window, &panel, ExportFormat::Csv);

    assert_eq!(copied, "id,name\n1,row 1\n2,row 2\n3,row 3\n");
    let expected_toast =
        crate::labels::context_menu_clipboard_copied_rows_toast(3, "CSV", copied.len());
    window.run_until_parked();
    assert_eq!(last_toast_title(window), Some(expected_toast));
}

#[gpui::test]
fn pending_rows_and_edits_are_copied_as_the_grid_shows_them(cx: &mut TestAppContext) {
    let (panel, table_state, window) = result_grid(cx);
    window.update(|_, cx| {
        table_state.update(cx, |state, _| {
            let buffer = state.edit_buffer_mut();
            buffer.set_cell(0, 1, CellValue::text("edited"));
            buffer.add_pending_insert_after(0, vec![CellValue::int(9), CellValue::text("new")]);
        });
    });
    select(
        window,
        &table_state,
        CellCoord::new(0, 0),
        CellCoord::new(2, 1),
    );

    let copied = copy_as(window, &panel, ExportFormat::Csv);

    assert_eq!(copied, "id,name\n1,edited\n9,new\n2,row 2\n");
}

#[gpui::test]
fn the_whole_result_copy_shows_pending_rows_and_edits(cx: &mut TestAppContext) {
    let (panel, table_state, window) = result_grid(cx);
    window.update(|_, cx| {
        table_state.update(cx, |state, _| {
            let buffer = state.edit_buffer_mut();
            buffer.set_cell(0, 1, CellValue::text("edited"));
            buffer.add_pending_insert_after(0, vec![CellValue::int(9), CellValue::text("new")]);
        });
    });
    select(
        window,
        &table_state,
        CellCoord::new(2, 1),
        CellCoord::new(2, 1),
    );

    let copied = copy_as(window, &panel, ExportFormat::Csv);

    assert_eq!(copied, "id,name\n1,edited\n9,new\n2,row 2\n3,row 3\n");
    let expected_toast =
        crate::labels::context_menu_clipboard_copied_rows_toast(4, "CSV", copied.len());
    window.run_until_parked();
    assert_eq!(last_toast_title(window), Some(expected_toast));
}
