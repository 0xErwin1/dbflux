//! Long and multi-line cells edited in the modal editor, added and renamed
//! columns, and saving a change that only touches the columns.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use dbflux_components::components::data_table::actions as table_actions;
use dbflux_components::components::data_table::model::{CellValue, VisualRowSource};
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_delimited::DialectOverrides;
use dbflux_ui_base::keyboard_coverage::{FrameCapture, KeyboardPath};
use gpui::{Entity, Modifiers, TestAppContext, VisualTestContext};

use super::columns::LoadingRest;
use super::document::DelimitedDocument;
use super::document_tests::{
    CITIES, column_titles, covered_ids, first_column, has_more_records, last_toast_title,
    open_local, open_local_in_small_pages, pane_action_ids, run_pane_action, set_overrides,
    toast_count,
};
use super::editing::CellEditRefusal;
use super::editing_tests::{
    asked_to_close, has_pending_operations, ids_under, is_dirty, local_file, press, read,
    record_events, save, save_results, select, table_state, type_into_cell,
};
use super::tests::TestDirectory;
use crate::keyboard_coverage::DELIMITED;

/// A record whose second field is 150 characters long, over the table's
/// inline limit.
fn long_value() -> String {
    "abcdefghij".repeat(15)
}

/// Starts editing the cell at `row`, `col` as Enter does and lets the
/// document react. Returns whether the table took the request.
pub(super) fn start_cell_edit(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    row: usize,
    col: usize,
) -> bool {
    let table_state = table_state(document, window);

    let started = window.update(|window, cx| {
        table_state.update(cx, |state, cx| {
            state.start_editing(CellCoord::new(row, col), window, cx)
        })
    });
    window.run_until_parked();

    started
}

fn cell_editor_is_open(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> bool {
    window.update(|_, cx| {
        document
            .read(cx)
            .cell_editor()
            .is_some_and(|editor| editor.read(cx).is_visible())
    })
}

fn column_prompt_is_open(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> bool {
    window.update(|_, cx| document.read(cx).is_column_prompt_open())
}

fn column_prompt_note(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Option<String> {
    window.update(|_, cx| document.read(cx).column_prompt_note())
}

/// Replaces the name in the open column prompt with `name` and presses
/// Enter, as the user confirms it.
fn confirm_column_name(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    name: &str,
) {
    window.update(|window, cx| {
        let input = document
            .read(cx)
            .column_prompt
            .as_ref()
            .expect("the column prompt is open")
            .input
            .clone();

        input.update(cx, |input, cx| input.set_value(name, window, cx));
    });
    window.run_until_parked();

    press(window, "enter");
}

fn add_column(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext, name: &str) {
    run_pane_action(document, window, "delimited-add-column");

    assert!(column_prompt_is_open(document, window));

    confirm_column_name(document, window, name);
}

fn rename_column(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    column: usize,
    name: &str,
) {
    select_column(document, window, column);
    run_pane_action(document, window, "delimited-rename-column");

    assert!(column_prompt_is_open(document, window));

    confirm_column_name(document, window, name);
}

fn select_column(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    column: usize,
) {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.select_cell(CellCoord::new(0, column), cx)
        })
    });
    window.run_until_parked();
}

/// The text of the first cell of every row the table shows, inserted rows
/// and staged edits included.
fn shown_first_column(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Vec<String> {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        let state = table_state.read(cx);
        let buffer = state.edit_buffer();
        let absent = CellValue::text("");

        buffer
            .compute_visual_order()
            .into_iter()
            .map(|source| match source {
                VisualRowSource::Base(row) => {
                    let base = state.model().cell(row, 0).unwrap_or(&absent);
                    buffer.get_cell(row, 0, base).edit_text()
                }

                VisualRowSource::Insert(insert) => buffer
                    .get_pending_insert_by_idx(insert)
                    .and_then(|cells| cells.first())
                    .map(CellValue::edit_text)
                    .unwrap_or_default(),
            })
            .collect()
    })
}

/// Clicks the middle of the element drawn with `id`.
pub(super) fn click(window: &mut VisualTestContext, id: &str) {
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    let bounds = frame
        .nodes()
        .find(|(_, node)| node.id() == id)
        .map(|(_, node)| node.bounds())
        .unwrap_or_else(|| panic!("`{id}` is drawn"));

    window.simulate_click(bounds.center(), Modifiers::default());
    window.run_until_parked();
}

fn active_cell(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Option<CellCoord> {
    let table_state = table_state(document, window);

    window.update(|_, cx| table_state.read(cx).selection().active)
}

// -- The modal cell editor ----------------------------------------------------

#[gpui::test]
fn a_long_cell_edited_in_the_modal_is_saved_with_its_exact_value(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("modal-long");
    let long = long_value();
    let bytes = format!("name,note\nAna,{long}\n\"Bo, Jr\",short\n");
    let path = local_file(&directory, "notes.csv", bytes.as_bytes());

    let (document, window) = open_local(cx, path.clone());

    assert!(start_cell_edit(&document, window, 0, 1));
    assert!(cell_editor_is_open(&document, window));

    window.simulate_input("X");
    press(window, "ctrl-s");

    assert!(!cell_editor_is_open(&document, window));
    assert!(is_dirty(&document, window));

    save(&document, window);

    assert_eq!(
        read(&path),
        format!("name,note\nAna,X{long}\n\"Bo, Jr\",short\n").into_bytes()
    );
    assert!(!is_dirty(&document, window));
}

#[gpui::test]
fn a_multi_line_cell_edited_in_the_modal_is_saved_with_its_line_breaks(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("modal-multi-line");
    let bytes: &[u8] = b"name,note\nAna,\"first\nsecond\"\nBo,\"a, b\"\n";
    let path = local_file(&directory, "notes.csv", bytes);

    let (document, window) = open_local(cx, path.clone());

    assert!(start_cell_edit(&document, window, 0, 1));
    assert!(cell_editor_is_open(&document, window));

    window.simulate_input("Z");
    press(window, "ctrl-s");
    save(&document, window);

    assert_eq!(
        read(&path),
        b"name,note\nAna,\"Zfirst\nsecond\"\nBo,\"a, b\"\n"
    );
}

#[gpui::test]
fn cancel_in_the_modal_stages_nothing(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("modal-cancel");
    let bytes = format!("name,note\nAna,{}\n", long_value());
    let path = local_file(&directory, "notes.csv", bytes.as_bytes());

    let (document, window) = open_local(cx, path.clone());

    assert!(start_cell_edit(&document, window, 0, 1));

    window.simulate_input("X");
    press(window, "escape");

    assert!(!cell_editor_is_open(&document, window));
    assert!(!has_pending_operations(&document, window));
    assert!(!is_dirty(&document, window));
}

/// An unchanged multi-line value saved from the modal is no edit, so the
/// record keeps its bytes.
#[gpui::test]
fn an_unchanged_value_saved_from_the_modal_stages_nothing(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("modal-unchanged");
    let bytes: &[u8] = b"name,note\r\nAna,\"first\r\nsecond\"\r\n";
    let path = local_file(&directory, "notes.csv", bytes);

    let (document, window) = open_local(cx, path.clone());

    assert!(start_cell_edit(&document, window, 0, 1));
    press(window, "ctrl-s");

    assert!(!cell_editor_is_open(&document, window));
    assert!(!has_pending_operations(&document, window));
}

/// While a save runs the table is read-only: the modal does not open, and a
/// modal that was open when the save started stages nothing and says so.
#[gpui::test]
fn the_modal_stages_nothing_while_the_document_is_read_only(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("modal-read-only");
    let bytes = format!("name,note\nAna,{}\n", long_value());
    let path = local_file(&directory, "notes.csv", bytes.as_bytes());

    let (document, window) = open_local(cx, path.clone());

    assert!(start_cell_edit(&document, window, 0, 1));

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.saving = true;
            document.sync_table_editing(cx);
        })
    });
    window.run_until_parked();

    window.simulate_input("X");
    press(window, "ctrl-s");

    assert!(!cell_editor_is_open(&document, window));
    assert!(!has_pending_operations(&document, window));
    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not edit notes.csv")
    );

    assert!(!start_cell_edit(&document, window, 0, 1));
    assert!(!cell_editor_is_open(&document, window));

    window.update(|_, cx| document.update(cx, |document, _cx| document.saving = false));
    assert_eq!(read(&path), bytes.as_bytes());
}

// -- Adding a column ----------------------------------------------------------

#[gpui::test]
fn a_column_added_to_a_loaded_file_is_saved_with_its_values(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-add");
    let bytes: &[u8] = b"name,city\nAna,Lima\n\"Bo, Jr\",Quito\nCy,\"Oslo\"\n";
    let path = local_file(&directory, "cities.csv", bytes);

    let (document, window) = open_local(cx, path.clone());

    add_column(&document, window, "country");

    assert!(!column_prompt_is_open(&document, window));
    assert_eq!(
        column_titles(&document, window),
        ["name", "city", "country"]
    );
    assert!(is_dirty(&document, window));

    type_into_cell(&document, window, 0, 2, "Peru");
    type_into_cell(&document, window, 2, 2, "Norway");

    save(&document, window);

    assert_eq!(
        read(&path),
        b"name,city,country\nAna,Lima,Peru\n\"Bo, Jr\",Quito,\nCy,\"Oslo\",Norway\n"
    );
    assert!(!is_dirty(&document, window));
    assert_eq!(
        column_titles(&document, window),
        ["name", "city", "country"]
    );
}

/// A file that is not fully loaded offers to read the rest first, and the
/// column is asked for once it is read.
#[gpui::test]
fn a_column_on_a_partly_loaded_file_loads_the_rest_first(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-load-rest");
    let path = local_file(
        &directory,
        "numbers.csv",
        &super::document_tests::numbered_csv(5),
    );

    let (document, window) = open_local_in_small_pages(cx, path.clone());

    assert!(has_more_records(&document, window));

    run_pane_action(&document, window, "delimited-add-column");

    assert!(window.update(|_, cx| document.read(cx).is_load_rest_prompt_open()));
    assert!(!column_prompt_is_open(&document, window));
    assert!(has_more_records(&document, window));

    press(window, "enter");

    assert!(!has_more_records(&document, window));
    assert_eq!(first_column(&document, window), ["0", "1", "2", "3", "4"]);
    assert!(column_prompt_is_open(&document, window));

    confirm_column_name(&document, window, "country");
    type_into_cell(&document, window, 4, 2, "Peru");
    save(&document, window);

    assert_eq!(
        read(&path),
        b"id,city,country\n0,Lima,\n1,Lima,\n2,Lima,\n3,Lima,\n4,Lima,Peru\n"
    );
}

/// Dismissing the offer reads nothing and adds nothing.
#[gpui::test]
fn dismissing_the_load_rest_offer_reads_nothing(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-load-rest-dismiss");
    let path = local_file(
        &directory,
        "numbers.csv",
        &super::document_tests::numbered_csv(5),
    );

    let (document, window) = open_local_in_small_pages(cx, path);

    run_pane_action(&document, window, "delimited-add-column");
    press(window, "escape");

    assert!(!window.update(|_, cx| document.read(cx).is_load_rest_prompt_open()));
    assert!(has_more_records(&document, window));
    assert_eq!(first_column(&document, window), ["0", "1"]);
    assert!(!column_prompt_is_open(&document, window));
}

/// A load of the rest that is cancelled keeps the pages read so far and
/// opens no prompt.
#[gpui::test]
fn a_cancelled_load_of_the_rest_adds_no_column(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-load-rest-cancel");
    let path = local_file(
        &directory,
        "numbers.csv",
        &super::document_tests::numbered_csv(9),
    );

    let (document, window) = open_local_in_small_pages(cx, path);

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.load_rest(cx);
            assert!(document.is_loading_rest());
            document.cancel_load_rest(cx);
        })
    });
    window.run_until_parked();

    window.update(|_, cx| assert!(!document.read(cx).is_loading_rest()));
    assert!(!column_prompt_is_open(&document, window));
    assert!(has_more_records(&document, window));
}

#[gpui::test]
fn a_column_added_to_a_headerless_file_writes_no_name(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-headerless");
    let path = local_file(&directory, "cities.tsv", b"1\tLima\n2\tQuito\n");

    let (document, window) = open_local(cx, path.clone());

    assert_eq!(column_titles(&document, window), ["column_1", "column_2"]);

    run_pane_action(&document, window, "delimited-add-column");

    let note = column_prompt_note(&document, window).expect("the prompt explains the name");
    assert!(note.contains("no header row"), "{note}");

    confirm_column_name(&document, window, "country");
    type_into_cell(&document, window, 1, 2, "Ecuador");
    save(&document, window);

    assert_eq!(read(&path), b"1\tLima\t\n2\tQuito\tEcuador\n");
}

#[gpui::test]
fn a_column_added_to_a_file_with_a_header_has_no_note(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-note");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    run_pane_action(&document, window, "delimited-add-column");

    assert!(column_prompt_is_open(&document, window));
    assert_eq!(column_prompt_note(&document, window), None);
}

/// The table is rebuilt with one more column, and every pending edit, the
/// inserted rows included, stays where it was.
#[gpui::test]
fn pending_edits_survive_adding_a_column(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-keeps-edits");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 0, 1, "Cusco");
    select(&document, window, 1);
    press(window, "a a");
    type_into_cell(&document, window, 2, 0, "Cy");

    add_column(&document, window, "country");

    assert_eq!(shown_first_column(&document, window), ["Ana", "Bo", "Cy"]);

    type_into_cell(&document, window, 2, 2, "Norway");
    type_into_cell(&document, window, 1, 2, "Ecuador");

    save(&document, window);

    assert_eq!(
        read(&path),
        b"name,city,country\nAna,Cusco,\nBo,Quito,Ecuador\nCy,,Norway\n"
    );
}

/// A row restored by redo after a column was added gets a field for the
/// column, so a value typed there is kept.
#[gpui::test]
fn a_row_restored_after_a_column_was_added_takes_a_value_in_it(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-redo-row");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    select(&document, window, 1);
    press(window, "a a");
    add_column(&document, window, "country");

    select(&document, window, 2);
    window.update(|window, cx| window.dispatch_action(Box::new(table_actions::Undo), cx));
    window.run_until_parked();
    window.update(|window, cx| window.dispatch_action(Box::new(table_actions::Redo), cx));
    window.run_until_parked();

    assert_eq!(shown_first_column(&document, window), ["Ana", "Bo", ""]);

    type_into_cell(&document, window, 2, 2, "Norway");
    save(&document, window);

    assert_eq!(
        read(&path),
        b"name,city,country\nAna,Lima,\nBo,Quito,\n,,Norway\n"
    );
}

// -- Renaming a column --------------------------------------------------------

#[gpui::test]
fn a_renamed_header_column_and_a_renamed_new_column_are_saved(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-rename");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    rename_column(&document, window, 1, "town");
    assert_eq!(column_titles(&document, window), ["name", "town"]);
    assert!(is_dirty(&document, window));

    add_column(&document, window, "country");
    rename_column(&document, window, 2, "nation");
    assert_eq!(column_titles(&document, window), ["name", "town", "nation"]);

    type_into_cell(&document, window, 0, 2, "Peru");
    save(&document, window);

    assert_eq!(read(&path), b"name,town,nation\nAna,Lima,Peru\nBo,Quito,\n");
    assert!(!is_dirty(&document, window));
}

/// Right-clicking a column header is how the pointer asks to rename it.
#[gpui::test]
fn a_header_context_request_opens_the_rename_prompt(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-rename-header");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let table_state = table_state(&document, window);

    window.update(|_, cx| {
        table_state.update(cx, |_, cx| {
            cx.emit(
                dbflux_components::components::data_table::DataTableEvent::ContextMenuRequested {
                    row: 0,
                    col: 1,
                    position: gpui::point(gpui::px(0.0), gpui::px(0.0)),
                    is_column_header: true,
                },
            );
        })
    });
    window.run_until_parked();

    assert!(column_prompt_is_open(&document, window));

    confirm_column_name(&document, window, "town");
    save(&document, window);

    assert_eq!(read(&path), b"name,town\nAna,Lima\nBo,Quito\n");
}

#[gpui::test]
fn renaming_a_column_of_a_headerless_file_is_refused(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-rename-headerless");
    let path = local_file(&directory, "cities.tsv", b"1\tLima\n2\tQuito\n");

    let (document, window) = open_local(cx, path.clone());

    select_column(&document, window, 1);
    run_pane_action(&document, window, "delimited-rename-column");

    assert!(!column_prompt_is_open(&document, window));
    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not rename a column of cities.tsv")
    );

    let cause = crate::delimited::columns::rename_refused_cause(true);
    assert!(cause.contains("no header row"), "{cause}");
    assert!(cause.contains("Header row"), "{cause}");
    assert!(!is_dirty(&document, window));
}

// -- Saving a column-only change ----------------------------------------------

#[gpui::test]
fn the_save_key_saves_a_column_only_change(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-save-key");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.rename_column(1, "town".into(), cx)
        })
    });
    window.run_until_parked();

    assert!(is_dirty(&document, window));
    assert!(!has_pending_operations(&document, window));

    press(window, "ctrl-s");

    assert_eq!(read(&path), b"name,town\nAna,Lima\nBo,Quito\n");
    assert!(!is_dirty(&document, window));
}

#[gpui::test]
fn the_save_button_saves_a_column_only_change(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-save-button");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.append_column("country".into(), cx)
        })
    });
    window.run_until_parked();

    let checked = covered_ids(&document, window);
    assert!(
        checked.iter().any(|id| id == "delimited-save"),
        "{checked:?}"
    );

    click(window, "delimited-save");

    assert_eq!(read(&path), b"name,city,country\nAna,Lima,\nBo,Quito,\n");
    assert!(!is_dirty(&document, window));
}

#[gpui::test]
fn a_column_only_change_is_saved_on_close(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-save-close");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.rename_column(0, "who".into(), cx)
        })
    });
    window.run_until_parked();

    let started = window.update(|window, cx| {
        let pane = DelimitedDocument::into_pane(document.clone(), cx);
        pane.save_for_close(window, cx)
    });
    window.run_until_parked();

    assert!(started);
    assert_eq!(read(&path), b"who,city\nAna,Lima\nBo,Quito\n");
    assert_eq!(save_results(&events), [true]);
    assert!(asked_to_close(&events));
}

/// The save key on a table with row edits still saves once.
#[gpui::test]
fn the_save_key_saves_row_and_column_changes_once(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-save-key-once");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");
    rename_column(&document, window, 1, "town");
    select(&document, window, 0);

    press(window, "ctrl-s");

    assert_eq!(read(&path), b"name,town\nAna,Cusco\nBo,Quito\n");
    assert_eq!(save_results(&events), [true]);
}

/// The save key on a clean table saves nothing and reports nothing.
#[gpui::test]
fn the_save_key_on_a_clean_table_does_nothing(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-save-key-clean");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    press(window, "ctrl-s");

    assert!(save_results(&events).is_empty());
    assert_eq!(read(&path), CITIES);
}

// -- Discard, reload and dialect ----------------------------------------------

#[gpui::test]
fn discard_after_add_and_rename_returns_to_the_files_columns(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-discard");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    rename_column(&document, window, 0, "who");
    add_column(&document, window, "country");
    type_into_cell(&document, window, 1, 2, "Ecuador");
    select_column(&document, window, 2);

    run_pane_action(&document, window, "delimited-discard");

    assert_eq!(column_titles(&document, window), ["name", "city"]);
    assert!(!is_dirty(&document, window));
    assert!(!has_pending_operations(&document, window));

    let active = active_cell(&document, window).expect("the cursor stays in the table");
    assert!(active.col < 2 && active.row < 2, "{active:?}");

    save(&document, window);
    assert_eq!(read(&path), CITIES);
}

#[gpui::test]
fn column_changes_refuse_a_dialect_override_and_a_reload(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-refuse-reread");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.append_column("country".into(), cx)
        })
    });
    window.run_until_parked();

    set_overrides(
        &document,
        window,
        DialectOverrides {
            delimiter: Some(b';'),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not re-read cities.csv")
    );
    assert!(!window.update(|_, cx| document.read(cx).is_rereading()));

    press(window, "f5");

    assert_eq!(toast_count(window), 2);
    assert!(!window.update(|_, cx| document.read(cx).is_rereading()));
    assert_eq!(
        column_titles(&document, window),
        ["name", "city", "country"]
    );
    assert!(is_dirty(&document, window));
}

// -- Coverage -----------------------------------------------------------------

#[gpui::test]
fn the_column_controls_and_dialogs_are_covered(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-coverage");
    let bytes = format!("name,note\nAna,{}\n", long_value());
    let path = local_file(&directory, "notes.csv", bytes.as_bytes());

    let (document, window) = open_local(cx, path);

    let menu = pane_action_ids(&document, window);
    for entry in ["delimited-add-column", "delimited-rename-column"] {
        assert!(menu.iter().any(|id| id == entry), "{menu:?}");
    }

    let checked = covered_ids(&document, window);
    assert!(
        checked.iter().any(|id| id == "delimited-add-column"),
        "{checked:?}"
    );

    run_pane_action(&document, window, "delimited-add-column");
    let checked = covered_ids(&document, window);
    for control in [
        "delimited-column-name-cancel",
        "delimited-column-name-confirm",
    ] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }
    press(window, "escape");
    assert!(!column_prompt_is_open(&document, window));

    assert!(start_cell_edit(&document, window, 0, 1));
    let checked = covered_ids(&document, window);
    for control in ["cell-editor-cancel", "cell-editor-save"] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }
    press(window, "escape");
    assert!(!cell_editor_is_open(&document, window));
}

#[gpui::test]
fn the_load_rest_offer_and_its_cancel_are_covered(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("column-coverage-load-rest");
    let path = local_file(
        &directory,
        "numbers.csv",
        &super::document_tests::numbered_csv(5),
    );

    let (document, window) = open_local_in_small_pages(cx, path);

    run_pane_action(&document, window, "delimited-add-column");

    let checked = covered_ids(&document, window);
    for control in ["delimited-load-rest-dismiss", "delimited-load-rest-confirm"] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }

    press(window, "escape");

    // The state of a load of the rest that is still reading: the background
    // read holds the reader. A real one ends before the frame is drawn.
    let reader = window.update(|_, cx| {
        document.update(cx, |document, cx| {
            let loaded = document.loaded_mut().expect("the file is loaded");
            loaded.loading_rest = Some(LoadingRest {
                cancel: Arc::new(AtomicBool::new(false)),
            });
            cx.notify();

            loaded.reader.take()
        })
    });

    let menu = pane_action_ids(&document, window);
    assert!(
        menu.iter().any(|id| id == "delimited-load-rest-cancel"),
        "{menu:?}"
    );

    let checked = covered_ids(&document, window);
    assert!(
        checked.iter().any(|id| id == "delimited-load-rest-cancel"),
        "{checked:?}"
    );

    click(window, "delimited-load-rest-cancel");

    window.update(|_, cx| {
        document.update(cx, |document, _cx| {
            assert!(!document.is_loading_rest());

            if let Some(loaded) = document.loaded_mut() {
                loaded.reader = reader;
            }
        })
    });
}

#[test]
fn the_column_strings_resolve_in_every_locale() {
    for key in [
        "document.delimited.action.add_column",
        "document.delimited.action.rename_column",
        "document.delimited.action.cancel",
        "document.delimited.column.add_title",
        "document.delimited.column.rename_title",
        "document.delimited.column.name_placeholder",
        "document.delimited.column.confirm_add",
        "document.delimited.column.confirm_rename",
        "document.delimited.column.no_header_note",
        "document.delimited.load_rest.title",
        "document.delimited.load_rest.body",
        "document.delimited.load_rest.confirm",
        "document.delimited.load_rest.cancel",
        "document.delimited.footer.loading_rest",
        "document.delimited.error.edit_failed",
        "document.delimited.error.edit_while_read_only",
        "document.delimited.error.add_column_failed",
        "document.delimited.error.add_column_needs_full_load",
        "document.delimited.error.rename_column_failed",
        "document.delimited.error.rename_no_header",
        "document.delimited.error.rename_no_header_record",
        "document.delimited.error.edit_rows_read_again",
        "document.delimited.error.edit_changes_discarded",
        "document.delimited.error.edit_row_removed",
        "document.delimited.error.column_while_read_only",
        "document.delimited.error.save_while_loading_rest",
    ] {
        let english = dbflux_i18n::t!(key, locale = "en");

        assert!(!english.is_empty());
        assert_ne!(english, format!("en.{key}"));

        for locale in ["es", "ko", "pt_BR", "zh_Hans"] {
            let translated = dbflux_i18n::t!(key, locale = locale);

            assert_ne!(
                translated,
                format!("{locale}.{key}"),
                "{locale} lacks {key}"
            );
            assert_ne!(translated, english, "{locale} copies English for {key}");
        }
    }
}

// -- Correction pass ----------------------------------------------------------

/// Puts the document in the state of a load of the rest that is still
/// reading: the background read holds the reader. Returns the reader, for
/// [`end_loading_rest`].
fn start_loading_rest(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Option<dbflux_delimited::PagedReader<super::source::DelimitedSource>> {
    let reader = window.update(|_, cx| {
        document.update(cx, |document, cx| {
            let loaded = document.loaded_mut().expect("the file is loaded");
            loaded.loading_rest = Some(LoadingRest {
                cancel: Arc::new(AtomicBool::new(false)),
            });
            cx.notify();

            loaded.reader.take()
        })
    });
    window.run_until_parked();

    reader
}

fn end_loading_rest(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    reader: Option<dbflux_delimited::PagedReader<super::source::DelimitedSource>>,
) {
    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            if let Some(loaded) = document.loaded_mut() {
                loaded.loading_rest = None;
                loaded.reader = reader;
            }
            cx.notify();
        })
    });
    window.run_until_parked();
}

fn pane_action_enabled(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    id: &str,
) -> bool {
    window.update(|_, cx| {
        document
            .read(cx)
            .pane_actions(document)
            .into_iter()
            .find(|action| action.id.as_ref() == id)
            .unwrap_or_else(|| panic!("the pane actions list {id}"))
            .enabled
    })
}

/// A file whose second record holds a value the inline editor does not
/// take.
fn long_second_record() -> String {
    format!("name,note\nAna,short\nBo,{}\n", long_value())
}

/// The confirmed sequence: the editor is open on Bo's cell and a row is
/// inserted above it. The value lands on Bo's record, not on the row that
/// now sits at the position the editor was opened at.
#[gpui::test]
fn the_editor_value_follows_its_row_when_a_row_is_inserted_above(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-editor-row-moved");
    let bytes = long_second_record();
    let path = local_file(&directory, "notes.csv", bytes.as_bytes());

    let (document, window) = open_local(cx, path.clone());

    assert!(start_cell_edit(&document, window, 1, 1));

    // Whatever path inserts it: the row-changing actions are disabled while
    // the editor is open, so the buffer is changed directly.
    let table_state = table_state(&document, window);
    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.edit_buffer_mut().add_pending_insert_at(
                dbflux_components::components::data_table::model::InsertAnchor::BeforeFirst,
                vec![CellValue::text("Cy"), CellValue::text("new")],
            );
            cx.notify();
        })
    });
    window.run_until_parked();

    window.simulate_input("X");
    press(window, "ctrl-s");
    save(&document, window);

    assert_eq!(
        read(&path),
        format!("name,note\nCy,new\nAna,short\nBo,X{}\n", long_value()).into_bytes()
    );
}

/// The row the editor was opened on is gone when its value is saved: the
/// value is refused, said so, and staged nowhere.
#[gpui::test]
fn the_editor_value_is_refused_when_its_row_was_removed(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-editor-row-removed");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    select(&document, window, 1);
    press(window, "a a");
    stage_long_value_in_insert(&document, window);

    assert!(start_cell_edit(&document, window, 2, 1));

    let table_state = table_state(&document, window);
    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.edit_buffer_mut().remove_pending_insert_by_idx(0);
            cx.notify();
        })
    });
    window.run_until_parked();

    let refusal = editor_refusal(&document, window);
    assert_eq!(refusal, Some(CellEditRefusal::RowRemoved));

    window.simulate_input("X");
    press(window, "ctrl-s");

    assert!(!has_pending_operations(&document, window));
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not edit cities.csv")
    );
}

fn stage_long_value_in_insert(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.stage_cell_value(2, 1, CellValue::text(&long_value()));
            cx.notify();
        })
    });
    window.run_until_parked();
}

/// Why the value of the open editor would be refused now, or `None` when it
/// would be staged.
fn editor_refusal(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Option<CellEditRefusal> {
    window.update(|_, cx| {
        let document = document.read(cx);
        let target = document
            .cell_edit_target
            .as_ref()
            .expect("the editor is open");

        document.resolve_cell_edit(target, cx).err()
    })
}

/// Each refusal of the editor's value names what happened.
#[gpui::test]
fn each_refusal_of_the_editor_value_has_its_own_reason(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-editor-reasons");
    let bytes = long_second_record();
    let path = local_file(&directory, "notes.csv", bytes.as_bytes());

    let (document, window) = open_local(cx, path);

    assert!(start_cell_edit(&document, window, 1, 1));
    assert_eq!(editor_refusal(&document, window), None);

    let change = |window: &mut VisualTestContext, run: fn(&mut DelimitedDocument)| {
        window.update(|_, cx| document.update(cx, |document, _cx| run(document)));
    };

    change(window, |document| document.saving = true);
    assert_eq!(
        editor_refusal(&document, window),
        Some(CellEditRefusal::ReadOnly)
    );
    change(window, |document| document.saving = false);

    change(window, |document| {
        if let Some(loaded) = document.loaded_mut() {
            loaded.discard_generation += 1;
        }
    });
    assert_eq!(
        editor_refusal(&document, window),
        Some(CellEditRefusal::ChangesDiscarded)
    );

    change(window, |document| {
        if let Some(loaded) = document.loaded_mut() {
            loaded.reader_epoch += 1;
        }
    });
    assert_eq!(
        editor_refusal(&document, window),
        Some(CellEditRefusal::RowsReadAgain)
    );

    for (refusal, key) in [
        (
            CellEditRefusal::RowsReadAgain,
            "document.delimited.error.edit_rows_read_again",
        ),
        (
            CellEditRefusal::ChangesDiscarded,
            "document.delimited.error.edit_changes_discarded",
        ),
        (
            CellEditRefusal::ReadOnly,
            "document.delimited.error.edit_while_read_only",
        ),
        (
            CellEditRefusal::RowRemoved,
            "document.delimited.error.edit_row_removed",
        ),
    ] {
        assert_eq!(refusal.cause(), dbflux_i18n::t!(key), "{refusal:?}");
    }
}

const ROW_CHANGING_ACTIONS: [&str; 10] = [
    "delimited-delimiter",
    "delimited-quote",
    "delimited-header",
    "delimited-encoding",
    "delimited-insert-above",
    "delimited-add-column",
    "delimited-rename-column",
    "delimited-discard",
    "delimited-reload",
    "delimited-dialect-reset",
];

/// While the editor or a column prompt is open, nothing that changes the
/// rows or the columns can be run, from the menu or directly.
#[gpui::test]
fn row_changing_actions_wait_for_the_open_dialog(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-dialog-blocks");
    let path = local_file(
        &directory,
        "numbers.csv",
        &super::document_tests::numbered_csv(5),
    );

    let (document, window) = open_local_in_small_pages(cx, path);

    type_into_cell(&document, window, 0, 1, &long_value());
    set_overrides(
        &document,
        window,
        DialectOverrides {
            delimiter: Some(b','),
            ..DialectOverrides::default()
        },
    );

    for open_dialog in ["editor", "column prompt"] {
        match open_dialog {
            "editor" => assert!(start_cell_edit(&document, window, 0, 1)),
            _ => {
                select_column(&document, window, 0);
                run_pane_action(&document, window, "delimited-rename-column");
                assert!(column_prompt_is_open(&document, window));
            }
        }

        for id in ROW_CHANGING_ACTIONS {
            assert!(
                !pane_action_enabled(&document, window, id),
                "{id} is disabled while the {open_dialog} is open"
            );
        }

        window.update(|_, cx| {
            document.update(cx, |document, cx| {
                assert!(!document.can_load_more());

                document.insert_row_above(cx);
                document.add_column(cx);
                document.rename_active_column(cx);
                document.discard_changes(cx);
                document.load_more(cx);
                document.load_rest(cx);
                document.reload(cx);
                document.override_delimiter(b';', cx);
            })
        });
        window.run_until_parked();

        assert_eq!(shown_first_column(&document, window), ["0", "1"]);
        assert!(is_dirty(&document, window), "the edit was not discarded");
        assert!(!window.update(|_, cx| document.read(cx).is_rereading()));
        assert!(!window.update(|_, cx| document.read(cx).is_load_rest_prompt_open()));
        assert_eq!(column_titles(&document, window), ["id", "city"]);

        press(window, "escape");
        assert!(!cell_editor_is_open(&document, window));
        assert!(!column_prompt_is_open(&document, window));
    }

    assert!(pane_action_enabled(
        &document,
        window,
        "delimited-insert-above"
    ));
}

/// A save, the save key and the save of a close are refused while the
/// rest of the file loads, and the Save button takes no click.
#[gpui::test]
fn a_save_is_refused_while_the_rest_of_the_file_loads(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-save-during-rest");
    let bytes = super::document_tests::numbered_csv(5);
    let path = local_file(&directory, "numbers.csv", &bytes);

    let (document, window) = open_local_in_small_pages(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");

    let reader = start_loading_rest(&document, window);

    let checked = covered_ids(&document, window);
    assert!(
        !checked.iter().any(|id| id == "delimited-save"),
        "{checked:?}"
    );

    save(&document, window);
    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not save numbers.csv")
    );

    select(&document, window, 0);
    press(window, "ctrl-s");
    assert_eq!(toast_count(window), 2);

    let started = window.update(|window, cx| {
        let pane = DelimitedDocument::into_pane(document.clone(), cx);
        pane.save_for_close(window, cx)
    });
    window.run_until_parked();

    assert!(started);
    assert_eq!(save_results(&events), [false, false, false]);
    assert!(!asked_to_close(&events));
    assert_eq!(read(&path), bytes);
    assert!(is_dirty(&document, window));

    end_loading_rest(&document, window, reader);

    save(&document, window);
    assert_eq!(&read(&path)[..18], b"id,city\n0,Cusco\n1,");
}

/// A column prompt confirmed after the table became read-only says why the
/// column did not change.
#[gpui::test]
fn a_column_prompt_confirmed_while_read_only_reports_it(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-prompt-read-only");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    for (action, summary) in [
        (
            "delimited-add-column",
            "Could not add a column to cities.csv",
        ),
        (
            "delimited-rename-column",
            "Could not rename a column of cities.csv",
        ),
    ] {
        run_pane_action(&document, window, action);
        assert!(column_prompt_is_open(&document, window));

        window.update(|_, cx| {
            document.update(cx, |document, cx| {
                document.saving = true;
                document.sync_table_editing(cx);
            })
        });

        let toasts = toast_count(window);
        confirm_column_name(&document, window, "country");

        assert_eq!(toast_count(window), toasts + 1);
        assert_eq!(last_toast_title(window).as_deref(), Some(summary));
        assert_eq!(column_titles(&document, window), ["name", "city"]);

        window.update(|_, cx| {
            document.update(cx, |document, cx| {
                document.saving = false;
                document.sync_table_editing(cx);
            })
        });
    }
}

/// While the rest loads the footer shows one loading label and its cancel,
/// not Load more, and nothing in it runs past the pane.
#[gpui::test]
fn the_footer_shows_one_loading_state_while_the_rest_loads(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-footer-rest");
    let path = local_file(
        &directory,
        "numbers.csv",
        &super::document_tests::numbered_csv(5),
    );

    let (document, window) = open_local_in_small_pages(cx, path);

    let reader = start_loading_rest(&document, window);

    assert_eq!(
        ids_under(&document, window, "delimited-footer"),
        ["delimited-load-rest-cancel"]
    );
    assert_eq!(
        window.update(|_, cx| document.read(cx).progress_item()),
        Some("Loading the rest of the file…".to_string())
    );

    window.simulate_resize(gpui::size(gpui::px(320.0), gpui::px(480.0)));
    let footer = element_bounds(window, "delimited-footer");
    let cancel = element_bounds(window, "delimited-load-rest-cancel");
    assert!(
        cancel.right() <= footer.right(),
        "{cancel:?} inside {footer:?}"
    );

    end_loading_rest(&document, window, reader);
}

fn element_bounds(window: &mut VisualTestContext, id: &str) -> gpui::Bounds<gpui::Pixels> {
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    frame
        .nodes()
        .find(|(_, node)| node.id() == id)
        .map(|(_, node)| node.bounds())
        .unwrap_or_else(|| panic!("`{id}` is drawn"))
}

/// The keycap of Save is the save key a user presses, which the table
/// answers with a save, not the table's Ctrl+Enter.
#[gpui::test]
fn the_save_button_shows_the_save_key(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-save-keycap");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (_document, _window) = open_local(cx, path);

    let label = super::render::save_shortcut_label().expect("the save key is bound");
    let editor_save = dbflux_ui_base::keymap::shortcut_label(
        dbflux_app::keymap::ContextId::Editor,
        dbflux_app::keymap::Command::SaveQuery,
    );

    assert_eq!(Some(label.clone()), editor_save);
    assert!(!label.contains('↵'), "{label}");
}

/// The toolbar is two groups, the dialect controls and the file actions,
/// so it wraps as two blocks.
#[gpui::test]
fn the_toolbar_groups_the_dialect_controls_and_the_file_actions(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-toolbar-groups");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    set_overrides(
        &document,
        window,
        DialectOverrides {
            quote: Some(None),
            ..DialectOverrides::default()
        },
    );
    type_into_cell(&document, window, 0, 1, "Cusco");

    let dialect = ids_under(&document, window, "delimited-dialect-controls");
    for id in [
        "delimited-delimiter.dropdown-trigger",
        "delimited-quote.dropdown-trigger",
        "delimited-header",
        "delimited-encoding.dropdown-trigger",
        "delimited-dialect-reset",
    ] {
        assert!(dialect.iter().any(|entry| entry == id), "{id}: {dialect:?}");
    }

    let file_actions = [
        "delimited-reload",
        "delimited-insert-above",
        "delimited-add-column",
        "delimited-discard",
        "delimited-save",
    ];

    let mut actions = ids_under(&document, window, "delimited-file-actions");
    actions.sort();
    let mut expected = file_actions.map(str::to_string).to_vec();
    expected.sort();
    assert_eq!(actions, expected);

    let lefts: Vec<_> = file_actions
        .iter()
        .map(|id| element_bounds(window, id).left())
        .collect();
    assert!(
        lefts.windows(2).all(|pair| pair[0] < pair[1]),
        "the file actions are drawn in order: {lefts:?}"
    );

    window.simulate_resize(gpui::size(gpui::px(590.0), gpui::px(480.0)));
    let reload = element_bounds(window, "delimited-reload");
    let save = element_bounds(window, "delimited-save");
    assert!(
        save.top() - reload.top() < reload.size.height * 1.5,
        "the file actions stay on one line at 590 px: {reload:?} {save:?}"
    );
}

#[test]
fn the_load_of_the_rest_keeps_bytes_only_while_every_record_of_a_page_was_kept() {
    use super::columns::remaining_budget;
    use super::document::ReadPage;
    use dbflux_delimited::{Page, Record, RecordCount};

    let record = |start: u64, end: u64| Record {
        byte_range: start..end,
        fields: Vec::new(),
        had_replacements: false,
    };
    let read = |records: Vec<Record>, bytes: Option<Vec<u8>>| ReadPage {
        page: Page {
            first_record: 0,
            records,
        },
        record_count: RecordCount::IndexedSoFar(0),
        bytes,
    };

    let two = || vec![record(10, 14), record(14, 20)];

    assert_eq!(remaining_budget(100, &read(two(), Some(vec![0; 10]))), 90);
    assert_eq!(remaining_budget(100, &read(two(), Some(vec![0; 4]))), 0);
    assert_eq!(remaining_budget(100, &read(two(), None)), 0);
    assert_eq!(remaining_budget(100, &read(Vec::new(), None)), 100);
    assert_eq!(remaining_budget(5, &read(two(), Some(vec![0; 10]))), 0);
}
