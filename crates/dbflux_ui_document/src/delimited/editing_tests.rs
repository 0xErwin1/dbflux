//! Editing and saving a delimited file: the editable table, dirty state,
//! row operations, the save flow and its outcomes, and the interactions with
//! paging, dialect overrides and closing.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use dbflux_audit::query::AuditQueryFilter;
use dbflux_components::components::data_table::actions as table_actions;
use dbflux_components::components::data_table::model::CellValue;
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_components::components::data_table::{DataTableEvent, DataTableState};
use dbflux_delimited::DialectOverrides;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::keyboard_coverage::{FrameCapture, KeyboardPath, clickable_elements};
use dbflux_ui_base::toast::{ToastGlobal, ToastKind};
use gpui::{AppContext as _, Entity, Subscription, TestAppContext, VisualTestContext};

use super::document::DelimitedDocument;
use super::document::read_first_page;
use super::document_tests::{
    CITIES, SMALL_PAGES, WINDOWS_1252_NAMES, column_titles, connect_profile, covered_ids, dialect,
    first_column, has_more_records, last_toast_title, load_more, numbered_csv, open, open_local,
    open_local_in_small_pages, open_object_in_small_pages, pane_action_ids, row_count,
    run_pane_action, set_overrides, state, toast_count, warnings,
};
use super::source::{DelimitedLocation, open_source, read_version};
use super::tests::{BUCKET, FakeConnection, KEY, TestDirectory};
use crate::delimited::DelimitedWarning;
use crate::handle::DocumentEvent;
use crate::keyboard_coverage::DELIMITED;
use crate::keyboard_test_support::init_keyboard_runtime;
use crate::pane::PaneActionRun;
use crate::types::DocumentState;

pub(super) fn table_state(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Entity<DataTableState> {
    window.update(|_, cx| {
        document
            .read(cx)
            .table_state()
            .expect("a loaded document has a table")
            .clone()
    })
}

/// Types `text` into the cell at the visual `row` and `col` through the
/// table's inline editor and commits it, as Enter does.
pub(super) fn type_into_cell(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    row: usize,
    col: usize,
    text: &str,
) {
    let table_state = table_state(document, window);

    window.update(|window, cx| {
        table_state.update(cx, |state, cx| {
            assert!(
                state.start_editing(CellCoord::new(row, col), window, cx),
                "the cell at {row},{col} takes an editor"
            );

            let input = state
                .cell_input()
                .cloned()
                .expect("a short cell is edited inline");

            input.update(cx, |input, cx| {
                input.set_value(text.to_string(), window, cx)
            });
            state.stop_editing(true, cx);
        });
    });
    window.run_until_parked();
}

/// Stages `text` for a cell as an editor that commits it does. Used for the
/// values the inline editor does not take: long or multi-line ones.
pub(super) fn stage_cell(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    row: usize,
    col: usize,
    text: &str,
) {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.stage_cell_value(row, col, CellValue::text(text));
            cx.notify();
        });
    });
    window.run_until_parked();
}

pub(super) fn select(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    row: usize,
) {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.select_cell(CellCoord::new(row, 0), cx)
        });
    });
    window.run_until_parked();
}

pub(super) fn press(window: &mut VisualTestContext, keystrokes: &str) {
    window.simulate_keystrokes(keystrokes);
    window.run_until_parked();
}

pub(super) fn save(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) {
    window.update(|_, cx| document.update(cx, |document, cx| document.save(cx)));
    window.run_until_parked();
}

pub(super) fn is_dirty(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> bool {
    window.update(|_, cx| document.read(cx).is_dirty())
}

pub(super) fn has_pending_operations(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> bool {
    let table_state = table_state(document, window);

    window.update(|_, cx| table_state.read(cx).has_pending_operations())
}

pub(super) fn last_toast_kind(window: &mut VisualTestContext) -> Option<ToastKind> {
    window.update(|_, cx| cx.global::<ToastGlobal>().host.read(cx).last_toast_kind())
}

/// Records every event the document emits from now on.
pub(super) fn record_events(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> (Rc<RefCell<Vec<DocumentEvent>>>, Subscription) {
    let events: Rc<RefCell<Vec<DocumentEvent>>> = Rc::default();

    let subscription = window.update(|_, cx| {
        let events = events.clone();

        cx.subscribe(document, move |_, event: &DocumentEvent, _| {
            events.borrow_mut().push(event.clone());
        })
    });

    (events, subscription)
}

pub(super) fn save_results(events: &Rc<RefCell<Vec<DocumentEvent>>>) -> Vec<bool> {
    events
        .borrow()
        .iter()
        .filter_map(|event| match event {
            DocumentEvent::SaveFinished { succeeded } => Some(*succeeded),
            _ => None,
        })
        .collect()
}

pub(super) fn asked_to_close(events: &Rc<RefCell<Vec<DocumentEvent>>>) -> bool {
    events
        .borrow()
        .iter()
        .any(|event| matches!(event, DocumentEvent::RequestClose))
}

pub(super) fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).expect("the test file is readable")
}

/// The identity of the file at `path` on disk. A save replaces the file
/// with a new one, so an unchanged identity means nothing was written.
#[cfg(unix)]
fn inode(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;

    std::fs::metadata(path).expect("the test file exists").ino()
}

pub(super) fn local_file(directory: &TestDirectory, name: &str, bytes: &[u8]) -> PathBuf {
    directory.file(name, bytes).0
}

// -- Editable table -----------------------------------------------------------

#[gpui::test]
fn a_typed_cell_makes_the_document_dirty_and_the_tab_says_so(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("edit-dirty");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);
    let (events, _subscription) = record_events(&document, window);

    assert!(!is_dirty(&document, window));

    type_into_cell(&document, window, 0, 1, "Cusco");

    assert!(is_dirty(&document, window));
    assert_eq!(state(&document, window), DocumentState::Modified);
    assert_eq!(
        window.update(|_, cx| document.read(cx).change_summary()),
        Some("Unsaved edits to cities.csv".to_string())
    );
    assert!(
        events
            .borrow()
            .iter()
            .any(|event| matches!(event, DocumentEvent::MetaChanged)),
        "the tab is told the document became dirty"
    );

    window.update(|_, cx| {
        let pane = DelimitedDocument::into_pane(document.clone(), cx);

        assert_eq!(pane.meta_snapshot(cx).state, DocumentState::Modified);
        assert_eq!(
            pane.change_summary(cx),
            Some("Unsaved edits to cities.csv".to_string())
        );
    });
}

#[gpui::test]
fn a_file_that_cannot_be_saved_in_place_opens_read_only(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(CITIES);
    connection.store.omit_identity();

    let (app_state, profile_id) = connect_profile(cx, connection.clone());
    let (document, window) = open_object_in_small_pages(cx, app_state, profile_id, connection);

    assert_eq!(
        warnings(&document, window),
        [DelimitedWarning::CannotSaveInPlace]
    );

    let table_state = table_state(&document, window);

    window.update(|_, cx| {
        let state = table_state.read(cx);

        assert!(!state.is_editable());
        assert!(!state.is_insertable());
    });

    select(&document, window, 0);
    press(window, "a a");
    press(window, "d d");

    assert!(!has_pending_operations(&document, window));
    assert!(!is_dirty(&document, window));
}

/// A cell over the inline limit asks for a modal editor, which a later unit
/// adds. Until then the request opens nothing and stages nothing.
#[gpui::test]
fn a_long_or_multi_line_cell_asks_for_a_modal_editor_and_stages_nothing(cx: &mut TestAppContext) {
    let long = "x".repeat(150);
    let bytes = format!("name,note\nAna,{long}\nBo,\"two\nlines\"\n");

    let directory = TestDirectory::new("edit-modal");
    let path = local_file(&directory, "notes.csv", bytes.as_bytes());

    let (document, window) = open_local(cx, path);
    let table_state = table_state(&document, window);

    let requests: Rc<RefCell<Vec<(usize, usize)>>> = Rc::default();
    let _subscription = window.update(|_, cx| {
        let requests = requests.clone();

        cx.subscribe(&table_state, move |_, event: &DataTableEvent, _| {
            if let DataTableEvent::ModalEditRequested { row, col, .. } = event {
                requests.borrow_mut().push((*row, *col));
            }
        })
    });

    for row in [0, 1] {
        window.update(|window, cx| {
            table_state.update(cx, |state, cx| {
                state.start_editing(CellCoord::new(row, 1), window, cx);

                assert!(!state.is_editing(), "no inline editor opens");
            });
        });
        window.run_until_parked();
    }

    assert_eq!(*requests.borrow(), [(0, 1), (1, 1)]);
    assert!(!has_pending_operations(&document, window));
    assert!(!is_dirty(&document, window));
    assert_eq!(toast_count(window), 0);
}

// -- Saving -------------------------------------------------------------------

#[gpui::test]
fn a_save_without_edits_writes_nothing(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-clean");
    let path = local_file(&directory, "cities.csv", CITIES);

    #[cfg(unix)]
    let identity = inode(&path);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    save(&document, window);

    assert_eq!(read(&path), CITIES);
    #[cfg(unix)]
    assert_eq!(inode(&path), identity, "the file was not replaced");
    assert_eq!(save_results(&events), [true]);
    assert_eq!(toast_count(window), 0);
}

/// Every untouched record keeps its quoting and its line break. The edited
/// record is written again with the quoting it needs and its own line break.
#[gpui::test]
fn one_typed_cell_saves_only_its_record_and_reopens_clean(cx: &mut TestAppContext) {
    let original: &[u8] = b"\"name\",\"city\"\r\n\"Ana\",\"Lima\"\r\n\"Bo\",\"Quito\"\r\n";

    let directory = TestDirectory::new("save-one-cell");
    let (path, location) = directory.file("cities.csv", original);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");
    save(&document, window);

    assert_eq!(
        read(&path),
        b"\"name\",\"city\"\r\nAna,Cusco\r\n\"Bo\",\"Quito\"\r\n"
    );
    assert!(!is_dirty(&document, window));
    assert!(!has_pending_operations(&document, window));
    assert_eq!(state(&document, window), DocumentState::Clean);
    assert_eq!(first_column(&document, window), ["Ana", "Bo"]);
    assert_eq!(save_results(&events), [true]);
    assert_eq!(toast_count(window), 0);

    let current = read_version(&location).expect("the saved file has a version");
    assert_eq!(
        window.update(|_, cx| document.read(cx).source_version().cloned()),
        Some(current)
    );

    let cities = window.update(|_, cx| {
        let table_state = document
            .read(cx)
            .table_state()
            .expect("a loaded document has a table")
            .read(cx);

        table_state.model().rows[0].cells[1].edit_text()
    });
    assert_eq!(cities, "Cusco");
}

/// Both edits were lost before U5a: the table compared display text, which
/// collapses whitespace and cuts long values. The writer quotes a field that
/// ends in a space, so a reader that trims fields keeps it.
#[gpui::test]
fn a_whitespace_edit_and_a_tail_edit_of_a_long_value_are_saved(cx: &mut TestAppContext) {
    let long = "x".repeat(250);
    let original = format!("name,note\nAna,Lima\nBo,{long}\n");

    let directory = TestDirectory::new("save-whitespace");
    let path = local_file(&directory, "notes.csv", original.as_bytes());

    let (document, window) = open_local(cx, path.clone());

    let mut edited_long = "x".repeat(249);
    edited_long.push('y');

    stage_cell(&document, window, 0, 1, "Lima ");
    stage_cell(&document, window, 1, 1, &edited_long);

    assert!(is_dirty(&document, window));

    save(&document, window);

    assert_eq!(
        read(&path),
        format!("name,note\nAna,\"Lima \"\nBo,{edited_long}\n").into_bytes()
    );
    assert!(!is_dirty(&document, window));
}

#[gpui::test]
fn setting_a_cell_to_null_saves_an_empty_field(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-null");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    let table_state = table_state(&document, window);
    window.update(|_, cx| {
        table_state.update(cx, |state, cx| state.select_cell(CellCoord::new(0, 1), cx));
    });
    press(window, "ctrl-n");

    assert!(is_dirty(&document, window));

    save(&document, window);

    assert_eq!(read(&path), b"name,city\nAna,\nBo,Quito\n");
}

#[gpui::test]
fn a_row_added_below_is_saved_after_its_row(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-add-below");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    select(&document, window, 0);
    press(window, "a a");

    let table_state = table_state(&document, window);
    let visual_rows = window.update(|_, cx| {
        table_state
            .read(cx)
            .edit_buffer()
            .compute_visual_order()
            .len()
    });
    assert_eq!(visual_rows, 3);
    assert!(is_dirty(&document, window));

    type_into_cell(&document, window, 1, 0, "Cy");
    type_into_cell(&document, window, 1, 1, "Oslo");
    save(&document, window);

    assert_eq!(read(&path), b"name,city\nAna,Lima\nCy,Oslo\nBo,Quito\n");
    assert_eq!(first_column(&document, window), ["Ana", "Cy", "Bo"]);
    assert!(!is_dirty(&document, window));
}

#[gpui::test]
fn a_row_inserted_above_the_first_row_is_saved_first(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-insert-first");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    select(&document, window, 0);
    run_pane_action(&document, window, "delimited-insert-above");

    type_into_cell(&document, window, 0, 0, "Cy");
    type_into_cell(&document, window, 0, 1, "Oslo");
    save(&document, window);

    assert_eq!(read(&path), b"name,city\nCy,Oslo\nAna,Lima\nBo,Quito\n");
}

#[gpui::test]
fn a_row_inserted_above_a_middle_row_is_saved_before_it(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-insert-middle");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    select(&document, window, 1);
    run_pane_action(&document, window, "delimited-insert-above");

    type_into_cell(&document, window, 1, 0, "Cy");
    save(&document, window);

    assert_eq!(read(&path), b"name,city\nAna,Lima\nCy,\nBo,Quito\n");
}

#[gpui::test]
fn a_deleted_row_is_left_out_of_the_saved_file(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-delete");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    select(&document, window, 0);
    press(window, "d d");

    assert!(is_dirty(&document, window));

    save(&document, window);

    assert_eq!(read(&path), b"name,city\nBo,Quito\n");
    assert_eq!(first_column(&document, window), ["Bo"]);
}

#[gpui::test]
fn a_duplicated_row_is_saved_after_its_source_with_its_edits(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-duplicate");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 1, 1, "Cusco");
    select(&document, window, 1);
    press(window, "shift-a shift-a");

    save(&document, window);

    assert_eq!(read(&path), b"name,city\nAna,Lima\nBo,Cusco\nBo,Cusco\n");
}

#[gpui::test]
fn undoing_each_row_operation_leaves_nothing_to_save(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-undo");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    select(&document, window, 0);

    for operation in ["a a", "d d", "shift-a shift-a", "ctrl-n"] {
        press(window, operation);
        assert!(is_dirty(&document, window), "{operation} stages a change");

        press(window, "u");
        assert!(!is_dirty(&document, window), "undo drops {operation}");
    }

    run_pane_action(&document, window, "delimited-insert-above");
    assert!(is_dirty(&document, window));

    press(window, "u");
    assert!(!is_dirty(&document, window));

    type_into_cell(&document, window, 0, 1, "Cusco");
    select(&document, window, 0);
    press(window, "u");
    assert!(!is_dirty(&document, window));

    #[cfg(unix)]
    let identity = inode(&path);

    save(&document, window);

    assert_eq!(read(&path), CITIES);
    #[cfg(unix)]
    assert_eq!(inode(&path), identity);
}

#[gpui::test]
fn the_save_key_saves_the_table(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-key");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 0, 1, "Cusco");
    press(window, "ctrl-s");

    assert_eq!(read(&path), b"name,city\nAna,Cusco\nBo,Quito\n");
    assert!(!is_dirty(&document, window));
}

#[gpui::test]
fn edits_on_two_pages_survive_the_load_between_them_and_are_saved(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-two-pages");
    let path = local_file(&directory, "numbers.csv", &numbered_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path.clone());

    type_into_cell(&document, window, 0, 1, "Cusco");
    select(&document, window, 0);
    press(window, "a a");

    load_more(&document, window);

    assert_eq!(first_column(&document, window), ["0", "1", "2", "3"]);
    assert!(is_dirty(&document, window));

    let table_state = table_state(&document, window);
    window.update(|_, cx| {
        let buffer = table_state.read(cx).edit_buffer();

        assert!(buffer.is_cell_dirty(0, 1), "the first page's edit is kept");
        assert_eq!(buffer.pending_inserts().len(), 1);
        assert_eq!(buffer.base_row_count(), 4);
    });

    // Visual row 4 is base row 3: the insert sits after base row 0.
    type_into_cell(&document, window, 4, 1, "Quito");

    save(&document, window);

    assert_eq!(
        read(&path),
        b"id,city\n0,Cusco\n,\n1,Lima\n2,Lima\n3,Quito\n4,Lima\n"
    );
    assert!(!is_dirty(&document, window));
}

/// The value typed in an open editor is committed before the page lands,
/// because replacing the rows closes the editor.
#[gpui::test]
fn a_page_load_keeps_the_value_of_an_open_editor(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-editor-load");
    let path = local_file(&directory, "numbers.csv", &numbered_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);
    let table_state = table_state(&document, window);

    window.update(|window, cx| {
        table_state.update(cx, |state, cx| {
            state.start_editing(CellCoord::new(0, 1), window, cx);

            let input = state.cell_input().cloned().expect("an inline editor");
            input.update(cx, |input, cx| input.set_value("Cusco", window, cx));
        });
    });

    load_more(&document, window);

    window.update(|_, cx| {
        let state = table_state.read(cx);

        assert!(!state.is_editing());
        assert_eq!(state.model().rows.len(), 4);
        assert_eq!(
            state
                .edit_buffer()
                .get_cell(0, 1, &CellValue::text(""))
                .edit_text(),
            "Cusco"
        );
    });
}

#[gpui::test]
fn a_row_added_after_the_last_loaded_record_needs_the_next_page(cx: &mut TestAppContext) {
    let original = numbered_csv(5);

    let directory = TestDirectory::new("save-next-page");
    let path = local_file(&directory, "numbers.csv", &original);

    let (document, window) = open_local_in_small_pages(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    select(&document, window, 1);
    press(window, "a a");
    save(&document, window);

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not save numbers.csv")
    );
    assert_eq!(read(&path), original);
    assert!(is_dirty(&document, window));
    assert_eq!(save_results(&events), [false]);

    load_more(&document, window);
    save(&document, window);

    assert_eq!(
        read(&path),
        b"id,city\n0,Lima\n1,Lima\n,\n2,Lima\n3,Lima\n4,Lima\n"
    );
    assert_eq!(toast_count(window), 1);
}

#[gpui::test]
fn a_file_changed_elsewhere_is_not_overwritten_and_keeps_the_edits(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-changed");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 0, 1, "Cusco");

    let theirs: &[u8] = b"name,city\nAna,Lima\nBo,Quito\nCy,Oslo\n";
    std::fs::write(&path, theirs).expect("the test file is writable");

    save(&document, window);

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not save cities.csv")
    );
    assert_eq!(read(&path), theirs);
    assert!(is_dirty(&document, window));
    assert!(has_pending_operations(&document, window));
    assert_eq!(first_column(&document, window), ["Ana", "Bo"]);
}

/// The unencodable character fails the write after the version check, and
/// fixing the cell lets the next save through.
#[gpui::test]
fn a_failed_write_keeps_the_file_and_the_edits_and_can_be_retried(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-unencodable");
    let path = local_file(&directory, "names.csv", WINDOWS_1252_NAMES);

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 0, 1, "東京");
    save(&document, window);

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not save names.csv")
    );
    assert_eq!(read(&path), WINDOWS_1252_NAMES);
    assert!(is_dirty(&document, window));

    type_into_cell(&document, window, 0, 1, "Sevilla");
    save(&document, window);

    assert_eq!(
        read(&path),
        b"name;city\nJos\xE9;Sevilla\nMar\xEDa;C\xF3rdoba\n"
    );
    assert!(!is_dirty(&document, window));
    assert_eq!(toast_count(window), 1);
}

#[gpui::test]
fn a_second_save_during_a_save_is_ignored(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-twice");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.save(cx);
            assert!(document.is_saving());

            document.save(cx);
        });
    });
    window.run_until_parked();

    assert_eq!(read(&path), b"name,city\nAna,Cusco\nBo,Quito\n");
    assert_eq!(save_results(&events), [true]);
    assert_eq!(toast_count(window), 0);
    assert!(!window.update(|_, cx| document.read(cx).is_saving()));
}

/// Nothing changes the edits while a save runs: the save writes the edit
/// set it took when it started, and the reopened file would drop anything
/// staged after that. Every key and menu entry that edits is refused.
/// The document has no paste: the table has no paste action of its own.
#[gpui::test]
fn the_table_is_read_only_while_a_save_runs(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("save-read-only");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let table_state = table_state(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");
    type_into_cell(&document, window, 1, 1, "Oslo");
    select(&document, window, 0);

    let dirty_rows = |window: &mut VisualTestContext| {
        window.update(|_, cx| table_state.read(cx).edit_buffer().dirty_rows().len())
    };

    // The same dispatch reaches the table while it is editable.
    window.update(|window, cx| window.dispatch_action(Box::new(table_actions::Undo), cx));
    assert_eq!(dirty_rows(window), 1);
    window.update(|window, cx| window.dispatch_action(Box::new(table_actions::Redo), cx));
    assert_eq!(dirty_rows(window), 2);

    let menu = window.update(|_, cx| document.read(cx).pane_actions(&document));

    window.update(|_, cx| {
        document.update(cx, |document, cx| document.save(cx));

        let state = table_state.read(cx);
        assert!(!state.is_editable());
        assert!(!state.is_insertable());
    });

    window.update(|window, cx| {
        for action in [
            Box::new(table_actions::Undo) as Box<dyn gpui::Action>,
            Box::new(table_actions::Undo),
            Box::new(table_actions::Redo),
            Box::new(table_actions::SetNull),
            Box::new(table_actions::DeleteRow),
            Box::new(table_actions::AddRow),
            Box::new(table_actions::DuplicateRow),
        ] {
            window.dispatch_action(action, cx);
        }

        let edit_entries = [
            "delimited-insert-above",
            "delimited-discard",
            "delimited-reload",
        ];

        for action in menu {
            if !edit_entries.contains(&action.id.as_ref()) {
                continue;
            }

            if let PaneActionRun::Callback(run) = action.run {
                run(window, cx);
            }
        }
    });

    window.update(|_, cx| {
        let buffer = table_state.read(cx).edit_buffer();

        assert_eq!(buffer.dirty_rows().len(), 2, "no undo, redo or set null");
        assert!(buffer.pending_inserts().is_empty(), "no added row");
        assert!(buffer.pending_delete_rows().is_empty(), "no deleted row");
        assert!(document.read(cx).is_saving());
    });

    window.run_until_parked();

    assert_eq!(read(&path), b"name,city\nAna,Cusco\nBo,Oslo\n");
    assert!(!is_dirty(&document, window));

    window.update(|_, cx| {
        let state = table_state.read(cx);
        assert!(state.is_editable());
        assert!(state.is_insertable());
    });
}

#[gpui::test]
fn a_document_closed_during_a_save_completes_it_and_reports_nothing(cx: &mut TestAppContext) {
    init_keyboard_runtime(cx);

    let directory = TestDirectory::new("save-closed");
    let saved_path = local_file(&directory, "saved.csv", CITIES);
    let refused_path = local_file(&directory, "refused.csv", CITIES);

    let open_closed = |cx: &mut TestAppContext, path: PathBuf| {
        let document = cx.update(|cx| cx.new(|cx| DelimitedDocument::open_local(path, cx)));
        cx.run_until_parked();

        cx.update(|cx| {
            let table_state = document
                .read(cx)
                .table_state()
                .expect("a loaded document has a table")
                .clone();

            table_state.update(cx, |state, cx| {
                state.stage_cell_value(0, 1, CellValue::text("Cusco"));
                cx.notify();
            });
        });
        cx.run_until_parked();

        document
    };

    let saved = open_closed(cx, saved_path.clone());
    let refused = open_closed(cx, refused_path.clone());

    std::fs::write(&refused_path, b"name,city\n").expect("the test file is writable");

    cx.update(|cx| {
        saved.update(cx, |document, cx| document.save(cx));
        refused.update(cx, |document, cx| document.save(cx));
    });

    drop(saved);
    drop(refused);
    cx.update(|_| {});
    cx.run_until_parked();

    assert_eq!(read(&saved_path), b"name,city\nAna,Cusco\nBo,Quito\n");
    assert_eq!(read(&refused_path), b"name,city\n");

    let toasts = cx.update(|cx| cx.global::<ToastGlobal>().host.read(cx).toast_count());
    assert_eq!(toasts, 0);
}

// -- Objects ------------------------------------------------------------------

fn open_object(
    cx: &mut TestAppContext,
    connection: Arc<FakeConnection>,
) -> (
    Entity<DelimitedDocument>,
    Entity<AppStateEntity>,
    &mut VisualTestContext,
) {
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (document, window) = open(cx, {
        let app_state = app_state.clone();

        move |cx| {
            DelimitedDocument::open_object_with(
                app_state,
                profile_id,
                connection,
                BUCKET.to_string(),
                KEY.to_string(),
                SMALL_PAGES,
                cx,
            )
        }
    });

    (document, app_state, window)
}

fn save_audit_events(
    app_state: &Entity<AppStateEntity>,
    window: &mut VisualTestContext,
    action: &str,
) -> Vec<dbflux_storage::repositories::audit::AuditEventDto> {
    window.update(|_, cx| {
        app_state
            .read(cx)
            .audit_service()
            .query_extended(&AuditQueryFilter {
                action: Some(action.to_string()),
                ..AuditQueryFilter::default()
            })
            .expect("the audit log is readable")
    })
}

#[gpui::test]
fn an_object_save_replaces_the_object_reopens_it_and_is_audited_once(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(CITIES);
    let (document, app_state, window) = open_object(cx, connection.clone());

    type_into_cell(&document, window, 0, 1, "Cusco");
    save(&document, window);

    assert_eq!(
        connection.store.bytes(),
        b"name,city\nAna,Cusco\nBo,Quito\n"
    );
    assert!(!is_dirty(&document, window));
    assert_eq!(toast_count(window), 0);

    let location = DelimitedLocation::Object {
        connection: connection.clone(),
        bucket: BUCKET.to_string(),
        key: KEY.to_string(),
    };
    let current = read_version(&location).expect("the object has a version");
    assert_eq!(
        window.update(|_, cx| document.read(cx).source_version().cloned()),
        Some(current)
    );

    let audited = save_audit_events(&app_state, window, "object_edit_save");
    assert_eq!(audited.len(), 1);

    let event = &audited[0];
    assert_eq!(event.category.as_deref(), Some("object_storage"));
    assert_eq!(event.outcome.as_deref(), Some("success"));
    assert_eq!(event.object_type.as_deref(), Some("object"));
    assert_eq!(event.object_id.as_deref(), Some("reports/2026/cities.csv"));
    assert!(
        event
            .summary
            .as_deref()
            .is_some_and(|summary| summary.contains("s3://reports/2026/cities.csv")),
        "{:?}",
        event.summary
    );

    type_into_cell(&document, window, 1, 1, "Oslo");
    save(&document, window);

    assert_eq!(
        save_audit_events(&app_state, window, "object_edit_save").len(),
        2
    );
}

#[gpui::test]
fn an_object_saved_without_a_readable_version_warns_once_and_reopens_clean(
    cx: &mut TestAppContext,
) {
    let connection = FakeConnection::with_object(CITIES);
    connection
        .store
        .fail_one_head_after_an_upload_with("SlowDown: reduce the rate");

    let (document, app_state, window) = open_object(cx, connection.clone());
    let (events, _subscription) = record_events(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");
    save(&document, window);

    assert_eq!(
        connection.store.bytes(),
        b"name,city\nAna,Cusco\nBo,Quito\n"
    );
    assert!(!is_dirty(&document, window));
    assert_eq!(first_column(&document, window), ["Ana", "Bo"]);
    assert_eq!(toast_count(window), 1);
    assert_eq!(last_toast_kind(window), Some(ToastKind::Warning));
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("cities.csv was saved, but its new version could not be read")
    );
    assert_eq!(save_results(&events), [true]);
    assert_eq!(
        save_audit_events(&app_state, window, "object_edit_save").len(),
        1
    );
}

#[gpui::test]
fn a_refused_object_save_is_audited_as_a_failure_and_keeps_the_edits(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(CITIES);
    let (document, app_state, window) = open_object(cx, connection.clone());

    type_into_cell(&document, window, 0, 1, "Cusco");
    connection
        .store
        .fail_uploads_with("AccessDenied: no write permission");
    save(&document, window);

    assert_eq!(connection.store.bytes(), CITIES);
    assert!(is_dirty(&document, window));
    assert_eq!(toast_count(window), 1);
    assert!(save_audit_events(&app_state, window, "object_edit_save").is_empty());
    assert_eq!(
        save_audit_events(&app_state, window, "object_edit_save_failed").len(),
        1
    );
}

// -- Dialect, discard and close -----------------------------------------------

#[gpui::test]
fn a_dialect_override_is_refused_while_dirty_and_works_after_a_discard(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("edit-override");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    type_into_cell(&document, window, 0, 1, "Cusco");

    let headerless = DialectOverrides {
        has_header: Some(false),
        ..DialectOverrides::default()
    };
    set_overrides(&document, window, headerless);

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not re-read cities.csv")
    );
    assert_eq!(
        window.update(|_, cx| document.read(cx).dialect_overrides()),
        DialectOverrides::default()
    );
    assert!(is_dirty(&document, window));
    assert_eq!(row_count(&document, window), 2);

    run_pane_action(&document, window, "delimited-discard");

    assert!(!is_dirty(&document, window));
    assert!(!has_pending_operations(&document, window));

    set_overrides(&document, window, headerless);

    assert_eq!(row_count(&document, window), 3);
    assert_eq!(toast_count(window), 1);
}

/// Types `text` into the inline editor of the cell at `row`, `col` and leaves
/// the editor open, as a user does before pressing Enter.
fn type_without_committing(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    row: usize,
    col: usize,
    text: &str,
) {
    let table_state = table_state(document, window);

    window.update(|window, cx| {
        table_state.update(cx, |state, cx| {
            assert!(state.start_editing(CellCoord::new(row, col), window, cx));

            let input = state
                .cell_input()
                .cloned()
                .expect("a short cell is edited inline");

            input.update(cx, |input, cx| {
                input.set_value(text.to_string(), window, cx)
            });
        });
    });
    window.run_until_parked();
}

/// A value still in the inline editor is a pending edit too: a dialect
/// override or a reload commits it first, then refuses the reread instead
/// of dropping it.
#[gpui::test]
fn a_reread_commits_the_open_inline_edit_and_is_refused(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("edit-reread-open-editor");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    type_without_committing(&document, window, 0, 1, "Cusco");

    set_overrides(
        &document,
        window,
        DialectOverrides {
            has_header: Some(false),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(toast_count(window), 1);
    assert!(is_dirty(&document, window));
    assert_eq!(row_count(&document, window), 2);

    type_without_committing(&document, window, 1, 1, "Oslo");

    window.update(|_, cx| document.update(cx, |document, cx| document.reload(cx)));
    window.run_until_parked();

    assert_eq!(toast_count(window), 2);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not re-read cities.csv")
    );

    save(&document, window);

    assert_eq!(read(&path), b"name,city\nAna,Cusco\nBo,Oslo\n");
}

#[gpui::test]
fn discard_drops_row_edits_column_changes_and_the_undo_history(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("edit-discard");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    type_into_cell(&document, window, 0, 1, "Cusco");
    select(&document, window, 1);
    press(window, "a a");

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document
                .page_model_mut_for_test()
                .expect("a loaded document has a page model")
                .rename_column(0, "person".to_string())
                .expect("the column exists");
            document.refresh_dirty(cx);
        });
    });

    run_pane_action(&document, window, "delimited-discard");

    assert!(!is_dirty(&document, window));
    assert_eq!(column_titles(&document, window), ["name", "city"]);

    let table_state = table_state(&document, window);
    window.update(|_, cx| {
        let buffer = table_state.read(cx).edit_buffer();

        assert!(!buffer.has_pending_operations());
        assert!(!buffer.can_undo());
    });
}

#[gpui::test]
fn a_dirty_tab_saves_and_closes(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("close-save");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");

    let started = window.update(|window, cx| {
        let pane = DelimitedDocument::into_pane(document.clone(), cx);
        pane.save_for_close(window, cx)
    });
    window.run_until_parked();

    assert!(started);
    assert_eq!(read(&path), b"name,city\nAna,Cusco\nBo,Quito\n");
    assert_eq!(save_results(&events), [true]);
    assert!(asked_to_close(&events));
}

/// The failed save drops the close it was started for, so the save that
/// lands after the user fixed the cell does not close the tab.
#[gpui::test]
fn a_failed_save_keeps_the_tab_open(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("close-failed");
    let path = local_file(&directory, "names.csv", WINDOWS_1252_NAMES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    type_into_cell(&document, window, 0, 1, "東京");

    let started = window.update(|window, cx| {
        let pane = DelimitedDocument::into_pane(document.clone(), cx);
        pane.save_for_close(window, cx)
    });
    window.run_until_parked();

    assert!(started);
    assert_eq!(save_results(&events), [false]);
    assert!(!asked_to_close(&events));
    assert!(is_dirty(&document, window));
    assert_eq!(read(&path), WINDOWS_1252_NAMES);

    type_into_cell(&document, window, 0, 1, "Sevilla");
    save(&document, window);

    assert_eq!(save_results(&events), [false, true]);
    assert!(!asked_to_close(&events));
}

// -- Coverage and strings -----------------------------------------------------

#[gpui::test]
fn the_edit_controls_are_covered_and_take_clicks_only_when_they_apply(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("edit-coverage");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    let menu = pane_action_ids(&document, window);
    assert!(
        menu.iter().any(|id| id == "delimited-insert-above"),
        "{menu:?}"
    );
    assert!(menu.iter().any(|id| id == "delimited-discard"), "{menu:?}");

    let checked = covered_ids(&document, window);
    assert!(
        checked.iter().any(|id| id == "delimited-insert-above"),
        "{checked:?}"
    );
    for disabled in ["delimited-save", "delimited-discard"] {
        assert!(
            !checked.iter().any(|id| id == disabled),
            "{disabled} takes no click on a clean file: {checked:?}"
        );
    }

    type_into_cell(&document, window, 0, 1, "Cusco");

    let checked = covered_ids(&document, window);
    for enabled in [
        "delimited-save",
        "delimited-discard",
        "delimited-insert-above",
    ] {
        assert!(checked.iter().any(|id| id == enabled), "{checked:?}");
    }
}

#[test]
fn the_registry_reaches_the_edit_controls_from_the_keyboard() {
    let path_of = |id: &str| {
        DELIMITED
            .entries
            .iter()
            .find(|(pattern, _)| *pattern == id)
            .map(|(_, path)| *path)
    };

    assert_eq!(
        path_of("delimited-save"),
        Some(KeyboardPath::Command(dbflux_app::keymap::Command::SaveRow))
    );
    assert_eq!(
        path_of("delimited-discard"),
        Some(KeyboardPath::Menu("delimited-discard"))
    );
    assert_eq!(
        path_of("delimited-insert-above"),
        Some(KeyboardPath::Menu("delimited-insert-above"))
    );
    assert_eq!(
        path_of("delimited-reload"),
        Some(KeyboardPath::Command(
            dbflux_app::keymap::Command::RefreshSchema
        ))
    );
}

#[test]
fn the_editing_strings_resolve_in_every_locale() {
    for key in [
        "document.delimited.unsaved_summary",
        "document.delimited.action.save",
        "document.delimited.action.saving",
        "document.delimited.action.discard",
        "document.delimited.action.insert_above",
        "document.delimited.error.save_failed",
        "document.delimited.error.source_changed",
        "document.delimited.error.next_page_required",
        "document.delimited.error.full_load_required",
        "document.delimited.error.saved_version_unknown",
        "document.delimited.error.reopen_failed",
        "document.delimited.error.unsaved_changes_block_reread",
        "document.delimited.error.save_while_rereading",
        "document.delimited.error.add_row_failed",
        "document.delimited.error.no_columns",
        "document.delimited.action.reload",
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

/// The confirmed sequence: an edit and its undo leave the file clean, an
/// override starts a reread, redo and save follow before it lands. The
/// table is read-only during the reread, so redo changes nothing, the save
/// has nothing to write, and the document ends on the overridden dialect
/// with rows read under it. Every order of the reread and the save ends the
/// same way.
#[gpui::test(iterations = 64)]
fn a_save_during_a_reread_never_mixes_delimiters(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-reread-save");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 0, 1, "Oslo");
    select(&document, window, 0);
    press(window, "u");
    assert!(!is_dirty(&document, window));

    window.update(|window, cx| {
        document.update(cx, |document, cx| document.override_delimiter(b';', cx));
        window.dispatch_action(Box::new(table_actions::Redo), cx);
    });
    window.update(|_, cx| document.update(cx, |document, cx| document.save(cx)));
    window.run_until_parked();

    assert_eq!(dialect(&document, window).delimiter, b';');
    assert_eq!(column_titles(&document, window), ["name,city"]);
    assert_eq!(read(&path), CITIES);
    assert!(!is_dirty(&document, window));

    type_into_cell(&document, window, 0, 0, "Ana,Oslo");
    save(&document, window);

    assert_eq!(read(&path), b"name,city\nAna,Oslo\nBo,Quito\n");
}

/// Whatever made the document dirty during a reread, a save then is
/// refused and says why: the edits belong to rows the reread replaces.
#[gpui::test]
fn a_save_is_refused_while_the_file_is_read_again(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-reread-refused");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);
    let table_state = table_state(&document, window);

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.override_delimiter(b';', cx);
            assert!(document.is_rereading());
        });

        table_state.update(cx, |state, cx| {
            state
                .edit_buffer_mut()
                .set_cell(0, 1, CellValue::text("Oslo"));
            cx.notify();
        });
    });

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            assert!(document.is_dirty());
            assert!(!document.can_save());

            document.save(cx);
            assert!(!document.is_saving());
        });
    });
    window.run_until_parked();

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not save cities.csv")
    );
    assert_eq!(save_results(&events), [false]);
    assert_eq!(read(&path), CITIES);
}

/// Undoing a delete left the edited row without its dirty state, so the
/// table's save key found nothing to save.
#[gpui::test]
fn the_save_key_saves_an_edit_whose_row_was_deleted_and_restored(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-undo-delete");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 0, 1, "Cusco");
    select(&document, window, 0);
    press(window, "d d");
    press(window, "u");

    assert!(is_dirty(&document, window));

    press(window, "ctrl-s");

    assert_eq!(read(&path), b"name,city\nAna,Cusco\nBo,Quito\n");
    assert!(!is_dirty(&document, window));
}

/// The ids of the clickable elements whose element path runs through
/// `ancestor`.
pub(super) fn ids_under(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    ancestor: &str,
) -> Vec<String> {
    covered_ids(document, window);

    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    clickable_elements(&frame)
        .into_iter()
        .filter(|element| element.path.split('.').any(|segment| segment == ancestor))
        .map(|element| element.id)
        .collect()
}

/// The footer is a fixed-height row that does not wrap, so the edit
/// controls sit in the toolbar, which wraps.
#[gpui::test]
fn the_edit_controls_sit_in_the_toolbar_and_load_more_in_the_footer(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-toolbar");
    let path = local_file(&directory, "numbers.csv", &numbered_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);

    type_into_cell(&document, window, 0, 1, "Cusco");

    let toolbar = ids_under(&document, window, "delimited-toolbar");
    for control in [
        "delimited-insert-above",
        "delimited-discard",
        "delimited-save",
        "delimited-reload",
    ] {
        assert!(
            toolbar.iter().any(|id| id == control),
            "{control}: {toolbar:?}"
        );
    }
    assert!(!toolbar.iter().any(|id| id == "delimited-load-more"));

    let footer = ids_under(&document, window, "delimited-footer");
    assert_eq!(footer, ["delimited-load-more"]);
}

/// After a refused save the user discards, reloads the file from its source
/// with the dialect in effect, and edits it again.
#[gpui::test]
fn reload_reads_a_file_changed_elsewhere_after_a_discard(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-reload");
    let (path, location) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 0, 1, "Cusco");

    let theirs: &[u8] = b"name,city\nAna,Lima\nBo,Quito\nCy,Oslo\n";
    std::fs::write(&path, theirs).expect("the test file is writable");

    save(&document, window);
    assert_eq!(toast_count(window), 1);

    run_pane_action(&document, window, "delimited-reload");

    assert_eq!(toast_count(window), 2, "a dirty document is not reloaded");
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not re-read cities.csv")
    );
    assert_eq!(first_column(&document, window), ["Ana", "Bo"]);

    run_pane_action(&document, window, "delimited-discard");
    press(window, "f5");

    assert_eq!(first_column(&document, window), ["Ana", "Bo", "Cy"]);
    assert_eq!(
        window.update(|_, cx| document.read(cx).source_version().cloned()),
        Some(read_version(&location).expect("the file has a version"))
    );
    assert_eq!(toast_count(window), 2);

    type_into_cell(&document, window, 2, 1, "Bergen");
    save(&document, window);

    assert_eq!(read(&path), b"name,city\nAna,Lima\nBo,Quito\nCy,Bergen\n");
}

#[test]
fn the_source_changed_message_names_the_reload_action() {
    let message = dbflux_i18n::t!("document.delimited.error.source_changed");
    let reload = dbflux_i18n::t!("document.delimited.action.reload");

    assert!(message.contains(&reload), "{message}");
}

/// The pages loaded before a save are loaded again after it, and the active
/// cell stays where it was.
#[gpui::test]
fn a_save_reads_again_the_pages_that_were_loaded(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-pages");
    let path = local_file(&directory, "numbers.csv", &numbered_csv(9));

    let (document, window) = open_local_in_small_pages(cx, path);

    load_more(&document, window);
    load_more(&document, window);

    type_into_cell(&document, window, 4, 1, "Cusco");

    let table_state = table_state(&document, window);
    window.update(|_, cx| {
        table_state.update(cx, |state, cx| state.select_cell(CellCoord::new(5, 1), cx));
    });

    save(&document, window);

    assert_eq!(
        first_column(&document, window),
        ["0", "1", "2", "3", "4", "5"]
    );
    assert!(has_more_records(&document, window));
    assert_eq!(
        window.update(|_, cx| table_state.read(cx).selection().active),
        Some(CellCoord::new(5, 1))
    );
}

/// The save landed and every version read after it failed: one report
/// covers both.
#[gpui::test]
fn a_save_that_cannot_be_read_back_reports_once(cx: &mut TestAppContext) {
    let connection = FakeConnection::with_object(CITIES);
    connection
        .store
        .fail_heads_after_an_upload_with("SlowDown: reduce the rate");

    let (document, _app_state, window) = open_object(cx, connection.clone());
    let (events, _subscription) = record_events(&document, window);

    type_into_cell(&document, window, 0, 1, "Cusco");
    save(&document, window);

    assert_eq!(
        connection.store.bytes(),
        b"name,city\nAna,Cusco\nBo,Quito\n"
    );
    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("cities.csv was saved, but could not be opened again")
    );
    assert_eq!(save_results(&events), [true]);
    assert_eq!(state(&document, window), DocumentState::Error);
}

/// A row of a table without columns has no field, which the writer renders
/// as an empty quoted field and the reopened file reads as nothing.
#[gpui::test]
fn a_row_cannot_be_added_to_a_file_without_columns(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-no-columns");
    let path = local_file(&directory, "empty.csv", b"");

    let (document, window) = open_local(cx, path.clone());

    press(window, "a a");

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window).as_deref(),
        Some("Could not add a row to empty.csv")
    );

    run_pane_action(&document, window, "delimited-insert-above");

    assert_eq!(toast_count(window), 2);
    assert!(!has_pending_operations(&document, window));
    assert!(!is_dirty(&document, window));

    save(&document, window);
    assert_eq!(read(&path), b"");
}

/// The rows a save reopens were read with a dialect, and that dialect is the
/// document's from then on, whatever it was when the save started.
#[gpui::test]
fn the_dialect_is_always_the_one_the_rows_were_read_with(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("fix-dialect");
    let (path, location) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    let semicolon = dbflux_delimited::Dialect {
        delimiter: b';',
        ..dialect(&document, window)
    };
    let (source, version) = open_source(&location).expect("the test file opens");
    let Ok(reopened) = read_first_page(source, version, semicolon, SMALL_PAGES) else {
        panic!("the file reads with a semicolon");
    };

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document
                .loaded_mut()
                .expect("a loaded document")
                .show_saved(reopened, cx);
        });
    });

    assert_eq!(dialect(&document, window).delimiter, b';');
    assert_eq!(column_titles(&document, window), ["name,city"]);
}
