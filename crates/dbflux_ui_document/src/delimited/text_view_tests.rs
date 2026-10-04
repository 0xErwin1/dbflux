//! The text view of a delimited file: switching between it and the table by
//! key and by mouse, what survives a switch, and the refresh of the text
//! after the file changes under it.

use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::components::data_table::model::{CellValue, InsertAnchor, VisualRowSource};
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_components::controls::InputEvent;
use dbflux_delimited::DialectOverrides;
use gpui::{Entity, Focusable as _, TestAppContext, VisualTestContext};

use super::columns_tests::click;
use super::document::DelimitedDocument;
use super::document_tests::{
    CITIES, SMALL_PAGES, WINDOWS_1252_NAMES, connect_profile, covered_ids, encoding,
    has_more_records, load_more, numbered_csv, open_local, open_local_in_small_pages,
    open_object_in_small_pages, pane_action_ids, set_overrides,
};
use super::editing_tests::{
    asked_to_close, has_pending_operations, is_dirty, local_file, press, read, record_events,
    save_results, select, table_state, type_into_cell,
};
use super::tests::{FakeConnection, TestDirectory};
use super::text::{TextEnd, TextRow};
use super::text_view::{DelimitedView, TextMode};

pub(super) fn view(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> DelimitedView {
    window.update(|_, cx| document.read(cx).view())
}

pub(super) fn mode(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> TextMode {
    window.update(|_, cx| document.read(cx).text_mode())
}

pub(super) fn context(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> ContextId {
    window.update(|_, cx| document.read(cx).active_context())
}

/// The text the text view's editor holds.
pub(super) fn text(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> String {
    window.update(|_, cx| {
        document
            .read(cx)
            .text_input()
            .expect("the text view was shown")
            .read(cx)
            .value()
            .to_string()
    })
}

/// The line the text view's cursor is on.
fn cursor_line(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> u32 {
    window.update(|_, cx| {
        document
            .read(cx)
            .text_input()
            .expect("the text view was shown")
            .read(cx)
            .cursor_position()
            .line
    })
}

fn text_is_focused(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> bool {
    window.update(|window, cx| {
        document
            .read(cx)
            .text_input()
            .is_some_and(|input| input.focus_handle(cx).is_focused(window))
    })
}

fn table_is_focused(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> bool {
    let table_state = table_state(document, window);

    window.update(|window, cx| table_state.read(cx).focus_handle().is_focused(window))
}

fn active_cell(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Option<CellCoord> {
    let table_state = table_state(document, window);

    window.update(|_, cx| table_state.read(cx).selection().active)
}

pub(super) fn text_end(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> TextEnd {
    window.update(|_, cx| {
        document
            .read(cx)
            .rendered_text()
            .expect("the text renders")
            .end
    })
}

fn text_rows(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> Vec<TextRow> {
    window.update(|_, cx| {
        document
            .read(cx)
            .rendered_text()
            .expect("the text renders")
            .records
            .iter()
            .map(|record| record.row)
            .collect()
    })
}

fn notes(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> usize {
    window.update(|_, cx| document.read(cx).text_notes().len())
}

pub(super) fn show(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    view: DelimitedView,
) {
    window.update(|_, cx| document.update(cx, |document, cx| document.show_view(view, cx)));
    window.run_until_parked();
}

// -- Switching by key ----------------------------------------------------------

#[gpui::test]
fn t_shows_the_text_at_the_active_row_and_escape_then_t_returns_to_the_table(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("text-view-keys");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);
    select(&document, window, 1);
    assert!(table_is_focused(&document, window));

    press(window, "t");

    assert_eq!(view(&document, window), DelimitedView::Text);
    assert_eq!(text(&document, window), String::from_utf8_lossy(CITIES));
    assert!(text_is_focused(&document, window));
    assert_eq!(context(&document, window), ContextId::TextInput);
    assert_eq!(cursor_line(&document, window), 2, "the line of row 1");

    // The editor takes `t` as text, so the view stays.
    press(window, "t");
    assert_eq!(view(&document, window), DelimitedView::Text);
    assert_eq!(text(&document, window), "name,city\nAna,Lima\ntBo,Quito\n");

    press(window, "escape");
    assert_eq!(context(&document, window), ContextId::Results);
    assert!(!text_is_focused(&document, window));

    press(window, "t");

    assert_eq!(view(&document, window), DelimitedView::Table);
    assert!(table_is_focused(&document, window));
    assert_eq!(active_cell(&document, window), Some(CellCoord::new(1, 0)));
    assert_eq!(shown_rows(&document, window), ["Ana|Lima", "tBo|Quito"]);
}

#[gpui::test]
fn enter_gives_the_keyboard_back_to_the_text(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-enter");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    press(window, "t");
    press(window, "escape");
    assert!(!text_is_focused(&document, window));

    press(window, "enter");

    assert!(text_is_focused(&document, window));
    assert_eq!(context(&document, window), ContextId::TextInput);
}

#[gpui::test]
fn shift_t_switches_the_text_between_raw_and_aligned(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-mode-key");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    press(window, "t");
    press(window, "escape");
    press(window, "shift-t");

    assert_eq!(mode(&document, window), TextMode::Aligned);
    assert_eq!(
        text(&document, window),
        "name | city\nAna  | Lima\nBo   | Quito\n"
    );

    press(window, "shift-t");

    assert_eq!(mode(&document, window), TextMode::Raw);
    assert_eq!(text(&document, window), String::from_utf8_lossy(CITIES));
}

#[gpui::test]
fn the_cursor_stays_on_its_row_when_the_mode_changes(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-mode-cursor");
    let (path, _) = directory.file("lines.csv", b"a,b\n\"x\ny\",1\nlast,2\n");

    let (document, window) = open_local(cx, path);
    select(&document, window, 1);

    press(window, "t");
    assert_eq!(
        cursor_line(&document, window),
        3,
        "raw: the quoted line break"
    );

    press(window, "escape");
    press(window, "shift-t");
    assert_eq!(
        cursor_line(&document, window),
        2,
        "aligned: one line per row"
    );

    press(window, "shift-t");
    assert_eq!(cursor_line(&document, window), 3);
}

#[gpui::test]
fn shift_t_in_the_table_leaves_the_mode_alone(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-mode-table");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    let handled = window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.dispatch_command(Command::CycleResultView, window, cx)
        })
    });

    assert!(!handled);
    assert_eq!(mode(&document, window), TextMode::Raw);
}

// -- Switching by mouse --------------------------------------------------------

#[gpui::test]
fn the_view_controls_switch_by_mouse_even_while_the_editor_holds_the_keyboard(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("text-view-mouse");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);
    select(&document, window, 1);

    click(window, "segmented-delimited-view-text");

    assert_eq!(view(&document, window), DelimitedView::Text);
    assert!(text_is_focused(&document, window));

    click(window, "segmented-delimited-text-mode-aligned");
    assert_eq!(mode(&document, window), TextMode::Aligned);
    assert!(text(&document, window).starts_with("name | city\n"));

    click(window, "segmented-delimited-text-mode-raw");
    assert_eq!(mode(&document, window), TextMode::Raw);

    window.update(|window, cx| document.update(cx, |document, cx| document.focus(window, cx)));
    window.run_until_parked();
    assert!(text_is_focused(&document, window));

    click(window, "segmented-delimited-view-table");

    assert_eq!(view(&document, window), DelimitedView::Table);
    assert!(table_is_focused(&document, window));
    assert_eq!(context(&document, window), ContextId::Results);
    assert_eq!(active_cell(&document, window), Some(CellCoord::new(1, 0)));
}

// -- What survives a switch ------------------------------------------------------

#[gpui::test]
fn switching_keeps_the_pending_edits_both_ways(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-edits");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    type_into_cell(&document, window, 0, 1, "Cusco");

    let table = table_state(&document, window);
    window.update(|_, cx| {
        table.update(cx, |state, cx| {
            state.edit_buffer_mut().add_pending_insert_at(
                InsertAnchor::BeforeFirst,
                vec![
                    dbflux_components::components::data_table::model::CellValue::text("Zed"),
                    dbflux_components::components::data_table::model::CellValue::text("Oslo"),
                ],
            );
            cx.notify();
        });
    });
    window.run_until_parked();

    press(window, "t");

    assert_eq!(
        text(&document, window),
        "name,city\nZed,Oslo\nAna,Cusco\nBo,Quito\n"
    );
    assert!(is_dirty(&document, window));

    press(window, "escape");
    press(window, "t");

    assert!(is_dirty(&document, window));
    assert!(has_pending_operations(&document, window));

    press(window, "t");
    assert_eq!(
        text(&document, window),
        "name,city\nZed,Oslo\nAna,Cusco\nBo,Quito\n"
    );
}

#[gpui::test]
fn an_open_cell_editor_is_committed_before_the_text_is_shown(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-inline");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    let table = table_state(&document, window);
    window.update(|window, cx| {
        table.update(cx, |state, cx| {
            assert!(state.start_editing(CellCoord::new(1, 1), window, cx));

            let input = state.cell_input().cloned().expect("an inline editor");
            input.update(cx, |input, cx| {
                input.set_value("Lima".to_string(), window, cx)
            });
        });
    });
    window.run_until_parked();

    click(window, "segmented-delimited-view-text");

    assert_eq!(text(&document, window), "name,city\nAna,Lima\nBo,Lima\n");
    assert!(is_dirty(&document, window));
}

// -- The text follows the file ---------------------------------------------------

#[gpui::test]
fn the_text_follows_load_more_and_says_when_more_records_follow(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-load-more");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);

    press(window, "t");

    assert_eq!(text(&document, window), "id,city\n0,Lima\n1,Lima\n");
    assert_eq!(text_end(&document, window), TextEnd::MoreInFile);
    assert_eq!(
        notes(&document, window),
        2,
        "more in the file, and the edit note"
    );

    // The next page key, once the editor gave up the keyboard.
    press(window, "escape");
    press(window, "]");

    assert_eq!(
        text(&document, window),
        "id,city\n0,Lima\n1,Lima\n2,Lima\n3,Lima\n"
    );

    load_more(&document, window);
    load_more(&document, window);

    assert_eq!(
        text(&document, window),
        String::from_utf8_lossy(&numbered_csv(5))
    );
    assert_eq!(text_end(&document, window), TextEnd::Complete);
    assert_eq!(notes(&document, window), 1, "the edit note");

    // A reload reads the loaded pages again, with their bytes.
    press(window, "f5");

    assert_eq!(
        text(&document, window),
        String::from_utf8_lossy(&numbered_csv(5))
    );
    assert_eq!(text_end(&document, window), TextEnd::Complete);
}

#[gpui::test]
fn the_text_follows_the_load_of_the_rest_of_the_file(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-load-rest");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(7));

    let (document, window) = open_local_in_small_pages(cx, path);

    press(window, "t");
    assert_eq!(text_end(&document, window), TextEnd::MoreInFile);

    window.update(|_, cx| document.update(cx, |document, cx| document.load_rest(cx)));
    window.run_until_parked();

    assert_eq!(
        text(&document, window),
        String::from_utf8_lossy(&numbered_csv(7))
    );
    assert_eq!(text_end(&document, window), TextEnd::Complete);
}

#[gpui::test]
fn the_text_follows_a_reload(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-reload");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    press(window, "t");
    assert_eq!(text(&document, window), String::from_utf8_lossy(CITIES));

    std::fs::write(&path, b"name,city\nCy,Rome\n").expect("the file is rewritten");

    press(window, "escape");
    press(window, "f5");

    assert_eq!(text(&document, window), "name,city\nCy,Rome\n");
    assert_eq!(view(&document, window), DelimitedView::Text);
}

#[gpui::test]
fn the_text_follows_a_dialect_change(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-dialect");
    let (path, _) = directory.file("names.csv", WINDOWS_1252_NAMES);

    let (document, window) = open_local(cx, path);

    press(window, "t");
    assert!(text(&document, window).contains("Jos\u{e9};M\u{e1}laga"));

    set_overrides(
        &document,
        window,
        DialectOverrides {
            encoding: Some(encoding("utf-8")),
            ..DialectOverrides::default()
        },
    );

    assert!(
        text(&document, window).contains("Jos\u{fffd};M\u{fffd}laga"),
        "{}",
        text(&document, window)
    );
}

#[gpui::test]
fn the_text_follows_a_save_made_from_the_text_view(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-save");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 1, 1, "Cusco");
    select(&document, window, 0);
    window.update(|_, cx| document.update(cx, |document, cx| document.insert_row_above(cx)));
    window.run_until_parked();
    type_into_cell(&document, window, 0, 0, "Zed");

    press(window, "t");
    assert_eq!(
        text(&document, window),
        "name,city\nZed,\nAna,Lima\nBo,Cusco\n"
    );
    assert_eq!(
        text_rows(&document, window),
        [
            TextRow::Header,
            TextRow::Insert(0),
            TextRow::Base(0),
            TextRow::Base(1)
        ]
    );

    // The save key of the editors, while the editor holds the keyboard.
    press(window, "ctrl-s");

    assert!(!is_dirty(&document, window));
    assert_eq!(read(&path), b"name,city\nZed,\nAna,Lima\nBo,Cusco\n");
    assert_eq!(view(&document, window), DelimitedView::Text);

    // The saved file was read again, so the inserted row is now a record.
    assert_eq!(
        text(&document, window),
        "name,city\nZed,\nAna,Lima\nBo,Cusco\n"
    );
    assert_eq!(
        text_rows(&document, window),
        [
            TextRow::Header,
            TextRow::Base(0),
            TextRow::Base(1),
            TextRow::Base(2)
        ]
    );
}

// -- Table-only actions ------------------------------------------------------------

#[gpui::test]
fn row_and_column_changes_wait_for_the_table(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-actions");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    let checked = covered_ids(&document, window);
    assert!(
        checked.iter().any(|id| id == "delimited-insert-above"),
        "{checked:?}"
    );

    press(window, "t");

    let checked = covered_ids(&document, window);
    for disabled in ["delimited-insert-above", "delimited-add-column"] {
        assert!(
            !checked.iter().any(|id| id == disabled),
            "{disabled} takes no click in the text view: {checked:?}"
        );
    }

    let actions = window.update(|_, cx| document.read(cx).pane_actions(&document));
    for id in [
        "delimited-insert-above",
        "delimited-add-column",
        "delimited-rename-column",
    ] {
        let action = actions
            .iter()
            .find(|action| action.id.as_ref() == id)
            .unwrap_or_else(|| panic!("the pane actions list {id}"));

        assert!(!action.enabled, "{id} is disabled in the text view");
    }

    window.update(|_, cx| document.update(cx, |document, cx| document.insert_row_above(cx)));
    window.run_until_parked();
    assert!(!has_pending_operations(&document, window));

    // Save, discard and reload stay.
    assert!(
        pane_action_ids(&document, window)
            .iter()
            .any(|id| id == "delimited-reload")
    );
}

#[gpui::test]
fn the_view_does_not_switch_while_a_dialog_is_open(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-dialog");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    window.update(|_, cx| {
        document.update(cx, |document, cx| document.rename_active_column(cx));
    });
    window.run_until_parked();
    assert!(window.update(|_, cx| document.read(cx).is_column_prompt_open()));

    show(&document, window, DelimitedView::Text);

    assert_eq!(view(&document, window), DelimitedView::Table);
}

// -- Keyboard coverage ---------------------------------------------------------------

#[gpui::test]
fn the_view_controls_are_covered_in_both_views(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-coverage");
    let (path, _) = directory.file("cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    let checked = covered_ids(&document, window);
    for control in [
        "segmented-delimited-view-table",
        "segmented-delimited-view-text",
    ] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }
    assert!(
        !checked
            .iter()
            .any(|id| id.starts_with("segmented-delimited-text-mode")),
        "{checked:?}"
    );

    press(window, "t");

    let checked = covered_ids(&document, window);
    for control in [
        "segmented-delimited-view-table",
        "segmented-delimited-view-text",
        "segmented-delimited-text-mode-raw",
        "segmented-delimited-text-mode-aligned",
    ] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }
}

/// Every string of the text view resolves in English and is translated,
/// not copied, in the other catalogs.
#[test]
fn the_text_view_strings_resolve_in_every_locale() {
    for key in [
        "document.delimited.view.table",
        "document.delimited.view.text",
        "document.delimited.view.raw",
        "document.delimited.view.aligned",
        "document.delimited.text.more_in_file",
        "document.delimited.text.capped",
        "document.delimited.text.render_failed",
        "document.delimited.text.aligned_hint",
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

// -- What the kept bytes cost ----------------------------------------------------

/// An in-memory source that counts the range reads it answers.
struct CountingSource {
    bytes: Vec<u8>,
    reads: std::cell::Cell<usize>,
}

impl dbflux_delimited::ByteSource for CountingSource {
    fn byte_length(&self) -> Result<u64, dbflux_delimited::SourceError> {
        Ok(self.bytes.len() as u64)
    }

    fn read_range(
        &self,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, dbflux_delimited::SourceError> {
        self.reads.set(self.reads.get() + 1);

        Ok(self.bytes[range.start as usize..range.end as usize].to_vec())
    }
}

/// How many range reads a reader over `bytes` makes to open and to read
/// `pages`, with the document's small pages.
fn reader_reads(bytes: &[u8], pages: std::ops::Range<usize>) -> (usize, usize) {
    let source = CountingSource {
        bytes: bytes.to_vec(),
        reads: std::cell::Cell::new(0),
    };
    let dialect = dbflux_delimited::Dialect {
        delimiter: b',',
        quote: Some(b'"'),
        has_header: true,
        encoding: encoding("utf-8"),
    };

    let mut reader = dbflux_delimited::PagedReader::open(source, dialect, SMALL_PAGES)
        .expect("the reader opens");
    let opened = reader.source().reads.get();

    for page in pages {
        reader.read_page(page).expect("the page reads");
    }

    (opened, reader.source().reads.get() - opened)
}

/// An object is read exactly as often as detection and the reader need: the
/// bytes the text view keeps come from those reads.
#[gpui::test]
fn the_kept_bytes_cost_no_range_read(cx: &mut TestAppContext) {
    let bytes = numbered_csv(7);
    let connection = FakeConnection::with_object(&bytes);
    let (app_state, profile_id) = connect_profile(cx, connection.clone());

    let (reader_open, first_page) = reader_reads(&bytes, 0..1);
    let (_, second_page) = reader_reads(&bytes, 0..2);
    let second_page = second_page - first_page;

    let (document, window) =
        open_object_in_small_pages(cx, app_state, profile_id, connection.clone());

    // One read of the detection sample, then the reader's.
    let opened = connection.store.range_reads();
    assert_eq!(opened, 1 + reader_open + first_page, "open");

    load_more(&document, window);
    let after_more = connection.store.range_reads();
    assert_eq!(after_more - opened, second_page, "load more");

    window.update(|_, cx| document.update(cx, |document, cx| document.reload(cx)));
    window.run_until_parked();
    let (reopen, two_pages) = reader_reads(&bytes, 0..2);
    assert_eq!(
        connection.store.range_reads() - after_more,
        reopen + two_pages,
        "reload"
    );

    press(window, "t");
    assert_eq!(
        text(&document, window),
        "id,city\n0,Lima\n1,Lima\n2,Lima\n3,Lima\n"
    );
}

// -- A header too large to keep ---------------------------------------------------

#[gpui::test]
fn a_header_too_large_to_keep_shows_no_text_and_a_note(cx: &mut TestAppContext) {
    let single_line = vec![b'x'; 9 * 1024 * 1024];

    let mut unclosed_quote = b"\"".to_vec();
    unclosed_quote.extend(std::iter::repeat_n(b'a', 10 * 1024 * 1024));
    unclosed_quote.extend_from_slice(b"\n1,2\n");

    for (name, bytes) in [
        ("single-line.csv", single_line),
        ("unclosed.csv", unclosed_quote),
    ] {
        let directory = TestDirectory::new("text-view-large-header");
        let path = local_file(&directory, name, &bytes);

        let (document, window) = open_local(cx, path);

        press(window, "t");

        assert_eq!(text(&document, window), "", "{name}");
        assert_eq!(
            text_end(&document, window),
            TextEnd::Capped { shown: 0 },
            "{name}"
        );
        assert!(notes(&document, window) >= 1, "{name}");

        // A row inserted at the end changes nothing in the text.
        press(window, "escape");
        press(window, "t");
        window.update(|_, cx| document.update(cx, |document, cx| document.insert_row_above(cx)));
        window.run_until_parked();
        press(window, "t");

        assert_eq!(text(&document, window), "", "{name}");
        assert_eq!(
            text_rows(&document, window),
            Vec::<TextRow>::new(),
            "{name}"
        );
    }
}

// -- Saving after Escape ------------------------------------------------------------

#[gpui::test]
fn the_save_key_saves_from_the_text_view_after_escape(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-save-key");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 1, 1, "Cusco");
    press(window, "t");
    press(window, "escape");
    assert_eq!(context(&document, window), ContextId::Results);

    press(window, "ctrl-s");

    assert!(!is_dirty(&document, window));
    assert_eq!(read(&path), b"name,city\nAna,Lima\nBo,Cusco\n");
}

// -- Bare carriage returns ---------------------------------------------------------

#[gpui::test]
fn the_text_view_opens_at_the_byte_of_the_active_row_in_bare_cr_text(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-view-bare-cr");
    let (path, _) = directory.file("old-mac.csv", b"a,b\r1,2\r3,4\r");

    let (document, window) = open_local(cx, path);
    select(&document, window, 1);

    press(window, "t");

    let cursor = window.update(|_, cx| {
        document
            .read(cx)
            .text_input()
            .expect("the text view was shown")
            .read(cx)
            .cursor()
    });

    assert_eq!(cursor, "a,b\r1,2\r".len());
}

// -- Editing the raw text ------------------------------------------------------------

/// Replaces the text view's text with what `edit` makes of it, as typing
/// does: the editor tells the document that its text changed.
pub(super) fn edit_text(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    edit: impl FnOnce(&str) -> String,
) {
    let input = window.update(|_, cx| {
        document
            .read(cx)
            .text_input()
            .cloned()
            .expect("the text view was shown")
    });

    window.update(|window, cx| {
        input.update(cx, |state, cx| {
            let edited = edit(&state.value());

            state.replace_all(edited, window, cx);
            cx.emit(InputEvent::Change);
        });
    });
    window.run_until_parked();
}

/// Replaces the first `from` of the text view's text with `to`.
pub(super) fn replace_in_text(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
    from: &str,
    to: &str,
) {
    edit_text(document, window, |text| {
        assert!(text.contains(from), "{from:?} is in {text:?}");
        text.replacen(from, to, 1)
    });
}

pub(super) fn is_editable(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> bool {
    window.update(|_, cx| document.read(cx).is_text_editable())
}

fn apply_error(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Option<String> {
    window.update(|_, cx| document.read(cx).text.apply_error.clone())
}

/// The cells the table shows, pending changes included and deleted rows
/// left out.
pub(super) fn shown_rows(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Vec<String> {
    let table = table_state(document, window);

    window.update(|_, cx| {
        let state = table.read(cx);
        let buffer = state.edit_buffer();
        let absent = CellValue::text("");

        buffer
            .compute_visual_order()
            .into_iter()
            .filter_map(|source| {
                let cells: Vec<String> = match source {
                    VisualRowSource::Base(row) if buffer.is_pending_delete(row) => return None,

                    VisualRowSource::Base(row) => (0..state.col_count())
                        .map(|column| {
                            let base = state.model().cell(row, column).unwrap_or(&absent);
                            buffer.get_cell(row, column, base).edit_text()
                        })
                        .collect(),

                    VisualRowSource::Insert(index) => buffer.pending_inserts()[index]
                        .data
                        .iter()
                        .map(CellValue::edit_text)
                        .collect(),
                };

                Some(cells.join("|"))
            })
            .collect()
    })
}

#[gpui::test]
fn typing_in_the_raw_text_edits_the_row_and_a_save_rewrites_only_its_record(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("text-edit-typing");
    let path = local_file(
        &directory,
        "cities.csv",
        b"name,city\r\nAna,\"Lima\"\r\nBo,Quito\r\n",
    );

    let (document, window) = open_local(cx, path.clone());
    select(&document, window, 1);

    press(window, "t");
    assert!(is_editable(&document, window));

    window.simulate_input("z");

    assert_eq!(
        text(&document, window),
        "name,city\r\nAna,\"Lima\"\r\nzBo,Quito\r\n"
    );
    assert!(
        is_dirty(&document, window),
        "the text differs from the file"
    );

    press(window, "escape");
    press(window, "t");

    assert_eq!(view(&document, window), DelimitedView::Table);
    assert_eq!(shown_rows(&document, window), ["Ana|Lima", "zBo|Quito"]);
    assert!(is_dirty(&document, window));

    press(window, "ctrl-s");

    assert!(!is_dirty(&document, window));
    assert_eq!(
        read(&path),
        b"name,city\r\nAna,\"Lima\"\r\nzBo,Quito\r\n",
        "the untouched record keeps its quotes"
    );
}

#[gpui::test]
fn lines_inserted_deleted_and_replaced_in_the_text_are_saved_where_the_text_puts_them(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("text-edit-lines");
    let path = local_file(
        &directory,
        "cities.csv",
        b"name,city\nAna,Lima\nBo,Quito\nCy,Rome\n",
    );

    let (document, window) = open_local(cx, path.clone());

    press(window, "t");
    edit_text(&document, window, |_| {
        "name,city\nTop,1\nAna,Lima\nMid,2\nCy,Rome\nEnd,3\n".to_string()
    });
    press(window, "escape");
    press(window, "t");

    assert_eq!(
        shown_rows(&document, window),
        ["Top|1", "Ana|Lima", "Mid|2", "Cy|Rome", "End|3"]
    );

    press(window, "t");
    assert_eq!(
        text(&document, window),
        "name,city\nTop,1\nAna,Lima\nMid,2\nCy,Rome\nEnd,3\n",
        "the text is rendered again from the table"
    );

    // Several lines replaced at once, and one deleted.
    edit_text(&document, window, |_| {
        "name,city\nTop,1\nX,8\nY,9\nCy,Rome\n".to_string()
    });
    press(window, "ctrl-s");

    assert_eq!(read(&path), b"name,city\nTop,1\nX,8\nY,9\nCy,Rome\n");
    assert!(!is_dirty(&document, window));
}

#[gpui::test]
fn a_pending_insert_is_edited_and_removed_through_its_line(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-insert");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    select(&document, window, 1);
    window.update(|_, cx| document.update(cx, |document, cx| document.insert_row_above(cx)));
    window.run_until_parked();
    type_into_cell(&document, window, 1, 0, "Zed");

    press(window, "t");
    assert_eq!(
        text(&document, window),
        "name,city\nAna,Lima\nZed,\nBo,Quito\n"
    );

    replace_in_text(&document, window, "Zed,\n", "Zed,Oslo\n");
    show(&document, window, DelimitedView::Table);

    assert_eq!(
        shown_rows(&document, window),
        ["Ana|Lima", "Zed|Oslo", "Bo|Quito"]
    );

    show(&document, window, DelimitedView::Text);
    replace_in_text(&document, window, "Zed,Oslo\n", "");
    show(&document, window, DelimitedView::Table);

    assert_eq!(shown_rows(&document, window), ["Ana|Lima", "Bo|Quito"]);
    assert!(!has_pending_operations(&document, window));
    assert!(!is_dirty(&document, window));
}

#[gpui::test]
fn an_edit_that_restores_the_text_leaves_the_document_clean(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-restore");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    press(window, "t");
    replace_in_text(&document, window, "Lima", "Cusco");
    assert!(is_dirty(&document, window));

    replace_in_text(&document, window, "Cusco", "Lima");
    assert!(!is_dirty(&document, window));

    show(&document, window, DelimitedView::Table);
    assert!(!has_pending_operations(&document, window));
}

#[gpui::test]
fn a_line_break_or_quoting_changed_alone_is_kept_from_the_file_and_the_note_says_so(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("text-edit-line-break");
    let bytes = b"name,city\r\nAna,Lima\r\nBo,Quito\r\n";
    let path = local_file(&directory, "cities.csv", bytes);

    let (document, window) = open_local(cx, path.clone());

    press(window, "t");

    // The note on what an edit keeps from the file.
    assert_eq!(notes(&document, window), 1);

    replace_in_text(&document, window, "Ana,Lima\r\n", "Ana,Lima\n");
    replace_in_text(&document, window, "Bo,Quito", "\"Bo\",\"Quito\"");
    assert!(is_dirty(&document, window));

    press(window, "ctrl-s");

    assert!(!is_dirty(&document, window));
    assert_eq!(read(&path), bytes);
    assert_eq!(
        text(&document, window),
        String::from_utf8_lossy(bytes),
        "the text shows the file again"
    );
}

// -- The header ----------------------------------------------------------------------

#[gpui::test]
fn the_header_line_renames_and_adds_columns(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-header");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    press(window, "t");
    edit_text(&document, window, |_| {
        "name,town,age\nAna,Lima,30\nBo,Quito\n".to_string()
    });
    show(&document, window, DelimitedView::Table);

    assert_eq!(
        super::document_tests::column_titles(&document, window),
        ["name", "town", "age"]
    );
    assert_eq!(shown_rows(&document, window), ["Ana|Lima|30", "Bo|Quito|"]);

    super::editing_tests::save(&document, window);

    assert_eq!(read(&path), b"name,town,age\nAna,Lima,30\nBo,Quito,\n");
}

#[gpui::test]
fn header_changes_the_table_cannot_make_keep_the_text(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-header-refused");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);

    press(window, "t");

    for (from, to) in [
        ("id,city", "id,city,age"),
        ("id,city", "id"),
        ("id,city", "city,id"),
        ("0,Lima", "0,Lima,extra"),
    ] {
        replace_in_text(&document, window, from, to);
        show(&document, window, DelimitedView::Table);

        assert_eq!(view(&document, window), DelimitedView::Text, "{to}");
        assert!(apply_error(&document, window).is_some(), "{to}");
        assert!(is_dirty(&document, window), "{to}");
        assert!(!has_pending_operations(&document, window), "{to}");

        replace_in_text(&document, window, to, from);
        assert!(apply_error(&document, window).is_none(), "{to}");
    }
}

// -- Text that does not read ------------------------------------------------------------

#[gpui::test]
fn an_unclosed_quote_blocks_switching_and_saving_until_it_is_fixed(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-unclosed");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    press(window, "t");
    replace_in_text(&document, window, "Bo,Quito", "Bo,\"Quito");

    press(window, "ctrl-s");

    assert_eq!(save_results(&events), [false]);
    assert_eq!(read(&path), CITIES);
    assert!(
        apply_error(&document, window).is_some_and(|cause| cause.contains('3')),
        "the error names line 3: {:?}",
        apply_error(&document, window)
    );

    show(&document, window, DelimitedView::Table);
    assert_eq!(view(&document, window), DelimitedView::Text);
    assert!(
        text(&document, window).contains("Bo,\"Quito"),
        "the text is kept"
    );

    replace_in_text(&document, window, "Bo,\"Quito", "Bo,\"Quito\"");
    assert!(apply_error(&document, window).is_none());

    show(&document, window, DelimitedView::Table);
    assert_eq!(view(&document, window), DelimitedView::Table);

    super::editing_tests::save(&document, window);
    assert_eq!(read(&path), CITIES, "the quoted value is the same value");
}

// -- Applied before what needs the table ---------------------------------------------------

#[gpui::test]
fn the_text_is_applied_before_the_next_page_is_loaded(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-load-more");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);

    press(window, "t");
    assert!(
        is_editable(&document, window),
        "a partly loaded text is editable"
    );

    replace_in_text(&document, window, "1,Lima", "1,Cusco");
    press(window, "escape");
    press(window, "]");

    assert_eq!(
        text(&document, window),
        "id,city\n0,Lima\n1,Cusco\n2,Lima\n3,Lima\n"
    );
    assert!(is_dirty(&document, window));

    show(&document, window, DelimitedView::Table);
    assert_eq!(
        shown_rows(&document, window),
        ["0|Lima", "1|Cusco", "2|Lima", "3|Lima"]
    );
}

#[gpui::test]
fn the_text_is_applied_before_a_reload_or_a_dialect_change_which_it_then_blocks(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("text-edit-reread");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    press(window, "t");
    replace_in_text(&document, window, "Lima", "Cusco");

    window.update(|_, cx| document.update(cx, |document, cx| document.reload(cx)));
    window.run_until_parked();

    assert!(
        has_pending_operations(&document, window),
        "applied, then kept"
    );
    assert!(is_dirty(&document, window));

    show(&document, window, DelimitedView::Text);
    replace_in_text(&document, window, "Quito", "Rome");

    set_overrides(
        &document,
        window,
        DialectOverrides {
            delimiter: Some(b';'),
            ..DialectOverrides::default()
        },
    );

    assert_eq!(
        super::document_tests::dialect(&document, window).delimiter,
        b','
    );
    assert_eq!(text(&document, window), "name,city\nAna,Cusco\nBo,Rome\n");
    assert_eq!(read(&path), CITIES);
}

#[gpui::test]
fn discard_drops_a_valid_text_edit_with_the_table_changes(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-discard");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 0, 1, "Oslo");
    press(window, "t");
    replace_in_text(&document, window, "Quito", "Rome");

    window.update(|_, cx| document.update(cx, |document, cx| document.discard_changes(cx)));
    window.run_until_parked();

    assert!(!is_dirty(&document, window));
    assert!(!has_pending_operations(&document, window));
    assert_eq!(text(&document, window), String::from_utf8_lossy(CITIES));
    assert_eq!(read(&path), CITIES);
}

#[gpui::test]
fn discard_drops_text_that_does_not_parse_without_reading_it(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-discard-unclosed");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    press(window, "t");
    replace_in_text(&document, window, "Bo,Quito", "Bo,\"Quito");
    assert!(is_dirty(&document, window));

    let toasts_before = super::document_tests::toast_count(window);

    window.update(|_, cx| document.update(cx, |document, cx| document.discard_changes(cx)));
    window.run_until_parked();

    assert!(!is_dirty(&document, window));
    assert!(!has_pending_operations(&document, window));
    assert_eq!(apply_error(&document, window), None);
    assert_eq!(
        super::document_tests::toast_count(window),
        toasts_before,
        "no report"
    );
    assert_eq!(text(&document, window), String::from_utf8_lossy(CITIES));
    assert_eq!(view(&document, window), DelimitedView::Text);
    assert!(save_results(&events).is_empty(), "nothing was saved");
    assert_eq!(read(&path), CITIES);
}

#[gpui::test]
fn closing_saves_the_text_and_text_that_cannot_be_applied_keeps_the_tab(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-close");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    press(window, "t");
    replace_in_text(&document, window, "Quito", "\"Rome");

    window.update(|_, cx| document.update(cx, |document, cx| document.save_for_close(cx)));
    window.run_until_parked();

    assert_eq!(save_results(&events), [false]);
    assert!(!asked_to_close(&events));
    assert_eq!(read(&path), CITIES);

    replace_in_text(&document, window, "\"Rome", "Rome");

    window.update(|_, cx| document.update(cx, |document, cx| document.save_for_close(cx)));
    window.run_until_parked();

    assert_eq!(save_results(&events), [false, true]);
    assert!(asked_to_close(&events));
    assert_eq!(read(&path), b"name,city\nAna,Lima\nBo,Rome\n");
}

#[gpui::test]
fn leaving_the_raw_text_for_the_aligned_text_applies_it(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-aligned");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    press(window, "t");
    replace_in_text(&document, window, "Lima", "Cusco");

    press(window, "escape");
    press(window, "shift-t");

    assert_eq!(mode(&document, window), TextMode::Aligned);
    assert_eq!(
        text(&document, window),
        "name | city\nAna  | Cusco\nBo   | Quito\n"
    );
    assert!(has_pending_operations(&document, window));

    // Aligned text takes no typing.
    assert!(!is_editable(&document, window));
    press(window, "enter");
    window.simulate_input("z");
    assert_eq!(
        text(&document, window),
        "name | city\nAna  | Cusco\nBo   | Quito\n"
    );
}

// -- What cannot be edited ---------------------------------------------------------------

#[gpui::test]
fn a_capped_text_is_read_only_and_says_why(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-capped");

    let mut bytes = b"n\n".to_vec();
    bytes.extend(std::iter::repeat_n(b"1\n".as_slice(), 50_005).flatten());
    let (path, _) = directory.file("many.csv", &bytes);

    let options = dbflux_delimited::ReaderOptions {
        page_size: std::num::NonZeroUsize::new(100_000).expect("a non-zero page size"),
        window_size: std::num::NonZeroU64::new(1024 * 1024).expect("a non-zero window"),
    };

    let (document, window) = super::document_tests::open(cx, move |cx| {
        DelimitedDocument::open_local_with(path, options, cx)
    });

    press(window, "t");

    assert!(matches!(
        text_end(&document, window),
        TextEnd::Capped { .. }
    ));
    assert!(!is_editable(&document, window));
    assert_eq!(
        notes(&document, window),
        2,
        "capped, and why it is read-only"
    );

    let before = text(&document, window);
    window.simulate_input("z");

    assert_eq!(text(&document, window), before);
    assert!(!is_dirty(&document, window));
}

// -- Encodings -----------------------------------------------------------------------------

#[gpui::test]
fn a_utf_16_file_is_edited_in_the_text_and_saved_in_utf_16(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-utf-16");
    let text_of_file = "name,city\r\nAna,Lima\r\nBo,Quito\r\n";

    let mut bytes = b"\xFF\xFE".to_vec();
    bytes.extend(text_of_file.encode_utf16().flat_map(u16::to_le_bytes));
    let path = local_file(&directory, "cities.csv", &bytes);

    let (document, window) = open_local(cx, path.clone());

    press(window, "t");
    assert_eq!(text(&document, window), text_of_file);

    replace_in_text(&document, window, "Quito", "Asunci\u{f3}n");
    press(window, "ctrl-s");

    let mut expected = b"\xFF\xFE".to_vec();
    expected.extend(
        "name,city\r\nAna,Lima\r\nBo,Asunci\u{f3}n\r\n"
            .encode_utf16()
            .flat_map(u16::to_le_bytes),
    );

    assert_eq!(read(&path), expected);
}

// -- Undo ------------------------------------------------------------------------------------

/// One table undo after an applied text edit undoes the last operation the
/// edit made, here the second of its two cell edits.
#[gpui::test]
fn one_table_undo_undoes_the_last_operation_of_an_applied_text_edit(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edit-undo");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    press(window, "t");
    replace_in_text(&document, window, "Bo,Quito", "Bx,Qx");
    press(window, "escape");
    press(window, "t");

    assert_eq!(shown_rows(&document, window), ["Ana|Lima", "Bx|Qx"]);

    window.dispatch_action(dbflux_components::components::data_table::actions::Undo);
    window.run_until_parked();

    assert_eq!(shown_rows(&document, window), ["Ana|Lima", "Bx|Quito"]);

    window.dispatch_action(dbflux_components::components::data_table::actions::Undo);
    window.run_until_parked();

    assert_eq!(shown_rows(&document, window), ["Ana|Lima", "Bo|Quito"]);
    assert!(!is_dirty(&document, window));
}

/// Every string of editing the text resolves in English and is translated
/// in the other catalogs.
#[test]
fn the_text_editing_strings_resolve_in_every_locale() {
    for key in [
        "document.delimited.text.apply_failed",
        "document.delimited.text.read_only_capped",
        "document.delimited.text.raw_edit_hint",
        "document.delimited.error.text_apply_failed",
        "document.delimited.error.text_unclosed_quote",
        "document.delimited.error.text_unencodable",
        "document.delimited.error.text_header_field_removed",
        "document.delimited.error.text_header_reordered",
        "document.delimited.error.text_columns_need_full_load",
        "document.delimited.error.text_too_many_fields",
        "document.delimited.error.text_too_many_fields_no_header",
        "document.delimited.error.text_unmapped",
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

// -- Text a save would refuse ------------------------------------------------------------

/// Edits the text of the file `bytes`, read with `overrides`, by replacing
/// `from` with `to`, and checks that the edit is refused with the writer's
/// reason, which holds `reason`, and that nothing is applied or written.
fn assert_refused_by_the_writer(
    cx: &mut TestAppContext,
    bytes: &[u8],
    overrides: DialectOverrides,
    from: &str,
    to: &str,
    reason: &str,
) {
    let directory = TestDirectory::new("text-edit-writer-refuses");
    let path = local_file(&directory, "file.csv", bytes);

    let (document, window) = open_local(cx, path.clone());

    if overrides != DialectOverrides::default() {
        set_overrides(&document, window, overrides);
    }

    press(window, "t");
    replace_in_text(&document, window, from, to);
    let edited = text(&document, window);

    show(&document, window, DelimitedView::Table);

    assert_eq!(view(&document, window), DelimitedView::Text, "{to:?}");
    assert!(
        apply_error(&document, window).is_some_and(|cause| cause.contains(reason)),
        "{to:?}: {:?}",
        apply_error(&document, window)
    );
    assert_eq!(text(&document, window), edited, "the user's text is kept");
    assert!(
        !has_pending_operations(&document, window),
        "nothing is applied"
    );
    assert!(is_editable(&document, window), "the text stays editable");

    press(window, "ctrl-s");
    assert_eq!(read(&path), bytes, "nothing is written");
}

#[gpui::test]
fn deleting_the_line_before_one_that_starts_with_a_byte_order_mark_is_refused(
    cx: &mut TestAppContext,
) {
    assert_refused_by_the_writer(
        cx,
        "1,2\n\u{feff}3,4\n".as_bytes(),
        DialectOverrides::default(),
        "1,2\n",
        "",
        "U+FEFF",
    );
}

#[gpui::test]
fn a_field_starting_with_a_byte_order_mark_without_a_quote_character_is_refused(
    cx: &mut TestAppContext,
) {
    assert_refused_by_the_writer(
        cx,
        b"name,city\nAna,Lima\n",
        DialectOverrides {
            quote: Some(None),
            ..DialectOverrides::default()
        },
        "Ana,",
        "\u{feff}Ana,",
        "U+FEFF",
    );
}

#[gpui::test]
fn an_empty_line_in_a_one_column_file_without_a_quote_character_is_refused(
    cx: &mut TestAppContext,
) {
    assert_refused_by_the_writer(
        cx,
        b"name\nAna\nBo\n",
        DialectOverrides {
            quote: Some(None),
            ..DialectOverrides::default()
        },
        "Ana\n",
        "Ana\n\n",
        "empty",
    );
}

// -- Advice that works --------------------------------------------------------------------

#[gpui::test]
fn new_header_fields_on_a_partly_loaded_file_say_how_to_go_on_and_that_works(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("text-edit-header-partly-loaded");
    let (path, _) = directory.file("numbers.csv", &numbered_csv(5));

    let (document, window) = open_local_in_small_pages(cx, path);

    press(window, "t");
    replace_in_text(&document, window, "id,city", "id,city,age");
    show(&document, window, DelimitedView::Table);

    let cause = apply_error(&document, window).expect("the edit is refused");
    assert!(
        cause.contains("remove the new header fields"),
        "the advice says to remove the new fields: {cause}"
    );

    // Loading the rest applies the text first, and is refused the same way.
    window.update(|_, cx| document.update(cx, |document, cx| document.load_rest(cx)));
    window.run_until_parked();
    assert!(has_more_records(&document, window));

    // What the advice says: remove the new fields, load the rest from the
    // table, then add them.
    replace_in_text(&document, window, "id,city,age", "id,city");
    show(&document, window, DelimitedView::Table);
    assert_eq!(view(&document, window), DelimitedView::Table);

    while has_more_records(&document, window) {
        load_more(&document, window);
    }

    show(&document, window, DelimitedView::Text);
    replace_in_text(&document, window, "id,city", "id,city,age");
    show(&document, window, DelimitedView::Table);

    assert_eq!(
        super::document_tests::column_titles(&document, window),
        ["id", "city", "age"]
    );
}

#[gpui::test]
fn a_column_change_applies_the_text_first_and_waits_for_text_that_does_not_apply(
    cx: &mut TestAppContext,
) {
    let directory = TestDirectory::new("text-edit-column-change");
    let path = local_file(&directory, "cities.csv", CITIES);

    let (document, window) = open_local(cx, path);

    press(window, "t");
    replace_in_text(&document, window, "Lima", "Cusco");

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.rename_column(1, "town".to_string(), cx)
        })
    });
    window.run_until_parked();

    assert_eq!(text(&document, window), "name,town\nAna,Cusco\nBo,Quito\n");

    show(&document, window, DelimitedView::Table);
    assert_eq!(view(&document, window), DelimitedView::Table);
    assert_eq!(shown_rows(&document, window), ["Ana|Cusco", "Bo|Quito"]);

    show(&document, window, DelimitedView::Text);
    replace_in_text(&document, window, "Quito", "\"Quito");

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.append_column("age".to_string(), cx)
        })
    });
    window.run_until_parked();

    assert_eq!(
        super::document_tests::column_titles(&document, window),
        ["name", "town"],
        "no column is added over text that does not apply"
    );
    assert!(apply_error(&document, window).is_some());
}

#[test]
fn the_message_of_a_text_that_no_longer_matches_its_rows_names_a_way_out() {
    let cause = dbflux_i18n::t!("document.delimited.error.text_unmapped", locale = "en");

    assert!(cause.contains("undo"), "{cause}");
    assert!(!cause.contains("switch to the table"), "{cause}");
}

// -- A row added above a pending insert --------------------------------------------------

/// The text puts a new row above a pending insert the user did not touch:
/// the table shows it there, one undo removes only the new row, and a save
/// before or after that undo keeps the pending insert.
#[gpui::test]
fn a_row_added_above_a_pending_insert_leaves_that_insert_alone_in_undo_and_save(
    cx: &mut TestAppContext,
) {
    for undo_first in [false, true] {
        let directory = TestDirectory::new("text-edit-above-insert");
        let path = local_file(&directory, "cities.csv", CITIES);

        let (document, window) = open_local(cx, path.clone());

        let table = table_state(&document, window);
        window.update(|_, cx| {
            table.update(cx, |state, cx| {
                state.edit_buffer_mut().add_pending_insert_at(
                    InsertAnchor::After(0),
                    vec![CellValue::text("Cy"), CellValue::text("Rome")],
                );
                cx.notify();
            });
        });
        window.run_until_parked();

        press(window, "t");
        replace_in_text(&document, window, "Cy,Rome", "New,Row\nCy,Rome");
        show(&document, window, DelimitedView::Table);

        assert_eq!(
            shown_rows(&document, window),
            ["Ana|Lima", "New|Row", "Cy|Rome", "Bo|Quito"]
        );

        if undo_first {
            window.dispatch_action(dbflux_components::components::data_table::actions::Undo);
            window.run_until_parked();

            assert_eq!(
                shown_rows(&document, window),
                ["Ana|Lima", "Cy|Rome", "Bo|Quito"],
                "only the new row goes"
            );
        }

        super::editing_tests::save(&document, window);

        let expected: &[u8] = if undo_first {
            b"name,city\nAna,Lima\nCy,Rome\nBo,Quito\n"
        } else {
            b"name,city\nAna,Lima\nNew,Row\nCy,Rome\nBo,Quito\n"
        };
        assert_eq!(read(&path), expected);
    }
}
