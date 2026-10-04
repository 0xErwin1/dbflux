//! The text view of a delimited file: switching between it and the table by
//! key and by mouse, what survives a switch, and the refresh of the text
//! after the file changes under it.

use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::components::data_table::model::InsertAnchor;
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_delimited::DialectOverrides;
use gpui::{Entity, Focusable as _, TestAppContext, VisualTestContext};

use super::columns_tests::click;
use super::document::DelimitedDocument;
use super::document_tests::{
    CITIES, SMALL_PAGES, WINDOWS_1252_NAMES, connect_profile, covered_ids, encoding, load_more,
    numbered_csv, open_local, open_local_in_small_pages, open_object_in_small_pages,
    pane_action_ids, set_overrides,
};
use super::editing_tests::{
    has_pending_operations, is_dirty, local_file, press, read, select, table_state, type_into_cell,
};
use super::tests::{FakeConnection, TestDirectory};
use super::text::{TextEnd, TextRow};
use super::text_view::{DelimitedView, TextMode};

fn view(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> DelimitedView {
    window.update(|_, cx| document.read(cx).view())
}

fn mode(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> TextMode {
    window.update(|_, cx| document.read(cx).text_mode())
}

fn context(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> ContextId {
    window.update(|_, cx| document.read(cx).active_context())
}

/// The text the text view's editor holds.
fn text(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> String {
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

fn text_end(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> TextEnd {
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

fn show(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext, view: DelimitedView) {
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

    // The editor takes `t` as text, read-only, so the view stays.
    press(window, "t");
    assert_eq!(view(&document, window), DelimitedView::Text);

    press(window, "escape");
    assert_eq!(context(&document, window), ContextId::Results);
    assert!(!text_is_focused(&document, window));

    press(window, "t");

    assert_eq!(view(&document, window), DelimitedView::Table);
    assert!(table_is_focused(&document, window));
    assert_eq!(active_cell(&document, window), Some(CellCoord::new(1, 0)));
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
    assert_eq!(notes(&document, window), 1);

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
    assert_eq!(notes(&document, window), 0);

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
