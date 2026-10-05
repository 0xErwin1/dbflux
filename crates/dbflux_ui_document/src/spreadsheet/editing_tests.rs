//! Editing, saving, closing and quitting a spreadsheet tab.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use chrono::NaiveDate;
use dbflux_byte_source::MemorySource;
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_spreadsheet::{CellFormula, CellValue, SheetGrid};
use dbflux_ui_base::keyboard_coverage::FrameCapture;
use dbflux_ui_base::toast::ToastGlobal;
use gpui::{Entity, Modifiers, Subscription, TestAppContext, VisualTestContext};
use rust_xlsxwriter::{ExcelDateTime, Format};

use super::document::SpreadsheetDocument;
use super::grid_model::FormulaReadout;
use super::tests::{
    TestDirectory, active_sheet, display, fixture, items_workbook, open_local, readout_at,
    table_state, toast_count, xlsx_bytes,
};
use crate::handle::DocumentEvent;
use crate::pane::{PaneHandle, QuitDisposition};
use crate::types::DocumentState;

// -- Helpers ------------------------------------------------------------------

/// A workbook of one sheet, `Typed`, whose first row holds the text `label`,
/// the boolean TRUE, the date 2024-01-15 formatted as a date, the text
/// `text` and the number 1.
fn typed_workbook() -> Vec<u8> {
    xlsx_bytes(|workbook| {
        let date_format = Format::new().set_num_format("yyyy-mm-dd");
        let sheet = workbook.add_worksheet().set_name("Typed")?;

        sheet.write(0, 0, "label")?;
        sheet.write_boolean(0, 1, true)?;
        sheet.write_datetime_with_format(
            0,
            2,
            ExcelDateTime::from_ymd(2024, 1, 15)?,
            &date_format,
        )?;
        sheet.write(0, 3, "text")?;
        sheet.write(0, 4, 1)?;

        Ok(())
    })
}

/// Types `text` into the cell shown at `row`, `column` and commits it, as
/// Enter does.
fn type_into_cell(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
    row: usize,
    column: usize,
    text: &str,
) {
    let table_state = table_state(document, window);

    window.update(|window, cx| {
        table_state.update(cx, |state, cx| {
            assert!(
                state.start_editing(CellCoord::new(row, column), window, cx),
                "the cell at {row},{column} takes an editor"
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

fn select(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
    row: usize,
    column: usize,
) {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.select_cell(CellCoord::new(row, column), cx)
        })
    });
    window.run_until_parked();
}

fn press(window: &mut VisualTestContext, keystrokes: &str) {
    window.simulate_keystrokes(keystrokes);
    window.run_until_parked();
}

fn append_row(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) {
    window.update(|_, cx| document.update(cx, |document, cx| document.append_row(cx)));
    window.run_until_parked();
}

fn save(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) {
    window.update(|_, cx| document.update(cx, |document, cx| document.save(cx)));
    window.run_until_parked();
}

fn select_sheet(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
    index: usize,
) {
    window.update(|window, cx| {
        document.update(cx, |document, cx| document.select_sheet(index, window, cx))
    });
    window.run_until_parked();
}

/// The rows the table shows, appended rows included.
fn shown_rows(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) -> usize {
    let table_state = table_state(document, window);

    window.update(|_, cx| table_state.read(cx).row_count())
}

fn is_dirty(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) -> bool {
    window.update(|_, cx| document.read(cx).is_dirty())
}

fn last_toast_title(window: &mut VisualTestContext) -> Option<String> {
    window.update(|_, cx| cx.global::<ToastGlobal>().host.read(cx).last_toast_title())
}

fn pane(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) -> PaneHandle {
    window.update(|_, cx| SpreadsheetDocument::into_pane(document.clone(), cx))
}

/// Records every event the document emits from now on.
fn record_events(
    document: &Entity<SpreadsheetDocument>,
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

fn save_results(events: &Rc<RefCell<Vec<DocumentEvent>>>) -> Vec<bool> {
    events
        .borrow()
        .iter()
        .filter_map(|event| match event {
            DocumentEvent::SaveFinished { succeeded } => Some(*succeeded),
            _ => None,
        })
        .collect()
}

fn asked_to_close(events: &Rc<RefCell<Vec<DocumentEvent>>>) -> bool {
    events
        .borrow()
        .iter()
        .any(|event| matches!(event, DocumentEvent::RequestClose))
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).expect("the test file is readable")
}

/// Reads sheet `index` of the workbook at `path` as the reader sees it.
fn read_sheet(path: &Path, index: usize) -> SheetGrid {
    let mut workbook =
        dbflux_spreadsheet::open(MemorySource::new(read(path))).expect("the saved workbook opens");

    workbook.read_sheet(index).expect("the saved sheet reads")
}

fn value(grid: &SheetGrid, row: usize, column: usize) -> CellValue {
    grid.cell(row, column)
        .map(|cell| cell.value.clone())
        .unwrap_or(CellValue::Empty)
}

fn formula(grid: &SheetGrid, row: usize, column: usize) -> CellFormula {
    grid.cell(row, column)
        .map(|cell| cell.formula.clone())
        .unwrap_or(CellFormula::None)
}

fn text(value: &str) -> CellValue {
    CellValue::Text(value.into())
}

fn save_failed_title(path: &Path) -> String {
    let name = path
        .file_name()
        .expect("the test file has a name")
        .to_string_lossy()
        .into_owned();

    crate::labels::spreadsheet_save_failed_message(&name)
}

/// The CRC-32 and the uncompressed length of every entry of a zip archive,
/// by entry name, read from its central directory. Equal values mean equal
/// content.
fn zip_entries(bytes: &[u8]) -> BTreeMap<String, (u32, u32)> {
    const END_OF_DIRECTORY: [u8; 4] = [0x50, 0x4b, 0x05, 0x06];
    const DIRECTORY_ENTRY: [u8; 4] = [0x50, 0x4b, 0x01, 0x02];

    let u16_at = |at: usize| u16::from_le_bytes([bytes[at], bytes[at + 1]]) as usize;
    let u32_at =
        |at: usize| u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]);

    let end = (0..bytes.len().saturating_sub(21))
        .rev()
        .find(|&at| bytes[at..at + 4] == END_OF_DIRECTORY)
        .expect("the archive has an end of central directory record");

    let entry_count = u16_at(end + 10);
    let mut at = u32_at(end + 16) as usize;
    let mut entries = BTreeMap::new();

    for _ in 0..entry_count {
        assert_eq!(bytes[at..at + 4], DIRECTORY_ENTRY);

        let crc = u32_at(at + 16);
        let length = u32_at(at + 24);
        let name_length = u16_at(at + 28);
        let extra_length = u16_at(at + 30);
        let comment_length = u16_at(at + 32);
        let name = String::from_utf8_lossy(&bytes[at + 46..at + 46 + name_length]).into_owned();

        entries.insert(name, (crc, length));
        at += 46 + name_length + extra_length + comment_length;
    }

    entries
}

/// An ods workbook of one empty sheet, `Blank`, as LibreOffice writes one:
/// a single row holding one empty cell. The package is written with stored
/// entries, `mimetype` first.
fn empty_ods() -> Vec<u8> {
    const MIMETYPE: &str = "application/vnd.oasis.opendocument.spreadsheet";

    let content = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <office:document-content \
         xmlns:office=\"urn:oasis:names:tc:opendocument:xmlns:office:1.0\" \
         xmlns:table=\"urn:oasis:names:tc:opendocument:xmlns:table:1.0\" \
         xmlns:text=\"urn:oasis:names:tc:opendocument:xmlns:text:1.0\" \
         office:version=\"1.3\"><office:body><office:spreadsheet>\
         <table:table table:name=\"Blank\"><table:table-column/>\
         <table:table-row><table:table-cell/></table:table-row></table:table>\
         </office:spreadsheet></office:body></office:document-content>";
    let manifest = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <manifest:manifest xmlns:manifest=\"urn:oasis:names:tc:opendocument:xmlns:manifest:1.0\" \
         manifest:version=\"1.3\">\
         <manifest:file-entry manifest:full-path=\"/\" manifest:media-type=\"{MIMETYPE}\"/>\
         <manifest:file-entry manifest:full-path=\"content.xml\" manifest:media-type=\"text/xml\"/>\
         </manifest:manifest>"
    );

    stored_zip(&[
        ("mimetype", MIMETYPE.as_bytes()),
        ("content.xml", content.as_bytes()),
        ("META-INF/manifest.xml", manifest.as_bytes()),
    ])
}

/// A zip archive of `entries`, each stored without compression.
fn stored_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    const DOS_DATE_1980_01_01: u16 = (1 << 5) | 1;

    let mut archive = Vec::new();
    let mut directory = Vec::new();

    for (name, data) in entries {
        let mut crc = flate2::Crc::new();
        crc.update(data);

        let offset = u32::try_from(archive.len()).expect("the archive is small");
        let length = u32::try_from(data.len()).expect("the entry is small");
        let name_length = u16::try_from(name.len()).expect("the name is short");

        let mut fields = Vec::new();
        fields.extend_from_slice(&20u16.to_le_bytes());
        fields.extend_from_slice(&0u16.to_le_bytes());
        fields.extend_from_slice(&0u16.to_le_bytes());
        fields.extend_from_slice(&0u16.to_le_bytes());
        fields.extend_from_slice(&DOS_DATE_1980_01_01.to_le_bytes());
        fields.extend_from_slice(&crc.sum().to_le_bytes());
        fields.extend_from_slice(&length.to_le_bytes());
        fields.extend_from_slice(&length.to_le_bytes());
        fields.extend_from_slice(&name_length.to_le_bytes());
        fields.extend_from_slice(&0u16.to_le_bytes());

        archive.extend_from_slice(&[0x50, 0x4b, 0x03, 0x04]);
        archive.extend_from_slice(&fields);
        archive.extend_from_slice(name.as_bytes());
        archive.extend_from_slice(data);

        directory.extend_from_slice(&[0x50, 0x4b, 0x01, 0x02]);
        directory.extend_from_slice(&20u16.to_le_bytes());
        directory.extend_from_slice(&fields);
        directory.extend_from_slice(&0u16.to_le_bytes());
        directory.extend_from_slice(&0u16.to_le_bytes());
        directory.extend_from_slice(&0u16.to_le_bytes());
        directory.extend_from_slice(&0u32.to_le_bytes());
        directory.extend_from_slice(&offset.to_le_bytes());
        directory.extend_from_slice(name.as_bytes());
    }

    let directory_offset = u32::try_from(archive.len()).expect("the archive is small");
    let directory_length = u32::try_from(directory.len()).expect("the directory is small");
    let entry_count = u16::try_from(entries.len()).expect("few entries");

    archive.extend_from_slice(&directory);
    archive.extend_from_slice(&[0x50, 0x4b, 0x05, 0x06]);
    archive.extend_from_slice(&0u16.to_le_bytes());
    archive.extend_from_slice(&0u16.to_le_bytes());
    archive.extend_from_slice(&entry_count.to_le_bytes());
    archive.extend_from_slice(&entry_count.to_le_bytes());
    archive.extend_from_slice(&directory_length.to_le_bytes());
    archive.extend_from_slice(&directory_offset.to_le_bytes());
    archive.extend_from_slice(&0u16.to_le_bytes());

    archive
}

// -- Rows ---------------------------------------------------------------------

#[gpui::test]
fn dd_on_a_base_row_is_refused_with_a_message(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("dd-base");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path.clone());

    select(&document, window, 1, 0);
    press(window, "d d");

    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window),
        Some(dbflux_i18n::t!(
            "document.spreadsheet.error.rows_only_at_end"
        ))
    );
    assert_eq!(shown_rows(&document, window), 3);
    assert!(
        !is_dirty(&document, window),
        "a refused delete stages nothing"
    );

    append_row(&document, window);
    assert_eq!(shown_rows(&document, window), 4);
    assert!(is_dirty(&document, window));

    select(&document, window, 3, 0);
    press(window, "d d");

    assert_eq!(
        shown_rows(&document, window),
        3,
        "an appended row is removed"
    );
    assert!(!is_dirty(&document, window));
    assert_eq!(toast_count(window), 1);
}

#[gpui::test]
fn an_empty_sheet_takes_an_appended_first_row_and_saves_it(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("empty-append");
    let blank_xlsx = xlsx_bytes(|workbook| {
        workbook.add_worksheet().set_name("Blank")?;
        Ok(())
    });

    for (name, bytes) in [("blank.xlsx", blank_xlsx), ("blank.ods", empty_ods())] {
        let path = directory.file(name, &bytes);

        let (document, window) = open_local(cx, path.clone());
        let (events, _subscription) = record_events(&document, window);

        assert!(
            window.debug_bounds("spreadsheet-empty").is_none(),
            "{name}: an editable empty sheet shows a grid, not a notice"
        );

        let table_state = table_state(&document, window);
        let (columns, rows) = window.update(|_, cx| {
            let state = table_state.read(cx);
            let titles: Vec<String> = state
                .model()
                .columns
                .iter()
                .map(|column| column.title.to_string())
                .collect();

            (titles, state.row_count())
        });

        assert_eq!(
            columns,
            ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"],
            "{name}"
        );
        assert_eq!(rows, 0, "{name}");

        append_row(&document, window);
        type_into_cell(&document, window, 0, 0, "first");
        type_into_cell(&document, window, 0, 1, "2");
        save(&document, window);

        assert_eq!(save_results(&events), [true], "{name}");
        assert_eq!(toast_count(window), 0, "{name}");

        let grid = read_sheet(&path, 0);
        assert_eq!(value(&grid, 0, 0), text("first"), "{name}");
        assert_eq!(value(&grid, 0, 1), CellValue::Number(2.0), "{name}");

        assert_eq!(shown_rows(&document, window), 1, "{name}");
        assert_eq!(display(&document, window, 0, 0), "first", "{name}");
    }
}

#[gpui::test]
fn append_then_save_reopens_with_the_value(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("append-save");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    append_row(&document, window);
    type_into_cell(&document, window, 3, 0, "Cap");
    type_into_cell(&document, window, 3, 1, "9");

    save(&document, window);

    assert_eq!(save_results(&events), [true]);
    assert!(!is_dirty(&document, window));
    assert_eq!(toast_count(window), 0);

    let grid = read_sheet(&path, 0);
    assert_eq!(value(&grid, 3, 0), text("Cap"));
    assert_eq!(value(&grid, 3, 1), CellValue::Number(9.0));

    assert_eq!(
        shown_rows(&document, window),
        4,
        "the saved file is read again"
    );
    assert_eq!(display(&document, window, 3, 0), "Cap");
}

/// `bytes` without the zip entry `removed`, every other entry copied as it
/// is stored.
fn without_entry(bytes: &[u8], removed: &str) -> Vec<u8> {
    let mut archive =
        zip::ZipArchive::new(std::io::Cursor::new(bytes)).expect("the package must open");
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));

    for index in 0..archive.len() {
        let entry = archive.by_index_raw(index).expect("the entry must read");

        if entry.name() != removed {
            writer.raw_copy_file(entry).expect("the entry must copy");
        }
    }

    writer
        .finish()
        .expect("the package must write")
        .into_inner()
}

#[gpui::test]
fn sheet_without_an_append_row_takes_no_appended_row(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("no-append-row");
    let bytes = xlsx_bytes(|workbook| {
        let sheet = workbook.add_worksheet().set_name("Table")?;
        sheet.write(0, 0, "Key")?;
        sheet.write(1, 0, "a")?;
        sheet.add_table(0, 0, 3, 0, &rust_xlsxwriter::Table::new())?;
        Ok(())
    });
    // The sheet still names its table part, through a relationship the
    // package no longer has: calamine reads the cells, the append-row scan
    // cannot follow the table.
    let bytes = without_entry(&bytes, "xl/worksheets/_rels/sheet1.xml.rels");
    let path = directory.file("dangling.xlsx", &bytes);

    let (document, window) = open_local(cx, path);
    assert_eq!(display(&document, window, 1, 0), "a");
    let rows = shown_rows(&document, window);

    append_row(&document, window);

    assert_eq!(
        shown_rows(&document, window),
        rows,
        "a row whose file row is unknown would be dropped on save"
    );
    assert!(!is_dirty(&document, window));
}

#[gpui::test]
fn edits_on_two_sheets_are_saved_in_one_patch(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("two-sheets");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    type_into_cell(&document, window, 1, 0, "Quill");
    select_sheet(&document, window, 1);
    type_into_cell(&document, window, 0, 0, "FIRST");

    save(&document, window);

    assert_eq!(save_results(&events), [true], "one save writes both sheets");
    assert_eq!(value(&read_sheet(&path, 0), 1, 0), text("Quill"));
    assert_eq!(value(&read_sheet(&path, 1), 0, 0), text("FIRST"));
    assert!(!is_dirty(&document, window));
    assert_eq!(active_sheet(&document, window), Some(1));
    assert_eq!(display(&document, window, 0, 0), "FIRST");
}

// -- Typed input --------------------------------------------------------------

#[gpui::test]
fn typed_equals_writes_a_formula(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("typed-formula");
    let xlsx = directory.file("items.xlsx", &items_workbook());
    let ods = directory.file("in.ods", &fixture("in.ods"));

    let (document, window) = open_local(cx, xlsx.clone());

    type_into_cell(&document, window, 1, 2, "=B2*3");
    assert_eq!(
        readout_at(&document, window, 1, 2).1,
        FormulaReadout::Formula("=B2*3".into()),
        "the readout shows the pending formula"
    );

    save(&document, window);
    assert_eq!(
        formula(&read_sheet(&xlsx, 0), 1, 2),
        CellFormula::Text("=B2*3".into())
    );

    // An ods cell takes OpenFormula. An A1 reference is refused.
    let (document, window) = open_local(cx, ods.clone());
    let (events, _subscription) = record_events(&document, window);
    let original = read(&ods);

    type_into_cell(&document, window, 1, 3, "=B2*3");
    save(&document, window);

    assert_eq!(save_results(&events), [false]);
    assert_eq!(last_toast_title(window), Some(save_failed_title(&ods)));
    assert_eq!(read(&ods), original, "nothing is written");
    assert!(is_dirty(&document, window));

    type_into_cell(&document, window, 1, 3, "=[.B2]*3");
    save(&document, window);

    assert_eq!(save_results(&events), [false, true]);
    assert_eq!(
        formula(&read_sheet(&ods, 0), 1, 3),
        CellFormula::Text("of:=[.B2]*3".into())
    );
}

/// Opens an editor on the cell at `row`, `column` and commits it without
/// typing, as Enter on an untouched editor does.
fn commit_untouched(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
    row: usize,
    column: usize,
) {
    let table_state = table_state(document, window);

    window.update(|window, cx| {
        table_state.update(cx, |state, cx| {
            assert!(
                state.start_editing(CellCoord::new(row, column), window, cx),
                "the cell at {row},{column} takes an editor"
            );
            state.stop_editing(true, cx);
        });
    });
    window.run_until_parked();
}

/// A cell showing the recalculation legend holds empty text: committing its
/// editor untouched, or leaving the sheet with its editor open, stages
/// nothing, and a save of another edit keeps its formula.
#[gpui::test]
fn the_legend_is_not_an_edit_and_is_not_saved(cx: &mut TestAppContext) {
    let legend = dbflux_i18n::t!("document.spreadsheet.formula_pending").to_string();

    let directory = TestDirectory::new("legend-not-an-edit");
    let xlsx = directory.file("items.xlsx", &items_workbook());
    let ods = directory.file("in.ods", &fixture("in.ods"));

    let (document, window) = open_local(cx, xlsx.clone());

    type_into_cell(&document, window, 1, 3, "=B2+1");
    save(&document, window);
    assert!(!is_dirty(&document, window));
    assert_eq!(display(&document, window, 1, 3), legend);

    commit_untouched(&document, window, 1, 3);
    assert!(
        !is_dirty(&document, window),
        "an untouched commit is no edit"
    );

    let table = table_state(&document, window);
    window.update(|window, cx| {
        table.update(cx, |state, cx| {
            assert!(state.start_editing(CellCoord::new(1, 3), window, cx));
        })
    });
    select_sheet(&document, window, 1);
    select_sheet(&document, window, 0);
    assert!(
        !is_dirty(&document, window),
        "leaving the sheet with the editor open is no edit"
    );

    type_into_cell(&document, window, 1, 0, "Quill");
    save(&document, window);

    let grid = read_sheet(&xlsx, 0);
    assert_eq!(value(&grid, 1, 0), text("Quill"));
    assert_eq!(formula(&grid, 1, 3), CellFormula::Text("=B2+1".into()));

    // Every ods formula loses its cached result on a save.
    let (document, window) = open_local(cx, ods.clone());

    type_into_cell(&document, window, 1, 0, "first");
    save(&document, window);
    assert_eq!(display(&document, window, 1, 3), legend);

    commit_untouched(&document, window, 1, 3);
    assert!(
        !is_dirty(&document, window),
        "an untouched commit is no edit"
    );

    type_into_cell(&document, window, 2, 0, "second");
    save(&document, window);

    let grid = read_sheet(&ods, 0);
    assert_eq!(value(&grid, 2, 0), text("second"));
    assert_eq!(
        formula(&grid, 1, 3),
        CellFormula::Text("of:=[.B2]*2".into())
    );
}

#[gpui::test]
fn leading_apostrophe_forces_text(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("apostrophe");
    let path = directory.file("typed.xlsx", &typed_workbook());

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 0, 0, "'42");
    type_into_cell(&document, window, 0, 1, "false");
    type_into_cell(&document, window, 0, 3, "42");
    type_into_cell(&document, window, 0, 4, "'=1+1");

    save(&document, window);
    assert_eq!(toast_count(window), 0);

    let grid = read_sheet(&path, 0);
    assert_eq!(
        value(&grid, 0, 0),
        text("42"),
        "the apostrophe is not stored"
    );
    assert_eq!(value(&grid, 0, 1), CellValue::Bool(false));
    assert_eq!(value(&grid, 0, 3), CellValue::Number(42.0));
    assert_eq!(value(&grid, 0, 4), text("=1+1"));
    assert_eq!(formula(&grid, 0, 4), CellFormula::None);
}

#[gpui::test]
fn iso_date_in_a_date_cell_is_a_date_and_elsewhere_text(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("iso-date");
    let path = directory.file("typed.xlsx", &typed_workbook());

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 0, 2, "2025-03-04");
    type_into_cell(&document, window, 0, 3, "2025-03-04");

    save(&document, window);
    assert_eq!(toast_count(window), 0);

    let date = NaiveDate::from_ymd_opt(2025, 3, 4)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .expect("the date is valid");

    let grid = read_sheet(&path, 0);
    assert_eq!(value(&grid, 0, 2), CellValue::Date(date));
    assert_eq!(value(&grid, 0, 3), text("2025-03-04"));
}

// -- Saving -------------------------------------------------------------------

#[gpui::test]
fn a_refused_cell_writes_nothing_and_keeps_edits(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("refused");
    let path = directory.file("items.xlsx", &items_workbook());
    let original = read(&path);

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);

    type_into_cell(&document, window, 1, 0, "Quill");
    type_into_cell(&document, window, 2, 0, "bad\u{1}");

    save(&document, window);

    assert_eq!(save_results(&events), [false]);
    assert_eq!(toast_count(window), 1);
    assert_eq!(last_toast_title(window), Some(save_failed_title(&path)));
    assert_eq!(read(&path), original, "nothing is written");
    assert!(is_dirty(&document, window), "the edits stay");
    assert_eq!(display(&document, window, 1, 0), "Pen");

    let table_state = table_state(&document, window);
    assert!(window.update(|_, cx| table_state.read(cx).has_pending_operations()));
}

#[gpui::test]
fn untouched_parts_stay_byte_identical_after_a_ui_save(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("byte-identical");
    let path = directory.file("items.xlsx", &items_workbook());
    let before = zip_entries(&read(&path));

    let (document, window) = open_local(cx, path.clone());

    type_into_cell(&document, window, 1, 1, "11");
    save(&document, window);
    assert_eq!(toast_count(window), 0);

    let after = zip_entries(&read(&path));

    // The edited sheet and the workbook part (fullCalcOnLoad) change; a
    // calculation chain is dropped.
    let rewritten = ["xl/worksheets/sheet1.xml", "xl/workbook.xml"];

    for (name, entry) in &before {
        if rewritten.contains(&name.as_str()) || name == "xl/calcChain.xml" {
            continue;
        }

        assert_eq!(after.get(name), Some(entry), "{name} changed");
    }

    assert_ne!(
        after.get("xl/worksheets/sheet1.xml"),
        before.get("xl/worksheets/sheet1.xml")
    );
    assert_eq!(value(&read_sheet(&path, 0), 1, 1), CellValue::Number(11.0));
}

#[gpui::test]
fn formula_replacement_warning_counts_formula_cells(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("formula-warning");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path);
    let count = |window: &mut VisualTestContext| {
        window.update(|_, cx| document.read(cx).formula_replacement_count(cx))
    };

    assert_eq!(count(window), 0);
    assert!(window.debug_bounds("spreadsheet-formula-warning").is_none());

    type_into_cell(&document, window, 1, 2, "5");
    type_into_cell(&document, window, 2, 2, "=B3*5");
    type_into_cell(&document, window, 1, 0, "x");

    assert_eq!(count(window), 1, "only a formula cell given a value counts");
    assert!(window.debug_bounds("spreadsheet-formula-warning").is_some());
    assert_eq!(
        readout_at(&document, window, 1, 2).1,
        FormulaReadout::NoFormula,
        "a value replaces the formula"
    );
}

#[gpui::test]
fn pending_edits_survive_a_sheet_switch(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("switch-edits");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path);

    type_into_cell(&document, window, 1, 0, "Quill");
    append_row(&document, window);
    type_into_cell(&document, window, 3, 0, "Cap");

    select_sheet(&document, window, 1);

    assert!(is_dirty(&document, window));
    assert!(window.update(|_, cx| document.read(cx).sheet_has_pending_edits(0, cx)));
    assert!(!window.update(|_, cx| document.read(cx).sheet_has_pending_edits(1, cx)));
    assert_eq!(display(&document, window, 0, 0), "first");

    select_sheet(&document, window, 0);

    assert_eq!(shown_rows(&document, window), 4);

    let table_state = table_state(&document, window);
    let (edited, appended) = window.update(|_, cx| {
        let state = table_state.read(cx);
        let buffer = state.edit_buffer();
        let base = dbflux_components::components::data_table::model::CellValue::text("");

        (
            buffer.get_cell(1, 0, &base).display_text().to_string(),
            buffer
                .get_pending_insert_by_idx(0)
                .and_then(|row| row.first())
                .map(|cell| cell.display_text().to_string()),
        )
    });

    assert_eq!(edited, "Quill");
    assert_eq!(appended.as_deref(), Some("Cap"));
    assert!(window.update(|_, cx| document.read(cx).sheet_has_pending_edits(0, cx)));
}

// -- Close and quit -----------------------------------------------------------

#[gpui::test]
fn close_with_edits_asks(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("close");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);
    let pane = pane(&document, window);

    window.update(|_, cx| assert!(pane.change_summary(cx).is_none()));

    type_into_cell(&document, window, 1, 0, "Quill");

    window.update(|_, cx| assert!(pane.change_summary(cx).is_some()));
    assert_eq!(
        window.update(|_, cx| document.read(cx).state()),
        DocumentState::Modified
    );

    let started = window.update(|window, cx| pane.save_for_close(window, cx));
    window.run_until_parked();

    assert!(started);
    assert_eq!(save_results(&events), [true]);
    assert!(asked_to_close(&events), "a save for a close closes the tab");
    assert_eq!(value(&read_sheet(&path, 0), 1, 0), text("Quill"));
}

#[gpui::test]
fn quit_saves_a_small_local_file(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("quit");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path.clone());
    let (events, _subscription) = record_events(&document, window);
    let pane = pane(&document, window);

    assert_eq!(
        window.update(|_, cx| pane.quit_disposition(cx)),
        QuitDisposition::Clean
    );

    type_into_cell(&document, window, 1, 0, "Quill");

    assert_eq!(
        window.update(|_, cx| pane.quit_disposition(cx)),
        QuitDisposition::SavedOnQuit
    );

    let mut finished = false;
    for _ in 0..50 {
        let outstanding = window.update(|_, cx| pane.flush_for_shutdown(cx));
        window.run_until_parked();

        if !outstanding {
            finished = true;
            break;
        }
    }

    assert!(finished, "the shutdown flush finishes");
    assert_eq!(save_results(&events), [true]);
    assert!(!asked_to_close(&events));
    assert_eq!(value(&read_sheet(&path, 0), 1, 0), text("Quill"));
}

// -- Read-only formats --------------------------------------------------------

#[gpui::test]
fn xls_is_read_only_with_a_banner(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("xls");
    let path: PathBuf = directory.file("in.xls", &fixture("in.xls"));
    let original = read(&path);

    let (document, window) = open_local(cx, path.clone());

    let table_state = table_state(&document, window);
    let (editable, insertable) = window.update(|_, cx| {
        let state = table_state.read(cx);
        (state.is_editable(), state.is_insertable())
    });

    assert!(!editable && !insertable);
    assert!(window.debug_bounds("spreadsheet-read-only").is_some());
    assert!(window.debug_bounds("spreadsheet-append-row").is_none());

    let rows = shown_rows(&document, window);
    append_row(&document, window);
    save(&document, window);

    assert_eq!(shown_rows(&document, window), rows);
    assert!(!is_dirty(&document, window));
    assert_eq!(read(&path), original);
}

// -- Editing through clicks and keys ------------------------------------------

/// Opens `path` in an active window, as the app's window is: gpui hides
/// focus changes of an inactive window from its listeners, and the cell
/// editor closes on the blur they report.
fn open_active(
    cx: &mut TestAppContext,
    path: PathBuf,
) -> (Entity<SpreadsheetDocument>, &mut VisualTestContext) {
    let (document, window) = open_local(cx, path);

    window.update(|window, _| window.activate_window());
    window.run_until_parked();

    (document, window)
}

/// Clicks the middle of the element drawn with `id`, as a mouse does.
fn click(window: &mut VisualTestContext, id: &str) {
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

/// The cell the table edits, and the value its inline editor holds.
fn inline_editor(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
) -> Option<(CellCoord, String)> {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        let state = table_state.read(cx);
        let coord = state.editing_cell()?;
        let input = state.cell_input()?;

        Some((coord, input.read(cx).value().to_string()))
    })
}

/// Clicks the cell at `row`, `column`, presses Enter, and checks that the
/// cell's inline editor opened with the cell's value.
fn assert_click_and_enter_edits(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
    row: usize,
    column: usize,
) {
    let expected = display(document, window, row, column);

    click(window, &format!("cell-{}", row * 10000 + column));
    press(window, "enter");

    assert_eq!(
        inline_editor(document, window),
        Some((CellCoord::new(row, column), expected)),
        "a click on the cell and Enter open its inline editor"
    );
}

#[gpui::test]
fn a_clicked_cell_opens_its_editor_on_enter_and_the_button_appends_a_row(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("click-edit");
    let path = directory.file("in.xlsx", &fixture("libreoffice.xlsx"));

    let (document, window) = open_active(cx, path);

    assert_click_and_enter_edits(&document, window, 2, 0);

    let rows = shown_rows(&document, window);
    click(window, "spreadsheet-append-row");

    assert_eq!(shown_rows(&document, window), rows + 1);
    assert!(is_dirty(&document, window));
}

#[gpui::test]
fn a_double_clicked_cell_opens_its_editor(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("double-click-edit");
    let path = directory.file("in.xlsx", &fixture("libreoffice.xlsx"));

    let (document, window) = open_active(cx, path);
    let expected = display(&document, window, 2, 0);

    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    let bounds = frame
        .nodes()
        .find(|(_, node)| node.id() == "cell-20000")
        .map(|(_, node)| node.bounds())
        .expect("`cell-20000` is drawn");

    window.simulate_click(bounds.center(), Modifiers::default());
    window.run_until_parked();
    window.simulate_event(gpui::MouseDownEvent {
        position: bounds.center(),
        modifiers: Modifiers::default(),
        button: gpui::MouseButton::Left,
        click_count: 2,
        first_mouse: false,
    });
    window.simulate_event(gpui::MouseUpEvent {
        position: bounds.center(),
        modifiers: Modifiers::default(),
        button: gpui::MouseButton::Left,
        click_count: 2,
    });
    window.run_until_parked();

    assert_eq!(
        inline_editor(&document, window),
        Some((CellCoord::new(2, 0), expected))
    );
}

#[gpui::test]
fn a_cell_of_another_sheet_and_of_an_ods_opens_its_editor_on_enter(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("click-edit-ods");
    let path = directory.file("in.ods", &fixture("in.ods"));

    let (document, window) = open_active(cx, path);

    assert_click_and_enter_edits(&document, window, 2, 0);

    select_sheet(&document, window, 1);
    assert_click_and_enter_edits(&document, window, 0, 1);

    let rows = shown_rows(&document, window);
    click(window, "spreadsheet-append-row");
    assert_eq!(shown_rows(&document, window), rows + 1);
}
