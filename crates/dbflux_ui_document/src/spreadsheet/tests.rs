use std::path::PathBuf;

use dbflux_app::keymap::Command;
use dbflux_components::components::data_table::DataTableState;
use dbflux_components::components::data_table::model::{CellValue, ColumnKind};
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};
use dbflux_ui_base::toast::ToastGlobal;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use rust_xlsxwriter::{Chart, ChartType, Formula, XlsxError};

use super::document::SpreadsheetDocument;
use super::grid_model::FormulaReadout;
use crate::keyboard_coverage::SPREADSHEET;
use crate::keyboard_test_support::{host_document, init_keyboard_runtime};
use crate::types::{DocumentKind, DocumentState};

/// A directory removed when the test ends.
pub(super) struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    pub(super) fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "dbflux-spreadsheet-{name}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).expect("the test directory must be creatable");

        Self { path }
    }

    pub(super) fn file(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.path.join(name);
        std::fs::write(&path, bytes).expect("the test file must be writable");
        path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).ok();
    }
}

/// The bytes of a file checked in under `dbflux_spreadsheet/tests/fixtures/`.
pub(super) fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../dbflux_spreadsheet/tests/fixtures")
        .join(name);

    std::fs::read(&path).expect("the fixture must be readable")
}

/// Builds an xlsx in memory.
pub(super) fn xlsx_bytes(
    build: impl FnOnce(&mut rust_xlsxwriter::Workbook) -> Result<(), XlsxError>,
) -> Vec<u8> {
    let mut workbook = rust_xlsxwriter::Workbook::new();
    build(&mut workbook).expect("the test workbook must build");
    workbook
        .save_to_buffer()
        .expect("the test workbook must save")
}

/// A workbook of four sheets: `Items` with a header row, numbers and a
/// formula; `Notes`, a second worksheet; `Archive`, hidden; and `Chart`, a
/// chart sheet.
pub(super) fn items_workbook() -> Vec<u8> {
    xlsx_bytes(|workbook| {
        let items = workbook.add_worksheet().set_name("Items")?;
        items.write(0, 0, "Name")?;
        items.write(0, 1, "Amount")?;
        items.write(0, 2, "Double")?;

        for (row, (name, amount)) in [("Pen", 3), ("Ink", 7)].into_iter().enumerate() {
            let row = row as u32 + 1;
            items.write(row, 0, name)?;
            items.write(row, 1, amount)?;
            items.write_formula(
                row,
                2,
                Formula::new(format!("=B{}*2", row + 1)).set_result((amount * 2).to_string()),
            )?;
        }

        let notes = workbook.add_worksheet().set_name("Notes")?;
        notes.write(0, 0, "first")?;
        notes.write(1, 0, "second")?;
        notes.write(2, 0, "third")?;

        workbook
            .add_worksheet()
            .set_name("Archive")?
            .set_hidden(true)
            .write(0, 0, "old")?;

        let mut chart = Chart::new(ChartType::Column);
        chart.add_series().set_values("Items!$B$2:$B$3");

        workbook
            .add_chartsheet()
            .set_name("Chart")?
            .insert_chart(0, 0, &chart)?;

        Ok(())
    })
}

pub(super) fn open_local(
    cx: &mut TestAppContext,
    path: PathBuf,
) -> (Entity<SpreadsheetDocument>, &mut VisualTestContext) {
    init_keyboard_runtime(cx);

    let (host, window) = host_document(
        cx,
        move |_window, cx| cx.new(|cx| SpreadsheetDocument::open_local(path, cx)),
        |document, _cx| document.active_context(),
        |document, command, window, cx| document.dispatch_command(command, window, cx),
    );

    let document = window.update(|_, cx| host.read(cx).document.clone());
    window.update(|window, cx| document.update(cx, |document, cx| document.focus(window, cx)));
    window.run_until_parked();

    (document, window)
}

pub(super) fn table_state(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
) -> Entity<DataTableState> {
    window.update(|_, cx| {
        document
            .read(cx)
            .table_state()
            .expect("a shown sheet has a table")
            .clone()
    })
}

fn titles(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) -> Vec<String> {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state
            .read(cx)
            .model()
            .columns
            .iter()
            .map(|column| column.title.to_string())
            .collect()
    })
}

fn kinds(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
) -> Vec<ColumnKind> {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state
            .read(cx)
            .model()
            .columns
            .iter()
            .map(|column| column.kind)
            .collect()
    })
}

pub(super) fn display(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
    row: usize,
    column: usize,
) -> String {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state
            .read(cx)
            .model()
            .cell(row, column)
            .expect("the cell exists")
            .display_text()
            .to_string()
    })
}

pub(super) fn row_count(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
) -> usize {
    let table_state = table_state(document, window);

    window.update(|_, cx| table_state.read(cx).model().row_count())
}

fn sheet_names(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
) -> Vec<String> {
    window.update(|_, cx| {
        document
            .read(cx)
            .sheets()
            .iter()
            .map(|sheet| sheet.name.clone())
            .collect()
    })
}

pub(super) fn active_sheet(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
) -> Option<usize> {
    window.update(|_, cx| document.read(cx).active_sheet())
}

fn failure(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) -> String {
    window
        .update(|_, cx| document.read(cx).failure().map(str::to_string))
        .expect("the document reports a failure")
}

pub(super) fn toast_count(window: &mut VisualTestContext) -> usize {
    window.update(|_, cx| cx.global::<ToastGlobal>().host.read(cx).toast_count())
}

/// Selects the cell at zero-based `row` and `column` and returns what the
/// formula readout says about it.
pub(super) fn readout_at(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
    row: usize,
    column: usize,
) -> (String, FormulaReadout) {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.select_cell(CellCoord::new(row, column), cx)
        })
    });
    window.run_until_parked();

    window
        .update(|_, cx| document.read(cx).selected_formula(cx))
        .expect("a cell is selected")
}

#[gpui::test]
fn xlsx_opens_with_its_sheets_as_tabs(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("tabs");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path);

    assert_eq!(
        window.update(|_, cx| document.read(cx).state()),
        DocumentState::Clean
    );
    assert_eq!(
        sheet_names(&document, window),
        ["Items", "Notes", "Archive", "Chart"]
    );
    assert_eq!(active_sheet(&document, window), Some(0));
    assert_eq!(row_count(&document, window), 3);

    let summary = window.update(|_, cx| document.read(cx).summary());
    let summary = summary.expect("an opened workbook has a summary");
    assert!(
        summary.starts_with("4 sheets · 3 rows × 3 columns"),
        "{summary}"
    );
    assert!(summary.contains("in memory"), "{summary}");

    let table_state = table_state(&document, window);
    let (editable, insertable) = window.update(|_, cx| {
        let state = table_state.read(cx);
        (state.is_editable(), state.is_insertable())
    });
    assert!(editable && insertable, "xlsx cells can be edited");

    window.update(|_, cx| table_state.update(cx, |state, cx| state.cycle_sort(1, cx)));
    window.run_until_parked();
    assert!(window.update(|_, cx| table_state.read(cx).sort().is_none()));
    assert_eq!(display(&document, window, 1, 0), "Pen");

    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn sheet_tab_switches_resident_sheet(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("switch");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path);
    let first_table = table_state(&document, window);

    window.update(|window, cx| {
        document.update(cx, |document, cx| document.select_sheet(1, window, cx))
    });
    assert!(window.update(|_, cx| document.read(cx).is_reading_sheet()));
    assert!(
        window.update(|_, cx| document.read(cx).table_state().is_none()),
        "the previous sheet is dropped before the next is read"
    );

    window.run_until_parked();

    assert_eq!(active_sheet(&document, window), Some(1));
    assert_eq!(row_count(&document, window), 3);
    assert_eq!(display(&document, window, 2, 0), "third");
    assert_ne!(
        table_state(&document, window).entity_id(),
        first_table.entity_id()
    );

    // A chart sheet cannot be selected.
    window.update(|window, cx| {
        document.update(cx, |document, cx| document.select_sheet(3, window, cx))
    });
    window.run_until_parked();
    assert_eq!(active_sheet(&document, window), Some(1));

    // Alt+L steps to the hidden worksheet, then skips the chart sheet and
    // wraps around to the first; Alt+H goes back.
    window.simulate_keystrokes("alt-l");
    window.run_until_parked();
    assert_eq!(active_sheet(&document, window), Some(2));
    assert_eq!(display(&document, window, 0, 0), "old");

    window.simulate_keystrokes("alt-l");
    window.run_until_parked();
    assert_eq!(active_sheet(&document, window), Some(0));

    window.simulate_keystrokes("alt-h");
    window.run_until_parked();
    assert_eq!(active_sheet(&document, window), Some(2));

    let handled = window.update(|window, cx| {
        document.update(cx, |document, cx| {
            document.dispatch_command(Command::PrevResultTab, window, cx)
        })
    });
    window.run_until_parked();
    assert!(handled);
    assert_eq!(active_sheet(&document, window), Some(1));
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn formula_readout_shows_formula_no_formula_or_unavailable(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("formula");
    let xlsx = directory.file("items.xlsx", &items_workbook());
    let ods = directory.file("in.ods", &fixture("in.ods"));
    let xls = directory.file("in.xls", &fixture("in.xls"));

    let (document, window) = open_local(cx, xlsx);

    assert_eq!(
        window.update(|_, cx| document.read(cx).selected_formula(cx)),
        None,
        "no cell is selected yet"
    );
    assert_eq!(
        readout_at(&document, window, 1, 2),
        ("C2".to_string(), FormulaReadout::Formula("=B2*2".into()))
    );
    assert_eq!(display(&document, window, 1, 2), "6");
    assert_eq!(
        readout_at(&document, window, 1, 1),
        ("B2".to_string(), FormulaReadout::NoFormula)
    );
    assert!(window.debug_bounds("spreadsheet-formula").is_some());

    let (document, window) = open_local(cx, ods);
    assert_eq!(
        readout_at(&document, window, 1, 3).1,
        FormulaReadout::Formula("of:=[.B2]*2".into())
    );

    let (document, window) = open_local(cx, xls);
    assert_eq!(
        readout_at(&document, window, 1, 3).1,
        FormulaReadout::Unavailable
    );
    assert_eq!(
        readout_at(&document, window, 6, 0).1,
        FormulaReadout::NoFormula,
        "an empty cell holds no formula even in an xls file"
    );
}

/// `bytes`, an xlsx or ods package, with `edits` written into it by the
/// patcher a save uses.
fn patched(
    bytes: &[u8],
    format: dbflux_spreadsheet::SpreadsheetFormat,
    edits: &[(usize, usize, usize, dbflux_spreadsheet::CellEdit)],
) -> Vec<u8> {
    use dbflux_spreadsheet::{SheetEdits, SpreadsheetFormat, patch_ods, patch_xlsx};

    let mut sheet_edits = SheetEdits::new();
    for (sheet, row, column, edit) in edits {
        sheet_edits.set(*sheet, *row, *column, edit.clone());
    }

    let source = dbflux_byte_source::MemorySource::new(bytes.to_vec());
    let mut sink = std::io::Cursor::new(Vec::new());

    match format {
        SpreadsheetFormat::Ods => patch_ods(source, &sheet_edits, &mut sink),
        _ => patch_xlsx(source, &sheet_edits, &mut sink),
    }
    .expect("the patch must succeed");

    sink.into_inner()
}

fn is_placeholder(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
    row: usize,
    column: usize,
) -> bool {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state
            .read(cx)
            .model()
            .cell(row, column)
            .is_some_and(|cell| cell.is_placeholder())
    })
}

/// A formula written without a cached result (one DBFlux writes into an
/// xlsx, and every ods formula after a save) shows a legend instead of an
/// empty cell, while the formula readout still shows its formula.
#[gpui::test]
fn a_formula_without_a_cached_value_shows_the_legend(cx: &mut TestAppContext) {
    use dbflux_spreadsheet::{CellEdit, SpreadsheetFormat};

    let legend = dbflux_i18n::t!("document.spreadsheet.formula_pending").to_string();
    assert_ne!(legend, "document.spreadsheet.formula_pending");

    let directory = TestDirectory::new("formula-pending");
    let xlsx = directory.file(
        "items.xlsx",
        &patched(
            &items_workbook(),
            SpreadsheetFormat::Xlsx,
            &[(0, 1, 3, CellEdit::Formula("B2+1".into()))],
        ),
    );
    let ods = directory.file(
        "in.ods",
        &patched(
            &fixture("in.ods"),
            SpreadsheetFormat::Ods,
            &[(0, 1, 0, CellEdit::Text("first".into()))],
        ),
    );

    let (document, window) = open_local(cx, xlsx);

    assert_eq!(display(&document, window, 1, 3), legend);
    assert!(is_placeholder(&document, window, 1, 3));
    assert_eq!(
        readout_at(&document, window, 1, 3).1,
        FormulaReadout::Formula("=B2+1".into())
    );
    assert_eq!(
        display(&document, window, 1, 2),
        "6",
        "a cached result shows as before"
    );
    assert!(!is_placeholder(&document, window, 1, 2));
    assert_eq!(display(&document, window, 1, 0), "Pen");

    let (document, window) = open_local(cx, ods);

    assert_eq!(display(&document, window, 1, 3), legend);
    assert!(is_placeholder(&document, window, 1, 3));
    assert_eq!(
        readout_at(&document, window, 1, 3).1,
        FormulaReadout::Formula("of:=[.B2]*2".into())
    );
    assert_eq!(display(&document, window, 1, 0), "first");
    assert!(
        !is_placeholder(&document, window, 6, 0),
        "an empty cell shows no legend"
    );
}

#[gpui::test]
fn column_titles_are_letters_and_row_one_is_data(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("titles");
    let bytes = xlsx_bytes(|workbook| {
        let sheet = workbook.add_worksheet();
        sheet.write(0, 0, 1)?;
        sheet.write(1, 0, 2)?;
        sheet.write(0, 1, 1.5)?;
        sheet.write(1, 1, 2)?;
        sheet.write(0, 2, "label")?;
        sheet.write(1, 2, 3)?;
        sheet.write(1, 27, "far")?;
        Ok(())
    });
    let path = directory.file("titles.xlsx", &bytes);

    let (document, window) = open_local(cx, path);

    let titles = titles(&document, window);
    assert_eq!(titles.len(), 28);
    assert_eq!(&titles[..4], ["A", "B", "C", "D"]);
    assert_eq!(&titles[25..], ["Z", "AA", "AB"]);

    assert_eq!(row_count(&document, window), 2);
    assert_eq!(display(&document, window, 0, 0), "1");
    assert_eq!(display(&document, window, 0, 2), "label");
    assert_eq!(display(&document, window, 0, 3), "");

    let kinds = kinds(&document, window);
    assert_eq!(kinds[0], ColumnKind::Integer);
    assert_eq!(kinds[1], ColumnKind::Float);
    assert_eq!(
        kinds[2],
        ColumnKind::Text,
        "row 1 counts: text and a number"
    );
    assert_eq!(kinds[3], ColumnKind::Unknown);
    assert_eq!(kinds[27], ColumnKind::Text);
}

#[gpui::test]
fn a_sheet_too_large_shows_the_typed_error(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("too-large");
    let bytes = xlsx_bytes(|workbook| {
        workbook
            .add_worksheet()
            .set_name("Far")?
            .write(1_048_575, 16_383, "far")?;
        workbook
            .add_worksheet()
            .set_name("Near")?
            .write(0, 0, "near")?;
        Ok(())
    });
    let path = directory.file("far.xlsx", &bytes);

    let (document, window) = open_local(cx, path);

    assert_eq!(
        window.update(|_, cx| document.read(cx).state()),
        DocumentState::Clean,
        "the workbook opened; only its sheet could not be read"
    );
    assert_eq!(
        failure(&document, window),
        dbflux_i18n::t!(
            "document.spreadsheet.error.sheet_too_large",
            sheet = "Far",
            rows = 1_048_576,
            columns = 16_384,
            cells = 1_048_576_usize * 16_384,
            limit = dbflux_spreadsheet::MAX_GRID_CELLS
        )
    );
    assert!(window.debug_bounds("spreadsheet-sheet-failed").is_some());
    assert_eq!(toast_count(window), 1);

    window.simulate_keystrokes("alt-l");
    window.run_until_parked();

    assert_eq!(active_sheet(&document, window), Some(1));
    assert_eq!(display(&document, window, 0, 0), "near");
    assert!(window.update(|_, cx| document.read(cx).failure().is_none()));
}

#[gpui::test]
fn an_encrypted_file_shows_the_typed_error(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("encrypted");
    let path = directory.file("secret.xlsx", &fixture("encrypted.xlsx"));

    let (document, window) = open_local(cx, path);

    assert_eq!(
        window.update(|_, cx| document.read(cx).state()),
        DocumentState::Error
    );
    assert_eq!(
        failure(&document, window),
        dbflux_i18n::t!("document.spreadsheet.error.encrypted")
    );
    assert!(window.debug_bounds("spreadsheet-failed").is_some());
    assert_eq!(toast_count(window), 1);
}

#[gpui::test]
fn a_file_that_is_not_a_spreadsheet_shows_the_typed_error(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("not-a-spreadsheet");
    let path = directory.file("cities.xlsx", b"name,city\nAna,Lima\n");

    let (document, window) = open_local(cx, path);

    assert!(failure(&document, window).starts_with(&dbflux_i18n::t!(
        "document.spreadsheet.error.not_a_spreadsheet"
    )),);
}

#[gpui::test]
fn the_pane_carries_the_close_and_quit_hooks(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("pane");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path);

    let pane = window.update(|_, cx| SpreadsheetDocument::into_pane(document.clone(), cx));

    assert_eq!(pane.kind(), DocumentKind::Spreadsheet);
    window.update(|_, cx| assert_eq!(pane.tab_title(cx), "items.xlsx"));
    assert!(pane.commit_pending_input.is_some());
    assert!(pane.save_for_close.is_some());
    assert!(pane.quit_disposition.is_some());
    assert!(pane.save_for_quit.is_some());
    assert!(pane.discard_for_quit.is_some());
    assert!(pane.flush_for_shutdown.is_some());
}

#[gpui::test]
fn the_spreadsheet_document_is_covered(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("coverage");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path);

    // Save takes a click only while there is an edit to save.
    let table_state = table_state(&document, window);
    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.stage_cell_value(1, 0, CellValue::text("Quill"));
            cx.notify();
        })
    });
    window.run_until_parked();

    let capture = FrameCapture::observe(window);
    let checked: Vec<String> = Coverage::new(SPREADSHEET)
        .assert_covered(&capture.frame(window))
        .into_iter()
        .map(|id| id.to_string())
        .collect();

    for prefix in ["cell-", "header-col-"] {
        assert!(
            checked.iter().any(|id| id.starts_with(prefix)),
            "{checked:?}"
        );
    }

    for id in [
        "spreadsheet-sheet-0",
        "spreadsheet-sheet-1",
        "spreadsheet-sheet-2",
        "spreadsheet-append-row",
        "spreadsheet-save",
    ] {
        assert!(
            checked.iter().any(|checked_id| checked_id == id),
            "{checked:?}"
        );
    }

    assert!(
        !checked.iter().any(|id| id == "spreadsheet-sheet-3"),
        "a chart sheet's tab takes no click: {checked:?}"
    );

    // The switch between the table and the text, drawn in both views.
    for id in [
        "segmented-spreadsheet-view-table",
        "segmented-spreadsheet-view-text",
    ] {
        assert!(
            checked.iter().any(|checked_id| checked_id == id),
            "{checked:?}"
        );
    }

    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            document.show_view(super::text_view::SheetView::Text, cx)
        })
    });
    window.run_until_parked();
    assert!(window.debug_bounds("spreadsheet-text-view").is_some());

    let checked: Vec<String> = Coverage::new(SPREADSHEET)
        .assert_covered(&capture.frame(window))
        .into_iter()
        .map(|id| id.to_string())
        .collect();

    assert!(
        checked
            .iter()
            .any(|id| id == "segmented-spreadsheet-view-table"),
        "{checked:?}"
    );
}
