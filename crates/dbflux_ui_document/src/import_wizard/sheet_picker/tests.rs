// Import only what the tests need: re-globbing `use gpui::*` next to
// `#[gpui::test]` blows rustc's macro recursion limit in this crate.
use std::path::{Path, PathBuf};

use dbflux_transfer::FileFormat;
use dbflux_transfer::import_file::ImportSheet;
use gpui::{Entity, VisualTestContext};
use rust_xlsxwriter::{Formula, Workbook, Worksheet, XlsxError};

use super::{choose_sheet, uses_sheet_picker, visible_sheet_names};
use crate::import_wizard::file_flow::tests::{scratch_file, wizard_on_fake_connection};
use crate::import_wizard::{ImportWizard, WizardStep};

fn add_sheet<'a>(book: &'a mut Workbook, name: &str) -> Result<&'a mut Worksheet, XlsxError> {
    let sheet = book.add_worksheet();
    sheet.set_name(name)?;
    Ok(sheet)
}

fn write_header(sheet: &mut Worksheet, names: &[&str]) -> Result<(), XlsxError> {
    for (column, name) in (0u16..).zip(names) {
        sheet.write_string(0, column, *name)?;
    }
    Ok(())
}

/// Saves a workbook the test builds into a fresh scratch directory and
/// returns the directory (for cleanup) with the file.
pub(in crate::import_wizard) fn write_workbook(
    tag: &str,
    name: &str,
    build: impl FnOnce(&mut Workbook) -> Result<(), XlsxError>,
) -> (PathBuf, PathBuf) {
    let mut book = Workbook::new();
    build(&mut book).expect("build the workbook");
    let bytes = book.save_to_buffer().expect("save the workbook");

    scratch_file(tag, name, &bytes)
}

/// Ventas Q3 (id, total), Clientes (name), a hidden sheet and Resumen (key).
pub(in crate::import_wizard) fn several_sheets(tag: &str) -> (PathBuf, PathBuf) {
    write_workbook(tag, "ventas_2026.xlsx", |book| {
        let sales = add_sheet(book, "Ventas Q3")?;
        write_header(sales, &["id", "total"])?;
        sales.write_number(1, 0, 1.0)?;
        sales.write_number(1, 1, 10.5)?;
        let clients = add_sheet(book, "Clientes")?;
        write_header(clients, &["name"])?;
        clients.write_string(1, 0, "Ana")?;
        add_sheet(book, "Oculta")?.set_hidden(true);
        let summary = add_sheet(book, "Resumen")?;
        write_header(summary, &["key"])?;
        summary.write_string(1, 0, "k")?;
        Ok(())
    })
}

pub(in crate::import_wizard) fn pick(
    wizard: &Entity<ImportWizard>,
    window: &mut VisualTestContext,
    path: &Path,
) {
    let path = path.to_path_buf();
    wizard.update(window, |this, cx| {
        let connection = this.resolve_connection(cx).expect("connection");
        this.open_picked_file(connection, path, cx);
    });
    window.run_until_parked();
}

fn source_names(wizard: &Entity<ImportWizard>, window: &mut VisualTestContext) -> Vec<String> {
    window.update(|_, cx| {
        wizard.read(cx).rows[0]
            .config
            .source_columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    })
}

fn dropdown_label(wizard: &Entity<ImportWizard>, window: &mut VisualTestContext) -> Option<String> {
    window.update(|_, cx| {
        let dropdown = wizard
            .read(cx)
            .file_inputs
            .as_ref()
            .expect("inputs")
            .sheet_dropdown
            .clone();
        dropdown
            .read(cx)
            .selected_label()
            .map(|label| label.to_string())
    })
}

#[test]
fn only_a_spreadsheet_is_imported_one_sheet_at_a_time() {
    assert!(uses_sheet_picker(FileFormat::Spreadsheet));
    for format in [
        FileFormat::Csv,
        FileFormat::Json,
        FileFormat::JsonLines,
        FileFormat::Xml,
    ] {
        assert!(!uses_sheet_picker(format), "{format:?}");
    }
}

#[test]
fn hidden_worksheets_are_left_out_of_the_list() {
    let sheets = vec![
        ImportSheet {
            name: "Data".to_string(),
            visible: true,
        },
        ImportSheet {
            name: "Hidden".to_string(),
            visible: false,
        },
        ImportSheet {
            name: "Other".to_string(),
            visible: true,
        },
    ];

    assert_eq!(visible_sheet_names(sheets), ["Data", "Other"]);
}

#[test]
fn the_choice_lists_the_visible_sheets_and_starts_on_the_first() {
    let (directory, path) = several_sheets("choice_default");

    let choice = choose_sheet(&path, None).expect("choice");

    assert_eq!(choice.sheets, ["Ventas Q3", "Clientes", "Resumen"]);
    assert_eq!(choice.selected, "Ventas Q3");
    std::fs::remove_dir_all(&directory).ok();
}

#[test]
fn a_requested_visible_sheet_is_kept_and_a_hidden_or_unknown_one_falls_back() {
    let (directory, path) = several_sheets("choice_requested");

    assert_eq!(
        choose_sheet(&path, Some("Clientes"))
            .expect("choice")
            .selected,
        "Clientes"
    );
    for requested in ["Oculta", "Nope"] {
        assert_eq!(
            choose_sheet(&path, Some(requested))
                .expect("choice")
                .selected,
            "Ventas Q3",
            "{requested}"
        );
    }
    std::fs::remove_dir_all(&directory).ok();
}

#[test]
fn a_file_that_is_not_a_workbook_is_not_listed() {
    let (directory, path) = scratch_file("choice_fake", "fake.xlsx", b"id,name\n1,Ada\n");

    let error = choose_sheet(&path, None).expect_err("not a workbook");

    assert!(error.to_string().contains("not a spreadsheet"), "{error}");
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn a_workbook_lands_on_configure_with_the_sheet_row_on_the_first_sheet(
    cx: &mut gpui::TestAppContext,
) {
    let (directory, path) = several_sheets("lands");
    let (wizard, window) = wizard_on_fake_connection(cx);

    pick(&wizard, window, &path);

    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert!(wizard.step == WizardStep::Configure);
        let state = wizard.file_import.as_ref().expect("file");
        assert_eq!(state.sheets, ["Ventas Q3", "Clientes", "Resumen"]);
        assert_eq!(state.sheet.as_deref(), Some("Ventas Q3"));
        assert!(!wizard.file_import_blocked());
    });
    assert_eq!(source_names(&wizard, window), ["id", "total"]);
    assert_eq!(
        dropdown_label(&wizard, window).as_deref(),
        Some("Ventas Q3")
    );
    assert!(window.debug_bounds("import-wizard-sheet").is_some());
    assert!(window.debug_bounds("import-wizard-sheet-hint").is_some());
    assert!(window.debug_bounds("import-wizard-sheet-single").is_none());
    assert!(
        window
            .debug_bounds("import-wizard-json-no-options")
            .is_some(),
        "the header line"
    );
    assert!(
        window
            .debug_bounds("import-wizard-file-null-text")
            .is_none()
    );
    window.update(|_, cx| {
        let dropdown = wizard
            .read(cx)
            .file_inputs
            .as_ref()
            .expect("inputs")
            .sheet_dropdown
            .clone();
        dropdown.update(cx, |dropdown, cx| dropdown.open(cx));
        assert!(
            dropdown.read(cx).is_open(),
            "several sheets: it can be opened"
        );
    });
    std::fs::remove_dir_all(&directory).ok();
}

#[test]
fn the_header_line_and_hints_read_as_the_board_words_them() {
    assert_eq!(
        dbflux_i18n::translate_in("en", "document.import_wizard.file.spreadsheet_header"),
        "Row 1 is the header. A spreadsheet has no delimiter or encoding options: the sheet defines the columns, and an empty cell is NULL."
    );
    assert_eq!(
        dbflux_i18n::translate_in("en", "document.import_wizard.file.sheet_hint"),
        "Each import reads one sheet. Only visible worksheets are listed. Choosing another sheet reads the file again at once."
    );
    assert_eq!(
        dbflux_i18n::translate_in("en", "document.import_wizard.file.sheet_single"),
        "This workbook has one sheet, so there is nothing to choose."
    );
}

#[gpui::test]
fn a_workbook_with_one_visible_sheet_says_so_and_its_dropdown_is_off(
    cx: &mut gpui::TestAppContext,
) {
    let (directory, path) = write_workbook("single", "inventario.xlsx", |book| {
        let only = add_sheet(book, "Inventario")?;
        write_header(only, &["sku"])?;
        only.write_string(1, 0, "a")?;
        add_sheet(book, "Oculta")?.set_hidden(true);
        Ok(())
    });
    let (wizard, window) = wizard_on_fake_connection(cx);

    pick(&wizard, window, &path);

    assert!(window.debug_bounds("import-wizard-sheet").is_some());
    assert!(window.debug_bounds("import-wizard-sheet-single").is_some());
    assert!(window.debug_bounds("import-wizard-sheet-hint").is_none());
    assert_eq!(
        dropdown_label(&wizard, window).as_deref(),
        Some("Inventario")
    );
    window.update(|_, cx| {
        let dropdown = wizard
            .read(cx)
            .file_inputs
            .as_ref()
            .expect("inputs")
            .sheet_dropdown
            .clone();
        dropdown.update(cx, |dropdown, cx| dropdown.open(cx));
        assert!(!dropdown.read(cx).is_open(), "nothing to choose");
    });
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn a_csv_file_has_no_sheet_row(cx: &mut gpui::TestAppContext) {
    let (directory, path) = scratch_file("csv_no_sheet", "people.csv", b"id,name\n1,Ada\n");
    let (wizard, window) = wizard_on_fake_connection(cx);

    pick(&wizard, window, &path);

    assert!(window.debug_bounds("import-wizard-sheet").is_none());
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn choosing_another_sheet_reads_the_file_again_in_the_background(cx: &mut gpui::TestAppContext) {
    let (directory, path) = several_sheets("switch");
    let (wizard, window) = wizard_on_fake_connection(cx);
    pick(&wizard, window, &path);

    wizard.update(window, |this, cx| this.select_sheet(1, cx));
    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert!(wizard.loading, "the read is in flight, not done inline");
        assert!(wizard.file_import_blocked());
        assert_eq!(
            wizard.file_import.as_ref().and_then(|s| s.sheet.as_deref()),
            Some("Clientes")
        );
    });
    window.run_until_parked();

    assert_eq!(
        source_names(&wizard, window),
        ["name"],
        "mapping is rebuilt"
    );
    assert_eq!(dropdown_label(&wizard, window).as_deref(), Some("Clientes"));
    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert!(!wizard.file_import_blocked());
        assert_eq!(
            wizard.file_import.as_ref().expect("file").sheets.len(),
            3,
            "the list survives the read"
        );
    });
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn choosing_the_sheet_already_chosen_does_not_read_again(cx: &mut gpui::TestAppContext) {
    let (directory, path) = several_sheets("same_sheet");
    let (wizard, window) = wizard_on_fake_connection(cx);
    pick(&wizard, window, &path);

    wizard.update(window, |this, cx| this.select_sheet(0, cx));

    window.update(|_, cx| assert!(!wizard.read(cx).loading));
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn the_run_reads_the_chosen_sheet(cx: &mut gpui::TestAppContext) {
    use crate::import_wizard::file_flow::ImportJob;

    let (directory, path) = several_sheets("job");
    let (wizard, window) = wizard_on_fake_connection(cx);
    pick(&wizard, window, &path);
    wizard.update(window, |this, cx| this.select_sheet(2, cx));
    window.run_until_parked();

    window.update(|_, cx| {
        let Some(ImportJob::File { sheet, format, .. }) = wizard.read(cx).import_job() else {
            panic!("a picked file is the job");
        };
        assert_eq!(format, FileFormat::Spreadsheet);
        assert_eq!(sheet.as_deref(), Some("Resumen"));
    });
    std::fs::remove_dir_all(&directory).ok();
}

/// What the Configure step shows for a sheet that cannot be imported: the
/// block, the footer's reason and the Sheet row to pick another sheet.
fn assert_blocked_with(
    wizard: &Entity<ImportWizard>,
    window: &mut VisualTestContext,
    message: &str,
    next_step: &str,
    footer_reason: &str,
) {
    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert!(wizard.step == WizardStep::Configure, "stays on Configure");
        assert!(wizard.file_import_blocked());
        let block = wizard
            .file_import
            .as_ref()
            .and_then(|state| state.error.clone())
            .expect("the error block");
        assert_eq!(block.message, message);
        assert_eq!(block.next_step, next_step);
        assert_eq!(wizard.file_blocked_reason().as_deref(), Some(footer_reason));
    });
    assert!(window.debug_bounds("import-wizard-error").is_some());
    assert!(window.debug_bounds("import-wizard-footer-reason").is_some());
    assert!(
        window.debug_bounds("import-wizard-sheet").is_some(),
        "another sheet can be chosen"
    );
    assert!(
        window
            .debug_bounds("import-wizard-json-no-options")
            .is_none(),
        "a sheet that cannot be read has no header line"
    );
}

#[gpui::test]
fn an_empty_sheet_is_a_danger_block_with_its_footer_reason(cx: &mut gpui::TestAppContext) {
    let (directory, path) = write_workbook("empty", "ventas_2026.xlsx", |book| {
        add_sheet(book, "Hoja4")?;
        let data = add_sheet(book, "Ventas Q3")?;
        write_header(data, &["id"])?;
        Ok(())
    });
    let (wizard, window) = wizard_on_fake_connection(cx);

    pick(&wizard, window, &path);

    assert_blocked_with(
        &wizard,
        window,
        "Sheet “Hoja4” is empty: it has no rows.",
        "Pick another sheet above, or add a header row and data to this sheet in the spreadsheet app, save, and choose the file again.",
        "Continue is off: sheet “Hoja4” has no rows.",
    );
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn an_invalid_header_names_the_blank_and_the_repeated_columns(cx: &mut gpui::TestAppContext) {
    let (directory, path) = write_workbook("header", "ventas_2026.xlsx", |book| {
        let data = add_sheet(book, "Ventas Q3")?;
        data.write_string(0, 0, "id")?;
        data.write_string(0, 1, "name")?;
        data.write_string(0, 3, "total")?;
        data.write_string(0, 4, "other")?;
        data.write_string(0, 5, "total")?;
        data.write_string(1, 0, "1")?;
        add_sheet(book, "Otra")?;
        Ok(())
    });
    let (wizard, window) = wizard_on_fake_connection(cx);

    pick(&wizard, window, &path);

    assert_blocked_with(
        &wizard,
        window,
        "Row 1 of sheet “Ventas Q3” is not a valid header: column C is blank and columns D and F are both named “total”.",
        "Give every column of row 1 a unique name in the spreadsheet app, save, and choose the file again, or pick another sheet above.",
        "Continue is off: row 1 needs a unique name in every column.",
    );
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn an_error_cell_names_the_sheet_row_column_and_code(cx: &mut gpui::TestAppContext) {
    let (directory, path) = write_workbook("cell", "ventas_2026.xlsx", |book| {
        let data = add_sheet(book, "Ventas Q3")?;
        write_header(data, &["id", "ingreso", "costo", "margen"])?;
        for row in 1..=6u32 {
            data.write_number(row, 0, f64::from(row))?;
        }
        data.write_formula(6, 3, Formula::new("=1/0").set_result("#DIV/0!"))?;
        Ok(())
    });
    let (wizard, window) = wizard_on_fake_connection(cx);

    pick(&wizard, window, &path);

    assert_blocked_with(
        &wizard,
        window,
        "Sheet “Ventas Q3”, row 7, column D (“margen”) holds the error #DIV/0!.",
        "Fix or clear that cell in the spreadsheet app, save, and choose the file again. Formulas are read as the result last saved in the file.",
        "Continue is off: a cell holds an error value.",
    );
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn a_sheet_over_the_cell_limit_says_the_size_and_the_way_out(cx: &mut gpui::TestAppContext) {
    let (directory, path) = write_workbook("large", "ventas_2026.xlsx", |book| {
        let data = add_sheet(book, "Registro")?;
        write_header(data, &["a"])?;
        data.write_string(999_999, 15, "far")?;
        add_sheet(book, "Otra")?;
        Ok(())
    });
    let (wizard, window) = wizard_on_fake_connection(cx);

    pick(&wizard, window, &path);

    assert_blocked_with(
        &wizard,
        window,
        "Sheet “Registro” has 16,000,000 cells, over the limit of 10,000,000. The whole sheet is read into memory, so it cannot be imported.",
        "Export it as CSV from the spreadsheet app (Save as → CSV) and import the CSV instead, or pick a smaller sheet above.",
        "Continue is off: the sheet is over 10,000,000 cells.",
    );
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn choosing_a_good_sheet_clears_the_error_and_unblocks_continue(cx: &mut gpui::TestAppContext) {
    let (directory, path) = write_workbook("recover", "ventas_2026.xlsx", |book| {
        add_sheet(book, "Hoja4")?;
        let data = add_sheet(book, "Ventas Q3")?;
        write_header(data, &["id"])?;
        data.write_number(1, 0, 1.0)?;
        Ok(())
    });
    let (wizard, window) = wizard_on_fake_connection(cx);
    pick(&wizard, window, &path);
    window.update(|_, cx| assert!(wizard.read(cx).file_import_blocked()));

    wizard.update(window, |this, cx| this.select_sheet(1, cx));
    window.run_until_parked();

    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert!(!wizard.file_import_blocked());
        assert!(wizard.file_blocked_reason().is_none());
    });
    assert_eq!(source_names(&wizard, window), ["id"]);
    assert!(window.debug_bounds("import-wizard-error").is_none());
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn a_file_that_is_not_a_workbook_returns_to_pick_source_with_its_error(
    cx: &mut gpui::TestAppContext,
) {
    let (directory, path) = scratch_file("fake", "fake.xlsx", b"id,name\n1,Ada\n");
    let (wizard, window) = wizard_on_fake_connection(cx);

    pick(&wizard, window, &path);

    window.update(|_, cx| {
        let wizard = wizard.read(cx);
        assert!(wizard.step == WizardStep::PickFolder);
        assert!(wizard.file_import.is_none());
        let block = wizard.manifest_error.clone().expect("the error");
        assert!(block.message.contains("not a spreadsheet"), "{block:?}");
        assert_eq!(
            block.next_step,
            "Check that the file is a valid workbook without a password and choose it again, or choose another file."
        );
    });
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn an_error_value_in_the_header_row_blocks_the_sheet(cx: &mut gpui::TestAppContext) {
    let (directory, path) = write_workbook("header_cell", "ventas_2026.xlsx", |book| {
        let data = add_sheet(book, "Ventas Q3")?;
        data.write_formula(0, 0, Formula::new("=1/0").set_result("#DIV/0!"))?;
        data.write_string(0, 1, "total")?;
        data.write_number(1, 1, 1.0)?;
        add_sheet(book, "Otra")?;
        Ok(())
    });
    let (wizard, window) = wizard_on_fake_connection(cx);

    pick(&wizard, window, &path);

    assert_blocked_with(
        &wizard,
        window,
        "Sheet “Ventas Q3”, row 1, column A (“#DIV/0!”) holds the error #DIV/0!.",
        "Fix or clear that cell in the spreadsheet app, save, and choose the file again. Formulas are read as the result last saved in the file.",
        "Continue is off: a cell holds an error value.",
    );
    std::fs::remove_dir_all(&directory).ok();
}

#[gpui::test]
fn a_single_sheet_with_a_problem_does_not_say_to_pick_another_sheet(cx: &mut gpui::TestAppContext) {
    let (directory, path) = write_workbook("single_problem", "uno.xlsx", |book| {
        add_sheet(book, "Hoja4")?;
        Ok(())
    });
    let (wizard, window) = wizard_on_fake_connection(cx);

    pick(&wizard, window, &path);

    window.update(|_, cx| {
        let block = wizard
            .read(cx)
            .file_import
            .as_ref()
            .and_then(|state| state.error.clone())
            .expect("the error block");
        assert_eq!(
            block.next_step,
            "Add a header row and data to this sheet in the spreadsheet app, save, and choose the file again."
        );
    });
    std::fs::remove_dir_all(&directory).ok();
}
