#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::io::Write;
use std::ops::Range;
use std::sync::Arc;

use chrono::NaiveDate;
use dbflux_byte_source::{ByteSource, FileSource, MemorySource, SourceError};
use rust_xlsxwriter::{Chart, ChartType, ExcelDateTime, Format, Formula};

use crate::test_support::{fixture, fixture_path, open_bytes, xlsx_bytes};
use crate::{
    CellFormula, CellValue, SheetCell, SheetGrid, SheetKind, SpreadsheetError, SpreadsheetFormat,
    open,
};

fn cell(grid: &SheetGrid, row: usize, column: usize) -> &SheetCell {
    grid.cell(row, column)
        .unwrap_or_else(|| panic!("no cell at ({row}, {column})"))
}

fn sheet_names<S>(workbook: &crate::Workbook<S>) -> Vec<&str> {
    workbook
        .sheets()
        .iter()
        .map(|sheet| sheet.name.as_str())
        .collect()
}

#[test]
fn lists_sheets_of_xlsx_ods_xls() {
    let xlsx = xlsx_bytes(|workbook| {
        workbook.add_worksheet().set_name("Data")?;
        workbook.add_worksheet().set_name("Other")?;
        workbook
            .add_worksheet()
            .set_name("Hidden")?
            .set_hidden(true);
        Ok(())
    });

    let workbook = open_bytes(xlsx).unwrap();
    assert_eq!(workbook.format(), SpreadsheetFormat::Xlsx);
    assert!(!workbook.date_1904());
    assert_eq!(sheet_names(&workbook), ["Data", "Other", "Hidden"]);

    let sheets = workbook.sheets();
    assert!(
        sheets
            .iter()
            .all(|sheet| sheet.kind == SheetKind::Worksheet)
    );
    assert!(sheets[0].visible);
    assert!(sheets[1].visible);
    assert!(!sheets[2].visible);

    let ods = open_bytes(fixture("in.ods")).unwrap();
    assert_eq!(ods.format(), SpreadsheetFormat::Ods);
    assert_eq!(sheet_names(&ods), ["Data", "Other"]);

    let xls = open_bytes(fixture("in.xls")).unwrap();
    assert_eq!(xls.format(), SpreadsheetFormat::Xls);
    assert_eq!(sheet_names(&xls), ["Data", "Other"]);
}

#[test]
fn pads_grid_to_a1() {
    let bytes = xlsx_bytes(|workbook| {
        let sheet = workbook.add_worksheet();
        sheet.write(2, 2, "corner")?;
        sheet.write(3, 1, 7)?;
        Ok(())
    });

    let mut workbook = open_bytes(bytes).unwrap();
    let grid = workbook.read_sheet(0).unwrap();

    assert_eq!(grid.row_count(), 4);
    assert_eq!(grid.column_count(), 3);

    let origin = cell(&grid, 0, 0);
    assert_eq!(origin.value, CellValue::Empty);
    assert_eq!(&*origin.display, "");
    assert_eq!(origin.formula, CellFormula::None);

    assert_eq!(cell(&grid, 2, 2).value, CellValue::Text("corner".into()));
    assert_eq!(&*cell(&grid, 2, 2).display, "corner");
    assert_eq!(cell(&grid, 3, 1).value, CellValue::Number(7.0));
    assert_eq!(&*cell(&grid, 3, 1).display, "7");

    assert_eq!(grid.row(3).map(<[SheetCell]>::len), Some(3));
    assert!(grid.cell(4, 0).is_none());
    assert!(grid.cell(0, 3).is_none());
}

#[test]
fn xlsx_formula_has_equals_and_cached_value() {
    let bytes = xlsx_bytes(|workbook| {
        let sheet = workbook.add_worksheet();
        sheet.write(0, 0, 21.5)?;
        sheet.write_formula(0, 1, Formula::new("=A1*2").set_result("43"))?;
        sheet.write_formula(1, 0, Formula::new("=1/0").set_result("#DIV/0!"))?;
        Ok(())
    });

    let mut workbook = open_bytes(bytes).unwrap();
    let grid = workbook.read_sheet(0).unwrap();

    let formula_cell = cell(&grid, 0, 1);
    assert_eq!(formula_cell.value, CellValue::Number(43.0));
    assert_eq!(&*formula_cell.display, "43");
    assert_eq!(formula_cell.formula, CellFormula::Text("=A1*2".into()));

    assert_eq!(cell(&grid, 0, 0).formula, CellFormula::None);
    assert_eq!(&*cell(&grid, 0, 0).display, "21.5");

    let error_cell = cell(&grid, 1, 0);
    assert_eq!(&*error_cell.display, "#DIV/0!");
    assert_eq!(error_cell.formula, CellFormula::Text("=1/0".into()));
    assert!(matches!(error_cell.value, CellValue::Error(_)));
}

#[test]
fn ods_formula_is_openformula() {
    let mut workbook = open_bytes(fixture("in.ods")).unwrap();
    let grid = workbook.read_sheet(0).unwrap();

    let doubled = cell(&grid, 1, 3);
    assert_eq!(doubled.value, CellValue::Number(101.0));
    assert_eq!(&*doubled.display, "101");
    assert_eq!(doubled.formula, CellFormula::Text("of:=[.B2]*2".into()));

    let other = workbook.read_sheet(1).unwrap();
    assert_eq!(
        cell(&other, 0, 1).formula,
        CellFormula::Text("of:=[$Data.B5]*10".into())
    );
    assert_eq!(&*cell(&other, 0, 1).display, "2020");
}

#[test]
fn xls_formula_is_unavailable() {
    let mut workbook = open_bytes(fixture("in.xls")).unwrap();
    let grid = workbook.read_sheet(0).unwrap();

    let doubled = cell(&grid, 1, 3);
    assert_eq!(doubled.value, CellValue::Number(101.0));
    assert_eq!(doubled.formula, CellFormula::Unavailable);

    let header = cell(&grid, 0, 0);
    assert_eq!(header.value, CellValue::Text("Name".into()));
    assert_eq!(header.formula, CellFormula::Unavailable);

    let blank = cell(&grid, 6, 0);
    assert_eq!(blank.value, CellValue::Empty);
    assert_eq!(blank.formula, CellFormula::None);
}

#[test]
fn empty_sheet_reads_zero_rows() {
    let bytes = xlsx_bytes(|workbook| {
        workbook.add_worksheet().set_name("Empty")?;
        Ok(())
    });

    let mut workbook = open_bytes(bytes).unwrap();
    let grid = workbook.read_sheet(0).unwrap();

    assert_eq!(grid.row_count(), 0);
    assert_eq!(grid.column_count(), 0);
    assert!(grid.cell(0, 0).is_none());
    assert!(grid.row(0).is_none());
}

#[test]
fn chart_sheet_is_a_typed_error() {
    let bytes = xlsx_bytes(|workbook| {
        let data = workbook.add_worksheet().set_name("Data")?;
        data.write(0, 0, 1)?;
        data.write(1, 0, 2)?;

        let mut chart = Chart::new(ChartType::Column);
        chart.add_series().set_values("Data!$A$1:$A$2");

        workbook
            .add_chartsheet()
            .set_name("Chart")?
            .insert_chart(0, 0, &chart)?;
        Ok(())
    });

    let mut workbook = open_bytes(bytes).unwrap();
    assert_eq!(workbook.sheets()[0].kind, SheetKind::Worksheet);
    assert_eq!(workbook.sheets()[1].kind, SheetKind::ChartSheet);

    match workbook.read_sheet(1) {
        Err(SpreadsheetError::ChartSheet { name }) => assert_eq!(name, "Chart"),
        other => panic!("expected a chart sheet error, got {other:?}"),
    }

    match workbook.read_sheet(2) {
        Err(SpreadsheetError::SheetOutOfRange { index, sheet_count }) => {
            assert_eq!((index, sheet_count), (2, 2));
        }
        other => panic!("expected an out-of-range error, got {other:?}"),
    }
}

#[test]
fn not_a_spreadsheet_is_a_typed_error() {
    let mut other_zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    other_zip
        .start_file(
            "word/document.xml",
            zip::write::SimpleFileOptions::default(),
        )
        .unwrap();
    other_zip.write_all(b"<document/>").unwrap();
    let other_zip = other_zip.finish().unwrap().into_inner();

    let mut text_document = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    text_document
        .start_file("mimetype", zip::write::SimpleFileOptions::default())
        .unwrap();
    text_document
        .write_all(b"application/vnd.oasis.opendocument.text")
        .unwrap();
    let text_document = text_document.finish().unwrap().into_inner();

    for bytes in [
        Vec::new(),
        b"name,city\nAda,London\n".to_vec(),
        other_zip,
        text_document,
    ] {
        match open_bytes(bytes) {
            Err(SpreadsheetError::NotASpreadsheet { .. }) => {}
            other => panic!("expected a not-a-spreadsheet error, got {other:?}"),
        }
    }
}

#[test]
fn encrypted_file_is_typed_error() {
    match open_bytes(fixture("encrypted.xlsx")) {
        Err(SpreadsheetError::Encrypted) => {}
        other => panic!("expected an encrypted error, got {other:?}"),
    }
}

#[test]
fn format_is_detected_from_content_not_extension() {
    let xlsx = xlsx_bytes(|workbook| {
        workbook.add_worksheet();
        Ok(())
    });
    let xlsm = xlsx_bytes(|workbook| {
        workbook.add_worksheet();
        workbook.add_vba_project(fixture_path("vbaProject.bin"))?;
        Ok(())
    });

    assert_eq!(open_bytes(xlsx).unwrap().format(), SpreadsheetFormat::Xlsx);
    assert_eq!(open_bytes(xlsm).unwrap().format(), SpreadsheetFormat::Xlsm);

    // `open` takes no file name, so these fixtures are recognized by their
    // bytes; the file source reads `in.xls` under its own name to show the
    // name plays no part.
    assert_eq!(
        open_bytes(fixture("in.ods")).unwrap().format(),
        SpreadsheetFormat::Ods
    );
    let file = Arc::new(FileSource::open(fixture_path("in.xls")).unwrap());
    assert_eq!(open(file).unwrap().format(), SpreadsheetFormat::Xls);
}

#[test]
fn dates_display_iso_in_xlsx_and_ods() {
    let bytes = xlsx_bytes(|workbook| {
        let sheet = workbook.add_worksheet();
        let date = Format::new().set_num_format("yyyy-mm-dd");
        let date_time = Format::new().set_num_format("yyyy-mm-dd hh:mm:ss");

        sheet.write_datetime_with_format(0, 0, ExcelDateTime::from_ymd(2024, 1, 1)?, &date)?;
        sheet.write_datetime_with_format(
            0,
            1,
            ExcelDateTime::from_ymd(2024, 3, 5)?.and_hms(13, 45, 30)?,
            &date_time,
        )?;
        Ok(())
    });

    let mut workbook = open_bytes(bytes).unwrap();
    let grid = workbook.read_sheet(0).unwrap();

    let new_year = NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    assert_eq!(cell(&grid, 0, 0).value, CellValue::Date(new_year));
    assert_eq!(&*cell(&grid, 0, 0).display, "2024-01-01");
    assert_eq!(&*cell(&grid, 0, 1).display, "2024-03-05 13:45:30");

    let mut ods = open_bytes(fixture("in.ods")).unwrap();
    let ods_grid = ods.read_sheet(0).unwrap();
    assert_eq!(cell(&ods_grid, 1, 2).value, CellValue::Date(new_year));
    assert_eq!(&*cell(&ods_grid, 1, 2).display, "2024-01-01");
}

/// Serves the format header and fails every later read.
#[derive(Clone)]
struct FailsAfterHeader(MemorySource);

impl ByteSource for FailsAfterHeader {
    fn byte_length(&self) -> Result<u64, SourceError> {
        self.0.byte_length()
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        if range.start == 0 && range.end <= 8 {
            return self.0.read_range(range);
        }

        Err(SourceError::new("the object store went away"))
    }
}

#[test]
fn source_failure_stays_a_source_error() {
    let bytes = xlsx_bytes(|workbook| {
        workbook.add_worksheet();
        Ok(())
    });

    match open(FailsAfterHeader(MemorySource::new(bytes))) {
        Err(SpreadsheetError::Source(error)) => {
            assert!(error.to_string().contains("the object store went away"));
        }
        other => panic!("expected a source error, got {other:?}"),
    }
}

#[test]
fn compound_file_source_failure_stays_a_source_error() {
    match open(FailsAfterHeader(MemorySource::new(fixture("in.xls")))) {
        Err(SpreadsheetError::Source(error)) => {
            assert!(error.to_string().contains("the object store went away"));
        }
        other => panic!("expected a source error, got {other:?}"),
    }
}

#[test]
fn stray_far_cell_is_refused_instead_of_padded() {
    let bytes = xlsx_bytes(|workbook| {
        workbook.add_worksheet().write(1_048_575, 16_383, "far")?;
        Ok(())
    });

    let mut workbook = open_bytes(bytes).unwrap();

    match workbook.read_sheet(0) {
        Err(SpreadsheetError::SheetTooLarge { rows, columns, .. }) => {
            assert_eq!((rows, columns), (1_048_576, 16_384));
        }
        other => panic!("expected a sheet-too-large error, got {other:?}"),
    }
}
