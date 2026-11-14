#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use chrono::NaiveDate;
use rust_xlsxwriter::{Chart, ChartType};

use crate::test_support::{fixture, open_bytes, xlsx_bytes};
use crate::{CellErrorCode, CellFormula, CellValue, SheetGrid, SheetKind, write_values_xlsx};

/// Writes every worksheet of `bytes` into a new xlsx and returns its bytes.
fn write_values(bytes: Vec<u8>) -> Vec<u8> {
    let mut workbook = open_bytes(bytes).unwrap();
    let mut written = Vec::new();

    write_values_xlsx(&mut workbook, &mut written).unwrap();

    written
}

fn values(grid: &SheetGrid) -> Vec<Vec<CellValue>> {
    (0..grid.row_count())
        .map(|row| {
            grid.row(row)
                .unwrap()
                .iter()
                .map(|cell| cell.value.clone())
                .collect()
        })
        .collect()
}

#[test]
fn write_values_xlsx_round_trips_values_and_sheet_names() {
    let mut original = open_bytes(fixture("in.xls")).unwrap();
    let mut written = open_bytes(write_values(fixture("in.xls"))).unwrap();

    let original_names: Vec<String> = original
        .sheets()
        .iter()
        .map(|sheet| sheet.name.clone())
        .collect();
    let written_names: Vec<String> = written
        .sheets()
        .iter()
        .map(|sheet| sheet.name.clone())
        .collect();

    assert_eq!(written_names, original_names);
    assert_eq!(written_names, ["Data", "Other"]);

    for index in 0..original_names.len() {
        let before = original.read_sheet(index).unwrap();
        let after = written.read_sheet(index).unwrap();

        assert_eq!(values(&after), values(&before), "sheet {index}");

        for row in 0..after.row_count() {
            for cell in after.row(row).unwrap() {
                assert_eq!(cell.formula, CellFormula::None, "formulas become values");
            }
        }
    }

    let data = written.read_sheet(0).unwrap();
    let date = data.cell(1, 2).unwrap();
    assert_eq!(
        date.value,
        CellValue::Date(
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        )
    );
    assert_eq!(&*date.display, "2024-01-01");
}

#[test]
fn write_values_xlsx_skips_chart_sheets_and_keeps_typed_values() {
    let source = xlsx_bytes(|workbook| {
        let values = workbook.add_worksheet().set_name("Values")?;
        values.write_string(0, 0, "=not a formula")?;
        values.write_boolean(0, 1, true)?;
        values.write_number(0, 2, 2.5)?;
        values.write_formula(
            1,
            0,
            rust_xlsxwriter::Formula::new("=1/0").set_result("#DIV/0!"),
        )?;
        values.write_string(3, 3, "far")?;

        let mut chart = Chart::new(ChartType::Column);
        chart.add_series().set_values("Values!$C$1:$C$1");
        workbook
            .add_chartsheet()
            .set_name("Chart")?
            .insert_chart(0, 0, &chart)?;

        workbook
            .add_worksheet()
            .set_name("Hidden")?
            .set_hidden(true)
            .write_string(0, 0, "kept")?;

        workbook.add_worksheet().set_name("Empty")?;

        Ok(())
    });

    let mut written = open_bytes(write_values(source)).unwrap();

    let sheets: Vec<(String, SheetKind, bool)> = written
        .sheets()
        .iter()
        .map(|sheet| (sheet.name.clone(), sheet.kind, sheet.visible))
        .collect();
    assert_eq!(
        sheets,
        [
            ("Values".to_string(), SheetKind::Worksheet, true),
            ("Hidden".to_string(), SheetKind::Worksheet, false),
            ("Empty".to_string(), SheetKind::Worksheet, true),
        ]
    );

    let grid = written.read_sheet(0).unwrap();
    assert_eq!(
        grid.cell(0, 0).unwrap().value,
        CellValue::Text("=not a formula".into())
    );
    assert_eq!(grid.cell(0, 0).unwrap().formula, CellFormula::None);
    assert_eq!(grid.cell(0, 1).unwrap().value, CellValue::Bool(true));
    assert_eq!(grid.cell(0, 2).unwrap().value, CellValue::Number(2.5));
    assert_eq!(
        grid.cell(1, 0).unwrap().value,
        CellValue::Text(CellErrorCode::DivisionByZero.code().into()),
        "an error is written as its text"
    );
    assert_eq!(
        grid.cell(3, 3).unwrap().value,
        CellValue::Text("far".into())
    );
    assert_eq!(grid.cell(2, 0).unwrap().value, CellValue::Empty);

    assert_eq!(
        written.read_sheet(1).unwrap().cell(0, 0).unwrap().value,
        CellValue::Text("kept".into())
    );
    assert_eq!(written.read_sheet(2).unwrap().row_count(), 0);
}

#[test]
fn write_values_xlsx_shows_a_worksheet_when_only_a_chart_sheet_was_shown() {
    let source = xlsx_bytes(|workbook| {
        let mut chart = Chart::new(ChartType::Column);
        chart.add_series().set_values("Data!$A$1:$A$1");

        workbook
            .add_worksheet()
            .set_name("Data")?
            .set_hidden(true)
            .write_number(0, 0, 1)?;

        workbook
            .add_worksheet()
            .set_name("More")?
            .set_hidden(true)
            .write_number(0, 0, 2)?;

        workbook
            .add_chartsheet()
            .set_name("Chart")?
            .set_active(true)
            .insert_chart(0, 0, &chart)?;

        Ok(())
    });

    let source_visibility: Vec<bool> = open_bytes(source.clone())
        .unwrap()
        .sheets()
        .iter()
        .map(|sheet| sheet.visible)
        .collect();
    assert_eq!(source_visibility, [false, false, true]);

    let written = open_bytes(write_values(source)).unwrap();

    let sheets: Vec<(String, bool)> = written
        .sheets()
        .iter()
        .map(|sheet| (sheet.name.clone(), sheet.visible))
        .collect();
    assert_eq!(
        sheets,
        [("Data".to_string(), true), ("More".to_string(), false)],
        "a workbook needs one visible sheet, so the first one written is shown"
    );
}
