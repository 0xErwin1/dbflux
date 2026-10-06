#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::io::{Cursor, Read, Write};

use chrono::NaiveDate;
use dbflux_byte_source::MemorySource;
use rust_xlsxwriter::{Chart, ChartType, Format, Formula, Note, Table};
use zip::write::SimpleFileOptions;

use crate::test_support::{fixture, fixture_path, open_bytes, xlsx_bytes};
use crate::{CellFormula, CellValue, FormulaRangeKind, SheetWriteError};

use super::{CellEdit, XlsxEdits, patch_xlsx};

const MAIN_NAMESPACE: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const RELATIONSHIP_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
const WORKSHEET_TYPE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet";
const CALC_CHAIN_TYPE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain";
const CALC_CHAIN_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.calcChain+xml";

fn patch(bytes: &[u8], edits: &XlsxEdits) -> Result<Vec<u8>, SheetWriteError> {
    let mut sink = Cursor::new(Vec::new());
    patch_xlsx(MemorySource::new(bytes.to_vec()), edits, &mut sink)?;
    Ok(sink.into_inner())
}

fn edits(sheet: usize, cells: &[(usize, usize, CellEdit)]) -> XlsxEdits {
    let mut edits = XlsxEdits::new();
    for (row, column, edit) in cells {
        edits.set(sheet, *row, *column, edit.clone());
    }
    edits
}

fn read_part(bytes: &[u8], name: &str) -> String {
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut text = String::new();
    archive
        .by_name(name)
        .unwrap()
        .read_to_string(&mut text)
        .unwrap();
    text
}

/// Returns the `<c>` element of `reference` as the sheet XML writes it.
fn cell_element(xml: &str, reference: &str) -> Option<String> {
    let start = xml.find(&format!("<c r=\"{reference}\""))?;
    let rest = &xml[start..];
    let open_end = rest.find('>')?;

    if rest[..open_end].ends_with('/') {
        return Some(rest[..=open_end].to_string());
    }

    let close = rest.find("</c>")?;
    Some(rest[..close + "</c>".len()].to_string())
}

fn worksheet(sheet_data: &str) -> String {
    worksheet_with(sheet_data, "")
}

fn worksheet_with(sheet_data: &str, after_sheet_data: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n\
         <worksheet xmlns=\"{MAIN_NAMESPACE}\" xmlns:r=\"{RELATIONSHIP_NAMESPACE}\">\
         <sheetData>{sheet_data}</sheetData>{after_sheet_data}</worksheet>"
    )
}

/// A minimal xlsx package built by hand, for shapes `rust_xlsxwriter` does
/// not write.
#[derive(Default)]
struct HandPackage<'a> {
    workbook_properties: &'a str,
    /// Elements written after `<sheets>` in the workbook part.
    workbook_tail: &'a str,
    /// Each sheet is `(name, relationship target, worksheet XML)`.
    sheets: Vec<(&'a str, &'a str, String)>,
    /// Adds `xl/calcChain.xml` with its relationship and content type.
    calc_chain: bool,
}

impl HandPackage<'_> {
    fn build(&self) -> Vec<u8> {
        let mut sheet_elements = String::new();
        let mut relationships = String::new();
        let mut overrides = String::new();

        for (index, (name, target, _)) in self.sheets.iter().enumerate() {
            let number = index + 1;
            sheet_elements.push_str(&format!(
                "<sheet name=\"{name}\" sheetId=\"{number}\" r:id=\"rId{number}\"/>"
            ));
            relationships.push_str(&format!(
                "<Relationship Id=\"rId{number}\" Type=\"{WORKSHEET_TYPE}\" Target=\"{target}\"/>"
            ));
            overrides.push_str(&format!(
                "<Override PartName=\"/{}\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>",
                super::resolve_target(super::WORKBOOK_PART, target)
            ));
        }

        let mut extra_parts = Vec::new();
        if self.calc_chain {
            relationships.push_str(&format!(
                "<Relationship Id=\"rIdChain\" Type=\"{CALC_CHAIN_TYPE}\" Target=\"calcChain.xml\"/>"
            ));
            overrides.push_str(&format!(
                "<Override PartName=\"/xl/calcChain.xml\" ContentType=\"{CALC_CHAIN_CONTENT_TYPE}\"/>"
            ));
            extra_parts.push((
                "xl/calcChain.xml".to_string(),
                format!("<calcChain xmlns=\"{MAIN_NAMESPACE}\"><c r=\"A1\" i=\"1\"/></calcChain>"),
            ));
        }

        let workbook_properties = self.workbook_properties;
        let workbook_tail = self.workbook_tail;
        let parts = [
            (
                "[Content_Types].xml".to_string(),
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">\
                     <Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>\
                     <Default Extension=\"xml\" ContentType=\"application/xml\"/>\
                     <Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>\
                     {overrides}</Types>"
                ),
            ),
            (
                "_rels/.rels".to_string(),
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">\
                 <Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>\
                 </Relationships>"
                    .to_string(),
            ),
            (
                "xl/workbook.xml".to_string(),
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <workbook xmlns=\"{MAIN_NAMESPACE}\" xmlns:r=\"{RELATIONSHIP_NAMESPACE}\">\
                     <workbookPr {workbook_properties}/><sheets>{sheet_elements}</sheets>{workbook_tail}</workbook>"
                ),
            ),
            (
                "xl/_rels/workbook.xml.rels".to_string(),
                format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                     <Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">\
                     {relationships}</Relationships>"
                ),
            ),
        ];

        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let sheet_parts = self.sheets.iter().map(|(_, target, xml)| {
            (
                super::resolve_target(super::WORKBOOK_PART, target),
                xml.clone(),
            )
        });

        for (name, content) in parts.into_iter().chain(extra_parts).chain(sheet_parts) {
            writer
                .start_file(name, SimpleFileOptions::default())
                .unwrap();
            writer.write_all(content.as_bytes()).unwrap();
        }

        writer.finish().unwrap().into_inner()
    }
}

fn hand_package(workbook_properties: &str, sheets: &[(&str, &str, String)]) -> Vec<u8> {
    HandPackage {
        workbook_properties,
        sheets: sheets.to_vec(),
        ..HandPackage::default()
    }
    .build()
}

fn one_sheet(sheet_data: &str) -> Vec<u8> {
    one_sheet_with(sheet_data, "")
}

fn one_sheet_with(sheet_data: &str, after_sheet_data: &str) -> Vec<u8> {
    hand_package(
        "",
        &[(
            "Data",
            "worksheets/sheet1.xml",
            worksheet_with(sheet_data, after_sheet_data),
        )],
    )
}

/// Asserts that every entry but `changed` is stored exactly as before, in
/// the same order.
fn assert_entries_unchanged(original: &[u8], patched: &[u8], changed: &[&str]) {
    let mut before_archive = zip::ZipArchive::new(Cursor::new(original)).unwrap();
    let mut after_archive = zip::ZipArchive::new(Cursor::new(patched)).unwrap();
    assert_eq!(before_archive.len(), after_archive.len());

    for index in 0..before_archive.len() {
        let mut before = before_archive.by_index_raw(index).unwrap();
        let mut after = after_archive.by_index_raw(index).unwrap();
        assert_eq!(before.name(), after.name(), "entry order");

        if changed.contains(&before.name()) {
            continue;
        }

        assert_eq!(
            before.compression(),
            after.compression(),
            "{}",
            before.name()
        );
        assert_eq!(before.crc32(), after.crc32(), "{}", before.name());

        let mut before_raw = Vec::new();
        let mut after_raw = Vec::new();
        before.read_to_end(&mut before_raw).unwrap();
        after.read_to_end(&mut after_raw).unwrap();
        assert_eq!(before_raw, after_raw, "{}", before.name());
    }
}

fn entry_names(bytes: &[u8]) -> Vec<String> {
    zip::ZipArchive::new(Cursor::new(bytes))
        .unwrap()
        .file_names()
        .map(str::to_string)
        .collect()
}

#[test]
fn edit_keeps_style_index() {
    let bytes = xlsx_bytes(|workbook| {
        let money = Format::new().set_num_format("#,##0.00");
        let sheet = workbook.add_worksheet();
        sheet.write(0, 0, "Amount")?;
        sheet.write_with_format(1, 0, 50.5, &money)?;
        Ok(())
    });

    let original = read_part(&bytes, "xl/worksheets/sheet1.xml");
    let original_cell = cell_element(&original, "A2").unwrap();
    let style = original_cell
        .split("s=\"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .unwrap()
        .to_string();

    let patched = patch(&bytes, &edits(0, &[(1, 0, CellEdit::Number(1234.25))])).unwrap();

    let xml = read_part(&patched, "xl/worksheets/sheet1.xml");
    assert_eq!(
        cell_element(&xml, "A2").unwrap(),
        format!("<c r=\"A2\" s=\"{style}\"><v>1234.25</v></c>")
    );
    assert_eq!(
        xml.replace(&cell_element(&xml, "A2").unwrap(), ""),
        original
            .replace(&original_cell, "")
            .replace("<row r=\"2\" spans=\"1:1\">", "<row r=\"2\">"),
        "nothing but the edited cell and its row's spans changes"
    );
}

#[test]
fn text_written_inline_shared_strings_untouched() {
    let bytes = xlsx_bytes(|workbook| {
        let sheet = workbook.add_worksheet();
        sheet.write(0, 0, "first")?;
        sheet.write(0, 1, "second")?;
        Ok(())
    });

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[(0, 0, CellEdit::Text(" a < b & c\r\n_x0041_ ".to_string()))],
        ),
    )
    .unwrap();

    let xml = read_part(&patched, "xl/worksheets/sheet1.xml");
    assert_eq!(
        cell_element(&xml, "A1").unwrap(),
        "<c r=\"A1\" t=\"inlineStr\"><is><t xml:space=\"preserve\"> a &lt; b &amp; c&#13;\n\
         _x005F_x0041_ </t></is></c>"
    );
    assert_eq!(
        read_part(&patched, "xl/sharedStrings.xml"),
        read_part(&bytes, "xl/sharedStrings.xml")
    );

    let mut workbook = open_bytes(patched).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    assert_eq!(
        grid.cell(0, 0).unwrap().value,
        CellValue::Text(" a < b & c\r\n_x0041_ ".into())
    );
    assert_eq!(
        grid.cell(0, 1).unwrap().value,
        CellValue::Text("second".into())
    );
}

#[test]
fn inserts_missing_row_and_cell_in_order() {
    let bytes = one_sheet(
        "<row r=\"1\" spans=\"1:3\"><c r=\"A1\"><v>1</v></c><c r=\"C1\"><v>3</v></c></row>\
         <row r=\"5\" spans=\"1:1\"><c r=\"A5\"><v>5</v></c></row>\
         <row r=\"6\" ht=\"20\" customHeight=\"1\"/>",
    );

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[
                (0, 1, CellEdit::Number(2.0)),
                (0, 3, CellEdit::Number(4.0)),
                (2, 0, CellEdit::Bool(true)),
                (5, 1, CellEdit::Number(6.0)),
                (6, 0, CellEdit::Formula("A1+A5".to_string())),
                (8, 0, CellEdit::Clear),
            ],
        ),
    )
    .unwrap();

    assert_eq!(
        read_part(&patched, "xl/worksheets/sheet1.xml"),
        worksheet(
            "<row r=\"1\"><c r=\"A1\"><v>1</v></c><c r=\"B1\"><v>2</v></c>\
             <c r=\"C1\"><v>3</v></c><c r=\"D1\"><v>4</v></c></row>\
             <row r=\"3\"><c r=\"A3\" t=\"b\"><v>1</v></c></row>\
             <row r=\"5\" spans=\"1:1\"><c r=\"A5\"><v>5</v></c></row>\
             <row r=\"6\" ht=\"20\" customHeight=\"1\"><c r=\"B6\"><v>6</v></c></row>\
             <row r=\"7\"><c r=\"A7\"><f>A1+A5</f></c></row>"
        )
    );
}

#[test]
fn formula_with_leading_equals_is_written_without_it() {
    let bytes = one_sheet("<row r=\"1\"><c r=\"A1\"><v>1</v></c></row>");

    let patched = patch(
        &bytes,
        &edits(0, &[(0, 1, CellEdit::Formula("=A1*2".to_string()))]),
    )
    .unwrap();

    let xml = read_part(&patched, "xl/worksheets/sheet1.xml");
    assert_eq!(
        cell_element(&xml, "B1").unwrap(),
        "<c r=\"B1\"><f>A1*2</f></c>"
    );
}

#[test]
fn clear_keeps_style() {
    let bytes = one_sheet(
        "<row r=\"1\"><c r=\"A1\" s=\"3\" t=\"s\"><v>0</v></c><c r=\"B1\"><f>1+1</f><v>2</v></c>\
         <c r=\"C1\"><v>7</v></c></row>",
    );

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[
                (0, 0, CellEdit::Clear),
                (0, 1, CellEdit::Clear),
                (0, 4, CellEdit::Clear),
            ],
        ),
    )
    .unwrap();

    assert_eq!(
        read_part(&patched, "xl/worksheets/sheet1.xml"),
        worksheet("<row r=\"1\"><c r=\"A1\" s=\"3\"/><c r=\"C1\"><v>7</v></c></row>")
    );
}

#[test]
fn clear_before_cell_without_reference_keeps_its_place() {
    let bytes = one_sheet("<row r=\"1\"><c r=\"A1\"><v>1</v></c><c><v>2</v></c></row>");

    let patched = patch(&bytes, &edits(0, &[(0, 0, CellEdit::Clear)])).unwrap();

    assert_eq!(
        read_part(&patched, "xl/worksheets/sheet1.xml"),
        worksheet("<row r=\"1\"><c r=\"A1\"/><c><v>2</v></c></row>")
    );
}

#[test]
fn refuses_shared_formula_master() {
    let bytes = one_sheet(
        "<row r=\"1\"><c r=\"A1\"><f t=\"shared\" ref=\"A1:A3\" si=\"0\">B1*2</f><v>2</v></c></row>\
         <row r=\"2\"><c r=\"A2\"><f t=\"shared\" si=\"0\"/><v>4</v></c></row>\
         <row r=\"3\"><c r=\"A3\"><f t=\"shared\" si=\"0\"/><v>6</v></c></row>",
    );

    match patch(&bytes, &edits(0, &[(0, 0, CellEdit::Number(1.0))])) {
        Err(SheetWriteError::SharedFormulaMaster { sheet, cell, range }) => {
            assert_eq!(
                (sheet.as_str(), cell.as_str(), range.as_str()),
                ("Data", "A1", "A1:A3")
            );
        }
        other => panic!("expected a shared-formula refusal, got {other:?}"),
    }

    let patched = patch(&bytes, &edits(0, &[(1, 0, CellEdit::Number(9.0))])).unwrap();
    let xml = read_part(&patched, "xl/worksheets/sheet1.xml");
    assert_eq!(
        cell_element(&xml, "A2").unwrap(),
        "<c r=\"A2\"><v>9</v></c>"
    );
    assert!(xml.contains("<f t=\"shared\" ref=\"A1:A3\" si=\"0\">B1*2</f>"));
}

#[test]
fn refuses_cell_in_array_formula() {
    let bytes = one_sheet(
        "<row r=\"1\"><c r=\"A1\"><f t=\"array\" ref=\"A1:C2\">B5:D6*2</f><v>1</v></c>\
         <c r=\"B1\"><v>2</v></c></row>\
         <row r=\"2\"><c r=\"A2\"><v>3</v></c></row>\
         <row r=\"4\"><c r=\"E4\"><f t=\"dataTable\" ref=\"E4:F5\" dt2D=\"0\" dtr=\"0\" r1=\"A9\"/>\
         <v>0</v></c></row>",
    );

    let refusals = [
        ((0, 0), "A1", "A1:C2", FormulaRangeKind::Array),
        ((0, 1), "B1", "A1:C2", FormulaRangeKind::Array),
        // C2 is not in the file, but its value comes from the array formula.
        ((1, 2), "C2", "A1:C2", FormulaRangeKind::Array),
        ((4, 5), "F5", "E4:F5", FormulaRangeKind::DataTable),
    ];

    for ((row, column), expected_cell, expected_range, expected_kind) in refusals {
        match patch(&bytes, &edits(0, &[(row, column, CellEdit::Number(0.0))])) {
            Err(SheetWriteError::InsideFormulaRange {
                cell, range, kind, ..
            }) => {
                assert_eq!(
                    (cell.as_str(), range.as_str(), kind),
                    (expected_cell, expected_range, expected_kind)
                );
            }
            other => panic!("expected a formula-range refusal for {expected_cell}, got {other:?}"),
        }
    }

    patch(&bytes, &edits(0, &[(1, 3, CellEdit::Number(0.0))])).unwrap();
}

#[test]
fn refuses_values_a_cell_cannot_store() {
    let bytes = one_sheet("<row r=\"1\"><c r=\"A1\"><v>1</v></c></row>");

    let too_long = "x".repeat(32_768);
    match patch(&bytes, &edits(0, &[(0, 0, CellEdit::Text(too_long))])) {
        Err(SheetWriteError::TextTooLong { cell, length, .. }) => {
            assert_eq!((cell.as_str(), length), ("A1", 32_768));
        }
        other => panic!("expected a text-too-long refusal, got {other:?}"),
    }
    patch(
        &bytes,
        &edits(0, &[(0, 0, CellEdit::Text("x".repeat(32_767)))]),
    )
    .unwrap();

    match patch(
        &bytes,
        &edits(0, &[(2, 27, CellEdit::Text("bell\u{7}".to_string()))]),
    ) {
        Err(SheetWriteError::InvalidCharacter {
            cell, character, ..
        }) => {
            assert_eq!((cell.as_str(), character), ("AB3", '\u{7}'));
        }
        other => panic!("expected an invalid-character refusal, got {other:?}"),
    }

    assert!(matches!(
        patch(&bytes, &edits(0, &[(0, 0, CellEdit::Number(f64::NAN))])),
        Err(SheetWriteError::NonFiniteNumber { .. })
    ));
    assert!(matches!(
        patch(&bytes, &edits(0, &[(1_048_576, 0, CellEdit::Number(1.0))])),
        Err(SheetWriteError::CellOutOfRange { .. })
    ));
    assert!(matches!(
        patch(&bytes, &edits(1, &[(0, 0, CellEdit::Number(1.0))])),
        Err(SheetWriteError::SheetOutOfRange {
            index: 1,
            sheet_count: 1
        })
    ));
}

#[test]
fn date_uses_1904_system() {
    let noon = NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let sheet_data = "<row r=\"1\"><c r=\"A1\" s=\"1\"><v>0</v></c></row>";

    let system_1904 = hand_package(
        "date1904=\"1\"",
        &[("Data", "worksheets/sheet1.xml", worksheet(sheet_data))],
    );
    let patched = patch(&system_1904, &edits(0, &[(0, 0, CellEdit::Date(noon))])).unwrap();
    assert_eq!(
        cell_element(&read_part(&patched, "xl/worksheets/sheet1.xml"), "A1").unwrap(),
        "<c r=\"A1\" s=\"1\"><v>43830.5</v></c>"
    );

    let system_1900 = one_sheet(sheet_data);
    let patched = patch(&system_1900, &edits(0, &[(0, 0, CellEdit::Date(noon))])).unwrap();
    assert_eq!(
        cell_element(&read_part(&patched, "xl/worksheets/sheet1.xml"), "A1").unwrap(),
        "<c r=\"A1\" s=\"1\"><v>45292.5</v></c>"
    );

    let before_1904 = NaiveDate::from_ymd_opt(1903, 12, 31)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    assert!(matches!(
        patch(
            &system_1904,
            &edits(0, &[(0, 0, CellEdit::Date(before_1904))])
        ),
        Err(SheetWriteError::DateOutOfRange { .. })
    ));
    patch(
        &system_1900,
        &edits(0, &[(0, 0, CellEdit::Date(before_1904))]),
    )
    .unwrap();
}

#[test]
fn absolute_and_relative_sheet_targets_resolve() {
    let bytes = hand_package(
        "",
        &[
            (
                "Absolute",
                "/xl/worksheets/first.xml",
                worksheet("<row r=\"1\"><c r=\"A1\"><v>1</v></c></row>"),
            ),
            ("Relative", "worksheets/second.xml", worksheet("")),
        ],
    );

    let mut all_edits = edits(0, &[(0, 0, CellEdit::Number(10.0))]);
    all_edits.set(1, 0, 0, CellEdit::Number(20.0));
    let patched = patch(&bytes, &all_edits).unwrap();

    let mut workbook = open_bytes(patched).unwrap();
    assert_eq!(
        workbook.read_sheet(0).unwrap().cell(0, 0).unwrap().value,
        CellValue::Number(10.0)
    );
    assert_eq!(
        workbook.read_sheet(1).unwrap().cell(0, 0).unwrap().value,
        CellValue::Number(20.0)
    );

    // calamine cannot open a sheet behind a target with `.` or `..`
    // segments, so those are checked on the resolver alone.
    assert_eq!(
        super::resolve_target(
            super::WORKBOOK_PART,
            "./worksheets/../worksheets/sheet3.xml"
        ),
        "xl/worksheets/sheet3.xml"
    );
    assert_eq!(
        super::resolve_target("xl/worksheets/sheet1.xml", "../tables/table1.xml"),
        "xl/tables/table1.xml"
    );
}

#[test]
fn untouched_entries_byte_identical() {
    let bytes = xlsx_bytes(|workbook| {
        let data = workbook.add_worksheet().set_name("Data")?;
        data.write(0, 0, "Name")?;
        data.write(1, 0, 5)?;
        data.write(2, 0, 7)?;
        data.insert_note(1, 1, &Note::new("a note"))?;

        let mut chart = Chart::new(ChartType::Column);
        chart.add_series().set_values("Data!$A$2:$A$3");
        data.insert_chart(4, 4, &chart)?;

        workbook
            .add_worksheet()
            .set_name("Other")?
            .write(0, 0, "untouched")?;
        workbook.add_vba_project(fixture_path("vbaProject.bin"))?;
        Ok(())
    });

    let patched = patch(&bytes, &edits(0, &[(1, 0, CellEdit::Number(6.0))])).unwrap();

    assert_entries_unchanged(&bytes, &patched, &["xl/worksheets/sheet1.xml"]);
    assert!(patched.windows(4).any(|window| window == b"PK\x05\x06"));
}

#[test]
fn reopens_with_calamine_showing_edits() {
    let bytes = xlsx_bytes(|workbook| {
        let date = Format::new().set_num_format("yyyy-mm-dd");
        let sheet = workbook.add_worksheet();
        sheet.write(0, 0, "header")?;
        sheet.write(1, 0, 1)?;
        sheet.write_with_format(1, 1, 45000, &date)?;
        sheet.write_formula(1, 2, Formula::new("=A2*2").set_result("2"))?;
        Ok(())
    });

    let day = NaiveDate::from_ymd_opt(2024, 2, 29)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let patched = patch(
        &bytes,
        &edits(
            0,
            &[
                (0, 0, CellEdit::Text("renamed".to_string())),
                (1, 0, CellEdit::Number(21.0)),
                (1, 1, CellEdit::Date(day)),
                (1, 2, CellEdit::Formula("A2*3".to_string())),
                (2, 0, CellEdit::Bool(false)),
                (2, 3, CellEdit::Number(0.1)),
            ],
        ),
    )
    .unwrap();

    let mut workbook = open_bytes(patched).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    let value = |row, column| grid.cell(row, column).unwrap().value.clone();

    assert_eq!(value(0, 0), CellValue::Text("renamed".into()));
    assert_eq!(value(1, 0), CellValue::Number(21.0));
    assert_eq!(value(1, 1), CellValue::Date(day));
    assert_eq!(
        grid.cell(1, 2).unwrap().formula,
        CellFormula::Text("=A2*3".into())
    );
    assert_eq!(
        value(1, 2),
        CellValue::Empty,
        "a written formula has no cached value"
    );
    assert_eq!(value(2, 0), CellValue::Bool(false));
    assert_eq!(value(2, 3), CellValue::Number(0.1));
}

#[test]
fn calc_chain_removed_with_rel_and_override() {
    let bytes = HandPackage {
        sheets: vec![(
            "Data",
            "worksheets/sheet1.xml",
            worksheet(
                "<row r=\"1\"><c r=\"A1\"><v>1</v></c><c r=\"B1\"><f>A1*2</f><v>2</v></c></row>",
            ),
        )],
        calc_chain: true,
        ..HandPackage::default()
    }
    .build();

    let unchanged = patch(&bytes, &XlsxEdits::new()).unwrap();
    assert_entries_unchanged(&bytes, &unchanged, &[]);

    let patched = patch(&bytes, &edits(0, &[(0, 0, CellEdit::Number(5.0))])).unwrap();

    assert!(!entry_names(&patched).contains(&"xl/calcChain.xml".to_string()));

    let relationships = read_part(&patched, "xl/_rels/workbook.xml.rels");
    assert!(!relationships.contains("calcChain"), "{relationships}");
    assert!(relationships.contains("Target=\"worksheets/sheet1.xml\""));

    let content_types = read_part(&patched, "[Content_Types].xml");
    assert!(!content_types.contains("calcChain"), "{content_types}");
    assert!(content_types.contains("PartName=\"/xl/worksheets/sheet1.xml\""));

    let mut workbook = open_bytes(patched).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    assert_eq!(grid.cell(0, 0).unwrap().value, CellValue::Number(5.0));
}

#[test]
fn calcpr_full_calc_on_load_set_or_inserted_in_schema_order() {
    let patched_workbook = |workbook_tail: &str| {
        let bytes = HandPackage {
            workbook_tail,
            sheets: vec![("Data", "worksheets/sheet1.xml", worksheet(""))],
            ..HandPackage::default()
        }
        .build();
        let patched = patch(&bytes, &edits(0, &[(0, 0, CellEdit::Number(1.0))])).unwrap();
        let xml = read_part(&patched, "xl/workbook.xml");
        xml[xml.find("</sheets>").unwrap() + "</sheets>".len()..].to_string()
    };

    let defined_names =
        "<definedNames><definedName name=\"x\">Data!$A$1</definedName></definedNames>";

    assert_eq!(
        patched_workbook(&format!("{defined_names}<calcPr calcId=\"191029\"/>")),
        format!("{defined_names}<calcPr calcId=\"191029\" fullCalcOnLoad=\"1\"/></workbook>")
    );
    assert_eq!(
        patched_workbook("<calcPr fullCalcOnLoad=\"0\" calcId=\"1\"></calcPr>"),
        "<calcPr fullCalcOnLoad=\"1\" calcId=\"1\"></calcPr></workbook>"
    );
    assert_eq!(
        patched_workbook(&format!("{defined_names}<oleSize ref=\"A1:B2\"/><extLst/>")),
        format!(
            "{defined_names}<calcPr fullCalcOnLoad=\"1\"/><oleSize ref=\"A1:B2\"/><extLst/></workbook>"
        )
    );
    assert_eq!(
        patched_workbook(""),
        "<calcPr fullCalcOnLoad=\"1\"/></workbook>"
    );

    let already_set = xlsx_bytes(|workbook| {
        workbook.add_worksheet().write(0, 0, 1)?;
        Ok(())
    });
    assert!(read_part(&already_set, "xl/workbook.xml").contains("fullCalcOnLoad=\"1\""));
    let patched = patch(&already_set, &edits(0, &[(0, 0, CellEdit::Number(2.0))])).unwrap();
    assert_entries_unchanged(&already_set, &patched, &["xl/worksheets/sheet1.xml"]);
}

#[test]
fn append_row_skips_merge_ending_past_last_row() {
    let bytes = one_sheet_with(
        "<row r=\"1\"><c r=\"A1\" s=\"1\"><v>1</v></c></row>\
         <row r=\"3\"><c r=\"A3\" s=\"2\"><v>3</v></c><c r=\"B3\"><v>4</v></c></row>",
        "<mergeCells count=\"1\"><mergeCell ref=\"B3:C6\"/></mergeCells>",
    );

    let mut workbook = open_bytes(bytes.clone()).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    assert_eq!(grid.append_row(), Some(6));
    assert_eq!(grid.row_count(), 6, "padded to the append row");
    assert_eq!(grid.cell(5, 1).unwrap().value, CellValue::Empty);

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[
                (6, 0, CellEdit::Number(7.0)),
                (6, 1, CellEdit::Number(8.0)),
                (7, 0, CellEdit::Number(9.0)),
                (4, 0, CellEdit::Number(5.0)),
            ],
        ),
    )
    .unwrap();

    let xml = read_part(&patched, "xl/worksheets/sheet1.xml");
    assert_eq!(
        cell_element(&xml, "A7").unwrap(),
        "<c r=\"A7\" s=\"2\"><v>7</v></c>",
        "an appended cell takes the style of the cell above"
    );
    assert_eq!(
        cell_element(&xml, "B7").unwrap(),
        "<c r=\"B7\"><v>8</v></c>"
    );
    assert_eq!(
        cell_element(&xml, "A8").unwrap(),
        "<c r=\"A8\" s=\"2\"><v>9</v></c>"
    );
    assert_eq!(
        cell_element(&xml, "A5").unwrap(),
        "<c r=\"A5\"><v>5</v></c>",
        "a cell above the append row keeps no borrowed style"
    );
    assert!(xml.contains("<mergeCell ref=\"B3:C6\"/>"));
}

#[test]
fn append_row_follows_cell_references_in_rows_without_one() {
    let bytes = one_sheet(
        "<row r=\"1\"><c r=\"A1\"><v>1</v></c></row>\
         <row><c r=\"A100\" s=\"2\"><v>2</v></c></row>",
    );

    let mut workbook = open_bytes(bytes).unwrap();
    let grid = workbook.read_sheet(0).unwrap();

    assert_eq!(grid.cell(99, 0).unwrap().value, CellValue::Number(2.0));
    assert_eq!(grid.append_row(), Some(100));
}

#[test]
fn sheet_whose_append_row_cannot_be_found_still_reads() {
    let bytes = one_sheet_with(
        "<row r=\"1\"><c r=\"A1\"><v>1</v></c></row>",
        "<tableParts count=\"1\"><tablePart r:id=\"rId9\"/></tableParts>",
    );

    let mut workbook = open_bytes(bytes.clone()).unwrap();
    let grid = workbook.read_sheet(0).unwrap();

    assert_eq!(grid.cell(0, 0).unwrap().value, CellValue::Number(1.0));
    assert_eq!(grid.append_row(), None);

    assert!(
        patch(&bytes, &edits(0, &[(1, 0, CellEdit::Number(2.0))])).is_err(),
        "the patcher scans the sheet again and refuses it"
    );
}

#[test]
fn append_after_table_does_not_grow_it() {
    let bytes = xlsx_bytes(|workbook| {
        let sheet = workbook.add_worksheet();
        sheet.write(0, 0, "Key")?;
        sheet.write(0, 1, "Value")?;
        sheet.write(1, 0, "a")?;
        sheet.write(1, 1, 1)?;
        sheet.add_table(0, 0, 4, 1, &Table::new())?;
        Ok(())
    });

    let table = read_part(&bytes, "xl/tables/table1.xml");
    assert!(table.contains("ref=\"A1:B5\""), "{table}");

    let mut workbook = open_bytes(bytes.clone()).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    assert_eq!(grid.append_row(), Some(5));
    assert_eq!(grid.row_count(), 5);

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[
                (5, 0, CellEdit::Text("after".to_string())),
                (5, 1, CellEdit::Number(2.0)),
            ],
        ),
    )
    .unwrap();

    assert_entries_unchanged(&bytes, &patched, &["xl/worksheets/sheet1.xml"]);

    let mut reopened = open_bytes(patched).unwrap();
    let grid = reopened.read_sheet(0).unwrap();
    assert_eq!(
        grid.cell(5, 0).unwrap().value,
        CellValue::Text("after".into())
    );
    assert_eq!(grid.append_row(), Some(6));
}

#[test]
fn dimension_updated_only_when_present() {
    let bytes = xlsx_bytes(|workbook| {
        let sheet = workbook.add_worksheet();
        sheet.write(0, 0, 1)?;
        sheet.write(1, 1, 2)?;
        Ok(())
    });
    assert!(read_part(&bytes, "xl/worksheets/sheet1.xml").contains("<dimension ref=\"A1:B2\"/>"));

    let inside = patch(&bytes, &edits(0, &[(0, 0, CellEdit::Number(3.0))])).unwrap();
    assert!(read_part(&inside, "xl/worksheets/sheet1.xml").contains("<dimension ref=\"A1:B2\"/>"));

    let grown = patch(
        &bytes,
        &edits(0, &[(4, 3, CellEdit::Number(4.0)), (9, 9, CellEdit::Clear)]),
    )
    .unwrap();
    assert!(read_part(&grown, "xl/worksheets/sheet1.xml").contains("<dimension ref=\"A1:D5\"/>"));

    let without = one_sheet("<row r=\"1\"><c r=\"A1\"><v>1</v></c></row>");
    let patched = patch(&without, &edits(0, &[(4, 3, CellEdit::Number(4.0))])).unwrap();
    assert!(!read_part(&patched, "xl/worksheets/sheet1.xml").contains("dimension"));
}

#[test]
fn xlsm_vba_part_identical() {
    let bytes = xlsx_bytes(|workbook| {
        workbook.add_worksheet().write(0, 0, "macro host")?;
        workbook.add_vba_project(fixture_path("vbaProject.bin"))?;
        Ok(())
    });

    let patched = patch(&bytes, &edits(0, &[(3, 0, CellEdit::Number(1.0))])).unwrap();

    assert_entries_unchanged(&bytes, &patched, &["xl/worksheets/sheet1.xml"]);
    assert!(entry_names(&patched).contains(&"xl/vbaProject.bin".to_string()));
    assert_eq!(
        open_bytes(patched).unwrap().format(),
        crate::SpreadsheetFormat::Xlsm
    );
}

#[test]
fn edits_on_two_sheets_in_one_patch() {
    let bytes = xlsx_bytes(|workbook| {
        workbook.add_worksheet().set_name("First")?.write(0, 0, 1)?;
        workbook
            .add_worksheet()
            .set_name("Middle")?
            .write(0, 0, 2)?;
        workbook.add_worksheet().set_name("Last")?.write(0, 0, 3)?;
        Ok(())
    });

    let mut all_edits = edits(0, &[(0, 0, CellEdit::Number(10.0))]);
    all_edits.set(2, 1, 0, CellEdit::Text("appended".to_string()));
    let patched = patch(&bytes, &all_edits).unwrap();

    assert_entries_unchanged(
        &bytes,
        &patched,
        &["xl/worksheets/sheet1.xml", "xl/worksheets/sheet3.xml"],
    );

    let mut workbook = open_bytes(patched).unwrap();
    let first = workbook.read_sheet(0).unwrap();
    let middle = workbook.read_sheet(1).unwrap();
    let last = workbook.read_sheet(2).unwrap();

    assert_eq!(first.cell(0, 0).unwrap().value, CellValue::Number(10.0));
    assert_eq!(middle.cell(0, 0).unwrap().value, CellValue::Number(2.0));
    assert_eq!(last.cell(0, 0).unwrap().value, CellValue::Number(3.0));
    assert_eq!(
        last.cell(1, 0).unwrap().value,
        CellValue::Text("appended".into())
    );
}

#[test]
fn libreoffice_written_file_keeps_every_other_part() {
    let bytes = fixture("libreoffice.xlsx");

    let mut workbook = open_bytes(bytes.clone()).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    assert_eq!(
        grid.append_row(),
        Some(15),
        "after the table ending on row 15"
    );

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[
                (1, 1, CellEdit::Number(99.0)),
                (15, 0, CellEdit::Text("appended".to_string())),
                (15, 1, CellEdit::Number(30.0)),
            ],
        ),
    )
    .unwrap();

    assert_entries_unchanged(
        &bytes,
        &patched,
        &["xl/worksheets/sheet1.xml", "xl/workbook.xml"],
    );

    let workbook_xml = read_part(&patched, "xl/workbook.xml");
    assert!(
        workbook_xml.contains(
            "<calcPr iterateCount=\"100\" refMode=\"A1\" iterate=\"false\" iterateDelta=\"0.0001\" fullCalcOnLoad=\"1\"/>"
        ),
        "{workbook_xml}"
    );

    let sheet = read_part(&patched, "xl/worksheets/sheet1.xml");
    assert_eq!(
        cell_element(&sheet, "B2").unwrap(),
        "<c r=\"B2\" s=\"2\"><v>99</v></c>"
    );
    assert!(
        sheet.contains(
            "<row r=\"16\" customFormat=\"false\" ht=\"13.8\" hidden=\"false\" customHeight=\"false\" \
             outlineLevel=\"0\" collapsed=\"false\"><c r=\"A16\" s=\"0\" t=\"inlineStr\">"
        ),
        "{sheet}"
    );

    let mut reopened = open_bytes(patched).unwrap();
    let grid = reopened.read_sheet(0).unwrap();
    assert_eq!(grid.cell(1, 1).unwrap().value, CellValue::Number(99.0));
    assert_eq!(
        grid.cell(15, 0).unwrap().value,
        CellValue::Text("appended".into())
    );
    assert_eq!(grid.cell(15, 1).unwrap().value, CellValue::Number(30.0));
    assert_eq!(
        reopened.read_sheet(1).unwrap().cell(0, 1).unwrap().formula,
        CellFormula::Text("=Data!B5*10".into())
    );
}
