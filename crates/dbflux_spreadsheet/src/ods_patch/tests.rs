#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::io::{Cursor, Read, Write};

use chrono::NaiveDate;
use dbflux_byte_source::MemorySource;
use zip::CompressionMethod;
use zip::write::SimpleFileOptions;

use crate::test_support::{assert_entries_unchanged, fixture, open_bytes};
use crate::{CellEdit, CellFormula, CellValue, FormulaRangeKind, SheetEdits, SheetWriteError};

use super::patch_ods;

const ODS_MIMETYPE: &str = "application/vnd.oasis.opendocument.spreadsheet";

fn patch(bytes: &[u8], edits: &SheetEdits) -> Result<Vec<u8>, SheetWriteError> {
    let mut sink = Cursor::new(Vec::new());
    patch_ods(MemorySource::new(bytes.to_vec()), edits, &mut sink)?;
    Ok(sink.into_inner())
}

fn edits(sheet: usize, cells: &[(usize, usize, CellEdit)]) -> SheetEdits {
    let mut edits = SheetEdits::new();
    for (row, column, edit) in cells {
        edits.set(sheet, *row, *column, edit.clone());
    }
    edits
}

fn content(bytes: &[u8]) -> String {
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut text = String::new();
    archive
        .by_name("content.xml")
        .unwrap()
        .read_to_string(&mut text)
        .unwrap();
    text
}

/// Returns the rows of the table named `sheet`, each as the XML the file
/// writes for it.
fn rows(xml: &str, sheet: &str) -> Vec<String> {
    let table_start = xml
        .find(&format!("<table:table table:name=\"{sheet}\""))
        .unwrap();
    let table = &xml[table_start..];
    let table = &table[..table.find("</table:table>").unwrap()];

    let mut rows = Vec::new();
    let mut rest = table;
    while let Some(start) = rest.find("<table:table-row") {
        let row = &rest[start..];
        let open_end = row.find('>').unwrap();
        let end = if row[..open_end].ends_with('/') {
            open_end + 1
        } else {
            row.find("</table:table-row>").unwrap() + "</table:table-row>".len()
        };
        rows.push(row[..end].to_string());
        rest = &row[end..];
    }
    rows
}

/// Builds a minimal ods package around the given `<table:table>` elements.
fn hand_ods(tables: &str) -> Vec<u8> {
    let content = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <office:document-content \
         xmlns:office=\"urn:oasis:names:tc:opendocument:xmlns:office:1.0\" \
         xmlns:table=\"urn:oasis:names:tc:opendocument:xmlns:table:1.0\" \
         xmlns:text=\"urn:oasis:names:tc:opendocument:xmlns:text:1.0\" \
         xmlns:dc=\"http://purl.org/dc/elements/1.1/\" \
         office:version=\"1.3\"><office:body><office:spreadsheet>{tables}\
         </office:spreadsheet></office:body></office:document-content>"
    );
    let manifest = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <manifest:manifest xmlns:manifest=\"urn:oasis:names:tc:opendocument:xmlns:manifest:1.0\" \
         manifest:version=\"1.3\">\
         <manifest:file-entry manifest:full-path=\"/\" manifest:media-type=\"{ODS_MIMETYPE}\"/>\
         <manifest:file-entry manifest:full-path=\"content.xml\" manifest:media-type=\"text/xml\"/>\
         </manifest:manifest>"
    );

    let stored = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    writer.start_file("mimetype", stored).unwrap();
    writer.write_all(ODS_MIMETYPE.as_bytes()).unwrap();

    for (name, text) in [
        ("content.xml", content),
        ("META-INF/manifest.xml", manifest),
    ] {
        writer
            .start_file(name, SimpleFileOptions::default())
            .unwrap();
        writer.write_all(text.as_bytes()).unwrap();
    }

    writer.finish().unwrap().into_inner()
}

fn table(name: &str, rows: &str) -> String {
    format!(
        "<table:table table:name=\"{name}\"><table:table-column table:number-columns-repeated=\"1024\"/>{rows}</table:table>"
    )
}

#[test]
fn splits_repeated_columns_around_edit() {
    let bytes = hand_ods(&table(
        "Data",
        "<table:table-row><table:table-cell table:style-name=\"ce1\" table:number-columns-repeated=\"6\"/></table:table-row>",
    ));

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[
                (0, 0, CellEdit::Number(1.0)),
                (0, 3, CellEdit::Number(7.0)),
                (0, 5, CellEdit::Bool(true)),
            ],
        ),
    )
    .unwrap();

    assert_eq!(
        rows(&content(&patched), "Data"),
        vec![
            "<table:table-row>\
             <table:table-cell table:style-name=\"ce1\" office:value-type=\"float\" office:value=\"1\"><text:p>1</text:p></table:table-cell>\
             <table:table-cell table:style-name=\"ce1\" table:number-columns-repeated=\"2\"/>\
             <table:table-cell table:style-name=\"ce1\" office:value-type=\"float\" office:value=\"7\"><text:p>7</text:p></table:table-cell>\
             <table:table-cell table:style-name=\"ce1\"/>\
             <table:table-cell table:style-name=\"ce1\" office:value-type=\"boolean\" office:boolean-value=\"true\"><text:p>TRUE</text:p></table:table-cell>\
             </table:table-row>"
        ]
    );

    let mut workbook = open_bytes(patched).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    assert_eq!(grid.cell(0, 3).unwrap().value, CellValue::Number(7.0));
    assert_eq!(grid.cell(0, 5).unwrap().value, CellValue::Bool(true));
}

#[test]
fn splits_repeated_rows_around_edit() {
    let row_cells = "<table:table-cell table:number-columns-repeated=\"3\"/>\
                     <table:table-cell table:style-name=\"Default\" table:number-columns-repeated=\"1021\"/>";
    let bytes = hand_ods(&table(
        "Data",
        &format!(
            "<table:table-row table:style-name=\"ro1\" table:number-rows-repeated=\"5\">{row_cells}</table:table-row>"
        ),
    ));

    let patched = patch(
        &bytes,
        &edits(0, &[(2, 1, CellEdit::Text("middle".to_string()))]),
    )
    .unwrap();

    assert_eq!(
        rows(&content(&patched), "Data"),
        vec![
            format!(
                "<table:table-row table:style-name=\"ro1\" table:number-rows-repeated=\"2\">{row_cells}</table:table-row>"
            ),
            "<table:table-row table:style-name=\"ro1\">\
             <table:table-cell/>\
             <table:table-cell office:value-type=\"string\"><text:p>middle</text:p></table:table-cell>\
             <table:table-cell/>\
             <table:table-cell table:style-name=\"Default\" table:number-columns-repeated=\"1021\"/>\
             </table:table-row>"
                .to_string(),
            format!(
                "<table:table-row table:style-name=\"ro1\" table:number-rows-repeated=\"2\">{row_cells}</table:table-row>"
            ),
        ]
    );
}

#[test]
fn edit_keeps_style_and_validation() {
    let bytes = fixture("in.ods");

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[
                (1, 1, CellEdit::Number(1234.25)),
                (1, 4, CellEdit::Text("yes".to_string())),
                (2, 1, CellEdit::Clear),
            ],
        ),
    )
    .unwrap();

    let data = rows(&content(&patched), "Data");
    assert!(data[1].contains(
        "<table:table-cell table:style-name=\"ce3\" office:value-type=\"float\" office:value=\"1234.25\"><text:p>1234.25</text:p></table:table-cell>"
    ));
    assert!(data[1].contains(
        "<table:table-cell table:content-validation-name=\"val1\" office:value-type=\"string\"><text:p>yes</text:p></table:table-cell>"
    ));
    assert!(
        data[2].contains("<text:p>item2</text:p></table:table-cell><table:table-cell table:style-name=\"ce3\"/><table:table-cell table:style-name=\"ce5\""),
        "a cleared cell keeps its style and nothing else: {}",
        data[2]
    );

    let original = rows(&content(&bytes), "Data");
    assert_eq!(
        data[8..],
        original[8..],
        "rows without edits or formulas are unchanged"
    );
}

#[test]
fn append_splits_trailing_filler_row() {
    let bytes = hand_ods(&table(
        "Data",
        "<table:table-row><table:table-cell office:value-type=\"string\"><text:p>Name</text:p></table:table-cell>\
         <table:table-cell table:number-columns-repeated=\"1023\"/></table:table-row>\
         <table:table-row><table:table-cell office:value-type=\"float\" office:value=\"1\"><text:p>1</text:p></table:table-cell>\
         <table:table-cell table:number-columns-repeated=\"1023\"/></table:table-row>\
         <table:table-row table:style-name=\"ro1\" table:number-rows-repeated=\"1048574\">\
         <table:table-cell table:number-columns-repeated=\"1024\"/></table:table-row>",
    ));

    let mut workbook = open_bytes(bytes.clone()).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    assert_eq!(grid.append_row(), Some(2));
    assert_eq!(grid.row_count(), 2);

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[(2, 0, CellEdit::Number(2.0)), (3, 0, CellEdit::Number(3.0))],
        ),
    )
    .unwrap();

    let data = rows(&content(&patched), "Data");
    assert_eq!(data.len(), 5);
    assert_eq!(
        data[2],
        "<table:table-row table:style-name=\"ro1\">\
         <table:table-cell office:value-type=\"float\" office:value=\"2\"><text:p>2</text:p></table:table-cell>\
         <table:table-cell table:number-columns-repeated=\"1023\"/></table:table-row>"
    );
    assert_eq!(
        data[4],
        "<table:table-row table:style-name=\"ro1\" table:number-rows-repeated=\"1048572\">\
         <table:table-cell table:number-columns-repeated=\"1024\"/></table:table-row>",
        "the filler row shrinks and stays last"
    );

    let mut workbook = open_bytes(patched).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    assert_eq!(grid.cell(3, 0).unwrap().value, CellValue::Number(3.0));
    assert_eq!(grid.append_row(), Some(4));
}

#[test]
fn append_row_skips_merged_rows() {
    let bytes = hand_ods(&table(
        "Data",
        "<table:table-row><table:table-cell office:value-type=\"string\"><text:p>Name</text:p></table:table-cell>\
         <table:table-cell office:value-type=\"string\" table:number-columns-spanned=\"2\" table:number-rows-spanned=\"4\"><text:p>merged</text:p></table:table-cell>\
         <table:covered-table-cell/></table:table-row>\
         <table:table-row table:number-rows-repeated=\"10\"><table:table-cell table:number-columns-repeated=\"3\"/></table:table-row>",
    ));

    let mut workbook = open_bytes(bytes.clone()).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    assert_eq!(grid.append_row(), Some(4));
    assert_eq!(grid.row_count(), 4, "the grid is padded to the append row");

    let fixture_grid = open_bytes(fixture("in.ods"))
        .unwrap()
        .read_sheet(0)
        .unwrap();
    assert_eq!(
        fixture_grid.append_row(),
        Some(20),
        "a cell holding only a picture still counts as content"
    );
}

#[test]
fn appends_rows_past_the_last_row_element() {
    let bytes = fixture("in.ods");

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[
                (22, 0, CellEdit::Text("appended".to_string())),
                (22, 2, CellEdit::Number(5.0)),
            ],
        ),
    )
    .unwrap();

    let data = rows(&content(&patched), "Data");
    assert_eq!(data.len(), 17);
    assert_eq!(
        data[15],
        "<table:table-row table:number-rows-repeated=\"2\"><table:table-cell/></table:table-row>"
    );
    assert_eq!(
        data[16],
        "<table:table-row>\
         <table:table-cell office:value-type=\"string\"><text:p>appended</text:p></table:table-cell>\
         <table:table-cell/>\
         <table:table-cell office:value-type=\"float\" office:value=\"5\"><text:p>5</text:p></table:table-cell>\
         </table:table-row>"
    );
    assert!(
        content(&patched).contains("</table:table-row><calcext:conditional-formats>"),
        "new rows go after the last row, before the elements that follow the rows"
    );

    let mut workbook = open_bytes(patched).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    assert_eq!(
        grid.cell(22, 0).unwrap().value,
        CellValue::Text("appended".into())
    );
}

#[test]
fn mimetype_first_and_stored() {
    let bytes = fixture("in.ods");

    let patched = patch(&bytes, &edits(0, &[(1, 1, CellEdit::Number(1.0))])).unwrap();

    let mut archive = zip::ZipArchive::new(Cursor::new(patched)).unwrap();
    let mut mimetype = archive.by_index(0).unwrap();
    assert_eq!(mimetype.name(), "mimetype");
    assert_eq!(mimetype.compression(), CompressionMethod::Stored);
    assert!(mimetype.extra_data().is_none_or(<[u8]>::is_empty));

    let mut text = String::new();
    mimetype.read_to_string(&mut text).unwrap();
    assert_eq!(text, ODS_MIMETYPE);
}

#[test]
fn refuses_covered_cell() {
    let bytes = fixture("in.ods");

    let result = patch(
        &bytes,
        &edits(0, &[(4, 6, CellEdit::Text("inside".to_string()))]),
    );
    assert!(
        matches!(
            &result,
            Err(SheetWriteError::CoveredCell { sheet, cell }) if sheet == "Data" && cell == "G5"
        ),
        "{result:?}"
    );

    let result = patch(&bytes, &edits(0, &[(5, 7, CellEdit::Clear)]));
    assert!(
        matches!(&result, Err(SheetWriteError::CoveredCell { cell, .. }) if cell == "H6"),
        "{result:?}"
    );

    patch(
        &bytes,
        &edits(0, &[(4, 5, CellEdit::Text("origin".to_string()))]),
    )
    .expect("the first cell of a merged range can be edited");
}

#[test]
fn refuses_values_a_cell_cannot_store() {
    let bytes = fixture("in.ods");

    let cases = [
        (CellEdit::Number(f64::NAN), "NonFiniteNumber"),
        (CellEdit::Number(f64::INFINITY), "NonFiniteNumber"),
        (CellEdit::Text("bell \u{7}".to_string()), "InvalidCharacter"),
        (CellEdit::Formula("\u{1}".to_string()), "InvalidCharacter"),
        (
            CellEdit::Date(
                NaiveDate::from_ymd_opt(10_000, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            "DateOutOfRange",
        ),
    ];

    for (edit, expected) in cases {
        let result = patch(&bytes, &edits(0, &[(1, 1, edit.clone())]));
        let matched = match &result {
            Err(SheetWriteError::NonFiniteNumber { cell, .. }) => {
                expected == "NonFiniteNumber" && cell == "B2"
            }
            Err(SheetWriteError::InvalidCharacter { cell, .. }) => {
                expected == "InvalidCharacter" && cell == "B2"
            }
            Err(SheetWriteError::DateOutOfRange { cell, .. }) => {
                expected == "DateOutOfRange" && cell == "B2"
            }
            _ => false,
        };
        assert!(matched, "{edit:?}: {result:?}");
    }

    let too_long = "\u{1F600}".repeat(16_384);
    match patch(&bytes, &edits(0, &[(1, 1, CellEdit::Text(too_long))])) {
        Err(SheetWriteError::TextTooLong { cell, length, .. }) => {
            assert_eq!((cell.as_str(), length), ("B2", 32_768));
        }
        other => panic!("expected a text-too-long refusal, got {other:?}"),
    }
    patch(
        &bytes,
        &edits(0, &[(1, 1, CellEdit::Text("x".repeat(32_767)))]),
    )
    .unwrap();

    let result = patch(&bytes, &edits(0, &[(1_048_576, 0, CellEdit::Number(1.0))]));
    assert!(
        matches!(
            result,
            Err(SheetWriteError::CellOutOfRange {
                row: 1_048_576,
                column: 0,
                ..
            })
        ),
        "{result:?}"
    );
    let result = patch(&bytes, &edits(0, &[(0, 16_384, CellEdit::Number(1.0))]));
    assert!(
        matches!(result, Err(SheetWriteError::CellOutOfRange { .. })),
        "{result:?}"
    );
    let result = patch(&bytes, &edits(5, &[(0, 0, CellEdit::Number(1.0))]));
    assert!(
        matches!(
            result,
            Err(SheetWriteError::SheetOutOfRange {
                index: 5,
                sheet_count: 2
            })
        ),
        "{result:?}"
    );
}

#[test]
fn refuses_cell_in_matrix_formula() {
    let bytes = hand_ods(&table(
        "Data",
        "<table:table-row><table:table-cell table:formula=\"of:=TRANSPOSE([.D1:.E1])\" \
         table:number-matrix-columns-spanned=\"1\" table:number-matrix-rows-spanned=\"2\" \
         office:value-type=\"float\" office:value=\"1\"><text:p>1</text:p></table:table-cell>\
         <table:table-cell table:number-columns-repeated=\"1023\"/></table:table-row>\
         <table:table-row><table:table-cell office:value-type=\"float\" office:value=\"2\"><text:p>2</text:p></table:table-cell>\
         <table:table-cell table:number-columns-repeated=\"1023\"/></table:table-row>",
    ));

    let result = patch(&bytes, &edits(0, &[(1, 0, CellEdit::Number(9.0))]));

    assert!(
        matches!(
            &result,
            Err(SheetWriteError::InsideFormulaRange { cell, range, kind: FormulaRangeKind::Array, .. })
                if cell == "A2" && range == "A1:A2"
        ),
        "{result:?}"
    );
}

#[test]
fn untouched_entries_byte_identical() {
    let bytes = fixture("in.ods");

    let unchanged = patch(&bytes, &SheetEdits::new()).unwrap();
    assert_entries_unchanged(&bytes, &unchanged, &[]);

    let patched = patch(&bytes, &edits(1, &[(1, 1, CellEdit::Number(6.0))])).unwrap();
    assert_entries_unchanged(&bytes, &patched, &["content.xml"]);

    let original = content(&bytes);
    let rewritten = content(&patched);
    let body = |xml: &str| xml[..xml.find("<table:table table:name=\"Data\"").unwrap()].to_string();
    assert_eq!(
        body(&rewritten),
        body(&original),
        "everything before the sheets is unchanged"
    );
    assert!(rewritten.ends_with(
        "<table:named-expressions><table:named-range table:name=\"Amounts\" table:base-cell-address=\"$Data.$A$1\" table:cell-range-address=\"$Data.$B$2:.$B$6\"/></table:named-expressions><table:database-ranges><table:database-range table:name=\"Table1\" table:target-range-address=\"Data.A12:Data.C15\" table:display-filter-buttons=\"true\"/></table:database-ranges></office:spreadsheet></office:body></office:document-content>"
    ));
}

#[test]
fn reopens_with_calamine_showing_edits() {
    let bytes = fixture("in.ods");
    let day = NaiveDate::from_ymd_opt(2024, 2, 29)
        .unwrap()
        .and_hms_opt(13, 30, 0)
        .unwrap();

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[
                (
                    0,
                    0,
                    CellEdit::Text("Renamed  twice\nsecond line".to_string()),
                ),
                (1, 1, CellEdit::Number(0.1)),
                (1, 2, CellEdit::Date(day)),
                (1, 3, CellEdit::Formula("[.B2]*3".to_string())),
                (2, 3, CellEdit::Formula("of:=[.B3]*4".to_string())),
                (2, 4, CellEdit::Bool(false)),
                (20, 0, CellEdit::Text("appended".to_string())),
            ],
        ),
    )
    .unwrap();

    let mut workbook = open_bytes(patched).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    let value = |row, column| grid.cell(row, column).unwrap().value.clone();

    assert_eq!(
        value(0, 0),
        CellValue::Text("Renamed  twice\nsecond line".into())
    );
    assert_eq!(value(1, 1), CellValue::Number(0.1));
    assert_eq!(value(1, 2), CellValue::Date(day));
    assert_eq!(
        grid.cell(1, 3).unwrap().formula,
        CellFormula::Text("of:=[.B2]*3".into())
    );
    assert_eq!(
        grid.cell(2, 3).unwrap().formula,
        CellFormula::Text("of:=[.B3]*4".into())
    );
    assert_eq!(value(2, 4), CellValue::Bool(false));
    assert_eq!(value(20, 0), CellValue::Text("appended".into()));
    assert_eq!(value(3, 0), CellValue::Text("item3".into()));
    assert_eq!(grid.append_row(), Some(21));
}

#[test]
fn formula_written_without_cached_value() {
    let bytes = fixture("in.ods");

    let patched = patch(
        &bytes,
        &edits(0, &[(1, 3, CellEdit::Formula("[.B2]*3".to_string()))]),
    )
    .unwrap();

    let xml = content(&patched);
    let data = rows(&xml, "Data");
    assert!(
        data[1].contains("<table:table-cell table:formula=\"of:=[.B2]*3\"/>"),
        "{}",
        data[1]
    );

    // LibreOffice keeps the cached result of every formula when it opens an
    // ods file, so the cached results an edit may have made stale are
    // dropped, on every sheet, and LibreOffice computes them on load.
    assert!(data[2].contains("<table:table-cell table:formula=\"of:=[.B3]*2\"/>"));
    assert!(data[7].contains(
        "<table:table-cell table:style-name=\"ce4\" table:formula=\"of:=SUM([.B2:.B6])\"/>"
    ));
    assert!(
        rows(&xml, "Other")[0].contains("<table:table-cell table:formula=\"of:=[$Data.B5]*10\"/>")
    );

    let mut workbook = open_bytes(patched).unwrap();
    let grid = workbook.read_sheet(0).unwrap();
    assert_eq!(grid.cell(1, 3).unwrap().value, CellValue::Empty);
    assert_eq!(grid.cell(1, 1).unwrap().value, CellValue::Number(50.5));
}

#[test]
fn formula_with_leading_equals_is_written_once() {
    let bytes = fixture("in.ods");

    let patched = patch(
        &bytes,
        &edits(0, &[(1, 3, CellEdit::Formula("=[.B2]*3".to_string()))]),
    )
    .unwrap();

    let data = rows(&content(&patched), "Data");
    assert!(
        data[1].contains("<table:table-cell table:formula=\"of:=[.B2]*3\"/>"),
        "{}",
        data[1]
    );
}

#[test]
fn annotation_kept_on_edited_cell() {
    let bytes = fixture("in.ods");
    let original = rows(&content(&bytes), "Data");
    let annotation_start = original[4].find("<office:annotation").unwrap();
    let annotation_end =
        original[4].find("</office:annotation>").unwrap() + "</office:annotation>".len();
    let annotation = &original[4][annotation_start..annotation_end];

    let frame_start = original[1].find("<draw:frame").unwrap();
    let frame_end = original[1].find("</draw:frame>").unwrap() + "</draw:frame>".len();
    let frame = &original[1][frame_start..frame_end];

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[
                (4, 0, CellEdit::Text("edited".to_string())),
                (1, 9, CellEdit::Number(3.0)),
            ],
        ),
    )
    .unwrap();

    let data = rows(&content(&patched), "Data");
    assert!(data[4].starts_with(&format!(
        "<table:table-row table:style-name=\"ro1\"><table:table-cell office:value-type=\"string\">{annotation}<text:p>edited</text:p></table:table-cell>"
    )));
    assert!(data[1].contains(&format!(
        "<table:table-cell table:style-name=\"Default\" office:value-type=\"float\" office:value=\"3\">{frame}<text:p>3</text:p></table:table-cell>"
    )));
}

#[test]
fn text_keeps_spaces_tabs_and_lines() {
    let bytes = hand_ods(&table(
        "Data",
        "<table:table-row><table:table-cell table:number-columns-repeated=\"1024\"/></table:table-row>",
    ));

    let patched = patch(
        &bytes,
        &edits(
            0,
            &[(0, 0, CellEdit::Text(" a  b\tc<&>\r\nnext ".to_string()))],
        ),
    )
    .unwrap();

    assert!(content(&patched).contains(
        "<table:table-cell office:value-type=\"string\">\
         <text:p><text:s/>a <text:s/>b<text:tab/>c&lt;&amp;&gt;</text:p>\
         <text:p>next<text:s/></text:p></table:table-cell>"
    ));
}
