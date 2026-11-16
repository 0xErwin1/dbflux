use dbflux_core::chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use rust_xlsxwriter::{ExcelDateTime, Format, Formula, Workbook, Worksheet, XlsxError};

use super::*;
use crate::file_source::FileSource;
use crate::pipeline::RowSource;

/// Builds a workbook in the scratch directory.
fn workbook(
    scratch: &Scratch,
    name: &str,
    build: impl FnOnce(&mut Workbook) -> Result<(), XlsxError>,
) -> PathBuf {
    let mut book = Workbook::new();
    build(&mut book).expect("build the workbook");
    let path = scratch.0.join(name);
    book.save(&path).expect("save the workbook");
    path
}

fn sheet<'a>(book: &'a mut Workbook, name: &str) -> Result<&'a mut Worksheet, XlsxError> {
    let sheet = book.add_worksheet();
    sheet.set_name(name)?;
    Ok(sheet)
}

fn header(sheet: &mut Worksheet, names: &[&str]) -> Result<(), XlsxError> {
    for (column, name) in (0u16..).zip(names) {
        sheet.write_string(0, column, *name)?;
    }
    Ok(())
}

fn inspect_xlsx(path: &Path, sheet: Option<&str>) -> Result<ImportFile, TransferError> {
    ImportFile::inspect_sheet(
        path,
        FileFormat::Spreadsheet,
        FormatOptions::default(),
        sheet,
    )
}

fn read_values(file: &ImportFile) -> Vec<Vec<Value>> {
    let mut source = FileSource::open_sheet(
        file.path(),
        file.format(),
        file.columns().to_vec(),
        100,
        None,
        file.options().clone(),
        file.sheet(),
    )
    .expect("open the sheet");
    let mut rows = Vec::new();
    while let Some(chunk) = source.next_chunk(&CancelToken::new()).expect("read") {
        rows.extend(chunk.0);
    }
    rows
}

fn typed_book(scratch: &Scratch) -> PathBuf {
    workbook(scratch, "facts.xlsx", |book| {
        let date = Format::new().set_num_format("yyyy-mm-dd");
        let date_time = Format::new().set_num_format("yyyy-mm-dd hh:mm:ss");
        let sheet = sheet(book, "Facts")?;
        header(
            sheet,
            &["id", "ratio", "label", "done", "born", "seen", "note"],
        )?;
        sheet.write_number(1, 0, 3.0)?;
        sheet.write_number(1, 1, 2.5)?;
        sheet.write_string(1, 2, "hello")?;
        sheet.write_boolean(1, 3, true)?;
        sheet.write_datetime_with_format(1, 4, ExcelDateTime::from_ymd(2024, 1, 31)?, &date)?;
        sheet.write_datetime_with_format(
            1,
            5,
            ExcelDateTime::from_ymd(2024, 3, 5)?.and_hms(13, 45, 30)?,
            &date_time,
        )?;
        // `note` stays empty.
        sheet.write_number(2, 0, 1e20)?;
        sheet.write_number(2, 1, 7.0)?;
        sheet.write_string(2, 2, "")?;
        sheet.write_boolean(2, 3, false)?;
        Ok(())
    })
}

#[test]
fn the_header_row_names_the_columns_of_the_first_sheet() {
    let scratch = Scratch::new();
    let path = typed_book(&scratch);

    let file = inspect_xlsx(&path, None).expect("inspect");

    assert_eq!(
        names(&file),
        ["id", "ratio", "label", "done", "born", "seen", "note"]
    );
    assert!(file.columns().iter().all(|c| c.type_name.is_none()));
    assert_eq!(file.table_name(), "facts");
    assert_eq!(file.manifest_table().format, "xlsx");
    assert_eq!(file.sheet(), None);
}

#[test]
fn cells_map_to_typed_values() {
    let scratch = Scratch::new();
    let path = typed_book(&scratch);
    let file = inspect_xlsx(&path, None).expect("inspect");

    let values = read_values(&file);

    let born = NaiveDate::from_ymd_opt(2024, 1, 31).expect("date");
    let seen = Utc
        .with_ymd_and_hms(2024, 3, 5, 13, 45, 30)
        .single()
        .expect("timestamp");
    assert_eq!(
        values,
        vec![
            vec![
                Value::Int(3),
                Value::Float(2.5),
                text("hello"),
                Value::Bool(true),
                Value::Date(born),
                Value::DateTime(seen),
                Value::Null,
            ],
            vec![
                Value::Float(1e20),
                Value::Int(7),
                Value::Null,
                Value::Bool(false),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
        ]
    );
}

#[test]
fn a_time_of_day_cell_maps_to_a_time() {
    let scratch = Scratch::new();
    let path = workbook(&scratch, "times.xlsx", |book| {
        let time = Format::new().set_num_format("hh:mm:ss");
        let sheet = sheet(book, "Times")?;
        header(sheet, &["at"])?;
        sheet.write_datetime_with_format(1, 0, ExcelDateTime::from_hms(13, 45, 30)?, &time)?;
        Ok(())
    });
    let file = inspect_xlsx(&path, None).expect("inspect");

    assert_eq!(
        read_values(&file),
        vec![vec![Value::Time(
            NaiveTime::from_hms_opt(13, 45, 30).expect("time")
        )]]
    );
}

#[test]
fn the_named_sheet_is_the_one_imported() {
    let scratch = Scratch::new();
    let path = workbook(&scratch, "book.xlsx", |book| {
        let first = sheet(book, "First")?;
        header(first, &["a"])?;
        first.write_number(1, 0, 1.0)?;
        let second = sheet(book, "Second")?;
        header(second, &["x", "y"])?;
        second.write_string(1, 0, "left")?;
        second.write_string(1, 1, "right")?;
        Ok(())
    });

    let default = inspect_xlsx(&path, None).expect("default sheet");
    let second = inspect_xlsx(&path, Some("Second")).expect("named sheet");

    assert_eq!(names(&default), ["a"]);
    assert_eq!(names(&second), ["x", "y"]);
    assert_eq!(second.sheet(), Some("Second"));
    assert_eq!(
        read_values(&second),
        vec![vec![text("left"), text("right")]]
    );
    assert_eq!(read_values(&default), vec![vec![Value::Int(1)]]);
}

#[test]
fn an_unknown_sheet_is_rejected_listing_the_sheets_the_workbook_has() {
    let scratch = Scratch::new();
    let path = workbook(&scratch, "book.xlsx", |book| {
        header(sheet(book, "First")?, &["a"])?;
        header(sheet(book, "Second")?, &["b"])?;
        Ok(())
    });

    let message = source_error(inspect_xlsx(&path, Some("Third")));

    assert!(message.contains("Third"), "{message}");
    assert!(
        message.contains("First") && message.contains("Second"),
        "{message}"
    );
}

#[test]
fn list_sheets_returns_the_worksheets_in_order_with_their_visibility() {
    let scratch = Scratch::new();
    let path = workbook(&scratch, "book.xlsx", |book| {
        sheet(book, "Data")?;
        sheet(book, "Other")?;
        sheet(book, "Hidden")?.set_hidden(true);
        Ok(())
    });

    let sheets = list_sheets(&path).expect("list");

    assert_eq!(
        sheets,
        vec![
            ImportSheet {
                name: "Data".to_string(),
                visible: true
            },
            ImportSheet {
                name: "Other".to_string(),
                visible: true
            },
            ImportSheet {
                name: "Hidden".to_string(),
                visible: false
            },
        ]
    );
}

#[test]
fn list_sheets_rejects_a_file_that_is_not_a_workbook() {
    let scratch = Scratch::new();
    let path = scratch.write("fake.xlsx", b"id,name\n1,Ada\n");

    let Err(TransferError::Source(message)) = list_sheets(&path) else {
        panic!("a CSV named .xlsx is not a workbook");
    };

    assert!(message.contains("fake.xlsx"), "{message}");
    assert!(message.contains("not a spreadsheet"), "{message}");
}

#[test]
fn an_empty_sheet_is_rejected() {
    let scratch = Scratch::new();
    let path = workbook(&scratch, "empty.xlsx", |book| {
        sheet(book, "Empty")?;
        Ok(())
    });

    let message = source_error(inspect_xlsx(&path, None));

    assert!(message.contains("sheet 'Empty' is empty"), "{message}");
}

#[test]
fn a_blank_or_repeated_header_name_is_rejected() {
    let scratch = Scratch::new();
    let blank = workbook(&scratch, "blank.xlsx", |book| {
        let data = sheet(book, "Data")?;
        data.write_string(0, 0, "id")?;
        data.write_string(0, 2, "name")?;
        data.write_string(1, 0, "1")?;
        Ok(())
    });
    let repeated = workbook(&scratch, "repeated.xlsx", |book| {
        header(sheet(book, "Data")?, &["id", "name", "id"])?;
        Ok(())
    });

    let blank_message = source_error(inspect_xlsx(&blank, None));
    let repeated_message = source_error(inspect_xlsx(&repeated, None));

    assert!(
        blank_message.contains("header column 2 has no name"),
        "{blank_message}"
    );
    assert!(
        repeated_message.contains("header column 'id' appears more than once"),
        "{repeated_message}"
    );
}

fn error_book(scratch: &Scratch) -> PathBuf {
    workbook(scratch, "errors.xlsx", |book| {
        let data = sheet(book, "Data")?;
        header(data, &["id", "ratio"])?;
        data.write_number(1, 0, 1.0)?;
        data.write_number(1, 1, 2.0)?;
        data.write_number(2, 0, 2.0)?;
        data.write_formula(2, 1, Formula::new("=1/0").set_result("#DIV/0!"))?;
        Ok(())
    })
}

#[test]
fn an_error_cell_that_appears_after_the_inspection_still_rejects_the_import() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, "CREATE TABLE errors (id INTEGER, ratio REAL)");
    run(&connection, "INSERT INTO errors (id) VALUES (100)");
    let path = workbook(&scratch, "errors.xlsx", |book| {
        let data = sheet(book, "Data")?;
        header(data, &["id", "ratio"])?;
        data.write_number(1, 0, 1.0)?;
        data.write_number(1, 1, 2.0)?;
        Ok(())
    });
    let file = inspect_xlsx(&path, None).expect("the sheet is fine when inspected");
    let replaced = error_book(&scratch);
    assert_eq!(replaced, path, "the second workbook replaces the first");

    let message = rejection(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "errors"),
    );

    assert!(message.contains("sheet 'Data'"), "{message}");
    assert!(message.contains("row 3"), "{message}");
    assert!(message.contains("column 'ratio'"), "{message}");
    assert!(message.contains("#DIV/0!"), "{message}");
    assert_eq!(
        rows(&connection, "SELECT id FROM errors"),
        vec![vec![Value::Int(100)]],
        "nothing was written"
    );
}

fn checked(path: &Path, sheet: Option<&str>) -> Result<ImportFile, InspectError> {
    ImportFile::inspect_checked(
        path,
        FileFormat::Spreadsheet,
        FormatOptions::default(),
        sheet,
    )
}

fn issue(result: Result<ImportFile, InspectError>) -> SheetIssue {
    match result {
        Err(InspectError::Sheet(issue)) => issue,
        other => panic!("expected a sheet issue, got {other:?}"),
    }
}

#[test]
fn an_empty_sheet_is_reported_as_an_empty_issue() {
    let scratch = Scratch::new();
    let path = workbook(&scratch, "empty.xlsx", |book| {
        sheet(book, "Hoja4")?;
        Ok(())
    });

    assert_eq!(
        issue(checked(&path, None)),
        SheetIssue::Empty {
            sheet: "Hoja4".to_string()
        }
    );
}

#[test]
fn a_blank_and_a_repeated_header_are_reported_with_every_column() {
    let scratch = Scratch::new();
    let path = workbook(&scratch, "header.xlsx", |book| {
        let data = sheet(book, "Ventas Q3")?;
        data.write_string(0, 0, "id")?;
        data.write_string(0, 1, "total")?;
        data.write_string(0, 3, "total")?;
        data.write_string(0, 4, "total")?;
        data.write_string(1, 0, "1")?;
        Ok(())
    });

    assert_eq!(
        issue(checked(&path, None)),
        SheetIssue::InvalidHeader {
            sheet: "Ventas Q3".to_string(),
            blank_columns: vec![2],
            duplicate_names: vec![DuplicateHeader {
                name: "total".to_string(),
                columns: vec![1, 3, 4],
            }],
        }
    );
}

#[test]
fn an_error_cell_is_reported_when_the_sheet_is_inspected() {
    let scratch = Scratch::new();
    let path = error_book(&scratch);

    assert_eq!(
        issue(checked(&path, None)),
        SheetIssue::ErrorCell {
            sheet: "Data".to_string(),
            row: 3,
            column: 1,
            column_name: "ratio".to_string(),
            code: "#DIV/0!",
        }
    );
}

#[test]
fn a_sheet_over_the_cell_limit_is_reported_with_its_size() {
    let scratch = Scratch::new();
    let path = workbook(&scratch, "big.xlsx", |book| {
        let data = sheet(book, "Registro")?;
        header(data, &["a"])?;
        data.write_string(999_999, 15, "far")?;
        Ok(())
    });

    assert_eq!(
        issue(checked(&path, None)),
        SheetIssue::TooLarge {
            sheet: "Registro".to_string(),
            cells: 16_000_000,
            limit: 10_000_000,
        }
    );
}

#[test]
fn a_sheet_issue_reads_as_a_transfer_error_with_the_same_wording_as_before() {
    let scratch = Scratch::new();
    let path = workbook(&scratch, "empty.xlsx", |book| {
        sheet(book, "Empty")?;
        Ok(())
    });

    let message = source_error(inspect_xlsx(&path, None));

    assert!(message.contains("empty.xlsx"), "{message}");
    assert!(message.contains("sheet 'Empty' is empty"), "{message}");
}

#[test]
fn the_default_sheet_is_the_first_visible_worksheet() {
    let scratch = Scratch::new();
    let path = workbook(&scratch, "hidden_first.xlsx", |book| {
        header(sheet(book, "Secret")?, &["hidden"])?;
        sheet(book, "Public")?.set_active(true);
        book.worksheet_from_name("Secret")?.set_hidden(true);
        header(book.worksheet_from_name("Public")?, &["shown"])?;
        Ok(())
    });

    let file = inspect_xlsx(&path, None).expect("default sheet");

    assert_eq!(names(&file), ["shown"]);
}

#[test]
fn a_workbook_loads_into_an_existing_table_decoded_by_its_column_types() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(
        &connection,
        "CREATE TABLE facts (id INTEGER, ratio REAL, label TEXT, done BOOLEAN, \
         born DATE, seen TIMESTAMP, note TEXT)",
    );
    let path = typed_book(&scratch);
    let file = inspect_xlsx(&path, None).expect("inspect");

    let outcome = import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "facts"),
        1,
    );

    assert_eq!(
        outcome.tables[0].status,
        TableTransferStatus::Completed { rows: 2 }
    );
    assert_eq!(
        rows(
            &connection,
            "SELECT id, ratio, label, done, born, note, typeof(id), typeof(ratio) \
             FROM facts ORDER BY rowid LIMIT 1"
        ),
        vec![vec![
            Value::Int(3),
            Value::Float(2.5),
            text("hello"),
            Value::Int(1),
            text("2024-01-31"),
            Value::Null,
            text("integer"),
            text("real"),
        ]]
    );
    assert_eq!(
        rows(
            &connection,
            "SELECT ratio, typeof(ratio) FROM facts ORDER BY rowid DESC LIMIT 1"
        ),
        vec![vec![Value::Float(7.0), text("real")]]
    );
}

#[test]
fn a_workbook_loads_into_a_new_table_as_text_columns() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    let path = workbook(&scratch, "orders.xlsx", |book| {
        let data = sheet(book, "Orders")?;
        header(data, &["code", "qty"])?;
        data.write_string(1, 0, "A-1")?;
        data.write_number(1, 1, 4.0)?;
        data.write_string(2, 0, "B-2")?;
        Ok(())
    });
    let file = inspect_xlsx(&path, None).expect("inspect");

    import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Create, "orders"),
        500,
    );

    assert_eq!(
        rows(
            &connection,
            "SELECT name, type FROM pragma_table_info('orders') ORDER BY cid"
        ),
        vec![
            vec![text("code"), text("TEXT")],
            vec![text("qty"), text("TEXT")]
        ]
    );
    assert_eq!(
        rows(&connection, "SELECT code, qty FROM orders ORDER BY rowid"),
        vec![vec![text("A-1"), text("4")], vec![text("B-2"), Value::Null]]
    );
}

#[test]
fn an_ods_and_an_xls_workbook_are_read_through_the_same_reader() {
    let fixtures =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../dbflux_spreadsheet/tests/fixtures");

    for name in ["in.ods", "in.xls"] {
        let path = fixtures.join(name);
        let sheets = list_sheets(&path).expect("list");
        assert_eq!(
            sheets.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
            ["Data", "Other"],
            "{name}"
        );
        assert_eq!(
            format_for_path(&path),
            Some(FileFormat::Spreadsheet),
            "{name}"
        );
        // The fixtures carry other content to the right of the table, so
        // the first row has a column without a name.
        let message = source_error(inspect_xlsx(&path, None));
        assert!(
            message.contains("header column 6 has no name"),
            "{name}: {message}"
        );
    }
}

#[test]
fn rows_past_the_data_that_only_pad_the_grid_are_not_imported() {
    let scratch = Scratch::new();
    let path = workbook(&scratch, "merged.xlsx", |book| {
        let data = sheet(book, "Data")?;
        header(data, &["a", "b"])?;
        data.write_string(1, 0, "x")?;
        data.write_string(3, 0, "after a gap")?;
        data.merge_range(10, 0, 99, 1, "", &Format::new())?;
        Ok(())
    });
    let file = inspect_xlsx(&path, None).expect("inspect");

    let values = read_values(&file);

    assert_eq!(
        values,
        vec![
            vec![text("x"), Value::Null],
            vec![Value::Null, Value::Null],
            vec![text("after a gap"), Value::Null],
        ],
        "interior empty rows stay, trailing padding does not"
    );
}
