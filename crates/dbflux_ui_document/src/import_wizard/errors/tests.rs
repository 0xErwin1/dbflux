use dbflux_transfer::import_file::{DuplicateHeader, SheetIssue};
use dbflux_transfer::{FileFormat, TransferError};

use super::sheet::column_letters;
use super::{ErrorBlock, FileLoadError};

fn read_failure() -> FileLoadError {
    FileLoadError::Read(TransferError::Source("invalid JSON".to_string()))
}

#[test]
fn a_csv_read_failure_points_at_the_delimiter_and_encoding() {
    let block = ErrorBlock::file_load_failure(&read_failure(), FileFormat::Csv);

    assert_eq!(
        block.message,
        "Cannot read the file: source error: invalid JSON"
    );
    assert_eq!(
        block.next_step,
        "Choose another encoding or delimiter above: the file is read again as soon as you pick one."
    );
}

#[test]
fn a_json_read_failure_says_to_fix_or_replace_the_file() {
    for format in [FileFormat::Json, FileFormat::JsonLines] {
        let block = ErrorBlock::file_load_failure(&read_failure(), format);

        assert_eq!(
            block.next_step,
            "Fix the JSON in the file and choose it again, or choose another file."
        );
    }
}

#[test]
fn an_xml_read_failure_says_xml_and_not_json() {
    let block = ErrorBlock::file_load_failure(&read_failure(), FileFormat::Xml);

    assert_eq!(
        block.next_step,
        "Fix the XML in the file and choose it again, or choose another file."
    );
}

/// The lookup failure names the table lookup instead of claiming the file
/// could not be read, and says to press Use Table again.
#[test]
fn a_failed_table_lookup_names_the_lookup_and_not_the_file() {
    let failure = FileLoadError::Lookup("Permission denied".to_string());

    let block = ErrorBlock::file_load_failure(&failure, FileFormat::Csv);

    assert_eq!(
        block.message,
        "Could not look up the target table: Permission denied"
    );
    assert!(!block.message.contains("Cannot read the file"));
    assert_eq!(
        block.next_step,
        "Check the table name and your access to the connection, then press Use Table again."
    );
}

#[test]
fn an_unsupported_extension_names_what_to_pick() {
    let block = ErrorBlock::unsupported_file(false);

    assert_eq!(
        block.message,
        "Only .csv, .json, .jsonl, .xml, .xlsx, .xlsm, .xls and .ods files can be imported."
    );
    assert_eq!(
        block.next_step,
        "Choose a file with one of those extensions."
    );
}

#[test]
fn an_unsupported_extension_on_a_sql_connection_also_lists_sql() {
    let block = ErrorBlock::unsupported_file(true);

    assert_eq!(
        block.message,
        "Only .csv, .json, .jsonl, .xml, .xlsx, .xlsm, .xls, .ods and .sql files can be imported."
    );
    assert_eq!(
        block.next_step,
        "Choose a file with one of those extensions."
    );
}

#[test]
fn every_folder_error_ends_with_a_next_step() {
    for block in [
        ErrorBlock::no_connection(),
        ErrorBlock::no_dialog(),
        ErrorBlock::invalid_bundle("manifest.json is missing"),
    ] {
        assert!(!block.next_step.is_empty(), "{block:?}");
        assert!(!block.next_step.starts_with("document."), "{block:?}");
    }
}

#[test]
fn columns_are_lettered_like_a_spreadsheet_app() {
    for (index, letters) in [
        (0, "A"),
        (2, "C"),
        (25, "Z"),
        (26, "AA"),
        (27, "AB"),
        (51, "AZ"),
        (52, "BA"),
        (701, "ZZ"),
        (702, "AAA"),
    ] {
        assert_eq!(column_letters(index), letters, "{index}");
    }
}

fn header_issue(blank_columns: Vec<usize>, duplicate_names: Vec<DuplicateHeader>) -> SheetIssue {
    SheetIssue::InvalidHeader {
        sheet: "Ventas Q3".to_string(),
        blank_columns,
        duplicate_names,
    }
}

fn repeated(name: &str, columns: Vec<usize>) -> DuplicateHeader {
    DuplicateHeader {
        name: name.to_string(),
        columns,
    }
}

#[test]
fn a_header_problem_lists_columns_by_letter_and_says_which_name_repeats() {
    for (issue, problems) in [
        (header_issue(vec![2], vec![]), "column C is blank"),
        (
            header_issue(vec![1, 3], vec![]),
            "columns B and D are blank",
        ),
        (
            header_issue(vec![], vec![repeated("total", vec![3, 5])]),
            "columns D and F are both named “total”",
        ),
        (
            header_issue(vec![], vec![repeated("total", vec![1, 3, 5])]),
            "columns B, D and F are all named “total”",
        ),
        (
            header_issue(vec![2], vec![repeated("total", vec![3, 5])]),
            "column C is blank and columns D and F are both named “total”",
        ),
        (
            header_issue(vec![0, 1, 2, 3, 4, 5, 6, 7], vec![]),
            "columns A, B, C, D, E and F and 2 more are blank",
        ),
    ] {
        let block = ErrorBlock::sheet_issue(&issue, true);

        assert_eq!(
            block.message,
            format!("Row 1 of sheet “Ventas Q3” is not a valid header: {problems}."),
        );
    }
}

#[test]
fn every_sheet_problem_ends_with_a_next_step_and_a_footer_reason() {
    for issue in [
        SheetIssue::Empty {
            sheet: "S".to_string(),
        },
        header_issue(vec![0], vec![]),
        SheetIssue::ErrorCell {
            sheet: "S".to_string(),
            row: 2,
            column: 0,
            column_name: "a".to_string(),
            code: "#N/A",
        },
        SheetIssue::TooLarge {
            sheet: "S".to_string(),
            cells: 10_000_001,
            limit: 10_000_000,
        },
    ] {
        let block = ErrorBlock::sheet_issue(&issue, true);

        assert!(!block.message.is_empty(), "{issue:?}");
        assert!(!block.next_step.is_empty(), "{issue:?}");
        assert!(
            block
                .footer_reason
                .as_deref()
                .is_some_and(|reason| reason.starts_with("Continue is off")),
            "{issue:?}"
        );
        for text in [&block.message, &block.next_step] {
            assert!(!text.starts_with("document."), "{text}");
            assert!(!text.contains("%{"), "{text}");
        }
    }
}

#[test]
fn a_sheet_problem_goes_through_the_load_failure_block() {
    let failure = FileLoadError::Sheet(SheetIssue::Empty {
        sheet: "Hoja4".to_string(),
    });

    let block = ErrorBlock::file_load_failure(&failure, FileFormat::Spreadsheet);

    assert_eq!(
        block,
        ErrorBlock::sheet_issue(
            &SheetIssue::Empty {
                sheet: "Hoja4".to_string()
            },
            true
        )
    );
}

#[test]
fn the_spreadsheet_strings_exist_in_every_locale() {
    let keys = [
        "document.import_wizard.pick_file.description",
        "document.import_wizard.pick_file.filter_label",
        "document.import_wizard.pick_file.error.unsupported",
        "document.import_wizard.pick_file.error.next_spreadsheet",
        "document.import_wizard.file.spreadsheet_header",
        "document.import_wizard.file.sheet_label",
        "document.import_wizard.file.sheet_hint",
        "document.import_wizard.file.sheet_single",
        "document.import_wizard.sheet.empty.message",
        "document.import_wizard.sheet.empty.next",
        "document.import_wizard.sheet.empty.next_single",
        "document.import_wizard.sheet.header.next_single",
        "document.import_wizard.sheet.large.next_single",
        "document.import_wizard.sheet.header.message",
        "document.import_wizard.sheet.header.next",
        "document.import_wizard.sheet.header.blank_one",
        "document.import_wizard.sheet.header.blank_many",
        "document.import_wizard.sheet.header.duplicate_two",
        "document.import_wizard.sheet.header.duplicate_many",
        "document.import_wizard.sheet.cell.message",
        "document.import_wizard.sheet.cell.next",
        "document.import_wizard.sheet.large.message",
        "document.import_wizard.sheet.large.next",
        "document.import_wizard.sheet.list_and",
        "document.import_wizard.sheet.list_more",
        "document.import_wizard.footer.reason.sheet_empty",
        "document.import_wizard.footer.reason.sheet_header",
        "document.import_wizard.footer.reason.sheet_cell",
        "document.import_wizard.footer.reason.sheet_large",
    ];

    for locale in ["en", "es", "ko", "pt_BR", "zh_Hans"] {
        for key in keys {
            let text = dbflux_i18n::translate_in(locale, key);
            assert!(!text.is_empty() && text != key, "{locale}: {key}");
        }
    }
}

#[test]
fn with_no_other_sheet_the_next_step_says_to_fix_the_file_and_choose_it_again() {
    for (issue, next_step) in [
        (
            SheetIssue::Empty {
                sheet: "S".to_string(),
            },
            "Add a header row and data to this sheet in the spreadsheet app, save, and choose the file again.",
        ),
        (
            header_issue(vec![0], vec![]),
            "Give every column of row 1 a unique name in the spreadsheet app, save, and choose the file again.",
        ),
        (
            SheetIssue::ErrorCell {
                sheet: "S".to_string(),
                row: 1,
                column: 0,
                column_name: "#N/A".to_string(),
                code: "#N/A",
            },
            "Fix or clear that cell in the spreadsheet app, save, and choose the file again. Formulas are read as the result last saved in the file.",
        ),
        (
            SheetIssue::TooLarge {
                sheet: "S".to_string(),
                cells: 10_000_001,
                limit: 10_000_000,
            },
            "Export it as CSV from the spreadsheet app (Save as → CSV) and import the CSV instead.",
        ),
    ] {
        let block = ErrorBlock::sheet_issue(&issue, false);

        assert_eq!(block.next_step, next_step, "{issue:?}");
        assert!(!block.next_step.contains("above"), "{issue:?}");
    }
}
