use dbflux_transfer::{FileFormat, TransferError};

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
        "Only .csv, .json and .jsonl files can be imported."
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
        "Only .csv, .json, .jsonl and .sql files can be imported."
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
