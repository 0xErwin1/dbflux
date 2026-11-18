use std::sync::atomic::{AtomicUsize, Ordering};

use dbflux_core::{ConnectionProfile, DbConfig, DbDriver, QueryRequest, Value};
use dbflux_driver_sqlite::SqliteDriver;
use dbflux_export::{CsvDelimiter, DateFormat, TextEncoding};

use super::*;
use crate::column_map::ColumnMappingOverride;
use crate::pipeline::{TableMappingMode, TableTransferStatus};

/// A scratch directory under the system temp dir, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "dbflux_transfer_import_file_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        Self(dir)
    }

    fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, bytes).expect("write scratch file");
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

fn names(file: &ImportFile) -> Vec<&str> {
    file.columns().iter().map(|c| c.name.as_str()).collect()
}

fn inspect(path: &Path, format: FileFormat) -> Result<ImportFile, TransferError> {
    ImportFile::inspect(path, format, FormatOptions::default())
}

fn source_error(result: Result<ImportFile, TransferError>) -> String {
    match result {
        Err(TransferError::Source(message)) => message,
        other => panic!("expected a source error, got {other:?}"),
    }
}

#[test]
fn csv_columns_come_from_the_header_row() {
    let scratch = Scratch::new();
    let path = scratch.write("people.csv", b"id,name,score\n1,Ada,9.5\n");

    let file = inspect(&path, FileFormat::Csv).expect("inspect");

    assert_eq!(names(&file), ["id", "name", "score"]);
    assert!(file.columns().iter().all(|c| c.type_name.is_none()));
    assert!(
        file.columns()
            .iter()
            .all(|c| c.nullable && !c.is_primary_key)
    );
}

#[test]
fn csv_header_follows_the_chosen_delimiter() {
    let scratch = Scratch::new();
    let path = scratch.write("people.csv", b"id;name\n1;Ada\n");

    let comma = inspect(&path, FileFormat::Csv).expect("inspect");
    assert_eq!(names(&comma), ["id;name"]);

    let options = FormatOptions {
        delimiter: CsvDelimiter::Semicolon,
        ..FormatOptions::default()
    };
    let semicolon = ImportFile::inspect(&path, FileFormat::Csv, options).expect("inspect");
    assert_eq!(names(&semicolon), ["id", "name"]);
}

#[test]
fn csv_header_is_decoded_from_windows_1252() {
    let scratch = Scratch::new();
    let path = scratch.write("people.csv", b"id,caf\xE9\n1,x\n");
    let options = FormatOptions {
        encoding: TextEncoding::Windows1252,
        ..FormatOptions::default()
    };

    let file = ImportFile::inspect(&path, FileFormat::Csv, options).expect("inspect");

    assert_eq!(names(&file), ["id", "café"]);
}

#[test]
fn csv_header_is_decoded_from_utf16le() {
    let scratch = Scratch::new();
    let mut bytes = vec![0xFF, 0xFE];
    for unit in "id,naïve\n1,x\n".encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    let path = scratch.write("people.csv", &bytes);
    let options = FormatOptions {
        encoding: TextEncoding::Utf16Le,
        ..FormatOptions::default()
    };

    let file = ImportFile::inspect(&path, FileFormat::Csv, options).expect("inspect");

    assert_eq!(names(&file), ["id", "naïve"]);
}

#[test]
fn csv_header_drops_the_utf8_byte_order_mark() {
    let scratch = Scratch::new();
    let path = scratch.write("people.csv", b"\xEF\xBB\xBFid,name\n1,Ada\n");
    let options = FormatOptions {
        encoding: TextEncoding::Utf8Bom,
        ..FormatOptions::default()
    };

    let file = ImportFile::inspect(&path, FileFormat::Csv, options).expect("inspect");

    assert_eq!(names(&file), ["id", "name"]);
}

#[test]
fn csv_without_a_header_row_is_rejected() {
    let scratch = Scratch::new();
    let path = scratch.write("empty.csv", b"");

    let message = source_error(inspect(&path, FileFormat::Csv));

    assert!(message.contains("no header row"), "{message}");
    assert!(message.contains("empty.csv"), "{message}");
}

#[test]
fn csv_header_with_a_blank_or_repeated_name_is_rejected() {
    let scratch = Scratch::new();

    let blank = scratch.write("blank.csv", b"id,,name\n1,2,3\n");
    let message = source_error(inspect(&blank, FileFormat::Csv));
    assert!(message.contains("column 2 has no name"), "{message}");

    let repeated = scratch.write("repeated.csv", b"id,name,id\n1,2,3\n");
    let message = source_error(inspect(&repeated, FileFormat::Csv));
    assert!(message.contains("'id' appears more than once"), "{message}");
}

#[test]
fn json_lines_columns_are_the_union_of_keys_in_first_seen_order() {
    let scratch = Scratch::new();
    let path = scratch.write(
        "events.jsonl",
        b"{\"b\":1,\"a\":2}\n{\"c\":3,\"a\":1}\n{\"d\":null}\n",
    );

    let file = inspect(&path, FileFormat::JsonLines).expect("inspect");

    assert_eq!(names(&file), ["b", "a", "c", "d"]);
    assert!(file.columns().iter().all(|c| c.type_name.is_none()));
}

#[test]
fn json_array_columns_keep_nested_values_as_one_column() {
    let scratch = Scratch::new();
    let path = scratch.write(
        "events.json",
        b"[{\"id\":1,\"profile\":{\"city\":\"Oslo\",\"tags\":[1,2]}},{\"id\":2,\"extra\":[]}]",
    );

    let file = inspect(&path, FileFormat::Json).expect("inspect");

    assert_eq!(names(&file), ["id", "profile", "extra"]);
}

#[test]
fn json_file_holding_json_lines_is_read_as_json_lines() {
    let scratch = Scratch::new();
    let path = scratch.write("legacy.json", b"{\"id\":1}\n{\"id\":2,\"name\":\"x\"}\n");

    let file = inspect(&path, FileFormat::Json).expect("inspect");

    assert_eq!(names(&file), ["id", "name"]);
}

#[test]
fn json_discovery_samples_a_bounded_number_of_records() {
    let scratch = Scratch::new();
    let mut lines = String::new();
    for _ in 0..DISCOVERY_SAMPLE_ROWS {
        lines.push_str("{\"early\":1}\n");
    }
    lines.push_str("{\"late\":1}\n");
    let path = scratch.write("many.jsonl", lines.as_bytes());

    let file = inspect(&path, FileFormat::JsonLines).expect("inspect");

    assert_eq!(names(&file), ["early"]);
}

#[test]
fn json_records_that_are_not_objects_are_rejected() {
    let scratch = Scratch::new();

    let array = scratch.write("numbers.json", b"[1,2]");
    let message = source_error(inspect(&array, FileFormat::Json));
    assert!(message.contains("must be an object"), "{message}");

    let lines = scratch.write("numbers.jsonl", b"{\"id\":1}\n7\n");
    let message = source_error(inspect(&lines, FileFormat::JsonLines));
    assert!(message.contains("expected a JSON object"), "{message}");
}

#[test]
fn json_without_any_record_is_rejected() {
    let scratch = Scratch::new();
    let empty_array = scratch.write("none.json", b"[]");
    let empty_lines = scratch.write("none.jsonl", b"");

    for (path, format) in [
        (&empty_array, FileFormat::Json),
        (&empty_lines, FileFormat::JsonLines),
    ] {
        let message = source_error(inspect(path, format));
        assert!(message.contains("no records"), "{message}");
    }
}

#[test]
fn formats_that_cannot_be_imported_are_rejected() {
    let scratch = Scratch::new();
    let path = scratch.write("page.html", b"<table></table>");

    for format in [FileFormat::Html, FileFormat::Markdown, FileFormat::Sql] {
        let message = source_error(inspect(&path, format));
        assert!(
            message.ends_with(&format!("{} files cannot be imported", format.label())),
            "{message}"
        );
    }
}

#[test]
fn a_missing_file_is_a_source_error_naming_the_path() {
    let scratch = Scratch::new();
    let message = source_error(inspect(&scratch.0.join("absent.csv"), FileFormat::Csv));

    assert!(message.contains("absent.csv"), "{message}");
}

#[test]
fn format_comes_from_the_extension_for_importable_formats_only() {
    assert_eq!(format_for_path(Path::new("a/b.csv")), Some(FileFormat::Csv));
    assert_eq!(format_for_path(Path::new("B.CSV")), Some(FileFormat::Csv));
    assert_eq!(format_for_path(Path::new("b.json")), Some(FileFormat::Json));
    assert_eq!(
        format_for_path(Path::new("b.JsonL")),
        Some(FileFormat::JsonLines)
    );
    for accepted in ["b.xml", "B.XML"] {
        assert_eq!(
            format_for_path(Path::new(accepted)),
            Some(FileFormat::Xml),
            "{accepted}"
        );
    }
    for accepted in ["a.xlsx", "a.XLSM", "a.xls", "a.ods"] {
        assert_eq!(
            format_for_path(Path::new(accepted)),
            Some(FileFormat::Spreadsheet),
            "{accepted}"
        );
    }
    for rejected in ["b.html", "b.md", "b.sql", "b.txt", "b"] {
        assert_eq!(format_for_path(Path::new(rejected)), None, "{rejected}");
    }
}

#[test]
fn the_table_defaults_to_the_file_stem_and_describes_a_one_table_manifest() {
    let scratch = Scratch::new();
    let path = scratch.write("monthly.report.csv", b"id,name\n1,Ada\n");
    let options = FormatOptions {
        null_text: "\\N".to_string(),
        ..FormatOptions::default()
    };

    let file = ImportFile::inspect(&path, FileFormat::Csv, options.clone()).expect("inspect");
    let table = file.manifest_table();

    assert_eq!(file.table_name(), "monthly.report");
    assert_eq!(table.name, "monthly.report");
    assert_eq!(table.file, "monthly.report.csv");
    assert_eq!(table.format, "csv");
    assert_eq!(table.columns, file.columns());
    assert_eq!(table.options, options);
    assert!(
        !scratch.0.join("manifest.json").exists(),
        "no manifest file is written"
    );
}

fn sqlite(scratch: &Scratch) -> Arc<dyn Connection> {
    let profile = ConnectionProfile::new(
        "import-file-test",
        DbConfig::SQLite {
            path: scratch.0.join("target.db"),
            connection_id: None,
        },
    );
    let connection = SqliteDriver::new().connect(&profile).expect("connect");
    Arc::from(connection)
}

fn run(connection: &Arc<dyn Connection>, sql: &str) {
    connection.execute(&QueryRequest::new(sql)).expect(sql);
}

fn rows(connection: &Arc<dyn Connection>, sql: &str) -> Vec<Vec<Value>> {
    connection
        .execute(&QueryRequest::new(sql))
        .expect(sql)
        .rows
        .into_iter()
        .map(|row| row.to_vec())
        .collect()
}

fn text(value: &str) -> Value {
    Value::Text(value.to_string())
}

fn plan(file: &ImportFile, mode: TableMappingMode, target: &str) -> ImportTablePlan {
    ImportTablePlan {
        source_table: file.table_name(),
        target_schema: None,
        target_table: target.to_string(),
        mapping_mode: mode,
        column_overrides: None,
    }
}

fn import_options(segment_size: u32) -> ImportOptions {
    ImportOptions {
        segment_size,
        target_database: "main".to_string(),
        destructive_confirmed: false,
    }
}

fn import(
    connection: &Arc<dyn Connection>,
    file: &ImportFile,
    plan: &ImportTablePlan,
    segment_size: u32,
) -> ImportOutcome {
    run_file_import(
        connection,
        file,
        plan,
        &import_options(segment_size),
        &CancelToken::new(),
        |_, _| {},
    )
    .expect("the import runs")
}

const PEOPLE: &str = "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, \
                      score REAL, note TEXT)";

/// A JSON integer past `i64::MAX` reaches a text column with every digit.
#[test]
fn json_integer_beyond_i64_keeps_its_digits_in_a_text_column() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, "CREATE TABLE big (id TEXT)");
    let path = scratch.write("big.jsonl", b"{\"id\":9223372036854775809}\n");
    let file = inspect(&path, FileFormat::JsonLines).expect("inspect");

    import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "big"),
        10,
    );

    assert_eq!(
        rows(&connection, "SELECT id FROM big"),
        vec![vec![text("9223372036854775809")]]
    );
}

/// One CSV column mapped onto an integer and a text column keeps its leading
/// zeros in the text column.
#[test]
fn one_source_column_mapped_onto_two_target_types_keeps_its_text() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(
        &connection,
        "CREATE TABLE codes (numeric_code INTEGER, original_code TEXT)",
    );
    let path = scratch.write("codes.csv", b"code\n00123\n");
    let file = inspect(&path, FileFormat::Csv).expect("inspect");
    let mut plan = plan(&file, TableMappingMode::Existing, "codes");
    plan.column_overrides = Some(vec![
        ColumnMappingOverride {
            target_column: "numeric_code".to_string(),
            source_column: Some("code".to_string()),
        },
        ColumnMappingOverride {
            target_column: "original_code".to_string(),
            source_column: Some("code".to_string()),
        },
    ]);

    import(&connection, &file, &plan, 10);

    assert_eq!(
        rows(
            &connection,
            "SELECT typeof(numeric_code), numeric_code, original_code FROM codes"
        ),
        vec![vec![text("integer"), Value::Int(123), text("00123")]]
    );
}

fn map_code_onto(plan: &mut ImportTablePlan, targets: &[&str]) {
    plan.column_overrides = Some(
        targets
            .iter()
            .map(|target| ColumnMappingOverride {
                target_column: (*target).to_string(),
                source_column: Some("code".to_string()),
            })
            .collect(),
    );
}

/// Each binding of one source column decodes by its own target's type: `true`
/// is `1` in a BOOLEAN column and stays `'true'` in a TEXT column.
#[test]
fn one_source_column_mapped_onto_boolean_and_text_decodes_each_binding() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(
        &connection,
        "CREATE TABLE flags (flag BOOLEAN, original TEXT)",
    );
    let path = scratch.write("flags.csv", b"code\ntrue\nfalse\n");
    let file = inspect(&path, FileFormat::Csv).expect("inspect");
    let mut plan = plan(&file, TableMappingMode::Existing, "flags");
    map_code_onto(&mut plan, &["flag", "original"]);

    import(&connection, &file, &plan, 10);

    assert_eq!(
        rows(
            &connection,
            "SELECT typeof(flag), flag, original FROM flags ORDER BY rowid"
        ),
        vec![
            vec![text("integer"), Value::Int(1), text("true")],
            vec![text("integer"), Value::Int(0), text("false")],
        ]
    );
}

fn timestamp_and_text_import(options: FormatOptions, content: &[u8]) -> Vec<Vec<Value>> {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(
        &connection,
        "CREATE TABLE events (happened TIMESTAMP, original TEXT)",
    );
    let path = scratch.write("events.csv", content);
    let file = ImportFile::inspect(&path, FileFormat::Csv, options).expect("inspect");
    let mut plan = plan(&file, TableMappingMode::Existing, "events");
    map_code_onto(&mut plan, &["happened", "original"]);

    import(&connection, &file, &plan, 10);

    rows(&connection, "SELECT happened, original FROM events")
}

#[test]
fn one_source_column_mapped_onto_timestamp_and_text_decodes_the_date_preset_per_binding() {
    let decoded =
        timestamp_and_text_import(FormatOptions::default(), b"code\n2024-03-05T10:20:30Z\n");
    let epoch = timestamp_and_text_import(
        FormatOptions {
            date_format: DateFormat::Epoch,
            ..FormatOptions::default()
        },
        b"code\n1709634030\n",
    );

    // The same instant decodes to the same stored timestamp under either
    // preset; the TEXT column keeps whatever the file held.
    assert_eq!(decoded[0][0], epoch[0][0]);
    assert_ne!(decoded[0][0], text("2024-03-05T10:20:30Z"));
    assert_eq!(decoded[0][1], text("2024-03-05T10:20:30Z"));
    assert_eq!(epoch[0][1], text("1709634030"));
}

#[test]
fn csv_loads_into_an_existing_table_decoded_by_its_column_types() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, PEOPLE);
    let path = scratch.write(
        "people.csv",
        b"id;name;score;note\n1;Ada;9.5;\\N\n2;Grace;7;kept\n",
    );
    let options = FormatOptions {
        delimiter: CsvDelimiter::Semicolon,
        null_text: "\\N".to_string(),
        ..FormatOptions::default()
    };
    let file = ImportFile::inspect(&path, FileFormat::Csv, options).expect("inspect");

    let outcome = import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "people"),
        500,
    );

    assert_eq!(
        outcome.tables[0].status,
        TableTransferStatus::Completed { rows: 2 }
    );
    assert_eq!(
        rows(
            &connection,
            "SELECT id, name, score, note, typeof(id), typeof(score) FROM people ORDER BY id"
        ),
        vec![
            vec![
                Value::Int(1),
                text("Ada"),
                Value::Float(9.5),
                Value::Null,
                text("integer"),
                text("real")
            ],
            vec![
                Value::Int(2),
                text("Grace"),
                Value::Float(7.0),
                text("kept"),
                text("integer"),
                text("real")
            ],
        ]
    );
}

#[test]
fn json_lines_load_into_an_existing_table_with_nested_values_as_json() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(
        &connection,
        "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, score REAL, profile TEXT)",
    );
    let path = scratch.write(
        "people.jsonl",
        b"{\"id\":1,\"name\":\"Ada\",\"score\":9.5,\"profile\":{\"city\":\"Oslo\"}}\n\
          {\"id\":2,\"name\":null,\"score\":7}\n",
    );
    let file = inspect(&path, FileFormat::JsonLines).expect("inspect");

    let outcome = import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "people"),
        500,
    );

    assert_eq!(
        outcome.tables[0].status,
        TableTransferStatus::Completed { rows: 2 }
    );
    let stored = rows(
        &connection,
        "SELECT id, name, score, profile FROM people ORDER BY id",
    );
    assert_eq!(
        &stored[0][..3],
        [Value::Int(1), text("Ada"), Value::Float(9.5)]
    );
    let Value::Text(profile) = &stored[0][3] else {
        panic!("the nested value is stored as text: {:?}", stored[0][3]);
    };
    let parsed: serde_json::Value = serde_json::from_str(profile).expect("stored as JSON");
    assert_eq!(parsed["city"], "Oslo");
    assert_eq!(
        stored[1],
        vec![Value::Int(2), Value::Null, Value::Float(7.0), Value::Null]
    );
}

#[test]
fn a_json_array_loads_into_an_existing_table() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, PEOPLE);
    let path = scratch.write(
        "people.json",
        b"[{\"id\":1,\"name\":\"Ada\"},{\"id\":2,\"name\":\"Grace\"}]",
    );
    let file = inspect(&path, FileFormat::Json).expect("inspect");

    import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "people"),
        500,
    );

    assert_eq!(
        rows(&connection, "SELECT id, name FROM people ORDER BY id"),
        vec![
            vec![Value::Int(1), text("Ada")],
            vec![Value::Int(2), text("Grace")]
        ]
    );
}

#[test]
fn csv_loads_into_a_new_table_keeping_the_text_it_holds() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    let path = scratch.write("events.csv", b"id,label\n1,start\n2,stop\n");
    let file = inspect(&path, FileFormat::Csv).expect("inspect");

    let outcome = import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Create, "events"),
        500,
    );

    assert_eq!(
        outcome.tables[0].status,
        TableTransferStatus::Completed { rows: 2 }
    );
    assert_eq!(
        rows(&connection, "SELECT id, label FROM events ORDER BY label"),
        vec![
            vec![text("1"), text("start")],
            vec![text("2"), text("stop")]
        ]
    );
}

#[test]
fn json_lines_load_into_a_new_table_as_text_columns() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    let path = scratch.write(
        "events.jsonl",
        b"{\"id\":1,\"ratio\":0.5,\"label\":\"start\"}\n{\"id\":2,\"ratio\":1.5,\"label\":null}\n",
    );
    let file = inspect(&path, FileFormat::JsonLines).expect("inspect");

    import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Create, "events"),
        500,
    );

    assert_eq!(
        rows(
            &connection,
            "SELECT id, ratio, label FROM events ORDER BY id"
        ),
        vec![
            vec![text("1"), text("0.5"), text("start")],
            vec![text("2"), text("1.5"), Value::Null]
        ],
        "a new table's columns have the dialect's text type, so every scalar is stored as its text"
    );
}

fn rejection(
    connection: &Arc<dyn Connection>,
    file: &ImportFile,
    plan: &ImportTablePlan,
) -> String {
    let result = run_file_import(
        connection,
        file,
        plan,
        &import_options(1),
        &CancelToken::new(),
        |_, _| {},
    );

    match result {
        Err(TransferError::Source(message)) => message,
        Err(other) => panic!("expected a source error, got {other:?}"),
        Ok(_) => panic!("the import must be rejected"),
    }
}

#[test]
fn a_malformed_csv_row_rejects_the_import_and_leaves_the_table_unchanged() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, PEOPLE);
    run(
        &connection,
        "INSERT INTO people (id, name) VALUES (100, 'kept')",
    );
    let path = scratch.write("people.csv", b"id,name\n1,Ada\n2,Grace\n3\n");
    let file = inspect(&path, FileFormat::Csv).expect("inspect");

    // One row per chunk and a target that has no transaction: rows 1 and 2
    // would already be written when row 3 fails, were the file not checked first.
    let message = rejection(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "people"),
    );

    assert!(message.contains("fields"), "{message}");
    assert_eq!(
        rows(&connection, "SELECT id, name FROM people ORDER BY id"),
        vec![vec![Value::Int(100), text("kept")]]
    );
}

#[test]
fn a_malformed_json_line_inside_the_sample_is_rejected_when_inspecting() {
    let scratch = Scratch::new();
    let path = scratch.write(
        "people.jsonl",
        b"{\"id\":1,\"name\":\"Ada\"}\n{\"id\":2,\"name\":\n",
    );

    let message = source_error(inspect(&path, FileFormat::JsonLines));

    assert!(message.starts_with("invalid JSON:"), "{message}");
}

#[test]
fn a_malformed_json_line_past_the_sample_rejects_the_import_and_leaves_the_table_unchanged() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, PEOPLE);
    let mut lines = String::new();
    for id in 1..=DISCOVERY_SAMPLE_ROWS + 1 {
        lines.push_str(&format!("{{\"id\":{id},\"name\":\"n\"}}\n"));
    }
    lines.push_str("{\"id\":0,\"name\":\n");
    let path = scratch.write("people.jsonl", lines.as_bytes());
    let file = inspect(&path, FileFormat::JsonLines).expect("inspect");

    let message = rejection(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "people"),
    );

    assert!(message.starts_with("invalid JSON:"), "{message}");
    assert_eq!(
        rows(&connection, "SELECT id FROM people"),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn progress_has_no_estimated_total_for_a_loose_file() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, PEOPLE);
    let path = scratch.write("people.csv", b"id,name\n1,Ada\n2,Grace\n");
    let file = inspect(&path, FileFormat::Csv).expect("inspect");
    let mut reported = Vec::new();

    run_file_import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "people"),
        &import_options(1),
        &CancelToken::new(),
        |done, estimated| reported.push((done, estimated)),
    )
    .expect("the import runs");

    assert_eq!(reported, vec![(1, None), (2, None)]);
}

#[test]
fn an_unconfirmed_destructive_plan_is_rejected_before_touching_the_table() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, PEOPLE);
    run(
        &connection,
        "INSERT INTO people (id, name) VALUES (100, 'kept')",
    );
    let path = scratch.write("people.csv", b"id,name\n1,Ada\n");
    let file = inspect(&path, FileFormat::Csv).expect("inspect");

    let result = run_file_import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Recreate, "people"),
        &import_options(500),
        &CancelToken::new(),
        |_, _| {},
    );

    assert!(result.is_err());
    assert_eq!(
        rows(&connection, "SELECT id FROM people"),
        vec![vec![Value::Int(100)]]
    );
}

#[test]
fn folder_import_keeps_a_manifest_column_without_a_type_untyped() {
    use crate::import::run_import;
    use crate::manifest::{ManifestSource, TransferManifest};

    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    scratch.write("t.jsonl", b"{\"id\":1,\"n\":42}\n");
    let manifest = TransferManifest {
        version: TransferManifest::CURRENT_VERSION,
        source: ManifestSource {
            driver: "sqlite".to_string(),
            database: "main".to_string(),
            schema: None,
        },
        created_at: "2026-07-07T10:00:00+00:00".to_string(),
        tables: vec![ManifestTable {
            schema: None,
            name: "t".to_string(),
            file: "t.jsonl".to_string(),
            format: "jsonl".to_string(),
            columns: vec![
                TransferColumn {
                    name: "id".to_string(),
                    type_name: Some("INTEGER".to_string()),
                    nullable: true,
                    is_primary_key: false,
                },
                TransferColumn {
                    name: "n".to_string(),
                    type_name: None,
                    nullable: true,
                    is_primary_key: false,
                },
            ],
            row_count: 1,
            fk_order_index: 0,
            options: FormatOptions::default(),
        }],
    };
    scratch.write(
        "manifest.json",
        serde_json::to_string(&manifest)
            .expect("manifest")
            .as_bytes(),
    );
    let plans = [ImportTablePlan {
        source_table: "t".to_string(),
        target_schema: None,
        target_table: "t".to_string(),
        mapping_mode: TableMappingMode::Create,
        column_overrides: None,
    }];

    run_import(
        &connection,
        &scratch.0,
        &plans,
        &import_options(500),
        &CancelToken::new(),
        |_, _, _| {},
    )
    .expect("the import runs");

    assert_eq!(
        rows(&connection, "SELECT typeof(n) FROM t"),
        vec![vec![text("integer")]]
    );
}

#[test]
fn a_new_table_from_a_loose_file_declares_its_untyped_columns_as_text() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    let path = scratch.write("events.csv", b"id,label\n1,start\n");
    let file = inspect(&path, FileFormat::Csv).expect("inspect");

    import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Create, "events"),
        500,
    );

    assert_eq!(
        rows(
            &connection,
            "SELECT name, type FROM pragma_table_info('events') ORDER BY cid"
        ),
        vec![
            vec![text("id"), text("TEXT")],
            vec![text("label"), text("TEXT")]
        ]
    );
}

#[test]
fn a_non_object_json_record_after_the_sample_rejects_the_import_with_the_target_unchanged() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, PEOPLE);
    let mut lines = String::new();
    for id in 1..=120 {
        lines.push_str(&format!("{{\"id\":{id},\"name\":\"n\"}}\n"));
    }
    lines.push_str("5\n");
    let path = scratch.write("people.jsonl", lines.as_bytes());
    let file = inspect(&path, FileFormat::JsonLines).expect("inspect");
    let before = std::fs::read(scratch.0.join("target.db")).expect("db bytes");

    let message = rejection(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "people"),
    );

    assert!(message.contains("expected a JSON object"), "{message}");
    assert_eq!(
        std::fs::read(scratch.0.join("target.db")).expect("db bytes"),
        before
    );
}

/// A cancelled token stops the JSON preflight before it reads the file: the
/// import returns the cancelled outcome a cancelled import returns, with the
/// target untouched, instead of scanning (and rejecting) the file.
#[test]
fn a_cancelled_json_import_stops_before_the_preflight_scan_and_writes_nothing() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, PEOPLE);
    let path = scratch.write("people.jsonl", b"{\"id\":1,\"name\":\"Ada\"}\n");
    let file = inspect(&path, FileFormat::Json).expect("inspect");
    scratch.write("people.jsonl", b"{\"id\":1}\n[1,2]\n{broken\n");

    let cancel = CancelToken::new();
    cancel.cancel();

    let outcome = run_file_import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "people"),
        &import_options(1),
        &cancel,
        |_, _| {},
    )
    .expect("a cancelled import is an outcome, not an error");

    assert!(outcome.cancelled);
    assert!(
        rows(&connection, "SELECT id FROM people").is_empty(),
        "nothing is written"
    );
}

mod spreadsheet;
mod xml;
