use super::*;
use crate::file_sink::FileSink;
use crate::pipeline::{RowChunk, RowSink};

fn transfer_column(name: &str, type_name: &str) -> TransferColumn {
    TransferColumn {
        name: name.to_string(),
        type_name: Some(type_name.to_string()),
        nullable: true,
        is_primary_key: false,
    }
}

/// Writes `rows` the way an XML export does, through the real writer.
fn export_xml(scratch: &Scratch, name: &str, columns: &[&str], rows: Vec<Vec<Value>>) -> PathBuf {
    let path = scratch.0.join(name);
    let columns: Vec<TransferColumn> = columns
        .iter()
        .map(|column| transfer_column(column, "text"))
        .collect();
    let mut sink = FileSink::to_file(&path, FileFormat::Xml);
    sink.begin(&columns, TableMappingMode::Existing)
        .expect("begin");
    sink.write_chunk(&RowChunk(rows)).expect("write");
    sink.finish().expect("finish");
    path
}

const NOTES: &str = "CREATE TABLE notes (id INTEGER, body TEXT, extra TEXT)";

#[test]
fn an_exported_xml_file_round_trips_nulls_empty_strings_and_escaped_text() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, NOTES);
    let tricky = "tab\there\nnew line\rcarriage <b> & \"quoted\" 'single'";
    let path = export_xml(
        &scratch,
        "notes.xml",
        &["id", "body", "extra"],
        vec![
            vec![Value::Int(1), text("plain"), Value::Null],
            vec![Value::Int(2), text(""), text(tricky)],
            vec![Value::Int(3), Value::Null, text("tail")],
        ],
    );
    let file = inspect(&path, FileFormat::Xml).expect("inspect");
    assert_eq!(names(&file), ["id", "body", "extra"]);

    let outcome = import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "notes"),
        2,
    );

    assert_eq!(
        outcome.tables[0].status,
        TableTransferStatus::Completed { rows: 3 }
    );
    assert_eq!(
        rows(
            &connection,
            "SELECT id, body, extra, typeof(id) FROM notes ORDER BY id"
        ),
        vec![
            vec![Value::Int(1), text("plain"), Value::Null, text("integer")],
            vec![Value::Int(2), text(""), text(tricky), text("integer")],
            vec![Value::Int(3), Value::Null, text("tail"), text("integer")],
        ]
    );
}

#[test]
fn numeric_character_references_and_cdata_are_decoded() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, NOTES);
    let path = scratch.write(
        "notes.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rows>\n<row><field name=\"id\">7</field>\
         <field name=\"body\">A&#66;&#x43;&#10;&#9;&#13;&lt;&gt;&amp;&quot;&apos;</field>\
         <field name=\"extra\"><![CDATA[<raw & text>]]></field></row>\n</rows>\n"
            .as_bytes(),
    );
    let file = inspect(&path, FileFormat::Xml).expect("inspect");

    import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "notes"),
        500,
    );

    assert_eq!(
        rows(&connection, "SELECT id, body, extra FROM notes"),
        vec![vec![
            Value::Int(7),
            text("ABC\n\t\r<>&\"'"),
            text("<raw & text>")
        ]]
    );
}

#[test]
fn xml_columns_are_the_union_of_field_names_in_first_seen_order() {
    let scratch = Scratch::new();
    let path = scratch.write(
        "t.xml",
        b"<rows><row><field name=\"b\">1</field><field name=\"a\">2</field></row>\
          <row><field name=\"c\">3</field><field name=\"a\" null=\"true\"/></row></rows>",
    );

    let file = inspect(&path, FileFormat::Xml).expect("inspect");

    assert_eq!(names(&file), ["b", "a", "c"]);
    assert!(file.columns().iter().all(|c| c.type_name.is_none()));
}

#[test]
fn xml_discovery_samples_a_bounded_number_of_rows() {
    let scratch = Scratch::new();
    let mut document = String::from("<rows>");
    for _ in 0..DISCOVERY_SAMPLE_ROWS {
        document.push_str("<row><field name=\"a\">1</field></row>");
    }
    document.push_str("<row><field name=\"late\">1</field></row></rows>");
    let path = scratch.write("t.xml", document.as_bytes());

    let file = inspect(&path, FileFormat::Xml).expect("inspect");

    assert_eq!(names(&file), ["a"]);
}

#[test]
fn xml_in_another_shape_is_rejected_saying_which_shape_is_expected() {
    let scratch = Scratch::new();
    let cases: [(&str, &str); 6] = [
        (
            "<catalog><item/></catalog>",
            "the root element is <catalog>",
        ),
        ("<rows><item/></rows>", "found <item> inside <rows>"),
        (
            "<rows><row><cell>1</cell></row></rows>",
            "found <cell> inside <row>",
        ),
        (
            "<rows><row><field>1</field></row></rows>",
            "a <field> without a name attribute",
        ),
        (
            "<rows><row><field name=\"a\"><b/></field></row></rows>",
            "found <b> inside <field>",
        ),
        ("just text", "unexpected text"),
    ];

    for (document, reason) in cases {
        let path = scratch.write("t.xml", document.as_bytes());
        let message = source_error(inspect(&path, FileFormat::Xml));

        assert!(message.contains(reason), "{document}: {message}");
        assert!(
            message.contains("<rows><row><field name=\"...\">"),
            "the message states the expected shape: {message}"
        );
    }
}

#[test]
fn xml_without_any_row_is_rejected() {
    let scratch = Scratch::new();

    for document in ["<rows></rows>", "<rows/>", "<rows><row/></rows>"] {
        let path = scratch.write("t.xml", document.as_bytes());
        let message = source_error(inspect(&path, FileFormat::Xml));

        assert!(
            message.contains("no records") || message.contains("no keys"),
            "{document}: {message}"
        );
    }
}

#[test]
fn malformed_xml_inside_the_sample_is_rejected_when_inspecting() {
    let scratch = Scratch::new();

    for document in [
        "<rows><row><field name=\"a\">1</row></rows>",
        "<rows><row><field name=\"a\">1</field></row>",
        "<rows><row><field name=\"a\">&bogus;</field></row></rows>",
        "<rows><row><field name=\"a\">1</field></row></rows><extra/>",
    ] {
        let path = scratch.write("t.xml", document.as_bytes());
        let message = source_error(inspect(&path, FileFormat::Xml));

        assert!(message.contains("t.xml"), "{document}: {message}");
    }
}

#[test]
fn malformed_xml_past_the_sample_rejects_the_import_and_leaves_the_table_unchanged() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, NOTES);
    run(
        &connection,
        "INSERT INTO notes (id, body) VALUES (100, 'kept')",
    );
    let mut document = String::from("<rows>");
    for id in 0..(DISCOVERY_SAMPLE_ROWS + 5) {
        document.push_str(&format!(
            "<row><field name=\"id\">{id}</field><field name=\"body\">x</field></row>"
        ));
    }
    document.push_str("<row><field name=\"id\">999</field></rows>");
    let path = scratch.write("notes.xml", document.as_bytes());
    let file = inspect(&path, FileFormat::Xml).expect("the sample is well formed");

    // One row per chunk and a target without a transaction: the rows before
    // the malformed one would already be written were the file not checked.
    let message = rejection(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "notes"),
    );

    assert!(message.contains("invalid XML"), "{message}");
    assert_eq!(
        rows(&connection, "SELECT id, body FROM notes ORDER BY id"),
        vec![vec![Value::Int(100), text("kept")]]
    );
}

#[test]
fn xml_loads_into_a_new_table_as_text_columns() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    let path = export_xml(
        &scratch,
        "events.xml",
        &["id", "label"],
        vec![vec![text("1"), text("start")], vec![text("2"), Value::Null]],
    );
    let file = inspect(&path, FileFormat::Xml).expect("inspect");

    import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Create, "events"),
        500,
    );

    assert_eq!(
        rows(
            &connection,
            "SELECT id, label, typeof(id) FROM events ORDER BY rowid"
        ),
        vec![
            vec![text("1"), text("start"), text("text")],
            vec![text("2"), Value::Null, text("text")],
        ]
    );
}

#[test]
fn xml_is_decoded_by_the_types_of_an_existing_table() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, PEOPLE);
    let path = scratch.write(
        "people.xml",
        b"<rows><row><field name=\"id\">1</field><field name=\"name\">Ada</field>\
          <field name=\"score\">9.5</field><field name=\"note\" null=\"true\"/></row></rows>",
    );
    let file = inspect(&path, FileFormat::Xml).expect("inspect");

    import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "people"),
        500,
    );

    assert_eq!(
        rows(
            &connection,
            "SELECT id, name, score, note, typeof(score) FROM people"
        ),
        vec![vec![
            Value::Int(1),
            text("Ada"),
            Value::Float(9.5),
            Value::Null,
            text("real")
        ]]
    );
}

#[test]
fn a_folder_manifest_naming_xml_imports_the_file_with_the_manifest_types() {
    use crate::import::run_import;
    use crate::manifest::{ManifestSource, TransferManifest};

    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    let path = export_xml(
        &scratch,
        "t.xml",
        &["id", "score", "note"],
        vec![vec![Value::Int(1), Value::Float(2.5), Value::Null]],
    );
    assert!(path.exists());
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
            file: "t.xml".to_string(),
            format: "xml".to_string(),
            columns: vec![
                transfer_column("id", "INTEGER"),
                transfer_column("score", "REAL"),
                transfer_column("note", "TEXT"),
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
        rows(
            &connection,
            "SELECT id, score, note, typeof(id), typeof(score) FROM t"
        ),
        vec![vec![
            Value::Int(1),
            Value::Float(2.5),
            Value::Null,
            text("integer"),
            text("real")
        ]]
    );
}

#[test]
fn a_field_name_repeated_inside_one_row_is_rejected_but_may_repeat_across_rows() {
    let scratch = Scratch::new();
    for document in [
        "<rows><row><field name=\"id\">1</field><field name=\"id\">2</field></row></rows>",
        "<rows><row><field name=\"id\">1</field><field name=\"id\" null=\"true\"/></row></rows>",
        "<rows><row><field name=\"id\"/><field name=\"id\"/></row></rows>",
    ] {
        let path = scratch.write("t.xml", document.as_bytes());
        let message = source_error(inspect(&path, FileFormat::Xml));
        assert!(
            message.contains("field 'id' appears more than once in a <row>"),
            "{document}: {message}"
        );
    }

    let path = scratch.write(
        "ok.xml",
        b"<rows><row><field name=\"id\">1</field></row><row><field name=\"id\">2</field></row></rows>",
    );
    assert_eq!(
        names(&inspect(&path, FileFormat::Xml).expect("inspect")),
        ["id"]
    );
}

#[test]
fn epoch_seconds_in_an_xml_field_import_into_a_timestamp_column() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, "CREATE TABLE ev (id INTEGER, at TIMESTAMP)");
    let path = scratch.write(
        "ev.xml",
        b"<rows><row><field name=\"id\">1</field><field name=\"at\">1706659200</field></row></rows>",
    );
    let file = inspect(&path, FileFormat::Xml).expect("inspect");

    import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "ev"),
        500,
    );

    let stored = rows(&connection, "SELECT at FROM ev");
    assert_ne!(stored, vec![vec![text("1706659200")]], "{stored:?}");
    let instant = rows(
        &connection,
        "SELECT CAST(strftime('%s', at) AS INTEGER) FROM ev",
    );
    assert_eq!(instant, vec![vec![Value::Int(1_706_659_200)]], "{stored:?}");
}

fn late_field_rows(first_row: &str, late_row: &str) -> String {
    let mut document = String::from("<rows>");
    for id in 0..DISCOVERY_SAMPLE_ROWS {
        document.push_str(&format!(
            "<row><field name=\"id\">{id}</field><field name=\"body\">x</field></row>"
        ));
    }
    document.push_str(first_row);
    document.push_str(late_row);
    document.push_str("</rows>");
    document
}

#[test]
fn an_xml_field_first_seen_after_the_sample_rejects_the_import_naming_field_and_row() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, NOTES);
    run(
        &connection,
        "INSERT INTO notes (id, body) VALUES (100, 'kept')",
    );
    let document = late_field_rows(
        "",
        "<row><field name=\"id\">500</field><field name=\"late\">y</field></row>",
    );
    let path = scratch.write("notes.xml", document.as_bytes());
    let file = inspect(&path, FileFormat::Xml).expect("the sample has no late field");

    let message = rejection(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "notes"),
    );

    assert!(message.contains("field 'late'"), "{message}");
    assert!(message.contains("row 101"), "{message}");
    assert_eq!(
        rows(&connection, "SELECT id, body FROM notes ORDER BY id"),
        vec![vec![Value::Int(100), text("kept")]]
    );
}

#[test]
fn a_json_key_first_seen_after_the_sample_is_rejected_the_same_way() {
    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(&connection, NOTES);
    let mut lines = String::new();
    for id in 0..DISCOVERY_SAMPLE_ROWS {
        lines.push_str(&format!("{{\"id\":{id}}}\n"));
    }
    lines.push_str("{\"id\":500,\"late\":\"y\"}\n");
    let path = scratch.write("notes.jsonl", lines.as_bytes());
    let file = inspect(&path, FileFormat::JsonLines).expect("inspect");

    let message = rejection(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "notes"),
    );

    assert!(message.contains("field 'late'"), "{message}");
    assert!(message.contains("row 101"), "{message}");
}

/// Writes a folder whose manifest names `file` with `format`, and truncates
/// `notes` from it.
fn truncating_folder_import(
    scratch: &Scratch,
    file: &str,
    format: &str,
    content: &[u8],
) -> (Arc<dyn Connection>, Result<ImportOutcome, TransferError>) {
    use crate::import::run_import;
    use crate::manifest::{ManifestSource, TransferManifest};

    let connection = sqlite(scratch);
    run(&connection, NOTES);
    run(
        &connection,
        "INSERT INTO notes (id, body) VALUES (100, 'kept')",
    );
    scratch.write(file, content);
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
            name: "notes".to_string(),
            file: file.to_string(),
            format: format.to_string(),
            columns: vec![
                transfer_column("id", "INTEGER"),
                transfer_column("body", "TEXT"),
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
        source_table: "notes".to_string(),
        target_schema: None,
        target_table: "notes".to_string(),
        mapping_mode: TableMappingMode::Truncate,
        column_overrides: None,
    }];
    let outcome = run_import(
        &connection,
        &scratch.0,
        &plans,
        &ImportOptions {
            destructive_confirmed: true,
            ..import_options(1)
        },
        &CancelToken::new(),
        |_, _, _| {},
    );
    (connection, outcome)
}

#[test]
fn a_broken_folder_file_fails_before_truncate_removes_the_rows() {
    let cases: [(&str, &str, &[u8]); 3] = [
        ("notes.xml", "xml", b"<catalog/>"),
        (
            "notes.xml",
            "xml",
            b"<rows><row><field name=\"id\">1</field></row><row><field name=\"id\">2</rows>",
        ),
        ("notes.jsonl", "jsonl", b"{\"id\":1}\n5\n"),
    ];

    for (file, format, content) in cases {
        let scratch = Scratch::new();
        let (connection, outcome) = truncating_folder_import(&scratch, file, format, content);

        let failed = match outcome {
            Err(_) => true,
            Ok(outcome) => matches!(outcome.tables[0].status, TableTransferStatus::Failed { .. }),
        };
        assert!(failed, "{file}: the import must fail");
        assert_eq!(
            rows(&connection, "SELECT id, body FROM notes ORDER BY id"),
            vec![vec![Value::Int(100), text("kept")]],
            "{file}: the rows survive"
        );
    }
}

#[test]
fn epoch_dates_and_times_in_an_exported_xml_file_import_into_date_time_and_timestamp_columns() {
    use dbflux_core::chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
    use dbflux_export::DateFormat;

    let scratch = Scratch::new();
    let connection = sqlite(&scratch);
    run(
        &connection,
        "CREATE TABLE ev (day DATE, at TIME, seen TIMESTAMP)",
    );
    let path = scratch.0.join("ev.xml");
    let columns: Vec<TransferColumn> = ["day", "at", "seen"]
        .into_iter()
        .map(|name| transfer_column(name, "text"))
        .collect();
    let mut sink = FileSink::to_file(&path, FileFormat::Xml).with_options(FormatOptions {
        date_format: DateFormat::Epoch,
        ..FormatOptions::default()
    });
    sink.begin(&columns, TableMappingMode::Existing)
        .expect("begin");
    sink.write_chunk(&RowChunk(vec![vec![
        Value::Date(NaiveDate::from_ymd_opt(2024, 1, 31).expect("date")),
        Value::Time(NaiveTime::from_hms_opt(13, 45, 30).expect("time")),
        Value::DateTime(
            Utc.with_ymd_and_hms(2024, 3, 5, 10, 20, 30)
                .single()
                .expect("ts"),
        ),
    ]]))
    .expect("write");
    sink.finish().expect("finish");
    let file = inspect(&path, FileFormat::Xml).expect("inspect");

    import(
        &connection,
        &file,
        &plan(&file, TableMappingMode::Existing, "ev"),
        500,
    );

    let stored = rows(&connection, "SELECT day, at, seen FROM ev");
    let Value::Text(day) = &stored[0][0] else {
        panic!("{stored:?}");
    };
    let Value::Text(time) = &stored[0][1] else {
        panic!("{stored:?}");
    };
    assert!(day.starts_with("2024-01-31"), "{stored:?}");
    assert!(time.starts_with("13:45:30"), "{stored:?}");
    assert!(
        matches!(&stored[0][2], Value::Text(t) if t.starts_with("2024-03-05")),
        "{stored:?}"
    );
}
