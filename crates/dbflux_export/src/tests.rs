use super::*;
use dbflux_core::{ColumnKind, ColumnMeta, Value};
use std::time::Duration;

fn column(name: &str, kind: ColumnKind) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        type_name: "text".to_string(),
        kind,
        nullable: true,
        is_primary_key: false,
    }
}

fn issue_example() -> QueryResult {
    QueryResult::table(
        vec![
            column("id", ColumnKind::Integer),
            column("name", ColumnKind::Text),
            column("email", ColumnKind::Text),
            column("created_at", ColumnKind::Text),
        ],
        vec![
            vec![
                Value::Int(1),
                Value::Text("Alice".to_string()),
                Value::Text("alice@example.com".to_string()),
                Value::Text("2026-11-01 10:00:00".to_string()),
            ],
            vec![
                Value::Int(3),
                Value::Text("Bob".to_string()),
                Value::Null,
                Value::Text("2026-11-03 14:30:00".to_string()),
            ],
        ],
        None,
        Duration::from_millis(1),
    )
}

fn export_to_string(result: &QueryResult, format: ExportFormat) -> String {
    let mut buffer = Vec::new();
    export(result, format, &mut buffer).expect("export succeeds");
    String::from_utf8(buffer).expect("utf-8 output")
}

#[test]
fn table_results_offer_markdown_after_the_existing_formats() {
    assert_eq!(
        available_formats(&QueryResultShape::Table),
        &[
            ExportFormat::Csv,
            ExportFormat::JsonPretty,
            ExportFormat::JsonCompact,
            ExportFormat::Markdown,
        ]
    );
}

#[test]
fn non_table_results_keep_their_formats() {
    assert_eq!(
        available_formats(&QueryResultShape::Json),
        &[
            ExportFormat::JsonPretty,
            ExportFormat::JsonCompact,
            ExportFormat::Csv,
        ]
    );
    assert_eq!(
        available_formats(&QueryResultShape::Text),
        &[ExportFormat::Text, ExportFormat::JsonPretty]
    );
}

#[test]
fn markdown_is_saved_to_a_md_file() {
    assert_eq!(ExportFormat::Markdown.name(), "Markdown");
    assert_eq!(ExportFormat::Markdown.extension(), "md");
}

#[test]
fn markdown_export_writes_header_separator_and_one_line_per_row() {
    let output = export_to_string(&issue_example(), ExportFormat::Markdown);

    assert_eq!(
        output,
        "| id | name | email | created\\_at |\n\
         | --- | --- | --- | --- |\n\
         | 1 | Alice | alice@example.com | 2026-11-01 10:00:00 |\n\
         | 3 | Bob |  | 2026-11-03 14:30:00 |\n"
    );
}

#[test]
fn markdown_export_keeps_pipes_and_line_breaks_inside_one_cell() {
    let result = QueryResult::table(
        vec![column("note", ColumnKind::Text)],
        vec![vec![Value::Text("a|b\nc".to_string())]],
        None,
        Duration::from_millis(1),
    );

    let output = export_to_string(&result, ExportFormat::Markdown);

    assert_eq!(output, "| note |\n| --- |\n| a\\|b<br>c |\n");
}

#[test]
fn markdown_export_of_an_empty_result_still_writes_the_header() {
    let result = QueryResult::table(
        vec![column("id", ColumnKind::Integer)],
        Vec::new(),
        None,
        Duration::from_millis(1),
    );

    assert_eq!(
        export_to_string(&result, ExportFormat::Markdown),
        "| id |\n| --- |\n"
    );
}
