use super::*;
use dbflux_core::ColumnKind;

fn column(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        type_name: "text".to_string(),
        kind: ColumnKind::Text,
        nullable: true,
        is_primary_key: false,
    }
}

fn columns() -> Vec<ColumnMeta> {
    vec![column("id"), column("note")]
}

fn rows() -> Vec<Vec<Value>> {
    vec![
        vec![Value::Int(1), Value::Text("a <b> & \"c\"".to_string())],
        vec![Value::Int(2), Value::Null],
    ]
}

fn html(columns: &[ColumnMeta], rows: &[Vec<Value>]) -> String {
    let mut output = Vec::new();
    let mut writer = HtmlStreamWriter::new(&mut output, "public.notes");
    writer.write_header(columns).unwrap();
    for row in rows {
        writer.write_row(row).unwrap();
    }
    writer.finish().unwrap();
    String::from_utf8(output).unwrap()
}

fn markdown(columns: &[ColumnMeta], rows: &[Vec<Value>]) -> String {
    let mut output = Vec::new();
    let mut writer = MarkdownStreamWriter::new(&mut output);
    writer.write_header(columns).unwrap();
    for row in rows {
        writer.write_row(row).unwrap();
    }
    writer.finish().unwrap();
    String::from_utf8(output).unwrap()
}

fn xml(columns: &[ColumnMeta], rows: &[Vec<Value>]) -> String {
    let mut output = Vec::new();
    let mut writer = XmlStreamWriter::new(&mut output);
    writer.write_header(columns).unwrap();
    for row in rows {
        writer.write_row(row).unwrap();
    }
    writer.finish().unwrap();
    String::from_utf8(output).unwrap()
}

#[test]
fn html_writes_one_escaped_table_document() {
    assert_eq!(
        html(&columns(), &rows()),
        "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n<title>public.notes</title>\n<style>th, td { white-space: pre-wrap; }</style>\n</head>\n<body>\n<table>\n<thead>\n<tr><th>id</th><th>note</th></tr>\n</thead>\n<tbody>\n\
<tr><td>1</td><td>a &lt;b&gt; &amp; &quot;c&quot;</td></tr>\n\
<tr><td>2</td><td></td></tr>\n\
</tbody>\n</table>\n</body>\n</html>\n"
    );
}

#[test]
fn html_keeps_multiline_values_in_one_cell_with_whitespace_preserved() {
    let rows = vec![vec![
        Value::Int(1),
        Value::Text("first\nsecond  end".to_string()),
    ]];
    let output = html(&columns(), &rows);
    assert!(output.contains("<style>th, td { white-space: pre-wrap; }</style>"));
    assert!(output.contains("<tr><td>1</td><td>first\nsecond  end</td></tr>\n"));
}

#[test]
fn html_with_no_rows_still_closes_the_document() {
    let output = html(&columns(), &[]);
    assert!(output.ends_with("<tbody>\n</tbody>\n</table>\n</body>\n</html>\n"));
}

#[test]
fn markdown_writes_a_table_with_header_separator_and_rows() {
    assert_eq!(
        markdown(&columns(), &rows()),
        "| id | note |\n| --- | --- |\n| 1 | a &lt;b&gt; &amp; \"c\" |\n| 2 |  |\n"
    );
}

#[test]
fn markdown_keeps_pipes_and_line_breaks_inside_the_cell() {
    let rows = vec![vec![Value::Int(1), Value::Text("x | y\nz\\w".to_string())]];
    assert_eq!(
        markdown(&columns(), &rows),
        "| id | note |\n| --- | --- |\n| 1 | x \\| y<br>z\\\\w |\n"
    );
}

#[test]
fn markdown_escapes_html_entities_and_formatting_so_the_text_renders_as_stored() {
    let rows = vec![vec![
        Value::Int(1),
        Value::Text("<b>t</b> &copy; **x** _y_ `z` ~w~ [l](u)\r\nnext".to_string()),
    ]];
    assert_eq!(
        markdown(&columns(), &rows),
        "| id | note |\n| --- | --- |\n\
| 1 | &lt;b&gt;t&lt;/b&gt; &amp;copy; \\*\\*x\\*\\* \\_y\\_ \\`z\\` \\~w\\~ \\[l\\](u)<br>next |\n"
    );
}

#[test]
fn markdown_escapes_column_names_too() {
    let columns = vec![column("a|b"), column("*c*")];
    let output = markdown(&columns, &[]);
    assert!(output.starts_with("| a\\|b | \\*c\\* |\n"), "{output}");
}

#[test]
fn xml_writes_tab_line_feed_and_carriage_return_as_references() {
    let columns = vec![column("a\nb"), column("a b"), column("c")];
    let rows = vec![vec![
        Value::Int(1),
        Value::Int(2),
        Value::Text("x\r\ny\tz".to_string()),
    ]];
    let output = xml(&columns, &rows);
    assert!(
        output.contains("<field name=\"a&#10;b\">1</field>"),
        "{output}"
    );
    assert!(output.contains("<field name=\"a b\">2</field>"), "{output}");
    assert!(
        output.contains("<field name=\"c\">x&#13;&#10;y&#9;z</field>"),
        "{output}"
    );
}

#[test]
fn xml_writes_rows_with_named_fields_and_marks_null() {
    assert_eq!(
        xml(&columns(), &rows()),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rows>\n\
<row><field name=\"id\">1</field><field name=\"note\">a &lt;b&gt; &amp; &quot;c&quot;</field></row>\n\
<row><field name=\"id\">2</field><field name=\"note\" null=\"true\"/></row>\n\
</rows>\n"
    );
}

#[test]
fn xml_keeps_an_empty_string_distinct_from_null() {
    let rows = vec![vec![Value::Int(1), Value::Text(String::new())]];
    assert!(xml(&columns(), &rows).contains("<field name=\"note\"></field>"));
}

#[test]
fn xml_escapes_column_names_and_replaces_characters_xml_cannot_hold() {
    let columns = vec![column("a\"b"), column("c")];
    let rows = vec![vec![Value::Int(1), Value::Text("bell\u{7}".to_string())]];
    let output = xml(&columns, &rows);
    assert!(output.contains("<field name=\"a&quot;b\">1</field>"));
    assert!(output.contains("<field name=\"c\">bell\u{FFFD}</field>"));
}

#[test]
fn xml_with_no_rows_is_an_empty_rows_element() {
    assert_eq!(
        xml(&columns(), &[]),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rows>\n</rows>\n"
    );
}
