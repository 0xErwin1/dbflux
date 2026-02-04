use crate::{ExportError, Exporter};
use csv::Writer;
use dbflux_core::{QueryResult, Value};
use std::io::Write;

pub struct CsvExporter;

impl Exporter for CsvExporter {
    fn name(&self) -> &'static str {
        "CSV"
    }

    fn extension(&self) -> &'static str {
        "csv"
    }

    fn export(&self, result: &QueryResult, writer: &mut dyn Write) -> Result<(), ExportError> {
        let mut csv_writer = Writer::from_writer(writer);

        let headers: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        csv_writer.write_record(&headers)?;

        for row in &result.rows {
            for value in row.iter() {
                let field = value_to_csv_field(value);
                csv_writer.write_field(&field)?;
            }
            csv_writer.write_record(None::<&[u8]>)?;
        }

        csv_writer.flush()?;
        Ok(())
    }
}

fn value_to_csv_field(value: &Value) -> String {
    match value {
        Value::Null => "\\N".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_nan() {
                "NaN".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "Infinity".to_string()
                } else {
                    "-Infinity".to_string()
                }
            } else {
                f.to_string()
            }
        }
        Value::Text(s) | Value::Json(s) | Value::Decimal(s) => s.clone(),
        Value::Bytes(b) => format!("\\x{}", hex::encode(b)),
        Value::DateTime(dt) => dt.to_rfc3339(),
        Value::Date(d) => d.format("%Y-%m-%d").to_string(),
        Value::Time(t) => t.format("%H:%M:%S%.f").to_string(),
        Value::ObjectId(id) => id.clone(),
        Value::Array(arr) => serde_json::to_string(arr).unwrap_or_else(|_| "[]".to_string()),
        Value::Document(doc) => serde_json::to_string(doc).unwrap_or_else(|_| "{}".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::ColumnMeta;
    use std::time::Duration;

    fn make_result(columns: Vec<&str>, rows: Vec<Vec<Value>>) -> QueryResult {
        QueryResult {
            columns: columns
                .into_iter()
                .map(|name| ColumnMeta {
                    name: name.to_string(),
                    type_name: "text".to_string(),
                    nullable: true,
                })
                .collect(),
            rows,
            affected_rows: None,
            execution_time: Duration::from_millis(10),
            is_document_result: false,
        }
    }

    #[test]
    fn exports_simple_data() {
        let result = make_result(
            vec!["id", "name"],
            vec![
                vec![Value::Int(1), Value::Text("Alice".to_string())],
                vec![Value::Int(2), Value::Text("Bob".to_string())],
            ],
        );

        let mut buf = Vec::new();
        CsvExporter.export(&result, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("id,name"));
        assert!(output.contains("1,Alice"));
        assert!(output.contains("2,Bob"));
    }

    #[test]
    fn handles_commas_and_quotes() {
        let result = make_result(
            vec!["text"],
            vec![
                vec![Value::Text("hello, world".to_string())],
                vec![Value::Text("say \"hello\"".to_string())],
            ],
        );

        let mut buf = Vec::new();
        CsvExporter.export(&result, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("\"hello, world\""));
        assert!(output.contains("\"say \"\"hello\"\"\""));
    }

    #[test]
    fn handles_newlines() {
        let result = make_result(
            vec!["text"],
            vec![vec![Value::Text("line1\nline2".to_string())]],
        );

        let mut buf = Vec::new();
        CsvExporter.export(&result, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("\"line1\nline2\""));
    }

    #[test]
    fn null_exports_as_backslash_n() {
        let result = make_result(vec!["value"], vec![vec![Value::Null]]);

        let mut buf = Vec::new();
        CsvExporter.export(&result, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("\\N"));
    }

    #[test]
    fn empty_string_is_distinct_from_null() {
        let result = make_result(
            vec!["null_col", "empty_col"],
            vec![vec![Value::Null, Value::Text(String::new())]],
        );

        let mut buf = Vec::new();
        CsvExporter.export(&result, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[1].starts_with("\\N,"));
    }

    #[test]
    fn handles_all_value_types() {
        let result = make_result(
            vec!["bool", "int", "float", "text", "bytes"],
            vec![vec![
                Value::Bool(true),
                Value::Int(42),
                Value::Float(3.14),
                Value::Text("hello".to_string()),
                Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ]],
        );

        let mut buf = Vec::new();
        CsvExporter.export(&result, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("true"));
        assert!(output.contains("42"));
        assert!(output.contains("3.14"));
        assert!(output.contains("hello"));
        assert!(output.contains("\\xdeadbeef"));
    }

    #[test]
    fn nan_exports_as_nan_string() {
        let result = make_result(vec!["value"], vec![vec![Value::Float(f64::NAN)]]);

        let mut buf = Vec::new();
        CsvExporter.export(&result, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("NaN"));
    }

    #[test]
    fn handles_infinity() {
        let result = make_result(
            vec!["pos_inf", "neg_inf"],
            vec![vec![
                Value::Float(f64::INFINITY),
                Value::Float(f64::NEG_INFINITY),
            ]],
        );

        let mut buf = Vec::new();
        CsvExporter.export(&result, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("Infinity"));
        assert!(output.contains("-Infinity"));
    }

    #[test]
    fn handles_empty_result() {
        let result = make_result(vec!["id", "name"], vec![]);

        let mut buf = Vec::new();
        CsvExporter.export(&result, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output.trim(), "id,name");
    }

    #[test]
    fn large_binary_exports_efficiently() {
        let large_blob = vec![0xAB; 10000];
        let result = make_result(vec!["data"], vec![vec![Value::Bytes(large_blob)]]);

        let mut buf = Vec::new();
        CsvExporter.export(&result, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("\\x"));
        assert!(output.contains(&"ab".repeat(10000)));
    }
}
