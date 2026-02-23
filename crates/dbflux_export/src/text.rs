use crate::ExportError;
use dbflux_core::{QueryResult, QueryResultShape};
use std::io::Write;

pub struct TextExporter;

impl TextExporter {
    pub fn export(&self, result: &QueryResult, writer: &mut dyn Write) -> Result<(), ExportError> {
        match &result.shape {
            QueryResultShape::Text => {
                if let Some(body) = &result.text_body {
                    writer.write_all(body.as_bytes())?;
                }
            }

            QueryResultShape::Table | QueryResultShape::Json => {
                write_rows_as_text(result, writer)?;
            }

            QueryResultShape::Binary => {
                if let Some(bytes) = &result.raw_bytes {
                    writer.write_all(bytes)?;
                }
            }
        }

        Ok(())
    }
}

fn write_rows_as_text(result: &QueryResult, writer: &mut dyn Write) -> Result<(), ExportError> {
    if !result.columns.is_empty() {
        let header: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        writeln!(writer, "{}", header.join("\t"))?;
    }

    for row in &result.rows {
        let fields: Vec<String> = row.iter().map(|v| v.as_display_string()).collect();
        writeln!(writer, "{}", fields.join("\t"))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{ColumnMeta, Value};
    use std::time::Duration;

    #[test]
    fn exports_text_body() {
        let result = QueryResult::text("hello world".to_string(), Duration::from_millis(1));

        let mut buf = Vec::new();
        TextExporter.export(&result, &mut buf).unwrap();

        assert_eq!(String::from_utf8(buf).unwrap(), "hello world");
    }

    #[test]
    fn exports_table_as_tsv() {
        let result = QueryResult::table(
            vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_name: "int".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_name: "text".to_string(),
                    nullable: true,
                },
            ],
            vec![
                vec![Value::Int(1), Value::Text("Alice".to_string())],
                vec![Value::Int(2), Value::Null],
            ],
            None,
            Duration::from_millis(5),
        );

        let mut buf = Vec::new();
        TextExporter.export(&result, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines[0], "id\tname");
        assert_eq!(lines[1], "1\tAlice");
        assert_eq!(lines[2], "2\tNULL");
    }

    #[test]
    fn exports_binary_as_raw() {
        let result = QueryResult::binary(vec![0xDE, 0xAD], Duration::from_millis(1));

        let mut buf = Vec::new();
        TextExporter.export(&result, &mut buf).unwrap();

        assert_eq!(buf, vec![0xDE, 0xAD]);
    }

    #[test]
    fn exports_empty_table() {
        let result = QueryResult::table(
            vec![ColumnMeta {
                name: "x".to_string(),
                type_name: "int".to_string(),
                nullable: false,
            }],
            vec![],
            None,
            Duration::from_millis(1),
        );

        let mut buf = Vec::new();
        TextExporter.export(&result, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output.trim(), "x");
    }
}
