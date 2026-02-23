use crate::ExportError;
use dbflux_core::{ColumnMeta, QueryResult, QueryResultShape, Row, Value};
use std::io::Write;

pub struct JsonExporter {
    pub pretty: bool,
}

impl JsonExporter {
    pub fn export(&self, result: &QueryResult, writer: &mut dyn Write) -> Result<(), ExportError> {
        let json_value = match &result.shape {
            QueryResultShape::Table | QueryResultShape::Json => {
                rows_to_json_array(&result.columns, &result.rows)
            }

            QueryResultShape::Text => {
                serde_json::Value::String(result.text_body.clone().unwrap_or_default())
            }

            QueryResultShape::Binary => {
                use base64::Engine;
                let encoded = result
                    .raw_bytes
                    .as_deref()
                    .map(|b| base64::engine::general_purpose::STANDARD.encode(b))
                    .unwrap_or_default();
                serde_json::json!({ "data": encoded })
            }
        };

        if self.pretty {
            serde_json::to_writer_pretty(writer, &json_value)?;
        } else {
            serde_json::to_writer(writer, &json_value)?;
        }

        Ok(())
    }
}

fn rows_to_json_array(columns: &[ColumnMeta], rows: &[Row]) -> serde_json::Value {
    serde_json::Value::Array(
        rows.iter()
            .map(|row| row_to_json_object(columns, row))
            .collect(),
    )
}

fn row_to_json_object(columns: &[ColumnMeta], row: &Row) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    for (col, value) in columns.iter().zip(row.iter()) {
        map.insert(col.name.clone(), Value::to_serde_json(value));
    }

    serde_json::Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::ColumnMeta;
    use std::time::Duration;

    fn make_table(columns: Vec<&str>, rows: Vec<Vec<Value>>) -> QueryResult {
        QueryResult::table(
            columns
                .into_iter()
                .map(|name| ColumnMeta {
                    name: name.to_string(),
                    type_name: "text".to_string(),
                    nullable: true,
                })
                .collect(),
            rows,
            None,
            Duration::from_millis(10),
        )
    }

    #[test]
    fn exports_table_as_json_array() {
        let result = make_table(
            vec!["id", "name"],
            vec![
                vec![Value::Int(1), Value::Text("Alice".to_string())],
                vec![Value::Int(2), Value::Text("Bob".to_string())],
            ],
        );

        let mut buf = Vec::new();
        JsonExporter { pretty: false }
            .export(&result, &mut buf)
            .unwrap();

        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["id"], 1);
        assert_eq!(arr[0]["name"], "Alice");
        assert_eq!(arr[1]["name"], "Bob");
    }

    #[test]
    fn exports_json_shape_preserves_documents() {
        let result = QueryResult::json(
            vec![ColumnMeta {
                name: "_id".to_string(),
                type_name: "ObjectId".to_string(),
                nullable: false,
            }],
            vec![vec![Value::ObjectId(
                "507f1f77bcf86cd799439011".to_string(),
            )]],
            Duration::from_millis(5),
        );

        let mut buf = Vec::new();
        JsonExporter { pretty: false }
            .export(&result, &mut buf)
            .unwrap();

        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr[0]["_id"]["$oid"], "507f1f77bcf86cd799439011");
    }

    #[test]
    fn exports_text_as_json_string() {
        let result = QueryResult::text("OK".to_string(), Duration::from_millis(1));

        let mut buf = Vec::new();
        JsonExporter { pretty: false }
            .export(&result, &mut buf)
            .unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output, "\"OK\"");
    }

    #[test]
    fn pretty_output_contains_newlines() {
        let result = make_table(vec!["x"], vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

        let mut buf = Vec::new();
        JsonExporter { pretty: true }
            .export(&result, &mut buf)
            .unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains('\n'));
        assert!(output.contains("  ")); // indentation
    }

    #[test]
    fn handles_empty_result() {
        let result = make_table(vec!["id"], vec![]);

        let mut buf = Vec::new();
        JsonExporter { pretty: false }
            .export(&result, &mut buf)
            .unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output, "[]");
    }

    #[test]
    fn exports_nested_document_values() {
        use std::collections::BTreeMap;

        let mut doc = BTreeMap::new();
        doc.insert("city".to_string(), Value::Text("NYC".to_string()));
        doc.insert("zip".to_string(), Value::Int(10001));

        let result = make_table(
            vec!["name", "address"],
            vec![vec![Value::Text("Alice".to_string()), Value::Document(doc)]],
        );

        let mut buf = Vec::new();
        JsonExporter { pretty: false }
            .export(&result, &mut buf)
            .unwrap();

        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed[0]["address"]["city"], "NYC");
        assert_eq!(parsed[0]["address"]["zip"], 10001);
    }
}
