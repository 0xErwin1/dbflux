//! A table's rows as documents for the document tree, with their JSON values
//! expanded.

use dbflux_core::{QueryResult, Value};

/// `result` with every JSON cell parsed into nested documents and arrays, so
/// the document tree can expand a `jsonb` value. A cell that does not parse
/// stays as text.
pub(super) fn table_rows_as_documents(result: &QueryResult) -> QueryResult {
    let mut documents = result.clone();

    for row in &mut documents.rows {
        for value in row.iter_mut() {
            if let Value::Json(text) = value
                && let Ok(json) = serde_json::from_str::<serde_json::Value>(text)
            {
                *value = json_to_value(json);
            }
        }
    }

    documents
}

/// Plain JSON into a value: objects become documents, arrays arrays. Unlike
/// the MongoDB reader it gives `$oid`-style keys no meaning.
fn json_to_value(json: serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(boolean) => Value::Bool(boolean),
        serde_json::Value::Number(number) => match number.as_i64() {
            Some(integer) => Value::Int(integer),
            None => number
                .as_f64()
                .map(Value::Float)
                .unwrap_or_else(|| Value::Decimal(number.to_string())),
        },
        serde_json::Value::String(text) => Value::Text(text),
        serde_json::Value::Array(items) => {
            Value::Array(items.into_iter().map(json_to_value).collect())
        }
        serde_json::Value::Object(object) => Value::Document(
            object
                .into_iter()
                .map(|(key, field)| (key, json_to_value(field)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    #[test]
    fn json_cells_become_nested_documents_and_other_cells_stay() {
        let result = QueryResult::table(
            vec![
                dbflux_core::ColumnMeta {
                    name: "id".into(),
                    type_name: "int8".into(),
                    nullable: false,
                    is_primary_key: true,
                    kind: dbflux_core::ColumnKind::Integer,
                },
                dbflux_core::ColumnMeta {
                    name: "payload".into(),
                    type_name: "jsonb".into(),
                    nullable: true,
                    is_primary_key: false,
                    kind: dbflux_core::ColumnKind::Unknown,
                },
            ],
            vec![
                vec![
                    Value::Int(1),
                    Value::Json(r#"{"total": 129.5, "items": [1, "two"], "$oid": "x"}"#.into()),
                ],
                vec![Value::Int(2), Value::Json("not json".into())],
            ],
            None,
            Duration::ZERO,
        );

        let documents = table_rows_as_documents(&result);

        let mut expected = BTreeMap::new();
        expected.insert("$oid".to_string(), Value::Text("x".into()));
        expected.insert(
            "items".to_string(),
            Value::Array(vec![Value::Int(1), Value::Text("two".into())]),
        );
        expected.insert("total".to_string(), Value::Float(129.5));

        assert_eq!(
            documents.rows[0],
            vec![Value::Int(1), Value::Document(expected)]
        );
        assert_eq!(
            documents.rows[1],
            vec![Value::Int(2), Value::Json("not json".into())]
        );
    }
}
