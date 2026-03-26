use dbflux_core::{QueryResult, SqlDialect, Value};
use rmcp::ErrorData;

#[allow(dead_code)]
pub fn json_to_sql_literal(value: &serde_json::Value, _dialect: &dyn SqlDialect) -> String {
    match value {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr
                .iter()
                .map(|v| json_to_sql_literal(v, _dialect))
                .collect();
            format!("({})", items.join(", "))
        }
        serde_json::Value::Object(_) => "'{}'".to_string(), // Empty object as literal
    }
}

#[allow(dead_code)]
pub fn json_to_db_value(value: serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::Text(s),
        serde_json::Value::Array(arr) => {
            Value::Array(arr.into_iter().map(json_to_db_value).collect())
        }
        serde_json::Value::Object(obj) => Value::Document(
            obj.iter()
                .map(|(k, v)| (k.clone(), json_to_db_value(v.clone())))
                .collect::<std::collections::BTreeMap<_, _>>(),
        ),
    }
}

/// Serialize a QueryResult into a JSON value suitable for MCP responses
#[allow(dead_code)]
pub fn serialize_query_result(result: &QueryResult) -> serde_json::Value {
    let columns: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
    let rows = serialize_rows(result);

    serde_json::json!({
        "columns": columns,
        "rows": rows,
        "row_count": result.rows.len(),
    })
}

pub fn serialize_rows(result: &QueryResult) -> Vec<serde_json::Value> {
    let columns: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();

    result
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (col, cell) in columns.iter().zip(row.iter()) {
                obj.insert((*col).to_string(), value_to_json(cell));
            }
            serde_json::Value::Object(obj)
        })
        .collect()
}

pub fn mutation_affected_rows(result: &QueryResult) -> u64 {
    result.affected_rows.unwrap_or(result.rows.len() as u64)
}

pub fn serialize_mutation_result(
    result: &QueryResult,
    affected_key: &str,
    include_records: bool,
) -> serde_json::Value {
    let mut response = serde_json::Map::new();
    response.insert(
        affected_key.to_string(),
        serde_json::json!(mutation_affected_rows(result)),
    );

    if include_records {
        response.insert(
            "records".to_string(),
            serde_json::Value::Array(serialize_rows(result)),
        );
    }

    serde_json::Value::Object(response)
}

#[allow(dead_code)]
pub fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(f.to_string())),
        Value::Text(s)
        | Value::Json(s)
        | Value::Decimal(s)
        | Value::ObjectId(s)
        | Value::Unsupported(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::json!({ "_type": "bytes", "length": b.len() }),
        Value::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        Value::Date(d) => serde_json::Value::String(d.to_string()),
        Value::Time(t) => serde_json::Value::String(t.to_string()),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Document(doc) => {
            let map: serde_json::Map<_, _> = doc
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}

/// Helper trait to convert String errors to ErrorData
#[allow(dead_code)]
pub(crate) trait IntoErrorData {
    fn into_error_data(self) -> ErrorData;
}

impl IntoErrorData for String {
    fn into_error_data(self) -> ErrorData {
        ErrorData::internal_error(self, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{ColumnMeta, QueryResult, Value};
    use std::time::Duration;

    #[test]
    fn mutation_affected_rows_prefers_driver_metadata() {
        let result = QueryResult::table(Vec::new(), Vec::new(), Some(7), Duration::ZERO);

        assert_eq!(mutation_affected_rows(&result), 7);
    }

    #[test]
    fn mutation_affected_rows_falls_back_to_returned_row_count() {
        let result = QueryResult::table(
            vec![ColumnMeta {
                name: "id".into(),
                type_name: "int".into(),
                nullable: false,
                is_primary_key: true,
            }],
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
            None,
            Duration::ZERO,
        );

        assert_eq!(mutation_affected_rows(&result), 2);
    }
}
