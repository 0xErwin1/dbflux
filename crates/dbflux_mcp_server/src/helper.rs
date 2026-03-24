use base64::Engine;
use dbflux_core::{QueryResult, SqlDialect, Value};
use rmcp::ErrorData;

// Note: These helper functions are used by code generated from the #[tool] macro.
// Clippy cannot detect this usage, so we suppress dead_code warnings.
#[allow(dead_code)]
pub fn json_filter_to_sql(
    filter: &serde_json::Value,
    dialect: &dyn SqlDialect,
) -> Result<String, String> {
    match filter {
        serde_json::Value::Object(map) => {
            if map.is_empty() {
                return Ok("".to_string());
            }

            let mut conditions = Vec::new();
            for (key, value) in map.iter() {
                let condition = parse_condition(key, value, dialect)?;
                conditions.push(condition);
            }

            Ok(conditions.join(" AND "))
        }
        serde_json::Value::Null => Ok("".to_string()),
        serde_json::Value::String(s) => {
            if s.trim().starts_with('{') {
                match serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(s) {
                    Ok(map) => json_filter_to_sql(&serde_json::Value::Object(map), dialect),
                    Err(_) => Err(format!(
                        "Filter must be a JSON object. Received a string that looks like JSON but failed to parse: {}",
                        s
                    )),
                }
            } else {
                Err(format!(
                    "Filter must be a JSON object, not a string. Received: {:?}. \
                     If you intended to pass a JSON object, do not wrap it in quotes.",
                    s
                ))
            }
        }
        serde_json::Value::Array(_) => Err("Filter must be a JSON object, not an array. \
             Use an object with column conditions instead. \
             Example: {{\"column_name\": \"value\"}} or {{\"id\": {{\">\": 10}}}}"
            .to_string()),
        serde_json::Value::Bool(b) => Err(format!(
            "Filter must be a JSON object, not a boolean. Received: {}. \
             Use an object with column conditions instead. \
             Example: {{\"column_name\": \"value\"}}",
            b
        )),
        serde_json::Value::Number(n) => Err(format!(
            "Filter must be a JSON object, not a number. Received: {}. \
             Use an object with column conditions instead. \
             Example: {{\"column_name\": \"value\"}}",
            n
        )),
    }
}

#[allow(dead_code)]
pub fn parse_condition(
    key: &str,
    value: &serde_json::Value,
    dialect: &dyn SqlDialect,
) -> Result<String, String> {
    let quoted_key = dialect.quote_identifier(key);

    match value {
        serde_json::Value::Object(op_map) => {
            // Operator-based condition: {"column": {">": 5}}
            let mut conditions = Vec::new();
            for (op, val) in op_map.iter() {
                let cond = match op.as_str() {
                    "=" | "==" => format!("{} = {}", quoted_key, json_to_sql_literal(val, dialect)),
                    "!=" | "<>" => {
                        format!("{} != {}", quoted_key, json_to_sql_literal(val, dialect))
                    }
                    ">" => format!("{} > {}", quoted_key, json_to_sql_literal(val, dialect)),
                    ">=" => format!("{} >= {}", quoted_key, json_to_sql_literal(val, dialect)),
                    "<" => format!("{} < {}", quoted_key, json_to_sql_literal(val, dialect)),
                    "<=" => format!("{} <= {}", quoted_key, json_to_sql_literal(val, dialect)),
                    "like" | "LIKE" => {
                        format!("{} LIKE {}", quoted_key, json_to_sql_literal(val, dialect))
                    }
                    "in" | "IN" => {
                        let arr = val
                            .as_array()
                            .ok_or_else(|| format!("IN requires an array for column {}", key))?;
                        let items: Vec<String> = arr
                            .iter()
                            .map(|v| json_to_sql_literal(v, dialect))
                            .collect();
                        format!("{} IN ({})", quoted_key, items.join(", "))
                    }
                    "between" | "BETWEEN" => {
                        let arr = val.as_array().ok_or_else(|| {
                            format!("BETWEEN requires an array [min, max] for column {}", key)
                        })?;
                        if arr.len() != 2 {
                            return Err(format!(
                                "BETWEEN requires exactly 2 values for column {}",
                                key
                            ));
                        }
                        format!(
                            "{} BETWEEN {} AND {}",
                            quoted_key,
                            json_to_sql_literal(&arr[0], dialect),
                            json_to_sql_literal(&arr[1], dialect)
                        )
                    }
                    "is_null" | "IS_NULL" => {
                        if val.as_bool().unwrap_or(false) {
                            format!("{} IS NULL", quoted_key)
                        } else {
                            format!("{} IS NOT NULL", quoted_key)
                        }
                    }
                    "and" | "AND" => {
                        let sub = json_filter_to_sql(val, dialect)?;
                        format!("({})", sub)
                    }
                    "or" | "OR" => {
                        let sub = json_filter_to_sql(val, dialect)?;
                        format!("({})", sub.replace(" AND ", " OR "))
                    }
                    _ => return Err(format!("Unknown operator: {}", op)),
                };
                conditions.push(cond);
            }
            Ok(conditions.join(" AND "))
        }
        // Simple equality condition: {"column": "value"}
        _ => Ok(format!(
            "{} = {}",
            quoted_key,
            json_to_sql_literal(value, dialect)
        )),
    }
}

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

#[allow(dead_code)]
#[allow(clippy::only_used_in_recursion)]
pub fn db_value_to_sql(value: &Value, dialect: &dyn SqlDialect) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s)
        | Value::Json(s)
        | Value::Decimal(s)
        | Value::ObjectId(s)
        | Value::Unsupported(s) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        Value::Bytes(b) => format!("'{}'", base64::engine::general_purpose::STANDARD.encode(b)),
        Value::DateTime(dt) => format!("'{}'", dt.to_rfc3339()),
        Value::Date(d) => format!("'{}'", d),
        Value::Time(t) => format!("'{}'", t),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(|v| db_value_to_sql(v, dialect)).collect();
            format!("ARRAY[{}]", items.join(", "))
        }
        Value::Document(doc) => {
            let json_str = serde_json::to_string(doc).unwrap_or_default();
            format!("'{}'", json_str.replace('\'', "''"))
        }
    }
}

/// Serialize a QueryResult into a JSON value suitable for MCP responses
#[allow(dead_code)]
pub fn serialize_query_result(result: &QueryResult) -> serde_json::Value {
    let columns: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();

    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (col, cell) in columns.iter().zip(row.iter()) {
                obj.insert((*col).to_string(), value_to_json(cell));
            }
            serde_json::Value::Object(obj)
        })
        .collect();

    serde_json::json!({
        "columns": columns,
        "rows": rows,
        "row_count": result.rows.len(),
    })
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
mod filter_validation_tests {
    use super::*;
    use dbflux_core::DefaultSqlDialect;

    #[test]
    fn test_valid_object_filter() {
        let dialect = DefaultSqlDialect;
        let filter = serde_json::json!({
            "name": "Alice",
            "age": {">=": 18}
        });

        let result = json_filter_to_sql(&filter, &dialect);
        assert!(result.is_ok(), "Valid object filter should succeed");

        let sql = result.unwrap();
        assert!(sql.contains("name"));
        assert!(sql.contains("Alice"));
        assert!(sql.contains("age"));
        assert!(sql.contains(">="));
        assert!(sql.contains("18"));
    }

    #[test]
    fn test_string_encoded_json_recovery() {
        let dialect = DefaultSqlDialect;
        let filter = serde_json::json!(r#"{"status": "active"}"#);

        let result = json_filter_to_sql(&filter, &dialect);
        assert!(
            result.is_ok(),
            "String containing valid JSON object should be recovered"
        );

        let sql = result.unwrap();
        assert!(sql.contains("status"));
        assert!(sql.contains("active"));
    }

    #[test]
    fn test_array_filter_error() {
        let dialect = DefaultSqlDialect;
        let filter = serde_json::json!(["id", "name", "age"]);

        let result = json_filter_to_sql(&filter, &dialect);
        assert!(result.is_err(), "Array filter should produce error");

        let error = result.unwrap_err();
        assert!(
            error.contains("array"),
            "Error message should mention 'array'"
        );
        assert!(
            error.contains("object"),
            "Error message should mention 'object'"
        );
        assert!(
            error.contains("Example"),
            "Error message should include example"
        );
    }

    #[test]
    fn test_boolean_filter_error() {
        let dialect = DefaultSqlDialect;
        let filter = serde_json::json!(true);

        let result = json_filter_to_sql(&filter, &dialect);
        assert!(result.is_err(), "Boolean filter should produce error");

        let error = result.unwrap_err();
        assert!(
            error.contains("boolean"),
            "Error message should mention 'boolean'"
        );
        assert!(
            error.contains("true"),
            "Error message should show the actual boolean value"
        );
        assert!(
            error.contains("Example"),
            "Error message should include example"
        );
    }

    #[test]
    fn test_number_filter_error() {
        let dialect = DefaultSqlDialect;
        let filter = serde_json::json!(42);

        let result = json_filter_to_sql(&filter, &dialect);
        assert!(result.is_err(), "Number filter should produce error");

        let error = result.unwrap_err();
        assert!(
            error.contains("number"),
            "Error message should mention 'number'"
        );
        assert!(
            error.contains("42"),
            "Error message should show the actual number"
        );
        assert!(
            error.contains("Example"),
            "Error message should include example"
        );
    }

    #[test]
    fn test_plain_string_filter_error() {
        let dialect = DefaultSqlDialect;
        let filter = serde_json::json!("just a string");

        let result = json_filter_to_sql(&filter, &dialect);
        assert!(result.is_err(), "Plain string filter should produce error");

        let error = result.unwrap_err();
        assert!(
            error.contains("string"),
            "Error message should mention 'string'"
        );
        assert!(
            error.contains("just a string"),
            "Error message should show the actual string"
        );
        assert!(
            error.contains("do not wrap it in quotes"),
            "Error message should warn about quotes"
        );
    }

    #[test]
    fn test_malformed_json_string_error() {
        let dialect = DefaultSqlDialect;
        let filter = serde_json::json!(r#"{"status": invalid}"#);

        let result = json_filter_to_sql(&filter, &dialect);
        assert!(
            result.is_err(),
            "Malformed JSON string should produce error"
        );

        let error = result.unwrap_err();
        assert!(
            error.contains("looks like JSON but failed to parse"),
            "Error message should explain the parse failure"
        );
    }
}
