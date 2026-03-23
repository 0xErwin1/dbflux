use base64::Engine;
use dbflux_core::{QueryResult, SqlDialect, Value};
use rmcp::ErrorData;

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
        _ => Err("Filter must be a JSON object".to_string()),
    }
}

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
pub(crate) trait IntoErrorData {
    fn into_error_data(self) -> ErrorData;
}

impl IntoErrorData for String {
    fn into_error_data(self) -> ErrorData {
        ErrorData::internal_error(self, None)
    }
}
