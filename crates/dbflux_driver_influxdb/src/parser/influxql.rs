//! Parser for InfluxDB's InfluxQL JSON response format.
//!
//! InfluxDB v1 (and the v2 compatibility layer) returns results as:
//! ```json
//! {
//!   "results": [
//!     {
//!       "statement_id": 0,
//!       "series": [
//!         {
//!           "name": "cpu",
//!           "columns": ["time", "value", "host"],
//!           "values": [[...], [...]]
//!         }
//!       ]
//!     }
//!   ]
//! }
//! ```
//!
//! Multi-statement results: when multiple `results[]` entries are present (e.g.
//! `SHOW MEASUREMENTS; SHOW SERIES`), all rows are concatenated into a single
//! `QueryResult`. A synthesized `statement_index` column (integer) is prepended
//! so the caller can distinguish rows from different statements. When only one
//! statement is present the column is omitted to avoid cluttering the output.

use dbflux_core::{ColumnMeta, Value};
use serde_json::Value as Json;

use super::{ParseError, build_query_result, infer_column_type};
use dbflux_core::QueryResult;

const SERIES_COLUMN: &str = "_series";
const STATEMENT_INDEX_COLUMN: &str = "statement_index";

/// Parse the JSON body of an InfluxQL response into a `QueryResult`.
///
/// When multiple semicolon-separated statements are present all rows are
/// concatenated. A synthetic `statement_index` integer column is prepended only
/// when there is more than one non-empty statement so single-statement output
/// looks the same as before.
///
/// Within each statement, multiple series are flattened and a `_series` column
/// is prepended when more than one series is present.
pub fn parse_influxql_json(body: &str) -> Result<QueryResult, ParseError> {
    let root: Json =
        serde_json::from_str(body).map_err(|e| ParseError::Malformed(e.to_string()))?;

    let results = root
        .get("results")
        .and_then(|v| v.as_array())
        .ok_or_else(|| ParseError::Malformed("missing 'results' array".into()))?;

    if results.is_empty() {
        return Err(ParseError::Malformed("empty 'results' array".into()));
    }

    // Check for a statement-level error in any result before doing any work.
    // We surface the first error encountered — InfluxQL stops execution on error.
    for result in results.iter() {
        if let Some(err_msg) = result.get("error").and_then(|v| v.as_str()) {
            return Err(ParseError::QueryError(err_msg.to_string()));
        }
    }

    // Collect only statements that carry data (non-empty series arrays).
    let non_empty: Vec<(usize, &[Json])> = results
        .iter()
        .enumerate()
        .filter_map(|(idx, result)| {
            let series = result.get("series").and_then(|v| v.as_array())?;
            if series.is_empty() {
                None
            } else {
                Some((idx, series.as_slice()))
            }
        })
        .collect();

    if non_empty.is_empty() {
        return Ok(QueryResult::empty());
    }

    let multi_statement = non_empty.len() > 1;

    let mut all_columns: Vec<ColumnMeta> = Vec::new();
    let mut all_rows: Vec<Vec<Value>> = Vec::new();
    let mut columns_initialized = false;

    for (statement_idx, series) in non_empty {
        let multi_series = series.len() > 1;

        for serie in series {
            let series_name = serie
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let col_names: Vec<String> = serie
                .get("columns")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            let values_array = serie
                .get("values")
                .and_then(|v| v.as_array())
                .map(|a| a.as_slice())
                .unwrap_or(&[]);

            // Infer types from the first non-null row.
            let type_names = infer_type_names(&col_names, values_array);

            // Column layout is fixed by the first statement we encounter.
            // Subsequent statements must have a compatible shape (same field names);
            // mismatched shapes will produce misaligned rows. This is a known
            // limitation of concatenating heterogeneous InfluxQL results.
            if !columns_initialized {
                if multi_statement {
                    all_columns.push(ColumnMeta {
                        name: STATEMENT_INDEX_COLUMN.to_string(),
                        type_name: "integer".to_string(),
                        nullable: false,
                        is_primary_key: false,
                    });
                }

                if multi_series {
                    all_columns.push(ColumnMeta {
                        name: SERIES_COLUMN.to_string(),
                        type_name: "text".to_string(),
                        nullable: false,
                        is_primary_key: false,
                    });
                }

                for (name, type_name) in col_names.iter().zip(type_names.iter()) {
                    all_columns.push(ColumnMeta {
                        name: name.clone(),
                        type_name: type_name.clone(),
                        nullable: true,
                        is_primary_key: name == "time",
                    });
                }

                columns_initialized = true;
            }

            for row_json in values_array {
                let row_arr = match row_json.as_array() {
                    Some(r) => r,
                    None => continue,
                };

                let mut row: Vec<Value> = Vec::with_capacity(all_columns.len());

                if multi_statement {
                    row.push(Value::Int(statement_idx as i64));
                }

                if multi_series {
                    row.push(Value::Text(series_name.clone()));
                }

                for (idx, val) in row_arr.iter().enumerate() {
                    let type_name = type_names.get(idx).map(|s| s.as_str()).unwrap_or("text");
                    row.push(json_to_value(val, type_name));
                }

                all_rows.push(row);
            }
        }
    }

    Ok(build_query_result(all_columns, all_rows))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Infer column type names from the first non-null data row.
fn infer_type_names(col_names: &[String], values: &[Json]) -> Vec<String> {
    let first_row = values.iter().find_map(|row| row.as_array());

    col_names
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            if name == "time" {
                return "timestamp_ms".to_string();
            }

            let sample = first_row
                .and_then(|row| row.get(idx))
                .and_then(|v| v.as_str());

            match sample {
                Some(s) => infer_column_type(s).to_string(),
                None => {
                    // Check numeric
                    let num = first_row.and_then(|row| row.get(idx));
                    match num {
                        Some(Json::Number(n)) => {
                            if n.is_f64() {
                                "float".to_string()
                            } else {
                                "integer".to_string()
                            }
                        }
                        Some(Json::Bool(_)) => "boolean".to_string(),
                        _ => "text".to_string(),
                    }
                }
            }
        })
        .collect()
}

/// Convert a JSON value to a `Value` using the inferred type.
fn json_to_value(json: &Json, type_name: &str) -> Value {
    match json {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Bool(*b),
        Json::Number(n) => {
            if type_name == "float" {
                n.as_f64()
                    .map(Value::Float)
                    .unwrap_or_else(|| Value::Int(n.as_i64().unwrap_or(0)))
            } else {
                n.as_i64()
                    .map(Value::Int)
                    .unwrap_or_else(|| n.as_f64().map(Value::Float).unwrap_or(Value::Null))
            }
        }
        Json::String(s) => {
            if type_name == "timestamp_ms" {
                // InfluxDB returns ms timestamps as integers in JSON when epoch=ms.
                s.parse::<i64>()
                    .map(Value::Int)
                    .unwrap_or_else(|_| Value::Text(s.clone()))
            } else {
                Value::Text(s.clone())
            }
        }
        _ => Value::Text(json.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Tests (C.3.1 – C.3.6)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // C.3.1 — single series
    #[test]
    fn single_series_parses_columns_and_rows() {
        let body = r#"{
            "results": [{
                "statement_id": 0,
                "series": [{
                    "name": "cpu",
                    "columns": ["time", "value", "host"],
                    "values": [
                        [1704067200000, 0.5, "server1"],
                        [1704067260000, 0.7, "server1"]
                    ]
                }]
            }]
        }"#;

        let result = parse_influxql_json(body).expect("parse must succeed");
        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0].name, "time");
        assert_eq!(result.columns[0].is_primary_key, true);
        assert_eq!(result.rows.len(), 2);
    }

    // C.3.2 — multiple series
    #[test]
    fn multiple_series_flattened_with_series_column() {
        let body = r#"{
            "results": [{
                "statement_id": 0,
                "series": [
                    {
                        "name": "cpu",
                        "columns": ["time", "value"],
                        "values": [[1000, 0.5]]
                    },
                    {
                        "name": "mem",
                        "columns": ["time", "value"],
                        "values": [[2000, 0.8]]
                    }
                ]
            }]
        }"#;

        let result = parse_influxql_json(body).expect("parse must succeed");
        // First column is synthesized _series
        assert_eq!(result.columns[0].name, "_series");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Text("cpu".into()));
        assert_eq!(result.rows[1][0], Value::Text("mem".into()));
    }

    // C.3.3 — empty series array
    #[test]
    fn empty_series_returns_empty_result() {
        let body = r#"{"results": [{"statement_id": 0}]}"#;
        let result = parse_influxql_json(body).expect("parse must succeed");
        assert!(result.rows.is_empty());
    }

    // C.3.4 — result-level error
    #[test]
    fn result_level_error_returns_query_error() {
        let body = r#"{"results": [{"error": "field type conflict: input field \"value\" on measurement \"cpu\" is type float, already exists as type integer"}]}"#;
        match parse_influxql_json(body) {
            Err(ParseError::QueryError(msg)) => {
                assert!(msg.contains("field type conflict"));
            }
            other => panic!("expected QueryError, got: {:?}", other),
        }
    }

    // C.3.5 — malformed JSON
    #[test]
    fn malformed_json_returns_malformed_error() {
        let body = "{not valid json}";
        match parse_influxql_json(body) {
            Err(ParseError::Malformed(_)) => {}
            other => panic!("expected Malformed, got: {:?}", other),
        }
    }

    // C.3.6 — multi-statement: all statements are concatenated with a statement_index column
    #[test]
    fn multi_statement_returns_all_rows_with_statement_index() {
        let body = r#"{
            "results": [
                {
                    "statement_id": 0,
                    "series": [{"name": "cpu", "columns": ["time"], "values": [[1000]]}]
                },
                {
                    "statement_id": 1,
                    "series": [{"name": "mem", "columns": ["time"], "values": [[2000], [3000]]}]
                }
            ]
        }"#;

        let result = parse_influxql_json(body).expect("parse must succeed");

        // All rows from both statements must be present.
        assert_eq!(result.rows.len(), 3, "rows from all statements must be included");

        // First column must be the synthetic statement_index.
        assert_eq!(
            result.columns[0].name, STATEMENT_INDEX_COLUMN,
            "first column must be statement_index"
        );

        // Rows from statement 0 carry index 0.
        assert_eq!(result.rows[0][0], Value::Int(0), "first row from statement 0");

        // Rows from statement 1 carry index 1.
        assert_eq!(result.rows[1][0], Value::Int(1), "first row from statement 1");
        assert_eq!(result.rows[2][0], Value::Int(1), "second row from statement 1");
    }

    // C.3.7 — multi-statement: empty first statement does not affect output of second
    #[test]
    fn multi_statement_empty_first_statement_skipped() {
        let body = r#"{
            "results": [
                {
                    "statement_id": 0
                },
                {
                    "statement_id": 1,
                    "series": [{"name": "cpu", "columns": ["time", "value"], "values": [[1000, 0.5]]}]
                }
            ]
        }"#;

        let result = parse_influxql_json(body).expect("parse must succeed");

        // Only one non-empty statement, so no statement_index column.
        assert_eq!(result.rows.len(), 1, "only the non-empty statement contributes rows");
        assert_ne!(
            result.columns.first().map(|c| c.name.as_str()),
            Some(STATEMENT_INDEX_COLUMN),
            "statement_index must not appear for a single non-empty statement"
        );
    }

    // C.3.8 — multi-statement: statement-level error in any result is surfaced
    #[test]
    fn multi_statement_error_in_second_statement_is_surfaced() {
        let body = r#"{
            "results": [
                {
                    "statement_id": 0,
                    "series": [{"name": "cpu", "columns": ["time"], "values": [[1000]]}]
                },
                {
                    "statement_id": 1,
                    "error": "unknown measurement"
                }
            ]
        }"#;

        match parse_influxql_json(body) {
            Err(ParseError::QueryError(msg)) => {
                assert!(msg.contains("unknown measurement"), "error message must be forwarded: {msg}");
            }
            other => panic!("expected QueryError, got: {other:?}"),
        }
    }
}
