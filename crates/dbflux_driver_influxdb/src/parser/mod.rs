//! Query result parsers for InfluxDB response formats.
//!
//! - `influxql`: parses InfluxDB's JSON response format.
//! - `flux`: parses the annotated CSV format returned by the Flux API.

pub mod flux;
pub mod influxql;

use dbflux_core::{ColumnMeta, QueryResult, Value};
use thiserror::Error;

/// Errors that can occur while parsing InfluxDB responses.
#[derive(Debug, Error)]
pub enum ParseError {
    /// The server returned a query-level error message (e.g. syntax error).
    #[error("InfluxDB query error: {0}")]
    QueryError(String),

    /// The response body is structurally invalid (unexpected JSON / CSV shape).
    #[error("Malformed InfluxDB response: {0}")]
    Malformed(String),
}

/// Infer a `Value` type label from a raw string.
///
/// Used to produce `type_name` strings for `ColumnMeta`.
pub(crate) fn infer_column_type(sample: &str) -> &'static str {
    if sample.parse::<i64>().is_ok() {
        return "integer";
    }
    if sample.parse::<f64>().is_ok() {
        return "float";
    }
    if sample == "true" || sample == "false" {
        return "boolean";
    }
    "text"
}

/// Parse a raw string value into a `Value` using its declared type name.
pub(crate) fn parse_typed_value(raw: &str, type_name: &str) -> Value {
    if raw.is_empty() {
        return Value::Null;
    }

    match type_name {
        "integer" | "unsignedLong" | "long" => raw
            .parse::<i64>()
            .map(Value::Int)
            .unwrap_or_else(|_| Value::Text(raw.to_string())),

        "float" | "double" => raw
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or_else(|_| Value::Text(raw.to_string())),

        "boolean" => match raw {
            "true" | "True" | "TRUE" => Value::Bool(true),
            "false" | "False" | "FALSE" => Value::Bool(false),
            _ => Value::Text(raw.to_string()),
        },

        _ => Value::Text(raw.to_string()),
    }
}

/// Build a minimal `QueryResult` from parsed columns and rows.
pub(crate) fn build_query_result(columns: Vec<ColumnMeta>, rows: Vec<Vec<Value>>) -> QueryResult {
    QueryResult::table(columns, rows, None, std::time::Duration::ZERO)
}
