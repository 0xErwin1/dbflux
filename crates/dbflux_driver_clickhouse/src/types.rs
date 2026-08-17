use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use dbflux_core::{ColumnKind, Value};
use serde_json::Value as JsonValue;

const MAX_TYPE_INPUT_BYTES: usize = 4096;
const MAX_TYPE_DEPTH: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseType {
    pub name: String,
    pub arguments: Vec<ClickHouseTypeArgument>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickHouseTypeArgument {
    Type(ClickHouseType),
    Literal(String),
}

impl ClickHouseType {
    fn wrapped_type(&self) -> Option<&ClickHouseType> {
        self.arguments.first().and_then(|argument| match argument {
            ClickHouseTypeArgument::Type(value) => Some(value),
            ClickHouseTypeArgument::Literal(_) => None,
        })
    }

    fn effective(&self) -> &ClickHouseType {
        if matches!(self.name.as_str(), "Nullable" | "LowCardinality") {
            self.wrapped_type()
                .map(ClickHouseType::effective)
                .unwrap_or(self)
        } else {
            self
        }
    }
}

pub fn parse_clickhouse_type(input: &str) -> ClickHouseType {
    if input.len() > MAX_TYPE_INPUT_BYTES {
        return invalid_type();
    }
    TypeParser::new(input)
        .parse_type(0)
        .unwrap_or_else(|| ClickHouseType {
            name: input.trim().to_string(),
            arguments: Vec::new(),
        })
}

fn invalid_type() -> ClickHouseType {
    ClickHouseType {
        name: "Unknown".to_string(),
        arguments: Vec::new(),
    }
}

pub fn clickhouse_type_is_nullable(data_type: &ClickHouseType) -> bool {
    match data_type.name.as_str() {
        "Nullable" => true,
        "LowCardinality" => data_type
            .wrapped_type()
            .is_some_and(clickhouse_type_is_nullable),
        _ => false,
    }
}

pub fn clickhouse_type_to_column_kind(data_type: &ClickHouseType) -> ColumnKind {
    match data_type.effective().name.as_str() {
        "Int8" | "Int16" | "Int32" | "Int64" | "Int128" | "Int256" | "UInt8" | "UInt16"
        | "UInt32" | "UInt64" | "UInt128" | "UInt256" => ColumnKind::Integer,
        "Float32" | "Float64" | "BFloat16" | "Decimal" | "Decimal32" | "Decimal64"
        | "Decimal128" | "Decimal256" => ColumnKind::Float,
        "Date" | "Date32" | "DateTime" | "DateTime64" => ColumnKind::Timestamp,
        "String" | "FixedString" | "UUID" | "IPv4" | "IPv6" | "Enum8" | "Enum16" => {
            ColumnKind::Text
        }
        _ => ColumnKind::Unknown,
    }
}

pub(crate) fn json_to_value(value: &JsonValue, data_type: &ClickHouseType) -> Value {
    if value.is_null() {
        return Value::Null;
    }

    let effective = data_type.effective();
    match effective.name.as_str() {
        "Bool" => parse_bool(value),
        "Int8" | "Int16" | "Int32" | "Int64" => parse_i64(value),
        "UInt8" | "UInt16" | "UInt32" | "UInt64" => parse_u64(value),
        "Int128" | "Int256" | "UInt128" | "UInt256" | "Decimal" | "Decimal32" | "Decimal64"
        | "Decimal128" | "Decimal256" => parse_decimal(value),
        "Float32" | "Float64" | "BFloat16" => parse_float(value),
        "Date" | "Date32" => parse_date(value),
        "DateTime" | "DateTime64" => parse_datetime(value),
        "Array" => parse_array(value, effective.wrapped_type()),
        "Tuple" => parse_tuple(value, effective),
        "Map" => parse_map(value, effective),
        "JSON" | "Object" | "Nested" => Value::Json(value.to_string()),
        "Nothing" => Value::Null,
        _ => match value {
            JsonValue::String(text) => Value::Text(text.clone()),
            JsonValue::Bool(boolean) => Value::Bool(*boolean),
            JsonValue::Number(number) => number
                .as_i64()
                .map(Value::Int)
                .or_else(|| number.as_f64().map(Value::Float))
                .unwrap_or_else(|| Value::Text(number.to_string())),
            JsonValue::Array(_) | JsonValue::Object(_) => Value::Json(value.to_string()),
            JsonValue::Null => Value::Null,
        },
    }
}

fn parse_bool(value: &JsonValue) -> Value {
    match value {
        JsonValue::Bool(value) => Value::Bool(*value),
        JsonValue::Number(value) => Value::Bool(value.as_u64().is_some_and(|value| value != 0)),
        JsonValue::String(value) => Value::Bool(value == "1" || value.eq_ignore_ascii_case("true")),
        _ => Value::Text(value.to_string()),
    }
}

fn parse_i64(value: &JsonValue) -> Value {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .map(Value::Int)
        .unwrap_or_else(|| Value::Decimal(json_scalar_text(value)))
}

fn parse_u64(value: &JsonValue) -> Value {
    let parsed = value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()));
    match parsed {
        Some(value) if value <= i64::MAX as u64 => Value::Int(value as i64),
        Some(value) => Value::Decimal(value.to_string()),
        None => Value::Decimal(json_scalar_text(value)),
    }
}

fn parse_decimal(value: &JsonValue) -> Value {
    Value::Decimal(json_scalar_text(value))
}

fn parse_float(value: &JsonValue) -> Value {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .filter(|value| value.is_finite())
        .map(Value::Float)
        .unwrap_or_else(|| Value::Text(json_scalar_text(value)))
}

fn parse_date(value: &JsonValue) -> Value {
    value
        .as_str()
        .and_then(|text| NaiveDate::parse_from_str(text, "%Y-%m-%d").ok())
        .map(Value::Date)
        .unwrap_or_else(|| Value::Text(json_scalar_text(value)))
}

fn parse_datetime(value: &JsonValue) -> Value {
    let Some(text) = value.as_str() else {
        return Value::Text(json_scalar_text(value));
    };
    if let Ok(value) = DateTime::parse_from_rfc3339(text) {
        return Value::DateTime(value.with_timezone(&Utc));
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(value) = NaiveDateTime::parse_from_str(text, format) {
            return Value::DateTime(DateTime::from_naive_utc_and_offset(value, Utc));
        }
    }
    if let Ok(value) = NaiveTime::parse_from_str(text, "%H:%M:%S%.f") {
        return Value::Time(value);
    }
    Value::Text(text.to_string())
}

fn parse_array(value: &JsonValue, element_type: Option<&ClickHouseType>) -> Value {
    match value.as_array() {
        Some(values) => Value::Array(
            values
                .iter()
                .map(|value| match element_type {
                    Some(data_type) => json_to_value(value, data_type),
                    None => Value::Json(value.to_string()),
                })
                .collect(),
        ),
        None => Value::Json(value.to_string()),
    }
}

fn parse_tuple(value: &JsonValue, data_type: &ClickHouseType) -> Value {
    let Some(values) = value.as_array() else {
        return Value::Json(value.to_string());
    };
    Value::Array(
        values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                data_type
                    .arguments
                    .get(index)
                    .and_then(|argument| match argument {
                        ClickHouseTypeArgument::Type(data_type) => Some(data_type),
                        ClickHouseTypeArgument::Literal(_) => None,
                    })
                    .map(|data_type| json_to_value(value, data_type))
                    .unwrap_or_else(|| Value::Json(value.to_string()))
            })
            .collect(),
    )
}

fn parse_map(value: &JsonValue, data_type: &ClickHouseType) -> Value {
    let value_type = data_type
        .arguments
        .get(1)
        .and_then(|argument| match argument {
            ClickHouseTypeArgument::Type(data_type) => Some(data_type),
            ClickHouseTypeArgument::Literal(_) => None,
        });
    match value.as_object() {
        Some(object) => Value::Document(
            object
                .iter()
                .map(|(key, value)| {
                    let value = value_type
                        .map(|data_type| json_to_value(value, data_type))
                        .unwrap_or_else(|| Value::Json(value.to_string()));
                    (key.clone(), value)
                })
                .collect::<BTreeMap<_, _>>(),
        ),
        None => Value::Json(value.to_string()),
    }
}

fn json_scalar_text(value: &JsonValue) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

struct TypeParser<'a> {
    input: &'a str,
    position: usize,
}

impl<'a> TypeParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, position: 0 }
    }

    fn parse_type(&mut self, depth: usize) -> Option<ClickHouseType> {
        if depth >= MAX_TYPE_DEPTH {
            return None;
        }
        self.skip_space();
        let name = self.parse_identifier()?;
        self.skip_space();
        let mut arguments = Vec::new();
        if self.consume('(') {
            loop {
                self.skip_space();
                if self.consume(')') {
                    break;
                }
                arguments.push(self.parse_argument(depth)?);
                self.skip_space();
                if self.consume(')') {
                    break;
                }
                if !self.consume(',') {
                    return None;
                }
            }
        }
        Some(ClickHouseType { name, arguments })
    }

    fn parse_argument(&mut self, depth: usize) -> Option<ClickHouseTypeArgument> {
        self.skip_space();
        let start = self.position;
        if self.peek().is_some_and(|value| value.is_ascii_uppercase()) {
            if let Some(data_type) = self.parse_type(depth + 1) {
                return Some(ClickHouseTypeArgument::Type(data_type));
            }
            self.position = start;
        } else if self.peek().is_some_and(|value| value.is_ascii_lowercase()) {
            let field_start = self.position;
            let field_name = self.parse_identifier()?;
            let had_space = self
                .peek()
                .is_some_and(char::is_whitespace)
                .then(|| self.skip_space())
                .is_some();
            if had_space
                && self.peek().is_some_and(|value| value.is_ascii_uppercase())
                && let Some(data_type) = self.parse_type(depth + 1)
            {
                return Some(ClickHouseTypeArgument::Type(data_type));
            }
            self.position = field_start;
            if field_name.is_empty() {
                return None;
            }
        }
        self.position = start;
        let literal = self.parse_literal_argument().trim().to_string();
        (!literal.is_empty()).then_some(ClickHouseTypeArgument::Literal(literal))
    }

    fn parse_identifier(&mut self) -> Option<String> {
        let start = self.position;
        while self
            .peek()
            .is_some_and(|value| value.is_ascii_alphanumeric() || value == '_')
        {
            self.advance();
        }
        (self.position > start).then(|| self.input[start..self.position].to_string())
    }

    fn parse_literal_argument(&mut self) -> &str {
        let start = self.position;
        let mut quote = None;
        let mut escaped = false;
        let mut depth = 0_u32;
        while let Some(value) = self.peek() {
            if let Some(active_quote) = quote {
                self.advance();
                if escaped {
                    escaped = false;
                } else if value == '\\' {
                    escaped = true;
                } else if value == active_quote {
                    quote = None;
                }
                continue;
            }
            if matches!(value, '\'' | '"') {
                quote = Some(value);
                self.advance();
                continue;
            }
            if value == '(' {
                depth += 1;
            } else if value == ')' {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            } else if value == ',' && depth == 0 {
                break;
            }
            self.advance();
        }
        &self.input[start..self.position]
    }

    fn skip_space(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.advance();
        }
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.position..)?.chars().next()
    }

    fn advance(&mut self) {
        if let Some(value) = self.peek() {
            self.position += value.len_utf8();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nested_types_and_literal_arguments() {
        let parsed = parse_clickhouse_type("LowCardinality(Nullable(Array(Decimal(18, 4))))");
        assert_eq!(parsed.name, "LowCardinality");
        assert_eq!(clickhouse_type_to_column_kind(&parsed), ColumnKind::Unknown);

        let datetime = parse_clickhouse_type("DateTime64(6, 'UTC')");
        assert_eq!(datetime.name, "DateTime64");
        assert_eq!(datetime.arguments.len(), 2);
        assert_eq!(
            clickhouse_type_to_column_kind(&datetime),
            ColumnKind::Timestamp
        );

        let enumeration = parse_clickhouse_type("Enum8('ready' = 1, 'done' = 2)");
        assert_eq!(enumeration.name, "Enum8");
        assert_eq!(enumeration.arguments.len(), 2);
        assert_eq!(
            clickhouse_type_to_column_kind(&enumeration),
            ColumnKind::Text
        );

        let named_tuple = parse_clickhouse_type("Tuple(id UInt64, label String)");
        assert_eq!(named_tuple.name, "Tuple");
        assert_eq!(named_tuple.arguments.len(), 2);
    }

    #[test]
    fn nullable_integer_is_classified_and_decoded() {
        let data_type = parse_clickhouse_type("Nullable(UInt64)");
        assert_eq!(
            clickhouse_type_to_column_kind(&data_type),
            ColumnKind::Integer
        );
        assert_eq!(
            json_to_value(&serde_json::json!(42), &data_type),
            Value::Int(42)
        );
        assert_eq!(json_to_value(&JsonValue::Null, &data_type), Value::Null);
        assert!(clickhouse_type_is_nullable(&data_type));
        assert!(clickhouse_type_is_nullable(&parse_clickhouse_type(
            "LowCardinality(Nullable(String))"
        )));
        assert!(!clickhouse_type_is_nullable(&parse_clickhouse_type(
            "Array(Nullable(String))"
        )));
    }

    #[test]
    fn large_unsigned_integer_preserves_precision() {
        let data_type = parse_clickhouse_type("UInt64");
        assert_eq!(
            json_to_value(&serde_json::json!("18446744073709551615"), &data_type),
            Value::Decimal("18446744073709551615".to_string())
        );
    }

    #[test]
    fn arrays_decode_recursively() {
        let data_type = parse_clickhouse_type("Array(Nullable(Int32))");
        assert_eq!(
            json_to_value(&serde_json::json!([1, null, 3]), &data_type),
            Value::Array(vec![Value::Int(1), Value::Null, Value::Int(3)])
        );
    }

    #[test]
    fn malformed_type_degrades_to_unknown() {
        let parsed = parse_clickhouse_type("Tuple(String");
        assert_eq!(parsed.name, "Tuple(String");
        assert_eq!(clickhouse_type_to_column_kind(&parsed), ColumnKind::Unknown);
    }

    #[test]
    fn type_parser_limits_input_and_recursion_depth() {
        let oversized = "X".repeat(MAX_TYPE_INPUT_BYTES + 1);
        assert_eq!(parse_clickhouse_type(&oversized), invalid_type());

        let deeply_nested = format!(
            "{}UInt8{}",
            "Nullable(".repeat(MAX_TYPE_DEPTH + 1),
            ")".repeat(MAX_TYPE_DEPTH + 1)
        );
        assert_eq!(
            clickhouse_type_to_column_kind(&parse_clickhouse_type(&deeply_nested)),
            ColumnKind::Unknown
        );
    }
}
