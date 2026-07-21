//! Recovers typed `Value`s from the lossy text/JSON representation written
//! by `FileSink` (via `dbflux_export`'s CSV/JSON formatters), guided by the
//! source column's driver-reported `type_name` — the same "sniff the raw
//! type-name string" strategy `SqlDialect::value_to_literal_typed` overrides
//! already use for the encode direction (e.g. `needs_postgres_text_comparison_cast`).
//! This is only sound because Import is same-engine this slice: Export and
//! Import always share one dialect's type-name vocabulary, so there is no
//! cross-dialect type coercion to get wrong (R5).

use dbflux_core::Value;
use dbflux_core::chrono::{DateTime, NaiveDate, NaiveTime, Utc};

/// Decodes one CSV field back into a `Value`. An empty field is treated as
/// `NULL` — the same convention `FileSink`'s CSV writer uses for `Value::Null`
/// (and, unavoidably, for an actual empty string; CSV cannot distinguish the
/// two once written, matching `dbflux_export`'s existing encode-side limit).
pub fn value_from_csv_field(field: &str, type_name: Option<&str>) -> Value {
    if field.is_empty() {
        return Value::Null;
    }

    value_from_text(field, type_name)
}

/// Decodes one JSON value (as produced by `Value::to_serde_json`) back into a
/// `Value`, using `type_name` to disambiguate the JSON string cases that
/// collapse several `Value` variants into plain strings (Decimal, Date, Time).
pub fn value_from_json(value: &serde_json::Value, type_name: Option<&str>) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => value_from_text(s, type_name),
        serde_json::Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| value_from_json(item, None))
                .collect(),
        ),
        serde_json::Value::Object(map) => decode_json_object(map),
    }
}

fn decode_json_object(map: &serde_json::Map<String, serde_json::Value>) -> Value {
    if let Some(serde_json::Value::String(oid)) = map.get("$oid") {
        return Value::ObjectId(oid.clone());
    }

    if let Some(serde_json::Value::String(date)) = map.get("$date")
        && let Ok(dt) = DateTime::parse_from_rfc3339(date)
    {
        return Value::DateTime(dt.with_timezone(&Utc));
    }

    if let Some(hex) = map
        .get("$binary")
        .and_then(|b| b.get("hex"))
        .and_then(|h| h.as_str())
        && let Ok(bytes) = decode_hex(hex)
    {
        return Value::Bytes(bytes);
    }

    if let Some(serde_json::Value::String(unsupported)) = map.get("$unsupported") {
        return Value::Unsupported(unsupported.clone());
    }

    Value::Document(
        map.iter()
            .map(|(k, v)| (k.clone(), value_from_json(v, None)))
            .collect(),
    )
}

fn decode_hex(hex: &str) -> Result<Vec<u8>, std::num::ParseIntError> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..(i + 2).min(hex.len())], 16))
        .collect()
}

/// Type-name-directed text decoder shared by CSV fields and JSON's lossy
/// string cases. Anything not recognized from `type_name` falls back to
/// `Value::Text` — a conservative default that never fails the transfer,
/// though it means uncommonly-spelled driver type names may round-trip as
/// text instead of their original variant (documented limitation, not a bug:
/// there is no generic `type_name -> Value` classification seam in the
/// codebase to fall back on beyond this heuristic).
fn value_from_text(text: &str, type_name: Option<&str>) -> Value {
    let type_lower = type_name.unwrap_or_default().to_ascii_lowercase();

    if type_lower.contains("bool") {
        match text {
            "true" => return Value::Bool(true),
            "false" => return Value::Bool(false),
            _ => {}
        }
    }

    if type_lower.contains("numeric")
        || type_lower.contains("decimal")
        || type_lower.contains("money")
    {
        return Value::Decimal(text.to_string());
    }

    if (type_lower.contains("int") || type_lower.contains("serial"))
        && !type_lower.contains("point")
        && let Ok(n) = text.parse::<i64>()
    {
        return Value::Int(n);
    }

    if (type_lower.contains("float")
        || type_lower.contains("double")
        || type_lower.contains("real"))
        && let Ok(f) = text.parse::<f64>()
    {
        return Value::Float(f);
    }

    if type_lower.contains("timestamp") || type_lower.contains("datetime") {
        if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
            return Value::DateTime(dt.with_timezone(&Utc));
        }
    } else if type_lower == "date" {
        if let Ok(d) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
            return Value::Date(d);
        }
    } else if type_lower.contains("time")
        && let Ok(t) = NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
    {
        return Value::Time(t);
    }

    Value::Text(text.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_csv_field_decodes_as_null() {
        assert_eq!(value_from_csv_field("", Some("text")), Value::Null);
    }

    #[test]
    fn csv_field_decodes_as_int_when_type_name_says_int() {
        assert_eq!(value_from_csv_field("42", Some("int4")), Value::Int(42));
        assert_eq!(value_from_csv_field("42", Some("INTEGER")), Value::Int(42));
    }

    #[test]
    fn csv_field_decodes_as_bool_when_type_name_says_bool() {
        assert_eq!(
            value_from_csv_field("true", Some("boolean")),
            Value::Bool(true)
        );
        assert_eq!(
            value_from_csv_field("false", Some("bool")),
            Value::Bool(false)
        );
    }

    #[test]
    fn csv_field_decodes_as_float_when_type_name_says_float() {
        assert_eq!(
            value_from_csv_field("2.5", Some("double precision")),
            Value::Float(2.5)
        );
    }

    #[test]
    fn csv_field_decodes_as_decimal_when_type_name_says_numeric() {
        assert_eq!(
            value_from_csv_field("19.99", Some("numeric(10,2)")),
            Value::Decimal("19.99".to_string())
        );
    }

    #[test]
    fn csv_field_decodes_as_date_when_type_name_is_date() {
        assert_eq!(
            value_from_csv_field("2026-07-07", Some("date")),
            Value::Date(NaiveDate::from_ymd_opt(2026, 7, 7).unwrap())
        );
    }

    #[test]
    fn csv_field_falls_back_to_text_for_unrecognized_type() {
        assert_eq!(
            value_from_csv_field("hello", Some("citext")),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn csv_field_falls_back_to_text_when_type_name_is_missing() {
        assert_eq!(
            value_from_csv_field("42", None),
            Value::Text("42".to_string())
        );
    }

    #[test]
    fn json_number_decodes_directly_without_needing_a_type_name() {
        assert_eq!(value_from_json(&serde_json::json!(7), None), Value::Int(7));
        assert_eq!(
            value_from_json(&serde_json::json!(2.5), None),
            Value::Float(2.5)
        );
    }

    #[test]
    fn json_bool_and_null_decode_directly() {
        assert_eq!(
            value_from_json(&serde_json::json!(true), None),
            Value::Bool(true)
        );
        assert_eq!(value_from_json(&serde_json::Value::Null, None), Value::Null);
    }

    #[test]
    fn json_object_id_marker_decodes_back_to_object_id() {
        let json = serde_json::json!({"$oid": "507f1f77bcf86cd799439011"});
        assert_eq!(
            value_from_json(&json, None),
            Value::ObjectId("507f1f77bcf86cd799439011".to_string())
        );
    }

    #[test]
    fn json_date_marker_decodes_back_to_datetime() {
        let json = serde_json::json!({"$date": "2026-07-07T10:00:00+00:00"});
        let decoded = value_from_json(&json, None);
        assert!(matches!(decoded, Value::DateTime(_)));
    }

    #[test]
    fn json_string_uses_type_name_to_disambiguate_decimal_from_text() {
        assert_eq!(
            value_from_json(&serde_json::json!("19.99"), Some("numeric")),
            Value::Decimal("19.99".to_string())
        );
        assert_eq!(
            value_from_json(&serde_json::json!("hello"), Some("text")),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn json_plain_object_without_markers_decodes_as_document() {
        let json = serde_json::json!({"city": "NYC", "zip": 10001});
        let decoded = value_from_json(&json, None);
        let Value::Document(doc) = decoded else {
            panic!("expected a Document value");
        };
        assert_eq!(doc.get("city"), Some(&Value::Text("NYC".to_string())));
        assert_eq!(doc.get("zip"), Some(&Value::Int(10001)));
    }

    #[test]
    fn json_array_decodes_element_by_element() {
        let json = serde_json::json!([1, 2, 3]);
        assert_eq!(
            value_from_json(&json, None),
            Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        );
    }
}
