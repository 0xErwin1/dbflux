//! Recovers typed `Value`s from the lossy text/JSON representation written
//! by `FileSink` (via `dbflux_export`'s CSV/JSON formatters), guided by the
//! source column's driver-reported `type_name` — the same "sniff the raw
//! type-name string" strategy `SqlDialect::value_to_literal_typed` overrides
//! already use for the encode direction (e.g. `needs_postgres_text_comparison_cast`).
//! This is only sound because Import is same-engine this slice: Export and
//! Import always share one dialect's type-name vocabulary, so there is no
//! cross-dialect type coercion to get wrong (R5).
//!
//! The export options recorded in the manifest (NULL text, date format) say
//! how the writer rendered the values, so the decoders take them to read the
//! same text back.

use dbflux_core::Value;
use dbflux_core::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use dbflux_export::{DateFormat, FormatOptions};

/// Decodes one CSV field back into a `Value`. The field equal to the
/// options' NULL text is `NULL`. With the default (empty) NULL text an empty
/// field is `NULL` too, the same convention `FileSink`'s CSV writer uses
/// (CSV cannot tell it from an empty string once written); with a custom NULL
/// text an empty field is a real empty string.
pub(crate) fn value_from_csv_field(
    field: &str,
    type_name: Option<&str>,
    options: &FormatOptions,
) -> Value {
    if field == options.null_text {
        return Value::Null;
    }
    if field.is_empty() {
        return Value::Text(String::new());
    }

    value_from_text(field, type_name, options.date_format)
}

/// Decodes one JSON value (as produced by `Value::to_serde_json`) back into a
/// `Value`, using `type_name` to disambiguate the JSON string cases that
/// collapse several `Value` variants into plain strings (Decimal, Date, Time)
/// and the numbers that stand for dates under the epoch format.
pub(crate) fn value_from_json(
    value: &serde_json::Value,
    type_name: Option<&str>,
    date_format: DateFormat,
) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if date_format == DateFormat::Epoch
                && let Some(temporal) = decode_temporal(&n.to_string(), type_name, date_format)
            {
                return temporal;
            }
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => value_from_text(s, type_name, date_format),
        serde_json::Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| value_from_json(item, None, date_format))
                .collect(),
        ),
        serde_json::Value::Object(map) => decode_json_object(map, date_format),
    }
}

fn decode_json_object(
    map: &serde_json::Map<String, serde_json::Value>,
    date_format: DateFormat,
) -> Value {
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
            .map(|(k, v)| (k.clone(), value_from_json(v, None, date_format)))
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
fn value_from_text(text: &str, type_name: Option<&str>, date_format: DateFormat) -> Value {
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

    if let Some(temporal) = decode_temporal(text, type_name, date_format) {
        return temporal;
    }

    Value::Text(text.to_string())
}

/// Decodes a timestamp, date or time written in `date_format`, chosen by the
/// column's type name. A timestamp written as RFC 3339 is also accepted under
/// any format, so files from before the option existed still read.
fn decode_temporal(text: &str, type_name: Option<&str>, date_format: DateFormat) -> Option<Value> {
    match temporal_kind(type_name?) {
        Some(TemporalKind::Timestamp) => decode_timestamp(text, date_format).map(Value::DateTime),
        Some(TemporalKind::Date) => decode_date(text, date_format).map(Value::Date),
        Some(TemporalKind::Time) => decode_time(text, date_format).map(Value::Time),
        None => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemporalKind {
    Timestamp,
    Date,
    Time,
}

/// Classifies a driver type name as a timestamp, date or time. The base name
/// decides, so `timestamp with time zone`, `timestamptz`, `time(3)`,
/// `datetime2(3)` and `Nullable(DateTime)` match while a custom type such as
/// `runtime` does not.
fn temporal_kind(type_name: &str) -> Option<TemporalKind> {
    let lowered = type_name.to_ascii_lowercase();
    let mut base = lowered.trim();
    while let Some(inner) = base
        .strip_prefix("nullable(")
        .or_else(|| base.strip_prefix("lowcardinality("))
        .and_then(|rest| rest.strip_suffix(')'))
    {
        base = inner.trim();
    }
    let word = base
        .split(|character: char| character == '(' || character.is_whitespace())
        .next()
        .unwrap_or_default();

    if word.starts_with("timestamp") || word.starts_with("datetime") || word == "smalldatetime" {
        Some(TemporalKind::Timestamp)
    } else if matches!(word, "date" | "date32") {
        Some(TemporalKind::Date)
    } else if matches!(word, "time" | "timetz") {
        Some(TemporalKind::Time)
    } else {
        None
    }
}

fn decode_timestamp(text: &str, date_format: DateFormat) -> Option<DateTime<Utc>> {
    let selected = match date_format {
        DateFormat::Rfc3339 => None,
        DateFormat::DateTime => NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
            .ok()
            .map(|naive| naive.and_utc()),
        DateFormat::Epoch => text
            .parse::<i64>()
            .ok()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0)),
    };
    selected.or_else(|| {
        DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|parsed| parsed.with_timezone(&Utc))
    })
}

fn decode_date(text: &str, date_format: DateFormat) -> Option<NaiveDate> {
    if date_format == DateFormat::Epoch
        && let Some(timestamp) = text
            .parse::<i64>()
            .ok()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
    {
        return Some(timestamp.date_naive());
    }
    NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()
}

fn decode_time(text: &str, date_format: DateFormat) -> Option<NaiveTime> {
    if date_format == DateFormat::Epoch
        && let Some(time) = text
            .parse::<u32>()
            .ok()
            .and_then(|seconds| NaiveTime::from_num_seconds_from_midnight_opt(seconds, 0))
    {
        return Some(time);
    }
    NaiveTime::parse_from_str(text, "%H:%M:%S%.f").ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::chrono::TimeZone;

    /// Type names the drivers emit for timestamps decode as before under the
    /// default preset, including the ones `main` matched by substring.
    #[test]
    fn default_preset_temporal_names_decode_like_before() {
        let options = FormatOptions::default();
        for type_name in [
            "datetimeoffset",
            "datetimeoffset(5)",
            "timestamp_ns",
            "TIMESTAMP_LTZ",
            "timestamptz(3)",
            "datetime(6)",
            "DateTime64(3, 'UTC')",
            "LowCardinality(Nullable(DateTime))",
            "TIMESTAMP(6) WITH LOCAL TIME ZONE",
        ] {
            assert!(
                matches!(
                    value_from_csv_field("2026-07-07T10:30:15+00:00", Some(type_name), &options),
                    Value::DateTime(_)
                ),
                "{type_name}"
            );
        }
    }

    #[test]
    fn empty_csv_field_decodes_as_null() {
        assert_eq!(
            value_from_csv_field("", Some("text"), &FormatOptions::default()),
            Value::Null
        );
    }

    #[test]
    fn csv_field_decodes_as_int_when_type_name_says_int() {
        assert_eq!(
            value_from_csv_field("42", Some("int4"), &FormatOptions::default()),
            Value::Int(42)
        );
        assert_eq!(
            value_from_csv_field("42", Some("INTEGER"), &FormatOptions::default()),
            Value::Int(42)
        );
    }

    #[test]
    fn csv_field_decodes_as_bool_when_type_name_says_bool() {
        assert_eq!(
            value_from_csv_field("true", Some("boolean"), &FormatOptions::default()),
            Value::Bool(true)
        );
        assert_eq!(
            value_from_csv_field("false", Some("bool"), &FormatOptions::default()),
            Value::Bool(false)
        );
    }

    #[test]
    fn csv_field_decodes_as_float_when_type_name_says_float() {
        assert_eq!(
            value_from_csv_field("2.5", Some("double precision"), &FormatOptions::default()),
            Value::Float(2.5)
        );
    }

    #[test]
    fn csv_field_decodes_as_decimal_when_type_name_says_numeric() {
        assert_eq!(
            value_from_csv_field("19.99", Some("numeric(10,2)"), &FormatOptions::default()),
            Value::Decimal("19.99".to_string())
        );
    }

    #[test]
    fn csv_field_decodes_as_date_when_type_name_is_date() {
        assert_eq!(
            value_from_csv_field("2026-07-07", Some("date"), &FormatOptions::default()),
            Value::Date(NaiveDate::from_ymd_opt(2026, 7, 7).unwrap())
        );
    }

    #[test]
    fn csv_field_falls_back_to_text_for_unrecognized_type() {
        assert_eq!(
            value_from_csv_field("hello", Some("citext"), &FormatOptions::default()),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn csv_field_falls_back_to_text_when_type_name_is_missing() {
        assert_eq!(
            value_from_csv_field("42", None, &FormatOptions::default()),
            Value::Text("42".to_string())
        );
    }

    #[test]
    fn json_number_decodes_directly_without_needing_a_type_name() {
        assert_eq!(
            value_from_json(&serde_json::json!(7), None, DateFormat::default()),
            Value::Int(7)
        );
        assert_eq!(
            value_from_json(&serde_json::json!(2.5), None, DateFormat::default()),
            Value::Float(2.5)
        );
    }

    #[test]
    fn json_bool_and_null_decode_directly() {
        assert_eq!(
            value_from_json(&serde_json::json!(true), None, DateFormat::default()),
            Value::Bool(true)
        );
        assert_eq!(
            value_from_json(&serde_json::Value::Null, None, DateFormat::default()),
            Value::Null
        );
    }

    #[test]
    fn json_object_id_marker_decodes_back_to_object_id() {
        let json = serde_json::json!({"$oid": "507f1f77bcf86cd799439011"});
        assert_eq!(
            value_from_json(&json, None, DateFormat::default()),
            Value::ObjectId("507f1f77bcf86cd799439011".to_string())
        );
    }

    #[test]
    fn json_date_marker_decodes_back_to_datetime() {
        let json = serde_json::json!({"$date": "2026-07-07T10:00:00+00:00"});
        let decoded = value_from_json(&json, None, DateFormat::default());
        assert!(matches!(decoded, Value::DateTime(_)));
    }

    #[test]
    fn json_string_uses_type_name_to_disambiguate_decimal_from_text() {
        assert_eq!(
            value_from_json(
                &serde_json::json!("19.99"),
                Some("numeric"),
                DateFormat::default()
            ),
            Value::Decimal("19.99".to_string())
        );
        assert_eq!(
            value_from_json(
                &serde_json::json!("hello"),
                Some("text"),
                DateFormat::default()
            ),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn json_plain_object_without_markers_decodes_as_document() {
        let json = serde_json::json!({"city": "NYC", "zip": 10001});
        let decoded = value_from_json(&json, None, DateFormat::default());
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
            value_from_json(&json, None, DateFormat::default()),
            Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        );
    }

    fn epoch_options() -> FormatOptions {
        FormatOptions {
            date_format: DateFormat::Epoch,
            ..FormatOptions::default()
        }
    }

    #[test]
    fn csv_decodes_the_configured_null_text_as_null_and_an_empty_field_as_text() {
        let options = FormatOptions {
            null_text: "\\N".to_string(),
            ..FormatOptions::default()
        };
        assert_eq!(
            value_from_csv_field("\\N", Some("text"), &options),
            Value::Null
        );
        assert_eq!(
            value_from_csv_field("\\N", Some("int4"), &options),
            Value::Null
        );
        assert_eq!(
            value_from_csv_field("", Some("text"), &options),
            Value::Text(String::new())
        );
    }

    #[test]
    fn csv_decodes_each_date_preset() {
        let instant = Value::DateTime(Utc.with_ymd_and_hms(2026, 7, 7, 10, 30, 15).unwrap());
        let date = Value::Date(NaiveDate::from_ymd_opt(2026, 7, 7).unwrap());
        let time = Value::Time(NaiveTime::from_hms_opt(10, 30, 15).unwrap());
        let date_time = FormatOptions {
            date_format: DateFormat::DateTime,
            ..FormatOptions::default()
        };

        assert_eq!(
            value_from_csv_field("2026-07-07 10:30:15", Some("timestamptz"), &date_time),
            instant
        );
        assert_eq!(
            value_from_csv_field("1783420215", Some("timestamp"), &epoch_options()),
            instant
        );
        assert_eq!(
            value_from_csv_field("1783382400", Some("date"), &epoch_options()),
            date
        );
        assert_eq!(
            value_from_csv_field("37815", Some("time"), &epoch_options()),
            time
        );
        assert_eq!(
            value_from_csv_field("10:30:15", Some("time"), &date_time),
            time
        );
    }

    #[test]
    fn json_decodes_epoch_numbers_for_date_columns_and_leaves_other_numbers() {
        let instant = Value::DateTime(Utc.with_ymd_and_hms(2026, 7, 7, 10, 30, 15).unwrap());
        assert_eq!(
            value_from_json(
                &serde_json::json!(1783420215),
                Some("timestamptz"),
                DateFormat::Epoch
            ),
            instant
        );
        assert_eq!(
            value_from_json(&serde_json::json!(7), Some("int4"), DateFormat::Epoch),
            Value::Int(7)
        );
        assert_eq!(
            value_from_json(
                &serde_json::json!("2026-07-07 10:30:15"),
                Some("timestamp"),
                DateFormat::DateTime
            ),
            instant
        );
    }

    #[test]
    fn a_type_that_only_contains_time_stays_text_under_every_preset() {
        for date_format in DateFormat::ALL {
            let options = FormatOptions {
                date_format,
                ..FormatOptions::default()
            };
            for type_name in [
                "runtime",
                "timeline",
                "update_date_marker",
                "mytimestampish",
            ] {
                assert_eq!(
                    value_from_csv_field("100", Some(type_name), &options),
                    Value::Text("100".to_string()),
                    "{type_name} under {date_format:?}"
                );
            }
            assert_eq!(
                value_from_json(&serde_json::json!(100), Some("runtime"), date_format),
                Value::Int(100),
                "{date_format:?}"
            );
        }
    }

    #[test]
    fn temporal_type_names_with_modifiers_are_recognized() {
        let instant = Value::DateTime(Utc.with_ymd_and_hms(2026, 7, 7, 10, 30, 15).unwrap());
        let time = Value::Time(NaiveTime::from_hms_opt(10, 30, 15).unwrap());
        let date = Value::Date(NaiveDate::from_ymd_opt(2026, 7, 7).unwrap());
        let epoch = epoch_options();

        for type_name in [
            "timestamp",
            "TIMESTAMP",
            "timestamptz",
            "timestamp with time zone",
            "timestamp without time zone",
            "TIMESTAMP(6) WITH TIME ZONE",
            "datetime",
            "datetime2",
            "datetime2(3)",
            "DateTime64(3)",
            "Nullable(DateTime)",
        ] {
            assert_eq!(
                value_from_csv_field("1783420215", Some(type_name), &epoch),
                instant,
                "{type_name}"
            );
        }
        for type_name in [
            "time",
            "time(3)",
            "time without time zone",
            "timetz",
            "TIME WITH TIME ZONE",
        ] {
            assert_eq!(
                value_from_csv_field("37815", Some(type_name), &epoch),
                time,
                "{type_name}"
            );
        }
        for type_name in ["date", "DATE", "date32"] {
            assert_eq!(
                value_from_csv_field("1783382400", Some(type_name), &epoch),
                date,
                "{type_name}"
            );
        }
    }
}
