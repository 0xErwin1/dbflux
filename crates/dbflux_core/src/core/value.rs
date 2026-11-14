use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// Database value type.
///
/// Custom enum instead of `serde_json::Value` to enable proper type-aware
/// sorting, efficient rendering, and clean CSV export without JSON overhead.
///
/// Supports both relational (SQL) and document (NoSQL) value types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),

    /// JSON/JSONB stored as string for exact round-trip preservation.
    Json(String),

    /// Decimal stored as string to preserve exact precision.
    Decimal(String),

    /// Timestamp with timezone.
    DateTime(DateTime<Utc>),

    /// Date without time component.
    Date(NaiveDate),

    /// Time without date component.
    Time(NaiveTime),

    // === Document database types ===
    /// Array of values (MongoDB arrays, PostgreSQL arrays).
    Array(Vec<Value>),

    /// Nested document/object (MongoDB embedded documents).
    Document(BTreeMap<String, Value>),

    /// MongoDB ObjectId (24-character hex string).
    ObjectId(String),

    /// Value exists in database but this client cannot decode/render it yet.
    Unsupported(String),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_display_string(&self) -> String {
        self.as_display_string_truncated(1000)
    }

    pub fn as_display_string_truncated(&self, max_len: usize) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) | Value::Json(s) | Value::Decimal(s) => {
                if s.len() <= max_len {
                    s.clone()
                } else {
                    let truncated: String = s.chars().take(max_len).collect();
                    format!("{}...", truncated)
                }
            }
            Value::Bytes(b) => format!("<{} bytes>", b.len()),
            Value::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            Value::Date(d) => d.format("%Y-%m-%d").to_string(),
            Value::Time(t) => t.format("%H:%M:%S").to_string(),
            Value::Array(arr) => {
                let preview = format!("[{} items]", arr.len());
                if preview.len() <= max_len {
                    preview
                } else {
                    format!("[{}...]", arr.len())
                }
            }
            Value::Document(doc) => {
                let preview = format!("{{{} fields}}", doc.len());
                if preview.len() <= max_len {
                    preview
                } else {
                    format!("{{{}...}}", doc.len())
                }
            }
            Value::ObjectId(id) => {
                if id.len() <= max_len {
                    format!("ObjectId({})", id)
                } else {
                    format!("ObjectId({}...)", &id[..max_len.saturating_sub(12)])
                }
            }
            Value::Unsupported(type_name) => format!("UNSUPPORTED<{}>", type_name),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_display_string())
    }
}

impl Value {
    fn type_order(&self) -> u8 {
        match self {
            Value::Bool(_) => 0,
            Value::Int(_) => 1,
            Value::Float(_) => 2,
            Value::Decimal(_) => 3,
            Value::Text(_) => 4,
            Value::Json(_) => 5,
            Value::DateTime(_) => 6,
            Value::Date(_) => 7,
            Value::Time(_) => 8,
            Value::Bytes(_) => 9,
            Value::ObjectId(_) => 10,
            Value::Array(_) => 11,
            Value::Document(_) => 12,
            Value::Unsupported(_) => 13,
            Value::Null => 14,
        }
    }

    /// Equality that also requires the same representation, so a change
    /// between numerically equal values is still a change.
    ///
    /// `PartialEq` treats `Int(1)` and `Float(1.0)` as equal, which is the
    /// one equality definition the codebase orders by. Staging an edit
    /// needs the stricter question — the same variant at every level of the
    /// two values, including each element of an array or document — because
    /// a change only the representation makes is still a change the user
    /// made.
    pub fn same_representation(&self, other: &Value) -> bool {
        if std::mem::discriminant(self) != std::mem::discriminant(other) {
            return false;
        }

        if self.cmp(other) != Ordering::Equal {
            return false;
        }

        match (self, other) {
            (Value::Array(before), Value::Array(after)) => before
                .iter()
                .zip(after)
                .all(|(item, other_item)| item.same_representation(other_item)),
            (Value::Document(before), Value::Document(after)) => {
                before
                    .iter()
                    .zip(after)
                    .all(|((field, value), (other_field, other_value))| {
                        field == other_field && value.same_representation(other_value)
                    })
            }
            _ => true,
        }
    }

    pub fn is_complex(&self) -> bool {
        matches!(self, Value::Array(_) | Value::Document(_))
    }

    pub fn is_object_id(&self) -> bool {
        matches!(self, Value::ObjectId(_))
    }

    pub fn as_object_id(&self) -> Option<&str> {
        match self {
            Value::ObjectId(id) => Some(id),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Array(arr) => Some(arr),
            _ => None,
        }
    }

    pub fn as_document(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Document(doc) => Some(doc),
            _ => None,
        }
    }

    pub fn to_json_string(&self) -> String {
        match self {
            Value::Json(s) => s.clone(),
            other => {
                let json_value = Self::to_serde_json(other);
                serde_json::to_string(&json_value).unwrap_or_else(|_| self.as_display_string())
            }
        }
    }

    pub fn to_serde_json(value: &Value) -> serde_json::Value {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int(i) => serde_json::json!(*i),
            Value::Float(f) => serde_json::json!(*f),
            Value::Text(s) => serde_json::Value::String(s.clone()),
            Value::Bytes(b) => {
                let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                serde_json::json!({"$binary": {"hex": hex}})
            }
            Value::Json(j) => {
                serde_json::from_str(j).unwrap_or(serde_json::Value::String(j.clone()))
            }
            Value::Decimal(d) => serde_json::Value::String(d.clone()),
            Value::DateTime(dt) => serde_json::json!({"$date": dt.to_rfc3339()}),
            Value::Date(d) => serde_json::Value::String(d.to_string()),
            Value::Time(t) => serde_json::Value::String(t.to_string()),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(Self::to_serde_json).collect())
            }
            Value::Document(doc) => {
                let map: serde_json::Map<String, serde_json::Value> = doc
                    .iter()
                    .map(|(k, v)| (k.clone(), Self::to_serde_json(v)))
                    .collect();
                serde_json::Value::Object(map)
            }
            Value::ObjectId(oid) => serde_json::json!({"$oid": oid}),
            Value::Unsupported(type_name) => serde_json::json!({"$unsupported": type_name}),
        }
    }
}

// Equality follows `Ord`, so the two never disagree: floats compare through
// `f64::total_cmp`, which makes `NaN` equal to itself, and numerically equal
// values are equal across `Int` and `Float`. Consumers that must not fold a
// representation change (`Int(1)` edited to `Float(1.0)`) into equality use
// [`Value::same_representation`].
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Ordering of an integer against a float, exact at every magnitude.
///
/// Promoting the integer with `as f64` rounds above 2^53, which made
/// `Int(2^53 + 1) == Float(2^53) == Int(2^53)` while the two integers
/// differ — a broken `Eq`. Comparing through the float's integer part and
/// fraction instead keeps `cmp` a total order at every magnitude.
fn cmp_int_float(integer: i64, float: f64) -> Ordering {
    if float.is_nan() {
        // `total_cmp` places a negative NaN below every finite value and a
        // positive NaN above every one; `is_sign_negative` picks the side.
        if float.is_sign_negative() {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    } else {
        let truncated = float.trunc();
        if truncated >= i64::MAX as f64 {
            // The next representable integer at this magnitude is 2^63,
            // beyond `i64::MAX`, so no `i64` reaches the float.
            Ordering::Less
        } else if truncated < i64::MIN as f64 {
            Ordering::Greater
        } else {
            match integer.cmp(&(truncated as i64)) {
                Ordering::Equal => {
                    let fraction = float - truncated;
                    if fraction > 0.0 {
                        Ordering::Less
                    } else if fraction < 0.0 {
                        Ordering::Greater
                    } else if integer == 0 && float.is_sign_negative() {
                        // `-0.0` sorts below `0` the way `total_cmp` sorts it
                        // below `0.0`; treating it as equal would break
                        // transitivity (`Int(0)` would equal both zero signs
                        // while they stay unequal to each other).
                        Ordering::Greater
                    } else {
                        Ordering::Equal
                    }
                }
                other => other,
            }
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        use Value::*;

        match (self, other) {
            // Nulls last (SQL standard behavior)
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Greater,
            (_, Null) => Ordering::Less,

            // Same type comparisons
            (Bool(a), Bool(b)) => a.cmp(b),
            (Int(a), Int(b)) => a.cmp(b),
            (Float(a), Float(b)) => a.total_cmp(b),
            (Text(a), Text(b)) => a.cmp(b),
            (Bytes(a), Bytes(b)) => a.cmp(b),
            (Json(a), Json(b)) => a.cmp(b),
            (Decimal(a), Decimal(b)) => a.cmp(b),
            (DateTime(a), DateTime(b)) => a.cmp(b),
            (Date(a), Date(b)) => a.cmp(b),
            (Time(a), Time(b)) => a.cmp(b),
            (ObjectId(a), ObjectId(b)) => a.cmp(b),
            (Unsupported(a), Unsupported(b)) => a.cmp(b),
            (Array(a), Array(b)) => a.cmp(b),
            (Document(a), Document(b)) => a.cmp(b),

            // Cross-type numeric promotion, exact at every magnitude so
            // `Eq` stays transitive above 2^53.
            (Int(a), Float(b)) => cmp_int_float(*a, *b),
            (Float(a), Int(b)) => cmp_int_float(*b, *a).reverse(),

            // Different types: fallback to type order
            _ => self.type_order().cmp(&other.type_order()),
        }
    }
}

impl Eq for Value {}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn samples() -> Vec<(&'static str, Value)> {
        vec![
            ("null", Value::Null),
            ("bool", Value::Bool(true)),
            ("int", Value::Int(1)),
            ("float", Value::Float(1.5)),
            ("float_nan", Value::Float(f64::NAN)),
            ("text", Value::Text("1".into())),
            ("bytes", Value::Bytes(vec![1, 2])),
            ("json", Value::Json("{}".into())),
            ("decimal", Value::Decimal("1.00".into())),
            (
                "datetime",
                Value::DateTime(Utc.with_ymd_and_hms(2026, 9, 22, 14, 2, 11).unwrap()),
            ),
            (
                "date",
                Value::Date(NaiveDate::from_ymd_opt(2026, 9, 22).unwrap()),
            ),
            (
                "time",
                Value::Time(NaiveTime::from_hms_opt(14, 2, 11).unwrap()),
            ),
            ("array", Value::Array(vec![Value::Int(1)])),
            ("array_nan", Value::Array(vec![Value::Float(f64::NAN)])),
            (
                "document",
                Value::Document(BTreeMap::from([("a".to_string(), Value::Int(1))])),
            ),
            (
                "document_nan",
                Value::Document(BTreeMap::from([("m".to_string(), Value::Float(f64::NAN))])),
            ),
            ("object_id", Value::ObjectId("a".repeat(24))),
            ("unsupported", Value::Unsupported("custom".into())),
        ]
    }

    #[test]
    fn nan_equals_itself() {
        let value = Value::Float(f64::NAN);
        let same = Value::Float(f64::NAN);
        assert!(value == same, "NaN must equal itself, as Ord does");
    }

    #[test]
    fn numerically_equal_values_of_different_types_are_equal() {
        assert!(Value::Int(1) == Value::Float(1.0));
    }

    #[test]
    fn cross_type_pairs_that_ord_separates_stay_unequal() {
        let value = Value::Int(1);
        let other = Value::Decimal("1.00".into());
        assert_ne!(value.cmp(&other), Ordering::Equal);
        assert!(value != other);
    }

    #[test]
    fn same_representation_requires_the_same_variant_at_every_level() {
        let document = |value: Value| Value::Document(BTreeMap::from([("qty".to_string(), value)]));
        let array = |value: Value| Value::Array(vec![value]);

        assert!(Value::Int(1).same_representation(&Value::Int(1)));
        assert!(Value::Float(f64::NAN).same_representation(&Value::Float(f64::NAN)));
        assert!(document(Value::Int(1)).same_representation(&document(Value::Int(1))));

        let promoted = Value::Float(1.0);
        assert!(!Value::Int(1).same_representation(&promoted));
        assert!(!document(Value::Int(1)).same_representation(&document(promoted.clone())));
        assert!(!array(Value::Int(1)).same_representation(&array(promoted)));
    }

    #[test]
    fn equality_agrees_with_ordering_across_every_variant() {
        let samples = samples();

        for (a_name, a) in &samples {
            for (b_name, b) in &samples {
                assert_eq!(
                    a == b,
                    a.cmp(b) == Ordering::Equal,
                    "{a_name} == {b_name} must agree with cmp"
                );
            }
        }

        for (name, value) in samples {
            let same = value.clone();
            assert!(value == same, "{name} must equal itself, NaN included");
        }
    }
}
