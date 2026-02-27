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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
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

            // Cross-type numeric promotion
            (Int(a), Float(b)) => (*a as f64).total_cmp(b),
            (Float(a), Int(b)) => a.total_cmp(&(*b as f64)),

            // Different types: fallback to type order
            _ => self.type_order().cmp(&other.type_order()),
        }
    }
}

impl Eq for Value {}
