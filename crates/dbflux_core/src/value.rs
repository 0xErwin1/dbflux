use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Database value type.
///
/// Custom enum instead of `serde_json::Value` to enable proper type-aware
/// sorting, efficient rendering, and clean CSV export without JSON overhead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
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
            Value::Text(s) => {
                if s.len() <= max_len {
                    s.clone()
                } else {
                    let truncated: String = s.chars().take(max_len).collect();
                    format!("{}...", truncated)
                }
            }
            Value::Bytes(b) => format!("<{} bytes>", b.len()),
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
            Value::Text(_) => 3,
            Value::Bytes(_) => 4,
            Value::Null => 5,
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

            // Cross-type numeric promotion
            (Int(a), Float(b)) => (*a as f64).total_cmp(b),
            (Float(a), Int(b)) => a.total_cmp(&(*b as f64)),

            // Different types: fallback to type order
            _ => self.type_order().cmp(&other.type_order()),
        }
    }
}

impl Eq for Value {}
