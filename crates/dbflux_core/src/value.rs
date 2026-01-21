use serde::{Deserialize, Serialize};

/// Database value type.
///
/// Custom enum to avoid serde_json::Value overhead and enable proper
/// type-aware sorting, rendering, and CSV export.
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
