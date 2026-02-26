use dbflux_core::Value;
use dbflux_core::chrono::{DateTime, NaiveDate, Utc};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Unique identifier for a tree node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId {
    /// Path from root to this node (e.g., ["0", "address", "city"]).
    pub path: Vec<String>,
}

impl NodeId {
    pub fn root(doc_index: usize) -> Self {
        Self {
            path: vec![doc_index.to_string()],
        }
    }

    pub fn child(&self, key: &str) -> Self {
        let mut path = self.path.clone();
        path.push(key.to_string());
        Self { path }
    }

    pub fn parent(&self) -> Option<Self> {
        if self.path.len() <= 1 {
            return None;
        }
        let mut path = self.path.clone();
        path.pop();
        Some(Self { path })
    }

    pub fn depth(&self) -> usize {
        self.path.len().saturating_sub(1)
    }

    pub fn is_root(&self) -> bool {
        self.path.len() == 1
    }

    /// Returns the document index for root nodes.
    pub fn doc_index(&self) -> Option<usize> {
        self.path.first().and_then(|s| s.parse().ok())
    }
}

/// Value stored in a tree node.
#[derive(Debug, Clone)]
pub enum NodeValue {
    /// Primitive scalar value.
    Scalar(Value),
    /// Object/document with fields.
    Document(BTreeMap<String, Value>),
    /// Array with elements.
    Array(Vec<Value>),
}

impl NodeValue {
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Document(fields) => NodeValue::Document(fields.clone()),
            Value::Array(items) => NodeValue::Array(items.clone()),
            _ => NodeValue::Scalar(value.clone()),
        }
    }

    pub fn is_expandable(&self) -> bool {
        match self {
            NodeValue::Scalar(_) => false,
            NodeValue::Document(fields) => !fields.is_empty(),
            NodeValue::Array(items) => !items.is_empty(),
        }
    }

    /// Get a preview string for the value (truncated for display).
    pub fn preview(&self) -> Arc<str> {
        match self {
            NodeValue::Scalar(v) => format_value_preview(v, true).into(),
            NodeValue::Document(fields) => format!("{{{} fields}}", fields.len()).into(),
            NodeValue::Array(items) => format!("[{} items]", items.len()).into(),
        }
    }

    /// Get the full value string (not truncated).
    pub fn full_preview(&self) -> Arc<str> {
        match self {
            NodeValue::Scalar(v) => format_value_preview(v, false).into(),
            NodeValue::Document(fields) => format!("{{{} fields}}", fields.len()).into(),
            NodeValue::Array(items) => format!("[{} items]", items.len()).into(),
        }
    }

    /// Get the type label for display.
    pub fn type_label(&self) -> &'static str {
        match self {
            NodeValue::Scalar(v) => match v {
                Value::Null => "null",
                Value::Bool(_) => "bool",
                Value::Int(_) => "int",
                Value::Float(_) => "float",
                Value::Text(_) => "string",
                Value::ObjectId(_) => "ObjectId",
                Value::DateTime(_) => "datetime",
                Value::Date(_) => "date",
                Value::Time(_) => "time",
                Value::Bytes(_) => "bytes",
                Value::Decimal(_) => "decimal",
                Value::Json(_) => "json",
                Value::Document(_) | Value::Array(_) => unreachable!(),
            },
            NodeValue::Document(_) => "object",
            NodeValue::Array(_) => "array",
        }
    }
}

/// A node in the document tree.
#[derive(Debug, Clone)]
pub struct TreeNode {
    pub id: NodeId,
    pub key: Arc<str>,
    pub value: NodeValue,
    pub depth: usize,
    pub parent_id: Option<NodeId>,
}

impl TreeNode {
    pub fn new(id: NodeId, key: &str, value: NodeValue, parent_id: Option<NodeId>) -> Self {
        let depth = id.depth();
        Self {
            id,
            key: key.into(),
            value,
            depth,
            parent_id,
        }
    }

    pub fn is_expandable(&self) -> bool {
        self.value.is_expandable()
    }

    /// Generate child nodes for this node.
    pub fn children(&self) -> Vec<TreeNode> {
        match &self.value {
            NodeValue::Scalar(_) => Vec::new(),
            NodeValue::Document(fields) => fields
                .iter()
                .map(|(k, v)| {
                    let child_id = self.id.child(k);
                    TreeNode::new(child_id, k, NodeValue::from_value(v), Some(self.id.clone()))
                })
                .collect(),
            NodeValue::Array(items) => items
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let key = i.to_string();
                    let child_id = self.id.child(&key);
                    TreeNode::new(
                        child_id,
                        &key,
                        NodeValue::from_value(v),
                        Some(self.id.clone()),
                    )
                })
                .collect(),
        }
    }
}

const MAX_PREVIEW_LEN: usize = 80;

fn format_value_preview(value: &Value, truncate: bool) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.fract() == 0.0 && f.abs() < 1e15 {
                format!("{:.1}", f)
            } else {
                f.to_string()
            }
        }
        Value::Text(s) => {
            let escaped: String = s
                .chars()
                .flat_map(|c| match c {
                    '\n' => vec!['\\', 'n'],
                    '\r' => vec!['\\', 'r'],
                    '\t' => vec!['\\', 't'],
                    c => vec![c],
                })
                .collect();

            if truncate && (escaped.len() > MAX_PREVIEW_LEN || s.len() > MAX_PREVIEW_LEN) {
                format!("\"{}...\"", &escaped[..MAX_PREVIEW_LEN.min(escaped.len())])
            } else {
                format!("\"{}\"", escaped)
            }
        }
        Value::ObjectId(id) => format!("ObjectId(\"{}\")", id),
        Value::DateTime(dt) => format_datetime_relative(dt),
        Value::Date(d) => format_date_relative(d),
        Value::Time(t) => t.format("%H:%M:%S").to_string(),
        Value::Bytes(b) => format!("<{} bytes>", b.len()),
        Value::Decimal(d) => d.to_string(),
        Value::Json(j) => {
            let escaped: String = j
                .chars()
                .flat_map(|c| match c {
                    '\n' => vec!['\\', 'n'],
                    '\r' => vec!['\\', 'r'],
                    '\t' => vec!['\\', 't'],
                    c => vec![c],
                })
                .collect();

            if truncate && escaped.len() > MAX_PREVIEW_LEN {
                format!("{}...", &escaped[..MAX_PREVIEW_LEN.min(escaped.len())])
            } else {
                escaped
            }
        }
        Value::Document(fields) => format!("{{{} fields}}", fields.len()),
        Value::Array(items) => format!("[{} items]", items.len()),
    }
}

/// Format a datetime with relative format for recent dates.
fn format_datetime_relative(dt: &DateTime<Utc>) -> String {
    let now = Utc::now();
    let diff = now.signed_duration_since(*dt);

    // For dates in the future or very far in the past, use absolute format
    if diff.num_seconds() < 0 || diff.num_days() > 30 {
        return dt.format("%Y-%m-%d %H:%M:%S").to_string();
    }

    let seconds = diff.num_seconds();
    let minutes = diff.num_minutes();
    let hours = diff.num_hours();
    let days = diff.num_days();

    if seconds < 60 {
        "just now".to_string()
    } else if minutes < 60 {
        format!("{} min ago", minutes)
    } else if hours < 24 {
        format!("{} hr ago", hours)
    } else if days == 1 {
        "yesterday".to_string()
    } else {
        format!("{} days ago", days)
    }
}

/// Format a date with relative format for recent dates.
fn format_date_relative(d: &NaiveDate) -> String {
    let today = Utc::now().date_naive();
    let diff = today.signed_duration_since(*d);
    let days = diff.num_days();

    // For dates in the future or very far in the past, use absolute format
    if !(0..=30).contains(&days) {
        return d.format("%Y-%m-%d").to_string();
    }

    if days == 0 {
        "today".to_string()
    } else if days == 1 {
        "yesterday".to_string()
    } else {
        format!("{} days ago", days)
    }
}
