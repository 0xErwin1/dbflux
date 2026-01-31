use std::sync::Arc;

use dbflux_core::{ColumnMeta, QueryResult, Value};
use gpui::TextAlign;

#[derive(Debug, Clone)]
pub struct TableModel {
    pub columns: Vec<ColumnSpec>,
    pub rows: Vec<RowData>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub id: Arc<str>,
    pub title: Arc<str>,
    pub kind: ColumnKind,
    pub align: TextAlign,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Text,
    Integer,
    Float,
    Bool,
    Bytes,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct RowData {
    pub cells: Vec<CellValue>,
}

/// Cell values with pre-computed display strings.
/// The display_text is computed once at construction to avoid per-frame allocation.
#[derive(Debug, Clone)]
pub struct CellValue {
    pub kind: CellKind,
    display_text: Arc<str>,
}

#[derive(Debug, Clone)]
pub enum CellKind {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(Arc<str>),
    Bytes(usize),
}

impl TableModel {
    #[allow(dead_code)]
    pub fn new(columns: Vec<ColumnSpec>, rows: Vec<RowData>) -> Self {
        Self { columns, rows }
    }

    #[allow(dead_code)]
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn col_count(&self) -> usize {
        self.columns.len()
    }

    pub fn cell(&self, row: usize, col: usize) -> Option<&CellValue> {
        self.rows.get(row).and_then(|r| r.cells.get(col))
    }
}

impl From<&QueryResult> for TableModel {
    fn from(result: &QueryResult) -> Self {
        let columns = result.columns.iter().map(ColumnSpec::from).collect();

        let rows = result
            .rows
            .iter()
            .map(|row| RowData {
                cells: row.iter().map(CellValue::from).collect(),
            })
            .collect();

        Self { columns, rows }
    }
}

impl From<&ColumnMeta> for ColumnSpec {
    fn from(col: &ColumnMeta) -> Self {
        let kind = infer_column_kind(&col.type_name);
        let align = match kind {
            ColumnKind::Integer | ColumnKind::Float => TextAlign::Right,
            _ => TextAlign::Left,
        };

        Self {
            id: col.name.as_str().into(),
            title: col.name.as_str().into(),
            kind,
            align,
        }
    }
}

fn infer_column_kind(type_name: &str) -> ColumnKind {
    let lower = type_name.to_lowercase();

    if lower.contains("int") || lower.contains("serial") {
        ColumnKind::Integer
    } else if lower.contains("float")
        || lower.contains("double")
        || lower.contains("real")
        || lower.contains("numeric")
        || lower.contains("decimal")
    {
        ColumnKind::Float
    } else if lower.contains("bool") {
        ColumnKind::Bool
    } else if lower.contains("bytea") || lower.contains("blob") || lower.contains("binary") {
        ColumnKind::Bytes
    } else if lower.contains("text")
        || lower.contains("char")
        || lower.contains("varchar")
        || lower.contains("string")
        || lower.contains("json")
        || lower.contains("uuid")
        || lower.contains("timestamp")
        || lower.contains("datetime")
        || lower.contains("date")
        || lower.contains("time")
    {
        ColumnKind::Text
    } else {
        ColumnKind::Unknown
    }
}

impl From<&Value> for CellValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => CellValue::null(),
            Value::Bool(b) => CellValue::bool(*b),
            Value::Int(i) => CellValue::int(*i),
            Value::Float(f) => CellValue::float(*f),
            Value::Text(s) => CellValue::text(s.as_str()),
            Value::Bytes(b) => CellValue::bytes(b.len()),
        }
    }
}

const MAX_DISPLAY_LEN: usize = 200;

impl CellValue {
    pub fn null() -> Self {
        Self {
            kind: CellKind::Null,
            display_text: "NULL".into(),
        }
    }

    pub fn bool(b: bool) -> Self {
        Self {
            kind: CellKind::Bool(b),
            display_text: if b { "true" } else { "false" }.into(),
        }
    }

    pub fn int(i: i64) -> Self {
        Self {
            kind: CellKind::Int(i),
            display_text: i.to_string().into(),
        }
    }

    pub fn float(f: f64) -> Self {
        let display = if f.fract() == 0.0 && f.abs() < 1e15 {
            format!("{:.1}", f)
        } else {
            f.to_string()
        };
        Self {
            kind: CellKind::Float(f),
            display_text: display.into(),
        }
    }

    pub fn text(s: &str) -> Self {
        let display_text = if s.len() <= MAX_DISPLAY_LEN {
            Arc::from(s)
        } else {
            let truncated: String = s.chars().take(MAX_DISPLAY_LEN).collect();
            format!("{}...", truncated).into()
        };
        Self {
            kind: CellKind::Text(s.into()),
            display_text,
        }
    }

    pub fn bytes(len: usize) -> Self {
        Self {
            kind: CellKind::Bytes(len),
            display_text: format!("<{} bytes>", len).into(),
        }
    }

    /// Returns the pre-computed display text. Cloning Arc<str> is cheap (reference count bump).
    pub fn display_text(&self) -> Arc<str> {
        self.display_text.clone()
    }

    pub fn is_null(&self) -> bool {
        matches!(self.kind, CellKind::Null)
    }
}
