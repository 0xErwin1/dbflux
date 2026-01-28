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

/// Cell values use Arc<str> for text to avoid cloning large strings.
/// Bytes only stores length for display.
#[derive(Debug, Clone)]
pub enum CellValue {
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
        let columns = result
            .columns
            .iter()
            .map(|col| ColumnSpec::from(col))
            .collect();

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
            Value::Null => CellValue::Null,
            Value::Bool(b) => CellValue::Bool(*b),
            Value::Int(i) => CellValue::Int(*i),
            Value::Float(f) => CellValue::Float(*f),
            Value::Text(s) => CellValue::Text(s.as_str().into()),
            Value::Bytes(b) => CellValue::Bytes(b.len()),
        }
    }
}

impl CellValue {
    pub fn display_string(&self) -> String {
        self.display_string_truncated(200)
    }

    pub fn display_string_truncated(&self, max_len: usize) -> String {
        match self {
            CellValue::Null => "NULL".to_string(),
            CellValue::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            CellValue::Int(i) => i.to_string(),
            CellValue::Float(f) => {
                if f.fract() == 0.0 && f.abs() < 1e15 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            CellValue::Text(s) => {
                if s.len() <= max_len {
                    s.to_string()
                } else {
                    let truncated: String = s.chars().take(max_len).collect();
                    format!("{}...", truncated)
                }
            }
            CellValue::Bytes(len) => format!("<{} bytes>", len),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, CellValue::Null)
    }
}
