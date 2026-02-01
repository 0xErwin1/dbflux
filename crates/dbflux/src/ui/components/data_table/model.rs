use std::collections::HashMap;
use std::sync::Arc;

use dbflux_core::{ColumnMeta, QueryResult, RowState, Value};
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
            Value::Json(s) | Value::Decimal(s) => CellValue::text(s.as_str()),
            Value::DateTime(dt) => CellValue::text(&dt.format("%Y-%m-%d %H:%M:%S").to_string()),
            Value::Date(d) => CellValue::text(&d.format("%Y-%m-%d").to_string()),
            Value::Time(t) => CellValue::text(&t.format("%H:%M:%S").to_string()),
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

/// Buffer for tracking local edits before committing to the database.
///
/// Acts as an overlay on top of the read-only TableModel. When rendering,
/// cells check the buffer first; if an override exists, it's displayed instead
/// of the base value.
#[derive(Debug, Clone, Default)]
pub struct EditBuffer {
    /// Cell overrides: (row_idx, col_idx) -> new value.
    overrides: HashMap<(usize, usize), CellValue>,

    /// State per row (only tracked for rows with changes).
    row_states: HashMap<usize, RowState>,
}

impl EditBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the display value for a cell, checking the buffer first.
    pub fn get_cell<'a>(&'a self, row: usize, col: usize, base: &'a CellValue) -> &'a CellValue {
        self.overrides.get(&(row, col)).unwrap_or(base)
    }

    /// Check if a cell has been modified.
    pub fn is_cell_dirty(&self, row: usize, col: usize) -> bool {
        self.overrides.contains_key(&(row, col))
    }

    /// Set a cell value in the buffer.
    pub fn set_cell(&mut self, row: usize, col: usize, value: CellValue) {
        self.overrides.insert((row, col), value);
        self.row_states.insert(row, RowState::Dirty);
    }

    /// Clear all overrides for a specific row.
    pub fn clear_row(&mut self, row: usize) {
        self.overrides.retain(|&(r, _), _| r != row);
        self.row_states.remove(&row);
    }

    /// Clear all overrides.
    pub fn clear_all(&mut self) {
        self.overrides.clear();
        self.row_states.clear();
    }

    /// Check if there are any pending changes.
    pub fn has_changes(&self) -> bool {
        !self.overrides.is_empty()
    }

    /// Get the number of dirty rows.
    pub fn dirty_row_count(&self) -> usize {
        self.row_states
            .values()
            .filter(|s| s.is_dirty())
            .count()
    }

    /// Get the state of a row.
    pub fn row_state(&self, row: usize) -> &RowState {
        self.row_states.get(&row).unwrap_or(&RowState::Clean)
    }

    /// Set the state of a row.
    pub fn set_row_state(&mut self, row: usize, state: RowState) {
        if state.is_clean() {
            self.row_states.remove(&row);
        } else {
            self.row_states.insert(row, state);
        }
    }

    /// Get all dirty rows (row indices).
    pub fn dirty_rows(&self) -> Vec<usize> {
        self.row_states
            .iter()
            .filter(|(_, s)| s.is_dirty())
            .map(|(&row, _)| row)
            .collect()
    }

    /// Get all changes for a specific row as (col_idx, CellValue) pairs.
    pub fn row_changes(&self, row: usize) -> Vec<(usize, &CellValue)> {
        self.overrides
            .iter()
            .filter(|&(&(r, _), _)| r == row)
            .map(|(&(_, col), val)| (col, val))
            .collect()
    }
}
