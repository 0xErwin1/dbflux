use dbflux_core::SortDirection;
use gpui::{Pixels, Point};

use super::selection::SelectionState;

/// Direction for navigation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
    Left,
    Right,
}

/// Edge for navigation (Home/End, Ctrl+Home/Ctrl+End).
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Edge {
    Top,
    Bottom,
    Left,
    Right,
    Home,
    End,
}

/// Sort state for a single column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortState {
    pub column_ix: usize,
    pub direction: SortDirection,
}

impl SortState {
    pub fn new(column_ix: usize, direction: SortDirection) -> Self {
        Self {
            column_ix,
            direction,
        }
    }

    pub fn ascending(column_ix: usize) -> Self {
        Self::new(column_ix, SortDirection::Ascending)
    }

    pub fn descending(column_ix: usize) -> Self {
        Self::new(column_ix, SortDirection::Descending)
    }
}

/// Actions available in the context menu.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextMenuAction {
    /// Copy the selected cell value to clipboard.
    Copy,
    /// Paste from clipboard into the selected cell.
    Paste,
    /// Start inline editing of the selected cell.
    Edit,
    /// Open modal editor for the selected cell.
    EditInModal,
    /// Set the cell to its column's default value.
    SetDefault,
    /// Set the cell to NULL.
    SetNull,
    /// Insert a new row.
    AddRow,
    /// Duplicate the current row.
    DuplicateRow,
    /// Delete the current row.
    DeleteRow,
    /// Generate SELECT ... WHERE with row values.
    GenerateSelectWhere,
    /// Generate INSERT statement with row values.
    GenerateInsert,
    /// Generate UPDATE statement with row values.
    GenerateUpdate,
    /// Generate DELETE statement with row's primary key.
    GenerateDelete,
}

/// Events emitted by the DataTable component.
#[derive(Debug, Clone)]
pub enum DataTableEvent {
    /// Sort state changed (None means no sort).
    SortChanged(Option<SortState>),

    /// Selection changed.
    #[allow(dead_code)]
    SelectionChanged(SelectionState),

    /// Table received focus (clicked or otherwise activated).
    Focused,

    /// Request to save changes for a row.
    SaveRowRequested(usize),

    /// Request to show context menu at a position.
    ContextMenuRequested {
        row: usize,
        col: usize,
        position: Point<Pixels>,
    },

    // === Keyboard-triggered row operations ===

    /// Request to delete the current row (dd or Delete key).
    DeleteRowRequested(usize),

    /// Request to add a new row after the current row (aa).
    AddRowRequested(usize),

    /// Request to duplicate the current row (AA).
    DuplicateRowRequested(usize),

    /// Request to set the current cell to NULL (Ctrl+N).
    SetNullRequested { row: usize, col: usize },

    /// Request to copy the entire row as CSV (YY).
    CopyRowRequested(usize),

    /// Request to open modal editor for JSON/long text.
    ModalEditRequested {
        row: usize,
        col: usize,
        value: String,
        is_json: bool,
    },
}
