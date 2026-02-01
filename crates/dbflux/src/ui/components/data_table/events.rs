use dbflux_core::SortDirection;

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
}
