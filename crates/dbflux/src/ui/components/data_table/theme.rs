use gpui::{px, Pixels};

/// Height of each data row.
pub const ROW_HEIGHT: Pixels = px(28.0);

/// Height of the header row.
pub const HEADER_HEIGHT: Pixels = px(32.0);

/// Horizontal padding inside cells.
pub const CELL_PADDING_X: Pixels = px(8.0);

/// Vertical padding inside cells.
pub const CELL_PADDING_Y: Pixels = px(4.0);

/// Minimum width for a column.
#[allow(dead_code)]
pub const MIN_COLUMN_WIDTH: f32 = 50.0;

/// Default width for a column.
pub const DEFAULT_COLUMN_WIDTH: f32 = 120.0;

/// Width of the scrollbar.
pub const SCROLLBAR_WIDTH: Pixels = px(12.0);

/// Sort indicator for ascending sort.
pub const SORT_INDICATOR_ASC: &str = "↑";

/// Sort indicator for descending sort.
pub const SORT_INDICATOR_DESC: &str = "↓";
