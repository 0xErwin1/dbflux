use gpui::{Pixels, px};

use crate::tokens::GridMetrics;

/// Height of each data row, its 1 px divider included.
pub const ROW_HEIGHT: Pixels = GridMetrics::ROW_HEIGHT;

/// Height of the header row, its bottom edge included.
pub const HEADER_HEIGHT: Pixels = GridMetrics::HEADER_HEIGHT;

/// Horizontal padding inside cells.
pub const CELL_PADDING_X: Pixels = GridMetrics::CELL_PADDING_X;

/// Width of the row-number column at the start of every row.
pub const ROW_NUMBER_WIDTH: Pixels = GridMetrics::ROW_NUMBER_WIDTH;

/// Vertical padding inside cells.
#[allow(dead_code)]
pub const CELL_PADDING_Y: Pixels = px(4.0); // guardrail-allow: domain const, do not fold into Spacing

/// Minimum width for a column.
#[allow(dead_code)]
pub const MIN_COLUMN_WIDTH: f32 = 50.0;

/// Default width for a column.
pub const DEFAULT_COLUMN_WIDTH: f32 = 120.0;

/// Width of the scrollbar.
pub const SCROLLBAR_WIDTH: Pixels = px(12.0); // guardrail-allow: domain const, scrollbar width

/// Width of the name column in record mode.
pub const RECORD_NAME_WIDTH: Pixels = px(220.0); // guardrail-allow: domain const, record-mode label column

/// Minimum height of a record-mode field row. Values are single-line like the
/// grid, so this mirrors `ROW_HEIGHT` with a little more breathing room.
pub const RECORD_ROW_HEIGHT: Pixels = px(30.0); // guardrail-allow: domain const, record-mode row height
