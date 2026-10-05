use gpui::{Pixels, px};

use crate::tokens::GridMetrics;

/// Height of the header row at the default grid font size when a column
/// shows facts on a second line.
pub const ANNOTATED_HEADER_HEIGHT: Pixels = px(48.0);

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

/// Widest a column opens to fit its values. A longer value is cut, and the
/// column can still be dragged wider.
pub const MAX_AUTO_COLUMN_WIDTH: f32 = 240.0;

/// Rows read to find a column's longest value when it opens.
pub const AUTO_WIDTH_SAMPLE_ROWS: usize = 200;

/// Room kept past the longest value, so rounding in glyph advances never
/// cuts its last character.
pub const AUTO_WIDTH_SLACK: f32 = 2.0;

/// Width of the scrollbar.
pub const SCROLLBAR_WIDTH: Pixels = px(12.0); // guardrail-allow: domain const, scrollbar width

/// Width of the name column in record mode.
pub const RECORD_NAME_WIDTH: Pixels = px(220.0); // guardrail-allow: domain const, record-mode label column
