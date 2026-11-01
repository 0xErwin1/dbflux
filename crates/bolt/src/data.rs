//! The row-and-cell data contract a grid renderer asks for.
//!
//! A renderer that draws rows of cells should not know where the rows come
//! from: a query result, a request model, a history list. This module is the
//! seam between the two. A data owner implements [`RowSource`] and hands the
//! renderer [`ColumnView`] / [`CellView`] snapshots — display text already
//! formatted, a tone that says how the cell is emphasized, and a row state
//! that says how the row stands relative to its source.
//!
//! The contract carries display facts only. No formatting rules live here:
//! the data owner formats its values before it hands a [`CellView`] over,
//! so a renderer never re-derives precision, timezone or locale rules.

use gpui::{Pixels, SharedString};

/// Coarse semantic class of a column, as a renderer uses it: which cells
/// right-align, how time and boolean values are presented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    /// A string-like value drawn as text.
    Text,
    /// A whole number.
    Integer,
    /// A floating-point number.
    Float,
    /// A date, a time, or a timestamp.
    Timestamp,
    /// A true/false value.
    Boolean,
    /// The owner could not classify the column.
    Unknown,
}

impl ColumnKind {
    /// Whether values of this kind read better right-aligned: the numeric
    /// kinds. Everything else aligns left.
    pub fn is_numeric(self) -> bool {
        matches!(self, Self::Integer | Self::Float)
    }
}

/// Horizontal alignment of a cell's text within its column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellAlignment {
    Left,
    Center,
    Right,
}

impl CellAlignment {
    /// The alignment a column of `kind` reads with: numeric kinds right,
    /// the rest left.
    pub fn for_kind(kind: ColumnKind) -> Self {
        if kind.is_numeric() {
            Self::Right
        } else {
            Self::Left
        }
    }
}

/// The emphasis a cell's text is drawn with. A tone names a rendering
/// treatment (color, italic, strikethrough), not a meaning: the same tone
/// may be produced by different situations in different surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellTone {
    /// An ordinary value in the ordinary text color.
    Normal,
    /// The cell holds nothing; rendered distinct from any written value.
    Null,
    /// A legend or stand-in drawn muted: a column default, a placeholder
    /// the host chose, an auto-generated expression.
    Muted,
    /// A field the row's document does not have (as opposed to a field
    /// holding null).
    Missing,
    /// A summary of a nested value that can be stepped into rather than
    /// edited in place (`{4 fields}`, `[3]`).
    Nested,
    /// The cell holds a staged, not-yet-saved edit.
    Dirty,
    /// The cell sits in a row that is a new, not-yet-saved insert.
    PendingInsert,
    /// The cell sits in a row marked for deletion; renderers typically mute
    /// it and strike it through.
    PendingDelete,
    /// The cell sits in a row whose last save failed.
    Error,
}

/// How a row stands relative to its data source, as a grid paints it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowVisualState {
    /// No pending change.
    Clean,
    /// Has unsaved local modifications.
    Dirty,
    /// Currently being saved.
    Saving,
    /// The last save failed.
    Error,
    /// A new row pending its insert.
    PendingInsert,
    /// An existing row marked for deletion.
    PendingDelete,
}

/// A column as a renderer draws it: its display name, its kind, and the
/// layout facts the header and cells read.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnView {
    /// The column's display name (the header caption).
    pub name: SharedString,
    /// The column's semantic class.
    pub kind: ColumnKind,
    /// The engine-side type name shown beside the name, already abbreviated
    /// by the owner. Empty when the owner has none.
    pub type_name: SharedString,
    /// How the column's cells align.
    pub align: CellAlignment,
    /// A fixed width. `None` lets the renderer size the column itself.
    pub width: Option<Pixels>,
}

impl ColumnView {
    /// A column of `name` and `kind`, aligned by [`CellAlignment::for_kind`],
    /// with no type name and no fixed width.
    pub fn new(name: impl Into<SharedString>, kind: ColumnKind) -> Self {
        Self {
            name: name.into(),
            kind,
            type_name: SharedString::default(),
            align: CellAlignment::for_kind(kind),
            width: None,
        }
    }

    /// Attach the engine-side type name shown beside the display name.
    pub fn with_type_name(mut self, type_name: impl Into<SharedString>) -> Self {
        self.type_name = type_name.into();
        self
    }

    /// Give the column a fixed width instead of letting the renderer size it.
    pub fn with_fixed_width(mut self, width: Pixels) -> Self {
        self.width = Some(width);
        self
    }
}

/// A single cell as a renderer draws it: display text already formatted by
/// the owner, the emphasis it is drawn with, and its alignment.
#[derive(Debug, Clone, PartialEq)]
pub struct CellView {
    /// The text the renderer draws. Already truncated and formatted; the
    /// renderer never reformats it.
    pub text: SharedString,
    /// The emphasis the text is drawn with.
    pub tone: CellTone,
    /// How the text aligns in its column.
    pub align: CellAlignment,
}

impl CellView {
    pub fn new(text: impl Into<SharedString>, tone: CellTone, align: CellAlignment) -> Self {
        Self {
            text: text.into(),
            tone,
            align,
        }
    }
}

/// The read side of a grid: what a renderer asks for, row by row.
///
/// Every index is answered with a defaulted value rather than a panic: a
/// `row` or `column` past the counts reads as no cell / no column, and a
/// `row` past the count reads as [`RowVisualState::Clean`]. Renderers that
/// drive their loops from the counts never hit these; a refresh that shrinks
/// the data under a stale index does.
pub trait RowSource {
    /// Number of rows, including any pending inserts the owner shows.
    fn row_count(&self) -> usize;

    /// Number of columns.
    fn column_count(&self) -> usize;

    /// Column `index`, or `None` past the count.
    fn column(&self, index: usize) -> Option<ColumnView>;

    /// The cell at `row`, `column`, or `None` past either count.
    fn cell(&self, row: usize, column: usize) -> Option<CellView>;

    /// How row `row` stands relative to its source. `Clean` past the count.
    fn row_state(&self, row: usize) -> RowVisualState;
}

/// A [`RowSource`] over columns, rows and states built in place.
///
/// The reference implementation of the trait's bounds semantics and the
/// stand-in a component test builds a table with.
#[derive(Debug, Clone, Default)]
pub struct StaticRows {
    columns: Vec<ColumnView>,
    rows: Vec<Vec<CellView>>,
    states: Vec<RowVisualState>,
}

impl StaticRows {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a column. Existing rows keep the cells they have; a later
    /// `cell` read past a row's length reads as no cell, like any other
    /// short row.
    pub fn push_column(&mut self, column: ColumnView) {
        self.columns.push(column);
    }

    /// Append a row with its cells and how it stands.
    pub fn push_row(&mut self, cells: Vec<CellView>, state: RowVisualState) {
        self.rows.push(cells);
        self.states.push(state);
    }
}

impl RowSource for StaticRows {
    fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn column_count(&self) -> usize {
        self.columns.len()
    }

    fn column(&self, index: usize) -> Option<ColumnView> {
        self.columns.get(index).cloned()
    }

    fn cell(&self, row: usize, column: usize) -> Option<CellView> {
        self.rows.get(row)?.get(column).cloned()
    }

    fn row_state(&self, row: usize) -> RowVisualState {
        self.states
            .get(row)
            .copied()
            .unwrap_or(RowVisualState::Clean)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_kinds_align_right_and_the_rest_left() {
        for kind in [ColumnKind::Integer, ColumnKind::Float] {
            assert_eq!(
                CellAlignment::for_kind(kind),
                CellAlignment::Right,
                "{kind:?} is numeric and reads right-aligned"
            );
        }

        for kind in [
            ColumnKind::Text,
            ColumnKind::Timestamp,
            ColumnKind::Boolean,
            ColumnKind::Unknown,
        ] {
            assert_eq!(
                CellAlignment::for_kind(kind),
                CellAlignment::Left,
                "{kind:?} is not numeric and reads left-aligned"
            );
        }
    }

    #[test]
    fn column_view_derives_alignment_from_kind_and_defaults_the_rest() {
        let column = ColumnView::new("amount", ColumnKind::Float);

        assert_eq!(column.name, "amount");
        assert_eq!(column.kind, ColumnKind::Float);
        assert_eq!(column.align, CellAlignment::Right);
        assert_eq!(column.type_name, "");
        assert_eq!(column.width, None);

        let decorated = ColumnView::new("created", ColumnKind::Timestamp)
            .with_type_name("timestamptz")
            .with_fixed_width(Pixels::from(120.0));

        assert_eq!(decorated.type_name, "timestamptz");
        assert_eq!(decorated.align, CellAlignment::Left);
        assert_eq!(decorated.width, Some(Pixels::from(120.0)));
    }

    #[test]
    fn cell_view_keeps_what_it_is_given() {
        let cell = CellView::new("42", CellTone::Dirty, CellAlignment::Right);

        assert_eq!(cell.text, "42");
        assert_eq!(cell.tone, CellTone::Dirty);
        assert_eq!(cell.align, CellAlignment::Right);
    }

    #[test]
    fn static_rows_answer_counts_cells_and_states() {
        let mut rows = StaticRows::new();
        rows.push_column(ColumnView::new("n", ColumnKind::Integer));
        rows.push_column(ColumnView::new("label", ColumnKind::Text));
        rows.push_row(
            vec![
                CellView::new("1", CellTone::Normal, CellAlignment::Right),
                CellView::new("one", CellTone::Normal, CellAlignment::Left),
            ],
            RowVisualState::Dirty,
        );
        rows.push_row(
            vec![CellView::new("NULL", CellTone::Null, CellAlignment::Right)],
            RowVisualState::PendingInsert,
        );

        assert_eq!(rows.row_count(), 2);
        assert_eq!(rows.column_count(), 2);
        assert_eq!(rows.column(0).map(|c| c.name), Some("n".into()));
        assert_eq!(rows.cell(0, 1).map(|c| c.text), Some("one".into()));
        assert_eq!(rows.row_state(0), RowVisualState::Dirty);
        assert_eq!(rows.row_state(1), RowVisualState::PendingInsert);
    }

    #[test]
    fn static_rows_answer_out_of_range_indexes_without_panicking() {
        let mut rows = StaticRows::new();
        rows.push_column(ColumnView::new("n", ColumnKind::Integer));
        rows.push_row(
            vec![CellView::new("1", CellTone::Normal, CellAlignment::Right)],
            RowVisualState::Clean,
        );

        assert_eq!(rows.column(1), None);
        assert_eq!(rows.cell(1, 0), None);
        assert_eq!(rows.cell(0, 1), None, "a short row has no cell there");
        assert_eq!(
            rows.row_state(1),
            RowVisualState::Clean,
            "a row past the count reads as clean"
        );
    }
}
