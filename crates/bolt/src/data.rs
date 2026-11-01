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
//! What a cell draws beyond one text — a two-tone foreign-key label, a
//! staged edit shown against the value it replaced, a nested-value
//! marker — the [`CellView`] fields name explicitly.

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
    /// Whether the column is a child of an expanded object group in a
    /// document grid; a renderer washes the column's header and cells to
    /// mark them as belonging to the group.
    pub group_child: bool,
}

impl ColumnView {
    /// A column of `name` and `kind`, aligned by [`CellAlignment::for_kind`],
    /// with no type name, no fixed width and no group.
    pub fn new(name: impl Into<SharedString>, kind: ColumnKind) -> Self {
        Self {
            name: name.into(),
            kind,
            type_name: SharedString::default(),
            align: CellAlignment::for_kind(kind),
            width: None,
            group_child: false,
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

    /// Mark whether the column is a child of an expanded object group in a
    /// document grid.
    pub fn with_group_child(mut self, group_child: bool) -> Self {
        self.group_child = group_child;
        self
    }
}

/// What a cell draws as its content.
#[derive(Debug, Clone, PartialEq)]
pub enum CellContent {
    /// A single text drawn as the cell's whole content.
    Plain(SharedString),
    /// A staged edit drawn against the value it replaced: `from` first,
    /// then `to`, with an arrow between them. A document grid shows a
    /// staged edit this way instead of the plain dirty mark, so a renderer
    /// tells the two treatments apart by which variant the cell carries.
    ///
    /// The transition takes the color precedence: the content is drawn in
    /// the transition treatment whatever the [`CellTone`] says, and the
    /// tone still carries the cell's null-ness and marks (italic,
    /// strikethrough) on top of it.
    Transition {
        /// The text the cell drew before the edit was staged.
        from: SharedString,
        /// The staged text the cell draws now.
        to: SharedString,
    },
}

/// A second text a cell draws after its content, with its own emphasis.
/// A resolved foreign key draws its label with the raw id muted after it.
#[derive(Debug, Clone, PartialEq)]
pub struct CellSpan {
    /// The trailing text (` · #8` after a foreign-key label).
    pub text: SharedString,
    /// The emphasis the trailing text is drawn with.
    pub tone: CellTone,
}

/// Marks a cell whose content summarizes a value the grid can step into
/// rather than edit in place (`{4 fields}`, `[3]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NestedMarker {
    /// Whether the summary stands for an array rather than a document; a
    /// renderer draws stepping into an array differently from stepping
    /// into a document.
    pub is_array: bool,
}

/// A single cell as a renderer draws it: what it draws as its content,
/// the emphasis that content is drawn with, and its alignment.
#[derive(Debug, Clone, PartialEq)]
pub struct CellView {
    /// What the cell draws as its content. Already composed and formatted;
    /// the renderer never reformats it.
    pub content: CellContent,
    /// The emphasis the content is drawn with.
    pub tone: CellTone,
    /// How the content aligns in its column.
    pub align: CellAlignment,
    /// A second text drawn after the content with its own tone, if the
    /// cell draws one. When present it replaces the content as the text
    /// the cell leads with.
    pub secondary: Option<CellSpan>,
    /// The nested-value marker, if the content summarizes a value the
    /// grid can step into.
    pub nested: Option<NestedMarker>,
}

impl CellView {
    /// A plain cell of `text`, `tone` and `align`.
    pub fn new(text: impl Into<SharedString>, tone: CellTone, align: CellAlignment) -> Self {
        Self {
            content: CellContent::Plain(text.into()),
            tone,
            align,
            secondary: None,
            nested: None,
        }
    }

    /// A cell showing a staged edit against the value it replaced.
    pub fn transition(
        from: impl Into<SharedString>,
        to: impl Into<SharedString>,
        tone: CellTone,
        align: CellAlignment,
    ) -> Self {
        Self {
            content: CellContent::Transition {
                from: from.into(),
                to: to.into(),
            },
            tone,
            align,
            secondary: None,
            nested: None,
        }
    }

    /// Draw a second text after the content with its own tone.
    pub fn with_secondary(mut self, span: CellSpan) -> Self {
        self.secondary = Some(span);
        self
    }

    /// Mark the content as a summary of a value the grid can step into.
    pub fn with_nested(mut self, marker: NestedMarker) -> Self {
        self.nested = Some(marker);
        self
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

    /// The text a plain cell draws, failing the test on any other content.
    fn plain(cell: CellView) -> SharedString {
        match cell.content {
            CellContent::Plain(text) => text,
            other => panic!("expected a plain cell, got {other:?}"),
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
        assert!(!column.group_child, "a column defaults to no group");

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

        assert_eq!(plain(cell.clone()), "42");
        assert_eq!(cell.tone, CellTone::Dirty);
        assert_eq!(cell.align, CellAlignment::Right);
        assert_eq!(cell.secondary, None);
        assert_eq!(cell.nested, None);
    }

    #[test]
    fn cell_view_carries_its_extras() {
        let cell = CellView::transition("old", "new", CellTone::Dirty, CellAlignment::Left)
            .with_secondary(CellSpan {
                text: " · #8".into(),
                tone: CellTone::Muted,
            })
            .with_nested(NestedMarker { is_array: true });

        assert_eq!(
            cell.content,
            CellContent::Transition {
                from: "old".into(),
                to: "new".into()
            }
        );
        let secondary = cell.secondary.expect("the span is attached");
        assert_eq!(secondary.text, " · #8");
        assert_eq!(secondary.tone, CellTone::Muted);
        assert_eq!(cell.nested, Some(NestedMarker { is_array: true }));
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
        assert_eq!(rows.cell(0, 1).map(plain), Some("one".into()));
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
