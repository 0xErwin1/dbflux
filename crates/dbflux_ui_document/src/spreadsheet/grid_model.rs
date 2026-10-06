//! One sheet of a workbook, as the data table shows it.
//!
//! Columns are titled with spreadsheet letters (A, B, …, Z, AA, …) and the
//! sheet's first row is a row of data like any other, so the grid matches
//! what a spreadsheet application shows. Every cell shows the text the reader
//! built for it, except a formula without a cached result, which a file gets
//! when DBFlux writes a formula or patches an ods file: it shows a legend
//! saying it is recalculated when opened, as empty text in every other
//! respect, so it is neither an edit nor written back. The formula of each
//! cell is kept apart, sparsely, for the formula readout, and so is what an
//! edit needs to know about a cell: which cells hold a boolean or a date, and
//! where appended rows go. Nothing here does I/O.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dbflux_components::components::data_table::TableModel;
use dbflux_components::components::data_table::model::{
    CellKind, CellValue, ColumnKind, ColumnSpec, RowData,
};
use dbflux_spreadsheet::{CellFormula, CellValue as SheetValue, SheetGrid};
use gpui::TextAlign;

/// How many columns an empty sheet that can be edited is shown with.
pub(super) const BLANK_SHEET_COLUMNS: usize = 10;

/// The resident sheet: its table, and the formulas of its cells.
pub(super) struct SheetModel {
    table: Arc<TableModel>,

    /// The formula text of every cell that has one, by zero-based
    /// `(row, column)`.
    formulas: HashMap<(usize, usize), Arc<str>>,

    /// Whether the format hides formula text, as xls does: then no cell is
    /// known to hold or not hold a formula.
    formulas_unavailable: bool,

    /// The cells that hold a boolean, by zero-based `(row, column)`.
    booleans: HashSet<(usize, usize)>,

    /// The cells that hold a date, which the reader reports only for a cell
    /// whose number format is a date format.
    dates: HashSet<(usize, usize)>,

    /// Per column, whether the lowest cell holding a value holds a date.
    column_ends_in_date: Vec<bool>,

    /// The zero-based file row of the first appended row. `None` for a
    /// format without a writer.
    append_row: Option<usize>,

    rows: usize,
    columns: usize,
}

/// What an edit needs to know about the cell it writes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct CellTarget {
    /// The cell holds a boolean, so `true` and `false` stay booleans.
    pub(super) holds_boolean: bool,

    /// The cell's number format is a date format, so an ISO date typed into
    /// it is a date.
    pub(super) date_format: bool,
}

/// What the formula readout says about one cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum FormulaReadout {
    /// The cell's formula as the file writes it.
    Formula(Arc<str>),
    /// The cell holds a value and no formula, or nothing.
    NoFormula,
    /// The format does not let DBFlux read formula text.
    Unavailable,
}

impl SheetModel {
    /// Builds the table of `grid`, consuming it so the sheet is held once in
    /// the table's cells rather than twice.
    pub(super) fn new(grid: SheetGrid) -> Self {
        let rows = grid.row_count();
        let columns = grid.column_count();

        let mut kinds = vec![KindScan::Empty; columns];
        let mut formulas = HashMap::new();
        let mut formulas_unavailable = false;
        let mut pending_legend: Option<String> = None;
        let mut booleans = HashSet::new();
        let mut dates = HashSet::new();
        let mut column_ends_in_date = vec![false; columns];
        let mut table_rows = Vec::with_capacity(rows);

        for row in 0..rows {
            let cells = grid.row(row).unwrap_or(&[]);
            let mut table_cells = Vec::with_capacity(columns);

            for column in 0..columns {
                let Some(cell) = cells.get(column) else {
                    table_cells.push(CellValue::text(""));
                    continue;
                };

                if let Some(scan) = kinds.get_mut(column) {
                    *scan = scan.with(&cell.value);
                }

                match &cell.value {
                    SheetValue::Empty => {}
                    SheetValue::Bool(_) => {
                        booleans.insert((row, column));
                    }
                    SheetValue::Date(_) => {
                        dates.insert((row, column));
                    }
                    _ => {}
                }

                if cell.value != SheetValue::Empty
                    && let Some(ends_in_date) = column_ends_in_date.get_mut(column)
                {
                    *ends_in_date = matches!(cell.value, SheetValue::Date(_));
                }

                match &cell.formula {
                    CellFormula::None => {}
                    CellFormula::Text(text) => {
                        formulas.insert((row, column), text.clone());
                    }
                    CellFormula::Unavailable => formulas_unavailable = true,
                }

                let awaits_recalculation =
                    cell.value == SheetValue::Empty && matches!(cell.formula, CellFormula::Text(_));

                if awaits_recalculation {
                    let legend = pending_legend
                        .get_or_insert_with(crate::labels::spreadsheet_formula_pending);
                    table_cells.push(CellValue::placeholder(legend));
                } else {
                    table_cells.push(CellValue::text(&cell.display));
                }
            }

            table_rows.push(RowData { cells: table_cells });
        }

        let specs = kinds
            .iter()
            .enumerate()
            .map(|(index, scan)| column_spec(index, *scan))
            .collect();

        Self {
            table: Arc::new(TableModel::new(specs, table_rows)),
            formulas,
            formulas_unavailable,
            booleans,
            dates,
            column_ends_in_date,
            append_row: grid.append_row().map(|append_row| append_row.max(rows)),
            rows,
            columns,
        }
    }

    /// An empty sheet as `columns` blank columns, so rows can be appended to
    /// it. The `rows` empty rows the reader padded the sheet with stay, so
    /// every row of the table is still the file row of its number, and
    /// appended rows are written from `append_row` on.
    pub(super) fn blank(rows: usize, columns: usize, append_row: usize) -> Self {
        let specs = (0..columns)
            .map(|index| column_spec(index, KindScan::Empty))
            .collect();

        let table_rows = (0..rows)
            .map(|_| RowData {
                cells: vec![CellValue::text(""); columns],
            })
            .collect();

        Self {
            table: Arc::new(TableModel::new(specs, table_rows)),
            formulas: HashMap::new(),
            formulas_unavailable: false,
            booleans: HashSet::new(),
            dates: HashSet::new(),
            column_ends_in_date: vec![false; columns],
            append_row: Some(append_row.max(rows)),
            rows,
            columns,
        }
    }

    /// The zero-based file row the first appended row is written at, past
    /// every row the table shows. `None` for a format without a writer.
    pub(super) fn append_row(&self) -> Option<usize> {
        self.append_row
    }

    /// Whether the cell at zero-based `row` and `column` holds a formula.
    pub(super) fn has_formula(&self, row: usize, column: usize) -> bool {
        self.formulas.contains_key(&(row, column))
    }

    /// What an edit of the cell at zero-based `row` and `column` needs to
    /// know. `row` is `None` for a cell of an appended row, which is given a
    /// date format when the lowest value of its column is a date: the
    /// patcher gives a new xlsx cell the style of the cell above it.
    pub(super) fn target(&self, row: Option<usize>, column: usize) -> CellTarget {
        match row {
            Some(row) => CellTarget {
                holds_boolean: self.booleans.contains(&(row, column)),
                date_format: self.dates.contains(&(row, column)),
            },

            None => CellTarget {
                holds_boolean: false,
                date_format: self
                    .column_ends_in_date
                    .get(column)
                    .copied()
                    .unwrap_or(false),
            },
        }
    }

    pub(super) fn table_model(&self) -> Arc<TableModel> {
        self.table.clone()
    }

    pub(super) fn row_count(&self) -> usize {
        self.rows
    }

    pub(super) fn column_count(&self) -> usize {
        self.columns
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows == 0 || self.columns == 0
    }

    /// What the formula readout says about the cell at zero-based `row` and
    /// `column`. In a format that hides formula text, a cell holding a value
    /// may or may not come from a formula, so only an empty cell is said to
    /// hold none.
    pub(super) fn formula_at(&self, row: usize, column: usize) -> FormulaReadout {
        if let Some(formula) = self.formulas.get(&(row, column)) {
            return FormulaReadout::Formula(formula.clone());
        }

        let holds_value = self
            .table
            .cell(row, column)
            .is_some_and(|cell| match &cell.kind {
                CellKind::Text(text) => !text.is_empty(),
                _ => true,
            });

        if self.formulas_unavailable && holds_value {
            FormulaReadout::Unavailable
        } else {
            FormulaReadout::NoFormula
        }
    }
}

/// The spreadsheet letters of the zero-based `column`: A for 0, Z for 25,
/// AA for 26, and so on.
pub(super) fn column_title(column: usize) -> String {
    let mut letters = Vec::new();
    let mut remaining = column + 1;

    while remaining > 0 {
        let digit = (remaining - 1) % 26;
        letters.push(char::from(b'A' + digit as u8));
        remaining = (remaining - 1) / 26;
    }

    letters.iter().rev().collect()
}

/// The address of a cell in A1 notation, from zero-based `row` and `column`.
pub(super) fn cell_address(row: usize, column: usize) -> String {
    format!("{}{}", column_title(column), row + 1)
}

/// What the non-empty cells of a column seen so far have in common.
///
/// The rule: every non-empty cell of the column counts, the first row
/// included, because it is shown as data. Whole numbers only make the column
/// Integer, numbers only make it Float, dates only make it a date column,
/// anything else or any mix makes it Text, and a column without a value is
/// Unknown. The data table has no date kind, so a date column is shown as
/// Text, labelled `date`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KindScan {
    Empty,
    Integer,
    Float,
    Date,
    Text,
}

impl KindScan {
    fn with(self, value: &SheetValue) -> Self {
        let seen = match value {
            SheetValue::Empty => return self,
            SheetValue::Number(number) if is_whole(*number) => Self::Integer,
            SheetValue::Number(_) => Self::Float,
            SheetValue::Date(_) => Self::Date,
            SheetValue::Bool(_) | SheetValue::Text(_) | SheetValue::Error(_) => Self::Text,
        };

        match (self, seen) {
            (Self::Empty, seen) => seen,
            (current, seen) if current == seen => current,
            (Self::Integer, Self::Float) | (Self::Float, Self::Integer) => Self::Float,
            _ => Self::Text,
        }
    }
}

/// Whether `number` is a whole number an `i64` holds exactly.
fn is_whole(number: f64) -> bool {
    const LARGEST_EXACT_INTEGER: f64 = 9_007_199_254_740_992.0;

    number.fract() == 0.0 && number.abs() <= LARGEST_EXACT_INTEGER
}

/// The header of one column. The kind decides alignment only: the rows are
/// never sorted, and a typed value is written by the cell input rules, not by
/// the kind of its column.
fn column_spec(index: usize, scan: KindScan) -> ColumnSpec {
    let (kind, type_name) = match scan {
        KindScan::Empty => (ColumnKind::Unknown, ""),
        KindScan::Integer => (ColumnKind::Integer, "integer"),
        KindScan::Float => (ColumnKind::Float, "number"),
        KindScan::Date => (ColumnKind::Text, "date"),
        KindScan::Text => (ColumnKind::Text, "text"),
    };

    let align = match kind {
        ColumnKind::Integer | ColumnKind::Float => TextAlign::Right,
        _ => TextAlign::Left,
    };

    ColumnSpec {
        id: Arc::from(index.to_string()),
        title: Arc::from(column_title(index)),
        kind,
        align,
        type_name: Arc::from(type_name),
    }
}

#[cfg(test)]
mod tests {
    use super::{cell_address, column_title};

    #[test]
    fn column_titles_count_in_spreadsheet_letters() {
        assert_eq!(column_title(0), "A");
        assert_eq!(column_title(25), "Z");
        assert_eq!(column_title(26), "AA");
        assert_eq!(column_title(51), "AZ");
        assert_eq!(column_title(52), "BA");
        assert_eq!(column_title(701), "ZZ");
        assert_eq!(column_title(702), "AAA");
        assert_eq!(column_title(16_383), "XFD");
        assert_eq!(cell_address(0, 0), "A1");
        assert_eq!(cell_address(9, 27), "AB10");
    }
}
