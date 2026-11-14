//! The cell edits [`crate::patch_xlsx`] and [`crate::patch_ods`] write.

use std::collections::BTreeMap;

use chrono::NaiveDateTime;

/// The new content of one cell.
#[derive(Debug, Clone, PartialEq)]
pub enum CellEdit {
    /// Empties the cell. A cell with a style keeps it, so its formatting stays.
    Clear,
    /// Text. An xlsx cell stores it inline, so the shared string table is not
    /// rewritten; an ods cell stores it as one paragraph per line.
    Text(String),
    Number(f64),
    Bool(bool),
    /// A date and time. An xlsx cell stores it as a serial number in the
    /// workbook's date system and keeps its own number format, so it shows
    /// as a date only when that format is a date format. An ods cell stores
    /// it as an ISO 8601 date value.
    Date(NaiveDateTime),
    /// Formula text, written without a cached result.
    ///
    /// For xlsx it is the formula, such as `SUM(A1:A3)`. For ods it is
    /// OpenFormula text after `of:=`, such as `SUM([.A1:.A3])`, or the whole
    /// attribute value with its namespace prefix, such as
    /// `of:=SUM([.A1:.A3])`, which is what [`crate::CellFormula::Text`]
    /// reports for an ods cell. One leading `=` before a formula without a
    /// namespace prefix, as typed into a cell, is dropped.
    Formula(String),
}

/// Cell edits for one or more sheets of a workbook.
///
/// Sheets are addressed by their index in workbook order (the order of
/// [`crate::Workbook::sheets`]), and cells by a zero-based `(row, column)`
/// as in [`crate::SheetGrid`]. Setting a cell twice keeps the last edit.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SheetEdits {
    pub(crate) sheets: BTreeMap<usize, BTreeMap<(usize, usize), CellEdit>>,
}

impl SheetEdits {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, sheet: usize, row: usize, column: usize, edit: CellEdit) {
        self.sheets
            .entry(sheet)
            .or_default()
            .insert((row, column), edit);
    }

    pub fn is_empty(&self) -> bool {
        self.sheets.values().all(BTreeMap::is_empty)
    }
}
