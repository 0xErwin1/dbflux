//! What is wrong with a sheet that cannot be imported, as data the caller can
//! word for the user, and the error that carries it out of an inspection.

use std::fmt;
use std::path::Path;

use crate::pipeline::TransferError;

/// A header name that more than one column of the sheet uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateHeader {
    pub name: String,
    /// Zero-based columns that carry the name, in sheet order.
    pub columns: Vec<usize>,
}

/// Why the chosen sheet of a workbook cannot be imported. Columns are
/// zero-based; `row` is the row number a spreadsheet application shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SheetIssue {
    /// The sheet holds no rows.
    Empty { sheet: String },
    /// Row 1 has a blank or a repeated column name.
    InvalidHeader {
        sheet: String,
        blank_columns: Vec<usize>,
        duplicate_names: Vec<DuplicateHeader>,
    },
    /// The first cell that holds an error value such as `#DIV/0!`.
    ErrorCell {
        sheet: String,
        row: usize,
        column: usize,
        column_name: String,
        code: &'static str,
    },
    /// The sheet's grid, padded to A1, has more cells than can be read.
    TooLarge {
        sheet: String,
        cells: usize,
        limit: usize,
    },
}

impl fmt::Display for SheetIssue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { sheet } => write!(
                formatter,
                "sheet '{sheet}' is empty; the first row must hold the column names"
            ),
            Self::InvalidHeader {
                sheet,
                blank_columns,
                duplicate_names,
            } => {
                let blanks = blank_columns
                    .iter()
                    .map(|column| format!("header column {} has no name", column + 1));
                let duplicates = duplicate_names.iter().map(|duplicate| {
                    format!("header column '{}' appears more than once", duplicate.name)
                });
                let problems: Vec<String> = blanks.chain(duplicates).collect();

                write!(formatter, "sheet '{sheet}': {}", problems.join("; "))
            }
            Self::ErrorCell {
                sheet,
                row,
                column_name,
                code,
                ..
            } => write!(
                formatter,
                "sheet '{sheet}', row {row}, column '{column_name}': the cell holds the error value {code}"
            ),
            Self::TooLarge {
                sheet,
                cells,
                limit,
            } => write!(
                formatter,
                "sheet '{sheet}' has {cells} cells, more than the {limit}-cell limit"
            ),
        }
    }
}

/// A failed inspection: a problem with the sheet the user can act on, or any
/// other failure to read the file.
#[derive(Debug)]
pub enum InspectError {
    Sheet(SheetIssue),
    Failed(TransferError),
}

impl From<TransferError> for InspectError {
    fn from(error: TransferError) -> Self {
        Self::Failed(error)
    }
}

impl From<SheetIssue> for InspectError {
    fn from(issue: SheetIssue) -> Self {
        Self::Sheet(issue)
    }
}

impl InspectError {
    /// The error as a [`TransferError`], naming the file a sheet issue is in.
    pub fn into_transfer_error(self, path: &Path) -> TransferError {
        match self {
            Self::Sheet(issue) => TransferError::Source(format!("{}: {issue}", path.display())),
            Self::Failed(error) => error,
        }
    }
}
