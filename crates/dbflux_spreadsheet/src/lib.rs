//! Reads xlsx, xlsm, xls and ods workbooks through a
//! [`dbflux_byte_source::ByteSource`] with calamine.
//!
//! [`open`] recognizes the format from the content and lists the sheets.
//! [`Workbook::read_sheet`] then decodes one sheet into a [`SheetGrid`]
//! padded to A1, where each cell carries its value, the text to show for it,
//! and its formula when the format exposes one.
//!
//! [`patch_xlsx`] and [`patch_ods`] write cell edits into an xlsx or ods
//! package in place, and [`write_values_xlsx`] writes the values of any
//! workbook, xls included, into a new xlsx package.
//!
//! ```
//! use dbflux_byte_source::MemorySource;
//! use dbflux_spreadsheet::{SpreadsheetError, open};
//!
//! let source = MemorySource::new(b"name,city\nAda,London\n".to_vec());
//!
//! assert!(matches!(
//!     open(source),
//!     Err(SpreadsheetError::NotASpreadsheet { .. })
//! ));
//! ```

mod edits;
mod error;
mod grid;
mod ods_patch;
mod values_xlsx;
mod workbook;
mod xlsx_patch;

pub use edits::{CellEdit, SheetEdits};
pub use error::{FormulaRangeKind, SheetWriteError, SpreadsheetError, ValuesWriteError};
pub use grid::{CellErrorCode, CellFormula, CellValue, MAX_GRID_CELLS, SheetCell, SheetGrid};
pub use ods_patch::patch_ods;
pub use values_xlsx::write_values_xlsx;
pub use workbook::{SheetInfo, SheetKind, SpreadsheetFormat, Workbook, open};
pub use xlsx_patch::patch_xlsx;

#[cfg(test)]
mod grid_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod values_xlsx_tests;
#[cfg(test)]
mod workbook_tests;
