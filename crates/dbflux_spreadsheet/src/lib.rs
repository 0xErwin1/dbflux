//! Reads xlsx, xlsm, xls and ods workbooks through a
//! [`dbflux_byte_source::ByteSource`] with calamine.
//!
//! [`open`] recognizes the format from the content and lists the sheets.
//! [`Workbook::read_sheet`] then decodes one sheet into a [`SheetGrid`]
//! padded to A1, where each cell carries its value, the text to show for it,
//! and its formula when the format exposes one.
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

mod error;
mod grid;
mod workbook;

pub use error::SpreadsheetError;
pub use grid::{CellErrorCode, CellFormula, CellValue, MAX_GRID_CELLS, SheetCell, SheetGrid};
pub use workbook::{SheetInfo, SheetKind, SpreadsheetFormat, Workbook, open};

#[cfg(test)]
mod grid_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod workbook_tests;
