//! The spreadsheet document: an xlsx, xlsm, xls or ods workbook shown one
//! sheet at a time, with its sheets as tabs. xlsx, xlsm and ods cells can be
//! edited and rows appended, and a save patches the file in place; xls is
//! read-only.
//!
//! The workbook is read through `dbflux_spreadsheet` over the shared storage
//! layer in [`crate::file_source`]. The grid model turns a decoded sheet into
//! the data table's model and does no I/O. The document opens the file and
//! reads each sheet on the background executor, and keeps one sheet in
//! memory at a time.

mod document;
mod editing;
mod grid_model;
mod input;
mod pane;
mod render;
mod save;

#[cfg(test)]
mod editing_tests;
#[cfg(test)]
mod object_tests;
#[cfg(test)]
mod tests;

pub use document::SpreadsheetDocument;
