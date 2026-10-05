//! The spreadsheet document: an xlsx, xlsm, xls or ods workbook shown
//! read-only, one sheet at a time, with its sheets as tabs.
//!
//! The workbook is read through `dbflux_spreadsheet` over the shared storage
//! layer in [`crate::file_source`]. The grid model turns a decoded sheet into
//! the data table's model and does no I/O. The document opens the file and
//! reads each sheet on the background executor, and keeps one sheet in
//! memory at a time.

mod document;
mod grid_model;
mod pane;
mod render;

#[cfg(test)]
mod tests;

pub use document::SpreadsheetDocument;
