//! The Parquet document: a Parquet file shown read-only as a paged table.
//!
//! The footer, the statistics and the row windows are read through
//! `dbflux_parquet` over the shared storage layer in [`crate::file_source`].
//! The page model turns the reader's cells into the data table's model and
//! does no I/O. The document opens the file on the background executor with
//! the default projection, reads further windows on request after checking
//! the file's version, and never sorts or edits the rows.

mod document;
mod page_model;
mod pane;
mod render;

#[cfg(test)]
mod tests;

pub use document::ParquetDocument;
