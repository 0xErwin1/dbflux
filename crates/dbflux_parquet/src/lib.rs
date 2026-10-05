//! Reads Parquet files through a [`dbflux_byte_source::ByteSource`], so local
//! files, in-memory bytes and object-store objects share one read path.
//!
//! [`open`] reads the footer and the page index with a bounded number of
//! range requests and refuses, with a typed [`ParquetError`], files this build
//! cannot decode: encrypted files and columns compressed with Brotli or LZO.
//!
//! [`read_window`] then decodes a [`RowWindow`] of rows for chosen top-level
//! columns, reading only the pages that hold those rows when the file has an
//! offset index.
//!
//! ```
//! use dbflux_byte_source::MemorySource;
//! use dbflux_parquet::{ParquetError, open};
//!
//! let source = MemorySource::new(b"name,city\nAda,London\n".to_vec());
//!
//! assert!(matches!(open(&source), Err(ParquetError::NotParquet { .. })));
//! ```

mod cells;
mod decode;
mod error;
mod footer;
mod nested;
mod window;

pub use cells::{
    BINARY_PREVIEW_BYTES, Cell, CellKind, CellPage, ColumnDisplay, ColumnKind,
    NESTED_DISPLAY_CHARS, cells_of,
};
pub use decode::{UNINDEXED_CHUNK_BUDGET, WindowRows, read_window, window_byte_ranges};
pub use error::ParquetError;
pub use footer::{MAX_FOOTER_BYTES, ParquetFile, TAIL_READ_BYTES, open};
pub use window::RowWindow;

#[cfg(test)]
mod cells_tests;
#[cfg(test)]
mod decode_tests;
#[cfg(test)]
mod footer_tests;
#[cfg(test)]
mod test_support;
