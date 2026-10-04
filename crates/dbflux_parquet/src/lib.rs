//! Reads Parquet files through a [`dbflux_byte_source::ByteSource`], so local
//! files, in-memory bytes and object-store objects share one read path.
//!
//! [`open`] reads the footer and the page index with a bounded number of
//! range requests and refuses, with a typed [`ParquetError`], files this build
//! cannot decode: encrypted files and columns compressed with Brotli or LZO.
//!
//! ```
//! use dbflux_byte_source::MemorySource;
//! use dbflux_parquet::{ParquetError, open};
//!
//! let source = MemorySource::new(b"name,city\nAda,London\n".to_vec());
//!
//! assert!(matches!(open(&source), Err(ParquetError::NotParquet { .. })));
//! ```

mod error;
mod footer;

pub use error::ParquetError;
pub use footer::{MAX_FOOTER_BYTES, ParquetFile, TAIL_READ_BYTES, open};

#[cfg(test)]
mod footer_tests;
