//! The byte source the paged reader reads through. The types live in
//! `dbflux_byte_source` so readers of other file formats share them.

#[cfg(any(unix, windows))]
pub use dbflux_byte_source::FileSource;
pub use dbflux_byte_source::{ByteSource, MemorySource, SourceError};
