//! Random-access byte sources that file readers read through, with in-memory
//! and local-file implementations.
//!
//! A reader asks a [`ByteSource`] for its length and for bounded byte ranges,
//! so it never needs the whole file in memory and does not care whether the
//! bytes come from memory, a local file or an object store.
//!
//! ```
//! use dbflux_byte_source::{ByteSource, MemorySource};
//!
//! let source = MemorySource::new(b"0123456789".to_vec());
//!
//! assert_eq!(source.byte_length().unwrap(), 10);
//! assert_eq!(source.read_range(2..5).unwrap(), b"234");
//! assert_eq!(source.read_range(8..50).unwrap(), b"89");
//! ```
//!
//! Parsers that want [`std::io::Read`] and [`std::io::Seek`] read through a
//! [`ByteSourceReader`]:
//!
//! ```
//! use std::io::{Read, Seek, SeekFrom};
//!
//! use dbflux_byte_source::{ByteSourceReader, MemorySource};
//!
//! let mut reader = ByteSourceReader::new(MemorySource::new(b"0123456789".to_vec()));
//! reader.seek(SeekFrom::End(-3)).unwrap();
//!
//! let mut tail = String::new();
//! reader.read_to_string(&mut tail).unwrap();
//! assert_eq!(tail, "789");
//! ```

use std::error::Error;
use std::fmt;
use std::io;
use std::ops::Range;

/// A failure reported by a [`ByteSource`].
///
/// It wraps the error of whatever backs the source (a file, an object store)
/// and is transparent: it displays as that error's own message and reports
/// that error's own cause.
#[derive(Debug)]
pub struct SourceError(Box<dyn Error + Send + Sync + 'static>);

impl SourceError {
    /// Wraps an error value, or a message given as a `String` or `&str`.
    pub fn new(error: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        Self(error.into())
    }

    /// Returns the wrapped error, which the caller can downcast to its own
    /// error type.
    pub fn into_inner(self) -> Box<dyn Error + Send + Sync + 'static> {
        self.0
    }
}

impl fmt::Display for SourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl Error for SourceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source()
    }
}

impl From<std::io::Error> for SourceError {
    fn from(error: std::io::Error) -> Self {
        Self::new(error)
    }
}

/// Random-access bytes that a file reader reads in bounded ranges.
///
/// Both methods block, so a caller with a slow source (an object store, a
/// network file system) runs the reader off its UI thread.
pub trait ByteSource {
    /// Returns the total number of bytes in the source.
    fn byte_length(&self) -> Result<u64, SourceError>;

    /// Returns the bytes of the half-open `range`, clamped to the source's
    /// length.
    ///
    /// The result holds every byte of the clamped range: a range that ends
    /// past the end of the source returns the bytes up to the end, and a range
    /// that starts at or past the end returns no bytes. A reader may treat
    /// any shorter result as an error.
    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError>;
}

/// Clamps `range` to a source of `length` bytes. The result is never inverted.
fn clamp_range(range: Range<u64>, length: u64) -> Range<u64> {
    let end = range.end.min(length);
    let start = range.start.min(end);

    start..end
}

/// A [`ByteSource`] over bytes held in memory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemorySource {
    bytes: Vec<u8>,
}

impl MemorySource {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl ByteSource for MemorySource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        Ok(u64::try_from(self.bytes.len()).unwrap_or(u64::MAX))
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        let clamped = clamp_range(range, self.byte_length()?);

        // Both bounds are at most the length of the vector, so they fit.
        let start = usize::try_from(clamped.start).unwrap_or(usize::MAX);
        let end = usize::try_from(clamped.end).unwrap_or(usize::MAX);

        Ok(self.bytes.get(start..end).unwrap_or_default().to_vec())
    }
}

/// A [`ByteSource`] over a local file, read with positioned reads that do not
/// move a shared file cursor.
///
/// The length is asked from the file system on every call, so it follows a
/// file that another process grows or truncates.
#[cfg(any(unix, windows))]
#[derive(Debug)]
pub struct FileSource {
    file: std::fs::File,
}

#[cfg(any(unix, windows))]
impl FileSource {
    /// Opens the file at `path` for reading.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, SourceError> {
        Ok(Self::new(std::fs::File::open(path)?))
    }

    /// Wraps a file that is already open for reading.
    pub fn new(file: std::fs::File) -> Self {
        Self { file }
    }

    fn read_at(&self, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
        #[cfg(unix)]
        {
            std::os::unix::fs::FileExt::read_at(&self.file, buffer, offset)
        }

        #[cfg(windows)]
        {
            std::os::windows::fs::FileExt::seek_read(&self.file, buffer, offset)
        }
    }
}

#[cfg(any(unix, windows))]
impl ByteSource for FileSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        Ok(self.file.metadata()?.len())
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        let clamped = clamp_range(range, self.byte_length()?);

        let size = usize::try_from(clamped.end - clamped.start).map_err(|_| {
            SourceError::new("the requested range does not fit in memory on this platform")
        })?;

        let mut bytes = vec![0; size];
        let mut filled = 0;
        let mut offset = clamped.start;

        while let Some(unfilled) = bytes.get_mut(filled..)
            && !unfilled.is_empty()
        {
            match self.read_at(unfilled, offset) {
                // The file was truncated after its length was read.
                Ok(0) => break,

                Ok(read) => {
                    filled += read;
                    offset = offset.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
                }

                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}

                Err(error) => return Err(error.into()),
            }
        }

        bytes.truncate(filled);

        Ok(bytes)
    }
}

/// The smallest range a [`ByteSourceReader`] asks its source for.
///
/// Zip and spreadsheet parsers issue many reads of a few bytes, and 64 KiB
/// turns those into one request each while keeping a reader's buffer small.
pub const READ_AHEAD_BLOCK: usize = 64 * 1024;

/// Reads a [`ByteSource`] through [`io::Read`] and [`io::Seek`], for parsers
/// such as zip and spreadsheet readers that need both.
///
/// Reads are served from a buffered block of at least [`READ_AHEAD_BLOCK`]
/// bytes, so small sequential reads do not each become a range request. The
/// source's length is read once, on the first read or end-relative seek.
///
/// A source that returns fewer bytes than its length promises makes a read
/// fail with [`io::ErrorKind::UnexpectedEof`]. A [`SourceError`] becomes an
/// [`io::Error`] with the same message.
#[derive(Debug, Clone)]
pub struct ByteSourceReader<S> {
    source: S,
    position: u64,
    length: Option<u64>,
    block_start: u64,
    block: Vec<u8>,
}

impl<S: ByteSource> ByteSourceReader<S> {
    /// Wraps `source`, positioned at its first byte.
    pub fn new(source: S) -> Self {
        Self {
            source,
            position: 0,
            length: None,
            block_start: 0,
            block: Vec::new(),
        }
    }

    pub fn get_ref(&self) -> &S {
        &self.source
    }

    pub fn into_inner(self) -> S {
        self.source
    }

    fn length(&mut self) -> io::Result<u64> {
        if let Some(length) = self.length {
            return Ok(length);
        }

        let length = self.source.byte_length().map_err(source_error_to_io)?;
        self.length = Some(length);

        Ok(length)
    }

    fn buffered(&self) -> &[u8] {
        self.position
            .checked_sub(self.block_start)
            .and_then(|offset| usize::try_from(offset).ok())
            .and_then(|offset| self.block.get(offset..))
            .unwrap_or_default()
    }

    fn fill_block(&mut self, wanted: usize) -> io::Result<()> {
        let length = self.length()?;

        self.block_start = self.position;
        self.block.clear();

        if self.position >= length {
            return Ok(());
        }

        let request = u64::try_from(wanted.max(READ_AHEAD_BLOCK)).unwrap_or(u64::MAX);
        let end = self.position.saturating_add(request).min(length);

        let bytes = self
            .source
            .read_range(self.position..end)
            .map_err(source_error_to_io)?;

        let expected = end - self.position;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) < expected {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "the source returned {} of the {expected} bytes asked for at offset {}",
                    bytes.len(),
                    self.position
                ),
            ));
        }

        self.block = bytes;

        Ok(())
    }
}

impl<S: ByteSource> io::Read for ByteSourceReader<S> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }

        if self.buffered().is_empty() {
            self.fill_block(buffer.len())?;
        }

        let available = self.buffered();
        let count = available.len().min(buffer.len());

        if let (Some(target), Some(bytes)) = (buffer.get_mut(..count), available.get(..count)) {
            target.copy_from_slice(bytes);
        }

        self.position = self
            .position
            .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));

        Ok(count)
    }
}

impl<S: ByteSource> io::Seek for ByteSourceReader<S> {
    fn seek(&mut self, target: io::SeekFrom) -> io::Result<u64> {
        let position = match target {
            io::SeekFrom::Start(offset) => Some(offset),
            io::SeekFrom::Current(delta) => self.position.checked_add_signed(delta),
            io::SeekFrom::End(delta) => self.length()?.checked_add_signed(delta),
        };

        let position = position.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot seek to a position before the start of the source",
            )
        })?;

        self.position = position;

        Ok(position)
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.position)
    }
}

/// Converts a [`SourceError`] to an [`io::Error`] with the same message,
/// keeping the original when the source failed with an [`io::Error`].
fn source_error_to_io(error: SourceError) -> io::Error {
    match error.into_inner().downcast::<io::Error>() {
        Ok(io_error) => *io_error,
        Err(other) => io::Error::other(other),
    }
}

#[cfg(test)]
mod tests;
