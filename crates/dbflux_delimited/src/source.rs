//! The byte source the paged reader reads through, with its in-memory and
//! local-file implementations.

use std::error::Error;
use std::fmt;
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

/// Random-access bytes that a [`crate::PagedReader`] reads in bounded ranges.
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
    /// that starts at or past the end returns no bytes. The reader treats any
    /// shorter result as an error.
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
