//! Where a file document's file lives, how its bytes are read by range, and
//! which version of it was opened.
//!
//! Nothing here depends on the file's format, except that
//! [`StorageError::Write`] carries the delimited writer's error, because only
//! the delimited document saves edits. Every function blocks on file
//! or network I/O and touches no GPUI state, so callers run it on the
//! background executor and report the returned errors themselves.

use std::fmt;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use dbflux_byte_source::{ByteSource, FileSource, SourceError};
use dbflux_core::chrono::{DateTime, Utc};
use dbflux_core::{Connection, DbError, ObjectMetadata, ObjectStoreConnection};
use dbflux_delimited::WriteError;

/// Where a file lives.
///
/// An object is reached the way the object editor reaches one: through the
/// profile's live connection and its `object_store_api()`. The caller resolves
/// that connection from the application state on the foreground thread and
/// hands it over, because nothing in this layer may touch GPUI.
#[derive(Clone)]
pub enum FileLocation {
    /// A file on the local file system.
    Local { path: PathBuf },

    /// An object in object storage.
    Object {
        connection: Arc<dyn Connection>,
        bucket: String,
        key: String,
    },
}

/// The identity of a file's content at one moment, comparable for equality.
///
/// A version is captured when the file is opened and compared again right
/// before a save. Byte ranges read from one version must never be applied to
/// another, and this value is the only thing that tells them apart when the
/// length did not change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceVersion {
    /// A local file: its modification time and its length. The time is `None`
    /// on a file system that does not report one, which leaves the length as
    /// the only signal.
    Local {
        modified: Option<SystemTime>,
        length: u64,
    },

    /// An object: the `etag`, `last_modified` and `size_bytes` its
    /// `head_object` metadata reports. A store that reports neither an etag
    /// nor a time leaves the length as the only signal.
    Object {
        etag: Option<String>,
        last_modified: Option<DateTime<Utc>>,
        length: u64,
    },
}

impl SourceVersion {
    /// Whether comparing this version with a later one can tell that the
    /// content was replaced by other content of the same length.
    ///
    /// A local file needs a modification time for that, and an object needs
    /// an etag or a last-modified time. Without them the length is the only
    /// thing compared, and [`crate::delimited::save_edited`] refuses to save,
    /// because it could overwrite a change it cannot see. A caller checks this
    /// when the file is opened to tell the user that the file cannot be saved.
    pub fn detects_same_length_change(&self) -> bool {
        match self {
            Self::Local { modified, .. } => modified.is_some(),

            Self::Object {
                etag,
                last_modified,
                ..
            } => etag.is_some() || last_modified.is_some(),
        }
    }
}

/// A failure of a file document's storage layer.
#[derive(Debug)]
pub enum StorageError {
    /// The file is not the version that was opened, so the byte ranges read
    /// from it no longer name its records. Nothing was written.
    SourceChanged,

    /// The file reports nothing but its length to tell one version from
    /// another, so a change of the same length made elsewhere could not be
    /// seen and would be overwritten. Nothing was written.
    VersionUnverifiable,

    /// The file's bytes could not be read. The message is the file system's
    /// or the driver's own.
    Read(SourceError),

    /// The edited file could not be produced. Nothing replaced the target.
    Write(WriteError),

    /// A local file operation on `path` failed.
    LocalIo {
        path: PathBuf,
        source: std::io::Error,
    },

    /// The temporary file a save writes the edited bytes to could not be
    /// created or written in `directory`. `target` is the file or object
    /// being saved, which was not replaced.
    TemporaryFile {
        target: String,
        directory: PathBuf,
        source: std::io::Error,
    },

    /// The object store refused an operation on the object. `source` is the
    /// driver's error, unchanged.
    ObjectStore {
        bucket: String,
        key: String,
        source: Box<DbError>,
    },
}

impl StorageError {
    pub(crate) fn local_io(path: &Path, source: std::io::Error) -> Self {
        Self::LocalIo {
            path: path.to_path_buf(),
            source,
        }
    }

    pub(crate) fn object_store(bucket: &str, key: &str, source: DbError) -> Self {
        Self::ObjectStore {
            bucket: bucket.to_string(),
            key: key.to_string(),
            source: Box::new(source),
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceChanged => {
                formatter.write_str(&dbflux_i18n::t!("document.file.error.source_changed"))
            }

            Self::VersionUnverifiable => formatter.write_str(&dbflux_i18n::t!(
                "document.file.warning.cannot_save_in_place"
            )),

            Self::Read(source) => {
                formatter.write_str(&crate::labels::file_read_failed_cause(source))
            }

            Self::Write(source) => {
                formatter.write_str(&crate::labels::delimited_write_error_cause(source))
            }

            Self::LocalIo { path, source } => formatter.write_str(&dbflux_i18n::t!(
                "document.file.error.storage.local_io",
                path = path.display(),
                cause = source
            )),

            Self::TemporaryFile {
                target,
                directory,
                source,
            } => formatter.write_str(&dbflux_i18n::t!(
                "document.file.error.storage.temporary_file",
                target = target,
                directory = directory.display(),
                cause = source
            )),

            Self::ObjectStore {
                bucket,
                key,
                source,
            } => formatter.write_str(&dbflux_i18n::t!(
                "document.file.error.storage.object_store",
                bucket = bucket,
                key = key,
                cause = source
            )),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SourceChanged | Self::VersionUnverifiable => None,
            Self::Read(source) => Some(source),
            Self::Write(source) => Some(source),
            Self::LocalIo { source, .. } | Self::TemporaryFile { source, .. } => Some(source),
            Self::ObjectStore { source, .. } => Some(source.as_ref()),
        }
    }
}

/// A [`ByteSource`] over one object of an object store.
///
/// Each range read is one `get_object_range` call. The length is the one the
/// object had when the source was built and is never asked again, because the
/// reader calls [`ByteSource::byte_length`] at open and at every invalidation
/// and a `head_object` round trip each time would be wasted. A source is
/// therefore tied to one version of the object: after a save, open a new one.
///
/// Reads are not pinned to that version. The store is asked for a byte range
/// of whatever the key holds at the time of each call.
///
/// Memory stays bounded by the range only when the driver overrides
/// `ObjectStoreConnection::get_object_range` with a ranged request. The
/// trait's default downloads the whole object on every call and slices it.
pub struct ObjectSource {
    connection: Arc<dyn Connection>,
    bucket: String,
    key: String,
    length: u64,
}

impl ObjectSource {
    pub(crate) fn new(
        connection: Arc<dyn Connection>,
        bucket: &str,
        key: &str,
        length: u64,
    ) -> Self {
        Self {
            connection,
            bucket: bucket.to_string(),
            key: key.to_string(),
            length,
        }
    }
}

impl ByteSource for ObjectSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        Ok(self.length)
    }

    #[allow(clippy::result_large_err)]
    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        let end = range.end.min(self.length);
        let start = range.start.min(end);

        if start == end {
            return Ok(Vec::new());
        }

        object_store(self.connection.as_ref())
            .and_then(|api| api.get_object_range(&self.bucket, &self.key, start..end))
            .map_err(SourceError::new)
    }
}

/// The bytes of a file at either kind of location, as the one source type a
/// reader is opened over.
pub enum LocationSource {
    Local(FileSource),
    Object(ObjectSource),
}

impl LocationSource {
    /// Reads an object through `connection` from now on, for a profile that
    /// reconnected since the source was opened. The byte ranges already read
    /// stay valid: they belong to the object's version, not to a connection.
    /// A local file has no connection and is left as it is.
    pub(crate) fn use_connection(&mut self, connection: Arc<dyn Connection>) {
        match self {
            Self::Local(_) => {}
            Self::Object(source) => source.connection = connection,
        }
    }
}

impl ByteSource for LocationSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        match self {
            Self::Local(source) => source.byte_length(),
            Self::Object(source) => source.byte_length(),
        }
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        match self {
            Self::Local(source) => source.read_range(range),
            Self::Object(source) => source.read_range(range),
        }
    }
}

/// Opens the file at `location` for range reads and captures the version
/// that was opened.
///
/// The version and the source describe the same content: a local file's
/// version is read from the handle the source reads through, and an object's
/// source keeps the length its version reports. Opening an object costs one
/// `head_object` call and no body read.
pub fn open_source(
    location: &FileLocation,
) -> Result<(LocationSource, SourceVersion), StorageError> {
    match location {
        FileLocation::Local { path } => {
            let (file, metadata) = open_local_file(path)?;

            Ok((
                LocationSource::Local(FileSource::new(file)),
                local_version(&metadata),
            ))
        }

        FileLocation::Object {
            connection,
            bucket,
            key,
        } => {
            let metadata = head_object(connection.as_ref(), bucket, key)?;

            let source = ObjectSource::new(connection.clone(), bucket, key, metadata.size_bytes);

            Ok((LocationSource::Object(source), object_version(&metadata)))
        }
    }
}

/// Reads the version the file at `location` has now: one `stat` for a local
/// file, one `head_object` call for an object.
pub fn read_version(location: &FileLocation) -> Result<SourceVersion, StorageError> {
    match location {
        FileLocation::Local { path } => std::fs::metadata(path)
            .map(|metadata| local_version(&metadata))
            .map_err(|error| StorageError::local_io(path, error)),

        FileLocation::Object {
            connection,
            bucket,
            key,
        } => {
            head_object(connection.as_ref(), bucket, key).map(|metadata| object_version(&metadata))
        }
    }
}

/// Whether the file at `location` is no longer the `captured` version.
pub fn has_changed_since(
    location: &FileLocation,
    captured: &SourceVersion,
) -> Result<bool, StorageError> {
    Ok(read_version(location)? != *captured)
}

/// Opens a local file for reading and returns it with the metadata of that
/// same handle, so the version taken from it describes the bytes it reads.
pub(crate) fn open_local_file(
    path: &Path,
) -> Result<(std::fs::File, std::fs::Metadata), StorageError> {
    std::fs::File::open(path)
        .and_then(|file| {
            let metadata = file.metadata()?;

            Ok((file, metadata))
        })
        .map_err(|error| StorageError::local_io(path, error))
}

pub(crate) fn local_version(metadata: &std::fs::Metadata) -> SourceVersion {
    SourceVersion::Local {
        modified: metadata.modified().ok(),
        length: metadata.len(),
    }
}

pub(crate) fn object_version(metadata: &ObjectMetadata) -> SourceVersion {
    SourceVersion::Object {
        etag: metadata.etag.clone(),
        last_modified: metadata.last_modified,
        length: metadata.size_bytes,
    }
}

#[allow(clippy::result_large_err)]
pub(crate) fn head_object(
    connection: &dyn Connection,
    bucket: &str,
    key: &str,
) -> Result<ObjectMetadata, StorageError> {
    object_store(connection)
        .and_then(|api| api.head_object(bucket, key))
        .map_err(|error| StorageError::object_store(bucket, key, error))
}

/// The object-store API of `connection`, or the error every other object
/// document reports when the connection has none.
#[allow(clippy::result_large_err)]
pub(crate) fn object_store(
    connection: &dyn Connection,
) -> Result<&dyn ObjectStoreConnection, DbError> {
    connection.object_store_api().ok_or_else(|| {
        DbError::NotSupported(dbflux_i18n::t!(
            "document.object_browser.error.api_unavailable"
        ))
    })
}
