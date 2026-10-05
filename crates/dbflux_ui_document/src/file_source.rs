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

use dbflux_byte_source::{ByteSource, FileSource, MemorySource, SourceError};
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
    /// The length in bytes the file or object had at this version.
    pub fn length(&self) -> u64 {
        match self {
            Self::Local { length, .. } | Self::Object { length, .. } => *length,
        }
    }

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

/// Objects up to this size are downloaded whole into memory, and larger ones
/// into a temporary file.
///
/// A Parquet page of the default projection reads about 3 to 14 MiB, and a
/// footer at most a few MiB, so 64 MiB keeps an in-memory copy within a few
/// pages' worth of memory while most exported files skip the disk write. A
/// larger object costs disk space instead of memory, the way the object
/// browser's Download does.
pub const DOWNLOAD_IN_MEMORY_LIMIT_BYTES: u64 = 64 * 1024 * 1024;

/// How the bytes of an object are read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectReads {
    /// Each read asks the store for the byte range it needs.
    ByRange,

    /// The object was downloaded whole once, after the user agreed, and is
    /// read from that copy. The store cannot read a byte range of an object.
    Downloaded,
}

impl ObjectReads {
    /// How `connection` reads objects: by range when its store declares
    /// ranged reads, and by a single whole download otherwise.
    pub fn of(connection: &dyn Connection) -> Self {
        let by_range = connection
            .object_store_api()
            .is_some_and(|api| api.supports_range_reads());

        if by_range {
            Self::ByRange
        } else {
            Self::Downloaded
        }
    }
}

/// A whole object downloaded into a temporary file, which is removed when
/// this is dropped.
pub struct DownloadedFile {
    /// `None` only while dropping: the file is closed before it is removed,
    /// because Windows refuses to remove an open file.
    source: Option<FileSource>,
    path: PathBuf,
}

impl DownloadedFile {
    /// Where the copy lives.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DownloadedFile {
    fn drop(&mut self) {
        drop(self.source.take());
        remove_download_file(&self.path);
    }
}

impl ByteSource for DownloadedFile {
    fn byte_length(&self) -> Result<u64, SourceError> {
        match &self.source {
            Some(source) => source.byte_length(),
            None => Ok(0),
        }
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        match &self.source {
            Some(source) => source.read_range(range),
            None => Ok(Vec::new()),
        }
    }
}

/// The bytes of a file at either kind of location, as the one source type a
/// reader is opened over.
pub enum LocationSource {
    Local(FileSource),
    Object(ObjectSource),

    /// A whole object downloaded into memory.
    Memory(MemorySource),

    /// A whole object downloaded into a temporary file.
    Downloaded(DownloadedFile),
}

impl LocationSource {
    /// Reads an object through `connection` from now on, for a profile that
    /// reconnected since the source was opened. The byte ranges already read
    /// stay valid: they belong to the object's version, not to a connection.
    /// A local file has no connection and is left as it is.
    pub(crate) fn use_connection(&mut self, connection: Arc<dyn Connection>) {
        match self {
            Self::Local(_) | Self::Memory(_) | Self::Downloaded(_) => {}
            Self::Object(source) => source.connection = connection,
        }
    }
}

impl ByteSource for LocationSource {
    fn byte_length(&self) -> Result<u64, SourceError> {
        match self {
            Self::Local(source) => source.byte_length(),
            Self::Object(source) => source.byte_length(),
            Self::Memory(source) => source.byte_length(),
            Self::Downloaded(source) => source.byte_length(),
        }
    }

    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>, SourceError> {
        match self {
            Self::Local(source) => source.read_range(range),
            Self::Object(source) => source.read_range(range),
            Self::Memory(source) => source.read_range(range),
            Self::Downloaded(source) => source.read_range(range),
        }
    }
}

/// Downloads the object `key` of `bucket` whole, once: into memory when the
/// store reports `in_memory_limit` bytes or fewer, and otherwise into a new
/// temporary file in the system's temporary directory. Costs one
/// `head_object` call and one whole read.
///
/// The copy is a snapshot: nothing read from it asks the store again. The
/// version returned is the one `head_object` reported right before the
/// download, which a later [`read_version`] is compared with. An object
/// replaced between the two calls makes that comparison report a change that
/// is already in the copy, and one that arrives larger than `in_memory_limit`
/// is moved to the temporary file instead of staying in memory. The store
/// cannot read part of an object, so its bytes are still held in memory while
/// they arrive.
#[allow(clippy::result_large_err)]
pub fn download_whole_object(
    connection: &dyn Connection,
    bucket: &str,
    key: &str,
    in_memory_limit: u64,
) -> Result<(LocationSource, SourceVersion), StorageError> {
    let metadata = head_object(connection, bucket, key)?;
    let version = object_version(&metadata);

    let api =
        object_store(connection).map_err(|error| StorageError::object_store(bucket, key, error))?;

    if metadata.size_bytes <= in_memory_limit {
        let bytes = api
            .get_object(bucket, key)
            .map_err(|error| StorageError::object_store(bucket, key, error))?;

        let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);

        if length <= in_memory_limit {
            return Ok((LocationSource::Memory(MemorySource::new(bytes)), version));
        }

        let path = create_download_file(&std::env::temp_dir())?;
        let written =
            std::fs::write(&path, &bytes).map_err(|error| StorageError::local_io(&path, error));
        drop(bytes);

        return Ok((downloaded_source(path, written)?, version));
    }

    let path = create_download_file(&std::env::temp_dir())?;

    let downloaded = api
        .download_object(bucket, key, &path)
        .map(drop)
        .map_err(|error| StorageError::object_store(bucket, key, error));

    Ok((downloaded_source(path, downloaded)?, version))
}

/// The copy of an object written to `path`, or `written`'s error after the
/// file is removed.
fn downloaded_source(
    path: PathBuf,
    written: Result<(), StorageError>,
) -> Result<LocationSource, StorageError> {
    match written.and_then(|()| open_local_file(&path)) {
        Ok((file, _metadata)) => Ok(LocationSource::Downloaded(DownloadedFile {
            source: Some(FileSource::new(file)),
            path,
        })),

        Err(error) => {
            remove_download_file(&path);

            Err(error)
        }
    }
}

/// Creates an empty file only this user can read, under a name no other
/// file has, in `directory`, for a download to write into.
fn create_download_file(directory: &Path) -> Result<PathBuf, StorageError> {
    let path = directory.join(format!("dbflux-object-{}", uuid::Uuid::new_v4()));

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }

    options
        .open(&path)
        .map(drop)
        .map_err(|error| StorageError::local_io(&path, error))?;

    Ok(path)
}

/// Removes a downloaded copy. A copy that cannot be removed is traced: the
/// document that read it is gone, and nothing else can act on the failure.
fn remove_download_file(path: &Path) {
    if let Err(error) = std::fs::remove_file(path) {
        log::warn!(
            "Failed to remove downloaded copy {}: {error}",
            path.display()
        );
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
