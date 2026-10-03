//! How an edited delimited file replaces the one that was opened.

use std::io::BufWriter;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dbflux_core::Connection;
use dbflux_delimited::{ByteSource, Dialect, EditSet, FileSource, WriteError, write_edited};

use super::source::{
    DelimitedLocation, ObjectSource, SourceVersion, StorageError, head_object, local_version,
    object_store, object_version, open_local_file, read_version,
};

/// What a save that replaced the target knows about the saved file.
#[derive(Debug)]
pub enum SaveOutcome {
    /// The target holds the edited bytes and this is its version.
    Saved(SourceVersion),

    /// The target holds the edited bytes, and reading its new version failed
    /// with this error. The edits are saved and must not be applied again.
    /// The caller reads the version when it reopens the file.
    SavedVersionUnknown(StorageError),
}

/// Saves the file at `location` with `edits` applied.
///
/// `Ok` means the target was replaced with the edited bytes. `Err` means it
/// was not: the target holds what it held before the call and the temporary
/// file is removed.
///
/// `captured` is the version the byte ranges in `edits` were read from. The
/// file's current version is read first. A file that is no longer that
/// version is refused with [`StorageError::SourceChanged`], and a version
/// that cannot show a change of the same length (see
/// [`SourceVersion::detects_same_length_change`]) is refused with
/// [`StorageError::VersionUnverifiable`], both before anything is written.
///
/// The edited bytes are produced by `dbflux_delimited::write_edited`, reading
/// the source in ranges of `window_size` bytes, into a temporary file and
/// never into the target. Neither the old nor the new file is held whole in
/// memory.
///
/// # Local files
///
/// The temporary file is created in the target's own directory, readable and
/// writable by its owner only, then given the target's permission bits,
/// synced to disk and renamed onto the target, so a reader of the path sees
/// either the whole old file or the whole new one. A symlink is followed and
/// the file it points at is replaced. A target this process may not write is
/// refused instead of being replaced through its directory: one whose
/// permission bits forbid writing, and one the operating system refuses to
/// open for writing, such as a file owned by another user.
///
/// The saved file is a new file under the old name. Its permission bits are
/// the old file's. Its owner is the user who saved it, access control lists
/// and extended attributes are not carried over, and other hard links to the
/// old file keep the old content.
///
/// The new content is synced before the rename, so a crash never leaves a
/// partly written file under the target's name. On unix the directory is
/// synced after the rename to make the rename itself survive a crash. A
/// failure of that last sync is logged and the save is still reported as
/// saved, because the target already holds the new content: after a crash
/// the path may then name the old file again. Other platforms do not sync
/// the directory.
///
/// The version is checked again after the edited bytes are staged, right
/// before the rename, as an object's is before its upload. That last check
/// and the rename are still separate system calls: a change that another
/// process makes between them is overwritten.
///
/// The outcome is always [`SaveOutcome::Saved`], with the version taken from
/// the new file before it was renamed.
///
/// # Objects
///
/// The temporary file is created in the system temporary directory, readable
/// and writable by its owner only, and sent with
/// `ObjectStoreConnection::upload_object`, keeping the content type the
/// object has. It is removed once the upload returns.
///
/// The object's version is read twice: before its bytes are read, and again
/// after the temporary file is complete and right before the upload. An
/// object replaced while it was being read is refused with
/// [`StorageError::SourceChanged`] and nothing is uploaded.
///
/// These are not covered:
///
/// - Range reads are not tied to a version. An object swapped for another
///   one between two range reads and swapped back before the second check
///   passes both checks, and the temporary file then mixes bytes of both.
/// - The object store has no conditional put. Another writer that replaces
///   the object between the second check and the upload is overwritten.
///
/// After the upload the new version is read with `head_object`. When that
/// read fails the outcome is [`SaveOutcome::SavedVersionUnknown`]. When
/// another writer replaces the object between the upload and that read, the
/// version returned is that writer's.
///
/// # After a save
///
/// Every byte range read before the save is stale. The caller opens a new
/// source with [`super::open_source`] and drops its stored ranges and edits.
pub fn save_edited(
    location: &DelimitedLocation,
    captured: &SourceVersion,
    dialect: &Dialect,
    edits: &EditSet,
    window_size: NonZeroU64,
) -> Result<SaveOutcome, StorageError> {
    let request = SaveRequest {
        captured,
        dialect,
        edits,
        window_size,
    };

    save_staging_objects_in(location, &request, &std::env::temp_dir())
}

/// What to write, and the version it was read from.
pub(super) struct SaveRequest<'a> {
    pub(super) captured: &'a SourceVersion,
    pub(super) dialect: &'a Dialect,
    pub(super) edits: &'a EditSet,
    pub(super) window_size: NonZeroU64,
}

/// [`save_edited`] with the directory an object's temporary file is created
/// in named by the caller. A local file is always staged next to its target.
pub(super) fn save_staging_objects_in(
    location: &DelimitedLocation,
    request: &SaveRequest<'_>,
    object_staging_directory: &Path,
) -> Result<SaveOutcome, StorageError> {
    match location {
        DelimitedLocation::Local { path } => save_local(path, request).map(SaveOutcome::Saved),

        DelimitedLocation::Object {
            connection,
            bucket,
            key,
        } => {
            save_object(connection, bucket, key, request, object_staging_directory)?;

            Ok(match read_version(location) {
                Ok(version) => SaveOutcome::Saved(version),
                Err(error) => SaveOutcome::SavedVersionUnknown(error),
            })
        }
    }
}

/// Refuses a save when the file is no longer the `captured` version, or when
/// its version could not show a change that kept the length.
pub(super) fn verify_version(
    captured: &SourceVersion,
    current: &SourceVersion,
) -> Result<(), StorageError> {
    if current != captured {
        return Err(StorageError::SourceChanged);
    }

    if !current.detects_same_length_change() {
        return Err(StorageError::VersionUnverifiable);
    }

    Ok(())
}

fn save_local(path: &Path, request: &SaveRequest<'_>) -> Result<SourceVersion, StorageError> {
    let destination =
        resolve_write_destination(path).map_err(|error| StorageError::local_io(path, error))?;

    let (file, metadata) = open_local_file(&destination)?;

    verify_version(request.captured, &local_version(&metadata))?;

    if metadata.permissions().readonly() {
        return Err(read_only_file_error(path));
    }

    check_write_access(path, &destination)?;

    let source = FileSource::new(file);

    let target = path.display().to_string();
    let directory = parent_directory(&destination);
    let staged_path = stage_path(directory);

    let staging_error = |error| StorageError::TemporaryFile {
        target: target.clone(),
        directory: directory.to_path_buf(),
        source: error,
    };

    let staged = create_staging_file(&staged_path).map_err(staging_error)?;

    let committed = write_staged(&source, request, &staged, &target, directory).and_then(|()| {
        #[cfg(test)]
        run_while_staging_hook();

        let saved = commit_staged(&staged, &metadata).map_err(staging_error)?;

        // Staging a large file takes long enough for another process to
        // change the target meanwhile, so its version is read again last.
        let latest =
            std::fs::metadata(&destination).map_err(|error| StorageError::local_io(path, error))?;

        verify_version(request.captured, &local_version(&latest))?;

        // Windows refuses to replace a file that is still open.
        drop(source);

        std::fs::rename(&staged_path, &destination)
            .map_err(|error| StorageError::local_io(path, error))?;

        Ok(saved)
    });

    match committed {
        Ok(saved) => {
            sync_directory(directory);

            Ok(saved)
        }

        Err(error) => {
            discard_staging(&staged_path);

            Err(error)
        }
    }
}

/// The refusal of a save to a file whose permission bits forbid writing.
fn read_only_file_error(path: &Path) -> StorageError {
    StorageError::local_io(
        path,
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            dbflux_i18n::t!("document.delimited.error.storage.read_only_file"),
        ),
    )
}

/// Checks that a save of the local file at `path` could replace it, the way
/// [`save_edited`] would: the path resolves, the file's permission bits allow
/// writing, the operating system lets this process open it for writing, and
/// a staging file can be created in the directory the file is in. The staging file is created exactly as a save creates one and removed
/// at once, so nothing stays behind; a removal that fails is traced.
///
/// Blocks on a few file system calls. A save that passes this can still fail
/// when it writes, for example on a full disk.
pub(super) fn check_local_save(path: &Path) -> Result<(), StorageError> {
    let destination =
        resolve_write_destination(path).map_err(|error| StorageError::local_io(path, error))?;

    let metadata =
        std::fs::metadata(&destination).map_err(|error| StorageError::local_io(path, error))?;

    if metadata.permissions().readonly() {
        return Err(read_only_file_error(path));
    }

    check_write_access(path, &destination)?;

    let directory = parent_directory(&destination);
    let probe = stage_path(directory);

    create_staging_file(&probe).map_err(|error| StorageError::TemporaryFile {
        target: path.display().to_string(),
        directory: directory.to_path_buf(),
        source: error,
    })?;

    discard_staging(&probe);

    Ok(())
}

/// Refuses a save to `destination` when the operating system does not let
/// this process open it for writing, which also covers ownership and access
/// control lists that the permission bits do not show. The file is opened
/// without truncation and closed at once, so its content is untouched.
fn check_write_access(path: &Path, destination: &Path) -> Result<(), StorageError> {
    std::fs::OpenOptions::new()
        .write(true)
        .open(destination)
        .map(drop)
        .map_err(|error| StorageError::local_io(path, error))
}

#[cfg(test)]
thread_local! {
    static WHILE_STAGING: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Runs `hook` once, on this thread, during the next local save, after the
/// edited bytes are staged and before the target is replaced, the way another
/// process could change the target while a save writes.
#[cfg(test)]
pub(super) fn while_next_local_save_stages(hook: impl FnOnce() + 'static) {
    WHILE_STAGING.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

#[cfg(test)]
fn run_while_staging_hook() {
    if let Some(hook) = WHILE_STAGING.with(|slot| slot.borrow_mut().take()) {
        hook();
    }
}

/// Gives the staged file the permissions of the file it replaces and syncs
/// it to disk, then returns the version it will have once renamed: a rename
/// keeps both the modification time and the length.
fn commit_staged(
    staged: &std::fs::File,
    replaced: &std::fs::Metadata,
) -> std::io::Result<SourceVersion> {
    staged.set_permissions(replaced.permissions())?;
    staged.sync_all()?;

    Ok(local_version(&staged.metadata()?))
}

/// Syncs `directory` so a rename inside it survives a crash. The rename has
/// already replaced the target when this runs, so a failure is traced and
/// never reported as a failed save.
#[cfg(unix)]
fn sync_directory(directory: &Path) {
    if let Err(error) = std::fs::File::open(directory).and_then(|handle| handle.sync_all()) {
        log::warn!(
            "Failed to sync directory {} after a save: {error}",
            directory.display()
        );
    }
}

#[cfg(not(unix))]
fn sync_directory(_directory: &Path) {}

#[allow(clippy::result_large_err)]
fn save_object(
    connection: &Arc<dyn Connection>,
    bucket: &str,
    key: &str,
    request: &SaveRequest<'_>,
    staging_directory: &Path,
) -> Result<(), StorageError> {
    let metadata = head_object(connection.as_ref(), bucket, key)?;

    verify_version(request.captured, &object_version(&metadata))?;

    let source = ObjectSource::new(connection.clone(), bucket, key, metadata.size_bytes);

    let target = format!("{bucket}/{key}");
    let staged_path = stage_path(staging_directory);

    let staged =
        create_staging_file(&staged_path).map_err(|error| StorageError::TemporaryFile {
            target: target.clone(),
            directory: staging_directory.to_path_buf(),
            source: error,
        })?;

    let uploaded =
        write_staged(&source, request, &staged, &target, staging_directory).and_then(|()| {
            drop(staged);

            let latest = head_object(connection.as_ref(), bucket, key)?;

            verify_version(request.captured, &object_version(&latest))?;

            object_store(connection.as_ref())
                .and_then(|api| {
                    api.upload_object(bucket, key, &staged_path, latest.content_type.as_deref())
                })
                .map_err(|error| StorageError::object_store(bucket, key, error))
        });

    discard_staging(&staged_path);

    uploaded
}

/// Writes `source` with the request's edits applied into the staged file.
///
/// `target` names the file or object being saved and `directory` the place
/// the staged file lives in, for the error a failed write of it reports.
pub(super) fn write_staged<S: ByteSource>(
    source: &S,
    request: &SaveRequest<'_>,
    staged: &std::fs::File,
    target: &str,
    directory: &Path,
) -> Result<(), StorageError> {
    let mut sink = BufWriter::new(staged);

    write_edited(
        source,
        request.dialect,
        request.edits,
        request.window_size,
        &mut sink,
    )
    .map_err(|error| match error {
        WriteError::Source(error) => StorageError::Read(error),

        WriteError::Sink(error) => StorageError::TemporaryFile {
            target: target.to_string(),
            directory: directory.to_path_buf(),
            source: error,
        },

        other => StorageError::Write(other),
    })
}

/// Resolves the path the bytes must land on.
///
/// Symlinks are followed: replacing the link itself with a regular file would
/// detach the document from the file the link points at.
fn resolve_write_destination(path: &Path) -> std::io::Result<PathBuf> {
    std::fs::canonicalize(path)
}

/// The directory a file lives in. A bare file name means the current
/// directory.
fn parent_directory(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// A unique hidden staging name inside `directory`. Staging next to a local
/// target keeps the commit rename on one file system.
fn stage_path(directory: &Path) -> PathBuf {
    directory.join(format!(".dbflux-stage-{}.tmp", uuid::Uuid::new_v4()))
}

/// Creates the staging file exclusively. On unix it is created readable and
/// writable by its owner only, so the uncommitted bytes are never readable
/// by others under their temporary name.
///
/// A name collision fails at `create_new`, before the file belongs to this
/// save, so an existing file is never removed.
fn create_staging_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }

    options.open(path)
}

/// Removes a staging file that must not be committed. A leftover that cannot
/// be removed is traced, because the error that led here is the one the
/// caller reports.
fn discard_staging(staged_path: &Path) {
    if let Err(error) = std::fs::remove_file(staged_path) {
        log::warn!(
            "Failed to remove staging file {}: {error}",
            staged_path.display()
        );
    }
}
