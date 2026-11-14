//! How an edited file document replaces the file it opened.
//!
//! The steps every format shares live here: the version checks, the staging
//! file and its permissions, the sync and rename that replace a local file,
//! and the upload that replaces an object. What the edited bytes are is the
//! caller's: a stage writer reads the opened file and writes the new bytes
//! into the staging file. Every function blocks on file or network I/O and
//! touches no GPUI state, so callers run it on the background executor and
//! report the returned errors themselves.

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dbflux_byte_source::FileSource;
use dbflux_core::Connection;

use crate::file_source::{
    FileLocation, LocationSource, ObjectSource, SourceVersion, StorageError, head_object,
    local_version, object_store, object_version, open_local_file, read_version,
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

/// Why a stage writer could not produce the edited bytes.
#[derive(Debug)]
pub(crate) enum StageWriteError {
    /// Writing into the staging file failed. The save reports it as a
    /// [`StorageError::TemporaryFile`] naming the target and the staging
    /// directory.
    Sink(std::io::Error),

    /// Any other failure, reported as it is.
    Storage(StorageError),
}

/// Saves the file at `location` with the bytes `write` produces.
///
/// `Ok` means the target was replaced with those bytes. `Err` means it was
/// not: the target holds what it held before the call and the temporary file
/// is removed.
///
/// `captured` is the version the caller's edits were read from. The file's
/// current version is read first. A file that is no longer that version is
/// refused with [`StorageError::SourceChanged`], and a version that cannot
/// show a change of the same length (see
/// [`SourceVersion::detects_same_length_change`]) is refused with
/// [`StorageError::VersionUnverifiable`], both before anything is written.
///
/// `write` is called once, with the opened file as a byte source and a
/// buffered writer over a temporary file, never over the target. It reads the
/// source by range, so neither the old nor the new file needs to be held
/// whole in memory. The buffer is flushed after it returns.
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
/// source with [`crate::file_source::open_source`] and drops its stored ranges and edits.
pub(crate) fn save_file<W>(
    location: &FileLocation,
    captured: &SourceVersion,
    write: W,
) -> Result<SaveOutcome, StorageError>
where
    W: FnOnce(&LocationSource, &mut BufWriter<&std::fs::File>) -> Result<(), StageWriteError>,
{
    save_staging_objects_in(location, captured, &std::env::temp_dir(), write)
}

/// [`save_file`] with the directory an object's temporary file is created in
/// named by the caller. A local file is always staged next to its target.
pub(crate) fn save_staging_objects_in<W>(
    location: &FileLocation,
    captured: &SourceVersion,
    object_staging_directory: &Path,
    write: W,
) -> Result<SaveOutcome, StorageError>
where
    W: FnOnce(&LocationSource, &mut BufWriter<&std::fs::File>) -> Result<(), StageWriteError>,
{
    match location {
        FileLocation::Local { path } => save_local(path, captured, write).map(SaveOutcome::Saved),

        FileLocation::Object {
            connection,
            bucket,
            key,
        } => {
            save_object(
                connection,
                bucket,
                key,
                captured,
                object_staging_directory,
                write,
            )?;

            Ok(match read_version(location) {
                Ok(version) => SaveOutcome::Saved(version),
                Err(error) => SaveOutcome::SavedVersionUnknown(error),
            })
        }
    }
}

/// Refuses a save when the file is no longer the `captured` version, or when
/// its version could not show a change that kept the length.
pub(crate) fn verify_version(
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

fn save_local<W>(
    path: &Path,
    captured: &SourceVersion,
    write: W,
) -> Result<SourceVersion, StorageError>
where
    W: FnOnce(&LocationSource, &mut BufWriter<&std::fs::File>) -> Result<(), StageWriteError>,
{
    let destination =
        resolve_write_destination(path).map_err(|error| StorageError::local_io(path, error))?;

    let (file, metadata) = open_local_file(&destination)?;

    verify_version(captured, &local_version(&metadata))?;

    if metadata.permissions().readonly() {
        return Err(read_only_file_error(path));
    }

    check_write_access(path, &destination)?;

    let source = LocationSource::Local(FileSource::new(file));

    let target = path.display().to_string();
    let directory = parent_directory(&destination);
    let staged_path = stage_path(directory);

    let staging_error = |error| StorageError::TemporaryFile {
        target: target.clone(),
        directory: directory.to_path_buf(),
        source: error,
    };

    let staged = create_staging_file(&staged_path).map_err(staging_error)?;

    let committed =
        write_stage(&staged, &target, directory, |sink| write(&source, sink)).and_then(|()| {
            #[cfg(test)]
            run_while_staging_hook();

            let saved = commit_staged(&staged, &metadata).map_err(staging_error)?;

            // Staging a large file takes long enough for another process to
            // change the target meanwhile, so its version is read again last.
            let latest = std::fs::metadata(&destination)
                .map_err(|error| StorageError::local_io(path, error))?;

            verify_version(captured, &local_version(&latest))?;

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
            dbflux_i18n::t!("document.file.error.storage.read_only_file"),
        ),
    )
}

/// Checks that a save of the local file at `path` could replace it, the way
/// [`save_file`] would: the path resolves, the file's permission bits allow
/// writing, the operating system lets this process open it for writing, and
/// a staging file can be created in the directory the file is in. The staging file is created exactly as a save creates one and removed
/// at once, so nothing stays behind; a removal that fails is traced.
///
/// Blocks on a few file system calls. A save that passes this can still fail
/// when it writes, for example on a full disk.
pub(crate) fn check_local_save(path: &Path) -> Result<(), StorageError> {
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
pub(crate) fn while_next_local_save_stages(hook: impl FnOnce() + 'static) {
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
fn save_object<W>(
    connection: &Arc<dyn Connection>,
    bucket: &str,
    key: &str,
    captured: &SourceVersion,
    staging_directory: &Path,
    write: W,
) -> Result<(), StorageError>
where
    W: FnOnce(&LocationSource, &mut BufWriter<&std::fs::File>) -> Result<(), StageWriteError>,
{
    let metadata = head_object(connection.as_ref(), bucket, key)?;

    verify_version(captured, &object_version(&metadata))?;

    let source = LocationSource::Object(ObjectSource::new(
        connection.clone(),
        bucket,
        key,
        metadata.size_bytes,
    ));

    let target = format!("{bucket}/{key}");
    let staged_path = stage_path(staging_directory);

    let staged =
        create_staging_file(&staged_path).map_err(|error| StorageError::TemporaryFile {
            target: target.clone(),
            directory: staging_directory.to_path_buf(),
            source: error,
        })?;

    let uploaded = write_stage(&staged, &target, staging_directory, |sink| {
        write(&source, sink)
    })
    .and_then(|()| {
        drop(staged);

        let latest = head_object(connection.as_ref(), bucket, key)?;

        verify_version(captured, &object_version(&latest))?;

        object_store(connection.as_ref())
            .and_then(|api| {
                api.upload_object(bucket, key, &staged_path, latest.content_type.as_deref())
            })
            .map_err(|error| StorageError::object_store(bucket, key, error))
    });

    discard_staging(&staged_path);

    uploaded
}

/// Runs `write` over a buffered writer into the staged file and flushes it.
///
/// `target` names the file or object being saved and `directory` the place
/// the staged file lives in, for the error a failed write of it reports.
pub(crate) fn write_stage<W>(
    staged: &std::fs::File,
    target: &str,
    directory: &Path,
    write: W,
) -> Result<(), StorageError>
where
    W: FnOnce(&mut BufWriter<&std::fs::File>) -> Result<(), StageWriteError>,
{
    let sink_error = |error| StorageError::TemporaryFile {
        target: target.to_string(),
        directory: directory.to_path_buf(),
        source: error,
    };

    let mut sink = BufWriter::new(staged);

    write(&mut sink).map_err(|error| match error {
        StageWriteError::Sink(error) => sink_error(error),
        StageWriteError::Storage(error) => error,
    })?;

    sink.flush().map_err(sink_error)
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

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::{Path, PathBuf};

    use dbflux_byte_source::ByteSource;

    use super::{SaveOutcome, StageWriteError, save_staging_objects_in};
    use crate::file_source::{FileLocation, StorageError, read_version};

    const ORIGINAL: &[u8] = b"original bytes";
    const REPLACED: &[u8] = b"replaced bytes, longer than before";

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir()
                .join(format!("dbflux-file-save-{name}-{}", uuid::Uuid::new_v4()));

            std::fs::create_dir_all(&path).expect("the test directory must be creatable");

            Self { path }
        }

        fn file(&self, name: &str, bytes: &[u8]) -> (PathBuf, FileLocation) {
            let path = self.path.join(name);
            std::fs::write(&path, bytes).expect("the test file must be writable");

            let location = FileLocation::Local { path: path.clone() };

            (path, location)
        }

        fn entry_names(&self) -> Vec<String> {
            let mut names: Vec<String> = std::fs::read_dir(&self.path)
                .expect("the test directory must be readable")
                .filter_map(Result::ok)
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect();

            names.sort();
            names
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.path).ok();
        }
    }

    fn unused_staging() -> &'static Path {
        Path::new("/nonexistent-object-staging")
    }

    #[test]
    fn generic_save_refuses_changed_version() {
        let directory = TestDirectory::new("changed-version");
        let (path, location) = directory.file("sheet.bin", ORIGINAL);

        let captured = read_version(&location).expect("the version reads");

        let foreign = b"changed elsewhere, with another length";
        std::fs::write(&path, foreign).expect("the foreign write lands");

        let mut writer_ran = false;

        let error = save_staging_objects_in(&location, &captured, unused_staging(), |_, sink| {
            writer_ran = true;
            sink.write_all(REPLACED).map_err(StageWriteError::Sink)
        })
        .expect_err("a file changed since it was opened must be refused");

        assert!(matches!(error, StorageError::SourceChanged), "{error}");
        assert!(!writer_ran, "nothing is staged for a changed file");
        assert_eq!(std::fs::read(&path).expect("the file reads"), foreign);
        assert_eq!(directory.entry_names(), ["sheet.bin"]);
    }

    #[test]
    fn generic_save_keeps_permissions_and_renames_atomically() {
        let directory = TestDirectory::new("permissions");
        let (path, location) = directory.file("sheet.bin", ORIGINAL);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640))
                .expect("the permissions must be settable");
        }

        #[cfg(unix)]
        let original_inode = {
            use std::os::unix::fs::MetadataExt;

            std::fs::metadata(&path)
                .expect("the file has metadata")
                .ino()
        };

        let captured = read_version(&location).expect("the version reads");

        let outcome =
            save_staging_objects_in(&location, &captured, unused_staging(), |source, sink| {
                let length = source.byte_length().expect("the source has a length");
                let bytes = source.read_range(0..length).expect("the source reads");

                assert_eq!(bytes, ORIGINAL, "the writer reads the opened file");
                assert_eq!(
                    std::fs::read(&path).expect("the target reads"),
                    ORIGINAL,
                    "the target is untouched while the new bytes are staged"
                );

                sink.write_all(REPLACED).map_err(StageWriteError::Sink)
            })
            .expect("the save lands");

        let SaveOutcome::Saved(saved) = outcome else {
            panic!("a local save always knows its new version");
        };

        assert_eq!(std::fs::read(&path).expect("the file reads"), REPLACED);
        assert_eq!(saved, read_version(&location).expect("the version reads"));
        assert_eq!(directory.entry_names(), ["sheet.bin"]);

        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};

            let metadata = std::fs::metadata(&path).expect("the file has metadata");

            assert_eq!(metadata.permissions().mode() & 0o777, 0o640);
            assert_ne!(
                metadata.ino(),
                original_inode,
                "the staged file is renamed onto the target, not written into it"
            );
        }
    }
}
