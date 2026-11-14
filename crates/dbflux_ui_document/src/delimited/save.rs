//! How an edited delimited file replaces the one that was opened.
//!
//! The version checks, the staging file and the replacement of the target
//! belong to [`crate::file_save`]. This module produces the edited bytes.

use std::io::BufWriter;
use std::num::NonZeroU64;
use std::path::Path;

use dbflux_delimited::{ByteSource, Dialect, EditSet, WriteError, write_edited};

use crate::file_save::StageWriteError;
use crate::file_source::{FileLocation, SourceVersion, StorageError, WriteFailure};

pub use crate::file_save::SaveOutcome;

#[cfg(test)]
pub(super) use crate::file_save::{verify_version, while_next_local_save_stages};

/// Saves the file at `location` with `edits` applied.
///
/// The save follows [`crate::file_save::save_file`]: `Ok` means the target
/// was replaced with the edited bytes, and `Err` means the target holds what
/// it held before the call. `captured` is the version the byte ranges in
/// `edits` were read from, and a file that is no longer that version is
/// refused before anything is written.
///
/// The edited bytes are produced by `dbflux_delimited::write_edited`, reading
/// the source in ranges of `window_size` bytes, into a temporary file and
/// never into the target. Neither the old nor the new file is held whole in
/// memory.
///
/// # After a save
///
/// Every byte range read before the save is stale. The caller opens a new
/// source with [`crate::file_source::open_source`] and drops its stored ranges and edits.
pub fn save_edited(
    location: &FileLocation,
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

    crate::file_save::save_file(location, request.captured, |source, sink| {
        write_edits(source, &request, sink)
    })
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
#[cfg(test)]
pub(super) fn save_staging_objects_in(
    location: &FileLocation,
    request: &SaveRequest<'_>,
    object_staging_directory: &Path,
) -> Result<SaveOutcome, StorageError> {
    crate::file_save::save_staging_objects_in(
        location,
        request.captured,
        object_staging_directory,
        |source, sink| write_edits(source, request, sink),
    )
}

/// Writes `source` with the request's edits applied into the staged file.
///
/// `target` names the file or object being saved and `directory` the place
/// the staged file lives in, for the error a failed write of it reports.
/// The error path it exercises needs a device that fails writes, so the only
/// caller is the `/dev/full` test on Linux.
#[cfg(all(test, target_os = "linux"))]
pub(super) fn write_staged<S: ByteSource>(
    source: &S,
    request: &SaveRequest<'_>,
    staged: &std::fs::File,
    target: &str,
    directory: &Path,
) -> Result<(), StorageError> {
    crate::file_save::write_stage(staged, target, directory, |sink| {
        write_edits(source, request, sink)
    })
}

/// Writes `source` with the request's edits applied into `sink`.
fn write_edits<S: ByteSource>(
    source: &S,
    request: &SaveRequest<'_>,
    sink: &mut BufWriter<&std::fs::File>,
) -> Result<(), StageWriteError> {
    write_edited(
        source,
        request.dialect,
        request.edits,
        request.window_size,
        sink,
    )
    .map_err(|error| match error {
        WriteError::Source(error) => StageWriteError::Storage(StorageError::Read(error)),

        WriteError::Sink(error) => StageWriteError::Sink(error),

        other => StageWriteError::Storage(StorageError::Write(WriteFailure::Delimited(other))),
    })
}
