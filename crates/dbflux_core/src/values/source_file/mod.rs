//! Opens a user-owned file named by a value reference at the moment the value
//! is used.
//!
//! The file is opened on every read, so a file replaced by an atomic rename or
//! a symlink swap is picked up on the next read. Every error names the file
//! under a caller-chosen label but never its content.

use std::io::Read;
use std::path::PathBuf;

use secrecy::SecretString;

use crate::DbError;

/// Largest file a value source reads.
pub(crate) const MAX_FILE_BYTES: u64 = 64 * 1024;

/// Permission bits that let anyone but the owner read or change the file.
#[cfg(unix)]
const UNSAFE_MODE_BITS: u32 = 0o066;

/// Reads the whole file at `path` as UTF-8.
///
/// `path` must be absolute or start with `~/`. The file must be a regular
/// file, at most [`MAX_FILE_BYTES`] long, valid UTF-8, and on Unix readable
/// and writable by its owner only. `label` starts every error message, for
/// example `"Dotenv file"`.
pub(crate) fn read_owner_only_file(label: &str, path: &str) -> Result<SecretString, DbError> {
    let resolved_path = resolve_path(label, path)?;
    read_file(label, path, &resolved_path)
}

fn error(message: String) -> DbError {
    DbError::value_resolution_failed(message)
}

fn resolve_path(label: &str, path: &str) -> Result<PathBuf, DbError> {
    let trimmed = path.trim();

    if trimmed.is_empty() {
        return Err(error(format!("{label} path is empty")));
    }

    if let Some(rest) = trimmed.strip_prefix("~/") {
        let home = dirs::home_dir().ok_or_else(|| {
            error(format!(
                "{label} '{path}' starts with '~' but the home directory is unknown"
            ))
        })?;

        return Ok(home.join(rest));
    }

    let candidate = PathBuf::from(trimmed);

    if !candidate.is_absolute() {
        return Err(error(format!(
            "{label} '{path}' must be an absolute path or start with '~/'"
        )));
    }

    Ok(candidate)
}

/// Opening a FIFO for reading blocks until a writer appears, which would hang the
/// resolver before the regular-file check could reject it. `O_NONBLOCK` makes that open
/// return at once; it has no effect on reads from a regular file.
#[cfg(unix)]
fn open_without_blocking(path: &PathBuf) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
}

#[cfg(not(unix))]
fn open_without_blocking(path: &PathBuf) -> std::io::Result<std::fs::File> {
    std::fs::File::open(path)
}

fn read_file(label: &str, display_path: &str, path: &PathBuf) -> Result<SecretString, DbError> {
    let file = open_without_blocking(path).map_err(|io_error| match io_error.kind() {
        std::io::ErrorKind::NotFound => error(format!("{label} '{display_path}' does not exist")),
        std::io::ErrorKind::PermissionDenied => error(format!(
            "{label} '{display_path}' cannot be read: permission denied"
        )),
        _ => error(format!(
            "{label} '{display_path}' cannot be opened: {io_error}"
        )),
    })?;

    // Checked on the open handle, so the file that is read is the file whose
    // metadata passed, even if the path is swapped in between.
    let metadata = file.metadata().map_err(|io_error| {
        error(format!(
            "{label} '{display_path}' cannot be inspected: {io_error}"
        ))
    })?;

    if !metadata.is_file() {
        return Err(error(format!(
            "{label} '{display_path}' is not a regular file"
        )));
    }

    check_permissions(label, display_path, &metadata)?;

    if metadata.len() > MAX_FILE_BYTES {
        return Err(too_large(label, display_path));
    }

    let mut bytes = Vec::new();
    file.take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|io_error| {
            error(format!(
                "{label} '{display_path}' cannot be read: {io_error}"
            ))
        })?;

    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err(too_large(label, display_path));
    }

    let content = String::from_utf8(bytes)
        .map_err(|_| error(format!("{label} '{display_path}' is not valid UTF-8")))?;

    Ok(SecretString::from(content))
}

fn too_large(label: &str, display_path: &str) -> DbError {
    error(format!(
        "{label} '{display_path}' is larger than {} KiB",
        MAX_FILE_BYTES / 1024
    ))
}

#[cfg(unix)]
fn check_permissions(
    label: &str,
    display_path: &str,
    metadata: &std::fs::Metadata,
) -> Result<(), DbError> {
    use std::os::unix::fs::PermissionsExt;

    let mode = metadata.permissions().mode() & 0o777;

    if mode & UNSAFE_MODE_BITS != 0 {
        return Err(error(format!(
            "{label} '{display_path}' can be read or changed by other users (mode {mode:o}); restrict it with chmod 600"
        )));
    }

    Ok(())
}

#[cfg(not(unix))]
fn check_permissions(
    _label: &str,
    _display_path: &str,
    _metadata: &std::fs::Metadata,
) -> Result<(), DbError> {
    Ok(())
}
