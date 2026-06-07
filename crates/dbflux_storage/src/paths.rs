use std::path::PathBuf;

use crate::error::StorageError;

/// Returns `~/.config/dbflux/`, creating it if necessary.
pub fn config_data_dir() -> Result<PathBuf, StorageError> {
    let base = dirs::config_dir().ok_or(StorageError::ConfigDirUnavailable)?;
    let dir = base.join("dbflux");
    std::fs::create_dir_all(&dir).map_err(|source| StorageError::Io {
        path: dir.clone(),
        source,
    })?;
    Ok(dir)
}

/// Returns `~/.local/share/dbflux/`, creating it if necessary.
///
/// This directory is used for:
/// - The unified `dbflux.db` database
/// - Session artifacts and scratch files
pub fn data_dir() -> Result<PathBuf, StorageError> {
    let base = dirs::data_dir().ok_or(StorageError::DataDirUnavailable)?;
    let dir = base.join("dbflux");
    std::fs::create_dir_all(&dir).map_err(|source| StorageError::Io {
        path: dir.clone(),
        source,
    })?;
    Ok(dir)
}

/// Marker file inside the data directory that opts a nightly build into the
/// stable database. Its presence is the whole signal — the file is empty.
///
/// This setting cannot live in the database itself: it decides *which* database
/// to open, so it must be readable before any database is opened.
fn shared_db_marker_path() -> Result<PathBuf, StorageError> {
    Ok(data_dir()?.join("use-stable-db"))
}

/// Whether the running nightly build is configured to share the stable
/// database instead of its own `dbflux-nightly.db`.
///
/// Always `false` outside nightly. Best-effort: a probing error is treated as
/// "not shared" so a filesystem hiccup never silently redirects writes onto the
/// stable database.
pub fn nightly_shares_stable_db() -> bool {
    if dbflux_core::ReleaseChannel::current() != dbflux_core::ReleaseChannel::Nightly {
        return false;
    }

    shared_db_marker_path()
        .map(|path| path.exists())
        .unwrap_or(false)
}

/// Opts the nightly build into (`enabled`) or out of (`!enabled`) the stable
/// database. The change takes effect on the next launch, since the database is
/// opened once at startup.
pub fn set_nightly_shares_stable_db(enabled: bool) -> Result<(), StorageError> {
    let path = shared_db_marker_path()?;

    if enabled {
        std::fs::write(&path, b"").map_err(|source| StorageError::Io {
            path: path.clone(),
            source,
        })?;
    } else if path.exists() {
        std::fs::remove_file(&path).map_err(|source| StorageError::Io {
            path: path.clone(),
            source,
        })?;
    }

    Ok(())
}

/// Returns the path for the unified database inside the data directory.
///
/// The file name is channel-specific: nightly builds use `dbflux-nightly.db`
/// so a pre-release migration cannot corrupt the stable `dbflux.db` of a user
/// who runs both channels side by side. A nightly build may opt into the stable
/// database via [`set_nightly_shares_stable_db`].
pub fn dbflux_db_path() -> Result<PathBuf, StorageError> {
    let file_name = if nightly_shares_stable_db() {
        "dbflux.db"
    } else {
        dbflux_core::ReleaseChannel::current().db_file_name()
    };
    Ok(data_dir()?.join(file_name))
}
