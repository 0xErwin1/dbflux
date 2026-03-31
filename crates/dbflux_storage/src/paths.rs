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

/// Returns the path for the unified database (`dbflux.db`).
pub fn dbflux_db_path() -> Result<PathBuf, StorageError> {
    Ok(data_dir()?.join("dbflux.db"))
}
