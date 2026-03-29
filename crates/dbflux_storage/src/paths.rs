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
pub fn data_dir() -> Result<PathBuf, StorageError> {
    let base = dirs::data_dir().ok_or(StorageError::DataDirUnavailable)?;
    let dir = base.join("dbflux");
    std::fs::create_dir_all(&dir).map_err(|source| StorageError::Io {
        path: dir.clone(),
        source,
    })?;
    Ok(dir)
}

/// Returns the path for the config database (`config.db`).
pub fn config_db_path() -> Result<PathBuf, StorageError> {
    Ok(config_data_dir()?.join("config.db"))
}

/// Returns the path for the state database (`state.db`).
pub fn state_db_path() -> Result<PathBuf, StorageError> {
    Ok(data_dir()?.join("state.db"))
}
