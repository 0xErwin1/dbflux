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

/// Returns the path for the internal application database.
pub fn app_db_path() -> Result<PathBuf, StorageError> {
    Ok(config_data_dir()?.join("dbflux.sqlite"))
}
