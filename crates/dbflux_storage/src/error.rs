use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage sqlite error for {path}: {source}")]
    Sqlite {
        path: PathBuf,
        source: rusqlite::Error,
    },

    #[error("storage io error for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("config directory not found — cannot resolve storage path")]
    ConfigDirUnavailable,
}
