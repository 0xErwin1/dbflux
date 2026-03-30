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

    #[error("data directory not found — cannot resolve state database path")]
    DataDirUnavailable,

    #[error("legacy import failed: {0}")]
    LegacyImportFailed(String),
}

impl StorageError {
    /// Returns the inner `rusqlite::Error` if this is a `Sqlite` variant, otherwise `None`.
    pub fn into_sqlite_error(self) -> Option<rusqlite::Error> {
        match self {
            StorageError::Sqlite { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for StorageError {
    fn from(source: rusqlite::Error) -> Self {
        StorageError::Sqlite {
            path: PathBuf::from("<unknown>"),
            source,
        }
    }
}

impl From<StorageError> for dbflux_core::DbError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::Sqlite { source, .. } => {
                dbflux_core::DbError::query_failed(source.to_string())
            }
            StorageError::Io { source, .. } => dbflux_core::DbError::IoError(source),
            StorageError::ConfigDirUnavailable => {
                dbflux_core::DbError::InvalidProfile("config directory not available".to_string())
            }
            StorageError::DataDirUnavailable => {
                dbflux_core::DbError::InvalidProfile("data directory not available".to_string())
            }
            StorageError::LegacyImportFailed(msg) => dbflux_core::DbError::InvalidProfile(msg),
        }
    }
}
