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

    #[error("storage data error: {0}")]
    Data(String),

    #[error("migration {kind} verification failed: {details}")]
    Migration { kind: String, details: String },
}

/// Error type for repository operations.
#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("repository sqlite error: {source}")]
    Sqlite { source: rusqlite::Error },

    #[error("entity not found: {0}")]
    NotFound(String),

    #[error("serialization error: {source}")]
    Serialization { source: serde_json::Error },
}

impl From<rusqlite::Error> for RepositoryError {
    fn from(source: rusqlite::Error) -> Self {
        RepositoryError::Sqlite { source }
    }
}

impl From<serde_json::Error> for RepositoryError {
    fn from(source: serde_json::Error) -> Self {
        RepositoryError::Serialization { source }
    }
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
            StorageError::Data(msg) => dbflux_core::DbError::InvalidProfile(msg),
            StorageError::Migration { kind, details } => dbflux_core::DbError::InvalidProfile(
                format!("migration {} failed: {}", kind, details),
            ),
        }
    }
}

impl From<RepositoryError> for StorageError {
    fn from(err: RepositoryError) -> Self {
        match err {
            RepositoryError::Sqlite { source } => StorageError::Sqlite {
                path: PathBuf::from("<unknown>"),
                source,
            },
            RepositoryError::NotFound(msg) => StorageError::Data(msg),
            RepositoryError::Serialization { source } => {
                StorageError::Data(format!("serialization error: {}", source))
            }
        }
    }
}
