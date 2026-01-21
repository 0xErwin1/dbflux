use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Query failed: {0}")]
    QueryFailed(String),

    #[error("Query timed out")]
    Timeout,

    #[error("Query cancelled")]
    Cancelled,

    #[error("Operation not supported: {0}")]
    NotSupported(String),

    #[error("Invalid profile: {0}")]
    InvalidProfile(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}
