use thiserror::Error;

/// Database operation errors.
///
/// All driver operations return this error type to provide consistent
/// error handling across different database backends.
#[derive(Debug, Error)]
pub enum DbError {
    /// Failed to establish a connection to the database.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Query execution failed (syntax error, constraint violation, etc.).
    #[error("{0}")]
    QueryFailed(String),

    /// Query exceeded the configured timeout.
    #[error("Query timed out")]
    Timeout,

    /// Query was cancelled via `Connection::cancel()`.
    #[error("Query cancelled")]
    Cancelled,

    /// Operation not supported by this database (e.g., SQLite cancellation).
    #[error("Operation not supported: {0}")]
    NotSupported(String),

    /// Connection profile is malformed or missing required fields.
    #[error("Invalid profile: {0}")]
    InvalidProfile(String),

    /// Filesystem or network I/O error.
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}
