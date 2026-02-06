use crate::FormattedError;
use thiserror::Error;

/// Database operation errors.
///
/// All driver operations return this error type to provide consistent
/// error handling across different database backends. Variants carrying
/// `FormattedError` preserve structured information (detail, hint, code,
/// location) from the database for rich UI display.
#[derive(Debug, Error)]
pub enum DbError {
    /// Failed to establish a connection to the database.
    #[error("Connection failed: {0}")]
    ConnectionFailed(FormattedError),

    /// Query execution failed (general catch-all for query errors).
    #[error("{0}")]
    QueryFailed(FormattedError),

    /// Authentication failed (wrong password, expired credentials, etc.).
    #[error("Authentication failed: {0}")]
    AuthFailed(FormattedError),

    /// A constraint was violated (unique, foreign key, check, not null).
    #[error("Constraint violation: {0}")]
    ConstraintViolation(FormattedError),

    /// Query has a syntax error.
    #[error("Syntax error: {0}")]
    SyntaxError(FormattedError),

    /// Insufficient privileges for the operation.
    #[error("Permission denied: {0}")]
    PermissionDenied(FormattedError),

    /// Referenced object (table, column, function, etc.) does not exist.
    #[error("Object not found: {0}")]
    ObjectNotFound(FormattedError),

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

impl DbError {
    pub fn connection_failed(msg: impl Into<String>) -> Self {
        Self::ConnectionFailed(FormattedError::new(msg))
    }

    pub fn query_failed(msg: impl Into<String>) -> Self {
        Self::QueryFailed(FormattedError::new(msg))
    }

    pub fn auth_failed(msg: impl Into<String>) -> Self {
        Self::AuthFailed(FormattedError::new(msg))
    }

    pub fn constraint_violation(msg: impl Into<String>) -> Self {
        Self::ConstraintViolation(FormattedError::new(msg))
    }

    pub fn syntax_error(msg: impl Into<String>) -> Self {
        Self::SyntaxError(FormattedError::new(msg))
    }

    pub fn permission_denied(msg: impl Into<String>) -> Self {
        Self::PermissionDenied(FormattedError::new(msg))
    }

    pub fn object_not_found(msg: impl Into<String>) -> Self {
        Self::ObjectNotFound(FormattedError::new(msg))
    }

    /// Access the structured error information, if the variant carries one.
    pub fn formatted(&self) -> Option<&FormattedError> {
        match self {
            Self::ConnectionFailed(f)
            | Self::QueryFailed(f)
            | Self::AuthFailed(f)
            | Self::ConstraintViolation(f)
            | Self::SyntaxError(f)
            | Self::PermissionDenied(f)
            | Self::ObjectNotFound(f) => Some(f),
            Self::Timeout
            | Self::Cancelled
            | Self::NotSupported(_)
            | Self::InvalidProfile(_)
            | Self::IoError(_) => None,
        }
    }

    /// Whether the error is retriable (e.g., transient network issues).
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::ConnectionFailed(f)
            | Self::QueryFailed(f)
            | Self::AuthFailed(f)
            | Self::ConstraintViolation(f)
            | Self::SyntaxError(f)
            | Self::PermissionDenied(f)
            | Self::ObjectNotFound(f) => f.retriable,
            Self::Timeout => true,
            _ => false,
        }
    }
}
