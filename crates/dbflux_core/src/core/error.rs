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

    /// A value reference (env var, secret, parameter) could not be resolved.
    #[error("Value resolution failed: {0}")]
    ValueResolutionFailed(String),

    /// Filesystem or network I/O error.
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Failed to parse input data (JSON, CSV, etc.) — not a DB-level error.
    #[error("Parse error: {0}")]
    Parse(String),

    /// The requested operation or format is not supported by this driver or context.
    #[error("Unsupported: {0}")]
    Unsupported(String),
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

    pub fn value_resolution_failed(msg: impl Into<String>) -> Self {
        Self::ValueResolutionFailed(msg.into())
    }

    /// Wraps a failure that does not come with an [`std::io::Error`] — a
    /// third-party crate error, a status string — as [`DbError::IoError`].
    pub(crate) fn io_message(message: impl std::fmt::Display) -> Self {
        Self::IoError(std::io::Error::other(message.to_string()))
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
            | Self::ValueResolutionFailed(_)
            | Self::IoError(_)
            | Self::Parse(_)
            | Self::Unsupported(_) => None,
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

    /// Appends a note about the session's transaction to the error's hint.
    ///
    /// Drivers call this after a failed execution so the user learns what
    /// happened to the transaction the failure touched. Variants without a
    /// [`FormattedError`] carry no hint and are returned unchanged.
    pub fn with_transaction_note(mut self, note: TransactionStateNote) -> Self {
        let formatted = match &mut self {
            Self::ConnectionFailed(f)
            | Self::QueryFailed(f)
            | Self::AuthFailed(f)
            | Self::ConstraintViolation(f)
            | Self::SyntaxError(f)
            | Self::PermissionDenied(f)
            | Self::ObjectNotFound(f) => f,
            _ => return self,
        };

        formatted.hint = Some(match formatted.hint.take() {
            Some(hint) => format!("{hint} {}", note.message()),
            None => note.message().to_string(),
        });

        self
    }
}

/// What a failed execution left behind in the session's transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStateNote {
    /// The failed execution opened a transaction and the driver rolled it back.
    RolledBack,

    /// A transaction opened before the failed execution is still open.
    StillOpen,

    /// The session is inside a transaction the server has aborted.
    Aborted,
}

impl TransactionStateNote {
    pub fn message(self) -> &'static str {
        match self {
            Self::RolledBack => "The transaction this script opened was rolled back.",
            Self::StillOpen => {
                "The connection is still inside a transaction. Run COMMIT or ROLLBACK to end it."
            }
            Self::Aborted => {
                "The connection is inside an aborted transaction. Run ROLLBACK to end it."
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_note_becomes_the_hint() {
        let error =
            DbError::query_failed("boom").with_transaction_note(TransactionStateNote::RolledBack);

        assert_eq!(
            error.formatted().and_then(|f| f.hint.as_deref()),
            Some(TransactionStateNote::RolledBack.message())
        );
    }

    #[test]
    fn transaction_note_is_appended_to_an_existing_hint() {
        let error = DbError::QueryFailed(FormattedError::new("boom").with_hint("Check the key."))
            .with_transaction_note(TransactionStateNote::StillOpen);

        assert_eq!(
            error.formatted().and_then(|f| f.hint.as_deref()),
            Some(
                format!(
                    "Check the key. {}",
                    TransactionStateNote::StillOpen.message()
                )
                .as_str()
            )
        );
    }

    #[test]
    fn transaction_note_leaves_variants_without_formatted_error_unchanged() {
        let error = DbError::Timeout.with_transaction_note(TransactionStateNote::Aborted);
        assert!(matches!(error, DbError::Timeout));
    }
}
