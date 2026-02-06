use crate::DbError;
use std::fmt;

/// Formatted error with structured information for display.
#[derive(Debug, Clone, Default)]
pub struct FormattedError {
    /// Primary error message.
    pub message: String,

    /// Additional detail about the error (e.g., PostgreSQL's DETAIL field).
    pub detail: Option<String>,

    /// Suggestion for how to fix the error (e.g., PostgreSQL's HINT field).
    pub hint: Option<String>,

    /// Error code from the database (e.g., SQLSTATE, MySQL error code).
    pub code: Option<String>,

    /// Location information if available.
    pub location: Option<ErrorLocation>,

    /// Whether the error is retriable (e.g., transient network issues).
    pub retriable: bool,
}

impl FormattedError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ..Default::default()
        }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    pub fn with_location(mut self, location: ErrorLocation) -> Self {
        self.location = Some(location);
        self
    }

    pub fn with_retriable(mut self, retriable: bool) -> Self {
        self.retriable = retriable;
        self
    }

    /// Convert to a single-line display string.
    pub fn to_display_string(&self) -> String {
        let mut parts = vec![self.message.clone()];

        if let Some(ref detail) = self.detail {
            parts.push(format!("Detail: {}", detail));
        }

        if let Some(ref hint) = self.hint {
            parts.push(format!("Hint: {}", hint));
        }

        if let Some(ref loc) = self.location {
            if let Some(ref table) = loc.table {
                parts.push(format!("Table: {}", table));
            }
            if let Some(ref column) = loc.column {
                parts.push(format!("Column: {}", column));
            }
            if let Some(ref constraint) = loc.constraint {
                parts.push(format!("Constraint: {}", constraint));
            }
        }

        if let Some(ref code) = self.code {
            parts.push(format!("Code: {}", code));
        }

        parts.join(". ")
    }

    /// Classify into the appropriate `DbError` variant for query errors.
    ///
    /// Uses SQLSTATE codes (when present) to route to semantic variants:
    /// - `42xxx` -> `SyntaxError`
    /// - `23xxx` -> `ConstraintViolation`
    /// - `28xxx` -> `AuthFailed`
    /// - `42501` / `42000` (with permission context) -> `PermissionDenied`
    /// - `42P01` / `1146` -> `ObjectNotFound`
    /// - Everything else -> `QueryFailed`
    pub fn into_query_error(self) -> DbError {
        if let Some(variant) = self.code.as_deref().and_then(classify_query_sqlstate) {
            return match variant {
                ErrorClass::Syntax => DbError::SyntaxError(self),
                ErrorClass::Constraint => DbError::ConstraintViolation(self),
                ErrorClass::Auth => DbError::AuthFailed(self),
                ErrorClass::Permission => DbError::PermissionDenied(self),
                ErrorClass::NotFound => DbError::ObjectNotFound(self),
            };
        }

        DbError::QueryFailed(self)
    }

    /// Classify into the appropriate `DbError` variant for connection errors.
    ///
    /// Uses SQLSTATE codes (when present) to route to semantic variants:
    /// - `28xxx` -> `AuthFailed`
    /// - Everything else -> `ConnectionFailed`
    pub fn into_connection_error(self) -> DbError {
        if self.code.as_deref().is_some_and(|c| c.starts_with("28")) {
            return DbError::AuthFailed(self);
        }

        DbError::ConnectionFailed(self)
    }
}

impl fmt::Display for FormattedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_display_string())
    }
}

impl From<String> for FormattedError {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for FormattedError {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

enum ErrorClass {
    Syntax,
    Constraint,
    Auth,
    Permission,
    NotFound,
}

/// Classify a SQLSTATE or MySQL error code into a semantic error class.
fn classify_query_sqlstate(code: &str) -> Option<ErrorClass> {
    // Exact matches first (more specific)
    match code {
        // PostgreSQL: insufficient_privilege
        "42501" => return Some(ErrorClass::Permission),
        // PostgreSQL: undefined_table
        "42P01" => return Some(ErrorClass::NotFound),
        // PostgreSQL: undefined_column
        "42703" => return Some(ErrorClass::NotFound),
        // PostgreSQL: undefined_function
        "42883" => return Some(ErrorClass::NotFound),
        // MySQL: Table doesn't exist
        "1146" => return Some(ErrorClass::NotFound),
        // MySQL: Unknown column
        "1054" => return Some(ErrorClass::NotFound),
        // MySQL: Access denied
        "1044" | "1045" => return Some(ErrorClass::Auth),
        _ => {}
    }

    // Class-level matching (first 2 characters of SQLSTATE)
    if code.len() >= 2 {
        match &code[..2] {
            // Class 23: Integrity constraint violation
            "23" => return Some(ErrorClass::Constraint),
            // Class 28: Invalid authorization specification
            "28" => return Some(ErrorClass::Auth),
            // Class 42: Syntax error or access rule violation
            "42" => return Some(ErrorClass::Syntax),
            _ => {}
        }
    }

    None
}

/// Location information for database errors.
#[derive(Debug, Clone, Default)]
pub struct ErrorLocation {
    /// Schema where the error occurred.
    pub schema: Option<String>,

    /// Table where the error occurred.
    pub table: Option<String>,

    /// Column where the error occurred.
    pub column: Option<String>,

    /// Constraint that was violated.
    pub constraint: Option<String>,
}

impl ErrorLocation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    pub fn with_column(mut self, column: impl Into<String>) -> Self {
        self.column = Some(column.into());
        self
    }

    pub fn with_constraint(mut self, constraint: impl Into<String>) -> Self {
        self.constraint = Some(constraint.into());
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn is_empty(&self) -> bool {
        self.schema.is_none()
            && self.table.is_none()
            && self.column.is_none()
            && self.constraint.is_none()
    }
}

/// Trait for formatting database-specific errors into a structured format.
///
/// Each driver implements this to extract detailed error information
/// from their specific error types.
pub trait QueryErrorFormatter: Send + Sync {
    /// Format a query execution error.
    ///
    /// This is called when a SQL query or database operation fails.
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError;
}

/// Trait for formatting connection errors.
///
/// Separated from QueryErrorFormatter because connection errors often need
/// additional context (host, port) that query errors don't have.
pub trait ConnectionErrorFormatter: Send + Sync {
    /// Format a connection error with host/port context.
    fn format_connection_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        host: &str,
        port: u16,
    ) -> FormattedError;

    /// Format a URI-based connection error.
    ///
    /// The URI should be sanitized (password removed) before display.
    fn format_uri_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        sanitized_uri: &str,
    ) -> FormattedError;
}

/// Default implementation that just uses Display.
pub struct DefaultErrorFormatter;

impl QueryErrorFormatter for DefaultErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        FormattedError::new(error.to_string())
    }
}

impl ConnectionErrorFormatter for DefaultErrorFormatter {
    fn format_connection_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        host: &str,
        port: u16,
    ) -> FormattedError {
        FormattedError::new(format!("Failed to connect to {}:{}: {}", host, port, error))
    }

    fn format_uri_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        sanitized_uri: &str,
    ) -> FormattedError {
        FormattedError::new(format!(
            "Failed to connect using URI {}: {}",
            sanitized_uri, error
        ))
    }
}

/// Sanitize a connection URI by removing credentials.
///
/// Returns a safe-to-display version of the URI with password replaced by `***`.
pub fn sanitize_uri(uri: &str) -> String {
    if uri.contains('@') {
        let parts: Vec<&str> = uri.splitn(2, '@').collect();
        if parts.len() == 2 {
            // Find the scheme://user: part
            if let Some(colon_pos) = parts[0].rfind(':') {
                let prefix = &parts[0][..=colon_pos];
                format!("{}***@{}", prefix, parts[1])
            } else {
                format!("***@{}", parts[1])
            }
        } else {
            "***".to_string()
        }
    } else {
        uri.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_formatted_error_display() {
        let err = FormattedError::new("syntax error")
            .with_detail("near 'FROM'")
            .with_code("42601");

        assert_eq!(
            err.to_display_string(),
            "syntax error. Detail: near 'FROM'. Code: 42601"
        );
    }

    #[test]
    fn test_formatted_error_display_trait() {
        let err = FormattedError::new("test error");
        assert_eq!(format!("{}", err), "test error");
    }

    #[test]
    fn test_formatted_error_from_string() {
        let err: FormattedError = "hello".into();
        assert_eq!(err.message, "hello");

        let err: FormattedError = String::from("world").into();
        assert_eq!(err.message, "world");
    }

    #[test]
    fn test_formatted_error_with_location() {
        let err = FormattedError::new("duplicate key")
            .with_location(
                ErrorLocation::new()
                    .with_table("users")
                    .with_constraint("users_pkey"),
            )
            .with_code("23505");

        assert_eq!(
            err.to_display_string(),
            "duplicate key. Table: users. Constraint: users_pkey. Code: 23505"
        );
    }

    #[test]
    fn test_formatted_error_retriable() {
        let err = FormattedError::new("timeout").with_retriable(true);
        assert!(err.retriable);

        let err = FormattedError::new("syntax error");
        assert!(!err.retriable);
    }

    #[test]
    fn test_classify_constraint_violation() {
        let err = FormattedError::new("duplicate key").with_code("23505");
        match err.into_query_error() {
            DbError::ConstraintViolation(f) => assert_eq!(f.message, "duplicate key"),
            other => panic!("Expected ConstraintViolation, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_syntax_error() {
        let err = FormattedError::new("syntax error").with_code("42601");
        match err.into_query_error() {
            DbError::SyntaxError(f) => assert_eq!(f.message, "syntax error"),
            other => panic!("Expected SyntaxError, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_permission_denied() {
        let err = FormattedError::new("insufficient privilege").with_code("42501");
        match err.into_query_error() {
            DbError::PermissionDenied(f) => assert_eq!(f.message, "insufficient privilege"),
            other => panic!("Expected PermissionDenied, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_object_not_found() {
        let err = FormattedError::new("table not found").with_code("42P01");
        match err.into_query_error() {
            DbError::ObjectNotFound(f) => assert_eq!(f.message, "table not found"),
            other => panic!("Expected ObjectNotFound, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_auth_failed() {
        let err = FormattedError::new("invalid password").with_code("28P01");
        match err.into_query_error() {
            DbError::AuthFailed(f) => assert_eq!(f.message, "invalid password"),
            other => panic!("Expected AuthFailed, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_auth_connection() {
        let err = FormattedError::new("invalid password").with_code("28P01");
        match err.into_connection_error() {
            DbError::AuthFailed(f) => assert_eq!(f.message, "invalid password"),
            other => panic!("Expected AuthFailed, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_no_code_defaults_to_query_failed() {
        let err = FormattedError::new("some error");
        match err.into_query_error() {
            DbError::QueryFailed(f) => assert_eq!(f.message, "some error"),
            other => panic!("Expected QueryFailed, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_mysql_not_found() {
        let err = FormattedError::new("table not found").with_code("1146");
        match err.into_query_error() {
            DbError::ObjectNotFound(f) => assert_eq!(f.message, "table not found"),
            other => panic!("Expected ObjectNotFound, got {:?}", other),
        }
    }

    #[test]
    fn test_sanitize_uri_with_password() {
        let uri = "postgres://user:secret@localhost:5432/db";
        assert_eq!(sanitize_uri(uri), "postgres://user:***@localhost:5432/db");
    }

    #[test]
    fn test_sanitize_uri_without_password() {
        let uri = "postgres://localhost:5432/db";
        assert_eq!(sanitize_uri(uri), "postgres://localhost:5432/db");
    }

    #[test]
    fn test_error_location_is_empty() {
        assert!(ErrorLocation::new().is_empty());
        assert!(!ErrorLocation::new().with_table("users").is_empty());
    }
}
