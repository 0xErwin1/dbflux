use crate::DbError;

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

    /// Convert to DbError::QueryFailed.
    pub fn into_query_error(self) -> DbError {
        DbError::QueryFailed(self.to_display_string())
    }

    /// Convert to DbError::ConnectionFailed.
    pub fn into_connection_error(self) -> DbError {
        DbError::ConnectionFailed(self.to_display_string())
    }
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
