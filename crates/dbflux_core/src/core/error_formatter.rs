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
/// Returns a safe-to-display version of the URI with the userinfo password and
/// credential-bearing parameters replaced by `***`. See
/// [`redact_uri_credentials`] for the exact rules.
pub fn sanitize_uri(uri: &str) -> String {
    redact_uri_credentials(uri, "***").0
}

/// Parameter names whose values are credentials when they appear in the query
/// string or in `;`-separated parameters of a URL. Compared case-insensitively.
const SENSITIVE_URI_PARAMETERS: &[&str] = &[
    "password",
    "passwd",
    "pwd",
    "pass",
    "token",
    "access_token",
    "auth_token",
    "authtoken",
    "refresh_token",
    "id_token",
    "secret",
    "client_secret",
    "secret_key",
    "api_key",
    "apikey",
    "sessiontoken",
    "session_token",
    "x-amz-security-token",
    "x-amz-signature",
];

/// Redacts credentials from every URL embedded in `text`, whatever its scheme.
///
/// For each `scheme://` occurrence it replaces:
/// - the password of the userinfo (`scheme://user:password@host`), keeping the
///   user name and host so the message stays useful. The last `@` of the
///   authority ends the userinfo, so an unencoded `@` inside the password is
///   still covered, and bracketed IPv6 hosts are left intact;
/// - the value of credential-bearing parameters (`password=`, `pwd=`,
///   `token=`, `secret=`, ...) in the query string or in JDBC-style
///   `;key=value` segments.
///
/// A URL ends at whitespace, a quote or an angle bracket, so the function can
/// run over free text such as error messages and log lines. A userinfo without
/// a password (`scheme://user@host`) is left unchanged.
///
/// Returns the redacted text and the number of values replaced.
pub fn redact_uri_credentials(text: &str, replacement: &str) -> (String, usize) {
    let mut output = String::with_capacity(text.len());
    let mut redaction_count = 0;
    let mut cursor = 0;

    while let Some(offset) = text[cursor..].find("://") {
        let separator_start = cursor + offset;

        if !has_scheme_before(&text[..separator_start]) {
            output.push_str(&text[cursor..separator_start + 3]);
            cursor = separator_start + 3;
            continue;
        }

        let url_start = separator_start + 3;
        let url_end = text[url_start..]
            .find(is_url_terminator)
            .map_or(text.len(), |length| url_start + length);

        output.push_str(&text[cursor..url_start]);

        let (redacted_url, url_redactions) =
            redact_url_remainder(&text[url_start..url_end], replacement);
        output.push_str(&redacted_url);
        redaction_count += url_redactions;

        cursor = url_end;
    }

    output.push_str(&text[cursor..]);

    (output, redaction_count)
}

fn has_scheme_before(prefix: &str) -> bool {
    let scheme: String = prefix
        .chars()
        .rev()
        .take_while(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.')
        })
        .collect();

    scheme
        .chars()
        .last()
        .is_some_and(|first| first.is_ascii_alphabetic())
}

fn is_url_terminator(character: char) -> bool {
    character.is_whitespace() || matches!(character, '"' | '\'' | '`' | '<' | '>')
}

/// Redacts one URL given everything after its `scheme://`.
fn redact_url_remainder(remainder: &str, replacement: &str) -> (String, usize) {
    let authority_end = remainder
        .find(['/', '?', '#', ';'])
        .unwrap_or(remainder.len());
    let authority = &remainder[..authority_end];
    let tail = &remainder[authority_end..];

    let mut redaction_count = 0;
    let mut output = String::with_capacity(remainder.len());

    match authority.rsplit_once('@') {
        Some((userinfo, host)) => match userinfo.split_once(':') {
            Some((user, password)) if !password.is_empty() => {
                output.push_str(user);
                output.push(':');
                output.push_str(replacement);
                output.push('@');
                output.push_str(host);
                redaction_count += 1;
            }
            _ => output.push_str(authority),
        },
        None => output.push_str(authority),
    }

    let (redacted_tail, tail_redactions) = redact_sensitive_parameters(tail, replacement);
    output.push_str(&redacted_tail);
    redaction_count += tail_redactions;

    (output, redaction_count)
}

/// Replaces the values of sensitive `key=value` parameters that follow a `?`,
/// `&` or `;`, stopping each value at the next `&`, `;` or `#`.
fn redact_sensitive_parameters(tail: &str, replacement: &str) -> (String, usize) {
    let mut output = String::with_capacity(tail.len());
    let mut redaction_count = 0;
    let mut cursor = 0;

    while let Some(offset) = tail[cursor..].find(['?', '&', ';']) {
        let key_start = cursor + offset + 1;
        output.push_str(&tail[cursor..key_start]);
        cursor = key_start;

        let segment_end = tail[key_start..]
            .find(['&', ';', '#'])
            .map_or(tail.len(), |length| key_start + length);
        let segment = &tail[key_start..segment_end];

        let Some((key, value)) = segment.split_once('=') else {
            continue;
        };

        let is_sensitive = SENSITIVE_URI_PARAMETERS
            .iter()
            .any(|name| key.eq_ignore_ascii_case(name));

        if is_sensitive && !value.is_empty() {
            output.push_str(key);
            output.push('=');
            output.push_str(replacement);
            redaction_count += 1;
            cursor = segment_end;
        }
    }

    output.push_str(&tail[cursor..]);

    (output, redaction_count)
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

    fn redact(text: &str) -> String {
        redact_uri_credentials(text, "***").0
    }

    #[test]
    fn redacts_userinfo_password_for_every_scheme() {
        let cases = [
            ("postgres://u:pw@h:5432/db", "postgres://u:***@h:5432/db"),
            (
                "postgresql://u:pw@h:5432/db",
                "postgresql://u:***@h:5432/db",
            ),
            ("mysql://root:pw@h:3306/app", "mysql://root:***@h:3306/app"),
            ("mariadb://root:pw@h/app", "mariadb://root:***@h/app"),
            (
                "mongodb://u:pw@h1:27017,h2:27017/db",
                "mongodb://u:***@h1:27017,h2:27017/db",
            ),
            (
                "mongodb+srv://u:pw@cluster.example.net/db",
                "mongodb+srv://u:***@cluster.example.net/db",
            ),
            ("redis://:pw@h:6379/0", "redis://:***@h:6379/0"),
            ("rediss://default:pw@h:6380", "rediss://default:***@h:6380"),
            (
                "sqlserver://sa:pw@h:1433/master",
                "sqlserver://sa:***@h:1433/master",
            ),
            ("clickhouse://u:pw@h:8123", "clickhouse://u:***@h:8123"),
            ("http://u:pw@h:8086/api", "http://u:***@h:8086/api"),
            ("https://u:pw@h/path", "https://u:***@h/path"),
        ];

        for (input, expected) in cases {
            assert_eq!(redact(input), expected, "input: {input}");
        }
    }

    #[test]
    fn leaves_uris_without_password_unchanged() {
        for input in [
            "postgres://u@h:5432/db",
            "postgresql://h/db",
            "mongodb://h:27017/db?replicaSet=rs0",
            "redis://[::1]:6379",
            "https://example.com/a@b",
        ] {
            let (output, count) = redact_uri_credentials(input, "***");
            assert_eq!(output, input);
            assert_eq!(count, 0, "input: {input}");
        }
    }

    #[test]
    fn redacts_password_with_encoded_or_raw_at_sign() {
        assert_eq!(
            redact("postgresql://alice:p%40ss@localhost/app"),
            "postgresql://alice:***@localhost/app"
        );
        assert_eq!(
            redact("postgresql://alice:p@ss:w0rd@localhost/app"),
            "postgresql://alice:***@localhost/app"
        );
    }

    #[test]
    fn redacts_password_before_ipv6_host() {
        assert_eq!(
            redact("postgresql://u:pw@[2001:db8::1]:5432/db"),
            "postgresql://u:***@[2001:db8::1]:5432/db"
        );
        assert_eq!(redact("redis://:pw@[::1]:6379"), "redis://:***@[::1]:6379");
    }

    #[test]
    fn redacts_sensitive_query_and_jdbc_parameters() {
        assert_eq!(
            redact("postgresql://h/db?user=u&password=pw&sslmode=require"),
            "postgresql://h/db?user=u&password=***&sslmode=require"
        );
        assert_eq!(
            redact("https://h/api?Token=abc#frag"),
            "https://h/api?Token=***#frag"
        );
        assert_eq!(
            redact("mysql://h/db?PWD=x&secret=y"),
            "mysql://h/db?PWD=***&secret=***"
        );
        assert_eq!(
            redact("jdbc:sqlserver://h:1433;user=sa;password=pw;databaseName=app"),
            "jdbc:sqlserver://h:1433;user=sa;password=***;databaseName=app"
        );
    }

    #[test]
    fn redacts_urls_inside_free_text() {
        let (output, count) = redact_uri_credentials(
            "Failed to connect using URI postgresql://u:pw@h:5432: refused; retry mysql://r:x@h2/db",
            "[REDACTED]",
        );

        assert_eq!(
            output,
            "Failed to connect using URI postgresql://u:[REDACTED]@h:5432: refused; retry mysql://r:[REDACTED]@h2/db"
        );
        assert_eq!(count, 2);
    }

    #[test]
    fn stops_at_quotes_and_ignores_schemeless_separators() {
        assert_eq!(
            redact(r#"{"uri":"redis://u:pw@h:6379","note":"a@b"}"#),
            r#"{"uri":"redis://u:***@h:6379","note":"a@b"}"#
        );
        assert_eq!(redact("://u:pw@h"), "://u:pw@h");
    }

    #[test]
    fn test_error_location_is_empty() {
        assert!(ErrorLocation::new().is_empty());
        assert!(!ErrorLocation::new().with_table("users").is_empty());
    }
}
