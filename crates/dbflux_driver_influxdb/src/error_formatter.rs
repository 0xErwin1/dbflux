//! InfluxDB-specific error formatter.
//!
//! Parses structured error JSON from InfluxDB HTTP responses and formats them
//! into human-readable `FormattedError` values without exposing raw Debug
//! output such as `Error {`, `reqwest::`, etc.

use dbflux_core::{ConnectionErrorFormatter, FormattedError, QueryErrorFormatter};

const BODY_EXCERPT_LIMIT: usize = 4 * 1024; // 4 KiB

/// Formats InfluxDB errors from HTTP status codes and response bodies.
pub struct InfluxErrorFormatter;

impl InfluxErrorFormatter {
    /// Produce a `FormattedError` from an HTTP status code and response body.
    ///
    /// Attempts to parse the body as `{"error": "..."}`. Falls back to plain
    /// text and then to the HTTP status description when JSON parsing fails.
    pub fn format_http_error(status: u16, body: &str) -> FormattedError {
        let server_message = extract_error_field(body);

        match status {
            401 | 403 => {
                let msg = server_message
                    .map(|m| format!("authentication failed: {m}"))
                    .unwrap_or_else(|| "authentication failed".to_string());
                FormattedError::new(msg)
            }

            404 => {
                let msg = server_message
                    .map(|m| format!("not found: {m}"))
                    .unwrap_or_else(|| "not found".to_string());
                FormattedError::new(msg)
            }

            400 => {
                let msg = server_message
                    .map(|m| format!("bad request: {m}"))
                    .unwrap_or_else(|| "bad request".to_string());
                FormattedError::new(msg)
            }

            422 => {
                let msg = server_message
                    .map(|m| format!("unprocessable query: {m}"))
                    .unwrap_or_else(|| "unprocessable query".to_string());
                FormattedError::new(msg)
            }

            500..=599 => {
                let excerpt = body_excerpt(body);
                let base_msg = server_message.unwrap_or_else(|| format!("server error {status}"));
                FormattedError::new(base_msg).with_detail(excerpt)
            }

            other => {
                let msg = server_message.unwrap_or_else(|| format!("HTTP {other}"));
                FormattedError::new(msg)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Extract the `"error"` field from an InfluxDB JSON error body.
///
/// Returns `None` when the body is not JSON or lacks an `"error"` key.
fn extract_error_field(body: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(body.trim()).ok()?;
    parsed
        .get("error")
        .or_else(|| parsed.get("message"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Truncate the body to at most `BODY_EXCERPT_LIMIT` bytes for display.
fn body_excerpt(body: &str) -> String {
    if body.len() <= BODY_EXCERPT_LIMIT {
        body.to_string()
    } else {
        format!("{}…", &body[..BODY_EXCERPT_LIMIT])
    }
}

// ---------------------------------------------------------------------------
// Trait implementations (no-op stubs — InfluxDB errors are HTTP-level)
// ---------------------------------------------------------------------------

impl QueryErrorFormatter for InfluxErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        // For query-level errors (e.g., from the parser), use Display which
        // is already a clean, non-Debug string.
        FormattedError::new(error.to_string())
    }
}

impl ConnectionErrorFormatter for InfluxErrorFormatter {
    fn format_connection_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        host: &str,
        port: u16,
    ) -> FormattedError {
        // Sanitize reqwest errors: use Display, not Debug.
        let msg = error.to_string();
        FormattedError::new(format!("failed to connect to {host}:{port}: {msg}"))
    }

    fn format_uri_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        sanitized_uri: &str,
    ) -> FormattedError {
        let msg = error.to_string();
        FormattedError::new(format!("failed to connect to {sanitized_uri}: {msg}"))
    }
}

// ---------------------------------------------------------------------------
// Tests (C.5.1 – C.5.6)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // C.5.1
    #[test]
    fn status_401_with_json_error_reports_authentication_failed() {
        let fe = InfluxErrorFormatter::format_http_error(401, r#"{"error":"unauthorized"}"#);
        assert!(
            fe.message.contains("authentication failed"),
            "message: {}",
            fe.message
        );
        assert!(
            fe.message.contains("unauthorized"),
            "message: {}",
            fe.message
        );
    }

    // C.5.2
    #[test]
    fn status_404_body_produces_not_found_message() {
        let fe = InfluxErrorFormatter::format_http_error(404, r#"{"error":"database not found"}"#);
        assert!(fe.message.contains("not found"), "message: {}", fe.message);
    }

    // C.5.3
    #[test]
    fn status_400_with_json_error_reports_bad_request() {
        let fe =
            InfluxErrorFormatter::format_http_error(400, r#"{"error":"invalid query syntax"}"#);
        assert!(
            fe.message.contains("bad request"),
            "message: {}",
            fe.message
        );
        assert!(
            fe.message.contains("invalid query syntax"),
            "message: {}",
            fe.message
        );
    }

    // C.5.4
    #[test]
    fn status_5xx_populates_detail_excerpt() {
        let body = "a".repeat(100);
        let fe = InfluxErrorFormatter::format_http_error(500, &body);
        assert!(fe.detail.is_some(), "detail must be populated for 5xx");
    }

    #[test]
    fn status_5xx_large_body_truncated_to_4kib() {
        let body = "x".repeat(10_000);
        let fe = InfluxErrorFormatter::format_http_error(503, &body);
        let detail = fe.detail.expect("detail populated");
        assert!(
            detail.len() <= BODY_EXCERPT_LIMIT + 10,
            "excerpt must not exceed 4 KiB + ellipsis"
        );
    }

    // C.5.5
    #[test]
    fn malformed_json_body_falls_back_to_http_status_text() {
        let fe = InfluxErrorFormatter::format_http_error(400, "this is not json");
        assert!(
            !fe.message.is_empty(),
            "must produce some message even with non-JSON body"
        );
    }

    // C.5.6
    #[test]
    fn formatted_error_does_not_contain_raw_debug_patterns() {
        let fe = InfluxErrorFormatter::format_http_error(401, r#"{"error":"bad token"}"#);
        assert!(!fe.message.contains("Error {"), "must not contain Error {{");
        assert!(
            !fe.message.contains("reqwest::"),
            "must not contain reqwest::"
        );
        assert!(
            !fe.message.contains("status:"),
            "must not contain raw field names"
        );
    }
}
