use dbflux_core::{ConnectionErrorFormatter, FormattedError, QueryErrorFormatter};

use crate::http::ClickHouseHttpError;

pub struct ClickHouseErrorFormatter;

impl ClickHouseErrorFormatter {
    pub(crate) fn format_http_error(error: &ClickHouseHttpError) -> FormattedError {
        match error {
            ClickHouseHttpError::Server { status, code, body } => {
                let (body_code, message) = parse_server_error(body);
                let mut formatted = FormattedError::new(message);
                if let Some(code) = code.clone().or(body_code) {
                    formatted = formatted.with_code(code);
                }
                if *status == 429 {
                    formatted = formatted.with_retriable(true);
                }
                formatted
            }
            ClickHouseHttpError::Transport(message) => {
                FormattedError::new(format!("ClickHouse request failed: {message}"))
                    .with_retriable(true)
            }
            other => FormattedError::new(other.to_string()),
        }
    }

    pub(crate) fn into_connection_error(error: &ClickHouseHttpError) -> dbflux_core::DbError {
        let formatted = Self::format_http_error(error);
        match error {
            ClickHouseHttpError::Server { status, body, .. }
                if matches!(status, 401 | 403)
                    || body.contains("AUTHENTICATION_FAILED")
                    || body.contains("REQUIRED_PASSWORD") =>
            {
                dbflux_core::DbError::AuthFailed(formatted)
            }
            _ => formatted.into_connection_error(),
        }
    }
}

impl QueryErrorFormatter for ClickHouseErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        error
            .downcast_ref::<ClickHouseHttpError>()
            .map(Self::format_http_error)
            .unwrap_or_else(|| FormattedError::new(error.to_string()))
    }
}

impl ConnectionErrorFormatter for ClickHouseErrorFormatter {
    fn format_connection_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        host: &str,
        port: u16,
    ) -> FormattedError {
        let message = error.to_string();
        if message.contains("401") || message.contains("authentication") {
            FormattedError::new("ClickHouse authentication failed. Check the user and password.")
        } else {
            FormattedError::new(format!(
                "Could not connect to ClickHouse at {host}:{port}: {message}"
            ))
        }
    }

    fn format_uri_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        sanitized_uri: &str,
    ) -> FormattedError {
        let formatted = error
            .downcast_ref::<ClickHouseHttpError>()
            .map(Self::format_http_error)
            .unwrap_or_else(|| FormattedError::new(error.to_string()));
        FormattedError::new(format!(
            "ClickHouse connection to {sanitized_uri} failed: {}",
            formatted.message
        ))
        .with_retriable(formatted.retriable)
    }
}

fn parse_server_error(body: &str) -> (Option<String>, String) {
    let trimmed = body.trim().trim_start_matches("__exception__").trim();
    let code = trimmed
        .find("Code: ")
        .map(|position| &trimmed[position + "Code: ".len()..])
        .and_then(|rest| rest.split_once('.'))
        .map(|(code, _)| code.trim().to_string());
    let message = trimmed
        .split_once("DB::Exception:")
        .map(|(_, message)| message.trim())
        .unwrap_or(trimmed)
        .lines()
        .next()
        .unwrap_or("ClickHouse query failed")
        .trim()
        .to_string();
    (code, message)
}

#[cfg(test)]
mod tests {
    use super::{ClickHouseErrorFormatter, parse_server_error};
    use crate::http::ClickHouseHttpError;

    #[test]
    fn extracts_clickhouse_code_and_message() {
        let (code, message) = parse_server_error(
            "Code: 60. DB::Exception: Table default.missing does not exist. (UNKNOWN_TABLE)",
        );
        assert_eq!(code.as_deref(), Some("60"));
        assert_eq!(
            message,
            "Table default.missing does not exist. (UNKNOWN_TABLE)"
        );

        let (code, message) =
            parse_server_error("__exception__Code: 241. DB::Exception: memory limit exceeded");
        assert_eq!(code.as_deref(), Some("241"));
        assert_eq!(message, "memory limit exceeded");
    }

    #[test]
    fn does_not_mark_generic_server_failures_retriable() {
        let formatted = ClickHouseErrorFormatter::format_http_error(&ClickHouseHttpError::Server {
            status: 500,
            code: None,
            body: "temporarily unavailable".to_string(),
        });
        assert!(!formatted.retriable);

        let throttled = ClickHouseErrorFormatter::format_http_error(&ClickHouseHttpError::Server {
            status: 429,
            code: None,
            body: "too many requests".to_string(),
        });
        assert!(throttled.retriable);
    }

    #[test]
    fn authentication_failure_maps_to_auth_error() {
        let error = ClickHouseHttpError::Server {
            status: 403,
            code: Some("516".to_string()),
            body: "Code: 516. DB::Exception: Authentication failed. (AUTHENTICATION_FAILED)"
                .to_string(),
        };
        assert!(matches!(
            ClickHouseErrorFormatter::into_connection_error(&error),
            dbflux_core::DbError::AuthFailed(_)
        ));
    }
}
