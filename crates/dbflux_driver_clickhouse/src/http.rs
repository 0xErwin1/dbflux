use std::io::Read;
use std::time::Duration;

use dbflux_core::secrecy::{ExposeSecret, SecretString};
use reqwest::blocking::{Client, Response};
use reqwest::header::{CONTENT_LENGTH, HeaderMap};
use reqwest::{Url, redirect::Policy};
use thiserror::Error;

const MAX_RESPONSE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_ERROR_BYTES: u64 = 64 * 1024;

#[derive(Debug, Error)]
pub(crate) enum ClickHouseHttpError {
    #[error("invalid ClickHouse endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("ClickHouse HTTP request failed: {0}")]
    Transport(String),
    #[error("ClickHouse returned HTTP {status}: {body}")]
    Server {
        status: u16,
        code: Option<String>,
        body: String,
    },
    #[error("ClickHouse response exceeds the {limit} byte safety limit")]
    ResponseTooLarge { limit: u64 },
    #[error("failed to read ClickHouse response: {0}")]
    Body(String),
}

pub(crate) struct HttpResponse {
    pub body: Vec<u8>,
    pub headers: HeaderMap,
}

pub(crate) struct ClickHouseHttpClient {
    client: Client,
    endpoint: Url,
    user: String,
    password: Option<SecretString>,
    default_database: String,
    default_timeout: Duration,
}

impl ClickHouseHttpClient {
    pub(crate) fn validate_endpoint(endpoint: &str) -> Result<(), ClickHouseHttpError> {
        parse_endpoint(endpoint).map(|_| ())
    }

    pub(crate) fn new(
        endpoint: &str,
        user: String,
        password: Option<SecretString>,
        default_database: String,
        timeout: Duration,
    ) -> Result<Self, ClickHouseHttpError> {
        let mut endpoint = parse_endpoint(endpoint)?;
        let has_password = password
            .as_ref()
            .is_some_and(|password| !password.expose_secret().is_empty());
        if endpoint.scheme() == "http" && has_password && !is_loopback_endpoint(&endpoint) {
            return Err(ClickHouseHttpError::InvalidEndpoint(
                "non-empty passwords require HTTPS for non-loopback endpoints".to_string(),
            ));
        }
        endpoint.set_query(None);

        let client = Client::builder()
            .timeout(timeout)
            .gzip(true)
            .tcp_nodelay(true)
            .use_rustls_tls()
            .redirect(Policy::none())
            .build()
            .map_err(|error| ClickHouseHttpError::Transport(error.to_string()))?;

        Ok(Self {
            client,
            endpoint,
            user,
            password,
            default_database,
            default_timeout: timeout,
        })
    }

    pub(crate) fn execute(
        &self,
        sql: &str,
        database: Option<&str>,
        timeout: Option<Duration>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<HttpResponse, ClickHouseHttpError> {
        let effective_timeout = timeout.unwrap_or(self.default_timeout);
        let max_execution_time = effective_timeout
            .as_secs()
            .saturating_add(u64::from(effective_timeout.subsec_nanos() != 0))
            .max(1)
            .to_string();
        let max_result_bytes = MAX_RESPONSE_BYTES.to_string();
        let database = database.unwrap_or(&self.default_database);
        let mut request = self
            .client
            .post(self.endpoint.clone())
            .basic_auth(
                &self.user,
                self.password.as_ref().map(ExposeSecret::expose_secret),
            )
            .header("X-ClickHouse-Format", "JSONCompact")
            .query(&[
                ("database", database),
                ("wait_end_of_query", "1"),
                ("date_time_output_format", "iso"),
                ("output_format_json_quote_64bit_integers", "1"),
                ("output_format_json_quote_64bit_floats", "1"),
                ("output_format_json_quote_decimals", "1"),
                ("output_format_json_quote_denormals", "1"),
                ("output_format_json_named_tuples_as_objects", "0"),
                ("output_format_json_validate_utf8", "1"),
                ("result_overflow_mode", "throw"),
                ("max_result_bytes", max_result_bytes.as_str()),
                ("max_execution_time", max_execution_time.as_str()),
            ])
            .body(sql.to_string());

        if let Some(limit) = limit {
            request = request.query(&[("limit", limit)]);
        }
        if let Some(offset) = offset {
            request = request.query(&[("offset", offset)]);
        }

        request = request.timeout(effective_timeout);

        let response = request
            .send()
            .map_err(|error| ClickHouseHttpError::Transport(error.to_string()))?;
        self.read_response(response)
    }

    fn read_response(&self, response: Response) -> Result<HttpResponse, ClickHouseHttpError> {
        let status = response.status();
        let headers = response.headers().clone();
        let limit = if status.is_success() {
            MAX_RESPONSE_BYTES
        } else {
            MAX_ERROR_BYTES
        };

        if content_length(&headers).is_some_and(|length| length > limit) {
            return Err(ClickHouseHttpError::ResponseTooLarge { limit });
        }

        let mut body = Vec::new();
        response
            .take(limit + 1)
            .read_to_end(&mut body)
            .map_err(|error| ClickHouseHttpError::Body(error.to_string()))?;
        if body.len() as u64 > limit {
            return Err(ClickHouseHttpError::ResponseTooLarge { limit });
        }

        if !status.is_success() {
            return Err(ClickHouseHttpError::Server {
                status: status.as_u16(),
                code: exception_code(&headers),
                body: error_excerpt(&body, 0),
            });
        }

        if let Some((code, offset)) = success_exception(&headers, &body) {
            return Err(ClickHouseHttpError::Server {
                status: status.as_u16(),
                code,
                body: error_excerpt(&body, offset),
            });
        }

        Ok(HttpResponse { body, headers })
    }
}

fn parse_endpoint(endpoint: &str) -> Result<Url, ClickHouseHttpError> {
    let endpoint = Url::parse(endpoint)
        .map_err(|_| ClickHouseHttpError::InvalidEndpoint("URL is not valid".to_string()))?;
    if !matches!(endpoint.scheme(), "http" | "https") {
        return Err(ClickHouseHttpError::InvalidEndpoint(
            "URL scheme must be http or https".to_string(),
        ));
    }
    if endpoint.host_str().is_none() {
        return Err(ClickHouseHttpError::InvalidEndpoint(
            "URL must include a host".to_string(),
        ));
    }
    if !endpoint.username().is_empty() || endpoint.password().is_some() {
        return Err(ClickHouseHttpError::InvalidEndpoint(
            "credentials must be supplied through the user and password fields".to_string(),
        ));
    }
    if endpoint.query().is_some() || endpoint.fragment().is_some() {
        return Err(ClickHouseHttpError::InvalidEndpoint(
            "URL query parameters and fragments are not supported".to_string(),
        ));
    }
    Ok(endpoint)
}

fn is_loopback_endpoint(endpoint: &Url) -> bool {
    let Some(host) = endpoint.host_str() else {
        return false;
    };
    host.trim_matches(['[', ']'])
        .parse::<std::net::IpAddr>()
        .is_ok_and(|address| address.is_loopback())
        || host.eq_ignore_ascii_case("localhost")
        || host.ends_with(".localhost")
}

fn exception_code(headers: &HeaderMap) -> Option<String> {
    headers
        .get("X-ClickHouse-Exception-Code")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn success_exception(headers: &HeaderMap, body: &[u8]) -> Option<(Option<String>, usize)> {
    if let Some(code) = exception_code(headers) {
        return Some((Some(code), 0));
    }
    [
        b"\r\n__exception__\r\n".as_slice(),
        b"\n__exception__\n".as_slice(),
    ]
    .into_iter()
    .find_map(|marker| {
        body.windows(marker.len())
            .position(|window| window == marker)
            .map(|offset| (None, offset))
    })
}

fn error_excerpt(body: &[u8], offset: usize) -> String {
    let end = offset
        .saturating_add(MAX_ERROR_BYTES as usize)
        .min(body.len());
    String::from_utf8_lossy(body.get(offset..end).unwrap_or_default())
        .trim()
        .to_string()
}

fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::{ClickHouseHttpClient, ClickHouseHttpError, success_exception};
    use dbflux_core::secrecy::SecretString;
    use reqwest::header::{HeaderMap, HeaderValue};
    use std::time::Duration;

    fn client(url: &str) -> Result<ClickHouseHttpClient, ClickHouseHttpError> {
        ClickHouseHttpClient::new(
            url,
            "default".to_string(),
            Some(SecretString::from("secret".to_string())),
            "default".to_string(),
            Duration::from_secs(1),
        )
    }

    #[test]
    fn accepts_http_and_https_endpoints() {
        assert!(client("http://localhost:8123").is_ok());
        assert!(client("https://clickhouse.example.com").is_ok());
    }

    #[test]
    fn rejects_credentials_in_endpoint() {
        let result = client("https://default:secret@clickhouse.example.com");
        assert!(matches!(
            result,
            Err(ClickHouseHttpError::InvalidEndpoint(_))
        ));
        let Err(error) = result else {
            panic!("endpoint credentials must be rejected");
        };
        assert!(!error.to_string().contains("default:secret@"));
    }

    #[test]
    fn rejects_endpoint_query_parameters() {
        assert!(matches!(
            client("http://localhost:8123?password=secret"),
            Err(ClickHouseHttpError::InvalidEndpoint(_))
        ));
    }

    #[test]
    fn rejects_password_over_remote_http() {
        assert!(matches!(
            client("http://clickhouse.example.com"),
            Err(ClickHouseHttpError::InvalidEndpoint(_))
        ));
        assert!(client("http://127.0.0.1:8123").is_ok());
        assert!(client("http://[::1]:8123").is_ok());
    }

    #[test]
    fn accepts_empty_password_over_remote_http() {
        let result = ClickHouseHttpClient::new(
            "http://clickhouse.example.com",
            "default".to_string(),
            Some(SecretString::from(String::new())),
            "default".to_string(),
            Duration::from_secs(1),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn detects_exception_header_and_embedded_exception() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-ClickHouse-Exception-Code",
            HeaderValue::from_static("60"),
        );
        assert_eq!(
            success_exception(&headers, br#"{"data":[]}"#),
            Some((Some("60".to_string()), 0))
        );

        assert_eq!(
            success_exception(
                &HeaderMap::new(),
                b"{\"data\":[]}\n__exception__\nCode: 241. DB::Exception",
            ),
            Some((None, 11))
        );
        assert_eq!(
            success_exception(
                &HeaderMap::new(),
                b"{\"data\":[]}\r\n__exception__\r\nCode: 241. DB::Exception",
            ),
            Some((None, 11))
        );
        assert_eq!(
            success_exception(&HeaderMap::new(), br#"{"data":[["__exception__"]]}"#),
            None
        );
    }
}
