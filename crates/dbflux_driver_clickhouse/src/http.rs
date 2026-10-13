use std::io::{BufRead, Read};
use std::time::Duration;

use dbflux_core::secrecy::{ExposeSecret, SecretString};
use reqwest::blocking::{Client, RequestBuilder, Response};
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

/// A row-limited response read incrementally: at most `row_limit` rows are
/// retained and `truncated` records whether the server had more to send.
#[derive(Debug, Default)]
pub(crate) struct LimitedResponse {
    pub names: Vec<String>,
    pub types: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub truncated: bool,
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
            .user_agent(dbflux_core::client_identity())
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
        self.execute_with_settings(sql, database, timeout, limit, offset, &[])
    }

    /// Runs `sql` with extra ClickHouse settings, sent as query parameters so
    /// they apply to this request only.
    pub(crate) fn execute_with_settings(
        &self,
        sql: &str,
        database: Option<&str>,
        timeout: Option<Duration>,
        limit: Option<u32>,
        offset: Option<u32>,
        settings: &[(&str, &str)],
    ) -> Result<HttpResponse, ClickHouseHttpError> {
        let max_result_bytes = MAX_RESPONSE_BYTES.to_string();
        let mut request = self.request(sql, database, timeout, "JSONCompact").query(&[
            ("wait_end_of_query", "1"),
            ("result_overflow_mode", "throw"),
            ("max_result_bytes", max_result_bytes.as_str()),
        ]);

        if let Some(limit) = limit {
            request = request.query(&[("limit", limit)]);
        }
        if let Some(offset) = offset {
            request = request.query(&[("offset", offset)]);
        }
        if !settings.is_empty() {
            request = request.query(settings);
        }

        let response = request
            .send()
            .map_err(|error| ClickHouseHttpError::Transport(error.to_string()))?;
        self.read_response(response)
    }

    /// Runs `sql` and keeps at most `row_limit` rows of its result, reading
    /// the response as it streams in and dropping it one row past the limit.
    ///
    /// The server-side `max_result_rows` and `limit` settings are not used:
    /// the first also applies to subqueries and the second gives no signal
    /// that rows were dropped. `max_result_bytes` is left out as well, since
    /// the client never reads more than the retained rows, and
    /// `wait_end_of_query` is off so the server streams instead of buffering
    /// the whole result before the first byte.
    pub(crate) fn execute_row_limited(
        &self,
        sql: &str,
        database: Option<&str>,
        timeout: Option<Duration>,
        row_limit: u32,
        offset: Option<u32>,
        settings: &[(&str, &str)],
    ) -> Result<LimitedResponse, ClickHouseHttpError> {
        let mut request = self
            .request(
                sql,
                database,
                timeout,
                "JSONCompactEachRowWithNamesAndTypes",
            )
            // Lets the server stop a read-only query once the response is
            // dropped at the cut. ClickHouse ignores it for queries that are
            // not read-only.
            .query(&[("cancel_http_readonly_queries_on_client_close", "1")]);
        if let Some(offset) = offset {
            request = request.query(&[("offset", offset)]);
        }
        if !settings.is_empty() {
            request = request.query(settings);
        }

        let response = request
            .send()
            .map_err(|error| ClickHouseHttpError::Transport(error.to_string()))?;
        let status = response.status();
        let headers = response.headers().clone();
        if !status.is_success() || exception_code(&headers).is_some() {
            return Err(self.read_response(response).err().unwrap_or_else(|| {
                ClickHouseHttpError::Server {
                    status: status.as_u16(),
                    code: exception_code(&headers),
                    body: String::new(),
                }
            }));
        }

        let row_limit = usize::try_from(row_limit).unwrap_or(usize::MAX);
        let mut limited = read_limited_rows(
            std::io::BufReader::new(response),
            row_limit,
            MAX_RESPONSE_BYTES,
        )?;
        limited.headers = headers;
        Ok(limited)
    }

    fn request(
        &self,
        sql: &str,
        database: Option<&str>,
        timeout: Option<Duration>,
        format: &str,
    ) -> RequestBuilder {
        let effective_timeout = timeout.unwrap_or(self.default_timeout);
        let max_execution_time = effective_timeout
            .as_secs()
            .saturating_add(u64::from(effective_timeout.subsec_nanos() != 0))
            .max(1)
            .to_string();
        let database = database.unwrap_or(&self.default_database);
        self.client
            .post(self.endpoint.clone())
            .basic_auth(
                &self.user,
                self.password.as_ref().map(ExposeSecret::expose_secret),
            )
            .header("X-ClickHouse-Format", format)
            .query(&[
                ("database", database),
                ("date_time_output_format", "iso"),
                ("output_format_json_quote_64bit_integers", "1"),
                ("output_format_json_quote_64bit_floats", "1"),
                ("output_format_json_quote_decimals", "1"),
                ("output_format_json_quote_denormals", "1"),
                ("output_format_json_named_tuples_as_objects", "0"),
                ("output_format_json_validate_utf8", "1"),
                ("max_execution_time", max_execution_time.as_str()),
            ])
            .body(sql.to_string())
            .timeout(effective_timeout)
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

/// Reads a `JSONCompactEachRowWithNamesAndTypes` body one line at a time,
/// keeping at most `row_limit` rows. Once the limit is reached it only looks
/// for one more non-blank byte to learn whether rows were dropped, so the
/// bytes consumed stay bounded by the retained rows. `max_bytes` caps the
/// retained lines, not the size of the whole result.
pub(crate) fn read_limited_rows<R: BufRead>(
    mut reader: R,
    row_limit: usize,
    max_bytes: u64,
) -> Result<LimitedResponse, ClickHouseHttpError> {
    let mut consumed = 0u64;
    let mut line = Vec::new();

    if !read_line(&mut reader, &mut line, &mut consumed, max_bytes)? {
        return Ok(LimitedResponse::default());
    }
    let names = parse_header_line(&mut reader, &line)?;
    if !read_line(&mut reader, &mut line, &mut consumed, max_bytes)? {
        return Err(ClickHouseHttpError::Body(
            "response ended before the column types line".to_string(),
        ));
    }
    let types = parse_header_line(&mut reader, &line)?;

    let mut rows = Vec::new();
    while rows.len() < row_limit {
        if !read_line(&mut reader, &mut line, &mut consumed, max_bytes)? {
            return Ok(LimitedResponse {
                names,
                types,
                rows,
                truncated: false,
                headers: HeaderMap::new(),
            });
        }
        let row = serde_json::from_slice::<Vec<serde_json::Value>>(&line)
            .map_err(|_| in_band_error(&mut reader, &line))?;
        rows.push(row);
    }

    let truncated = has_more_content(&mut reader)?;
    Ok(LimitedResponse {
        names,
        types,
        rows,
        truncated,
        headers: HeaderMap::new(),
    })
}

/// Reads the next non-blank line into `line`, returning `false` at the end of
/// the body.
fn read_line<R: BufRead>(
    reader: &mut R,
    line: &mut Vec<u8>,
    consumed: &mut u64,
    max_bytes: u64,
) -> Result<bool, ClickHouseHttpError> {
    loop {
        line.clear();
        let remaining = max_bytes.saturating_sub(*consumed);
        let read = reader
            .by_ref()
            .take(remaining.saturating_add(1))
            .read_until(b'\n', line)
            .map_err(|error| ClickHouseHttpError::Body(error.to_string()))?;
        *consumed = consumed.saturating_add(read as u64);
        if *consumed > max_bytes {
            return Err(ClickHouseHttpError::ResponseTooLarge { limit: max_bytes });
        }
        if read == 0 {
            return Ok(false);
        }
        if !line.iter().all(u8::is_ascii_whitespace) {
            return Ok(true);
        }
    }
}

fn parse_header_line<R: BufRead>(
    reader: &mut R,
    line: &[u8],
) -> Result<Vec<String>, ClickHouseHttpError> {
    serde_json::from_slice::<Vec<String>>(line).map_err(|_| in_band_error(reader, line))
}

/// Without `wait_end_of_query` the server may already have sent a success
/// status when the query fails, so the failure arrives as body text. A line
/// that does not parse is reported as that exception when the marker is
/// present, otherwise as a malformed body.
fn in_band_error<R: BufRead>(reader: &mut R, line: &[u8]) -> ClickHouseHttpError {
    let mut text = line.to_vec();
    if let Err(error) = reader.by_ref().take(MAX_ERROR_BYTES).read_to_end(&mut text) {
        log::debug!("ClickHouse error body could not be read completely: {error}");
    }
    let marker = b"__exception__";
    match text
        .windows(marker.len())
        .position(|window| window == marker)
    {
        Some(offset) => ClickHouseHttpError::Server {
            status: 200,
            code: None,
            body: error_excerpt(&text, offset),
        },
        None => ClickHouseHttpError::Body(format!(
            "invalid JSONCompactEachRow line: {}",
            error_excerpt(&text, 0)
        )),
    }
}

/// Reports whether any non-blank byte follows, consuming only whitespace and
/// the buffered chunk that holds that byte.
fn has_more_content<R: BufRead>(reader: &mut R) -> Result<bool, ClickHouseHttpError> {
    loop {
        let buffer = reader
            .fill_buf()
            .map_err(|error| ClickHouseHttpError::Body(error.to_string()))?;
        if buffer.is_empty() {
            return Ok(false);
        }
        if buffer.iter().any(|byte| !byte.is_ascii_whitespace()) {
            return Ok(true);
        }
        let length = buffer.len();
        reader.consume(length);
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
    use super::{ClickHouseHttpClient, ClickHouseHttpError, read_limited_rows, success_exception};
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

    /// Serves one HTTP request on a local port, answers it with an empty
    /// JSONCompact result and hands back the request line.
    fn serve_one_request() -> (String, std::thread::JoinHandle<String>) {
        use std::io::{BufRead, BufReader, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local listener");
        let address = listener.local_addr().expect("listener address");

        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept request");
            let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));

            let mut request_line = String::new();
            reader
                .read_line(&mut request_line)
                .expect("read request line");

            let mut content_length = 0usize;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).expect("read header");
                if header.trim().is_empty() {
                    break;
                }
                if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = value.trim().parse().unwrap_or(0);
                }
            }
            let mut body = vec![0u8; content_length];
            std::io::Read::read_exact(&mut reader, &mut body).expect("read body");

            let response_body = r#"{"meta":[],"data":[],"rows":0}"#;
            let mut stream = stream;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            )
            .expect("write response");

            request_line
        });

        (format!("http://{address}"), server)
    }

    #[test]
    fn extra_settings_are_sent_as_query_parameters() {
        let (endpoint, server) = serve_one_request();
        let client = client(&endpoint).expect("loopback endpoint");

        client
            .execute_with_settings("SELECT 1", None, None, None, None, &[("readonly", "2")])
            .expect("the local server answers");

        let request_line = server.join().expect("server thread");
        assert!(request_line.contains("readonly=2"), "{request_line}");
    }

    #[test]
    fn plain_execution_sends_no_readonly_setting() {
        let (endpoint, server) = serve_one_request();
        let client = client(&endpoint).expect("loopback endpoint");

        client
            .execute("SELECT 1", None, None, None, None)
            .expect("the local server answers");

        let request_line = server.join().expect("server thread");
        assert!(!request_line.contains("readonly="), "{request_line}");
    }

    const HEADER_LINES: &str = "[\"n\"]\n[\"UInt8\"]\n";

    fn stream(rows: &str) -> Vec<u8> {
        format!("{HEADER_LINES}{rows}").into_bytes()
    }

    #[test]
    fn limited_reader_exact_fit_is_not_truncated() {
        let body = stream("[1]\n[2]\n");
        let response = read_limited_rows(body.as_slice(), 2, 1024).expect("valid stream");
        assert_eq!(response.names, vec!["n".to_string()]);
        assert_eq!(response.types, vec!["UInt8".to_string()]);
        assert_eq!(
            response.rows,
            vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]]
        );
        assert!(!response.truncated);
    }

    #[test]
    fn limited_reader_one_row_past_the_limit_is_truncated() {
        let body = stream("[1]\n[2]\n[3]\n");
        let response = read_limited_rows(body.as_slice(), 2, 1024).expect("valid stream");
        assert_eq!(
            response.rows,
            vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]]
        );
        assert!(response.truncated);
    }

    #[test]
    fn limited_reader_zero_limit_keeps_columns_and_marks_truncation() {
        let body = stream("[1]\n");
        let response = read_limited_rows(body.as_slice(), 0, 1024).expect("valid stream");
        assert_eq!(response.names, vec!["n".to_string()]);
        assert!(response.rows.is_empty());
        assert!(response.truncated);
    }

    #[test]
    fn limited_reader_zero_limit_on_rowless_result_is_not_truncated() {
        let body = stream("");
        let response = read_limited_rows(body.as_slice(), 0, 1024).expect("valid stream");
        assert_eq!(response.names, vec!["n".to_string()]);
        assert!(response.rows.is_empty());
        assert!(!response.truncated);
    }

    #[test]
    fn limited_reader_empty_body_has_no_columns() {
        let response = read_limited_rows(b"".as_slice(), 5, 1024).expect("empty body");
        assert!(response.names.is_empty());
        assert!(response.rows.is_empty());
        assert!(!response.truncated);
    }

    #[test]
    fn limited_reader_ignores_malformed_data_after_the_cut() {
        let body = stream("[1]\nnot json\n__exception__\nCode: 241.");
        let response = read_limited_rows(body.as_slice(), 1, 1024).expect("cut before garbage");
        assert_eq!(response.rows, vec![vec![serde_json::json!(1)]]);
        assert!(response.truncated);
    }

    #[test]
    fn limited_reader_reports_exception_before_the_cut() {
        let body = stream("[1]\n__exception__\nCode: 241. DB::Exception: memory\n");
        let result = read_limited_rows(body.as_slice(), 5, 1024);
        let Err(ClickHouseHttpError::Server { body, .. }) = result else {
            panic!("an exception before the cut must fail: {result:?}");
        };
        assert!(body.contains("Code: 241"), "{body}");
    }

    #[test]
    fn limited_reader_does_not_read_past_the_cut() {
        let mut body = stream("[1]\n[2]\n");
        body.extend(std::iter::repeat_n(b'x', 1024 * 1024));
        let mut reader = std::io::BufReader::with_capacity(16, body.as_slice());
        let response = read_limited_rows(&mut reader, 1, 64).expect("bounded read");
        assert!(response.truncated);
        let remaining = std::io::Read::bytes(reader).count();
        assert!(
            remaining > 1024 * 1024 - 64,
            "read {remaining} bytes too many"
        );
    }

    #[test]
    fn limited_reader_still_caps_the_retained_rows() {
        let body = stream("[1]\n[2]\n");
        assert!(matches!(
            read_limited_rows(body.as_slice(), 5, 8),
            Err(ClickHouseHttpError::ResponseTooLarge { limit: 8 })
        ));
    }
}
