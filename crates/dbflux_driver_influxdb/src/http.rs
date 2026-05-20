//! HTTP transport layer for InfluxDB queries.
//!
//! Pure URL builders and auth header construction are kept separate from the
//! blocking HTTP client so they can be unit-tested without network access.

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use thiserror::Error;

use dbflux_core::InfluxVersion;

// ---------------------------------------------------------------------------
// Auth credential types
// ---------------------------------------------------------------------------

/// Authentication credentials for an InfluxDB connection.
#[derive(Debug, Clone)]
pub enum AuthCreds {
    /// InfluxDB v2 token-based authentication.
    Token(String),
    /// InfluxDB v1 username+password authentication.
    Basic { user: String, password: String },
    /// No authentication.
    None,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Raw HTTP response body returned by the InfluxDB transport layer.
#[derive(Debug)]
pub struct HttpResponseBody {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: String,
}

/// Errors from the InfluxDB HTTP transport layer.
#[derive(Debug, Error)]
pub enum HttpError {
    #[error("HTTP request failed: {0}")]
    Transport(String),

    #[error("InfluxDB returned HTTP {status}: {body}")]
    Server { status: u16, body: String },

    #[error("Failed to read response body: {0}")]
    Body(String),
}

// ---------------------------------------------------------------------------
// Pure URL builders — no I/O, fully testable
// ---------------------------------------------------------------------------

/// Build the InfluxDB v1 query URL with encoded InfluxQL query.
///
/// Uses Authorization header for auth; URL params carry db and epoch only.
pub fn build_v1_influxql_url(base: &str, db: &str, query: &str) -> String {
    let base = base.trim_end_matches('/');
    let q = urlencoding::encode(query);
    let db_enc = urlencoding::encode(db);
    format!("{base}/query?db={db_enc}&q={q}&epoch=ms")
}

/// Build the InfluxDB v2 compatibility endpoint URL for InfluxQL queries.
///
/// The v2 compatibility API exposes `/query` with `db` mapped to the bucket
/// and an optional `org` param required for multi-org setups.
pub fn build_v2_influxql_url(base: &str, bucket: &str, org: &str, query: &str) -> String {
    let base = base.trim_end_matches('/');
    let q = urlencoding::encode(query);
    let bucket_enc = urlencoding::encode(bucket);
    let org_enc = urlencoding::encode(org);
    format!("{base}/query?db={bucket_enc}&org={org_enc}&q={q}&epoch=ms")
}

/// Build the InfluxDB v2 Flux query URL.
///
/// The Flux API endpoint takes org as a query param; the query itself is sent
/// as a JSON body by the caller.
pub fn build_v2_flux_url(base: &str, org: &str) -> String {
    let base = base.trim_end_matches('/');
    let org_enc = urlencoding::encode(org);
    format!("{base}/api/v2/query?org={org_enc}")
}

/// Build the JSON body for a v2 Flux query request.
///
/// The `dialect.annotations` request is required: without it, InfluxDB v2
/// returns annotation-free CSV (header + data only), so the response parser
/// cannot recover column types and every column degrades to text — which in
/// turn breaks chart auto-detection (no Timestamp / numeric columns). We request
/// all three annotations the CSV parser understands.
pub fn build_flux_request_body(query: &str) -> serde_json::Value {
    serde_json::json!({
        "query": query,
        "type": "flux",
        "dialect": {
            "annotations": ["datatype", "group", "default"],
            "header": true,
            "delimiter": ",",
            "dateTimeFormat": "RFC3339"
        }
    })
}

/// Build the Authorization header value for the given credentials.
///
/// v2 tokens use the `Token <value>` scheme. v1 credentials use HTTP Basic
/// encoded in Base64, matching InfluxDB 1.x's own documentation. We prefer
/// header-based auth over URL query params because query params appear in
/// server logs.
///
/// Returns `None` when `AuthCreds::None`.
pub fn auth_header(creds: &AuthCreds) -> Option<(String, String)> {
    match creds {
        AuthCreds::Token(token) => Some(("Authorization".into(), format!("Token {token}"))),
        AuthCreds::Basic { user, password } => {
            let encoded = BASE64.encode(format!("{user}:{password}"));
            Some(("Authorization".into(), format!("Basic {encoded}")))
        }
        AuthCreds::None => None,
    }
}

// ---------------------------------------------------------------------------
// Blocking HTTP client
// ---------------------------------------------------------------------------

/// Blocking HTTP client for InfluxDB queries.
pub struct HttpClient {
    client: reqwest::blocking::Client,
    pub base_url: String,
    pub auth: AuthCreds,
    pub version: InfluxVersion,
}

impl HttpClient {
    /// Build a new HTTP client for the given base URL and credentials.
    pub fn new(
        base_url: String,
        auth: AuthCreds,
        version: InfluxVersion,
    ) -> Result<Self, HttpError> {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .gzip(true)
            .tcp_nodelay(true)
            .use_rustls_tls()
            .build()
            .map_err(|error| HttpError::Transport(error.to_string()))?;

        Ok(Self {
            client,
            base_url,
            auth,
            version,
        })
    }

    /// Execute a v1 InfluxQL query and return the raw response body.
    pub fn execute_influxql_v1(
        &self,
        db: &str,
        query: &str,
    ) -> Result<HttpResponseBody, HttpError> {
        let url = build_v1_influxql_url(&self.base_url, db, query);
        self.get(&url)
    }

    /// Execute a v2 InfluxQL query and return the raw response body.
    pub fn execute_influxql_v2(
        &self,
        bucket: &str,
        org: &str,
        query: &str,
    ) -> Result<HttpResponseBody, HttpError> {
        let url = build_v2_influxql_url(&self.base_url, bucket, org, query);
        self.get(&url)
    }

    /// Execute a v2 Flux query and return the raw response body.
    ///
    /// The Flux API accepts the query as a JSON body via POST.
    pub fn execute_flux_v2(&self, org: &str, query: &str) -> Result<HttpResponseBody, HttpError> {
        let url = build_v2_flux_url(&self.base_url, org);
        self.post_flux(&url, query)
    }

    /// Issue a GET request, applying auth headers.
    fn get(&self, url: &str) -> Result<HttpResponseBody, HttpError> {
        let mut req = self.client.get(url);

        if let Some((name, value)) = auth_header(&self.auth) {
            req = req.header(name, value);
        }

        let resp = req
            .send()
            .map_err(|error| HttpError::Transport(error.to_string()))?;

        self.read_response(resp)
    }

    /// Issue a POST request with a Flux query body.
    ///
    /// The `dialect.annotations` request is required: without it, InfluxDB v2
    /// returns annotation-free CSV (header + data only), so the response parser
    /// cannot recover column types and every column degrades to text — which in
    /// turn breaks chart auto-detection (no Timestamp / numeric columns). We
    /// request all three annotations the CSV parser understands.
    fn post_flux(&self, url: &str, query: &str) -> Result<HttpResponseBody, HttpError> {
        let body = build_flux_request_body(query);

        let mut req = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/csv")
            .json(&body);

        if let Some((name, value)) = auth_header(&self.auth) {
            req = req.header(name, value);
        }

        let resp = req
            .send()
            .map_err(|error| HttpError::Transport(error.to_string()))?;

        self.read_response(resp)
    }

    fn read_response(
        &self,
        resp: reqwest::blocking::Response,
    ) -> Result<HttpResponseBody, HttpError> {
        let status = resp.status().as_u16();
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|s| s.to_string());

        let body = resp
            .text()
            .map_err(|error| HttpError::Body(error.to_string()))?;

        Ok(HttpResponseBody {
            status,
            content_type,
            body,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests (C.1.1 – C.1.4 / C.1.7)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // C.1.1
    #[test]
    fn build_v1_influxql_url_encodes_query_and_sets_epoch() {
        let url = build_v1_influxql_url("http://localhost:8086", "mydb", "SELECT * FROM cpu");
        assert!(
            url.starts_with("http://localhost:8086/query"),
            "path must be /query, got: {url}"
        );
        assert!(url.contains("db=mydb"), "must carry db param, got: {url}");
        assert!(
            url.contains("epoch=ms"),
            "must request epoch=ms, got: {url}"
        );
        // The query is URL-encoded
        assert!(
            url.contains("SELECT+%2A+FROM+cpu") || url.contains("SELECT%20%2A%20FROM%20cpu"),
            "query must be URL-encoded, got: {url}"
        );
    }

    #[test]
    fn build_v1_influxql_url_strips_trailing_slash_from_base() {
        let url = build_v1_influxql_url("http://localhost:8086/", "db", "SHOW MEASUREMENTS");
        assert!(
            !url.contains("//query"),
            "must not double-slash, got: {url}"
        );
    }

    // C.1.2
    #[test]
    fn build_v2_influxql_url_includes_bucket_org_and_epoch() {
        let url = build_v2_influxql_url(
            "http://localhost:8086",
            "my-bucket",
            "my-org",
            "SELECT * FROM cpu",
        );
        assert!(url.contains("/query"), "path must be /query, got: {url}");
        assert!(
            url.contains("db=my-bucket"),
            "must carry db=bucket, got: {url}"
        );
        assert!(
            url.contains("org=my-org"),
            "must carry org param, got: {url}"
        );
        assert!(
            url.contains("epoch=ms"),
            "must request epoch=ms, got: {url}"
        );
    }

    // C.1.3
    #[test]
    fn build_v2_flux_url_includes_org_param() {
        let url = build_v2_flux_url("http://localhost:8086", "my-org");
        assert!(
            url.contains("/api/v2/query"),
            "path must be /api/v2/query, got: {url}"
        );
        assert!(
            url.contains("org=my-org"),
            "must carry org param, got: {url}"
        );
    }

    #[test]
    fn flux_request_body_requests_datatype_annotations() {
        let body = build_flux_request_body("from(bucket: \"b\") |> range(start: -1h)");

        assert_eq!(body["type"], "flux");
        assert_eq!(body["query"], "from(bucket: \"b\") |> range(start: -1h)");

        let annotations = body["dialect"]["annotations"]
            .as_array()
            .expect("annotations must be an array");

        // The datatype annotation is what lets the CSV parser recover column
        // types (Timestamp / Float); without it, charts cannot auto-detect.
        assert!(
            annotations.iter().any(|a| a == "datatype"),
            "dialect must request the datatype annotation, got: {annotations:?}"
        );
    }

    // C.1.4 — token auth
    #[test]
    fn auth_header_token_returns_authorization_header() {
        let (name, value) = auth_header(&AuthCreds::Token("mytoken".into())).unwrap();
        assert_eq!(name, "Authorization");
        assert_eq!(value, "Token mytoken");
    }

    // C.1.4 — basic auth
    #[test]
    fn auth_header_basic_returns_base64_authorization() {
        let (name, value) = auth_header(&AuthCreds::Basic {
            user: "user".into(),
            password: "pass".into(),
        })
        .unwrap();
        assert_eq!(name, "Authorization");
        // "user:pass" in base64 = "dXNlcjpwYXNz"
        assert_eq!(value, "Basic dXNlcjpwYXNz");
    }

    // C.1.4 — no auth
    #[test]
    fn auth_header_none_returns_none() {
        assert!(auth_header(&AuthCreds::None).is_none());
    }

    #[test]
    fn build_v2_influxql_url_encodes_special_chars_in_bucket() {
        let url = build_v2_influxql_url("http://host:8086", "my bucket/2024", "org", "SELECT 1");
        assert!(
            !url.contains("my bucket"),
            "spaces must be encoded, got: {url}"
        );
    }
}
