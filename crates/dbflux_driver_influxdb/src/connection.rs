//! InfluxDB connection implementation.
//!
//! `InfluxConnection` implements the `Connection` trait from `dbflux_core`.
//! It routes queries to the correct HTTP endpoint based on (version, language),
//! optionally injects time windows, and stores per-query metadata.

use std::sync::RwLock;
use std::time::Instant;

use dbflux_core::{
    Connection, DatabaseInfo, DbError, DbKind, DefaultSqlDialect, DriverMetadata, InfluxVersion,
    MeasurementInfo, QueryLanguage, QueryRequest, QueryResult, ResolvedWindow, SchemaFeatures,
    SchemaLoadingStrategy, SchemaSnapshot, SourceContextSpec, SourceQueryMode, TimeSeriesSchema,
};

use crate::error_formatter::InfluxErrorFormatter;
use crate::http::{HttpClient, HttpError, auth_header};
use crate::injection::{
    ResolvedWindow as InjectionWindow, flux_has_range_call, influxql_has_time_predicate,
    inject_flux_window, inject_influxql_window,
};
use crate::metadata::InfluxQueryMetadata;
use crate::parser::flux::parse_flux_csv;
use crate::parser::influxql::parse_influxql_json;

const INFLUXQL_MODE: &str = "influxql";
const FLUX_MODE: &str = "flux";

/// Active connection to an InfluxDB instance.
pub struct InfluxConnection {
    http: HttpClient,
    pub version: InfluxVersion,
    pub default_language: QueryLanguage,
    pub bucket_or_db: String,
    pub org: Option<String>,
    last_metadata: RwLock<Option<InfluxQueryMetadata>>,
}

impl InfluxConnection {
    /// Create a new connection. Called by `InfluxDriver::connect_with_secrets`.
    pub fn new(
        http: HttpClient,
        version: InfluxVersion,
        default_language: QueryLanguage,
        bucket_or_db: String,
        org: Option<String>,
    ) -> Self {
        Self {
            http,
            version,
            default_language,
            bucket_or_db,
            org,
            last_metadata: RwLock::new(None),
        }
    }

    /// Return the metadata from the most recently executed query.
    pub fn last_query_metadata(&self) -> Option<InfluxQueryMetadata> {
        self.last_metadata.read().ok()?.clone()
    }

    // -----------------------------------------------------------------------
    // Internal query dispatch
    // -----------------------------------------------------------------------

    /// Resolve the effective query language for this request.
    ///
    /// Reads the query mode from the execution context when present, otherwise
    /// falls back to `default_language`. Returns an error if Flux is requested
    /// on v1 (not supported).
    fn resolve_language(&self, req: &QueryRequest) -> Result<QueryLanguage, DbError> {
        let mode = req
            .execution_context
            .as_ref()
            .and_then(|ctx| ctx.source.as_ref())
            .and_then(|src| {
                use dbflux_core::ExecutionSourceContext;
                match src {
                    ExecutionSourceContext::CollectionWindow { query_mode, .. } => {
                        query_mode.as_deref()
                    }
                }
            });

        let language = match mode {
            Some(FLUX_MODE) => QueryLanguage::Flux,
            Some(INFLUXQL_MODE) => QueryLanguage::InfluxQuery,
            Some(other) => {
                return Err(DbError::query_failed(format!(
                    "unknown InfluxDB query mode: {other}"
                )));
            }
            None => self.default_language.clone(),
        };

        if language == QueryLanguage::Flux && self.version == InfluxVersion::V1 {
            return Err(DbError::query_failed(
                "Flux queries are not supported on InfluxDB v1".to_string(),
            ));
        }

        Ok(language)
    }

    /// Read the time window from the execution context (milliseconds → RFC 3339 strings).
    fn extract_window(req: &QueryRequest) -> InjectionWindow {
        let (start_ms, end_ms) = req
            .execution_context
            .as_ref()
            .and_then(|ctx| ctx.source.as_ref())
            .map(|src| {
                use dbflux_core::ExecutionSourceContext;
                match src {
                    ExecutionSourceContext::CollectionWindow {
                        start_ms, end_ms, ..
                    } => (Some(*start_ms), Some(*end_ms)),
                }
            })
            .unwrap_or((None, None));

        InjectionWindow {
            start_rfc3339: start_ms.map(ms_to_rfc3339),
            end_rfc3339: end_ms.map(ms_to_rfc3339),
        }
    }

    /// Execute the final query string using the appropriate HTTP endpoint.
    fn dispatch(
        &self,
        language: QueryLanguage,
        query: &str,
        started: Instant,
    ) -> Result<QueryResult, DbError> {
        let http_result = match (self.version, &language) {
            (InfluxVersion::V1, QueryLanguage::InfluxQuery) => {
                self.http.execute_influxql_v1(&self.bucket_or_db, query)
            }
            (InfluxVersion::V2, QueryLanguage::InfluxQuery) => {
                let org = self.org.as_deref().unwrap_or("");
                self.http
                    .execute_influxql_v2(&self.bucket_or_db, org, query)
            }
            (InfluxVersion::V2, QueryLanguage::Flux) => {
                let org = self.org.as_deref().unwrap_or("");
                self.http.execute_flux_v2(org, query)
            }
            (InfluxVersion::V1, QueryLanguage::Flux) => {
                // Guard: should have been caught by resolve_language, but be defensive.
                return Err(DbError::query_failed(
                    "Flux queries are not supported on InfluxDB v1".to_string(),
                ));
            }
            _ => {
                return Err(DbError::query_failed(format!(
                    "unsupported language {:?} for this InfluxDB version",
                    language
                )));
            }
        };

        let resp = http_result.map_err(map_http_error)?;

        if resp.status >= 400 {
            let fe = InfluxErrorFormatter::format_http_error(resp.status, &resp.body);
            return Err(if resp.status == 401 || resp.status == 403 {
                DbError::AuthFailed(fe)
            } else {
                DbError::QueryFailed(fe)
            });
        }

        let mut result = match language {
            QueryLanguage::Flux => {
                parse_flux_csv(&resp.body).map_err(|e| DbError::query_failed(e.to_string()))?
            }
            _ => {
                parse_influxql_json(&resp.body).map_err(|e| DbError::query_failed(e.to_string()))?
            }
        };

        result.execution_time = started.elapsed();
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Connection trait
// ---------------------------------------------------------------------------

impl Connection for InfluxConnection {
    fn metadata(&self) -> &DriverMetadata {
        &crate::driver::INFLUXDB_METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        // Use a lightweight query to probe liveness.
        let query = "SHOW MEASUREMENTS LIMIT 1";
        match self.version {
            InfluxVersion::V1 => {
                let resp = self
                    .http
                    .execute_influxql_v1(&self.bucket_or_db, query)
                    .map_err(map_http_error)?;
                if resp.status >= 400 {
                    return Err(DbError::connection_failed(format!(
                        "ping failed: HTTP {}",
                        resp.status
                    )));
                }
            }
            InfluxVersion::V2 => {
                let org = self.org.as_deref().unwrap_or("");
                let resp = self
                    .http
                    .execute_influxql_v2(&self.bucket_or_db, org, query)
                    .map_err(map_http_error)?;
                if resp.status >= 400 {
                    return Err(DbError::connection_failed(format!(
                        "ping failed: HTTP {}",
                        resp.status
                    )));
                }
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        let started = Instant::now();
        let language = self.resolve_language(req)?;

        let window = Self::extract_window(req);

        let injected_window;
        let resolved_window: Option<ResolvedWindow>;
        let final_query: String;

        match language {
            QueryLanguage::Flux => {
                let had_range = flux_has_range_call(&req.sql);
                final_query = inject_flux_window(&req.sql, &window);
                injected_window = !had_range
                    && (window.start_rfc3339.is_some() || window.end_rfc3339.is_some())
                    && flux_has_range_call(&final_query);
                resolved_window = if injected_window {
                    window_to_resolved(&window, language.clone())
                } else {
                    None
                };
            }
            _ => {
                let had_predicate = influxql_has_time_predicate(&req.sql);
                final_query = inject_influxql_window(&req.sql, &window);
                injected_window = !had_predicate
                    && (window.start_rfc3339.is_some() || window.end_rfc3339.is_some());
                resolved_window = if injected_window {
                    window_to_resolved(&window, language.clone())
                } else {
                    None
                };
            }
        }

        let mut result = self.dispatch(language.clone(), &final_query, started)?;

        if let Some(ref rw) = resolved_window {
            result.resolved_window = Some(rw.clone());
        }

        // Store metadata for this query.
        if let Ok(mut guard) = self.last_metadata.write() {
            *guard = Some(InfluxQueryMetadata {
                version: self.version,
                language,
                resolved_window: resolved_window.as_ref().map(|rw| InjectionWindow {
                    start_rfc3339: Some(rfc3339_from_ms(rw.start_ms)),
                    end_rfc3339: Some(rfc3339_from_ms(rw.end_ms)),
                }),
                bucket_or_database: self.bucket_or_db.clone(),
                injected_window,
            });
        }

        Ok(result)
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Query cancellation not supported for InfluxDB".to_string(),
        ))
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let measurements = self.fetch_measurements()?;

        let schema = TimeSeriesSchema {
            databases: vec![DatabaseInfo {
                name: self.bucket_or_db.clone(),
                is_current: true,
            }],
            current_database: Some(self.bucket_or_db.clone()),
            measurements,
            retention_policies: Vec::new(),
        };

        Ok(SchemaSnapshot::time_series(schema))
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        match self.version {
            InfluxVersion::V1 => {
                let resp = self
                    .http
                    .execute_influxql_v1("_internal", "SHOW DATABASES")
                    .map_err(map_http_error)?;
                parse_influxql_string_list(&resp.body, "name")
            }
            InfluxVersion::V2 => {
                let org = self.org.as_deref().unwrap_or("");
                let base = self.http.base_url.trim_end_matches('/');
                let url = format!("{base}/api/v2/buckets?org={}", urlencoding::encode(org));
                let http_resp = self.http_get_raw(&url).map_err(map_http_error)?;
                parse_v2_buckets_response(&http_resp)
            }
        }
    }

    fn kind(&self) -> DbKind {
        DbKind::InfluxDB
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::SingleDatabase
    }

    fn schema_features(&self) -> SchemaFeatures {
        SchemaFeatures::empty()
    }

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        &DefaultSqlDialect
    }

    fn source_context_spec(&self) -> Option<SourceContextSpec> {
        let query_modes = match self.version {
            InfluxVersion::V1 => vec![SourceQueryMode {
                value: INFLUXQL_MODE.to_string(),
                label: "InfluxQL".to_string(),
                query_language: QueryLanguage::InfluxQuery,
            }],
            InfluxVersion::V2 => vec![
                SourceQueryMode {
                    value: INFLUXQL_MODE.to_string(),
                    label: "InfluxQL".to_string(),
                    query_language: QueryLanguage::InfluxQuery,
                },
                SourceQueryMode {
                    value: FLUX_MODE.to_string(),
                    label: "Flux".to_string(),
                    query_language: QueryLanguage::Flux,
                },
            ],
        };

        let targets_label = match self.version {
            InfluxVersion::V1 => "Database".to_string(),
            InfluxVersion::V2 => "Bucket".to_string(),
        };

        Some(SourceContextSpec {
            targets_label,
            targets_placeholder: self.bucket_or_db.clone(),
            start_label: "Start".to_string(),
            end_label: "End".to_string(),
            query_mode_label: Some("Syntax".to_string()),
            default_query_mode: Some(INFLUXQL_MODE.to_string()),
            query_modes,
        })
    }

    fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
        None
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

impl InfluxConnection {
    fn fetch_measurements(&self) -> Result<Vec<MeasurementInfo>, DbError> {
        let query = "SHOW MEASUREMENTS";
        let resp = match self.version {
            InfluxVersion::V1 => self
                .http
                .execute_influxql_v1(&self.bucket_or_db, query)
                .map_err(map_http_error)?,
            InfluxVersion::V2 => {
                let org = self.org.as_deref().unwrap_or("");
                self.http
                    .execute_influxql_v2(&self.bucket_or_db, org, query)
                    .map_err(map_http_error)?
            }
        };

        let result =
            parse_influxql_json(&resp.body).map_err(|e| DbError::query_failed(e.to_string()))?;

        let measurements = result
            .rows
            .iter()
            .filter_map(|row| {
                row.first().and_then(|v| match v {
                    dbflux_core::Value::Text(s) => Some(MeasurementInfo {
                        name: s.clone(),
                        tags: Vec::new(),
                        fields: Vec::new(),
                    }),
                    _ => None,
                })
            })
            .collect();

        Ok(measurements)
    }

    /// Issue a raw GET request using the embedded HTTP client (for v2 REST APIs
    /// not available as InfluxQL, such as `/api/v2/buckets`).
    fn http_get_raw(&self, url: &str) -> Result<crate::http::HttpResponseBody, HttpError> {
        // Reuse the client through a helper path that constructs the URL externally.
        // We delegate to HttpClient::execute_influxql_v1 as a proxy because the HttpClient
        // does not expose a raw GET method publicly; instead we add a dedicated path here.
        // This is a minor design concession: a full REST client would expose `get(url)`.
        //
        // For now we call the v2 buckets URL via the blocking reqwest client indirectly
        // through execute_influxql_v2 with an empty bucket. However, this does not match
        // the correct path. We therefore expose a workaround: build the HTTP client ourselves.
        //
        // The cleanest solution without breaking the HttpClient API is to make this call
        // directly, accepting a small code duplication.
        use std::time::Duration;

        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .gzip(true)
            .use_rustls_tls()
            .build()
            .map_err(|e| HttpError::Transport(e.to_string()))?;

        let mut req_builder = client.get(url);
        if let Some((name, value)) = auth_header(&self.http.auth) {
            req_builder = req_builder.header(name, value);
        }

        let resp = req_builder
            .send()
            .map_err(|e| HttpError::Transport(e.to_string()))?;

        let status = resp.status().as_u16();
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let body = resp.text().map_err(|e| HttpError::Body(e.to_string()))?;

        Ok(crate::http::HttpResponseBody {
            status,
            content_type,
            body,
        })
    }
}

/// Convert an HTTP error to a `DbError`.
fn map_http_error(e: HttpError) -> DbError {
    match e {
        HttpError::Server { status, ref body } => {
            let fe = InfluxErrorFormatter::format_http_error(status, body);
            if status == 401 || status == 403 {
                DbError::AuthFailed(fe)
            } else {
                DbError::ConnectionFailed(fe)
            }
        }
        HttpError::Transport(msg) | HttpError::Body(msg) => DbError::connection_failed(msg),
    }
}

/// Parse a flat InfluxQL string list from a SHOW DATABASES / SHOW MEASUREMENTS result.
fn parse_influxql_string_list(
    body: &str,
    _column_name: &str,
) -> Result<Vec<DatabaseInfo>, DbError> {
    let result = parse_influxql_json(body).map_err(|e| DbError::query_failed(e.to_string()))?;

    let databases: Vec<DatabaseInfo> = result
        .rows
        .iter()
        .filter_map(|row| {
            row.first().and_then(|v| match v {
                dbflux_core::Value::Text(s) => Some(DatabaseInfo {
                    name: s.clone(),
                    is_current: false,
                }),
                _ => None,
            })
        })
        .collect();

    Ok(databases)
}

/// Parse the InfluxDB v2 `/api/v2/buckets` JSON response into a list of `DatabaseInfo`.
fn parse_v2_buckets_response(
    resp: &crate::http::HttpResponseBody,
) -> Result<Vec<DatabaseInfo>, DbError> {
    let root: serde_json::Value = serde_json::from_str(&resp.body)
        .map_err(|e| DbError::query_failed(format!("failed to parse buckets response: {e}")))?;

    let buckets = root
        .get("buckets")
        .and_then(|v| v.as_array())
        .ok_or_else(|| DbError::query_failed("missing 'buckets' field in response".to_string()))?;

    let result = buckets
        .iter()
        .filter_map(|b| {
            b.get("name")
                .and_then(|v| v.as_str())
                .map(|name| DatabaseInfo {
                    name: name.to_string(),
                    is_current: false,
                })
        })
        .collect();

    Ok(result)
}

/// Convert an `InjectionWindow` into a `ResolvedWindow`.
fn window_to_resolved(window: &InjectionWindow, language: QueryLanguage) -> Option<ResolvedWindow> {
    let start_ms = window.start_rfc3339.as_deref().and_then(rfc3339_to_ms)?;
    let end_ms = window.end_rfc3339.as_deref().and_then(rfc3339_to_ms)?;

    Some(ResolvedWindow {
        start_ms,
        end_ms,
        language,
    })
}

/// Convert a Unix timestamp in milliseconds to an RFC 3339 string.
fn ms_to_rfc3339(ms: i64) -> String {
    use chrono::{DateTime, Utc};
    let dt = DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or(DateTime::UNIX_EPOCH);
    dt.to_rfc3339()
}

/// Convert an RFC 3339 string to a Unix timestamp in milliseconds.
fn rfc3339_to_ms(s: &str) -> Option<i64> {
    use chrono::DateTime;
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

/// Convert a millisecond timestamp back to RFC 3339 (used for metadata).
fn rfc3339_from_ms(ms: i64) -> String {
    ms_to_rfc3339(ms)
}

// ---------------------------------------------------------------------------
// Tests (C.8.1 – C.8.3) — dispatcher correctness using in-process checks
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{ExecutionContext, ExecutionSourceContext, QueryRequest};

    fn make_request_with_mode(sql: &str, mode: Option<&str>) -> QueryRequest {
        let source = ExecutionSourceContext::CollectionWindow {
            targets: vec![],
            start_ms: 1704067200000,
            end_ms: 1704070800000,
            query_mode: mode.map(|s| s.to_string()),
        };

        let ctx = ExecutionContext {
            source: Some(source),
            ..Default::default()
        };

        QueryRequest::new(sql).with_execution_context(Some(ctx))
    }

    // C.8.1 — V1 + Flux should fail early
    #[test]
    fn v1_flux_mode_returns_error_without_http_call() {
        // We test resolve_language directly (no HTTP needed).
        let conn = InfluxConnection::new(
            // We need an HttpClient but we can't easily mock it.
            // Instead we test resolve_language directly via a stub.
            build_stub_http(),
            InfluxVersion::V1,
            QueryLanguage::InfluxQuery,
            "mydb".to_string(),
            None,
        );

        let req = make_request_with_mode("from(bucket: \"b\")", Some("flux"));
        let result = conn.resolve_language(&req);
        assert!(result.is_err(), "Flux on V1 must return an error");
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not supported"),
            "error must mention not supported: {msg}"
        );
    }

    // C.8.2 — resolve_language dispatches correctly
    #[test]
    fn resolve_language_influxql_mode_returns_influxql() {
        let conn = InfluxConnection::new(
            build_stub_http(),
            InfluxVersion::V2,
            QueryLanguage::InfluxQuery,
            "b".to_string(),
            Some("org".to_string()),
        );

        let req = make_request_with_mode("SELECT 1", Some("influxql"));
        let lang = conn.resolve_language(&req).expect("must resolve");
        assert_eq!(lang, QueryLanguage::InfluxQuery);
    }

    #[test]
    fn resolve_language_flux_mode_on_v2_returns_flux() {
        let conn = InfluxConnection::new(
            build_stub_http(),
            InfluxVersion::V2,
            QueryLanguage::InfluxQuery,
            "b".to_string(),
            Some("org".to_string()),
        );

        let req = make_request_with_mode("from(bucket: \"b\")", Some("flux"));
        let lang = conn.resolve_language(&req).expect("must resolve");
        assert_eq!(lang, QueryLanguage::Flux);
    }

    // C.8.3 — extract_window reads start_ms / end_ms
    #[test]
    fn extract_window_reads_start_and_end_ms() {
        let req = make_request_with_mode("SELECT 1", None);
        let window = InfluxConnection::extract_window(&req);
        assert!(window.start_rfc3339.is_some(), "start must be present");
        assert!(window.end_rfc3339.is_some(), "end must be present");
    }

    fn build_stub_http() -> HttpClient {
        // Use a non-routable address; won't make real connections in unit tests.
        HttpClient::new(
            "http://127.0.0.1:19999".to_string(),
            crate::http::AuthCreds::None,
            InfluxVersion::V2,
        )
        .expect("stub HTTP client build")
    }
}
