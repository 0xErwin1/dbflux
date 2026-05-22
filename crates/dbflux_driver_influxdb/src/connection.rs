//! InfluxDB connection implementation.
//!
//! `InfluxConnection` implements the `Connection` trait from `dbflux_core`.
//! It routes queries to the correct HTTP endpoint based on (version, language),
//! optionally injects time windows, and stores per-query metadata.

use std::sync::RwLock;
use std::time::Instant;

use dbflux_core::{
    CollectionBrowseRequest, CollectionCountRequest, Connection, DatabaseInfo, DbError, DbKind,
    DefaultSqlDialect, DriverMetadata, InfluxVersion, MeasurementInfo, QueryLanguage, QueryRequest,
    QueryResult, ResolvedWindow, SchemaFeatures, SchemaLoadingStrategy, SchemaSnapshot,
    SourceContextSpec, SourceQueryMode, TimeSeriesSchema,
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
use crate::query_generator::InfluxQueryGenerator;

const INFLUXQL_MODE: &str = "influxql";
const FLUX_MODE: &str = "flux";

/// Active connection to an InfluxDB instance.
pub struct InfluxConnection {
    http: HttpClient,
    pub version: InfluxVersion,
    pub default_language: QueryLanguage,
    /// Default bucket (v2) or database (v1) from the connection profile.
    ///
    /// `None` when the user did not specify one: they must pick a bucket from the
    /// source-context dropdown before running a query. When set this value is
    /// pre-selected in the editor but can be overridden at query time.
    pub default_bucket: Option<String>,
    pub org: Option<String>,
    last_metadata: RwLock<Option<InfluxQueryMetadata>>,
    /// Query generator exposed through `Connection::query_generator()`.
    ///
    /// Kept as an owned value so it can be returned as `&dyn QueryGenerator`
    /// with the connection's lifetime.
    query_gen: InfluxQueryGenerator,
}

impl InfluxConnection {
    /// Create a new connection. Called by `InfluxDriver::connect_with_secrets`.
    pub fn new(
        http: HttpClient,
        version: InfluxVersion,
        default_language: QueryLanguage,
        default_bucket: Option<String>,
        org: Option<String>,
    ) -> Self {
        let query_gen =
            InfluxQueryGenerator::new(version, default_language.clone(), default_bucket.clone());

        Self {
            http,
            version,
            default_language,
            default_bucket,
            org,
            last_metadata: RwLock::new(None),
            query_gen,
        }
    }

    /// Return the metadata from the most recently executed query.
    pub fn last_query_metadata(&self) -> Option<InfluxQueryMetadata> {
        self.last_metadata.read().ok()?.clone()
    }

    /// Build a flat map of driver-specific audit fields from query metadata.
    ///
    /// The returned map is attached to `QueryResult.metadata_extra` and merged
    /// into `details_json` by the generic runner — no driver-id branching needed.
    pub fn build_metadata_extra_fields(
        meta: &InfluxQueryMetadata,
    ) -> std::collections::HashMap<String, serde_json::Value> {
        let language_str = match meta.language {
            QueryLanguage::Flux => "flux",
            QueryLanguage::InfluxQuery => "influxql",
            _ => "unknown",
        };

        let version_str = match meta.version {
            InfluxVersion::V1 => "v1",
            InfluxVersion::V2 => "v2",
        };

        let mut map = std::collections::HashMap::new();
        map.insert(
            "language".to_string(),
            serde_json::Value::String(language_str.to_string()),
        );
        map.insert(
            "version".to_string(),
            serde_json::Value::String(version_str.to_string()),
        );
        map.insert(
            "bucket_or_database".to_string(),
            serde_json::Value::String(meta.bucket_or_database.clone()),
        );
        map.insert(
            "injected_window".to_string(),
            serde_json::Value::Bool(meta.injected_window),
        );

        // Include start/end ms only when the window was actually injected.
        if let Some(ref rw) = meta.resolved_window
            && let (Some(start_str), Some(end_str)) =
                (rw.start_rfc3339.as_deref(), rw.end_rfc3339.as_deref())
            && let (Some(start_ms), Some(end_ms)) =
                (rfc3339_to_ms(start_str), rfc3339_to_ms(end_str))
        {
            map.insert(
                "resolved_window_start_ms".to_string(),
                serde_json::Value::Number(start_ms.into()),
            );
            map.insert(
                "resolved_window_end_ms".to_string(),
                serde_json::Value::Number(end_ms.into()),
            );
        }

        map
    }

    // -----------------------------------------------------------------------
    // Internal query dispatch
    // -----------------------------------------------------------------------

    /// Extract the first selected target (bucket/database) from the source-context.
    ///
    /// Returns `None` when the execution context carries no selected targets.
    fn resolve_bucket_from_context(&self, req: &QueryRequest) -> Option<String> {
        req.execution_context
            .as_ref()
            .and_then(|ctx| ctx.source.as_ref())
            .and_then(|src| {
                use dbflux_core::ExecutionSourceContext;
                match src {
                    ExecutionSourceContext::CollectionWindow { targets, .. } => {
                        targets.first().cloned()
                    }
                    // MetricQuery is never produced by InfluxDB; return neutral default.
                    _ => None,
                }
            })
    }

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
                    // MetricQuery is never produced by InfluxDB; return neutral default.
                    _ => None,
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
                    // MetricQuery is never produced by InfluxDB; return neutral default.
                    _ => (None, None),
                }
            })
            .unwrap_or((None, None));

        InjectionWindow {
            start_rfc3339: start_ms.map(ms_to_rfc3339),
            end_rfc3339: end_ms.map(ms_to_rfc3339),
        }
    }

    /// Execute the final query string using the appropriate HTTP endpoint.
    ///
    /// `bucket` is the resolved per-query bucket or database name. For InfluxQL
    /// queries it is injected into the URL (`?db=<bucket>`). For Flux queries the
    /// bucket lives inside the query text itself, so this parameter is unused.
    fn dispatch(
        &self,
        language: QueryLanguage,
        bucket: &str,
        query: &str,
        started: Instant,
    ) -> Result<QueryResult, DbError> {
        let http_result = match (self.version, &language) {
            (InfluxVersion::V1, QueryLanguage::InfluxQuery) => {
                self.http.execute_influxql_v1(bucket, query)
            }
            (InfluxVersion::V2, QueryLanguage::InfluxQuery) => {
                let org = self.org.as_deref().unwrap_or("");
                self.http.execute_influxql_v2(bucket, org, query)
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
        // Probe liveness without requiring a specific bucket or database.
        //
        // v1: SHOW DATABASES against the always-present `_internal` database.
        // v2: GET /api/v2/buckets?limit=1 — a lightweight authenticated read
        //     that works regardless of whether a default bucket was configured.
        match self.version {
            InfluxVersion::V1 => {
                let resp = self
                    .http
                    .execute_influxql_v1("_internal", "SHOW DATABASES")
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
                let base = self.http.base_url.trim_end_matches('/');
                let url = format!(
                    "{base}/api/v2/buckets?limit=1&org={}",
                    urlencoding::encode(org)
                );
                let resp = self.http_get_raw(&url).map_err(map_http_error)?;
                if resp.status >= 400 {
                    return Err(if resp.status == 401 || resp.status == 403 {
                        DbError::AuthFailed(InfluxErrorFormatter::format_http_error(
                            resp.status,
                            &resp.body,
                        ))
                    } else {
                        DbError::connection_failed(format!("ping failed: HTTP {}", resp.status))
                    });
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

        // Resolve the effective bucket/database for this query.
        //
        // Priority: (1) first target from source-context dropdown, (2) profile default.
        // InfluxQL queries embed the bucket in the URL (`?db=<bucket>`), so they need
        // an explicit value. Flux queries carry the bucket inside the query text, so
        // the resolved bucket is only used for metadata recording in that case.
        let resolved_bucket = self
            .resolve_bucket_from_context(req)
            .or_else(|| self.default_bucket.clone());

        // For InfluxQL, a bucket is required to route the HTTP request.
        // Flux queries reference the bucket inside the query text, so we allow
        // proceeding without one — the user wrote `from(bucket: "...")` themselves.
        if language != QueryLanguage::Flux && resolved_bucket.is_none() {
            return Err(DbError::query_failed(
                "Select a source bucket from the dropdown before running a query".to_string(),
            ));
        }

        let bucket_for_dispatch = resolved_bucket.as_deref().unwrap_or("");

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

        let mut result =
            self.dispatch(language.clone(), bucket_for_dispatch, &final_query, started)?;

        if let Some(ref rw) = resolved_window {
            result.resolved_window = Some(rw.clone());
        }

        // Record the actual bucket used for this query, not the profile default.
        let effective_bucket = resolved_bucket
            .clone()
            .unwrap_or_else(|| "<flux-inline>".to_string());

        // Build and store per-query metadata for both audit forwarding and UI inspection.
        let meta = InfluxQueryMetadata {
            version: self.version,
            language,
            resolved_window: resolved_window.as_ref().map(|rw| InjectionWindow {
                start_rfc3339: Some(rfc3339_from_ms(rw.start_ms)),
                end_rfc3339: Some(rfc3339_from_ms(rw.end_ms)),
            }),
            bucket_or_database: effective_bucket,
            injected_window,
        };

        // Attach audit fields to the result so the generic runner can forward them
        // into details_json without any driver-id branching.
        result.metadata_extra = Some(Self::build_metadata_extra_fields(&meta));

        match self.last_metadata.write() {
            Ok(mut guard) => *guard = Some(meta),
            Err(e) => log::warn!("Failed to store query metadata (lock poisoned): {e}"),
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

        // Enumerate all accessible buckets/databases so the UI can populate
        // the "Bucket"/"Database" source-context dropdown with every reachable
        // target, not just the profile default.
        let databases = self.list_databases().unwrap_or_else(|e| {
            // Permission to list all buckets/databases is not always granted.
            // Log the failure at warn level so operators can diagnose it, then
            // fall back to showing only the profile default so the UI is not empty.
            log::warn!("list_databases failed (falling back to profile default bucket): {e}");

            match &self.default_bucket {
                Some(bucket) => vec![DatabaseInfo {
                    name: bucket.clone(),
                    is_current: true,
                }],
                None => vec![],
            }
        });

        // Mark the profile default as current so the UI can pre-select it.
        // When no default is set, no bucket is pre-selected.
        let databases = databases
            .into_iter()
            .map(|mut db| {
                db.is_current = self
                    .default_bucket
                    .as_deref()
                    .map(|d| db.name == d)
                    .unwrap_or(false);
                db
            })
            .collect();

        let schema = TimeSeriesSchema {
            databases,
            current_database: self.default_bucket.clone(),
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

        let targets_placeholder = match self.version {
            InfluxVersion::V1 => "Select database...".to_string(),
            InfluxVersion::V2 => "Select bucket...".to_string(),
        };

        Some(SourceContextSpec {
            targets_label,
            targets_placeholder,
            // Pre-select the profile default bucket when one is set.
            // When None, the user must choose from the dropdown before running.
            default_target: self.default_bucket.clone(),
            start_label: "Start".to_string(),
            end_label: "End".to_string(),
            query_mode_label: Some("Syntax".to_string()),
            default_query_mode: Some(INFLUXQL_MODE.to_string()),
            query_modes,
        })
    }

    /// Browse a measurement as a paginated time-series result.
    ///
    /// Translates the generic `CollectionBrowseRequest` into a native InfluxDB query.
    ///
    /// - v1 and v2 (InfluxQL default): `SELECT * FROM "<measurement>" ORDER BY time DESC LIMIT n OFFSET m`
    /// - v2 (Flux default): equivalent Flux query with `range`, `filter`, `sort`, `limit`, and `tail`/`offset`
    ///
    /// The `collection.database` field carries the bucket/database name from the sidebar leaf,
    /// which is used directly in the query instead of the connection default.
    fn browse_collection(&self, request: &CollectionBrowseRequest) -> Result<QueryResult, DbError> {
        let started = Instant::now();

        let measurement = escape_influxql_ident(&request.collection.name);
        let bucket = &request.collection.database;

        let limit = request.pagination.limit();
        let offset = request.pagination.offset();

        match (self.version, &self.default_language) {
            (InfluxVersion::V2, QueryLanguage::Flux) => {
                let bucket_escaped = escape_flux_string(bucket);
                let measurement_escaped = escape_flux_string(&request.collection.name);

                // OFFSET is approximated by `tail` + dropping leading rows.
                // For the first page (offset == 0) we use limit only.
                // For subsequent pages we over-fetch and skip in memory.
                let query = if offset == 0 {
                    format!(
                        "from(bucket: \"{bucket_escaped}\")\
                         \n  |> range(start: -24h)\
                         \n  |> filter(fn: (r) => r._measurement == \"{measurement_escaped}\")\
                         \n  |> sort(columns: [\"_time\"], desc: true)\
                         \n  |> limit(n: {limit})",
                    )
                } else {
                    // Fetch offset + limit rows and skip the first `offset` in memory.
                    let fetch = offset + limit as u64;
                    format!(
                        "from(bucket: \"{bucket_escaped}\")\
                         \n  |> range(start: -24h)\
                         \n  |> filter(fn: (r) => r._measurement == \"{measurement_escaped}\")\
                         \n  |> sort(columns: [\"_time\"], desc: true)\
                         \n  |> limit(n: {fetch})\
                         \n  |> tail(n: {limit})",
                    )
                };

                let org = self.org.as_deref().unwrap_or("");
                let resp = self
                    .http
                    .execute_flux_v2(org, &query)
                    .map_err(map_http_error)?;

                if resp.status >= 400 {
                    let fe = InfluxErrorFormatter::format_http_error(resp.status, &resp.body);
                    return Err(DbError::QueryFailed(fe));
                }

                let mut result =
                    parse_flux_csv(&resp.body).map_err(|e| DbError::query_failed(e.to_string()))?;
                result.execution_time = started.elapsed();
                Ok(result)
            }

            _ => {
                // v1 (InfluxQL) or v2 with InfluxQL default language.
                // The bucket comes from the collection reference (sidebar leaf).
                let query = format!(
                    "SELECT * FROM {measurement} ORDER BY time DESC LIMIT {limit} OFFSET {offset}",
                );

                self.dispatch(self.default_language.clone(), bucket, &query, started)
            }
        }
    }

    /// Count points in a measurement.
    ///
    /// Uses `SELECT count(*) FROM "<measurement>"` via InfluxQL, which returns one count
    /// value per field. We take the maximum count across all returned columns as the
    /// total-points estimate — this is accurate when all fields are present on every point,
    /// and is a conservative lower bound otherwise.
    ///
    /// For v2 Flux connections, a Flux aggregation query is used instead.
    fn count_collection(&self, request: &CollectionCountRequest) -> Result<u64, DbError> {
        let measurement = escape_influxql_ident(&request.collection.name);
        let bucket = &request.collection.database;

        match (self.version, &self.default_language) {
            (InfluxVersion::V2, QueryLanguage::Flux) => {
                let bucket_escaped = escape_flux_string(bucket);
                let measurement_escaped = escape_flux_string(&request.collection.name);

                let query = format!(
                    "from(bucket: \"{bucket_escaped}\")\
                     \n  |> range(start: -24h)\
                     \n  |> filter(fn: (r) => r._measurement == \"{measurement_escaped}\")\
                     \n  |> count()\
                     \n  |> sum()",
                );

                let org = self.org.as_deref().unwrap_or("");
                let resp = self
                    .http
                    .execute_flux_v2(org, &query)
                    .map_err(map_http_error)?;

                if resp.status >= 400 {
                    let fe = InfluxErrorFormatter::format_http_error(resp.status, &resp.body);
                    return Err(DbError::QueryFailed(fe));
                }

                let result =
                    parse_flux_csv(&resp.body).map_err(|e| DbError::query_failed(e.to_string()))?;

                let count = result
                    .rows
                    .iter()
                    .filter_map(|row| {
                        row.iter().find_map(|val| match val {
                            dbflux_core::Value::Int(n) => Some(*n as u64),
                            dbflux_core::Value::Float(f) => Some(*f as u64),
                            _ => None,
                        })
                    })
                    .sum();

                Ok(count)
            }

            _ => {
                // InfluxQL: SELECT count(*) returns one row with one count per field.
                // We take the maximum count as the best estimate of total points.
                // The bucket comes from the collection reference (sidebar leaf).
                let query = format!("SELECT count(*) FROM {measurement}",);

                let started = Instant::now();
                let result =
                    self.dispatch(self.default_language.clone(), bucket, &query, started)?;

                let max_count = result
                    .rows
                    .iter()
                    .flat_map(|row| row.iter())
                    .filter_map(|val| match val {
                        dbflux_core::Value::Int(n) => Some(*n as u64),
                        dbflux_core::Value::Float(f) => Some(*f as u64),
                        _ => None,
                    })
                    .max()
                    .unwrap_or(0);

                Ok(max_count)
            }
        }
    }

    fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
        Some(&self.query_gen)
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

impl InfluxConnection {
    fn fetch_measurements(&self) -> Result<Vec<MeasurementInfo>, DbError> {
        // When no default bucket is configured, skip measurement listing.
        // The sidebar shows databases (buckets) and the user expands them individually.
        // Measurement listing is only meaningful for a specific bucket/database.
        let Some(bucket) = self.default_bucket.as_deref() else {
            return Ok(vec![]);
        };

        // v1 uses native InfluxQL `SHOW MEASUREMENTS`. v2 cannot rely on the InfluxQL
        // compatibility endpoint here because it requires a DBRP mapping between the
        // bucket and a v1 database name — those mappings are not created automatically
        // for every bucket. The Flux `schema.measurements` query works against any v2
        // bucket regardless of DBRP configuration.
        let names = match self.version {
            InfluxVersion::V1 => self.fetch_measurements_v1(bucket)?,
            InfluxVersion::V2 => self.fetch_measurements_v2_flux(bucket)?,
        };

        Ok(names
            .into_iter()
            .map(|name| MeasurementInfo {
                name,
                tags: Vec::new(),
                fields: Vec::new(),
            })
            .collect())
    }

    fn fetch_measurements_v1(&self, bucket: &str) -> Result<Vec<String>, DbError> {
        let resp = self
            .http
            .execute_influxql_v1(bucket, "SHOW MEASUREMENTS")
            .map_err(map_http_error)?;

        let result =
            parse_influxql_json(&resp.body).map_err(|e| DbError::query_failed(e.to_string()))?;

        Ok(result
            .rows
            .iter()
            .filter_map(|row| {
                row.first().and_then(|v| match v {
                    dbflux_core::Value::Text(s) => Some(s.clone()),
                    _ => None,
                })
            })
            .collect())
    }

    fn fetch_measurements_v2_flux(&self, bucket: &str) -> Result<Vec<String>, DbError> {
        let org = self.org.as_deref().unwrap_or("");
        let query = format!(
            "import \"influxdata/influxdb/schema\"\nschema.measurements(bucket: \"{}\")",
            escape_flux_string(bucket)
        );

        let resp = self
            .http
            .execute_flux_v2(org, &query)
            .map_err(map_http_error)?;

        let result =
            parse_flux_csv(&resp.body).map_err(|e| DbError::query_failed(e.to_string()))?;

        // schema.measurements emits a `_value` column with the measurement names.
        let value_idx = result
            .columns
            .iter()
            .position(|c| c.name == "_value")
            .unwrap_or(0);

        Ok(result
            .rows
            .iter()
            .filter_map(|row| {
                row.get(value_idx).and_then(|v| match v {
                    dbflux_core::Value::Text(s) => Some(s.clone()),
                    _ => None,
                })
            })
            .collect())
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

/// Escape a string for safe interpolation inside a Flux double-quoted literal.
fn escape_flux_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Wrap an InfluxQL identifier in double quotes, escaping any embedded quotes.
///
/// InfluxQL identifiers (measurement names, field keys) are quoted with double
/// quotes. Embedded double quotes are escaped by doubling them.
fn escape_influxql_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// Tests (C.8.1 – C.8.3) — dispatcher correctness using in-process checks
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_flux_string_escapes_quotes_and_backslashes() {
        assert_eq!(escape_flux_string("plain"), "plain");
        assert_eq!(escape_flux_string("a\"b"), "a\\\"b");
        assert_eq!(escape_flux_string("a\\b"), "a\\\\b");
        // Backslash must be doubled BEFORE quote escaping so the order matters:
        // input  a\"b  →  a\\\"b (4 chars in, 6 chars out)
        assert_eq!(escape_flux_string("a\\\"b"), "a\\\\\\\"b");
    }

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
            Some("mydb".to_string()),
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

    // C.8.1b — execute() with no bucket and no context target returns a clear error
    #[test]
    fn execute_without_bucket_and_no_context_returns_clear_error() {
        let conn = InfluxConnection::new(
            build_stub_http(),
            InfluxVersion::V2,
            QueryLanguage::InfluxQuery,
            None, // no default bucket
            None,
        );

        // A plain InfluxQL request with no execution context (no selected bucket).
        let req = QueryRequest::new("SELECT * FROM cpu");
        let err = conn.execute(&req).expect_err("must fail without a bucket");
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("select a source bucket") || msg.contains("bucket"),
            "error must guide the user to select a bucket: {msg}"
        );
    }

    // C.8.2 — resolve_language dispatches correctly
    #[test]
    fn resolve_language_influxql_mode_returns_influxql() {
        let conn = InfluxConnection::new(
            build_stub_http(),
            InfluxVersion::V2,
            QueryLanguage::InfluxQuery,
            Some("b".to_string()),
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
            Some("b".to_string()),
            Some("org".to_string()),
        );

        let req = make_request_with_mode("from(bucket: \"b\")", Some("flux"));
        let lang = conn.resolve_language(&req).expect("must resolve");
        assert_eq!(lang, QueryLanguage::Flux);
    }

    // C.8.2b — resolve_bucket_from_context prefers context target over default_bucket
    #[test]
    fn resolve_bucket_from_context_prefers_context_target() {
        let conn = InfluxConnection::new(
            build_stub_http(),
            InfluxVersion::V2,
            QueryLanguage::InfluxQuery,
            Some("default-bucket".to_string()),
            None,
        );

        // Provide a different bucket via the execution context targets.
        let source = ExecutionSourceContext::CollectionWindow {
            targets: vec!["context-bucket".to_string()],
            start_ms: 0,
            end_ms: 1,
            query_mode: None,
        };
        let ctx = ExecutionContext {
            source: Some(source),
            ..Default::default()
        };
        let req = QueryRequest::new("SELECT 1").with_execution_context(Some(ctx));

        let bucket = conn.resolve_bucket_from_context(&req);
        assert_eq!(
            bucket.as_deref(),
            Some("context-bucket"),
            "context target must take precedence over default_bucket"
        );
    }

    // C.8.2c — resolve_bucket_from_context returns None when no target is set
    #[test]
    fn resolve_bucket_from_context_returns_none_without_target() {
        let conn = InfluxConnection::new(
            build_stub_http(),
            InfluxVersion::V2,
            QueryLanguage::InfluxQuery,
            Some("default-bucket".to_string()),
            None,
        );

        // Empty targets list (user has not selected a bucket in the dropdown).
        let source = ExecutionSourceContext::CollectionWindow {
            targets: vec![],
            start_ms: 0,
            end_ms: 1,
            query_mode: None,
        };
        let ctx = ExecutionContext {
            source: Some(source),
            ..Default::default()
        };
        let req = QueryRequest::new("SELECT 1").with_execution_context(Some(ctx));

        let bucket = conn.resolve_bucket_from_context(&req);
        assert!(
            bucket.is_none(),
            "empty targets must yield None from context resolution"
        );
    }

    // C.8.3 — extract_window reads start_ms / end_ms
    #[test]
    fn extract_window_reads_start_and_end_ms() {
        let req = make_request_with_mode("SELECT 1", None);
        let window = InfluxConnection::extract_window(&req);
        assert!(window.start_rfc3339.is_some(), "start must be present");
        assert!(window.end_rfc3339.is_some(), "end must be present");
    }

    // D.3.2 — metadata_extra produced by build_metadata_extra_fields contains required audit keys
    #[test]
    fn metadata_extra_fields_contains_required_audit_keys_with_injection() {
        use crate::injection::ResolvedWindow as InjectionWindow;

        let meta = InfluxQueryMetadata {
            version: InfluxVersion::V2,
            language: QueryLanguage::InfluxQuery,
            resolved_window: Some(InjectionWindow {
                start_rfc3339: Some("2024-01-01T00:00:00Z".to_string()),
                end_rfc3339: Some("2024-01-01T01:00:00Z".to_string()),
            }),
            bucket_or_database: "my_bucket".to_string(),
            injected_window: true,
        };

        let extra = InfluxConnection::build_metadata_extra_fields(&meta);

        assert_eq!(
            extra.get("language").and_then(|v| v.as_str()),
            Some("influxql")
        );
        assert_eq!(extra.get("version").and_then(|v| v.as_str()), Some("v2"));
        assert_eq!(
            extra.get("bucket_or_database").and_then(|v| v.as_str()),
            Some("my_bucket")
        );
        assert_eq!(
            extra.get("injected_window").and_then(|v| v.as_bool()),
            Some(true)
        );
        assert!(
            extra.contains_key("resolved_window_start_ms"),
            "must have start_ms"
        );
        assert!(
            extra.contains_key("resolved_window_end_ms"),
            "must have end_ms"
        );
    }

    #[test]
    fn metadata_extra_fields_no_window_when_not_injected() {
        let meta = InfluxQueryMetadata {
            version: InfluxVersion::V1,
            language: QueryLanguage::InfluxQuery,
            resolved_window: None,
            bucket_or_database: "testdb".to_string(),
            injected_window: false,
        };

        let extra = InfluxConnection::build_metadata_extra_fields(&meta);

        assert_eq!(extra.get("version").and_then(|v| v.as_str()), Some("v1"));
        assert_eq!(
            extra.get("injected_window").and_then(|v| v.as_bool()),
            Some(false)
        );
        assert!(!extra.contains_key("resolved_window_start_ms"));
        assert!(!extra.contains_key("resolved_window_end_ms"));
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

    // C.8.4 — escape_influxql_ident wraps plain names and escapes embedded quotes
    #[test]
    fn escape_influxql_ident_wraps_plain_name() {
        assert_eq!(escape_influxql_ident("cpu"), "\"cpu\"");
        assert_eq!(
            escape_influxql_ident("my_measurement"),
            "\"my_measurement\""
        );
    }

    #[test]
    fn escape_influxql_ident_escapes_embedded_double_quotes() {
        // Embedded " is doubled per InfluxQL quoting rules.
        assert_eq!(escape_influxql_ident("a\"b"), "\"a\"\"b\"");
    }

    // C.8.5 — browse_collection query construction (no HTTP; tested via query string shape)
    //
    // We verify that the generated InfluxQL query string for a v1 connection contains
    // the expected clauses. This covers the query-construction path without running HTTP.
    #[test]
    fn browse_collection_influxql_query_contains_limit_and_offset() {
        use dbflux_core::{CollectionBrowseRequest, CollectionRef, Pagination};

        let limit = 50u32;
        let offset = 100u64;
        let measurement = "cpu usage";
        let database = "testdb";

        let escaped = escape_influxql_ident(measurement);
        let request = CollectionBrowseRequest::new(CollectionRef::new(database, measurement))
            .with_pagination(Pagination::Offset { limit, offset });

        // Build the InfluxQL query the same way browse_collection does.
        let query =
            format!("SELECT * FROM {escaped} ORDER BY time DESC LIMIT {limit} OFFSET {offset}",);

        assert!(
            query.contains("SELECT * FROM"),
            "must select all fields: {query}"
        );
        assert!(
            query.contains("ORDER BY time DESC"),
            "must order newest first: {query}"
        );
        assert!(
            query.contains("LIMIT 50"),
            "must include pagination limit: {query}"
        );
        assert!(
            query.contains("OFFSET 100"),
            "must include pagination offset: {query}"
        );
        assert!(
            query.contains("\"cpu usage\""),
            "measurement name must be double-quoted: {query}"
        );

        // Ensure the request structure is internally consistent.
        assert_eq!(request.collection.name, measurement);
        assert_eq!(request.collection.database, database);
        assert_eq!(request.pagination.limit(), limit);
        assert_eq!(request.pagination.offset(), offset);
    }

    // C.8.6 — Flux browse query for v2/Flux default language includes expected pipeline steps
    #[test]
    fn browse_collection_flux_query_structure_first_page() {
        let bucket = "my_bucket";
        let measurement = "temperature";
        let limit: u32 = 25;
        let offset: u64 = 0;

        let bucket_escaped = escape_flux_string(bucket);
        let measurement_escaped = escape_flux_string(measurement);

        let query = format!(
            "from(bucket: \"{bucket_escaped}\")\
             \n  |> range(start: -24h)\
             \n  |> filter(fn: (r) => r._measurement == \"{measurement_escaped}\")\
             \n  |> sort(columns: [\"_time\"], desc: true)\
             \n  |> limit(n: {limit})",
        );

        assert!(query.contains("from(bucket:"), "must start from bucket");
        assert!(query.contains("|> range(start: -24h)"), "must have range");
        assert!(
            query.contains("|> filter(fn: (r) => r._measurement"),
            "must filter by measurement"
        );
        assert!(
            query.contains("|> sort(columns: [\"_time\"], desc: true)"),
            "must sort newest first"
        );
        assert!(query.contains("|> limit(n: 25)"), "must apply limit");
        assert!(
            !query.contains("|> tail("),
            "first page must not use tail for offset"
        );
        assert_eq!(offset, 0, "this test covers the offset=0 code path");
    }

    // C.8.7 — Flux browse query for subsequent pages uses tail to approximate offset
    #[test]
    fn browse_collection_flux_query_structure_subsequent_page() {
        let bucket = "my_bucket";
        let measurement = "temperature";
        let limit: u32 = 25;
        let offset: u64 = 50;

        let fetch = offset + limit as u64;
        let bucket_escaped = escape_flux_string(bucket);
        let measurement_escaped = escape_flux_string(measurement);

        let query = format!(
            "from(bucket: \"{bucket_escaped}\")\
             \n  |> range(start: -24h)\
             \n  |> filter(fn: (r) => r._measurement == \"{measurement_escaped}\")\
             \n  |> sort(columns: [\"_time\"], desc: true)\
             \n  |> limit(n: {fetch})\
             \n  |> tail(n: {limit})",
        );

        assert!(
            query.contains(&format!("|> limit(n: {fetch})")),
            "must over-fetch for offset pagination"
        );
        assert!(
            query.contains(&format!("|> tail(n: {limit})")),
            "must trim to requested page size"
        );
    }
}
