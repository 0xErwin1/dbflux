//! Chart data-source seam: trait, error type, concrete implementations,
//! and the resolver factory.
//!
//! # Design rationale
//!
//! `ChartDataSource::build_request` is a pure, synchronous transform that turns
//! a source description + an optional time window into a `dbflux_core::QueryRequest`.
//! The trait deliberately contains NO async code and holds NO connection handle.
//! This keeps `dbflux_components` free of GPUI / executor dependencies and makes
//! every implementation unit-testable without a runtime.
//!
//! Connection resolution and `conn.execute` stay in `ChartDocument`, exactly as
//! they are today.

use crate::saved_chart::SavedChartSource;
use dbflux_core::{CollectionRef, ExecutionContext, ExecutionSourceContext, QueryRequest};

// ---------------------------------------------------------------------------
// TimeWindow
// ---------------------------------------------------------------------------

/// Caller-supplied time bounds for a chart pull.
///
/// Distinct from [`dbflux_core::ResolvedWindow`], which is a **driver-output**
/// type (carries `language`) reporting the effective window after execution.
/// `TimeWindow` is the **caller-input** type: the raw `(start_ms, end_ms)` pair
/// that `ChartDocument` holds in `pending_time_window`. `None` = no active window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeWindow {
    pub start_ms: i64,
    pub end_ms: i64,
}

// ---------------------------------------------------------------------------
// ChartSourceError
// ---------------------------------------------------------------------------

/// Errors that `ChartDataSource::build_request` may return.
#[derive(Debug)]
pub enum ChartSourceError {
    /// The source contains no query text (empty string after trimming).
    EmptyQuery,
    /// A collection source was asked to build a request without a time window.
    ///
    /// `ExecutionSourceContext::CollectionWindow` requires concrete `start_ms`/`end_ms`
    /// values (both `i64`, not `Option`) so "collection without window" cannot be
    /// represented. Callers must supply a `TimeWindow` before executing a collection
    /// source.
    WindowRequired,
    /// A source-specific error, e.g. a malformed configuration.
    Source(String),
}

impl std::fmt::Display for ChartSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChartSourceError::EmptyQuery => write!(f, "chart query is empty"),
            ChartSourceError::WindowRequired => {
                write!(f, "a collection source requires a time window")
            }
            ChartSourceError::Source(msg) => write!(f, "chart source error: {msg}"),
        }
    }
}

// ---------------------------------------------------------------------------
// ChartDataSource trait
// ---------------------------------------------------------------------------

/// A re-pullable chart data source.
///
/// Given an optional time window, produces a `QueryRequest` ready to hand to
/// `conn.execute`. Object-safe so `ChartDocument` can hold
/// `Box<dyn ChartDataSource>` without knowing the concrete kind at execution
/// sites.
pub trait ChartDataSource: Send + 'static {
    /// Build the execution request for this source and time window.
    ///
    /// Pure and synchronous — no IO, no GPUI context. The async
    /// `conn.execute` call remains in `ChartDocument`.
    fn build_request(
        &self,
        window: Option<TimeWindow>,
    ) -> Result<QueryRequest, ChartSourceError>;
}

// ---------------------------------------------------------------------------
// QuerySource
// ---------------------------------------------------------------------------

/// A query-string chart source.
///
/// Holds the raw query text; the driver executes it. When a `TimeWindow` is
/// present the request carries a `CollectionWindow` execution context so
/// drivers can inject time bounds when no explicit WHERE predicate is present.
pub(crate) struct QuerySource {
    query: String,
}

impl QuerySource {
    pub(crate) fn new(query: String) -> Self {
        Self { query }
    }
}

impl ChartDataSource for QuerySource {
    fn build_request(
        &self,
        window: Option<TimeWindow>,
    ) -> Result<QueryRequest, ChartSourceError> {
        let q = self.query.trim();

        if q.is_empty() {
            return Err(ChartSourceError::EmptyQuery);
        }

        // When a window is active, attach a CollectionWindow source context so
        // the driver can inject time bounds into queries without explicit WHERE
        // time predicates. Mirrors the inline logic in ChartDocument::request_reexecute
        // (chart_document/mod.rs lines ~350-362) exactly, preserving behavior.
        let exec_ctx = window.map(|w| ExecutionContext {
            source: Some(ExecutionSourceContext::CollectionWindow {
                targets: Vec::new(),
                start_ms: w.start_ms,
                end_ms: w.end_ms,
                query_mode: None,
            }),
            ..ExecutionContext::default()
        });

        Ok(QueryRequest::new(q.to_string()).with_execution_context(exec_ctx))
    }
}

// ---------------------------------------------------------------------------
// CollectionSource
// ---------------------------------------------------------------------------

/// A collection-browse chart source (MongoDB collection, InfluxDB measurement, etc.).
///
/// Derives the query target from `collection_ref` generically — no driver_id
/// branching. The driver interprets the `CollectionWindow` source context.
///
/// A time window is **required**: `ExecutionSourceContext::CollectionWindow` mandates
/// concrete `start_ms`/`end_ms` values (`i64`, not `Option`), so there is no
/// representation for "collection without window". Callers must supply a window;
/// `build_request(None)` returns `Err(ChartSourceError::WindowRequired)`.
///
/// Note: `ChartDocument` does NOT route `Collection` sources here yet — collection
/// charts still open via `DataDocument`. This implementation completes the two-kind
/// seam contract and is exercised by unit tests only in W0.
pub(crate) struct CollectionSource {
    collection_ref: CollectionRef,
}

impl ChartDataSource for CollectionSource {
    fn build_request(
        &self,
        window: Option<TimeWindow>,
    ) -> Result<QueryRequest, ChartSourceError> {
        // A window is mandatory: CollectionWindow requires concrete start/end bounds.
        let w = window.ok_or(ChartSourceError::WindowRequired)?;

        // Derive target generically from the collection name.
        // No driver_id branching — the driver interprets the CollectionWindow context.
        let targets = vec![self.collection_ref.name.clone()];

        let exec_ctx = ExecutionContext {
            source: Some(ExecutionSourceContext::CollectionWindow {
                targets,
                start_ms: w.start_ms,
                end_ms: w.end_ms,
                query_mode: None,
            }),
            ..ExecutionContext::default()
        };

        Ok(QueryRequest::new(String::new()).with_execution_context(Some(exec_ctx)))
    }
}

// ---------------------------------------------------------------------------
// Resolver factory
// ---------------------------------------------------------------------------

/// Map a `SavedChartSource` to its runtime `ChartDataSource` implementation.
///
/// This is the **single** place where the source kind is matched. All execution
/// sites hold `Box<dyn ChartDataSource>` and call `build_request` without
/// knowing the concrete kind.
///
/// # Extensibility
///
/// To add a new source kind:
/// 1. Add a `SavedChartSource` variant in `dbflux_components::saved_chart`.
/// 2. Implement `ChartDataSource` for the new type.
/// 3. Add one arm here.
///
/// No edits to `dbflux_ui` are required.
pub fn resolve_source(source: &SavedChartSource) -> Box<dyn ChartDataSource> {
    match source {
        // EXTENSION POINT: add new SavedChartSource variants here.
        SavedChartSource::Query { query } => Box::new(QuerySource::new(query.clone())),
        SavedChartSource::Collection {
            collection_ref, ..
        } => Box::new(CollectionSource {
            collection_ref: collection_ref.clone(),
        }),
    }
}

// ---------------------------------------------------------------------------
// Tests (TDD — written before implementation was finalised)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Task 1.8: QuerySource tests ---

    /// S-01 / R-04: empty query string must return EmptyQuery error.
    #[test]
    fn query_source_empty_query_returns_err_empty_query() {
        let src = QuerySource::new(String::new());
        let result = src.build_request(None);

        assert!(
            matches!(result, Err(ChartSourceError::EmptyQuery)),
            "expected EmptyQuery, got: {:?}",
            result
        );
    }

    /// S-01 / R-04: whitespace-only query must also be treated as empty.
    #[test]
    fn query_source_whitespace_only_returns_err_empty_query() {
        let src = QuerySource::new("   ".to_string());
        let result = src.build_request(None);

        assert!(
            matches!(result, Err(ChartSourceError::EmptyQuery)),
            "expected EmptyQuery for whitespace-only query"
        );
    }

    /// S-03 / R-04: a query with a window must produce a request carrying
    /// CollectionWindow context with the correct start_ms, end_ms, and empty
    /// targets (query sources do not pre-populate targets).
    #[test]
    fn query_source_with_window_produces_collection_window_context() {
        let src = QuerySource::new("SELECT * FROM metrics".to_string());
        let window = TimeWindow {
            start_ms: 1_000,
            end_ms: 2_000,
        };

        let request = src
            .build_request(Some(window))
            .expect("should produce Ok request");

        let ctx = request
            .execution_context
            .as_ref()
            .expect("request must carry an execution context");

        match ctx.source.as_ref().expect("source must be Some") {
            ExecutionSourceContext::CollectionWindow {
                targets,
                start_ms,
                end_ms,
                query_mode,
            } => {
                assert_eq!(*start_ms, 1_000);
                assert_eq!(*end_ms, 2_000);
                assert!(targets.is_empty(), "QuerySource targets must be empty");
                assert!(query_mode.is_none(), "query_mode must be None");
            }
        }
    }

    /// S-04 / R-04: a query without a window must produce no source context.
    #[test]
    fn query_source_without_window_produces_no_source_context() {
        let src = QuerySource::new("SELECT 1".to_string());

        let request = src
            .build_request(None)
            .expect("should produce Ok request");

        // Either no context at all, or context with source = None.
        let has_source = request
            .execution_context
            .as_ref()
            .and_then(|c| c.source.as_ref())
            .is_some();

        assert!(
            !has_source,
            "request must carry no source context when window is None"
        );
    }

    // --- Task 1.9: resolver tests ---

    /// S-02 / R-02: resolver Query arm produces a QuerySource (verified by
    /// confirming that build_request behaves as QuerySource does).
    #[test]
    fn resolver_query_arm_returns_query_source_behaviour() {
        let saved = SavedChartSource::Query {
            query: "SELECT 1".to_string(),
        };
        let source = resolve_source(&saved);

        // A QuerySource with a non-empty query must succeed.
        let request = source
            .build_request(None)
            .expect("Query resolver arm must succeed for non-empty query");

        // And an empty-query source must return EmptyQuery.
        let empty_saved = SavedChartSource::Query {
            query: String::new(),
        };
        let empty_source = resolve_source(&empty_saved);
        assert!(
            matches!(
                empty_source.build_request(None),
                Err(ChartSourceError::EmptyQuery)
            ),
            "empty query via resolver must return EmptyQuery"
        );

        // Silence unused variable warning — the request is the observable output.
        let _ = request;
    }

    /// S-02 / R-02: resolver Collection arm produces a CollectionSource (verified
    /// by confirming that build_request carries the collection name in targets).
    #[test]
    fn resolver_collection_arm_returns_collection_source_behaviour() {
        let collection_ref = CollectionRef::new("mydb", "measurements");
        let saved = SavedChartSource::Collection {
            collection_ref: collection_ref.clone(),
            time_window: None,
        };
        let source = resolve_source(&saved);

        let window = TimeWindow {
            start_ms: 0,
            end_ms: 1,
        };
        let request = source
            .build_request(Some(window))
            .expect("Collection resolver arm must succeed");

        let ctx = request
            .execution_context
            .as_ref()
            .expect("must carry execution context");

        match ctx.source.as_ref().expect("source must be Some") {
            ExecutionSourceContext::CollectionWindow { targets, .. } => {
                assert!(
                    targets.contains(&"measurements".to_string()),
                    "targets must contain the collection name"
                );
            }
        }
    }

    // --- Task 3.2: CollectionSource finalized tests ---

    /// S-02 / R-05: collection source with a window must forward the collection name
    /// as the single target, carry the exact start_ms/end_ms values, and leave
    /// query_mode as None.
    #[test]
    fn collection_source_with_window_carries_collection_ref_in_targets() {
        let src = CollectionSource {
            collection_ref: CollectionRef::new("influxdb", "cpu_metrics"),
        };
        let window = TimeWindow {
            start_ms: 1_700_000_000_000,
            end_ms: 1_700_003_600_000,
        };

        let request = src
            .build_request(Some(window))
            .expect("should produce Ok request with a window");

        let ctx = request
            .execution_context
            .as_ref()
            .expect("request must carry an execution context");

        match ctx.source.as_ref().expect("source must be Some") {
            ExecutionSourceContext::CollectionWindow {
                targets,
                start_ms,
                end_ms,
                query_mode,
            } => {
                assert_eq!(
                    targets,
                    &vec!["cpu_metrics".to_string()],
                    "targets must be exactly [collection_ref.name]"
                );
                assert_eq!(*start_ms, 1_700_000_000_000, "start_ms must be forwarded");
                assert_eq!(*end_ms, 1_700_003_600_000, "end_ms must be forwarded");
                assert!(query_mode.is_none(), "query_mode must be None");
            }
        }
    }

    /// S-02 / R-05: collection source without a window must return WindowRequired.
    /// A collection source cannot represent "no window" because CollectionWindow
    /// mandates concrete start_ms/end_ms (i64, not Option).
    #[test]
    fn collection_source_without_window_returns_err_window_required() {
        let src = CollectionSource {
            collection_ref: CollectionRef::new("influxdb", "cpu_metrics"),
        };

        let result = src.build_request(None);

        assert!(
            matches!(result, Err(ChartSourceError::WindowRequired)),
            "expected WindowRequired error when window is None, got: {:?}",
            result
        );
    }
}
