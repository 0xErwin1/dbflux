//! Chart data-source seam: trait, error type, concrete implementations,
//! and the resolver factory.
//!
//! # Design rationale
//!
//! `ChartDataSource::build_plan` is a pure, synchronous transform that turns
//! a source description + an optional time window into a [`ChartDataPlan`].
//! The trait deliberately contains NO async code and holds NO connection handle.
//! This keeps `dbflux_components` free of GPUI / executor dependencies and makes
//! every implementation unit-testable without a runtime.
//!
//! Connection resolution, driver execution, and local-store aggregation all stay
//! in the host crate (`dbflux_ui`), not here.

use crate::saved_chart::{MetricSeries, SavedChartSource};
use dbflux_core::{
    CollectionRef, ExecutionContext, ExecutionSourceContext, MetricQuerySeries, QueryRequest,
};

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

/// Errors that `ChartDataSource::build_plan` may return.
#[derive(Debug)]
pub enum ChartSourceError {
    /// The source contains no query text (empty string after trimming).
    EmptyQuery,
    /// A collection source was asked to build a plan without a time window.
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
                write!(f, "this chart source requires a time window")
            }
            ChartSourceError::Source(msg) => write!(f, "chart source error: {msg}"),
        }
    }
}

// ---------------------------------------------------------------------------
// AuditGroupBy + AuditAggregateSpec (pure value types, no IO, no GPUI)
// ---------------------------------------------------------------------------

/// Which audit column to group by in an aggregate chart.
///
/// A closed enum ensures the host can only request a SQL-safe, fixed column
/// name — no user-supplied string ever reaches the SQL layer as an identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditGroupBy {
    Category,
    Outcome,
    Level,
}

/// Declarative parameters for an audit-aggregate chart pull.
///
/// Pure value type: NO IO, NO store handle, NO GPUI. The host executor
/// (`dbflux_ui`) converts this into a `dbflux_audit::AuditAggregateParams`
/// and drives the actual aggregation query.
///
/// Filter facets are carried as plain `Vec<String>` / `Option<String>` — no
/// storage types leak into `dbflux_components`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditAggregateSpec {
    /// Bucket width in milliseconds. Must be > 0; the host executor guards this.
    pub bucket_ms: i64,
    /// Column to group by.
    pub group_by: AuditGroupBy,
    /// Optional inclusive start bound (epoch ms). `None` = open-ended start.
    pub start_ms: Option<i64>,
    /// Optional exclusive end bound (epoch ms). `None` = open-ended end.
    pub end_ms: Option<i64>,
    /// Event category filter; empty = no filter.
    pub categories: Vec<String>,
    /// Severity/level filter; empty = no filter.
    pub levels: Vec<String>,
    /// Outcome filter; empty = no filter.
    pub outcomes: Vec<String>,
    /// Free-text search; `None` = no filter.
    pub free_text: Option<String>,
}

// ---------------------------------------------------------------------------
// ChartDataPlan
// ---------------------------------------------------------------------------

/// Declarative description of how to obtain a chart `QueryResult`.
///
/// Built purely and synchronously by [`ChartDataSource::build_plan`]. The
/// host executor matches on this enum to dispatch to the appropriate substrate:
/// - `Driver` → resolve a connection and call `conn.execute`.
/// - `LocalAudit` → call `AuditService::aggregate` on the local store.
///
/// No IO or GPUI context is required to construct a plan.
#[derive(Debug)]
pub enum ChartDataPlan {
    /// Execute through a driver connection (existing W0 behavior).
    Driver(QueryRequest),
    /// Aggregate the local audit SQLite store.
    LocalAudit(AuditAggregateSpec),
}

// ---------------------------------------------------------------------------
// ChartSourceDescription
// ---------------------------------------------------------------------------

/// Human-readable metadata about a chart data source.
///
/// Used by `ChartDocument::set_data_source` to update the tab title when a
/// new source is installed. All fields are optional so that implementations
/// can provide only the information they know about.
#[derive(Debug, Clone, Default)]
pub struct ChartSourceDescription {
    /// Optional display title for the chart tab.
    ///
    /// When `Some`, `ChartDocument` updates its `title` field to this value.
    /// When `None`, the existing title is kept.
    pub title: Option<String>,
}

impl ChartSourceDescription {
    /// Returns a description with no information — used when a source has
    /// no displayable metadata.
    pub fn empty() -> Self {
        Self { title: None }
    }

    /// Returns the display title, if any.
    pub fn display_title(&self) -> Option<String> {
        self.title.clone()
    }
}

// ---------------------------------------------------------------------------
// ChartDataSource trait
// ---------------------------------------------------------------------------

/// A re-pullable chart data source.
///
/// Given an optional time window, produces a [`ChartDataPlan`] that describes
/// — without executing — how the chart result should be obtained. Object-safe
/// so `ChartDocument` can hold `Box<dyn ChartDataSource>` without knowing the
/// concrete kind at execution sites.
pub trait ChartDataSource: Send + 'static {
    /// Build the execution plan for this source and time window.
    ///
    /// Pure and synchronous — no IO, no GPUI context. All execution (driver
    /// round-trips, store aggregation) stays in the host crate.
    fn build_plan(&self, window: Option<TimeWindow>) -> Result<ChartDataPlan, ChartSourceError>;

    /// Returns human-readable metadata about this source.
    ///
    /// Used by `ChartDocument::set_data_source` to update the tab title.
    /// Default implementation returns an empty description so existing
    /// implementations do not need to opt in.
    fn describe(&self) -> ChartSourceDescription {
        ChartSourceDescription::empty()
    }

    /// Clone this source into a new heap-allocated box.
    ///
    /// Needed because `Clone` is not object-safe. The picker emits a
    /// `ChartShellEvent::MetricPickerApplied(Box<dyn ChartDataSource>)` and
    /// `ChartDocument` stashes the source in a `pending_data_source` field;
    /// `clone_box` lets that field be cloned without knowing the concrete type.
    fn clone_box(&self) -> Box<dyn ChartDataSource>;

    /// Expose the concrete type as `Any` for downcasting.
    ///
    /// Required by the `DocumentKey::MetricChart` deduplication path: the
    /// `matches_dedup_key` closure downcasts `&dyn ChartDataSource` to
    /// `&MetricSource` to compare `(namespace, metric_name)`.
    ///
    /// Default implementation always returns `None`; only `MetricSource`
    /// overrides it.
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    /// Returns `true` when the source is self-contained and auto-executes
    /// without requiring the user to type or confirm a query.
    ///
    /// `MetricSource` returns `true`: clicking a metric leaf auto-runs the chart.
    /// `QuerySource` returns `false` (default) because the chart stays idle
    /// until the user provides a query or presses Run.
    ///
    /// Used by render code to select the appropriate empty-state copy:
    /// - `is_self_executing() == true` + idle/empty → "No data points for the selected window"
    /// - `is_self_executing() == false` → "Run the query to populate the chart."
    fn is_self_executing(&self) -> bool {
        false
    }

    /// Returns `true` when this source produces a single instantaneous sample
    /// per fetch and requires the chart host to accumulate samples into a time
    /// series rather than replacing the display on each fetch.
    ///
    /// `InstanceMetricSource` returns `true` — drivers return one row per poll
    /// (the current gauge value). All other sources return full series natively
    /// and must return `false` (the default) so that `ChartDocument` does not
    /// accidentally buffer their already-complete results.
    fn is_accumulating(&self) -> bool {
        false
    }
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
    fn clone_box(&self) -> Box<dyn ChartDataSource> {
        Box::new(QuerySource {
            query: self.query.clone(),
        })
    }

    fn build_plan(&self, window: Option<TimeWindow>) -> Result<ChartDataPlan, ChartSourceError> {
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

        let request = QueryRequest::new(q.to_string()).with_execution_context(exec_ctx);

        Ok(ChartDataPlan::Driver(request))
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
/// `build_plan(None)` returns `Err(ChartSourceError::WindowRequired)`.
///
/// Note: `ChartDocument` does NOT route `Collection` sources here yet — collection
/// charts still open via `DataDocument`. This implementation completes the two-kind
/// seam contract and is exercised by unit tests only in W0.
pub(crate) struct CollectionSource {
    collection_ref: CollectionRef,
}

impl ChartDataSource for CollectionSource {
    fn clone_box(&self) -> Box<dyn ChartDataSource> {
        Box::new(CollectionSource {
            collection_ref: self.collection_ref.clone(),
        })
    }

    fn build_plan(&self, window: Option<TimeWindow>) -> Result<ChartDataPlan, ChartSourceError> {
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

        let request = QueryRequest::new(String::new()).with_execution_context(Some(exec_ctx));

        Ok(ChartDataPlan::Driver(request))
    }
}

// ---------------------------------------------------------------------------
// MetricSource
// ---------------------------------------------------------------------------

/// A CloudWatch metrics chart source.
///
/// Holds the five user-visible parameters that identify a CloudWatch metric
/// series. The time window (`start_ms`, `end_ms`) is NOT stored here — it is
/// supplied by the chart engine at `build_plan` time and embedded into the
/// resulting `MetricQuery` execution context.
///
/// A time window is **required**: `GetMetricData` mandates explicit `StartTime`
/// and `EndTime`, so there is no representation for "metrics without window".
/// `build_plan(None)` returns `Err(ChartSourceError::WindowRequired)`.
///
/// `MetricSource` is constructed directly by the UI entry point (not via
/// `SavedChartSource`) — same pattern as `AuditSource`. Carries one or more
/// `MetricSeries`; the driver issues a single GetMetricData batching every
/// series and the chart receives one Y column per series.
#[derive(Clone)]
pub struct MetricSource {
    /// Non-empty list of series. A single-metric chart holds exactly one entry.
    pub series: Vec<MetricSeries>,
}

impl MetricSource {
    /// Convenience constructor for a single-series source — the common case
    /// when a user clicks a metric leaf in the sidebar.
    pub fn single(
        namespace: String,
        metric_name: String,
        dimensions: Vec<(String, String)>,
        period_seconds: u32,
        statistic: String,
    ) -> Self {
        Self {
            series: vec![MetricSeries {
                namespace,
                metric_name,
                dimensions,
                period_seconds,
                statistic,
                region: None,
                label: None,
            }],
        }
    }

    /// Returns the namespace of the first series, or `""` when empty.
    /// Used by sidebar dedup to keep a stable identity even after the picker
    /// rebuilds the source value.
    pub fn primary_namespace(&self) -> &str {
        self.series
            .first()
            .map(|s| s.namespace.as_str())
            .unwrap_or("")
    }

    /// Returns the metric name of the first series, or `""` when empty.
    pub fn primary_metric_name(&self) -> &str {
        self.series
            .first()
            .map(|s| s.metric_name.as_str())
            .unwrap_or("")
    }
}

impl ChartDataSource for MetricSource {
    fn clone_box(&self) -> Box<dyn ChartDataSource> {
        Box::new(self.clone())
    }

    fn describe(&self) -> ChartSourceDescription {
        let title = match self.series.as_slice() {
            [] => String::new(),
            [single] => format!("{} / {}", single.namespace, single.metric_name),
            [first, ..] => format!(
                "{} / {} (+{} more)",
                first.namespace,
                first.metric_name,
                self.series.len() - 1
            ),
        };
        ChartSourceDescription { title: Some(title) }
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    fn is_self_executing(&self) -> bool {
        true
    }

    fn build_plan(&self, window: Option<TimeWindow>) -> Result<ChartDataPlan, ChartSourceError> {
        // GetMetricData requires explicit StartTime/EndTime — window is mandatory.
        let w = window.ok_or(ChartSourceError::WindowRequired)?;

        if self.series.is_empty() {
            return Err(ChartSourceError::Source(
                "MetricSource has no series".to_string(),
            ));
        }

        let series = self
            .series
            .iter()
            .map(|s| MetricQuerySeries {
                namespace: s.namespace.clone(),
                metric_name: s.metric_name.clone(),
                dimensions: s.dimensions.clone(),
                period_s: s.period_seconds,
                statistic: s.statistic.clone(),
                label: s.label.clone(),
            })
            .collect();

        let exec_ctx = ExecutionContext {
            source: Some(ExecutionSourceContext::MetricQuery {
                series,
                start_ms: w.start_ms,
                end_ms: w.end_ms,
            }),
            ..ExecutionContext::default()
        };

        let request = QueryRequest::new(String::new()).with_execution_context(Some(exec_ctx));

        Ok(ChartDataPlan::Driver(request))
    }
}

// ---------------------------------------------------------------------------
// InstanceMetricSource
// ---------------------------------------------------------------------------

/// A per-driver instance metric source resolved from `SavedChartSource::InstanceMetric`.
///
/// Builds a `ChartDataPlan::Driver` request carrying `InstanceMetricQuery`.
/// A time window is required; returns `ChartSourceError::WindowRequired` when
/// no window is supplied.
#[derive(Debug, Clone)]
pub struct InstanceMetricSource {
    pub metric_id: String,
}

impl ChartDataSource for InstanceMetricSource {
    fn clone_box(&self) -> Box<dyn ChartDataSource> {
        Box::new(self.clone())
    }

    fn describe(&self) -> ChartSourceDescription {
        ChartSourceDescription {
            title: Some(self.metric_id.clone()),
        }
    }

    fn is_self_executing(&self) -> bool {
        true
    }

    fn is_accumulating(&self) -> bool {
        true
    }

    fn build_plan(&self, window: Option<TimeWindow>) -> Result<ChartDataPlan, ChartSourceError> {
        let w = window.ok_or(ChartSourceError::WindowRequired)?;

        let exec_ctx = ExecutionContext {
            source: Some(ExecutionSourceContext::InstanceMetricQuery {
                metric_id: self.metric_id.clone(),
                start_ms: w.start_ms,
                end_ms: w.end_ms,
            }),
            ..ExecutionContext::default()
        };

        let request = QueryRequest::new(String::new()).with_execution_context(Some(exec_ctx));

        Ok(ChartDataPlan::Driver(request))
    }
}

// ---------------------------------------------------------------------------
// AuditSource
// ---------------------------------------------------------------------------

/// A local-audit chart source that aggregates the DBFlux audit SQLite store.
///
/// Returns `ChartDataPlan::LocalAudit(spec)` — no IO, no connection, no store
/// handle. The host executor in `dbflux_ui` performs the actual aggregation via
/// `dbflux_audit::AuditService::aggregate`.
///
/// When a `TimeWindow` is provided its bounds override `spec.start_ms`/`end_ms`.
///
/// `AuditSource` is NOT registered in `resolve_source` because audit charts are
/// ephemeral (not `SavedChartSource`-backed). `AuditDocument` constructs this
/// directly in Slice 3.
pub struct AuditSource {
    pub spec: AuditAggregateSpec,
}

impl ChartDataSource for AuditSource {
    fn clone_box(&self) -> Box<dyn ChartDataSource> {
        Box::new(AuditSource {
            spec: self.spec.clone(),
        })
    }

    fn build_plan(&self, window: Option<TimeWindow>) -> Result<ChartDataPlan, ChartSourceError> {
        let mut spec = self.spec.clone();

        // Window overrides the spec's time bounds when supplied by the caller.
        if let Some(w) = window {
            spec.start_ms = Some(w.start_ms);
            spec.end_ms = Some(w.end_ms);
        }

        Ok(ChartDataPlan::LocalAudit(spec))
    }
}

// ---------------------------------------------------------------------------
// Resolver factory
// ---------------------------------------------------------------------------

/// Map a `SavedChartSource` to its runtime `ChartDataSource` implementation.
///
/// This is the **single** place where the source kind is matched for SAVED
/// sources. All execution sites hold `Box<dyn ChartDataSource>` and call
/// `build_plan` without knowing the concrete kind.
///
/// Ephemeral (non-saved) sources such as `AuditSource` are constructed
/// directly by the host document (e.g. `AuditDocument`) and are NOT routed
/// through this factory.
///
/// # Extensibility
///
/// To add a new saved source kind:
/// 1. Add a `SavedChartSource` variant in `dbflux_components::saved_chart`.
/// 2. Implement `ChartDataSource` for the new concrete struct in this file
///    (see `QuerySource` and `CollectionSource` as examples).
/// 3. Add one `match` arm below, mapping the new variant to `Box::new(<NewSource>)`.
///
/// No changes to `dbflux_ui` are required. `ChartDocument::request_reexecute`
/// calls `self.data_source.build_plan(window)` without inspecting the kind.
pub fn resolve_source(source: &SavedChartSource) -> Box<dyn ChartDataSource> {
    match source {
        // EXTENSION POINT: add new SavedChartSource variants here.
        SavedChartSource::Query { query } => Box::new(QuerySource::new(query.clone())),
        SavedChartSource::Collection { collection_ref, .. } => Box::new(CollectionSource {
            collection_ref: collection_ref.clone(),
        }),
        SavedChartSource::Metric { series } => Box::new(MetricSource {
            series: series.clone(),
        }),
        SavedChartSource::InstanceMetric { metric_id } => Box::new(InstanceMetricSource {
            metric_id: metric_id.clone(),
        }),
    }
}

// ---------------------------------------------------------------------------
// Tests (TDD — written before implementation was finalised)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Display tests ---

    /// ChartSourceError::WindowRequired must display a source-agnostic message
    /// (not "collection source") so it reads correctly for MetricSource too.
    #[test]
    fn window_required_display_is_source_agnostic() {
        let msg = format!("{}", ChartSourceError::WindowRequired);
        assert_eq!(
            msg, "this chart source requires a time window",
            "WindowRequired Display must be source-agnostic"
        );
    }

    // --- QuerySource tests ---

    /// S-01 / R-04: empty query string must return EmptyQuery error.
    #[test]
    fn query_source_empty_query_returns_err_empty_query() {
        let src = QuerySource::new(String::new());
        let result = src.build_plan(None);

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
        let result = src.build_plan(None);

        assert!(
            matches!(result, Err(ChartSourceError::EmptyQuery)),
            "expected EmptyQuery for whitespace-only query"
        );
    }

    /// S-03 / R-04: a query with a window must produce a Driver plan carrying
    /// CollectionWindow context with the correct start_ms, end_ms, and empty
    /// targets (query sources do not pre-populate targets).
    #[test]
    fn query_source_with_window_produces_driver_plan_with_collection_window_context() {
        let src = QuerySource::new("SELECT * FROM metrics".to_string());
        let window = TimeWindow {
            start_ms: 1_000,
            end_ms: 2_000,
        };

        let plan = src
            .build_plan(Some(window))
            .expect("should produce Ok plan");

        let ChartDataPlan::Driver(request) = plan else {
            panic!("expected ChartDataPlan::Driver, got LocalAudit");
        };

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
            other => panic!("expected CollectionWindow source context, got: {other:?}"),
        }
    }

    /// S-04 / R-04: a query without a window must produce a Driver plan with no
    /// source context.
    #[test]
    fn query_source_without_window_produces_driver_plan_with_no_source_context() {
        let src = QuerySource::new("SELECT 1".to_string());

        let plan = src.build_plan(None).expect("should produce Ok plan");

        let ChartDataPlan::Driver(request) = plan else {
            panic!("expected ChartDataPlan::Driver");
        };

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

    // --- Resolver tests ---

    /// S-02 / R-02: resolver Query arm produces a QuerySource (verified by
    /// confirming that build_plan behaves as QuerySource does).
    #[test]
    fn resolver_query_arm_returns_query_source_behaviour() {
        let saved = SavedChartSource::Query {
            query: "SELECT 1".to_string(),
        };
        let source = resolve_source(&saved);

        // A QuerySource with a non-empty query must produce a Driver plan.
        let plan = source
            .build_plan(None)
            .expect("Query resolver arm must succeed for non-empty query");

        assert!(
            matches!(plan, ChartDataPlan::Driver(_)),
            "resolver Query arm must yield ChartDataPlan::Driver"
        );

        // And an empty-query source must return EmptyQuery.
        let empty_saved = SavedChartSource::Query {
            query: String::new(),
        };
        let empty_source = resolve_source(&empty_saved);
        assert!(
            matches!(
                empty_source.build_plan(None),
                Err(ChartSourceError::EmptyQuery)
            ),
            "empty query via resolver must return EmptyQuery"
        );
    }

    /// S-02 / R-02: resolver Collection arm produces a Driver plan carrying the
    /// collection name in targets.
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
        let plan = source
            .build_plan(Some(window))
            .expect("Collection resolver arm must succeed");

        let ChartDataPlan::Driver(request) = plan else {
            panic!("expected ChartDataPlan::Driver");
        };

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
            other => panic!("expected CollectionWindow source context, got: {other:?}"),
        }
    }

    // --- CollectionSource finalized tests ---

    /// S-02 / R-05: collection source with a window must produce a Driver plan,
    /// forward the collection name as the single target, carry the exact
    /// start_ms/end_ms values, and leave query_mode as None.
    #[test]
    fn collection_source_with_window_carries_collection_ref_in_targets() {
        let src = CollectionSource {
            collection_ref: CollectionRef::new("influxdb", "cpu_metrics"),
        };
        let window = TimeWindow {
            start_ms: 1_700_000_000_000,
            end_ms: 1_700_003_600_000,
        };

        let plan = src
            .build_plan(Some(window))
            .expect("should produce Ok plan with a window");

        let ChartDataPlan::Driver(request) = plan else {
            panic!("expected ChartDataPlan::Driver");
        };

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
            other => panic!("expected CollectionWindow source context, got: {other:?}"),
        }
    }

    /// S-02 / R-05: collection source without a window must return WindowRequired.
    #[test]
    fn collection_source_without_window_returns_err_window_required() {
        let src = CollectionSource {
            collection_ref: CollectionRef::new("influxdb", "cpu_metrics"),
        };

        let result = src.build_plan(None);

        assert!(
            matches!(result, Err(ChartSourceError::WindowRequired)),
            "expected WindowRequired error when window is None, got: {:?}",
            result
        );
    }

    // --- AuditSource tests ---

    /// AuditSource must yield ChartDataPlan::LocalAudit carrying the spec's fields.
    #[test]
    fn audit_source_yields_local_audit_plan() {
        let spec = AuditAggregateSpec {
            bucket_ms: 60_000,
            group_by: AuditGroupBy::Category,
            start_ms: Some(1_000_000),
            end_ms: Some(2_000_000),
            categories: vec!["Query".to_string()],
            levels: vec![],
            outcomes: vec![],
            free_text: None,
        };

        let source = AuditSource { spec: spec.clone() };
        let plan = source.build_plan(None).expect("AuditSource must succeed");

        let ChartDataPlan::LocalAudit(returned_spec) = plan else {
            panic!("expected ChartDataPlan::LocalAudit");
        };

        assert_eq!(
            returned_spec, spec,
            "spec must be returned unchanged when no window"
        );
    }

    /// When a TimeWindow is provided, AuditSource must override start_ms/end_ms.
    #[test]
    fn audit_source_window_overrides_time_bounds() {
        let spec = AuditAggregateSpec {
            bucket_ms: 60_000,
            group_by: AuditGroupBy::Outcome,
            start_ms: Some(100),
            end_ms: Some(200),
            categories: vec![],
            levels: vec![],
            outcomes: vec![],
            free_text: None,
        };

        let source = AuditSource { spec };
        let window = TimeWindow {
            start_ms: 9_000_000,
            end_ms: 18_000_000,
        };

        let plan = source
            .build_plan(Some(window))
            .expect("AuditSource with window must succeed");

        let ChartDataPlan::LocalAudit(returned_spec) = plan else {
            panic!("expected ChartDataPlan::LocalAudit");
        };

        assert_eq!(
            returned_spec.start_ms,
            Some(9_000_000),
            "window start_ms must override spec"
        );
        assert_eq!(
            returned_spec.end_ms,
            Some(18_000_000),
            "window end_ms must override spec"
        );
        assert_eq!(
            returned_spec.bucket_ms, 60_000,
            "bucket_ms must be unchanged"
        );
        assert_eq!(
            returned_spec.group_by,
            AuditGroupBy::Outcome,
            "group_by must be unchanged"
        );
    }

    /// AuditSource with no prior bounds and a window must populate start_ms/end_ms.
    #[test]
    fn audit_source_window_sets_bounds_when_spec_has_none() {
        let spec = AuditAggregateSpec {
            bucket_ms: 3_600_000,
            group_by: AuditGroupBy::Level,
            start_ms: None,
            end_ms: None,
            categories: vec![],
            levels: vec![],
            outcomes: vec![],
            free_text: None,
        };

        let source = AuditSource { spec };
        let window = TimeWindow {
            start_ms: 5_000,
            end_ms: 10_000,
        };

        let plan = source.build_plan(Some(window)).expect("must succeed");

        let ChartDataPlan::LocalAudit(returned_spec) = plan else {
            panic!("expected ChartDataPlan::LocalAudit");
        };

        assert_eq!(returned_spec.start_ms, Some(5_000));
        assert_eq!(returned_spec.end_ms, Some(10_000));
    }

    // --- MetricSource tests (T-1, T-2) ---

    /// T-1: MetricSource::build_plan with a window must return Ok(ChartDataPlan::Driver)
    /// carrying a MetricQuery execution context with the correct field values and the
    /// window's start_ms/end_ms.
    #[test]
    fn metric_source_build_plan_with_window() {
        let src = MetricSource::single(
            "AWS/Lambda".to_string(),
            "Invocations".to_string(),
            vec![],
            60,
            "Sum".to_string(),
        );

        let window = TimeWindow {
            start_ms: 1_000_000,
            end_ms: 2_000_000,
        };

        let plan = src
            .build_plan(Some(window))
            .expect("build_plan must succeed with a window");

        let ChartDataPlan::Driver(request) = plan else {
            panic!("expected ChartDataPlan::Driver, got LocalAudit");
        };

        let ctx = request
            .execution_context
            .as_ref()
            .expect("request must carry an execution context");

        match ctx.source.as_ref().expect("source must be Some") {
            ExecutionSourceContext::MetricQuery {
                series,
                start_ms,
                end_ms,
            } => {
                assert_eq!(series.len(), 1);
                let s = &series[0];
                assert_eq!(s.namespace, "AWS/Lambda");
                assert_eq!(s.metric_name, "Invocations");
                assert!(s.dimensions.is_empty());
                assert_eq!(s.period_s, 60);
                assert_eq!(s.statistic, "Sum");
                assert_eq!(*start_ms, 1_000_000);
                assert_eq!(*end_ms, 2_000_000);
            }
            other => panic!("expected MetricQuery source context, got: {other:?}"),
        }
    }

    /// T-2: MetricSource::build_plan without a window must return Err(WindowRequired).
    #[test]
    fn metric_source_build_plan_no_window() {
        let src = MetricSource::single(
            "AWS/Lambda".to_string(),
            "Invocations".to_string(),
            vec![],
            60,
            "Sum".to_string(),
        );

        let result = src.build_plan(None);

        assert!(
            matches!(result, Err(ChartSourceError::WindowRequired)),
            "expected WindowRequired when no window is supplied, got: {:?}",
            result
        );
    }

    // --- Purity guard ---

    /// Verify the source module carries no inappropriate import prefixes at test time.
    /// This is a compilation-level check: if dbflux_storage/dbflux_audit/gpui types
    /// leaked into data_source.rs the crate would fail to build (those crates are not
    /// in dbflux_components' dependencies). This test exists as documentation of the
    /// invariant; cargo check is the real enforcement.
    #[test]
    fn purity_guard_no_io_or_gpui_types_at_compile_time() {
        // If this test compiles and runs, the purity invariant holds:
        // dbflux_components does not depend on dbflux_storage, dbflux_audit,
        // or gpui, so any accidental import of those types causes a compile error
        // rather than a runtime failure.
        let _: AuditAggregateSpec = AuditAggregateSpec {
            bucket_ms: 1,
            group_by: AuditGroupBy::Category,
            start_ms: None,
            end_ms: None,
            categories: vec![],
            levels: vec![],
            outcomes: vec![],
            free_text: None,
        };
    }

    // ---- T17.1: is_self_executing ----

    /// T17.1: `MetricSource::is_self_executing` must return `true`.
    #[test]
    fn metric_source_is_self_executing() {
        let src = MetricSource::single(
            "AWS/EC2".to_string(),
            "CPUUtilization".to_string(),
            vec![],
            300,
            "Average".to_string(),
        );
        assert!(
            src.is_self_executing(),
            "MetricSource must report is_self_executing() == true"
        );
    }

    /// MetricSource::clone_box must produce a box with equal fields.
    #[test]
    fn metric_source_clone_box_produces_equal_source() {
        let src = MetricSource::single(
            "AWS/EC2".to_string(),
            "CPUUtilization".to_string(),
            vec![("InstanceId".to_string(), "i-abc".to_string())],
            300,
            "Average".to_string(),
        );

        let cloned = src.clone_box();

        // The cloned source must produce an identical plan for the same window.
        let window = TimeWindow {
            start_ms: 1_000,
            end_ms: 2_000,
        };
        let original_plan = src
            .build_plan(Some(window))
            .expect("MetricSource must succeed");
        let cloned_plan = cloned
            .build_plan(Some(window))
            .expect("cloned MetricSource must succeed");

        // Both plans are ChartDataPlan::Driver — compare their query requests.
        let ChartDataPlan::Driver(orig_req) = original_plan else {
            panic!("expected Driver plan");
        };
        let ChartDataPlan::Driver(clone_req) = cloned_plan else {
            panic!("expected Driver plan");
        };

        assert_eq!(
            orig_req.execution_context, clone_req.execution_context,
            "cloned MetricSource must produce identical execution context"
        );
    }
}
