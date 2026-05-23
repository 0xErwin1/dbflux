//! CloudWatch implementation of the `MetricCatalog` trait.
//!
//! Uses a thin internal `CloudWatchListMetricsClient` seam so unit tests can
//! inject a mock without hitting real AWS endpoints. Live integration tests
//! require `--run-ignored` and real `AWS_*` credentials.

use std::collections::BTreeSet;

use aws_sdk_cloudwatch::types::Metric as SdkMetric;
use dbflux_core::{DbError, MetricCatalog, MetricCatalogPage, MetricDescriptor, MetricNamespace};

// ---------------------------------------------------------------------------
// Internal seam: CloudWatchListMetricsClient
// ---------------------------------------------------------------------------

/// Mockable seam over the CloudWatch `ListMetrics` API.
///
/// The real implementation wraps `aws_sdk_cloudwatch::Client` and blocks on
/// the Tokio runtime. In unit tests, a mock struct implements this trait to
/// return pre-canned pages without any AWS calls.
pub trait CloudWatchListMetricsClient: Send + Sync {
    /// Call `ListMetrics` with an optional namespace filter and pagination token.
    ///
    /// Returns `(metrics_on_page, next_token_or_none)`.
    fn list_metrics(
        &self,
        ns: Option<&str>,
        token: Option<&str>,
    ) -> Result<(Vec<SdkMetric>, Option<String>), DbError>;
}

// ---------------------------------------------------------------------------
// Real client adapter (wraps aws_sdk_cloudwatch::Client)
// ---------------------------------------------------------------------------

pub(crate) struct RealCloudWatchClient {
    client: aws_sdk_cloudwatch::Client,
    /// Long-lived Tokio runtime shared across all `list_metrics` calls.
    ///
    /// Constructing a new runtime per call (previous behavior) wasted file
    /// descriptors and made full-namespace sweeps very expensive — each page
    /// would spin up and tear down its own reactor.
    runtime: tokio::runtime::Runtime,
}

impl RealCloudWatchClient {
    /// Build a client adapter with a long-lived runtime.
    ///
    /// Returns an error if the runtime cannot be constructed.
    pub(crate) fn new(client: aws_sdk_cloudwatch::Client) -> Result<Self, DbError> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| DbError::connection_failed(format!("Tokio runtime setup failed: {e}")))?;
        Ok(Self { client, runtime })
    }
}

impl CloudWatchListMetricsClient for RealCloudWatchClient {
    fn list_metrics(
        &self,
        ns: Option<&str>,
        token: Option<&str>,
    ) -> Result<(Vec<SdkMetric>, Option<String>), DbError> {
        let mut req = self.client.list_metrics();
        if let Some(n) = ns {
            req = req.namespace(n);
        }
        if let Some(t) = token {
            req = req.next_token(t);
        }

        let output = self
            .runtime
            .block_on(req.send())
            .map_err(|e| DbError::connection_failed(format!("{e}")))?;

        let metrics = output.metrics().to_vec();
        let next_token = output.next_token().map(ToOwned::to_owned);

        Ok((metrics, next_token))
    }
}

// ---------------------------------------------------------------------------
// Sweep limits
// ---------------------------------------------------------------------------

/// Hard cap on `ListMetrics` pages consumed during a single
/// `list_namespaces` sweep.
///
/// CloudWatch returns up to 500 metrics per page; this cap bounds the worst-case
/// sweep at roughly 25,000 metrics. On very large accounts (typically AWS
/// service-owned namespaces with per-instance metric explosion) the sweep
/// returns early instead of running indefinitely. The cap is documented in the
/// crate README and CHANGELOG; a full timeout + cancellation infrastructure is
/// tracked as a follow-up.
const MAX_NAMESPACE_SWEEP_PAGES: usize = 50;

// ---------------------------------------------------------------------------
// CloudWatchMetricCatalog
// ---------------------------------------------------------------------------

/// `MetricCatalog` implementation for CloudWatch.
///
/// Namespace listing is synthesized by sweeping `ListMetrics` with no
/// namespace filter and collecting distinct namespace strings — CloudWatch
/// has no native `ListNamespaces` API.
pub struct CloudWatchMetricCatalog {
    client: Box<dyn CloudWatchListMetricsClient>,
}

impl CloudWatchMetricCatalog {
    /// Construct with the given client adapter (real or mock).
    pub fn new(client: Box<dyn CloudWatchListMetricsClient>) -> Self {
        Self { client }
    }
}

impl MetricCatalog for CloudWatchMetricCatalog {
    /// Synthesize a sorted namespace list by sweeping `ListMetrics` with no filter.
    ///
    /// CloudWatch has no native `ListNamespaces` — this performs a full
    /// `ListMetrics` pagination, collects distinct `namespace` strings, and
    /// returns them sorted. The result is cached by `MetricCatalogCache` so
    /// this call is made at most once per session per connection.
    fn list_namespaces(&self) -> Result<Vec<MetricNamespace>, DbError> {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut token: Option<String> = None;
        let mut pages = 0_usize;

        loop {
            let (metrics, next) = self.client.list_metrics(None, token.as_deref())?;
            pages += 1;

            for m in &metrics {
                if let Some(ns) = m.namespace() {
                    seen.insert(ns.to_owned());
                }
            }

            token = next;
            if token.is_none() {
                break;
            }

            // Bound the worst case for accounts with massive metric volume.
            // The user sees the namespaces collected so far; the cap is
            // documented in the crate README.
            if pages >= MAX_NAMESPACE_SWEEP_PAGES {
                log::warn!(
                    "[cloudwatch] list_namespaces hit page cap of {} (results truncated)",
                    MAX_NAMESPACE_SWEEP_PAGES
                );
                break;
            }
        }

        Ok(seen.into_iter().collect())
    }

    /// Fetch one page of metrics for a namespace.
    ///
    /// Passes `next_token` verbatim to the underlying `ListMetrics` call.
    /// Each SDK `Metric` maps to one `MetricDescriptor` with ordered dimensions.
    fn list_metrics(
        &self,
        namespace: &MetricNamespace,
        next_token: Option<&str>,
    ) -> Result<MetricCatalogPage, DbError> {
        let (metrics, returned_token) = self
            .client
            .list_metrics(Some(namespace.as_str()), next_token)?;

        let descriptors = metrics.into_iter().map(sdk_metric_to_descriptor).collect();

        Ok(MetricCatalogPage {
            metrics: descriptors,
            next_token: returned_token,
        })
    }
}

// ---------------------------------------------------------------------------
// SDK type mapping
// ---------------------------------------------------------------------------

/// Map one SDK `Metric` to a `MetricDescriptor`, preserving dimension order.
fn sdk_metric_to_descriptor(m: SdkMetric) -> MetricDescriptor {
    let metric_name = m.metric_name().unwrap_or_default().to_owned();

    let dimensions = m
        .dimensions()
        .iter()
        .map(|d| {
            (
                d.name().unwrap_or_default().to_owned(),
                d.value().unwrap_or_default().to_owned(),
            )
        })
        .collect();

    MetricDescriptor {
        metric_name,
        dimensions,
    }
}
