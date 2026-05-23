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

pub(crate) struct RealCloudWatchClient(pub aws_sdk_cloudwatch::Client);

impl CloudWatchListMetricsClient for RealCloudWatchClient {
    fn list_metrics(
        &self,
        ns: Option<&str>,
        token: Option<&str>,
    ) -> Result<(Vec<SdkMetric>, Option<String>), DbError> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            DbError::connection_failed(format!("Tokio runtime setup failed: {e}"))
        })?;

        let mut req = self.0.list_metrics();
        if let Some(n) = ns {
            req = req.namespace(n);
        }
        if let Some(t) = token {
            req = req.next_token(t);
        }

        let output = rt
            .block_on(req.send())
            .map_err(|e| DbError::connection_failed(format!("{e}")))?;

        let metrics = output.metrics().to_vec();
        let next_token = output.next_token().map(ToOwned::to_owned);

        Ok((metrics, next_token))
    }
}

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

    /// Expose recorded calls for test assertions when the client is a mock.
    ///
    /// This method is only called from unit tests via the mock's own
    /// `recorded_calls()` method; this is a convenience wrapper on
    /// `CloudWatchMetricCatalog` that tests can use if needed.
    #[cfg(test)]
    pub fn inner_client_calls(&self) -> Vec<(Option<String>, Option<String>)> {
        // Downcast via Any is intentionally avoided — tests access the mock
        // directly. This method exists as a delegation point for the test that
        // uses `MockListMetricsClient::recorded_calls()` through the
        // `CloudWatchMetricCatalog` wrapper.
        //
        // Since `CloudWatchListMetricsClient` is not `Any`, we surface the
        // call record by casting via a known test helper. For actual unit
        // tests in this crate the mock exposes its own `recorded_calls()`.
        //
        // We re-implement a simple workaround: tests in `tests/` can access
        // the mock directly; tests that go through `CloudWatchMetricCatalog`
        // use a wrapper. Kept as a placeholder for future test wiring.
        vec![] // placeholder; real tests access the mock directly
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

        loop {
            let (metrics, next) = self
                .client
                .list_metrics(None, token.as_deref())?;

            for m in &metrics {
                if let Some(ns) = m.namespace() {
                    seen.insert(ns.to_owned());
                }
            }

            token = next;
            if token.is_none() {
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

        let descriptors = metrics
            .into_iter()
            .map(sdk_metric_to_descriptor)
            .collect();

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
