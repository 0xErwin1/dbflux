// TDD RED phase: tests written before the MetricCatalog implementation.
// All tests use the `CloudWatchListMetricsClient` mock seam.

use dbflux_core::{DbError, MetricCatalog, MetricNamespace};
use dbflux_driver_cloudwatch::metric_catalog::{
    CloudWatchListMetricsClient, CloudWatchMetricCatalog,
};

// ---------------------------------------------------------------------------
// Test double
// ---------------------------------------------------------------------------

/// Controlled mock for `CloudWatchListMetricsClient`.
///
/// Each call to `list_metrics` returns the next element from `pages`;
/// once exhausted every call panics (test bug if that happens).
struct MockListMetricsClient {
    /// Sequence of (metrics_on_page, next_token_for_page) pairs.
    pages: std::sync::Mutex<Vec<(Vec<aws_sdk_cloudwatch::types::Metric>, Option<String>)>>,
    /// Records the (namespace, token) arguments received for each call.
    calls: std::sync::Mutex<Vec<(Option<String>, Option<String>)>>,
}

impl MockListMetricsClient {
    fn new(pages: Vec<(Vec<aws_sdk_cloudwatch::types::Metric>, Option<String>)>) -> Self {
        Self {
            pages: std::sync::Mutex::new(pages),
            calls: std::sync::Mutex::new(vec![]),
        }
    }

    fn recorded_calls(&self) -> Vec<(Option<String>, Option<String>)> {
        self.calls.lock().unwrap().clone()
    }
}

impl CloudWatchListMetricsClient for MockListMetricsClient {
    fn list_metrics(
        &self,
        ns: Option<&str>,
        token: Option<&str>,
    ) -> Result<(Vec<aws_sdk_cloudwatch::types::Metric>, Option<String>), DbError> {
        self.calls
            .lock()
            .unwrap()
            .push((ns.map(ToOwned::to_owned), token.map(ToOwned::to_owned)));

        let mut pages = self.pages.lock().unwrap();
        assert!(!pages.is_empty(), "mock exhausted — test called list_metrics too many times");
        Ok(pages.remove(0))
    }
}

// ---------------------------------------------------------------------------
// Helper: build an SDK Metric value
// ---------------------------------------------------------------------------

fn sdk_metric(ns: &str, name: &str, dims: Vec<(&str, &str)>) -> aws_sdk_cloudwatch::types::Metric {
    let mut b = aws_sdk_cloudwatch::types::Metric::builder()
        .namespace(ns)
        .metric_name(name);

    for (k, v) in dims {
        let dim = aws_sdk_cloudwatch::types::Dimension::builder()
            .name(k)
            .value(v)
            .build();
        b = b.dimensions(dim);
    }

    b.build()
}

// ---------------------------------------------------------------------------
// list_namespaces: returns sorted distinct namespaces from a paginated sweep
// ---------------------------------------------------------------------------

#[test]
fn list_namespaces_returns_sorted_distinct_namespaces() {
    // Two pages; page 1 has overlapping namespaces with page 2.
    let page1 = vec![
        sdk_metric("AWS/Lambda", "Errors", vec![]),
        sdk_metric("AWS/EC2", "CPUUtilization", vec![]),
        sdk_metric("AWS/Lambda", "Duration", vec![]),
    ];
    let page2 = vec![
        sdk_metric("AWS/S3", "BucketSizeBytes", vec![]),
        sdk_metric("AWS/EC2", "NetworkIn", vec![]),
    ];

    let mock = MockListMetricsClient::new(vec![(page1, Some("tok1".into())), (page2, None)]);
    let catalog = CloudWatchMetricCatalog::new(Box::new(mock));

    let namespaces = catalog.list_namespaces().expect("must succeed");

    // Must be sorted and deduplicated.
    assert_eq!(
        namespaces,
        vec!["AWS/EC2".to_string(), "AWS/Lambda".to_string(), "AWS/S3".to_string(),],
        "namespaces must be sorted and distinct"
    );
}

// ---------------------------------------------------------------------------
// list_metrics: passes next_token verbatim
// ---------------------------------------------------------------------------

#[test]
fn list_metrics_passes_next_token_verbatim() {
    use std::sync::Arc;

    let ns: MetricNamespace = "AWS/EC2".to_string();
    let input_token = "my-opaque-token-123";

    // Use Arc so we can access recorded_calls() after the catalog is constructed.
    let calls: Arc<std::sync::Mutex<Vec<(Option<String>, Option<String>)>>> =
        Arc::new(std::sync::Mutex::new(vec![]));
    let calls_clone = calls.clone();

    struct TokenTracker {
        page: std::sync::Mutex<Option<(Vec<aws_sdk_cloudwatch::types::Metric>, Option<String>)>>,
        calls: Arc<std::sync::Mutex<Vec<(Option<String>, Option<String>)>>>,
    }

    impl CloudWatchListMetricsClient for TokenTracker {
        fn list_metrics(
            &self,
            ns: Option<&str>,
            token: Option<&str>,
        ) -> Result<(Vec<aws_sdk_cloudwatch::types::Metric>, Option<String>), DbError> {
            self.calls
                .lock()
                .unwrap()
                .push((ns.map(ToOwned::to_owned), token.map(ToOwned::to_owned)));
            let page = self.page.lock().unwrap().take().unwrap();
            Ok(page)
        }
    }

    let tracker = TokenTracker {
        page: std::sync::Mutex::new(Some((
            vec![sdk_metric("AWS/EC2", "CPUUtilization", vec![])],
            None,
        ))),
        calls: calls_clone,
    };

    let catalog = CloudWatchMetricCatalog::new(Box::new(tracker));
    let _result = catalog
        .list_metrics(&ns, Some(input_token))
        .expect("must succeed");

    let recorded = calls.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].1.as_deref(),
        Some(input_token),
        "next_token must be passed through verbatim"
    );
}

// ---------------------------------------------------------------------------
// MetricDescriptor: dimension order preserved from SDK
// ---------------------------------------------------------------------------

#[test]
fn metric_descriptor_maps_dimensions_in_order() {
    // SDK returns dimensions in a specific order — we must preserve it.
    let sdk_m = sdk_metric(
        "AWS/EC2",
        "CPUUtilization",
        vec![("InstanceId", "i-abc"), ("AutoScalingGroupName", "asg-1")],
    );

    let mock = MockListMetricsClient::new(vec![(vec![sdk_m], None)]);
    let catalog = CloudWatchMetricCatalog::new(Box::new(mock));

    let ns: MetricNamespace = "AWS/EC2".to_string();
    let page = catalog.list_metrics(&ns, None).expect("must succeed");

    assert_eq!(page.metrics.len(), 1);
    let desc = &page.metrics[0];

    assert_eq!(desc.metric_name, "CPUUtilization");
    assert_eq!(
        desc.dimensions,
        vec![
            ("InstanceId".to_string(), "i-abc".to_string()),
            ("AutoScalingGroupName".to_string(), "asg-1".to_string()),
        ],
        "dimension order must be preserved"
    );
}

// ---------------------------------------------------------------------------
// Error mapping: AWS error maps to DbError::Connection
// ---------------------------------------------------------------------------

struct ErrorClient;

impl CloudWatchListMetricsClient for ErrorClient {
    fn list_metrics(
        &self,
        _ns: Option<&str>,
        _token: Option<&str>,
    ) -> Result<(Vec<aws_sdk_cloudwatch::types::Metric>, Option<String>), DbError> {
        Err(DbError::connection_failed("timeout: connection refused"))
    }
}

#[test]
fn list_metrics_error_maps_to_db_error_connection() {
    let catalog = CloudWatchMetricCatalog::new(Box::new(ErrorClient));
    let ns: MetricNamespace = "AWS/EC2".to_string();

    let result = catalog.list_metrics(&ns, None);

    assert!(result.is_err(), "error client must produce Err");
    assert!(
        matches!(result, Err(DbError::ConnectionFailed(_))),
        "error must map to DbError::ConnectionFailed"
    );
}

#[test]
fn list_namespaces_error_propagates() {
    let catalog = CloudWatchMetricCatalog::new(Box::new(ErrorClient));
    let result = catalog.list_namespaces();

    assert!(
        matches!(result, Err(DbError::ConnectionFailed(_))),
        "namespace sweep error must propagate as DbError::ConnectionFailed"
    );
}
