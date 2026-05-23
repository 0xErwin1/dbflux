//! Integration tests for `MetricCatalogCache`.
//!
//! These tests exercise the full namespace → metrics → second-page → fully_loaded
//! lifecycle using the cache's store/peek/invalidate surface. The actual driver
//! calls (ListMetrics) happen in the GPUI layer; the cache is pure Rust and
//! exercisable without a GPUI runtime.

use dbflux_app::MetricCatalogCache;
use dbflux_core::{MetricDescriptor, MetricNamespace};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn profile() -> Uuid {
    Uuid::new_v4()
}

fn ns(name: &str) -> MetricNamespace {
    name.to_string()
}

fn metric(name: &str) -> MetricDescriptor {
    MetricDescriptor {
        metric_name: name.to_string(),
        dimensions: vec![],
    }
}

fn metric_with_dims(name: &str, dims: Vec<(&str, &str)>) -> MetricDescriptor {
    MetricDescriptor {
        metric_name: name.to_string(),
        dimensions: dims
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Integration scenario 1: namespace → metrics → second page → fully_loaded
// ---------------------------------------------------------------------------

/// Full lifecycle test: fetch namespaces, fetch first page of metrics, fetch
/// second and final page; assert `fully_loaded` is set and all metrics are present.
#[test]
fn full_flow_namespace_metrics_pagination_to_fully_loaded() {
    let cache = MetricCatalogCache::new();
    let profile_id = profile();
    let ec2 = ns("AWS/EC2");
    let lambda = ns("AWS/Lambda");

    // Step 1: store namespace list (simulates a completed ListMetrics sweep).
    assert!(
        cache.peek_namespaces(profile_id).is_none(),
        "cache must be empty before first store"
    );

    cache.store_namespaces(profile_id, vec![ec2.clone(), lambda.clone()]);

    let namespaces = cache
        .peek_namespaces(profile_id)
        .expect("namespaces must be present after store");
    assert_eq!(namespaces.len(), 2, "both namespaces must be stored");
    assert!(namespaces.contains(&ec2));
    assert!(namespaces.contains(&lambda));

    // Step 2: store first page of metrics for AWS/EC2 with a continuation token.
    assert!(
        cache.peek_metrics(profile_id, &ec2).is_none(),
        "metrics cache must be empty before first page"
    );

    let view1 = cache.store_metrics_page(
        profile_id,
        ec2.clone(),
        vec![metric("CPUUtilization"), metric("NetworkIn")],
        Some("page2-token".to_string()),
    );

    assert_eq!(view1.accumulated.len(), 2, "first page: 2 metrics");
    assert!(!view1.fully_loaded, "first page: not yet fully loaded");
    assert_eq!(
        cache.peek_next_token(profile_id, &ec2).as_deref(),
        Some("page2-token"),
        "continuation token must be stored"
    );

    // Step 3: store second (final) page with no continuation token.
    let view2 = cache.store_metrics_page(
        profile_id,
        ec2.clone(),
        vec![
            metric("NetworkOut"),
            metric_with_dims("DiskReadBytes", vec![("InstanceId", "i-0123456789abcdef0")]),
        ],
        None, // terminal page
    );

    assert_eq!(
        view2.accumulated.len(),
        4,
        "second page: 4 total accumulated"
    );
    assert!(
        view2.fully_loaded,
        "second page: fully_loaded must be true after terminal page"
    );
    assert!(
        cache.peek_next_token(profile_id, &ec2).is_none(),
        "next_token must be None after terminal page"
    );

    // Step 4: peek must reflect the final accumulated state.
    let peek = cache
        .peek_metrics(profile_id, &ec2)
        .expect("metrics must be present");
    assert_eq!(peek.accumulated.len(), 4);
    assert!(peek.fully_loaded);

    // Step 5: Lambda metrics are independent — should still be None.
    assert!(
        cache.peek_metrics(profile_id, &lambda).is_none(),
        "Lambda metrics must not be affected by EC2 stores"
    );
}

// ---------------------------------------------------------------------------
// Integration scenario 2: invalidate between runs clears cache
// ---------------------------------------------------------------------------

/// Simulates a user disconnecting and reconnecting: `invalidate` clears all
/// data for the profile so the next session fetches fresh data.
#[test]
fn invalidate_clears_cache_between_connection_sessions() {
    let cache = MetricCatalogCache::new();
    let profile_id = profile();
    let ec2 = ns("AWS/EC2");

    // First "connection session": populate the cache.
    cache.store_namespaces(profile_id, vec![ec2.clone()]);
    cache.store_metrics_page(
        profile_id,
        ec2.clone(),
        vec![metric("CPUUtilization")],
        None,
    );

    assert!(
        cache.peek_namespaces(profile_id).is_some(),
        "sanity: namespaces present before invalidate"
    );
    assert!(
        cache.peek_metrics(profile_id, &ec2).is_some(),
        "sanity: metrics present before invalidate"
    );

    // Disconnect: invalidate must clear all data for this profile.
    cache.invalidate(profile_id);

    assert!(
        cache.peek_namespaces(profile_id).is_none(),
        "namespaces must be cleared after invalidate"
    );
    assert!(
        cache.peek_metrics(profile_id, &ec2).is_none(),
        "metrics must be cleared after invalidate"
    );

    // Second "connection session": fresh populate must succeed.
    cache.store_namespaces(profile_id, vec![ec2.clone(), ns("AWS/Lambda")]);

    let fresh = cache
        .peek_namespaces(profile_id)
        .expect("namespaces must be present after fresh store");
    assert_eq!(
        fresh.len(),
        2,
        "fresh namespace list must contain both entries"
    );
}

// ---------------------------------------------------------------------------
// Integration scenario 3: two profiles are fully independent
// ---------------------------------------------------------------------------

/// Multiple simultaneous connections use independent cache entries; invalidating
/// one does not affect the other.
#[test]
fn two_profiles_have_independent_cache_entries() {
    let cache = MetricCatalogCache::new();
    let profile_a = profile();
    let profile_b = profile();
    let ec2 = ns("AWS/EC2");
    let s3 = ns("AWS/S3");

    cache.store_namespaces(profile_a, vec![ec2.clone()]);
    cache.store_namespaces(profile_b, vec![s3.clone()]);

    cache.store_metrics_page(profile_a, ec2.clone(), vec![metric("CPUUtilization")], None);
    cache.store_metrics_page(profile_b, s3.clone(), vec![metric("BucketSizeBytes")], None);

    // Invalidate profile_a — profile_b must be unaffected.
    cache.invalidate(profile_a);

    assert!(
        cache.peek_namespaces(profile_a).is_none(),
        "profile_a namespaces must be cleared"
    );
    assert!(
        cache.peek_metrics(profile_a, &ec2).is_none(),
        "profile_a metrics must be cleared"
    );

    let b_namespaces = cache
        .peek_namespaces(profile_b)
        .expect("profile_b namespaces must be unaffected");
    assert!(b_namespaces.contains(&s3));

    assert!(
        cache.peek_metrics(profile_b, &s3).is_some(),
        "profile_b metrics must be unaffected"
    );
}

// ---------------------------------------------------------------------------
// Integration scenario 4: namespace-level invalidation for targeted refresh
// ---------------------------------------------------------------------------

/// `invalidate_namespace` removes only the metrics for one namespace while
/// leaving the namespace list and other namespace metrics intact.
#[test]
fn invalidate_namespace_allows_targeted_refresh() {
    let cache = MetricCatalogCache::new();
    let profile_id = profile();
    let ec2 = ns("AWS/EC2");
    let lambda = ns("AWS/Lambda");

    cache.store_namespaces(profile_id, vec![ec2.clone(), lambda.clone()]);

    cache.store_metrics_page(
        profile_id,
        ec2.clone(),
        vec![metric("CPUUtilization")],
        None,
    );
    cache.store_metrics_page(profile_id, lambda.clone(), vec![metric("Errors")], None);

    // User triggers "Refresh" on AWS/EC2 in the picker.
    cache.invalidate_namespace(profile_id, &ec2);

    // Namespace list must remain.
    assert!(
        cache.peek_namespaces(profile_id).is_some(),
        "namespace list must survive invalidate_namespace"
    );

    // EC2 metrics are gone; Lambda metrics are intact.
    assert!(
        cache.peek_metrics(profile_id, &ec2).is_none(),
        "EC2 metrics must be cleared by invalidate_namespace"
    );
    assert!(
        cache.peek_metrics(profile_id, &lambda).is_some(),
        "Lambda metrics must survive invalidate_namespace targeting EC2"
    );

    // Re-store EC2 metrics (simulating a re-fetch after targeted invalidation).
    cache.store_metrics_page(
        profile_id,
        ec2.clone(),
        vec![metric("CPUUtilization"), metric("NetworkOut")],
        None,
    );

    let refreshed = cache
        .peek_metrics(profile_id, &ec2)
        .expect("EC2 metrics must be present after re-store");
    assert_eq!(
        refreshed.accumulated.len(),
        2,
        "re-stored EC2 metrics must have 2 entries"
    );
    assert!(refreshed.fully_loaded);
}
