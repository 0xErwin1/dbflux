//! Session-scoped in-memory cache for metric catalog data.
//!
//! The cache stores namespace lists and per-namespace metric pages keyed by
//! connection profile UUID. All mutation happens through short-lived
//! `std::sync::Mutex` critical sections (HashMap insert/remove only; no IO
//! inside the lock).
//!
//! # Lifecycle
//!
//! The cache is created once when `AppState` is constructed and held as
//! `Arc<MetricCatalogCache>`. It is NOT persisted across application restarts.
//!
//! # Fetch dispatch
//!
//! The cache holds only cached data and invalidation. Actual driver calls
//! (`MetricCatalog::list_namespaces`, `list_metrics`) happen in the GPUI
//! layer (`dbflux_ui_document`), which resolves the connection from
//! `AppStateEntity` and spawns background tasks via `cx.background_executor()`.
//! When a fetch completes, the background task writes the result into the cache
//! via `store_namespaces` / `store_metrics_page` and calls `cx.notify()`.
//!
//! # TODO(v2)
//!
//! Add per-(conn, namespace) in-flight dedup here if concurrent callers are
//! observed in practice (e.g. two pickers open on the same connection).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dbflux_core::{MetricDescriptor, MetricNamespace};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A snapshot of the accumulated metrics cache entry for one (conn, ns) pair.
pub struct MetricsPageView {
    /// All metric descriptors accumulated so far across all fetched pages.
    pub accumulated: Arc<Vec<MetricDescriptor>>,
    /// True when the last fetched page had `next_token = None` (fully loaded).
    pub fully_loaded: bool,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

#[derive(Default)]
struct NamespaceCache {
    /// Cached namespace list; None until first fetch completes.
    namespaces: Option<Arc<Vec<MetricNamespace>>>,
    /// Per-namespace metrics accumulator.
    metrics_pages: HashMap<MetricNamespace, MetricsCacheEntry>,
}

struct MetricsCacheEntry {
    /// All metric descriptors accumulated across fetched pages.
    accumulated: Arc<Vec<MetricDescriptor>>,
    /// Continuation token for the next page; None = fully loaded.
    next_token: Option<String>,
    /// True when the last stored page had no continuation token.
    fully_loaded: bool,
}

// ---------------------------------------------------------------------------
// MetricCatalogCache
// ---------------------------------------------------------------------------

/// Session-scoped cache for metric catalog data.
///
/// Thread-safe via `std::sync::Mutex`. Critical sections are O(1) HashMap
/// operations only — no IO or driver calls inside the lock.
pub struct MetricCatalogCache {
    inner: Mutex<HashMap<Uuid, NamespaceCache>>,
}

impl MetricCatalogCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
        })
    }

    // -----------------------------------------------------------------------
    // Peek (synchronous reads — called from the UI render path)
    // -----------------------------------------------------------------------

    /// Return the cached namespace list for `profile_id`, if present.
    ///
    /// Returns `None` if no fetch has completed yet for this connection.
    /// Callers that get `None` must spawn a fetch and store the result via
    /// `store_namespaces`.
    pub fn peek_namespaces(&self, profile_id: Uuid) -> Option<Arc<Vec<MetricNamespace>>> {
        self.inner
            .lock()
            .expect("MetricCatalogCache lock poisoned")
            .get(&profile_id)
            .and_then(|c| c.namespaces.clone())
    }

    /// Return a snapshot of the metrics cache for `(profile_id, namespace)`.
    ///
    /// Returns `None` if no fetch has started for this namespace yet.
    pub fn peek_metrics(
        &self,
        profile_id: Uuid,
        namespace: &MetricNamespace,
    ) -> Option<MetricsPageView> {
        self.inner
            .lock()
            .expect("MetricCatalogCache lock poisoned")
            .get(&profile_id)
            .and_then(|c| c.metrics_pages.get(namespace))
            .map(|e| MetricsPageView {
                accumulated: e.accumulated.clone(),
                fully_loaded: e.fully_loaded,
            })
    }

    // -----------------------------------------------------------------------
    // Store (called from background-task completion handlers)
    // -----------------------------------------------------------------------

    /// Store a completed namespace fetch result.
    pub fn store_namespaces(&self, profile_id: Uuid, namespaces: Vec<MetricNamespace>) {
        let mut inner = self.inner.lock().expect("MetricCatalogCache lock poisoned");
        inner
            .entry(profile_id)
            .or_default()
            .namespaces = Some(Arc::new(namespaces));
    }

    /// Append a fetched metrics page to the accumulator for `(profile_id, namespace)`.
    ///
    /// If `next_token` is `None`, `fully_loaded` is set to `true` on the entry.
    /// Returns a `MetricsPageView` snapshot for the caller to use immediately.
    pub fn store_metrics_page(
        &self,
        profile_id: Uuid,
        namespace: MetricNamespace,
        new_metrics: Vec<MetricDescriptor>,
        next_token: Option<String>,
    ) -> MetricsPageView {
        let mut inner = self.inner.lock().expect("MetricCatalogCache lock poisoned");
        let ns_cache = inner.entry(profile_id).or_default();
        let entry = ns_cache
            .metrics_pages
            .entry(namespace)
            .or_insert_with(|| MetricsCacheEntry {
                accumulated: Arc::new(vec![]),
                next_token: None,
                fully_loaded: false,
            });

        // Append new metrics to the accumulated list.
        let mut all: Vec<MetricDescriptor> = (*entry.accumulated).clone();
        all.extend(new_metrics);
        entry.accumulated = Arc::new(all);

        entry.fully_loaded = next_token.is_none();
        entry.next_token = next_token;

        MetricsPageView {
            accumulated: entry.accumulated.clone(),
            fully_loaded: entry.fully_loaded,
        }
    }

    /// Return the stored continuation token for the next page, if any.
    ///
    /// Returns `None` if no entry exists or the namespace is fully loaded.
    pub fn peek_next_token(
        &self,
        profile_id: Uuid,
        namespace: &MetricNamespace,
    ) -> Option<String> {
        self.inner
            .lock()
            .expect("MetricCatalogCache lock poisoned")
            .get(&profile_id)
            .and_then(|c| c.metrics_pages.get(namespace))
            .and_then(|e| e.next_token.clone())
    }

    // -----------------------------------------------------------------------
    // Invalidation
    // -----------------------------------------------------------------------

    /// Remove all cached data for `profile_id`.
    ///
    /// Called on connection disconnect to ensure stale data is not served
    /// if the user reconnects with a different account or region.
    pub fn invalidate(&self, profile_id: Uuid) {
        self.inner
            .lock()
            .expect("MetricCatalogCache lock poisoned")
            .remove(&profile_id);
    }

    /// Remove only the metrics cache for `(profile_id, namespace)`.
    ///
    /// The namespace list for `profile_id` is kept. Useful when the user
    /// triggers a refresh on a specific namespace.
    pub fn invalidate_namespace(&self, profile_id: Uuid, namespace: &MetricNamespace) {
        if let Some(ns_cache) = self
            .inner
            .lock()
            .expect("MetricCatalogCache lock poisoned")
            .get_mut(&profile_id)
        {
            ns_cache.metrics_pages.remove(namespace);
        }
    }
}

impl Default for MetricCatalogCache {
    fn default() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests (TDD — written for task 3.1)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::MetricDescriptor;

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

    /// peek_namespaces returns None before any fetch (proves laziness).
    #[test]
    fn peek_namespaces_returns_none_before_any_fetch() {
        let cache = MetricCatalogCache::new();
        let id = profile();
        assert!(
            cache.peek_namespaces(id).is_none(),
            "cache must return None before any store_namespaces call"
        );
    }

    /// peek_metrics returns None before any fetch.
    #[test]
    fn peek_metrics_returns_none_before_any_fetch() {
        let cache = MetricCatalogCache::new();
        let id = profile();
        let ns = ns("AWS/EC2");
        assert!(
            cache.peek_metrics(id, &ns).is_none(),
            "cache must return None before any store_metrics_page call"
        );
    }

    /// invalidate(conn_id) clears namespace + metrics entries for that conn.
    #[test]
    fn invalidate_clears_all_entries_for_connection() {
        let cache = MetricCatalogCache::new();
        let id = profile();
        let ns_name = ns("AWS/EC2");

        cache.store_namespaces(id, vec![ns_name.clone()]);
        cache.store_metrics_page(id, ns_name.clone(), vec![metric("CPUUtilization")], None);

        // Sanity check: data is there.
        assert!(cache.peek_namespaces(id).is_some());
        assert!(cache.peek_metrics(id, &ns_name).is_some());

        cache.invalidate(id);

        assert!(
            cache.peek_namespaces(id).is_none(),
            "invalidate must clear namespace list"
        );
        assert!(
            cache.peek_metrics(id, &ns_name).is_none(),
            "invalidate must clear metrics entries"
        );
    }

    /// invalidate(conn_id) does not affect other connections.
    #[test]
    fn invalidate_does_not_affect_other_connections() {
        let cache = MetricCatalogCache::new();
        let id1 = profile();
        let id2 = profile();

        cache.store_namespaces(id1, vec![ns("AWS/EC2")]);
        cache.store_namespaces(id2, vec![ns("AWS/Lambda")]);

        cache.invalidate(id1);

        assert!(
            cache.peek_namespaces(id1).is_none(),
            "id1 must be cleared"
        );
        assert!(
            cache.peek_namespaces(id2).is_some(),
            "id2 must be unaffected"
        );
    }

    /// invalidate_namespace clears only the metrics entry for that ns, not namespaces.
    #[test]
    fn invalidate_namespace_clears_only_metrics_for_that_namespace() {
        let cache = MetricCatalogCache::new();
        let id = profile();
        let ec2 = ns("AWS/EC2");
        let lambda = ns("AWS/Lambda");

        cache.store_namespaces(id, vec![ec2.clone(), lambda.clone()]);
        cache.store_metrics_page(id, ec2.clone(), vec![metric("CPUUtilization")], None);
        cache.store_metrics_page(id, lambda.clone(), vec![metric("Errors")], None);

        cache.invalidate_namespace(id, &ec2);

        // Namespace list must remain.
        assert!(
            cache.peek_namespaces(id).is_some(),
            "namespace list must not be cleared by invalidate_namespace"
        );
        // AWS/EC2 metrics must be gone.
        assert!(
            cache.peek_metrics(id, &ec2).is_none(),
            "AWS/EC2 metrics must be cleared"
        );
        // AWS/Lambda metrics must remain.
        assert!(
            cache.peek_metrics(id, &lambda).is_some(),
            "AWS/Lambda metrics must not be affected"
        );
    }

    /// After a simulated error (no store call), second peek_namespaces still returns None
    /// (cache NOT poisoned by errors — callers retry).
    #[test]
    fn failed_fetch_does_not_poison_cache_and_allows_retry() {
        let cache = MetricCatalogCache::new();
        let id = profile();

        // No store call — simulates a failed fetch.
        assert!(cache.peek_namespaces(id).is_none(), "must return None");

        // Second call still returns None, not an error or panic.
        assert!(cache.peek_namespaces(id).is_none(), "retry must still return None");

        // Now a successful store arrives.
        cache.store_namespaces(id, vec![ns("AWS/EC2")]);
        let result = cache.peek_namespaces(id);
        assert!(result.is_some(), "after successful store, must return Some");
        assert_eq!(result.unwrap().as_ref(), &[ns("AWS/EC2")]);
    }

    /// Pagination: store two pages; accumulated has all metrics, fully_loaded=true.
    #[test]
    fn store_metrics_page_accumulates_across_pages() {
        let cache = MetricCatalogCache::new();
        let id = profile();
        let ns_name = ns("AWS/EC2");

        // First page: 2 metrics, has continuation token.
        cache.store_metrics_page(
            id,
            ns_name.clone(),
            vec![metric("CPUUtilization"), metric("NetworkIn")],
            Some("tok1".to_string()),
        );

        let view = cache.peek_metrics(id, &ns_name).unwrap();
        assert_eq!(view.accumulated.len(), 2);
        assert!(!view.fully_loaded);

        // Second page: 1 metric, no continuation token.
        let view2 = cache.store_metrics_page(
            id,
            ns_name.clone(),
            vec![metric("NetworkOut")],
            None,
        );

        assert_eq!(view2.accumulated.len(), 3, "must accumulate across pages");
        assert!(view2.fully_loaded, "fully_loaded must be true after final page");

        let peek = cache.peek_metrics(id, &ns_name).unwrap();
        assert_eq!(peek.accumulated.len(), 3);
        assert!(peek.fully_loaded);
    }
}
