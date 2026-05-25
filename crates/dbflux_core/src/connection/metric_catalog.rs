// TDD RED phase: tests written before implementation.
// The types and trait referenced here do not yet exist.

/// Stable identifier for a metric namespace. Plain String in v1 — no newtype
/// until invariants justify it.
pub type MetricNamespace = String;

/// One observed metric+dimensions combination from a catalog source.
///
/// Dimensions are an ordered list of (name, value) pairs exactly as reported
/// by the underlying catalog API. The list is what the source returned; the UI
/// renders it as chips, rows, or a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetricDescriptor {
    pub metric_name: String,
    pub dimensions: Vec<(String, String)>,
}

/// One page of results from `list_metrics`.
///
/// `next_token` is opaque to the cache and the UI; it MUST be passed back
/// unchanged to fetch the next page. `None` signals no more pages.
#[derive(Debug, Clone)]
pub struct MetricCatalogPage {
    pub metrics: Vec<MetricDescriptor>,
    pub next_token: Option<String>,
}

/// How the user wants dimensions applied when building a query from a catalog
/// selection. Structured (not free-text) to prevent driver-specific syntax
/// leaking into the UI layer.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DimensionFilter {
    /// Aggregate across every dimension combination for the metric.
    AggregateAll,
    /// Pin to exactly this dimension set (typically copied from a
    /// `MetricDescriptor` the user selected).
    FilterTo(Vec<(String, String)>),
}

/// Driver-agnostic seam for browsing a metric catalog. Separate from
/// `METRIC_SERIES` so a driver can advertise execution-only or browse-only
/// support independently.
///
/// Implementations are `Send + Sync` because the cache invokes them from
/// background-executor tasks.
pub trait MetricCatalog: Send + Sync {
    /// List the namespaces this connection exposes (e.g. `"AWS/EC2"`).
    ///
    /// Implementations should return a stable, sorted result when feasible.
    fn list_namespaces(&self) -> Result<Vec<MetricNamespace>, crate::DbError>;

    /// Fetch one page of (metric_name, dimensions) combinations within a
    /// namespace.
    ///
    /// `next_token = None` requests the first page. The returned `next_token`
    /// (if any) must be fed back verbatim to fetch the next page.
    fn list_metrics(
        &self,
        namespace: &MetricNamespace,
        next_token: Option<&str>,
    ) -> Result<MetricCatalogPage, crate::DbError>;
}

// ---------------------------------------------------------------------------
// Tests — written before implementation (TDD RED phase for task 1.1)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// METRIC_CATALOG capability bit must equal 1 << 50 exactly.
    /// Companion test for the bit is in capabilities.rs; this one is here for
    /// symmetry with how metric_catalog.rs is tested alongside the types.
    #[test]
    fn metric_catalog_page_with_empty_metrics_and_no_token_is_valid() {
        // MetricCatalogPage with an empty metrics vec and next_token = None
        // is a valid terminal page and must construct without panic.
        let page = MetricCatalogPage {
            metrics: vec![],
            next_token: None,
        };

        assert!(page.metrics.is_empty());
        assert!(page.next_token.is_none());
    }

    /// DimensionFilter::AggregateAll must NOT equal FilterTo(vec![]).
    /// These are semantically distinct: AggregateAll = "no pinning" whereas
    /// FilterTo(vec![]) = "pin to the empty dimension set" (scalar metric).
    #[test]
    fn dimension_filter_aggregate_all_not_equal_to_filter_to_empty() {
        let agg = DimensionFilter::AggregateAll;
        let empty_filter = DimensionFilter::FilterTo(vec![]);

        assert_ne!(
            agg, empty_filter,
            "AggregateAll and FilterTo(vec![]) must be distinct"
        );
    }

    /// DimensionFilter::FilterTo must compare by its contained Vec contents.
    #[test]
    fn dimension_filter_filter_to_equality_by_contents() {
        let a = DimensionFilter::FilterTo(vec![("InstanceId".to_string(), "i-abc".to_string())]);
        let b = DimensionFilter::FilterTo(vec![("InstanceId".to_string(), "i-abc".to_string())]);
        let c = DimensionFilter::FilterTo(vec![("InstanceId".to_string(), "i-xyz".to_string())]);

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    /// MetricDescriptor must be cloneable and Hash-able (used in HashSet dedup).
    #[test]
    fn metric_descriptor_clone_and_hash() {
        use std::collections::HashSet;

        let d = MetricDescriptor {
            metric_name: "CPUUtilization".to_string(),
            dimensions: vec![("InstanceId".to_string(), "i-abc".to_string())],
        };
        let d2 = d.clone();

        assert_eq!(d, d2);

        let mut set = HashSet::new();
        set.insert(d);
        // Inserting the clone must not grow the set (same hash + equal).
        set.insert(d2);
        assert_eq!(set.len(), 1);
    }
}
