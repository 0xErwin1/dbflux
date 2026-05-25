//! `SavedChart` — persisted record for a named chart with its query and spec.
//!
//! `SavedChart` is the domain type for a named chart. Persistence is handled by
//! `dbflux_ui_base::SavedChartManager` backed by SQLite.
//!
//! # Crate placement
//!
//! `SavedChart` lives in `dbflux_components` rather than `dbflux_core` because
//! it embeds `ChartSpec` and `BindingSpec`, which are owned by this crate.
//!
//! # Schema note
//!
//! `SavedChartSource` was introduced as a breaking change from the old
//! `query: String` field. The `chart-everywhere` feature was unreleased at the
//! time, so no migration is needed. Old JSON without a `source` field
//! deserialises to `SavedChartSource::Query { query: "" }` via the
//! `#[serde(default)]` path.

use crate::chart::{BindingSpec, ChartSpec};
use chrono::{DateTime, Utc};
use dbflux_core::{CollectionRef, Identifiable, ResolvedWindow};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// SavedChartSource
// ---------------------------------------------------------------------------

/// The data source for a saved chart.
///
/// `Query` wraps a SQL/Flux/etc. query string and is executed inside
/// `ChartDocument`. `Collection` represents a collection-browse source
/// (Mongo collection, InfluxDB measurement) — opening it re-opens the
/// underlying `DataDocument` in chart mode. `Metric` persists a CloudWatch
/// metric source with native typed fields so the import round-trip is
/// lossless and the chart opens via `MetricSource` rather than a raw query.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum SavedChartSource {
    /// A query-string source executed inside ChartDocument.
    Query { query: String },
    /// A collection-browse source (no query string; the driver builds the request).
    Collection {
        collection_ref: CollectionRef,
        /// The time window that was active when the chart was saved, if any.
        time_window: Option<ResolvedWindow>,
    },
    /// A CloudWatch metric source. On open, reconstructs a `MetricSource`
    /// directly rather than routing through the Flux/SQL query path.
    Metric {
        namespace: String,
        metric_name: String,
        /// Ordered (name, value) dimension pairs. Empty for scalar metrics.
        dimensions: Vec<(String, String)>,
        /// Sampling period in seconds.
        period_seconds: u32,
        /// CloudWatch statistic (e.g. "Average", "Sum").
        statistic: String,
        /// AWS region override; `None` means use the connection's default region.
        region: Option<String>,
    },
}

impl Default for SavedChartSource {
    fn default() -> Self {
        SavedChartSource::Query {
            query: String::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// Quick-select time-range presets stored alongside a chart.
///
/// Mirrors the variants in `dbflux_ui::ui::common::time_range::TimeRange` but
/// lives here so `SavedChart` can be (de)serialized without a GPUI dependency.
/// Phase D will bridge between the two types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TimeRangePreset {
    Last15min,
    LastHour,
    Last6Hours,
    #[default]
    Last24Hours,
    Last7Days,
}

/// Refresh behaviour for a saved chart when it is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SavedChartRefreshPolicy {
    /// No automatic refresh; user must trigger re-execution manually.
    #[default]
    Off,
    /// Re-execute the query every `every_secs` seconds.
    Interval { every_secs: u32 },
    /// Re-execute once automatically when the chart is opened.
    OnOpen,
}

// ---------------------------------------------------------------------------
// SavedChart
// ---------------------------------------------------------------------------

/// A persisted chart record.
///
/// Only the query string (or collection reference) is persisted — raw result
/// data is never stored. `chart_spec` and `bindings` carry the full rendering
/// configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedChart {
    /// Stable identity for upsert and deduplication.
    pub id: Uuid,
    /// User-supplied display name.
    pub name: String,
    /// The connection profile this chart was created under.
    pub profile_id: Uuid,
    /// Data source for this chart.
    ///
    /// Old JSON without this field (or with only a `query` top-level key) will
    /// fail to parse; since the chart-everywhere feature was unreleased when
    /// this field was introduced, no migration is needed.
    #[serde(default)]
    pub source: SavedChartSource,
    /// Serialized chart spec. Uses `#[serde(default)]` fields so old JSON
    /// without newer fields is still loadable.
    pub chart_spec: ChartSpec,
    /// Column bindings for the AxisBar.
    pub bindings: BindingSpec,
    /// Optional time-range preset applied when the chart is opened.
    #[serde(default)]
    pub time_range_preset: Option<TimeRangePreset>,
    /// Refresh policy applied while the chart is open.
    #[serde(default)]
    pub refresh_policy: SavedChartRefreshPolicy,
    /// Creation timestamp (UTC).
    pub created_at: DateTime<Utc>,
    /// Last-modified timestamp (UTC); updated on every upsert.
    pub updated_at: DateTime<Utc>,
}

impl SavedChart {
    /// Create a new `SavedChart` from a query string source.
    pub fn new_query(
        name: String,
        profile_id: Uuid,
        query: String,
        chart_spec: ChartSpec,
        bindings: BindingSpec,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            profile_id,
            source: SavedChartSource::Query { query },
            chart_spec,
            bindings,
            time_range_preset: None,
            refresh_policy: SavedChartRefreshPolicy::Off,
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new `SavedChart` from a collection-browse source.
    pub fn new_collection(
        name: String,
        profile_id: Uuid,
        collection_ref: CollectionRef,
        time_window: Option<ResolvedWindow>,
        chart_spec: ChartSpec,
        bindings: BindingSpec,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            profile_id,
            source: SavedChartSource::Collection {
                collection_ref,
                time_window,
            },
            chart_spec,
            bindings,
            time_range_preset: None,
            refresh_policy: SavedChartRefreshPolicy::Off,
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new `SavedChart` from a CloudWatch metric source.
    #[allow(clippy::too_many_arguments)]
    pub fn new_metric(
        name: String,
        profile_id: Uuid,
        namespace: String,
        metric_name: String,
        dimensions: Vec<(String, String)>,
        period_seconds: u32,
        statistic: String,
        region: Option<String>,
        chart_spec: ChartSpec,
        bindings: BindingSpec,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            profile_id,
            source: SavedChartSource::Metric {
                namespace,
                metric_name,
                dimensions,
                period_seconds,
                statistic,
                region,
            },
            chart_spec,
            bindings,
            time_range_preset: None,
            refresh_policy: SavedChartRefreshPolicy::Off,
            created_at: now,
            updated_at: now,
        }
    }

    /// Convenience: returns the query string if this chart has a `Query` source.
    pub fn query(&self) -> Option<&str> {
        match &self.source {
            SavedChartSource::Query { query } => Some(query.as_str()),
            SavedChartSource::Collection { .. } | SavedChartSource::Metric { .. } => None,
        }
    }

    /// Returns `true` if this chart has a `Collection` source.
    pub fn is_collection_source(&self) -> bool {
        matches!(self.source, SavedChartSource::Collection { .. })
    }

    /// Returns `true` if this chart has a `Metric` source.
    pub fn is_metric_source(&self) -> bool {
        matches!(self.source, SavedChartSource::Metric { .. })
    }
}

impl Identifiable for SavedChart {
    fn id(&self) -> Uuid {
        self.id
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::{AggKind, AxisKind, AxisSpec, ChartKind, SeriesSpec};
    use dbflux_core::CollectionRef;

    fn sample_spec() -> ChartSpec {
        ChartSpec {
            kind: ChartKind::Line,
            x_axis: AxisSpec {
                column_index: 0,
                label: "time".to_string(),
                kind: AxisKind::Time,
                unit: None,
            },
            series: vec![SeriesSpec {
                column_index: 1,
                label: "value".to_string(),
                color_slot: 0,
            }],
            legend_visible: false,
            decimation_threshold: 10_000,
            binding: BindingSpec::default(),
            track_source_indices: false,
            y_scale: crate::chart::YScale::Linear,
        }
    }

    fn sample_chart(name: &str, profile_id: Uuid) -> SavedChart {
        SavedChart::new_query(
            name.to_string(),
            profile_id,
            "SELECT * FROM test".to_string(),
            sample_spec(),
            BindingSpec {
                x: 0,
                y: vec![1],
                group_by: None,
                filter: None,
                aggregation: AggKind::None,
            },
        )
    }

    fn sample_collection_chart(name: &str, profile_id: Uuid) -> SavedChart {
        SavedChart::new_collection(
            name.to_string(),
            profile_id,
            CollectionRef::new("mydb", "measurements"),
            None,
            sample_spec(),
            BindingSpec {
                x: 0,
                y: vec![1],
                group_by: None,
                filter: None,
                aggregation: AggKind::None,
            },
        )
    }

    fn sample_metric_chart(name: &str, profile_id: Uuid) -> SavedChart {
        SavedChart::new_metric(
            name.to_string(),
            profile_id,
            "AWS/EC2".to_string(),
            "CPUUtilization".to_string(),
            vec![
                ("InstanceId".to_string(), "i-12345".to_string()),
                ("Region".to_string(), "us-east-1".to_string()),
            ],
            300,
            "Average".to_string(),
            Some("us-east-1".to_string()),
            sample_spec(),
            BindingSpec {
                x: 0,
                y: vec![1],
                group_by: None,
                filter: None,
                aggregation: AggKind::None,
            },
        )
    }

    /// Metric source: `query()` returns `None` and `is_metric_source()` returns `true`.
    #[test]
    fn metric_source_query_returns_none() {
        let profile_id = Uuid::new_v4();
        let chart = sample_metric_chart("test", profile_id);
        assert_eq!(chart.query(), None);
        assert!(!chart.is_collection_source());
        assert!(chart.is_metric_source());
    }

    /// Metric source: all fields survive a round-trip through `SavedChartSource`.
    #[test]
    fn metric_source_fields_accessible() {
        let profile_id = Uuid::new_v4();
        let chart = sample_metric_chart("my-chart", profile_id);
        if let SavedChartSource::Metric {
            namespace,
            metric_name,
            dimensions,
            period_seconds,
            statistic,
            region,
        } = &chart.source
        {
            assert_eq!(namespace, "AWS/EC2");
            assert_eq!(metric_name, "CPUUtilization");
            assert_eq!(dimensions.len(), 2);
            assert_eq!(
                dimensions[0],
                ("InstanceId".to_string(), "i-12345".to_string())
            );
            assert_eq!(
                dimensions[1],
                ("Region".to_string(), "us-east-1".to_string())
            );
            assert_eq!(*period_seconds, 300);
            assert_eq!(statistic, "Average");
            assert_eq!(region.as_deref(), Some("us-east-1"));
        } else {
            panic!("expected Metric variant");
        }
    }

    /// T-CE-I07: Query source query() helper returns Some.
    #[test]
    fn query_helper_returns_some_for_query_source() {
        let profile_id = Uuid::new_v4();
        let chart = sample_chart("test", profile_id);
        assert_eq!(chart.query(), Some("SELECT * FROM test"));
        assert!(!chart.is_collection_source());
    }

    /// T-CE-I07: Collection source query() helper returns None.
    #[test]
    fn query_helper_returns_none_for_collection_source() {
        let profile_id = Uuid::new_v4();
        let chart = sample_collection_chart("test", profile_id);
        assert_eq!(chart.query(), None);
        assert!(chart.is_collection_source());
    }
}
