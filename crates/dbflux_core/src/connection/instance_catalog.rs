use crate::{DbError, QueryResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Stable, driver-defined ID for a single series-style metric or inspector.
///
/// Format convention: lowercase ASCII, dot-separated, e.g. `pg.tx_commit_rate`.
/// Used as the primary key in `SavedChartSource::InstanceMetric { metric_id }`
/// and in `ExecutionSourceContext::InstanceMetricQuery { metric_id, .. }`.
pub type InstanceMetricId = String;

/// Unit of measurement for a chartable metric series.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceMetricUnit {
    Count,
    Bytes,
    Percent,
    PerSecond,
    Milliseconds,
    Unknown,
}

/// Definition of a chartable series exposed by a connection.
///
/// Drivers return these from `InstanceCatalog::list_metrics()`. The UI uses
/// `display_name`, `group`, and `unit` for labelling; `id` is the stable
/// key used in persistence and execution-context routing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceMetricDef {
    pub id: InstanceMetricId,
    pub display_name: String,
    /// Logical grouping shown in the sidebar folder, e.g. "Throughput", "Cache".
    pub group: String,
    pub unit: InstanceMetricUnit,
    pub description: Option<String>,
    /// Suggested refresh interval in seconds; always >= 10.
    pub default_refresh_secs: u32,
}

/// Definition of a tabular inspector entry (process list, top queries, …).
///
/// Drivers return these from `InstanceCatalog::list_inspectors()`. The UI uses
/// `display_name` for labelling; `id` is the stable key used in routing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceInspectorDef {
    /// Stable, driver-scoped ID (same namespace as metric IDs; values must be distinct).
    pub id: InstanceMetricId,
    pub display_name: String,
    pub description: Option<String>,
    /// Suggested refresh interval in seconds; always >= 10.
    pub default_refresh_secs: u32,
}

/// Trait for drivers that expose live operational metrics and inspector snapshots.
///
/// Implementations may probe the database (e.g. check `pg_extension`) during
/// `list_metrics` / `list_inspectors`. Results are NOT cached across reconnects;
/// callers should re-probe after a reconnect event.
///
/// The return type for series data is:
/// - First column: `ColumnKind::Timestamp` (epoch milliseconds)
/// - Remaining columns: `ColumnKind::Float`
///
/// Drivers MUST NOT return other column kinds from `fetch_metric_series`.
#[async_trait]
pub trait InstanceCatalog: Send + Sync {
    /// Lists the series-style metrics currently available on this connection.
    async fn list_metrics(&self) -> Result<Vec<InstanceMetricDef>, DbError>;

    /// Lists the tabular inspector entries currently available.
    async fn list_inspectors(&self) -> Result<Vec<InstanceInspectorDef>, DbError>;

    /// Fetches a single metric series over `[start_ms, end_ms]` (epoch ms).
    ///
    /// Returns a `QueryResult` with one `Timestamp` column followed by one
    /// or more `Float` columns. Drivers MUST NOT include non-numeric columns.
    async fn fetch_metric_series(
        &self,
        metric_id: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<QueryResult, DbError>;

    /// Fetches a single inspector snapshot. Returns a `QueryResult` with
    /// arbitrary columns (driver-defined). Always reflects the current moment;
    /// no time window applies.
    async fn fetch_inspector_snapshot(&self, metric_id: &str) -> Result<QueryResult, DbError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_metric_def_serde_roundtrip() {
        let def = InstanceMetricDef {
            id: "pg.tx_commit_rate".to_string(),
            display_name: "Commits / sec".to_string(),
            group: "Throughput".to_string(),
            unit: InstanceMetricUnit::PerSecond,
            description: Some("Transaction commit rate".to_string()),
            default_refresh_secs: 30,
        };

        let json = serde_json::to_string(&def).unwrap();
        let restored: InstanceMetricDef = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.id, def.id);
        assert_eq!(restored.display_name, def.display_name);
        assert_eq!(restored.group, def.group);
        assert_eq!(restored.unit, def.unit);
        assert_eq!(restored.description, def.description);
        assert_eq!(restored.default_refresh_secs, def.default_refresh_secs);
    }

    #[test]
    fn instance_inspector_def_serde_roundtrip() {
        let def = InstanceInspectorDef {
            id: "pg.activity".to_string(),
            display_name: "Active sessions".to_string(),
            description: Some("pg_stat_activity snapshot".to_string()),
            default_refresh_secs: 10,
        };

        let json = serde_json::to_string(&def).unwrap();
        let restored: InstanceInspectorDef = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.id, def.id);
        assert_eq!(restored.display_name, def.display_name);
        assert_eq!(restored.description, def.description);
        assert_eq!(restored.default_refresh_secs, def.default_refresh_secs);
    }

    #[test]
    fn instance_metric_unit_variants_roundtrip() {
        for unit in [
            InstanceMetricUnit::Count,
            InstanceMetricUnit::Bytes,
            InstanceMetricUnit::Percent,
            InstanceMetricUnit::PerSecond,
            InstanceMetricUnit::Milliseconds,
            InstanceMetricUnit::Unknown,
        ] {
            let json = serde_json::to_string(&unit).unwrap();
            let restored: InstanceMetricUnit = serde_json::from_str(&json).unwrap();
            assert_eq!(restored, unit);
        }
    }

    /// Verify that `Box<dyn InstanceCatalog + Send + Sync>` is usable across
    /// thread boundaries. This test will fail to compile if the trait is not
    /// Send + Sync, and will fail at runtime if the thread panics.
    #[test]
    fn instance_catalog_trait_object_is_send_sync() {
        use std::sync::Arc;

        struct FakeCatalog;

        #[async_trait]
        impl InstanceCatalog for FakeCatalog {
            async fn list_metrics(&self) -> Result<Vec<InstanceMetricDef>, DbError> {
                Ok(vec![])
            }

            async fn list_inspectors(&self) -> Result<Vec<InstanceInspectorDef>, DbError> {
                Ok(vec![])
            }

            async fn fetch_metric_series(
                &self,
                _metric_id: &str,
                _start_ms: i64,
                _end_ms: i64,
            ) -> Result<QueryResult, DbError> {
                Err(DbError::NotSupported("test".to_string()))
            }

            async fn fetch_inspector_snapshot(
                &self,
                _metric_id: &str,
            ) -> Result<QueryResult, DbError> {
                Err(DbError::NotSupported("test".to_string()))
            }
        }

        let catalog: Arc<dyn InstanceCatalog + Send + Sync> = Arc::new(FakeCatalog);
        let catalog_clone = catalog.clone();

        let handle = std::thread::spawn(move || {
            let _ = catalog_clone.as_ref();
            true
        });

        assert!(handle.join().unwrap());
    }
}
