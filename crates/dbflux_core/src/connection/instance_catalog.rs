use crate::{DbError, QueryResult, Value};
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

/// One panel entry in a synthesized default dashboard.
///
/// Carries the metric or inspector ID, a layout hint, and whether the panel
/// is a chart or an inspector table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultDashboardPanel {
    /// Metric or inspector ID (e.g. `"pg.tps"`, `"pg.activity"`).
    pub metric_id: String,
    /// True when this panel is an inspector (tabular snapshot), false for metric charts.
    pub is_inspector: bool,
    /// Zero-based grid column (0..=11 on a 12-column grid).
    pub grid_column: u32,
    /// Zero-based grid row.
    pub grid_row: u32,
    /// Number of grid columns this panel spans (1..=12).
    pub grid_width: u32,
    /// Number of grid rows this panel spans.
    pub grid_height: u32,
}

/// Synthesized descriptor for a driver's default "Instance Overview" dashboard.
///
/// Returned by `InstanceCatalog::default_dashboard()`. The UI constructs a
/// read-only `DashboardDocument` from this at open time; no database writes
/// are performed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultInstanceDashboard {
    pub title: String,
    pub description: Option<String>,
    pub panels: Vec<DefaultDashboardPanel>,
}

/// A row-level action exposed by an inspector panel (e.g. "Kill connection").
///
/// Returned by `InstanceCatalog::row_actions()`. The UI renders these as a
/// right-click context menu in the inspector data grid. `is_destructive`
/// signals that the action requires a confirmation modal before execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectorRowAction {
    /// Unique action identifier, stable across calls (used as a key in `execute_row_action`).
    pub id: String,
    /// Human-readable label shown in the context menu.
    pub label: String,
    /// Optional longer description shown in the confirmation modal.
    pub description: Option<String>,
    /// When `true`, the UI shows a confirmation modal before executing.
    pub is_destructive: bool,
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

    /// Returns a synthesized descriptor for this driver's default "Instance Overview"
    /// dashboard, or `None` when the driver does not define a curated layout.
    ///
    /// The default implementation returns `None`. Drivers that support
    /// `INSTANCE_METRICS` or `INSTANCE_INSPECTOR` should override this to
    /// provide a curated metric/inspector layout.
    fn default_dashboard(&self) -> Option<DefaultInstanceDashboard> {
        None
    }

    /// Returns the row-level actions available for the given inspector `metric_id`.
    ///
    /// Each action is shown in the inspector's right-click context menu. The
    /// default implementation returns an empty list (no row actions).
    fn row_actions(&self, metric_id: &str) -> Vec<InspectorRowAction> {
        let _ = metric_id;
        Vec::new()
    }

    /// Execute a row-level action identified by `action_id` on a specific row.
    ///
    /// `row_values` contains the values of every column in the clicked row, in
    /// column order as returned by the inspector snapshot. The driver is
    /// responsible for extracting the relevant identifier (e.g. PID, session ID)
    /// from the correct column index.
    ///
    /// Returns `Ok(())` on success. The default implementation returns
    /// `DbError::NotSupported`.
    async fn execute_row_action(
        &self,
        metric_id: &str,
        action_id: &str,
        row_values: &[Value],
    ) -> Result<(), DbError> {
        let _ = (metric_id, action_id, row_values);
        Err(DbError::NotSupported(format!(
            "row action '{action_id}' not supported"
        )))
    }
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

    /// BF7: `DefaultInstanceDashboard` and `DefaultDashboardPanel` must round-trip
    /// through serde without data loss.
    #[test]
    fn default_instance_dashboard_serde_roundtrip() {
        let dashboard = DefaultInstanceDashboard {
            title: "Instance Overview".to_string(),
            description: Some("Key server metrics".to_string()),
            panels: vec![
                DefaultDashboardPanel {
                    metric_id: "pg.tps".to_string(),
                    is_inspector: false,
                    grid_column: 0,
                    grid_row: 0,
                    grid_width: 6,
                    grid_height: 3,
                },
                DefaultDashboardPanel {
                    metric_id: "pg.activity".to_string(),
                    is_inspector: true,
                    grid_column: 0,
                    grid_row: 3,
                    grid_width: 12,
                    grid_height: 4,
                },
            ],
        };

        let json = serde_json::to_string(&dashboard).unwrap();
        let restored: DefaultInstanceDashboard = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.title, dashboard.title);
        assert_eq!(restored.panels.len(), 2);
        assert_eq!(restored.panels[0].metric_id, "pg.tps");
        assert!(!restored.panels[0].is_inspector);
        assert_eq!(restored.panels[1].metric_id, "pg.activity");
        assert!(restored.panels[1].is_inspector);
    }

    /// BF7: the default `default_dashboard()` implementation on the trait
    /// returns `None` — drivers that don't override it produce no layout.
    #[test]
    fn default_dashboard_default_impl_returns_none() {
        struct MinimalCatalog;

        #[async_trait::async_trait]
        impl InstanceCatalog for MinimalCatalog {
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

        let catalog = MinimalCatalog;
        assert!(
            catalog.default_dashboard().is_none(),
            "default_dashboard() must return None when not overridden"
        );
    }

    /// BF8: `InspectorRowAction` must round-trip through serde.
    #[test]
    fn inspector_row_action_serde_roundtrip() {
        let action = InspectorRowAction {
            id: "kill".to_string(),
            label: "Kill connection".to_string(),
            description: Some("Terminates the backend process.".to_string()),
            is_destructive: true,
        };

        let json = serde_json::to_string(&action).unwrap();
        let restored: InspectorRowAction = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.id, action.id);
        assert_eq!(restored.label, action.label);
        assert_eq!(restored.description, action.description);
        assert_eq!(restored.is_destructive, action.is_destructive);
    }

    /// BF8: the default `row_actions()` implementation returns an empty vec.
    #[test]
    fn row_actions_default_impl_returns_empty() {
        struct MinimalCatalog2;

        #[async_trait::async_trait]
        impl InstanceCatalog for MinimalCatalog2 {
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

        let catalog = MinimalCatalog2;
        let actions = catalog.row_actions("any.inspector");
        assert!(
            actions.is_empty(),
            "row_actions() must return empty vec when not overridden"
        );
    }

    /// BF8: the default `execute_row_action()` implementation returns NotSupported.
    #[test]
    fn execute_row_action_default_returns_not_supported() {
        struct MinimalCatalog3;

        #[async_trait::async_trait]
        impl InstanceCatalog for MinimalCatalog3 {
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

        let catalog = MinimalCatalog3;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(catalog.execute_row_action("any.inspector", "kill", &[]));
        assert!(
            matches!(result, Err(DbError::NotSupported(_))),
            "execute_row_action() must return NotSupported when not overridden"
        );
    }
}
