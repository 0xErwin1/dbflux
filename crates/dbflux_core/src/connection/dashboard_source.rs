//! Dashboard source seam for DBFlux.
//!
//! Defines the `DashboardSource` trait that drivers implement to list and
//! fetch dashboards from an upstream system (e.g. CloudWatch). Drivers must
//! advertise `DriverCapabilities::DASHBOARD_SYNC`. All other drivers inherit
//! the default `None` return from `Connection::dashboard_source()`.
//!
//! Dashboards are browsed read-only: the UI lists them via `list_dashboards`
//! and opens one by fetching its body with `fetch_dashboard`, parsing it in
//! memory. Nothing is persisted locally.

use crate::DbError;

/// A dashboard fetched from upstream (e.g. CloudWatch `GetDashboard`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDashboard {
    /// Dashboard name as reported by upstream.
    pub name: String,
    /// Raw upstream JSON body, exactly as returned.
    pub body_json: String,
    /// `lastModified` from the listing call, ISO8601 when known.
    pub last_modified: Option<String>,
}

/// A reference to an upstream dashboard, returned from listing calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardRef {
    /// Dashboard name as reported by upstream.
    pub name: String,
    /// `lastModified` from the listing call, ISO8601 when known.
    pub last_modified: Option<String>,
}

/// Trait implemented by drivers that can list and fetch upstream dashboards.
///
/// Drivers register an instance via `Connection::dashboard_source()` and
/// MUST advertise `DriverCapabilities::DASHBOARD_SYNC` in their metadata.
///
/// Methods are synchronous and block internally on the driver's own async
/// runtime (mirroring `MetricCatalog`). Callers invoke them from a background
/// executor; they must NOT be called on the UI thread.
pub trait DashboardSource: Send + Sync {
    /// Fetches the dashboard named `name` and returns its raw body.
    fn fetch_dashboard(&self, name: &str) -> Result<RemoteDashboard, DbError>;

    /// Lists dashboards available in the upstream account / region.
    fn list_dashboards(&self) -> Result<Vec<DashboardRef>, DbError>;

    /// Label for the sidebar container that lists these dashboards.
    ///
    /// The UI reads this generically so it never hard-codes a driver-specific
    /// name. Defaults to `"Dashboards"`.
    fn container_label(&self) -> &str {
        "Dashboards"
    }
}
