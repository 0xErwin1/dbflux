//! Dashboard import seam for DBFlux.
//!
//! Defines the `DashboardImporter` trait and `PanelImportSpec` value type.
//! Drivers that can parse a dashboard JSON blob implement `DashboardImporter`
//! and advertise `DriverCapabilities::DASHBOARD_IMPORT`; all others inherit
//! the default `None` from `Connection::dashboard_importer()`.
//!
//! The UI never inspects `driver_id` — it calls `metadata.capabilities.contains(DASHBOARD_IMPORT)`
//! to decide whether to show the affordance.

use crate::DbError;

/// Specification for a single panel extracted from an imported dashboard.
///
/// All fields use owned `String`/`Vec` values so the result can be processed
/// after the raw JSON is discarded.
#[derive(Debug, Clone, PartialEq)]
pub struct PanelImportSpec {
    /// Human-readable title for the panel (sourced from the widget's title in the dashboard JSON).
    pub title: String,

    /// CloudWatch namespace (e.g. `"AWS/EC2"`).
    pub namespace: String,

    /// Metric name within the namespace (e.g. `"CPUUtilization"`).
    pub metric_name: String,

    /// Ordered list of dimension key-value pairs (e.g. `[("InstanceId", "i-12345")]`).
    pub dimensions: Vec<(String, String)>,

    /// Sampling period in seconds (e.g. `60`).
    pub period_seconds: u32,

    /// CloudWatch statistic (e.g. `"Average"`, `"Sum"`).
    pub statistic: String,

    /// AWS region override. `None` means use the connection's default region.
    pub region: Option<String>,
}

/// Parses a raw dashboard JSON string into a list of `PanelImportSpec` values.
///
/// Drivers that can import dashboards implement this trait and return `Some(&self.importer)`
/// from `Connection::dashboard_importer()`. Drivers without this capability inherit the
/// default `None` return.
pub trait DashboardImporter {
    /// Parse `json` and return one `PanelImportSpec` per importable panel.
    ///
    /// An empty widget array is valid and returns `Ok(vec![])`.
    ///
    /// # Errors
    ///
    /// Returns `Err(DbError::Parse(...))` for syntactically invalid JSON and
    /// `Err(DbError::Unsupported(...))` when any widget type is not supported by the importer.
    fn import(&self, json: &str) -> Result<Vec<PanelImportSpec>, DbError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_panel_import_spec_fields_accessible() {
        let spec = PanelImportSpec {
            title: "My Panel".to_string(),
            namespace: "AWS/EC2".to_string(),
            metric_name: "CPUUtilization".to_string(),
            dimensions: vec![("InstanceId".to_string(), "i-12345".to_string())],
            period_seconds: 60,
            statistic: "Average".to_string(),
            region: Some("us-east-1".to_string()),
        };

        // Verify all fields are accessible (compile-time check with runtime values).
        let _ = &spec.title;
        let _ = &spec.namespace;
        let _ = &spec.metric_name;
        let _ = &spec.dimensions;
        let _ = spec.period_seconds;
        let _ = &spec.statistic;
        let _ = &spec.region;
    }
}
