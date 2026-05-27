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

/// A single source-imported widget.
///
/// Each `WidgetImportSpec` corresponds to one widget in the source dashboard
/// — regardless of how many metric series it contains. The caller scales
/// `layout` once per widget when constructing dashboard panels and dispatches
/// on `kind` to decide whether to create a metric chart or a text divider.
#[derive(Debug, Clone, PartialEq)]
pub struct WidgetImportSpec {
    /// Human-readable title sourced from the widget's title in the dashboard JSON.
    /// Empty when the widget has no title (notably for text/divider widgets).
    pub title: String,

    /// Source-coordinate layout for the widget.
    ///
    /// CloudWatch dashboards use a 24-column grid. The caller is responsible
    /// for scaling these to the dashboard's own column count.
    pub layout: WidgetLayout,

    /// Widget kind discriminator.
    pub kind: WidgetImportKind,
}

/// Discriminated body of a `WidgetImportSpec`.
#[derive(Debug, Clone, PartialEq)]
pub enum WidgetImportKind {
    /// A metric widget — one or more CloudWatch series rendered as a chart.
    Metric {
        view: MetricView,
        series: Vec<ImportedMetricSeries>,
    },
    /// A text/markdown widget — a divider panel showing a markdown string.
    TextDivider { markdown: String },
}

/// Rendering preference for a metric widget.
///
/// `TimeSeries` produces a line chart; `SingleValue` produces a Number/single
/// stat chart. CloudWatch's `properties.view` field maps directly:
/// `"timeSeries"` (default) → `TimeSeries`, `"singleValue"` → `SingleValue`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricView {
    TimeSeries,
    SingleValue,
}

/// One metric series extracted from a CloudWatch metric widget's `metrics` array.
///
/// All fields use owned `String`/`Vec` values so the spec can be processed
/// after the raw JSON is discarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedMetricSeries {
    /// CloudWatch namespace (e.g. `"AWS/EC2"`).
    pub namespace: String,
    /// Metric name within the namespace (e.g. `"CPUUtilization"`).
    pub metric_name: String,
    /// Ordered list of dimension key-value pairs.
    pub dimensions: Vec<(String, String)>,
    /// Sampling period in seconds (e.g. `60`).
    pub period_seconds: u32,
    /// CloudWatch statistic (e.g. `"Average"`, `"Sum"`).
    pub statistic: String,
    /// AWS region override. `None` means use the connection's default region.
    pub region: Option<String>,
    /// Optional explicit label from the trailing options object (`{"label": "..."}`).
    pub label: Option<String>,
}

/// Source-coordinate layout for a CloudWatch (or future) dashboard widget.
///
/// Values are in the dashboard JSON's native grid units — for CloudWatch
/// that is a 24-column grid (`x`, `width` in `0..=24`). Row units map 1:1
/// onto DBFlux dashboard row multiples.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WidgetLayout {
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
}

/// Parses a raw dashboard JSON string into a list of `WidgetImportSpec` values.
///
/// Drivers that can import dashboards implement this trait and return `Some(&self.importer)`
/// from `Connection::dashboard_importer()`. Drivers without this capability inherit the
/// default `None` return.
pub trait DashboardImporter {
    /// Parse `json` and return one `WidgetImportSpec` per importable widget.
    ///
    /// An empty widget array is valid and returns `Ok(vec![])`.
    ///
    /// # Errors
    ///
    /// Returns `Err(DbError::Parse(...))` for syntactically invalid JSON and
    /// `Err(DbError::Unsupported(...))` when none of the widgets are importable.
    fn import(&self, json: &str) -> Result<Vec<WidgetImportSpec>, DbError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_widget_import_spec_fields_accessible() {
        let spec = WidgetImportSpec {
            title: "My Panel".to_string(),
            layout: WidgetLayout::default(),
            kind: WidgetImportKind::Metric {
                view: MetricView::TimeSeries,
                series: vec![ImportedMetricSeries {
                    namespace: "AWS/EC2".to_string(),
                    metric_name: "CPUUtilization".to_string(),
                    dimensions: vec![("InstanceId".to_string(), "i-12345".to_string())],
                    period_seconds: 60,
                    statistic: "Average".to_string(),
                    region: Some("us-east-1".to_string()),
                    label: None,
                }],
            },
        };

        // Verify all fields are accessible (compile-time check with runtime values).
        let _ = &spec.title;
        let _ = &spec.layout;
        let _ = &spec.kind;
    }
}
