//! CloudWatch Dashboard JSON importer.
//!
//! Parses the JSON body of a CloudWatch dashboard (as obtained from the AWS
//! console "Share" → "Copy JSON") and extracts `PanelImportSpec` records for
//! each metric widget. Non-metric widgets (e.g., `text`, `alarm`) cause the
//! import to fail with `DbError::Unsupported`.
//!
//! # Expected JSON shape
//!
//! ```json
//! {
//!   "widgets": [
//!     {
//!       "type": "metric",
//!       "properties": {
//!         "title": "CPU Utilization",
//!         "metrics": [
//!           [ "AWS/EC2", "CPUUtilization", "InstanceId", "i-1234" ]
//!         ],
//!         "period": 300,
//!         "stat": "Average",
//!         "region": "us-east-1"
//!       }
//!     }
//!   ]
//! }
//! ```
//!
//! The `metrics` array uses the CloudWatch shorthand form: each element is an
//! array where index 0 is the namespace, index 1 is the metric name, and the
//! remaining pairs are dimension key/value.

use dbflux_core::{DashboardImporter, DbError, PanelImportSpec};
use serde_json::Value;

pub struct CloudWatchDashboardImporter;

impl DashboardImporter for CloudWatchDashboardImporter {
    fn import(&self, json: &str) -> Result<Vec<PanelImportSpec>, DbError> {
        let root: Value = serde_json::from_str(json)
            .map_err(|e| DbError::Parse(format!("CloudWatch dashboard JSON is not valid: {e}")))?;

        let widgets = root
            .get("widgets")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                DbError::Parse("CloudWatch dashboard JSON missing 'widgets' array".to_string())
            })?;

        let mut specs: Vec<PanelImportSpec> = Vec::with_capacity(widgets.len());

        for widget in widgets {
            let widget_type = widget.get("type").and_then(|v| v.as_str()).unwrap_or("");

            if widget_type != "metric" {
                return Err(DbError::Unsupported(format!(
                    "CloudWatch dashboard contains a non-metric widget of type '{}'; \
                     only metric widgets are supported",
                    widget_type
                )));
            }

            let props = widget
                .get("properties")
                .ok_or_else(|| DbError::Parse("metric widget missing 'properties'".to_string()))?;

            let title = props
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let region = props
                .get("region")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let period_s = props.get("period").and_then(|v| v.as_u64()).unwrap_or(300) as u32;

            let statistic = props
                .get("stat")
                .and_then(|v| v.as_str())
                .unwrap_or("Average")
                .to_string();

            // The `metrics` array: each entry is a shorthand array
            // `[namespace, metric_name, dim_key, dim_value, ...]`.
            let metrics_arr = props
                .get("metrics")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    DbError::Parse(
                        "metric widget 'properties.metrics' is missing or not an array".to_string(),
                    )
                })?;

            for metric_entry in metrics_arr {
                let entry = metric_entry.as_array().ok_or_else(|| {
                    DbError::Parse(
                        "each entry in 'metrics' must be an array (shorthand form)".to_string(),
                    )
                })?;

                let namespace = entry
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| DbError::Parse("metric entry missing namespace".to_string()))?
                    .to_string();

                let metric_name = entry
                    .get(1)
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| DbError::Parse("metric entry missing metric_name".to_string()))?
                    .to_string();

                // Remaining pairs are dimension key/value (indices 2,3 then 4,5 …)
                let mut dimensions: Vec<(String, String)> = Vec::new();
                let mut i = 2;
                while i + 1 < entry.len() {
                    let key = entry.get(i).and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let val = entry.get(i + 1).and_then(|v| v.as_str()).unwrap_or("").to_string();
                    if !key.is_empty() {
                        dimensions.push((key, val));
                    }
                    i += 2;
                }

                specs.push(PanelImportSpec {
                    title: title.clone(),
                    namespace,
                    metric_name,
                    dimensions,
                    period_seconds: period_s,
                    statistic: statistic.clone(),
                    region: region.clone(),
                });
            }
        }

        Ok(specs)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// H.1 — `test_cloudwatch_import_metric_only_fixture`: a JSON string with
    /// 2 distinct metric widgets; assert `import()` returns `Ok(vec)` with 2
    /// entries.
    #[test]
    fn test_cloudwatch_import_metric_only_fixture() {
        let json = r#"{
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "title": "CPU Utilization",
                        "metrics": [
                            ["AWS/EC2", "CPUUtilization", "InstanceId", "i-1234"]
                        ],
                        "period": 300,
                        "stat": "Average",
                        "region": "us-east-1"
                    }
                },
                {
                    "type": "metric",
                    "properties": {
                        "title": "Network In",
                        "metrics": [
                            ["AWS/EC2", "NetworkIn", "InstanceId", "i-5678"]
                        ],
                        "period": 60,
                        "stat": "Sum",
                        "region": "eu-west-1"
                    }
                }
            ]
        }"#;

        let importer = CloudWatchDashboardImporter;
        let result = importer.import(json).expect("should parse valid fixture");

        assert_eq!(result.len(), 2, "expected 2 panel specs");

        assert_eq!(result[0].title, "CPU Utilization");
        assert_eq!(result[0].namespace, "AWS/EC2");
        assert_eq!(result[0].metric_name, "CPUUtilization");
        assert_eq!(result[0].period_seconds, 300);
        assert_eq!(result[0].statistic, "Average");
        assert_eq!(result[0].region, Some("us-east-1".to_string()));
        assert_eq!(
            result[0].dimensions,
            vec![("InstanceId".to_string(), "i-1234".to_string())]
        );

        assert_eq!(result[1].title, "Network In");
        assert_eq!(result[1].namespace, "AWS/EC2");
        assert_eq!(result[1].metric_name, "NetworkIn");
        assert_eq!(result[1].period_seconds, 60);
        assert_eq!(result[1].statistic, "Sum");
        assert_eq!(result[1].region, Some("eu-west-1".to_string()));
        assert_eq!(
            result[1].dimensions,
            vec![("InstanceId".to_string(), "i-5678".to_string())]
        );
    }

    /// H.1 — `test_cloudwatch_import_rejects_text_widget`: a JSON string with
    /// one metric and one text widget; assert `import()` returns
    /// `Err(DbError::Unsupported(...))` and the error message contains "text".
    #[test]
    fn test_cloudwatch_import_rejects_text_widget() {
        let json = r##"{
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "title": "OK",
                        "metrics": [["AWS/EC2", "CPUUtilization"]],
                        "period": 300,
                        "stat": "Average"
                    }
                },
                {
                    "type": "text",
                    "properties": {
                        "markdown": "# Header"
                    }
                }
            ]
        }"##;

        let importer = CloudWatchDashboardImporter;
        let result = importer.import(json);

        assert!(result.is_err(), "should fail for non-metric widget");
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("text"),
            "error must mention 'text': {err_msg}"
        );
    }

    /// H.1 — `test_cloudwatch_import_empty_widgets`: `{ "widgets": [] }` must
    /// return `Ok(vec![])`.
    #[test]
    fn test_cloudwatch_import_empty_widgets() {
        let json = r#"{ "widgets": [] }"#;
        let importer = CloudWatchDashboardImporter;
        let result = importer.import(json).expect("empty widgets must return Ok");
        assert!(result.is_empty(), "expected empty result for empty widgets");
    }

    /// H.1 — `test_cloudwatch_import_malformed_json`: syntactically invalid
    /// JSON must return `Err(DbError::Parse(...))`.
    #[test]
    fn test_cloudwatch_import_malformed_json() {
        let json = "{ not valid json";
        let importer = CloudWatchDashboardImporter;
        let result = importer.import(json);
        assert!(result.is_err(), "malformed JSON must return Err");
        assert!(
            matches!(result.unwrap_err(), DbError::Parse(_)),
            "expected DbError::Parse"
        );
    }
}
