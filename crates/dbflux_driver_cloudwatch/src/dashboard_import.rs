//! CloudWatch Dashboard JSON importer.
//!
//! Parses the JSON body of a CloudWatch dashboard (as obtained from the AWS
//! console "Share" → "Copy JSON") and extracts `PanelImportSpec` records for
//! each metric widget. Non-metric widgets (e.g., `text`, `alarm`, `log`) are
//! silently skipped — they carry no chartable data. The import only fails with
//! `DbError::Unsupported` when the dashboard contains zero metric widgets.
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

use dbflux_core::{
    DashboardImporter, DbError, ImportedMetricSeries, MetricView, WidgetImportKind,
    WidgetImportSpec, WidgetLayout,
};
use serde_json::Value;

pub struct CloudWatchDashboardImporter;

impl DashboardImporter for CloudWatchDashboardImporter {
    fn import(&self, json: &str) -> Result<Vec<WidgetImportSpec>, DbError> {
        let root: Value = serde_json::from_str(json)
            .map_err(|e| DbError::Parse(format!("CloudWatch dashboard JSON is not valid: {e}")))?;

        let widgets = root
            .get("widgets")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                DbError::Parse("CloudWatch dashboard JSON missing 'widgets' array".to_string())
            })?;

        let mut specs: Vec<WidgetImportSpec> = Vec::with_capacity(widgets.len());
        let mut had_importable_widget = false;

        for widget in widgets {
            let widget_type = widget.get("type").and_then(|v| v.as_str()).unwrap_or("");

            // Widget layout in the source's native 24-column grid. Defaulting
            // to a 6×4 cell at (0, 0) keeps very old CloudWatch JSON (without
            // explicit positions) from collapsing into a single overlap.
            let layout = WidgetLayout {
                x: widget.get("x").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                y: widget.get("y").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                width: widget.get("width").and_then(|v| v.as_u64()).unwrap_or(6) as u32,
                height: widget.get("height").and_then(|v| v.as_u64()).unwrap_or(4) as u32,
            };

            match widget_type {
                "metric" => {
                    let widget_spec = parse_metric_widget(widget, layout)?;
                    specs.push(widget_spec);
                    had_importable_widget = true;
                }
                "text" => {
                    let markdown = widget
                        .get("properties")
                        .and_then(|p| p.get("markdown"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    specs.push(WidgetImportSpec {
                        title: String::new(),
                        layout,
                        kind: WidgetImportKind::TextDivider { markdown },
                    });
                    had_importable_widget = true;
                }
                _ => {
                    // alarm / log widgets carry no chartable or markdown data — skip silently.
                    continue;
                }
            }
        }

        if !widgets.is_empty() && !had_importable_widget {
            return Err(DbError::Unsupported(
                "CloudWatch dashboard contains no importable widgets — nothing to import \
                 (only `metric` and `text` widgets are imported; alarm/log widgets are skipped)"
                    .to_string(),
            ));
        }

        Ok(specs)
    }
}

/// Parse a single CloudWatch `metric` widget into a `WidgetImportSpec` whose
/// `kind` is `WidgetImportKind::Metric`. Expands `"..."` and `"."` shorthand
/// tokens and respects the trailing per-metric options object.
fn parse_metric_widget(
    widget: &Value,
    layout: WidgetLayout,
) -> Result<WidgetImportSpec, DbError> {
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

    // CloudWatch's `properties.view` selects the rendering style. The
    // companion `properties.stacked` flag turns a `timeSeries` widget into a
    // filled stacked area; without it the widget renders as plain line
    // series. Anything other than the two documented values falls back to
    // TimeSeries.
    let stacked = props
        .get("stacked")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let view = match props.get("view").and_then(|v| v.as_str()) {
        Some("singleValue") => MetricView::SingleValue,
        _ if stacked => MetricView::StackedArea,
        _ => MetricView::TimeSeries,
    };

    let metrics_arr = props
        .get("metrics")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            DbError::Parse(
                "metric widget 'properties.metrics' is missing or not an array".to_string(),
            )
        })?;

    let mut series: Vec<ImportedMetricSeries> = Vec::with_capacity(metrics_arr.len());
    let mut previous: Option<MetricTuple> = None;

    for metric_entry in metrics_arr {
        let entry = metric_entry.as_array().ok_or_else(|| {
            DbError::Parse(
                "each entry in 'metrics' must be an array (shorthand form)".to_string(),
            )
        })?;

        let (tokens, options) = split_trailing_options(entry);

        let expanded = expand_metric_tokens(&tokens, previous.as_ref())
            .map_err(|e| DbError::Parse(format!("CloudWatch metric entry: {e}")))?;

        let entry_period = options
            .and_then(|o| o.get("period"))
            .and_then(|v| v.as_u64())
            .map(|n| n as u32)
            .unwrap_or(period_s);

        let entry_stat = options
            .and_then(|o| o.get("stat"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| statistic.clone());

        let entry_region = options
            .and_then(|o| o.get("region"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| region.clone());

        let label = options
            .and_then(|o| o.get("label"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        series.push(ImportedMetricSeries {
            namespace: expanded.namespace.clone(),
            metric_name: expanded.metric_name.clone(),
            dimensions: expanded.dimensions.clone(),
            period_seconds: entry_period,
            statistic: entry_stat,
            region: entry_region,
            label,
        });

        previous = Some(expanded);
    }

    Ok(WidgetImportSpec {
        title,
        layout,
        kind: WidgetImportKind::Metric { view, series },
    })
}

/// Expanded form of a single metric entry, used as the "previous metric" memo
/// when expanding `"..."` and `"."` shorthand tokens in later entries.
#[derive(Debug, Clone)]
struct MetricTuple {
    namespace: String,
    metric_name: String,
    /// Ordered dimension `(key, value)` pairs.
    dimensions: Vec<(String, String)>,
}

/// Splits the trailing options object (if present) from a metric-entry array.
///
/// CloudWatch allows the last element of a metric array to be a JSON object
/// containing per-metric overrides like `period`, `region`, `stat`, `label`,
/// `yAxis`, `color`. That object must never be parsed as a dimension token.
fn split_trailing_options(entry: &[Value]) -> (Vec<Value>, Option<&Value>) {
    if let Some(last) = entry.last()
        && last.is_object()
    {
        let head = entry.split_last().map(|(_, head)| head).unwrap_or(&[]);
        (head.to_vec(), Some(last))
    } else {
        (entry.to_vec(), None)
    }
}

/// Expands a metric entry's string tokens into a fully resolved
/// `MetricTuple`, applying CloudWatch shorthand rules against `previous`.
///
/// Rules:
///   * `"..."` at index 0 — inherit `namespace`, `metric_name`, and dim_keys
///     from `previous`. Subsequent tokens supply replacement dim_values in
///     positional order.
///   * `"."` at any index — inherit the same positional element from
///     `previous`.
///   * Any other string — used as a literal value at that position.
///
/// Returns `Err` only when the entry can't be expanded (missing previous when
/// shorthand is used, or no namespace/metric_name resolvable).
fn expand_metric_tokens(
    tokens: &[Value],
    previous: Option<&MetricTuple>,
) -> Result<MetricTuple, String> {
    // Coerce each JSON token into Option<&str>; non-strings become None so
    // that callers can produce a precise error for the offending position.
    let token_strs: Vec<Option<&str>> = tokens.iter().map(|v| v.as_str()).collect();

    // Case 1 — full "..." inheritance: namespace + metric + dim_keys come
    // from `previous`. Remaining tokens are dim_values in order.
    if let Some(first) = token_strs.first().copied().flatten()
        && first == "..."
    {
        let prev = previous.ok_or_else(|| {
            "shorthand '...' used in first metric entry — no previous metric to inherit from"
                .to_string()
        })?;

        // tokens after "..." replace dim_values in order; "." reuses the
        // previous value at that dim position.
        let new_values: Vec<String> = token_strs
            .iter()
            .skip(1)
            .enumerate()
            .map(|(i, tok)| match tok {
                Some(".") => prev
                    .dimensions
                    .get(i)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default(),
                Some(v) => v.to_string(),
                None => String::new(),
            })
            .collect();

        let dimensions: Vec<(String, String)> = prev
            .dimensions
            .iter()
            .enumerate()
            .map(|(i, (k, v))| {
                let new_v = new_values.get(i).cloned().unwrap_or_else(|| v.clone());
                (k.clone(), new_v)
            })
            .collect();

        return Ok(MetricTuple {
            namespace: prev.namespace.clone(),
            metric_name: prev.metric_name.clone(),
            dimensions,
        });
    }

    // Case 2 — positional expansion. Token at index 0 is namespace, index 1
    // is metric_name, and indices 2.. are alternating dim_key/dim_value pairs.
    // Each token may be "." to reuse the previous metric's element at that
    // position.
    let namespace = resolve_token(token_strs.first().copied().flatten(), || {
        previous.map(|p| p.namespace.clone())
    })
    .ok_or_else(|| "metric entry missing namespace".to_string())?;

    let metric_name = resolve_token(token_strs.get(1).copied().flatten(), || {
        previous.map(|p| p.metric_name.clone())
    })
    .ok_or_else(|| "metric entry missing metric_name".to_string())?;

    let mut dimensions: Vec<(String, String)> = Vec::new();
    let mut i = 2usize;
    while i + 1 < token_strs.len() {
        let dim_index = (i - 2) / 2;

        let key_token = token_strs.get(i).copied().flatten();
        let val_token = token_strs.get(i + 1).copied().flatten();

        let key = resolve_token(key_token, || {
            previous
                .and_then(|p| p.dimensions.get(dim_index).map(|(k, _)| k.clone()))
        });
        let val = resolve_token(val_token, || {
            previous
                .and_then(|p| p.dimensions.get(dim_index).map(|(_, v)| v.clone()))
        });

        if let (Some(k), Some(v)) = (key, val)
            && !k.is_empty()
        {
            dimensions.push((k, v));
        }
        i += 2;
    }

    Ok(MetricTuple {
        namespace,
        metric_name,
        dimensions,
    })
}

/// Resolves a single token: literal string wins; `"."` defers to `fallback`;
/// `None` (non-string JSON) also defers to `fallback`.
fn resolve_token(token: Option<&str>, fallback: impl FnOnce() -> Option<String>) -> Option<String> {
    match token {
        Some(".") => fallback(),
        Some(s) => Some(s.to_string()),
        None => fallback(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns the metric series carried by a widget, panicking when the kind
    /// is `TextDivider`. Keeps the assertion blocks below focused on field
    /// values rather than enum destructuring.
    fn series_of(spec: &WidgetImportSpec) -> &[ImportedMetricSeries] {
        match &spec.kind {
            WidgetImportKind::Metric { series, .. } => series.as_slice(),
            WidgetImportKind::TextDivider { .. } => panic!("expected metric widget"),
        }
    }

    fn view_of(spec: &WidgetImportSpec) -> MetricView {
        match &spec.kind {
            WidgetImportKind::Metric { view, .. } => *view,
            WidgetImportKind::TextDivider { .. } => panic!("expected metric widget"),
        }
    }

    /// H.1 — `test_cloudwatch_import_metric_only_fixture`: a JSON string with
    /// 2 distinct metric widgets; assert `import()` returns `Ok(vec)` with 2
    /// widgets, each carrying exactly one series.
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

        assert_eq!(result.len(), 2, "expected 2 widget specs");

        assert_eq!(result[0].title, "CPU Utilization");
        let s0 = series_of(&result[0]);
        assert_eq!(s0.len(), 1);
        assert_eq!(s0[0].namespace, "AWS/EC2");
        assert_eq!(s0[0].metric_name, "CPUUtilization");
        assert_eq!(s0[0].period_seconds, 300);
        assert_eq!(s0[0].statistic, "Average");
        assert_eq!(s0[0].region.as_deref(), Some("us-east-1"));
        assert_eq!(
            s0[0].dimensions,
            vec![("InstanceId".to_string(), "i-1234".to_string())]
        );

        assert_eq!(result[1].title, "Network In");
        let s1 = series_of(&result[1]);
        assert_eq!(s1.len(), 1);
        assert_eq!(s1[0].namespace, "AWS/EC2");
        assert_eq!(s1[0].metric_name, "NetworkIn");
        assert_eq!(s1[0].period_seconds, 60);
        assert_eq!(s1[0].statistic, "Sum");
        assert_eq!(s1[0].region.as_deref(), Some("eu-west-1"));
        assert_eq!(
            s1[0].dimensions,
            vec![("InstanceId".to_string(), "i-5678".to_string())]
        );
    }

    /// `test_cloudwatch_import_mixed_widgets`: a JSON string with one metric
    /// widget and one text widget; assert `import()` returns BOTH — metric as
    /// `WidgetImportKind::Metric` and text as `WidgetImportKind::TextDivider`.
    /// alarm/log widgets are still skipped silently.
    #[test]
    fn test_cloudwatch_import_mixed_widgets() {
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
                },
                {
                    "type": "alarm",
                    "properties": { "alarms": [] }
                }
            ]
        }"##;

        let importer = CloudWatchDashboardImporter;
        let result = importer
            .import(json)
            .expect("mixed widget dashboard must import metric + text");

        assert_eq!(result.len(), 2, "metric and text widgets must produce specs");

        let s = series_of(&result[0]);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].namespace, "AWS/EC2");
        assert_eq!(s[0].metric_name, "CPUUtilization");

        match &result[1].kind {
            WidgetImportKind::TextDivider { markdown } => assert_eq!(markdown, "# Header"),
            other => panic!("expected TextDivider, got {other:?}"),
        }
    }

    /// `test_cloudwatch_import_expands_ellipsis_shorthand`: two metric entries
    /// where the second uses `"..."` to inherit namespace + metric_name + dim
    /// keys from the first, supplying a new dim_value positionally.
    #[test]
    fn test_cloudwatch_import_expands_ellipsis_shorthand() {
        let json = r#"{
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["AWS/RDS", "CPUUtilization", "DBInstanceIdentifier", "primary-db"],
                            ["...", "replica-db"]
                        ],
                        "period": 60,
                        "stat": "Average",
                        "region": "us-east-1"
                    }
                }
            ]
        }"#;

        let importer = CloudWatchDashboardImporter;
        let result = importer.import(json).expect("ellipsis shorthand must expand");

        assert_eq!(result.len(), 1, "two series belong to one widget");
        let series = series_of(&result[0]);
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].namespace, "AWS/RDS");
        assert_eq!(series[0].metric_name, "CPUUtilization");
        assert_eq!(
            series[0].dimensions,
            vec![("DBInstanceIdentifier".to_string(), "primary-db".to_string())]
        );

        // Second series inherits namespace + metric + dim_keys, only dim_value changes.
        assert_eq!(series[1].namespace, "AWS/RDS");
        assert_eq!(series[1].metric_name, "CPUUtilization");
        assert_eq!(
            series[1].dimensions,
            vec![("DBInstanceIdentifier".to_string(), "replica-db".to_string())]
        );
    }

    /// `test_cloudwatch_import_expands_dot_shorthand`: positional `"."` reuses
    /// the element at that index from the previous metric (namespace, dim_key,
    /// or dim_value), while a non-`.` token at index 1 supplies a new metric
    /// name.
    #[test]
    fn test_cloudwatch_import_expands_dot_shorthand() {
        let json = r#"{
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["AWS/DocDB", "ReadLatency", "DBInstanceIdentifier", "cluster-a"],
                            [".", "WriteLatency", ".", "."]
                        ],
                        "period": 60,
                        "stat": "Average",
                        "region": "us-east-1"
                    }
                }
            ]
        }"#;

        let importer = CloudWatchDashboardImporter;
        let result = importer.import(json).expect("dot shorthand must expand");

        assert_eq!(result.len(), 1, "two series belong to one widget");
        let series = series_of(&result[0]);
        assert_eq!(series.len(), 2);
        assert_eq!(series[1].namespace, "AWS/DocDB");
        assert_eq!(series[1].metric_name, "WriteLatency");
        assert_eq!(
            series[1].dimensions,
            vec![("DBInstanceIdentifier".to_string(), "cluster-a".to_string())]
        );
    }

    /// `test_cloudwatch_import_skips_trailing_options_object`: the trailing
    /// `{period, region, stat}` object must NOT be consumed as a dimension
    /// token and its values must override the widget-level defaults.
    #[test]
    fn test_cloudwatch_import_skips_trailing_options_object() {
        let json = r#"{
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["AWS/EC2", "CPUUtilization", "InstanceId", "i-1",
                                {"period": 60, "stat": "Sum", "region": "eu-west-2"}]
                        ],
                        "period": 300,
                        "stat": "Average",
                        "region": "us-east-1"
                    }
                }
            ]
        }"#;

        let importer = CloudWatchDashboardImporter;
        let result = importer
            .import(json)
            .expect("trailing options object must be skipped, not parsed as dimension");

        assert_eq!(result.len(), 1);
        let series = series_of(&result[0]);
        assert_eq!(series.len(), 1);

        // Dim list contains exactly the InstanceId pair — no stray entries
        // from the options object.
        assert_eq!(
            series[0].dimensions,
            vec![("InstanceId".to_string(), "i-1".to_string())]
        );
        // Per-metric overrides win over widget defaults.
        assert_eq!(series[0].period_seconds, 60);
        assert_eq!(series[0].statistic, "Sum");
        assert_eq!(series[0].region.as_deref(), Some("eu-west-2"));
    }

    /// `test_cloudwatch_import_ellipsis_without_previous_errors`: using `"..."`
    /// as the very first metric entry has no prior metric to inherit from and
    /// must surface a `DbError::Parse`, not silently produce garbage.
    #[test]
    fn test_cloudwatch_import_ellipsis_without_previous_errors() {
        let json = r#"{
            "widgets": [{
                "type": "metric",
                "properties": {
                    "metrics": [["...", "i-1"]],
                    "period": 60,
                    "stat": "Average"
                }
            }]
        }"#;

        let importer = CloudWatchDashboardImporter;
        let err = importer
            .import(json)
            .expect_err("'...' as first entry must fail with no previous metric");
        assert!(matches!(err, DbError::Parse(_)), "got {err:?}");
    }

    /// `test_cloudwatch_import_all_unsupported_fails`: a JSON string whose
    /// widgets are exclusively alarm/log (no metric, no text) must return
    /// `Err(DbError::Unsupported(...))` so the user is notified that nothing
    /// importable was found. Text widgets now ARE importable as dividers, so
    /// this assertion uses alarm/log only.
    #[test]
    fn test_cloudwatch_import_all_unsupported_fails() {
        let json = r##"{
            "widgets": [
                { "type": "alarm", "properties": { "alarms": [] } },
                { "type": "log",   "properties": {} }
            ]
        }"##;

        let importer = CloudWatchDashboardImporter;
        let err = importer
            .import(json)
            .expect_err("all-unsupported dashboard must fail");

        assert!(
            matches!(err, DbError::Unsupported(_)),
            "expected DbError::Unsupported, got {err:?}"
        );
    }

    /// Layout fields are propagated verbatim from the source JSON so the
    /// downstream caller can perform a deterministic 24→12 scale.
    #[test]
    fn test_cloudwatch_import_preserves_widget_layout() {
        let json = r##"{
            "widgets": [
                {
                    "type": "metric",
                    "x": 12, "y": 6, "width": 8, "height": 3,
                    "properties": {
                        "metrics": [["AWS/EC2", "CPUUtilization"]],
                        "period": 60,
                        "stat": "Average"
                    }
                },
                {
                    "type": "text",
                    "x": 0, "y": 0, "width": 24, "height": 1,
                    "properties": { "markdown": "# Overview" }
                }
            ]
        }"##;

        let importer = CloudWatchDashboardImporter;
        let result = importer.import(json).expect("must parse");

        assert_eq!(result[0].layout.x, 12);
        assert_eq!(result[0].layout.y, 6);
        assert_eq!(result[0].layout.width, 8);
        assert_eq!(result[0].layout.height, 3);

        assert_eq!(result[1].layout.x, 0);
        assert_eq!(result[1].layout.width, 24);
        assert!(matches!(
            result[1].kind,
            WidgetImportKind::TextDivider { .. }
        ));
    }

    /// `singleValue` view widgets map to `MetricView::SingleValue`; missing or
    /// unknown view values fall back to `TimeSeries`.
    #[test]
    fn test_cloudwatch_import_single_value_view() {
        let json = r#"{
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "view": "singleValue",
                        "metrics": [["AWS/EC2", "CPUUtilization"]],
                        "period": 60,
                        "stat": "Average"
                    }
                },
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [["AWS/EC2", "NetworkIn"]],
                        "period": 60,
                        "stat": "Sum"
                    }
                }
            ]
        }"#;

        let importer = CloudWatchDashboardImporter;
        let result = importer.import(json).expect("must parse view=singleValue");

        assert_eq!(view_of(&result[0]), MetricView::SingleValue);
        assert_eq!(view_of(&result[1]), MetricView::TimeSeries);
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

    /// Design test #56: `CloudWatchDashboardImporter` parses a valid single-panel
    /// dashboard and returns `Some` (non-empty `Vec<PanelImportSpec>`).
    #[test]
    fn test_cloudwatch_dashboard_importer_returns_some() {
        let json = r#"{
            "widgets": [{
                "type": "metric",
                "properties": {
                    "title": "CPU",
                    "metrics": [["AWS/EC2", "CPUUtilization", "InstanceId", "i-1"]],
                    "period": 60,
                    "stat": "Sum",
                    "region": "eu-west-1"
                }
            }]
        }"#;

        let importer = CloudWatchDashboardImporter;
        let result = importer
            .import(json)
            .expect("valid CloudWatch JSON must parse");
        assert!(
            !result.is_empty(),
            "CloudWatchDashboardImporter must return at least one WidgetImportSpec"
        );
        let series = series_of(&result[0]);
        assert_eq!(series[0].namespace, "AWS/EC2");
        assert_eq!(series[0].metric_name, "CPUUtilization");
        assert_eq!(series[0].statistic, "Sum");
        assert_eq!(series[0].region.as_deref(), Some("eu-west-1"));
    }

    /// Design test #57: a non-CloudWatch JSON (missing "widgets" key) must fail
    /// with `Err(DbError::Parse(_))` — confirming that `CloudWatchDashboardImporter`
    /// does not silently accept arbitrary JSON.
    #[test]
    fn test_non_cloudwatch_returns_none() {
        // Simulate a non-CloudWatch dashboard JSON (e.g., a Grafana export)
        // that is valid JSON but does not contain "widgets".
        let json = r#"{"panels": [], "title": "Grafana Dashboard"}"#;
        let importer = CloudWatchDashboardImporter;
        let result = importer.import(json);
        assert!(
            result.is_err(),
            "non-CloudWatch JSON without 'widgets' must return Err"
        );
        assert!(
            matches!(result.unwrap_err(), DbError::Parse(_)),
            "expected DbError::Parse for non-CloudWatch JSON"
        );
    }
}
