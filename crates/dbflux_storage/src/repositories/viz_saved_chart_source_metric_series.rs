//! DTO for `viz_saved_chart_source_metric_series` rows.
//!
//! Mirrors one row of the multi-series CloudWatch metric source. Dimension
//! rows in `viz_saved_chart_source_metric_dimensions` are keyed by
//! `(chart_id, series_index)` and looked up against this table.

/// Data transfer object for a single metric series row.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricSeriesDto {
    pub chart_id: String,
    pub series_index: i64,
    pub namespace: String,
    pub metric_name: String,
    pub period_seconds: i64,
    pub statistic: String,
    pub region: Option<String>,
    pub label: Option<String>,
}
