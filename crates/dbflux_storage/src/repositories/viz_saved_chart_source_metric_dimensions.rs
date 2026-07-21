//! DTO for `viz_saved_chart_source_metric_dimensions` rows.
//!
//! This table stores the ordered list of CloudWatch dimension (name, value)
//! pairs for `SavedChartSource::Metric` charts. It mirrors the pattern used
//! by `viz_saved_chart_series` and `viz_saved_chart_binding_y`.

/// Data transfer object for a single dimension row.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricDimensionDto {
    pub chart_id: String,
    pub series_index: i64,
    pub dim_index: i64,
    pub dim_key: String,
    pub dim_value: String,
}
