//! Chart column auto-detection.
//!
//! This module is the boundary between the data owner's table and the chart.
//! It inspects [`ColumnKind`] values — never the owner's engine-side type
//! names — and only the shape of the table: whether it has rows and which
//! columns carry which kind.

use crate::data::ColumnKind;

use super::column::ChartColumn;

/// Outcome of attempting to auto-detect chart-suitable columns from a table.
#[derive(Debug, Clone, PartialEq)]
pub enum ChartDetection {
    /// Detection succeeded.
    Ok {
        /// Index of the leftmost `Timestamp` column (X axis).
        time_col: usize,
        /// Indices of all `Float` and `Integer` columns, in column order, excluding `time_col`.
        numeric_cols: Vec<usize>,
    },
    /// The table has no column with `kind == Timestamp`.
    NoTimeColumn,
    /// A `Timestamp` column was found but no `Float` or `Integer` columns remain.
    NoNumericSeries,
    /// The table has zero data rows; chart cannot be rendered.
    EmptyResult,
}

/// Auto-detect the columns suitable for charting in a table of `row_count`
/// rows and `columns` columns.
///
/// Detection rules (applied in order):
/// 1. If `row_count` is zero → `EmptyResult`.
/// 2. Pick the leftmost column with `kind == Timestamp`. If none → `NoTimeColumn`.
/// 3. Collect all columns (excluding the time column) with `kind == Float` or `Integer`.
///    If empty → `NoNumericSeries`.
/// 4. Otherwise → `Ok { time_col, numeric_cols }`.
///
/// The function never inspects a column's type name, only its kind.
pub fn detect_chart_columns(row_count: usize, columns: &[ChartColumn]) -> ChartDetection {
    if row_count == 0 {
        return ChartDetection::EmptyResult;
    }

    let Some(time_col) = columns.iter().position(|c| c.kind == ColumnKind::Timestamp) else {
        return ChartDetection::NoTimeColumn;
    };

    let numeric_cols: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(i, c)| {
            *i != time_col && (c.kind == ColumnKind::Float || c.kind == ColumnKind::Integer)
        })
        .map(|(i, _)| i)
        .collect();

    if numeric_cols.is_empty() {
        return ChartDetection::NoNumericSeries;
    }

    ChartDetection::Ok {
        time_col,
        numeric_cols,
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, kind: ColumnKind) -> ChartColumn {
        ChartColumn::new(name, kind)
    }

    #[test]
    fn returns_empty_result_for_no_rows() {
        let columns = vec![col("t", ColumnKind::Timestamp), col("v", ColumnKind::Float)];
        assert_eq!(
            detect_chart_columns(0, &columns),
            ChartDetection::EmptyResult
        );
    }

    #[test]
    fn returns_no_time_column_when_no_timestamp_kind() {
        let columns = vec![col("s", ColumnKind::Text), col("v", ColumnKind::Float)];
        assert_eq!(
            detect_chart_columns(1, &columns),
            ChartDetection::NoTimeColumn
        );
    }

    #[test]
    fn returns_no_numeric_series_when_only_timestamp_and_text() {
        let columns = vec![col("t", ColumnKind::Timestamp), col("s", ColumnKind::Text)];
        assert_eq!(
            detect_chart_columns(1, &columns),
            ChartDetection::NoNumericSeries
        );
    }

    #[test]
    fn detects_ok_for_timestamp_plus_floats() {
        let columns = vec![
            col("t", ColumnKind::Timestamp),
            col("v1", ColumnKind::Float),
            col("v2", ColumnKind::Integer),
        ];
        assert_eq!(
            detect_chart_columns(1, &columns),
            ChartDetection::Ok {
                time_col: 0,
                numeric_cols: vec![1, 2],
            }
        );
    }

    #[test]
    fn picks_leftmost_timestamp_when_multiple() {
        let columns = vec![
            col("a", ColumnKind::Text),
            col("t1", ColumnKind::Timestamp),
            col("t2", ColumnKind::Timestamp),
            col("v", ColumnKind::Float),
        ];
        match detect_chart_columns(1, &columns) {
            ChartDetection::Ok {
                time_col,
                numeric_cols,
            } => {
                assert_eq!(time_col, 1, "should pick leftmost Timestamp");
                assert_eq!(numeric_cols, vec![3]);
            }
            other => panic!("expected Ok, got {:?}", other),
        }
    }
}
