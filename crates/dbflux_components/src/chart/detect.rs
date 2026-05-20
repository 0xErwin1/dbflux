//! Chart column auto-detection.
//!
//! This module is the boundary between the query result model and the chart engine.
//! It inspects `ColumnKind` values — never `type_name` strings or driver identifiers.

use dbflux_core::{ColumnKind, QueryResult};

/// Outcome of attempting to auto-detect chart-suitable columns from a `QueryResult`.
#[derive(Debug, Clone, PartialEq)]
pub enum ChartDetection {
    /// Detection succeeded.
    Ok {
        /// Index of the leftmost `Timestamp` column (X axis).
        time_col: usize,
        /// Indices of all `Float` and `Integer` columns, in column order, excluding `time_col`.
        numeric_cols: Vec<usize>,
    },
    /// The result has no column with `kind == Timestamp`.
    NoTimeColumn,
    /// A `Timestamp` column was found but no `Float` or `Integer` columns remain.
    NoNumericSeries,
    /// The result has zero data rows; chart cannot be rendered.
    EmptyResult,
}

/// Auto-detect the columns suitable for charting in `result`.
///
/// Detection rules (applied in order):
/// 1. If `result.rows` is empty → `EmptyResult`.
/// 2. Pick the leftmost column with `kind == Timestamp`. If none → `NoTimeColumn`.
/// 3. Collect all columns (excluding the time column) with `kind == Float` or `Integer`.
///    If empty → `NoNumericSeries`.
/// 4. Otherwise → `Ok { time_col, numeric_cols }`.
///
/// The function never inspects `column.type_name`, `column.name`, or any driver identifier.
pub fn detect_chart_columns(result: &QueryResult) -> ChartDetection {
    if result.rows.is_empty() {
        return ChartDetection::EmptyResult;
    }

    let time_col = result
        .columns
        .iter()
        .position(|c| c.kind == ColumnKind::Timestamp);

    let time_col = match time_col {
        Some(idx) => idx,
        None => return ChartDetection::NoTimeColumn,
    };

    let numeric_cols: Vec<usize> = result
        .columns
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
// Unit tests (strict TDD: tests were written before the implementation above)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{ColumnKind, ColumnMeta, QueryResult, Value};
    use std::time::Duration;

    fn make_col(kind: ColumnKind) -> ColumnMeta {
        ColumnMeta {
            name: "col".to_string(),
            type_name: "t".to_string(),
            kind,
            nullable: true,
            is_primary_key: false,
        }
    }

    fn named_col(name: &str, kind: ColumnKind) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            type_name: "t".to_string(),
            kind,
            nullable: true,
            is_primary_key: false,
        }
    }

    fn one_row() -> Vec<Vec<Value>> {
        vec![vec![Value::Null]]
    }

    #[test]
    fn returns_empty_result_for_no_rows() {
        let result = QueryResult::table(
            vec![make_col(ColumnKind::Timestamp), make_col(ColumnKind::Float)],
            vec![],
            None,
            Duration::ZERO,
        );
        assert_eq!(detect_chart_columns(&result), ChartDetection::EmptyResult);
    }

    #[test]
    fn returns_no_time_column_when_no_timestamp_kind() {
        let result = QueryResult::table(
            vec![make_col(ColumnKind::Text), make_col(ColumnKind::Float)],
            one_row(),
            None,
            Duration::ZERO,
        );
        assert_eq!(detect_chart_columns(&result), ChartDetection::NoTimeColumn);
    }

    #[test]
    fn returns_no_numeric_series_when_only_timestamp_and_text() {
        let result = QueryResult::table(
            vec![make_col(ColumnKind::Timestamp), make_col(ColumnKind::Text)],
            one_row(),
            None,
            Duration::ZERO,
        );
        assert_eq!(
            detect_chart_columns(&result),
            ChartDetection::NoNumericSeries
        );
    }

    #[test]
    fn detects_ok_for_timestamp_plus_floats() {
        let result = QueryResult::table(
            vec![
                make_col(ColumnKind::Timestamp),
                make_col(ColumnKind::Float),
                make_col(ColumnKind::Integer),
            ],
            one_row(),
            None,
            Duration::ZERO,
        );
        assert_eq!(
            detect_chart_columns(&result),
            ChartDetection::Ok {
                time_col: 0,
                numeric_cols: vec![1, 2],
            }
        );
    }

    #[test]
    fn picks_leftmost_timestamp_when_multiple() {
        let result = QueryResult::table(
            vec![
                named_col("a", ColumnKind::Text),
                named_col("t1", ColumnKind::Timestamp),
                named_col("t2", ColumnKind::Timestamp),
                named_col("v", ColumnKind::Float),
            ],
            one_row(),
            None,
            Duration::ZERO,
        );
        match detect_chart_columns(&result) {
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
