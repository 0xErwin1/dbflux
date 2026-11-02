//! Chart column auto-detection, adapted from a `QueryResult`.
//!
//! The detection rules live in `bolt::chart::detect` over the product-neutral
//! column contract; this wrapper converts the result's metadata and forwards,
//! so call sites keep passing a `&QueryResult`.

use dbflux_core::QueryResult;

pub use bolt::chart::detect::ChartDetection;

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
    bolt::chart::detect::detect_chart_columns(
        result.rows.len(),
        &super::compat::chart_columns(&result.columns),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{ColumnKind, ColumnMeta, Value};
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
