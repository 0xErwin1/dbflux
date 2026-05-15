//! Architectural contract tests for `ColumnKind`-based chart detection.
//!
//! These tests assert the boundary between the query result model and the
//! chart engine:
//! - Detection operates on `ColumnKind` values, never on `type_name` strings.
//! - `ChartSpec::from_detection` builds a spec that `ChartView::build` accepts.
//! - No driver-specific logic appears at the chart boundary.

use dbflux_components::chart::{ChartDetection, ChartSpec, ChartView, detect_chart_columns};
use dbflux_core::{ColumnKind, ColumnMeta, QueryResult, Value};
use std::time::Duration;

fn make_col(name: &str, kind: ColumnKind, type_name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        type_name: type_name.to_string(),
        kind,
        nullable: true,
        is_primary_key: false,
    }
}

fn one_row(values: Vec<Value>) -> Vec<Vec<Value>> {
    vec![values]
}

// ---------------------------------------------------------------------------
// Contract: detection uses ColumnKind, not type_name
// ---------------------------------------------------------------------------

#[test]
fn detection_uses_column_kind_not_type_name() {
    // Column has kind=Timestamp but a deliberately misleading type_name.
    // Detection must succeed because ColumnKind is the source of truth.
    let result = QueryResult::table(
        vec![
            make_col("ts", ColumnKind::Timestamp, "varchar"),
            make_col("val", ColumnKind::Float, "blob"),
        ],
        one_row(vec![Value::Int(0), Value::Float(1.0)]),
        None,
        Duration::ZERO,
    );

    assert_eq!(
        detect_chart_columns(&result),
        ChartDetection::Ok {
            time_col: 0,
            numeric_cols: vec![1],
        },
        "detection must use ColumnKind, not type_name"
    );
}

#[test]
fn detection_ignores_text_columns_regardless_of_type_name() {
    // A column named "timestamp" but with kind=Text must not be treated as X axis.
    let result = QueryResult::table(
        vec![
            make_col("timestamp", ColumnKind::Text, "timestamptz"),
            make_col("val", ColumnKind::Float, "float8"),
        ],
        one_row(vec![
            Value::Text("2024-01-01".to_string()),
            Value::Float(1.0),
        ]),
        None,
        Duration::ZERO,
    );

    assert_eq!(
        detect_chart_columns(&result),
        ChartDetection::NoTimeColumn,
        "a column with kind=Text must not be used as the time axis even if named 'timestamp'"
    );
}

// ---------------------------------------------------------------------------
// Contract: detection → spec → ChartView::build round-trip
// ---------------------------------------------------------------------------

#[test]
fn detection_to_spec_to_chart_build_succeeds() {
    let columns = vec![
        make_col("ts", ColumnKind::Timestamp, "int8"),
        make_col("cpu", ColumnKind::Float, "float8"),
        make_col("mem", ColumnKind::Integer, "int4"),
    ];
    let rows = vec![
        vec![Value::Int(0), Value::Float(0.1), Value::Int(128)],
        vec![Value::Int(1000), Value::Float(0.5), Value::Int(256)],
        vec![Value::Int(2000), Value::Float(0.9), Value::Int(512)],
    ];
    let result = QueryResult::table(columns.clone(), rows, None, Duration::ZERO);

    let detection = detect_chart_columns(&result);
    let (time_col, numeric_cols) = match detection {
        ChartDetection::Ok {
            time_col,
            numeric_cols,
        } => (time_col, numeric_cols),
        other => panic!("expected ChartDetection::Ok, got {:?}", other),
    };

    let spec = ChartSpec::from_detection(time_col, numeric_cols, &columns, 10_000)
        .expect("spec should build from detection");

    assert!(
        ChartView::build(&result, spec).is_ok(),
        "ChartView::build should succeed for a detected chartable result"
    );
}

#[test]
fn integer_column_is_chartable_as_y_series() {
    // ColumnKind::Integer must be included in numeric_cols (not just Float).
    let result = QueryResult::table(
        vec![
            make_col("ts", ColumnKind::Timestamp, "timestamptz"),
            make_col("count", ColumnKind::Integer, "int8"),
        ],
        one_row(vec![Value::Int(1_700_000_000_000i64), Value::Int(42)]),
        None,
        Duration::ZERO,
    );

    let detection = detect_chart_columns(&result);
    assert!(
        matches!(
            &detection,
            ChartDetection::Ok { numeric_cols, .. } if numeric_cols.contains(&1)
        ),
        "Integer column must be included in numeric_cols; got {:?}",
        detection
    );
}

#[test]
fn empty_result_blocks_chart_construction() {
    let result = QueryResult::table(
        vec![
            make_col("ts", ColumnKind::Timestamp, "int8"),
            make_col("val", ColumnKind::Float, "float8"),
        ],
        vec![],
        None,
        Duration::ZERO,
    );

    assert_eq!(
        detect_chart_columns(&result),
        ChartDetection::EmptyResult,
        "empty result must block chart construction"
    );
}
