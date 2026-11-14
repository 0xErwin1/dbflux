use crate::profile::{
    ColumnSummary, EXACT_DISTINCT_LIMIT, ProfileAccumulator, TOP_VALUE_COUNT, ValueBounds,
    ValueKind,
};

/// The summaries of one column holding `values`, one record each.
fn summarize(values: &[&str]) -> ColumnSummary {
    let mut accumulator = ProfileAccumulator::new();

    for value in values {
        accumulator.push_record(&[*value]);
    }

    accumulator
        .summaries()
        .into_iter()
        .next()
        .expect("one column")
}

fn bounds(min: &str, max: &str) -> Option<ValueBounds> {
    Some(ValueBounds {
        min: min.to_string(),
        max: max.to_string(),
    })
}

// -- Type inference ---------------------------------------------------------

#[test]
fn integers_are_inferred_as_integer() {
    assert_eq!(
        summarize(&["1", "-20", "+3", "007"]).kind,
        ValueKind::Integer
    );
}

#[test]
fn integers_mixed_with_decimals_are_decimal() {
    assert_eq!(
        summarize(&["1", "2.5", "-1e3", ".5"]).kind,
        ValueKind::Decimal
    );
}

#[test]
fn booleans_are_inferred_case_insensitively() {
    assert_eq!(
        summarize(&["true", "FALSE", "t", "F", "Yes", "no"]).kind,
        ValueKind::Boolean
    );
}

#[test]
fn zero_and_one_are_integers_not_booleans() {
    assert_eq!(summarize(&["0", "1", "1"]).kind, ValueKind::Integer);
    assert_eq!(summarize(&["0", "true"]).kind, ValueKind::Text);
}

#[test]
fn dates_are_inferred_as_date() {
    assert_eq!(
        summarize(&["2024-01-31", "1999-12-01", "2024-02-29"]).kind,
        ValueKind::Date
    );
}

#[test]
fn an_impossible_date_is_text() {
    assert_eq!(summarize(&["2023-02-29"]).kind, ValueKind::Text);
    assert_eq!(summarize(&["2024-13-01"]).kind, ValueKind::Text);
}

#[test]
fn timestamps_with_and_without_zone_or_fraction_are_timestamp() {
    assert_eq!(
        summarize(&[
            "2024-01-31T10:20:30",
            "2024-01-31 10:20",
            "2024-01-31T10:20:30.123456Z",
            "2024-01-31T10:20:30+02:00",
            "2024-01-31T10:20:30-0530",
        ])
        .kind,
        ValueKind::Timestamp
    );
}

#[test]
fn dates_mixed_with_timestamps_are_timestamp() {
    assert_eq!(
        summarize(&["2024-01-31", "2024-01-31T10:20:30Z"]).kind,
        ValueKind::Timestamp
    );
}

#[test]
fn a_mixed_column_falls_back_to_text() {
    assert_eq!(summarize(&["1", "2", "Lima"]).kind, ValueKind::Text);
    assert_eq!(summarize(&["2024-01-31", "3"]).kind, ValueKind::Text);
    assert_eq!(summarize(&["NaN", "inf"]).kind, ValueKind::Text);
}

#[test]
fn a_column_of_only_empty_values_is_text_without_bounds() {
    let summary = summarize(&["", ""]);

    assert_eq!(summary.kind, ValueKind::Text);
    assert_eq!(summary.null_count, 2);
    assert_eq!(summary.distinct_count, 0);
    assert_eq!(summary.range, None);
    assert!(summary.top_values.is_empty());
}

// -- Nulls --------------------------------------------------------------------

#[test]
fn empty_values_count_as_nulls_and_do_not_decide_the_type() {
    let summary = summarize(&["1", "", "3", ""]);

    assert_eq!(summary.kind, ValueKind::Integer);
    assert_eq!(summary.null_count, 2);
    assert_eq!(summary.distinct_count, 2);
}

#[test]
fn a_short_record_counts_its_missing_fields_as_nulls() {
    let mut accumulator = ProfileAccumulator::new();

    accumulator.push_record(&["1", "a"]);
    accumulator.push_record(&["2"]);

    let summaries = accumulator.summaries();

    assert_eq!(accumulator.rows(), 2);
    assert_eq!(summaries[1].null_count, 1);
}

#[test]
fn a_wider_record_adds_columns_null_in_the_records_before_it() {
    let mut accumulator = ProfileAccumulator::new();

    accumulator.push_record(&["1"]);
    accumulator.push_record(&["2"]);
    accumulator.push_record(&["3", "x", "y"]);

    let summaries = accumulator.summaries();

    assert_eq!(accumulator.column_count(), 3);
    assert_eq!(summaries[1].null_count, 2);
    assert_eq!(summaries[2].null_count, 2);
    assert_eq!(summaries[2].distinct_count, 1);
}

#[test]
fn set_column_count_adds_columns_null_in_every_row_so_far() {
    let mut accumulator = ProfileAccumulator::new();

    accumulator.push_record(&["1"]);
    accumulator.set_column_count(2);

    assert_eq!(accumulator.summaries()[1].null_count, 1);
}

// -- Ranges -------------------------------------------------------------------

#[test]
fn numbers_are_compared_numerically() {
    assert_eq!(summarize(&["9", "10", "-2"]).range, bounds("-2", "10"));
    assert_eq!(summarize(&["9.5", "10", "1e1"]).range, bounds("9.5", "10"));
}

#[test]
fn text_is_compared_lexicographically() {
    assert_eq!(summarize(&["9", "10", "b"]).range, bounds("10", "b"));
}

#[test]
fn dates_and_timestamps_are_compared_chronologically() {
    assert_eq!(
        summarize(&["2024-03-01", "1999-12-31", "2024-02-29"]).range,
        bounds("1999-12-31", "2024-03-01")
    );

    // 10:00+02:00 is 08:00 UTC, before 09:00 UTC.
    assert_eq!(
        summarize(&["2024-01-01T09:00:00Z", "2024-01-01T10:00:00+02:00"]).range,
        bounds("2024-01-01T10:00:00+02:00", "2024-01-01T09:00:00Z")
    );
}

#[test]
fn booleans_range_from_false_to_true() {
    assert_eq!(summarize(&["yes", "No", "yes"]).range, bounds("No", "yes"));
}

// -- Distinct counts and top values -----------------------------------------------

#[test]
fn distinct_counts_and_top_values_are_exact_under_the_limit() {
    let summary = summarize(&["a", "b", "a", "c", "b", "a", "d"]);

    assert_eq!(summary.distinct_count, 4);
    assert!(summary.distinct_exact);
    assert_eq!(
        summary.top_values,
        vec![
            ("a".to_string(), 3),
            ("b".to_string(), 2),
            ("c".to_string(), 1)
        ]
    );
    assert_eq!(summary.top_values.len(), TOP_VALUE_COUNT);
}

#[test]
fn the_limit_itself_is_still_exact() {
    let values: Vec<String> = (0..EXACT_DISTINCT_LIMIT).map(|v| v.to_string()).collect();
    let mut accumulator = ProfileAccumulator::new();

    for value in &values {
        accumulator.push_record(&[value.as_str()]);
    }

    let summary = &accumulator.summaries()[0];

    assert!(summary.distinct_exact);
    assert_eq!(summary.distinct_count, EXACT_DISTINCT_LIMIT as u64);
    assert_eq!(summary.top_values.len(), TOP_VALUE_COUNT);
}

#[test]
fn past_the_limit_the_distinct_count_is_an_estimate_without_top_values() {
    let mut accumulator = ProfileAccumulator::new();

    for value in 0..=EXACT_DISTINCT_LIMIT {
        accumulator.push_record(&[value.to_string()]);
    }

    let summary = &accumulator.summaries()[0];

    assert!(!summary.distinct_exact);
    assert!(summary.top_values.is_empty());
    assert_eq!(summary.kind, ValueKind::Integer);
    assert_eq!(summary.range, bounds("0", "10000"));
}

#[test]
fn the_estimate_is_within_two_percent_on_100_000_distinct_values() {
    let mut accumulator = ProfileAccumulator::new();

    for value in 0..100_000_u64 {
        // Every value twice, so repeats do not inflate the estimate.
        let text = format!("value-{value}");
        accumulator.push_record(&[text.as_str()]);
        accumulator.push_record(&[text.as_str()]);
    }

    let estimate = accumulator.summaries()[0].distinct_count as f64;
    let error = (estimate - 100_000.0).abs() / 100_000.0;

    assert!(error < 0.02, "estimate {estimate} is off by {error}");
}
