//! Per-series statistics for the chart, adapted from column metadata.
//!
//! The pure computations live in `bolt::chart::stats`; this module re-exports
//! them and keeps the `dbflux_core::ColumnMeta`-typed helper the "why" copy
//! in the data grid reads.

use dbflux_core::ColumnMeta;

pub use bolt::chart::stats::{
    SeriesStats, compute_series_stats, format_resolution, format_span, hit_test_focused_series,
    interpolate_y_at_x,
};

/// Count numeric and timestamp-like columns in a result column list.
///
/// Returns `(numeric_count, timestamp_count)`.
pub fn count_columns_for_why(columns: &[ColumnMeta]) -> (usize, usize) {
    bolt::chart::stats::count_chart_columns(&super::compat::chart_columns(columns))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::ColumnKind;

    fn meta(name: &str, kind: ColumnKind) -> ColumnMeta {
        ColumnMeta {
            name: name.to_owned(),
            type_name: String::new(),
            kind,
            nullable: true,
            is_primary_key: false,
        }
    }

    #[test]
    fn count_columns_for_why_counts_numeric_and_timestamp_kinds() {
        let cols = vec![
            meta("t", ColumnKind::Timestamp),
            meta("v1", ColumnKind::Float),
            meta("v2", ColumnKind::Integer),
            meta("v3", ColumnKind::Float),
            meta("s", ColumnKind::Text),
            meta("u", ColumnKind::Unknown),
        ];
        let (num, ts) = count_columns_for_why(&cols);
        assert_eq!(num, 3); // v1, v2, v3
        assert_eq!(ts, 1); // t
    }

    #[test]
    fn count_columns_for_why_counts_nothing_for_unknown_and_text() {
        let cols = vec![meta("a", ColumnKind::Unknown), meta("b", ColumnKind::Text)];
        let (num, ts) = count_columns_for_why(&cols);
        assert_eq!(num, 0);
        assert_eq!(ts, 0);
    }
}
