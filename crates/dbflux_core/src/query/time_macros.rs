//! Pure helper for query time-range macro substitution.
//!
//! Recognizes Grafana-compatible token names and replaces them with
//! RFC3339-formatted time literals derived from a bound window.
//!
//! Supported tokens:
//! - InfluxQL: `$timeFilter`, `$__from`, `$__to`
//! - Flux: `v.timeRangeStart`, `v.timeRangeStop`
//!
//! Substitution is language-specific: only `InfluxQuery` and `Flux` documents
//! receive token expansion; all other languages pass through unchanged.
//!
//! # Known limitation
//!
//! Matching uses naïve substring search (`str::contains` / `str::replace`).
//! For Flux, `v.timeRangeStart` and `v.timeRangeStop` will incorrectly match
//! Flux variable names that merely start with those strings (e.g.
//! `v.timeRangeStartCustom`). Proper tokenisation is deferred to a future
//! version (REQ-9).

use crate::QueryLanguage;
use chrono::{DateTime, Utc};

// InfluxQL tokens (Grafana-aligned naming).
const INFLUXQL_TOKEN_TIME_FILTER: &str = "$timeFilter";
const TOKEN_FROM: &str = "$__from";
const TOKEN_TO: &str = "$__to";

// Flux tokens (Grafana variable-namespace convention).
const FLUX_TOKEN_RANGE_START: &str = "v.timeRangeStart";
const FLUX_TOKEN_RANGE_STOP: &str = "v.timeRangeStop";

/// Returns `true` when the query string contains at least one recognized
/// time-range macro token.
///
/// Recognized tokens:
/// - InfluxQL: `$timeFilter`, `$__from`, `$__to`
/// - Flux: `v.timeRangeStart`, `v.timeRangeStop`
///
/// Matching is exact and case-sensitive.
///
/// # Example
///
/// ```
/// use dbflux_core::contains_time_macros;
///
/// assert!(contains_time_macros("SELECT * FROM cpu WHERE $timeFilter"));
/// assert!(contains_time_macros("from(bucket: \"x\") |> range(start: v.timeRangeStart)"));
/// assert!(!contains_time_macros("SELECT * FROM cpu WHERE time > now() - 1h"));
/// ```
pub fn contains_time_macros(query: &str) -> bool {
    query.contains(INFLUXQL_TOKEN_TIME_FILTER)
        || query.contains(TOKEN_FROM)
        || query.contains(TOKEN_TO)
        || query.contains(FLUX_TOKEN_RANGE_START)
        || query.contains(FLUX_TOKEN_RANGE_STOP)
}

/// Replaces all occurrences of recognized time-range macro tokens with their
/// language-specific expansions.
///
/// When `window` is `None` the query is returned unchanged. When `window` is
/// `Some((start_ms, end_ms))`, both epoch-millisecond timestamps are converted
/// to RFC3339 second-precision UTC strings (`YYYY-MM-DDTHH:MM:SSZ`) and used
/// as substitution values.
///
/// Token mapping by language:
/// - `InfluxQuery`: `$timeFilter` → `time >= 'start' AND time <= 'end'`,
///   `$__from` → `'start'`, `$__to` → `'end'`
/// - `Flux`: `v.timeRangeStart` → `'start'`, `v.timeRangeStop` → `'end'`
/// - All other languages: pass through unchanged regardless of window presence
///
/// # Example
///
/// ```
/// use dbflux_core::{substitute_time_macros, QueryLanguage};
///
/// let q = "SELECT * FROM cpu WHERE $timeFilter GROUP BY time(1m)";
/// let result = substitute_time_macros(q, Some((1_710_000_000_000, 1_710_003_600_000)), QueryLanguage::InfluxQuery);
/// assert!(result.contains("time >=") && result.contains("time <="));
/// ```
pub fn substitute_time_macros(
    query: &str,
    window: Option<(i64, i64)>,
    lang: QueryLanguage,
) -> String {
    let Some((start_ms, end_ms)) = window else {
        // Diagnostic: macros only resolve when a CollectionWindow is bound on
        // the execution context. A macro-bearing query with no window will be
        // sent verbatim to the driver and almost certainly parse-error or
        // silently use a server-side default. Surface this state early so the
        // root cause is not buried in driver errors.
        if contains_time_macros(query) {
            log::warn!(
                "[time_macros] query contains time macros but no window is bound; \
                 macros will pass through unsubstituted (likely a stale exec_ctx or \
                 missing time-range panel)"
            );
        }
        return query.to_string();
    };

    let start = rfc3339(start_ms);
    let end = rfc3339(end_ms);

    match lang {
        QueryLanguage::InfluxQuery => query
            .replace(
                INFLUXQL_TOKEN_TIME_FILTER,
                &format!("time >= '{start}' AND time <= '{end}'"),
            )
            .replace(TOKEN_FROM, &format!("'{start}'"))
            .replace(TOKEN_TO, &format!("'{end}'")),

        QueryLanguage::Flux => query
            .replace(FLUX_TOKEN_RANGE_START, &format!("'{start}'"))
            .replace(FLUX_TOKEN_RANGE_STOP, &format!("'{end}'")),

        _ => query.to_string(),
    }
}

/// Format an epoch-millisecond timestamp as RFC3339 second-precision UTC.
///
/// Returns the Unix epoch (`1970-01-01T00:00:00Z`) for out-of-range values.
fn rfc3339(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .unwrap_or_default()
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- contains_time_macros ---

    #[test]
    fn contains_returns_true_for_influxql_time_filter_token() {
        // Grafana-aligned single-underscore token for InfluxQL.
        assert!(contains_time_macros("SELECT * FROM cpu WHERE $timeFilter"));
    }

    #[test]
    fn contains_returns_false_for_old_double_underscore_time_filter() {
        // The old `$__timeFilter` token is no longer recognized; it passes through.
        assert!(!contains_time_macros("$__timeFilter"));
    }

    #[test]
    fn contains_returns_true_for_from_token() {
        assert!(contains_time_macros(
            "SELECT * FROM cpu WHERE time >= $__from"
        ));
    }

    #[test]
    fn contains_returns_true_for_to_token() {
        assert!(contains_time_macros(
            "SELECT * FROM cpu WHERE time <= $__to"
        ));
    }

    #[test]
    fn contains_returns_true_for_flux_range_start_token() {
        assert!(contains_time_macros(
            "from(bucket: \"x\") |> range(start: v.timeRangeStart)"
        ));
    }

    #[test]
    fn contains_returns_true_for_flux_range_stop_token() {
        assert!(contains_time_macros(
            "from(bucket: \"x\") |> range(start: -6h, stop: v.timeRangeStop)"
        ));
    }

    #[test]
    fn contains_returns_false_for_unrelated_text() {
        assert!(!contains_time_macros(
            "SELECT * FROM cpu WHERE time > now() - 1h"
        ));
    }

    #[test]
    fn contains_returns_false_for_mixed_case_token() {
        // Matching is case-sensitive per REQ-1.
        assert!(!contains_time_macros("$TimeFilter"));
        assert!(!contains_time_macros("V.TimeRangeStart"));
    }

    #[test]
    fn contains_returns_true_when_multiple_tokens_present() {
        let q = "SELECT * FROM cpu WHERE time >= $__from AND time <= $__to";
        assert!(contains_time_macros(q));
    }

    // --- substring boundary: v.timeRangeStart must not match longer names ---
    //
    // Known limitation (REQ-9): naïve str::contains will match
    // `v.timeRangeStartCustom` because it contains `v.timeRangeStart`. This
    // test documents the current behaviour (the macro guard fires) so the
    // regression is visible if tokenisation is added later.
    #[test]
    fn contains_flux_range_start_fires_for_extended_name_known_limitation() {
        // This is intentionally documenting the limitation: the guard WILL fire.
        assert!(contains_time_macros("v.timeRangeStartButNot"));
    }

    // --- substitute_time_macros: no-window passthrough ---

    #[test]
    fn no_window_returns_query_unchanged_for_influxql() {
        let q = "SELECT * FROM cpu WHERE $timeFilter";
        assert_eq!(
            substitute_time_macros(q, None, QueryLanguage::InfluxQuery),
            q
        );
    }

    #[test]
    fn no_window_returns_query_unchanged_for_flux() {
        let q = "from(bucket: \"x\") |> range(start: v.timeRangeStart, stop: v.timeRangeStop)";
        assert_eq!(substitute_time_macros(q, None, QueryLanguage::Flux), q);
    }

    #[test]
    fn no_window_returns_query_unchanged_for_sql() {
        let q = "SELECT $__from FROM table";
        assert_eq!(substitute_time_macros(q, None, QueryLanguage::Sql), q);
    }

    // --- InfluxQL substitutions ---

    #[test]
    fn influxql_time_filter_expands_to_time_range_predicate() {
        let q = "SELECT mean(usage_user) FROM cpu WHERE $timeFilter GROUP BY time(1m)";
        let window = Some((1_710_000_000_000_i64, 1_710_003_600_000_i64));
        let result = substitute_time_macros(q, window, QueryLanguage::InfluxQuery);
        assert!(
            result.contains("time >= '2024-03-09T16:00:00Z' AND time <= '2024-03-09T17:00:00Z'"),
            "got: {result}"
        );
    }

    #[test]
    fn influxql_from_expands_to_quoted_rfc3339() {
        let q = "SELECT * FROM cpu WHERE time >= $__from";
        let window = Some((1_710_000_000_000_i64, 1_710_003_600_000_i64));
        let result = substitute_time_macros(q, window, QueryLanguage::InfluxQuery);
        assert!(
            result.contains("time >= '2024-03-09T16:00:00Z'"),
            "got: {result}"
        );
        // $__to not present — must not be injected
        assert!(!result.contains("$__to"), "got: {result}");
    }

    #[test]
    fn influxql_to_expands_to_quoted_rfc3339() {
        let q = "SELECT * FROM cpu WHERE time <= $__to";
        let window = Some((1_710_000_000_000_i64, 1_710_003_600_000_i64));
        let result = substitute_time_macros(q, window, QueryLanguage::InfluxQuery);
        assert!(
            result.contains("time <= '2024-03-09T17:00:00Z'"),
            "got: {result}"
        );
    }

    #[test]
    fn influxql_multiple_occurrences_of_same_token_all_replaced() {
        let q = "SELECT * FROM cpu WHERE $__from > $__from";
        let window = Some((1_710_000_000_000_i64, 1_710_003_600_000_i64));
        let result = substitute_time_macros(q, window, QueryLanguage::InfluxQuery);
        // No raw $__from must remain.
        assert!(!result.contains("$__from"), "got: {result}");
        assert_eq!(
            result.matches("2024-03-09T16:00:00Z").count(),
            2,
            "both occurrences must be replaced, got: {result}"
        );
    }

    // --- Flux substitutions ---

    #[test]
    fn flux_range_start_expands_to_quoted_rfc3339() {
        let q = "from(bucket: \"x\") |> range(start: v.timeRangeStart, stop: v.timeRangeStop)";
        let window = Some((1_710_000_000_000_i64, 1_710_003_600_000_i64));
        let result = substitute_time_macros(q, window, QueryLanguage::Flux);
        assert!(
            result.contains("start: '2024-03-09T16:00:00Z'"),
            "got: {result}"
        );
        assert!(
            result.contains("stop: '2024-03-09T17:00:00Z'"),
            "got: {result}"
        );
        // No raw tokens must remain.
        assert!(!result.contains("v.timeRangeStart"), "got: {result}");
        assert!(!result.contains("v.timeRangeStop"), "got: {result}");
    }

    #[test]
    fn flux_range_stop_only_expands_to_quoted_rfc3339() {
        let q = "from(bucket: \"x\") |> range(start: -6h, stop: v.timeRangeStop)";
        let window = Some((1_710_000_000_000_i64, 1_710_003_600_000_i64));
        let result = substitute_time_macros(q, window, QueryLanguage::Flux);
        assert!(
            result.contains("stop: '2024-03-09T17:00:00Z'"),
            "got: {result}"
        );
        // The literal `-6h` must be preserved.
        assert!(result.contains("start: -6h"), "got: {result}");
    }

    #[test]
    fn flux_multiple_occurrences_of_range_start_all_replaced() {
        let q = "from(bucket: \"x\") |> range(start: v.timeRangeStart) |> filter(fn: (r) => r._time >= v.timeRangeStart)";
        let window = Some((1_710_000_000_000_i64, 1_710_003_600_000_i64));
        let result = substitute_time_macros(q, window, QueryLanguage::Flux);
        assert!(!result.contains("v.timeRangeStart"), "got: {result}");
        assert_eq!(
            result.matches("2024-03-09T16:00:00Z").count(),
            2,
            "both occurrences must be replaced, got: {result}"
        );
    }

    #[test]
    fn flux_old_double_underscore_tokens_pass_through_unchanged() {
        // $__from / $__to are InfluxQL-only tokens. In Flux context they must
        // NOT be substituted — the user must use v.timeRangeStart/Stop instead.
        let q = "from(bucket: \"x\") |> range(start: $__from, stop: $__to)";
        let window = Some((1_710_000_000_000_i64, 1_710_003_600_000_i64));
        let result = substitute_time_macros(q, window, QueryLanguage::Flux);
        // Tokens remain verbatim — no expansion in Flux.
        assert_eq!(result, q, "Flux must not expand $__from/$__to");
    }

    // --- Non-InfluxDB languages pass through ---

    #[test]
    fn sql_language_passes_through_unchanged() {
        let q = "SELECT $__from FROM table WHERE id = $__to";
        let window = Some((1_710_000_000_000_i64, 1_710_003_600_000_i64));
        let result = substitute_time_macros(q, window, QueryLanguage::Sql);
        assert_eq!(result, q);
    }

    #[test]
    fn mongo_query_language_passes_through_unchanged() {
        let q = "{ \"$__from\": 1 }";
        let window = Some((1_710_000_000_000_i64, 1_710_003_600_000_i64));
        let result = substitute_time_macros(q, window, QueryLanguage::MongoQuery);
        assert_eq!(result, q);
    }

    #[test]
    fn redis_commands_language_passes_through_unchanged() {
        let q = "GET $__from";
        let window = Some((1_710_000_000_000_i64, 1_710_003_600_000_i64));
        let result = substitute_time_macros(q, window, QueryLanguage::RedisCommands);
        assert_eq!(result, q);
    }

    // --- RFC3339 determinism ---

    #[test]
    fn rfc3339_fixed_epoch_produces_expected_string() {
        // 1_710_000_000_000 ms = 2024-03-09T16:00:00Z
        assert_eq!(rfc3339(1_710_000_000_000), "2024-03-09T16:00:00Z");
    }

    #[test]
    fn rfc3339_second_epoch_ms_also_deterministic() {
        // 1_710_003_600_000 ms = 2024-03-09T17:00:00Z (1 hour later)
        assert_eq!(rfc3339(1_710_003_600_000), "2024-03-09T17:00:00Z");
    }
}
