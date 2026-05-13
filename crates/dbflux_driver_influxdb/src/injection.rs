//! Time-window injection for InfluxQL and Flux queries.
//!
//! When users omit time predicates (or don't write `|> range(...)` in Flux),
//! the driver appends the window from `ExecutionSourceContext::CollectionWindow`
//! so query results are scoped to the selected time range.
//!
//! Detection is intentionally regex-based and operates on the raw query string.
//! Known limitation: regex may false-positive on quoted string literals that
//! contain `time <` or `|> range(`. This is documented in the README.

use regex::Regex;
use std::sync::LazyLock;

// ---------------------------------------------------------------------------
// Window type
// ---------------------------------------------------------------------------

/// A resolved time window to be injected into a query.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedWindow {
    /// RFC 3339 start time string, e.g. "2024-01-01T00:00:00Z".
    pub start_rfc3339: Option<String>,
    /// RFC 3339 end time string, e.g. "2024-01-01T01:00:00Z".
    pub end_rfc3339: Option<String>,
}

// ---------------------------------------------------------------------------
// Compiled regexes — compiled once, reused across calls
// ---------------------------------------------------------------------------

// Regex literals are compiled once via LazyLock. Invalid patterns are a programming
// error, not a runtime condition, so `.expect()` is intentional here.
#[allow(clippy::expect_used)]
static INFLUXQL_TIME_PREDICATE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\btime\s*[<>=!]").expect("valid regex"));

#[allow(clippy::expect_used)]
static FLUX_RANGE_CALL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\|>\s*range\s*\(").expect("valid regex"));

#[allow(clippy::expect_used)]
static INFLUXQL_WHERE_CLAUSE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bWHERE\b").expect("valid regex"));

#[allow(clippy::expect_used)]
static INFLUXQL_BOUNDARY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(GROUP\s+BY|ORDER\s+BY|LIMIT|SLIMIT|OFFSET|SOFFSET|FILL|TZ)\b")
        .expect("valid regex")
});

#[allow(clippy::expect_used)]
static FLUX_FROM_CALL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bfrom\s*\(").expect("valid regex"));

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Returns `true` if the query already contains a time predicate.
pub fn influxql_has_time_predicate(query: &str) -> bool {
    INFLUXQL_TIME_PREDICATE.is_match(query)
}

/// Returns `true` if the Flux query already contains a `|> range(` call.
pub fn flux_has_range_call(query: &str) -> bool {
    FLUX_RANGE_CALL.is_match(query)
}

/// Inject time bounds into an InfluxQL query if no time predicate is present.
///
/// For multi-statement queries separated by `;`, injection is applied to each
/// statement individually.
///
/// Returns the query unchanged when:
/// - Both window bounds are `None`.
/// - The query already contains `time <` / `time >` / etc.
pub fn inject_influxql_window(query: &str, window: &ResolvedWindow) -> String {
    if window.start_rfc3339.is_none() && window.end_rfc3339.is_none() {
        return query.to_string();
    }

    // Split on `;` to handle multi-statement queries.
    let statements: Vec<&str> = query.split(';').collect();
    let mut results = Vec::with_capacity(statements.len());

    for stmt in &statements {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            results.push(trimmed.to_string());
            continue;
        }

        results.push(inject_into_single_influxql(trimmed, window));
    }

    results.join("; ")
}

/// Inject a `|> range(...)` call into a Flux query after the first `from(...)` call.
///
/// Returns the query unchanged when:
/// - Both window bounds are `None`.
/// - A `|> range(` call already exists.
/// - No `from(` call is found.
pub fn inject_flux_window(query: &str, window: &ResolvedWindow) -> String {
    if window.start_rfc3339.is_none() && window.end_rfc3339.is_none() {
        return query.to_string();
    }

    if flux_has_range_call(query) {
        return query.to_string();
    }

    let Some(from_match) = FLUX_FROM_CALL.find(query) else {
        return query.to_string();
    };

    // Find the closing paren of the first `from(...)`.
    let after_from = &query[from_match.start()..];
    let Some(close_paren_offset) = after_from.find(')') else {
        return query.to_string();
    };

    let insert_at = from_match.start() + close_paren_offset + 1;

    let range_call = build_flux_range_call(window);
    let mut result = String::with_capacity(query.len() + range_call.len() + 4);
    result.push_str(&query[..insert_at]);
    result.push_str("\n  |> range(");
    result.push_str(&range_call);
    result.push(')');
    result.push_str(&query[insert_at..]);

    result
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Inject time bounds into a single InfluxQL statement (no `;`).
fn inject_into_single_influxql(stmt: &str, window: &ResolvedWindow) -> String {
    if influxql_has_time_predicate(stmt) {
        // User owns the time predicates; leave query as-is.
        return stmt.to_string();
    }

    let time_clause = build_influxql_time_clause(window);

    if INFLUXQL_WHERE_CLAUSE.is_match(stmt) {
        // WHERE exists but no time predicate — append after the WHERE conditions,
        // before any GROUP BY / ORDER BY / LIMIT / etc.
        let insert_pos = find_influxql_boundary_pos(stmt);
        let mut result = String::with_capacity(stmt.len() + time_clause.len() + 8);
        result.push_str(&stmt[..insert_pos]);
        result.push_str(" AND ");
        result.push_str(&time_clause);
        result.push_str(&stmt[insert_pos..]);
        result
    } else {
        // No WHERE clause — insert before boundary keywords or at the end.
        let insert_pos = find_influxql_boundary_pos(stmt);
        let mut result = String::with_capacity(stmt.len() + time_clause.len() + 12);
        result.push_str(&stmt[..insert_pos]);
        let trailing = stmt[..insert_pos].trim_end();
        if trailing != &stmt[..insert_pos] {
            // preserve spacing
        }
        result.push_str(" WHERE ");
        result.push_str(&time_clause);
        result.push_str(&stmt[insert_pos..]);
        result
    }
}

/// Find the byte position where GROUP BY / ORDER BY / LIMIT / etc. begin.
/// Returns `stmt.len()` if no such keyword is found.
fn find_influxql_boundary_pos(stmt: &str) -> usize {
    INFLUXQL_BOUNDARY
        .find(stmt)
        .map(|m| m.start())
        .unwrap_or(stmt.len())
}

/// Build the InfluxQL time predicate expression for the given window.
fn build_influxql_time_clause(window: &ResolvedWindow) -> String {
    match (&window.start_rfc3339, &window.end_rfc3339) {
        (Some(start), Some(end)) => {
            format!("time >= '{start}' AND time <= '{end}'")
        }
        (Some(start), None) => format!("time >= '{start}'"),
        (None, Some(end)) => format!("time <= '{end}'"),
        (None, None) => String::new(),
    }
}

/// Build the argument string for Flux `|> range(start: ..., stop: ...)`.
fn build_flux_range_call(window: &ResolvedWindow) -> String {
    match (&window.start_rfc3339, &window.end_rfc3339) {
        (Some(start), Some(end)) => format!("start: {start}, stop: {end}"),
        (Some(start), None) => format!("start: {start}"),
        (None, Some(end)) => format!("start: 0, stop: {end}"),
        (None, None) => String::new(),
    }
}

// ---------------------------------------------------------------------------
// Tests (C.2.1 – C.2.12)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn window(start: Option<&str>, end: Option<&str>) -> ResolvedWindow {
        ResolvedWindow {
            start_rfc3339: start.map(str::to_string),
            end_rfc3339: end.map(str::to_string),
        }
    }

    // C.2.1 — no window, no injection
    #[test]
    fn influxql_no_window_returns_unchanged() {
        let q = "SELECT * FROM cpu";
        assert_eq!(inject_influxql_window(q, &window(None, None)), q);
    }

    // C.2.2 — both bounds, no WHERE
    #[test]
    fn influxql_both_bounds_no_where_appended() {
        let q = "SELECT * FROM cpu";
        let result = inject_influxql_window(
            q,
            &window(Some("2024-01-01T00:00:00Z"), Some("2024-01-01T01:00:00Z")),
        );
        assert!(
            result.contains(
                "WHERE time >= '2024-01-01T00:00:00Z' AND time <= '2024-01-01T01:00:00Z'"
            ),
            "got: {result}"
        );
    }

    // C.2.2 — injected BEFORE GROUP BY
    #[test]
    fn influxql_both_bounds_injected_before_group_by() {
        let q = "SELECT mean(value) FROM cpu GROUP BY time(1m)";
        let result = inject_influxql_window(
            q,
            &window(Some("2024-01-01T00:00:00Z"), Some("2024-01-01T01:00:00Z")),
        );
        let where_pos = result.find("WHERE").expect("WHERE injected");
        let group_pos = result.find("GROUP BY").expect("GROUP BY preserved");
        assert!(
            where_pos < group_pos,
            "WHERE must come before GROUP BY in: {result}"
        );
    }

    // C.2.3 — WHERE exists, no time predicate
    #[test]
    fn influxql_where_exists_no_time_appends_and_clause() {
        let q = "SELECT * FROM cpu WHERE host = 'server1'";
        let result = inject_influxql_window(
            q,
            &window(Some("2024-01-01T00:00:00Z"), Some("2024-01-01T01:00:00Z")),
        );
        assert!(
            result.contains("AND time >= '2024-01-01T00:00:00Z'"),
            "got: {result}"
        );
        assert!(
            result.contains("WHERE host = 'server1'"),
            "original WHERE kept, got: {result}"
        );
    }

    // C.2.4 — WHERE with time predicate → unchanged
    #[test]
    fn influxql_where_with_time_predicate_unchanged() {
        let q = "SELECT * FROM cpu WHERE time > now() - 1h";
        let result = inject_influxql_window(
            q,
            &window(Some("2024-01-01T00:00:00Z"), Some("2024-01-01T01:00:00Z")),
        );
        assert_eq!(result, q, "user-owned time predicate must not be modified");
    }

    // C.2.5 — start-only
    #[test]
    fn influxql_start_only_injects_lower_bound() {
        let q = "SELECT * FROM cpu";
        let result = inject_influxql_window(q, &window(Some("2024-01-01T00:00:00Z"), None));
        assert!(
            result.contains("time >= '2024-01-01T00:00:00Z'"),
            "got: {result}"
        );
        assert!(
            !result.contains("time <="),
            "no upper bound expected, got: {result}"
        );
    }

    // C.2.6 — end-only
    #[test]
    fn influxql_end_only_injects_upper_bound() {
        let q = "SELECT * FROM cpu";
        let result = inject_influxql_window(q, &window(None, Some("2024-01-01T01:00:00Z")));
        assert!(
            result.contains("time <= '2024-01-01T01:00:00Z'"),
            "got: {result}"
        );
        assert!(
            !result.contains("time >="),
            "no lower bound expected, got: {result}"
        );
    }

    // C.2.7 — multi-statement
    #[test]
    fn influxql_multi_statement_applied_per_statement() {
        let q = "SELECT * FROM cpu; SELECT * FROM mem";
        let w = window(Some("2024-01-01T00:00:00Z"), Some("2024-01-01T01:00:00Z"));
        let result = inject_influxql_window(q, &w);
        // Both statements should have time predicates
        let parts: Vec<&str> = result.split(';').collect();
        assert_eq!(parts.len(), 2, "two statements expected");
        for part in &parts {
            assert!(
                part.contains("time >= '2024-01-01T00:00:00Z'"),
                "each statement must get time bounds, got: {part}"
            );
        }
    }

    // C.2.8 — Flux, no range, single from
    #[test]
    fn flux_no_range_single_from_injects_range_after_from() {
        let q = r#"from(bucket: "my-bucket") |> filter(fn: (r) => r._measurement == "cpu")"#;
        let w = window(Some("2024-01-01T00:00:00Z"), Some("2024-01-01T01:00:00Z"));
        let result = inject_flux_window(q, &w);
        assert!(
            result.contains("|> range("),
            "range must be injected, got: {result}"
        );
        assert!(
            result.contains("start: 2024-01-01T00:00:00Z"),
            "got: {result}"
        );
        assert!(
            result.contains("stop: 2024-01-01T01:00:00Z"),
            "got: {result}"
        );
        // range must come before filter
        let range_pos = result.find("|> range(").unwrap();
        let filter_pos = result.find("|> filter(").unwrap();
        assert!(
            range_pos < filter_pos,
            "range must come before filter, got: {result}"
        );
    }

    // C.2.9 — Flux, existing range → unchanged
    #[test]
    fn flux_existing_range_unchanged() {
        let q = r#"from(bucket: "b") |> range(start: -1h) |> filter(fn: (r) => true)"#;
        let w = window(Some("2024-01-01T00:00:00Z"), Some("2024-01-01T01:00:00Z"));
        let result = inject_flux_window(q, &w);
        assert_eq!(result, q, "existing range must not be modified");
    }

    // C.2.10 — Flux, no from → unchanged (no panic)
    #[test]
    fn flux_no_from_unchanged_no_panic() {
        let q = "union(tables: [t1, t2]) |> filter(fn: (r) => true)";
        let w = window(Some("2024-01-01T00:00:00Z"), Some("2024-01-01T01:00:00Z"));
        let result = inject_flux_window(q, &w);
        assert_eq!(result, q, "no from means no injection, got: {result}");
    }

    // C.2.11 — Flux, multiple from → first only
    #[test]
    fn flux_multiple_from_injects_after_first_only() {
        let q = r#"from(bucket: "a") |> filter(fn: (r) => true) |> join(tables: {b: from(bucket: "b")})"#;
        let w = window(Some("2024-01-01T00:00:00Z"), Some("2024-01-01T01:00:00Z"));
        let result = inject_flux_window(q, &w);
        // Exactly one range call
        let count = result.matches("|> range(").count();
        assert_eq!(count, 1, "only one range should be injected, got: {result}");
    }

    // C.2.12 — Flux start-only
    #[test]
    fn flux_start_only_injects_start_without_stop() {
        let q = r#"from(bucket: "b")"#;
        let w = window(Some("2024-01-01T00:00:00Z"), None);
        let result = inject_flux_window(q, &w);
        assert!(
            result.contains("start: 2024-01-01T00:00:00Z"),
            "got: {result}"
        );
        assert!(!result.contains("stop:"), "no stop expected, got: {result}");
    }

    // C.2.12 — Flux end-only
    #[test]
    fn flux_end_only_injects_stop_with_epoch_start() {
        let q = r#"from(bucket: "b")"#;
        let w = window(None, Some("2024-01-01T01:00:00Z"));
        let result = inject_flux_window(q, &w);
        assert!(
            result.contains("stop: 2024-01-01T01:00:00Z"),
            "got: {result}"
        );
    }

    // helper for has_time_predicate
    #[test]
    fn influxql_has_time_predicate_detection() {
        assert!(influxql_has_time_predicate("WHERE time > now() - 1h"));
        assert!(influxql_has_time_predicate("WHERE time >= '2024-01-01'"));
        assert!(!influxql_has_time_predicate("WHERE host = 'server1'"));
    }

    #[test]
    fn flux_has_range_call_detection() {
        assert!(flux_has_range_call(r#"|> range(start: -1h)"#));
        assert!(flux_has_range_call(r#"|>range(start: 0)"#));
        assert!(!flux_has_range_call(
            r#"from(bucket: "b") |> filter(fn: (r) => true)"#
        ));
    }
}
