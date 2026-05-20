use crate::{ExecutionContext, QueryLanguage, Value};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

// -- Query Result Shape --

/// Shape of data returned by a query. Set by the driver; the UI never sniffs content.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum QueryResultShape {
    /// Tabular data with columns and rows (SQL results, Redis arrays that
    /// fit a uniform structure).
    #[default]
    Table,

    /// Structured JSON (MongoDB documents, Redis hash results).
    Json,

    /// Plain text (Redis status replies, single-value results).
    Text,

    /// Raw binary data (Redis bulk strings that failed UTF-8 decode).
    Binary,
}

impl QueryResultShape {
    pub fn is_table(&self) -> bool {
        matches!(self, Self::Table)
    }

    pub fn is_json(&self) -> bool {
        matches!(self, Self::Json)
    }

    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text)
    }

    pub fn is_binary(&self) -> bool {
        matches!(self, Self::Binary)
    }
}

// -- Query Request --

/// Parameters for executing a SQL query.
#[derive(Debug, Clone, Default)]
pub struct QueryRequest {
    /// The SQL statement to execute.
    pub sql: String,

    /// Bind parameters for parameterized queries.
    pub params: Vec<Value>,

    /// Maximum number of rows to return (applied as SQL LIMIT).
    pub limit: Option<u32>,

    /// Number of rows to skip (applied as SQL OFFSET).
    pub offset: Option<u32>,

    /// Maximum time to wait for query completion.
    pub statement_timeout: Option<Duration>,

    /// Target database for query execution (MySQL/MariaDB).
    ///
    /// When set, the driver issues `USE database` before executing the query
    /// if the connection's current database differs. Ignored by PostgreSQL
    /// and SQLite (which use connection-level database selection).
    pub database: Option<String>,

    /// Full per-document execution context for drivers that need more than
    /// the compatibility `database` field.
    pub execution_context: Option<ExecutionContext>,
}

impl QueryRequest {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            ..Default::default()
        }
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn with_database(mut self, database: Option<String>) -> Self {
        self.database = database;
        self
    }

    pub fn with_execution_context(mut self, execution_context: Option<ExecutionContext>) -> Self {
        self.execution_context = execution_context;
        self
    }
}

/// A single row of query results.
pub type Row = Vec<Value>;

/// Semantic classification of a result column.
///
/// Drivers populate this from their native type information. The chart engine
/// relies on this seam to detect time and numeric columns without inspecting
/// `type_name` strings or driver identifiers. `Unknown` is an explicit choice —
/// no `Default` impl is provided so every construction site opts in consciously.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ColumnKind {
    /// A date/time or timestamp column.
    Timestamp,
    /// A floating-point numeric column.
    Float,
    /// An integer numeric column.
    Integer,
    /// A text/string column.
    Text,
    /// The driver could not classify this column.
    Unknown,
}

/// Returns `ColumnKind::Unknown`. Used as a serde default so that JSON
/// fixtures serialised before this field was introduced deserialise cleanly.
pub fn default_column_kind_unknown() -> ColumnKind {
    ColumnKind::Unknown
}

/// Metadata for a result column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    /// Column name as returned by the database.
    pub name: String,

    /// Database-specific type name (e.g., "varchar", "int4", "TEXT").
    pub type_name: String,

    /// Semantic classification inferred from the driver's native type system.
    ///
    /// The chart engine uses this to detect `Timestamp` (X axis) and
    /// `Float`/`Integer` (Y axis) candidates without inspecting `type_name`.
    /// Callers that do not have enough type information should pass `Unknown`.
    #[serde(default = "default_column_kind_unknown")]
    pub kind: ColumnKind,

    /// Whether the column allows NULL values.
    pub nullable: bool,

    /// Whether the column is part of the primary key.
    pub is_primary_key: bool,
}

/// The effective time window resolved by the driver after executing a time-series query.
///
/// Drivers that interpret relative time ranges (e.g., Flux `range(start: -1h)`) can populate
/// this so the UI can display the concrete UTC boundaries that were actually queried.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedWindow {
    /// Start of the resolved window in milliseconds since Unix epoch (UTC).
    pub start_ms: i64,
    /// End of the resolved window in milliseconds since Unix epoch (UTC).
    pub end_ms: i64,
    /// Query language used to produce this result (used for UI context display).
    pub language: QueryLanguage,
}

// -- Query Result --

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub shape: QueryResultShape,
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Row>,
    pub affected_rows: Option<u64>,
    pub execution_time: Duration,
    pub text_body: Option<String>,
    pub raw_bytes: Option<Vec<u8>>,
    /// Pagination token for fetching the next page of results (used by PageToken-style pagination).
    pub next_page_token: Option<String>,
    /// Resolved time window for time-series queries. `None` for non-time-series results.
    pub resolved_window: Option<ResolvedWindow>,
    /// Driver-provided structured fields forwarded verbatim into the audit event's
    /// `details_json`. Drivers that need extra audit context (e.g., language, version,
    /// injected_window) populate this map; the runner merges it into the event without
    /// any driver-id branching.
    pub metadata_extra: Option<std::collections::HashMap<String, serde_json::Value>>,
    /// Additional result sets produced by the same query batch.
    ///
    /// Populated by drivers whose protocol can return more than one result
    /// set per statement-batch (e.g. SQL Server `SELECT 1; SELECT 2;`, or a
    /// stored procedure with multiple `SELECT`s). The primary result is
    /// `self`; the secondary sets follow in batch order. UIs that want every
    /// set should iterate `iter_result_sets()`.
    ///
    /// Each entry must itself have an empty `additional_results` field —
    /// `push_additional_result()` enforces this by flattening on insert.
    /// Drivers that always produce a single result set leave this empty
    /// (the default), so existing callers are unaffected.
    pub additional_results: Vec<QueryResult>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            shape: QueryResultShape::Table,
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: None,
            execution_time: Duration::ZERO,
            text_body: None,
            raw_bytes: None,
            next_page_token: None,
            resolved_window: None,
            metadata_extra: None,
            additional_results: Vec::new(),
        }
    }

    /// Attaches a resolved time window to this result (builder-style).
    pub fn with_resolved_window(mut self, window: ResolvedWindow) -> Self {
        self.resolved_window = Some(window);
        self
    }

    pub fn table(
        columns: Vec<ColumnMeta>,
        rows: Vec<Row>,
        affected_rows: Option<u64>,
        execution_time: Duration,
    ) -> Self {
        Self {
            shape: QueryResultShape::Table,
            columns,
            rows,
            affected_rows,
            execution_time,
            text_body: None,
            raw_bytes: None,
            next_page_token: None,
            resolved_window: None,
            metadata_extra: None,
            additional_results: Vec::new(),
        }
    }

    pub fn json(columns: Vec<ColumnMeta>, rows: Vec<Row>, execution_time: Duration) -> Self {
        Self {
            shape: QueryResultShape::Json,
            columns,
            rows,
            affected_rows: None,
            execution_time,
            text_body: None,
            raw_bytes: None,
            next_page_token: None,
            resolved_window: None,
            metadata_extra: None,
            additional_results: Vec::new(),
        }
    }

    pub fn text(body: String, execution_time: Duration) -> Self {
        Self {
            shape: QueryResultShape::Text,
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: None,
            execution_time,
            text_body: Some(body),
            raw_bytes: None,
            next_page_token: None,
            resolved_window: None,
            metadata_extra: None,
            additional_results: Vec::new(),
        }
    }

    pub fn binary(data: Vec<u8>, execution_time: Duration) -> Self {
        Self {
            shape: QueryResultShape::Binary,
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: None,
            execution_time,
            text_body: None,
            raw_bytes: Some(data),
            next_page_token: None,
            resolved_window: None,
            metadata_extra: None,
            additional_results: Vec::new(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns the number of result sets in this query result, counting the
    /// primary set. Drivers that produce a single set return `1`.
    pub fn result_set_count(&self) -> usize {
        1 + self.additional_results.len()
    }

    /// Returns `true` when the driver produced more than one result set for
    /// this batch (a `SELECT 1; SELECT 2;` style query or a multi-`SELECT`
    /// stored procedure).
    pub fn has_additional_results(&self) -> bool {
        !self.additional_results.is_empty()
    }

    /// Iterate every result set produced by the batch, primary first.
    pub fn iter_result_sets(&self) -> impl Iterator<Item = &QueryResult> {
        std::iter::once(self).chain(self.additional_results.iter())
    }

    /// Consuming iterator over every result set in the batch, primary first.
    ///
    /// Each yielded `QueryResult` has an empty `additional_results` field;
    /// the recursion is flattened at construction time via
    /// `push_additional_result`.
    pub fn into_result_sets(mut self) -> impl Iterator<Item = QueryResult> {
        let extras = std::mem::take(&mut self.additional_results);
        std::iter::once(self).chain(extras)
    }

    /// Attach a result set produced by the same batch. Flattens any nested
    /// `additional_results` from the argument so callers can safely pass a
    /// full `QueryResult` without producing a recursive tree.
    pub fn push_additional_result(&mut self, mut other: QueryResult) {
        let nested = std::mem::take(&mut other.additional_results);
        self.additional_results.push(other);
        self.additional_results.extend(nested);
    }
}

/// Opaque handle for cancelling a running query.
///
/// Returned by `Connection::execute_with_handle()`. The internal data
/// is driver-specific (e.g., PostgreSQL backend PID) but hidden from the UI.
#[derive(Debug, Clone)]
pub struct QueryHandle {
    pub id: Uuid,
}

impl QueryHandle {
    pub fn new() -> Self {
        Self { id: Uuid::new_v4() }
    }
}

impl Default for QueryHandle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryLanguage;

    #[test]
    fn resolved_window_serde_roundtrip() {
        let window = ResolvedWindow {
            start_ms: 1_000_000,
            end_ms: 2_000_000,
            language: QueryLanguage::Flux,
        };

        let serialized = serde_json::to_string(&window).expect("serialize ResolvedWindow");
        let deserialized: ResolvedWindow =
            serde_json::from_str(&serialized).expect("deserialize ResolvedWindow");

        assert_eq!(deserialized.start_ms, 1_000_000);
        assert_eq!(deserialized.end_ms, 2_000_000);
        assert_eq!(deserialized.language, QueryLanguage::Flux);
    }

    #[test]
    fn query_result_resolved_window_defaults_to_none() {
        let result = QueryResult::empty();
        assert!(
            result.resolved_window.is_none(),
            "resolved_window must default to None"
        );
    }

    #[test]
    fn query_result_with_resolved_window_builder() {
        let window = ResolvedWindow {
            start_ms: 0,
            end_ms: 3_600_000,
            language: QueryLanguage::InfluxQuery,
        };

        let result = QueryResult::empty().with_resolved_window(window.clone());
        assert_eq!(result.resolved_window.as_ref(), Some(&window));
    }

    fn make_set(label: &str) -> QueryResult {
        QueryResult::table(
            vec![ColumnMeta {
                name: label.to_string(),
                type_name: "text".to_string(),
                kind: ColumnKind::Unknown,
                nullable: true,
                is_primary_key: false,
            }],
            Vec::new(),
            None,
            Duration::ZERO,
        )
    }

    #[test]
    fn default_query_result_has_no_additional_sets() {
        let result = QueryResult::empty();
        assert_eq!(result.result_set_count(), 1);
        assert!(!result.has_additional_results());
        assert_eq!(result.iter_result_sets().count(), 1);
    }

    #[test]
    fn push_additional_result_appends_in_order() {
        let mut primary = make_set("a");
        primary.push_additional_result(make_set("b"));
        primary.push_additional_result(make_set("c"));

        assert_eq!(primary.result_set_count(), 3);
        assert!(primary.has_additional_results());

        let labels: Vec<&str> = primary
            .iter_result_sets()
            .map(|r| r.columns[0].name.as_str())
            .collect();
        assert_eq!(labels, vec!["a", "b", "c"]);
    }

    #[test]
    fn push_additional_result_flattens_nested_extras() {
        // Build a "chained" result and verify push_additional_result flattens
        // the nested additional_results to avoid building a recursive tree.
        let mut inner = make_set("b");
        inner.push_additional_result(make_set("c"));

        let mut primary = make_set("a");
        primary.push_additional_result(inner);

        // Three total sets, all at the same level.
        assert_eq!(primary.result_set_count(), 3);

        for extra in &primary.additional_results {
            assert!(
                extra.additional_results.is_empty(),
                "nested additional_results should have been flattened"
            );
        }
    }

    #[test]
    fn into_result_sets_yields_primary_then_extras() {
        let mut primary = make_set("a");
        primary.push_additional_result(make_set("b"));

        let labels: Vec<String> = primary
            .into_result_sets()
            .map(|r| r.columns[0].name.clone())
            .collect();
        assert_eq!(labels, vec!["a".to_string(), "b".to_string()]);
    }
}
