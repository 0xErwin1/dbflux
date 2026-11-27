//! Driver-agnostic structured query plan.
//!
//! Each driver runs its own EXPLAIN and maps the result into this model, so
//! the UI renders one tree regardless of the backend. Every numeric field is
//! optional because engines differ (SQLite reports no costs, for instance).

use crate::{DbError, QueryLanguage, QueryResult, Value};

/// Request for the structured plan of one query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlanRequest {
    pub query: String,
}

impl QueryPlanRequest {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
        }
    }
}

/// Format of the raw plan text a driver returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanFormat {
    Json,
    Text,
    Xml,
}

/// One operation in the plan tree.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PlanNode {
    /// Operation name as the engine reports it (for example `Index Scan`).
    pub operation: String,
    /// Table, view or other relation the operation reads.
    pub relation: Option<String>,
    /// Index the operation uses.
    pub index: Option<String>,
    pub estimated_startup_cost: Option<f64>,
    pub estimated_total_cost: Option<f64>,
    pub estimated_rows: Option<f64>,
    /// Plain-language explanation of what the operation does, from the
    /// driver's own glossary. `None` when the driver has no entry for it.
    pub description: Option<String>,
    /// The one condition that best explains the node (filter, index
    /// condition, join condition or sort key), chosen by the driver.
    pub key_condition: Option<PlanCondition>,
    /// Remaining engine attributes (join type, filters, sort keys...).
    pub details: Vec<(String, String)>,
    pub warnings: Vec<String>,
    pub children: Vec<PlanNode>,
}

/// A labelled condition shown on a plan node, such as `Index Cond`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanCondition {
    /// The engine's own name for the condition (for example `Filter`).
    pub label: String,
    pub value: String,
}

impl PlanNode {
    pub fn new(operation: impl Into<String>) -> Self {
        Self {
            operation: operation.into(),
            ..Self::default()
        }
    }

    /// Estimated cost of this node alone: its total cost minus the total
    /// cost of its direct children, never below zero. `None` when the node
    /// or any child reports no total cost.
    pub fn estimated_self_cost(&self) -> Option<f64> {
        let total = self.estimated_total_cost?;
        let children = self
            .children
            .iter()
            .map(|child| child.estimated_total_cost)
            .sum::<Option<f64>>()?;
        Some((total - children).max(0.0))
    }
}

/// A query plan: the root node(s), warnings, and the engine's raw output.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    pub roots: Vec<PlanNode>,
    pub warnings: Vec<String>,
    pub raw: String,
    pub raw_format: PlanFormat,
}

impl QueryPlan {
    pub fn new(roots: Vec<PlanNode>, raw: impl Into<String>, raw_format: PlanFormat) -> Self {
        Self {
            roots,
            warnings: Vec::new(),
            raw: raw.into(),
            raw_format,
        }
    }
}

/// Returns the one statement a plan covers, without its terminator.
///
/// The text is split with the shared SQL splitter and anything other than one
/// statement is refused, so a buffer is never sent to EXPLAIN as one string.
/// Drivers wrap the result in their own plain `EXPLAIN`; `ANALYZE` is never
/// added, so a write is planned without running.
pub fn single_plan_statement(query: &str) -> Result<String, DbError> {
    let mut statements = QueryLanguage::Sql.split_statements(query);
    statements.retain(|statement| !crate::strip_leading_comments(statement).is_empty());

    match statements.len() {
        0 => Err(DbError::query_failed(
            "A query plan needs a statement; the query is empty.",
        )),
        1 => Ok(statements.remove(0)),
        count => Err(DbError::query_failed(format!(
            "A query plan covers exactly one statement; the query has {count}. Select a single statement."
        ))),
    }
}

/// Reads the plan text an EXPLAIN returned in the first cell of the first row,
/// or in the result's text body.
pub fn plan_text_from_result(result: &QueryResult) -> Result<String, DbError> {
    match result.rows.first().and_then(|row| row.first()) {
        Some(Value::Json(text) | Value::Text(text)) => Ok(text.clone()),
        _ => result
            .text_body
            .clone()
            .ok_or_else(|| DbError::query_failed("EXPLAIN returned no plan")),
    }
}

mod sqlite;
pub use sqlite::{parse_sqlite_query_plan, sqlite_plan_statement};

#[cfg(test)]
mod tests;
