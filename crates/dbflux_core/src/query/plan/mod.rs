//! Driver-agnostic structured query plan.
//!
//! Each driver runs its own EXPLAIN and maps the result into this model, so
//! the UI renders one tree regardless of the backend. Every numeric field is
//! optional because engines differ (SQLite reports no costs, for instance).

use crate::{DbError, ExecutionClassification, QueryLanguage, QueryResult, Value};

/// What a plan request asks the engine to do with the statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryPlanMode {
    /// Plan the statement without running it.
    #[default]
    Estimate,
    /// Run the statement and report actual rows and timings. Only offered for
    /// read statements; see [`analyze_allowed`].
    Analyze,
}

/// Request for the structured plan of one query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlanRequest {
    pub query: String,
    pub mode: QueryPlanMode,
}

impl QueryPlanRequest {
    /// An estimate request: the statement is planned, never run.
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            mode: QueryPlanMode::Estimate,
        }
    }

    /// An analyze request: the statement runs. Drivers refuse it unless
    /// [`analyze_allowed`] passes.
    pub fn analyzed(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            mode: QueryPlanMode::Analyze,
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
    /// Time until the first row, in milliseconds, per loop. Analyzed plans only.
    pub actual_startup_time: Option<f64>,
    /// Time until the last row, in milliseconds, per loop. Analyzed plans only.
    pub actual_total_time: Option<f64>,
    /// Rows returned per loop. Analyzed plans only.
    pub actual_rows: Option<f64>,
    /// Number of times the operation ran. `Some(0)` means it never ran.
    pub actual_loops: Option<u64>,
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

    /// Time spent in this node alone, in milliseconds: its total time minus
    /// the total time of its direct children, never below zero.
    ///
    /// Engines report times and rows per loop, so each node's total is its
    /// reported total time multiplied by its loops (one when the engine
    /// reports none). `None` when the node or any child reports no total time.
    pub fn actual_self_time(&self) -> Option<f64> {
        let total = self.actual_total_time_across_loops()?;
        let children = self
            .children
            .iter()
            .map(PlanNode::actual_total_time_across_loops)
            .sum::<Option<f64>>()?;
        Some((total - children).max(0.0))
    }

    fn actual_total_time_across_loops(&self) -> Option<f64> {
        let loops = self.actual_loops.unwrap_or(1) as f64;
        Some(self.actual_total_time? * loops)
    }

    /// How far the actual rows per loop are from the estimate, as a factor of
    /// at least one, in either direction. Both numbers are compared as at
    /// least one row, so an estimate of one row against none is no gap.
    /// `None` without both numbers or when the node never ran.
    fn estimate_gap_ratio(&self) -> Option<f64> {
        if self.actual_loops == Some(0) {
            return None;
        }

        let estimated = self.estimated_rows?.max(1.0);
        let actual = self.actual_rows?.max(1.0);
        Some(estimated.max(actual) / estimated.min(actual))
    }
}

/// The smallest factor between estimated and actual rows worth marking.
pub const ESTIMATE_GAP_THRESHOLD: f64 = 10.0;

/// The node whose row estimate is furthest from the actual rows.
#[derive(Debug, Clone, PartialEq)]
pub struct EstimateGap<'plan> {
    pub node: &'plan PlanNode,
    /// Child indexes from the plan's roots: the first is the root's index.
    pub path: Vec<usize>,
    /// Factor between estimated and actual rows per loop, at least
    /// [`ESTIMATE_GAP_THRESHOLD`].
    pub ratio: f64,
}

/// A query plan: the root node(s), warnings, and the engine's raw output.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    pub roots: Vec<PlanNode>,
    pub warnings: Vec<String>,
    pub raw: String,
    pub raw_format: PlanFormat,
    /// Time the engine spent planning, in milliseconds. Analyzed plans only.
    pub planning_time: Option<f64>,
    /// Time the engine spent executing, in milliseconds. Analyzed plans only.
    pub execution_time: Option<f64>,
}

impl QueryPlan {
    pub fn new(roots: Vec<PlanNode>, raw: impl Into<String>, raw_format: PlanFormat) -> Self {
        Self {
            roots,
            warnings: Vec::new(),
            raw: raw.into(),
            raw_format,
            planning_time: None,
            execution_time: None,
        }
    }

    /// The node with the largest gap between estimated and actual rows, when
    /// that gap is at least [`ESTIMATE_GAP_THRESHOLD`] times. The deepest node
    /// wins a tie, and the first one in plan order when depth ties too.
    pub fn largest_estimate_gap(&self) -> Option<EstimateGap<'_>> {
        let mut best: Option<EstimateGap<'_>> = None;

        for (index, root) in self.roots.iter().enumerate() {
            collect_largest_gap(root, &mut vec![index], &mut best);
        }

        best
    }
}

fn collect_largest_gap<'plan>(
    node: &'plan PlanNode,
    path: &mut Vec<usize>,
    best: &mut Option<EstimateGap<'plan>>,
) {
    if let Some(ratio) = node.estimate_gap_ratio()
        && ratio >= ESTIMATE_GAP_THRESHOLD
    {
        let replaces = match best {
            None => true,
            Some(current) => {
                ratio > current.ratio || (ratio == current.ratio && path.len() > current.path.len())
            }
        };

        if replaces {
            *best = Some(EstimateGap {
                node,
                path: path.clone(),
                ratio,
            });
        }
    }

    for (index, child) in node.children.iter().enumerate() {
        path.push(index);
        collect_largest_gap(child, path, best);
        path.pop();
    }
}

/// Refuses an analyzed plan unless the shared classifier marks `statement` as
/// a read. `EXPLAIN ANALYZE` runs the statement, so a write is refused before
/// anything is sent to the server.
///
/// Only `Read` and `Metadata` pass; a language with no read verdict is
/// refused like a write. This is the first layer: drivers also run the
/// statement where the engine itself refuses writes.
pub fn analyze_allowed(language: &QueryLanguage, statement: &str) -> Result<(), DbError> {
    match crate::classify_query_for_language(language, statement) {
        ExecutionClassification::Read | ExecutionClassification::Metadata => Ok(()),
        _ => Err(DbError::query_failed(
            "Analyze is only available for read statements; this statement is not a plain read, so only the estimate can run.",
        )),
    }
}

/// The error a driver without analyzed plans returns for an analyze request,
/// before anything is sent.
pub fn analyze_unsupported() -> DbError {
    DbError::NotSupported("Analyzed plans are not supported by this driver".to_string())
}

/// The statement an analyzed SQL plan runs: exactly one statement, without
/// its terminator, that [`analyze_allowed`] accepts.
pub fn analyze_statement(language: &QueryLanguage, query: &str) -> Result<String, DbError> {
    let statement = single_plan_statement(query)?;
    analyze_allowed(language, &statement)?;
    Ok(statement)
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
