//! Driver-agnostic structured query plan.
//!
//! Each driver runs its own EXPLAIN and maps the result into this model, so
//! the UI renders one tree regardless of the backend. Every numeric field is
//! optional because engines differ (SQLite reports no costs, for instance).

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
    /// Remaining engine attributes (join type, filters, sort keys...).
    pub details: Vec<(String, String)>,
    pub warnings: Vec<String>,
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    pub fn new(operation: impl Into<String>) -> Self {
        Self {
            operation: operation.into(),
            ..Self::default()
        }
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
