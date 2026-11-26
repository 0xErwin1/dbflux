//! Structured query plan, built from `EXPLAIN QUERY PLAN`. The row parser is
//! shared through `dbflux_core` because both SQLite-dialect drivers return the
//! same `(id, parent, notused, detail)` rows.

use dbflux_core::DbError;

/// Builds the plan statement for `query` through the shared single-statement
/// guard. `EXPLAIN QUERY PLAN` never runs the statement.
pub(crate) fn explain_statement(query: &str) -> Result<String, DbError> {
    Ok(format!(
        "EXPLAIN QUERY PLAN {}",
        dbflux_core::single_plan_statement(query)?
    ))
}

#[cfg(test)]
mod tests;
