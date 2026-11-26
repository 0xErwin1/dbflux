//! Structured query plan for PostgreSQL, built from `EXPLAIN (FORMAT JSON)`.

use dbflux_core::{DbError, PlanFormat, PlanNode, QueryLanguage, QueryPlan, QueryResult, Value};
use serde_json::Value as Json;

/// Keys mapped to `PlanNode` fields; every other scalar/array key is a detail.
const MAPPED_KEYS: [&str; 7] = [
    "Node Type",
    "Relation Name",
    "Index Name",
    "Startup Cost",
    "Total Cost",
    "Plan Rows",
    "Plans",
];

/// Builds the plan statement for `query`.
///
/// The text is split with the shared SQL splitter and anything other than one
/// statement is refused, so a buffer is never sent to EXPLAIN as one string.
/// Plain `EXPLAIN` never executes the statement, so writes are allowed;
/// `ANALYZE` is deliberately never added.
pub(crate) fn explain_statement(query: &str) -> Result<String, DbError> {
    let mut statements = QueryLanguage::Sql.split_statements(query);

    match statements.len() {
        0 => Err(DbError::query_failed(
            "A query plan needs a statement; the query is empty.",
        )),
        1 => Ok(format!("EXPLAIN (FORMAT JSON) {}", statements.remove(0))),
        count => Err(DbError::query_failed(format!(
            "A query plan covers exactly one statement; the query has {count}. Select a single statement."
        ))),
    }
}

/// Extracts the JSON document `EXPLAIN (FORMAT JSON)` returned.
pub(crate) fn raw_plan_from_result(result: &QueryResult) -> Result<String, DbError> {
    let cell = result.rows.first().and_then(|row| row.first());

    match cell {
        Some(Value::Json(text) | Value::Text(text)) => Ok(text.clone()),
        _ => result
            .text_body
            .clone()
            .ok_or_else(|| DbError::query_failed("EXPLAIN returned no plan")),
    }
}

pub(crate) fn parse_explain_json(raw: &str) -> Result<QueryPlan, DbError> {
    let document: Json = serde_json::from_str(raw).map_err(|error| {
        DbError::query_failed(format!("EXPLAIN returned invalid JSON: {error}"))
    })?;

    let root = document
        .as_array()
        .and_then(|entries| entries.first())
        .and_then(|entry| entry.get("Plan"))
        .ok_or_else(|| DbError::query_failed("EXPLAIN JSON has no `Plan` entry"))?;

    Ok(QueryPlan::new(
        vec![parse_node(root)?],
        raw,
        PlanFormat::Json,
    ))
}

fn parse_node(value: &Json) -> Result<PlanNode, DbError> {
    let object = value
        .as_object()
        .ok_or_else(|| DbError::query_failed("EXPLAIN JSON plan node is not an object"))?;

    let operation = object
        .get("Node Type")
        .and_then(Json::as_str)
        .ok_or_else(|| DbError::query_failed("EXPLAIN JSON plan node has no `Node Type`"))?;

    let mut node = PlanNode::new(operation);
    node.relation = object
        .get("Relation Name")
        .and_then(Json::as_str)
        .map(str::to_string);
    node.index = object
        .get("Index Name")
        .and_then(Json::as_str)
        .map(str::to_string);
    node.estimated_startup_cost = object.get("Startup Cost").and_then(Json::as_f64);
    node.estimated_total_cost = object.get("Total Cost").and_then(Json::as_f64);
    node.estimated_rows = object.get("Plan Rows").and_then(Json::as_f64);

    node.details = object
        .iter()
        .filter(|(key, _)| !MAPPED_KEYS.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), detail_text(value)))
        .collect();

    if let Some(children) = object.get("Plans").and_then(Json::as_array) {
        node.children = children
            .iter()
            .map(parse_node)
            .collect::<Result<Vec<_>, _>>()?;
    }

    Ok(node)
}

fn detail_text(value: &Json) -> String {
    match value {
        Json::String(text) => text.clone(),
        Json::Array(items) => items.iter().map(detail_text).collect::<Vec<_>>().join(", "),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests;
