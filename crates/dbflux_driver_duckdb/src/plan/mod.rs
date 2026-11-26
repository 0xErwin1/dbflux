//! Structured query plan for DuckDB, built from `EXPLAIN (FORMAT JSON)`.
//!
//! The bundled engine returns one `physical_plan` row whose value is a JSON
//! array of `{name, children, extra_info}` nodes. DuckDB reports no costs, only
//! an `Estimated Cardinality` inside `extra_info`.

use dbflux_core::{DbError, PlanFormat, PlanNode, QueryPlan, QueryResult, Value};
use serde_json::Value as Json;

const CARDINALITY_KEY: &str = "Estimated Cardinality";
const TABLE_KEY: &str = "Table";

/// Builds the plan statement for `query` through the shared single-statement
/// guard; `ANALYZE` is deliberately never added.
pub(crate) fn explain_statement(query: &str) -> Result<String, DbError> {
    Ok(format!(
        "EXPLAIN (FORMAT JSON) {}",
        dbflux_core::single_plan_statement(query)?
    ))
}

/// Reads the JSON of the `physical_plan` row (`explain_key`, `explain_value`).
pub(crate) fn plan_text_from_result(result: &QueryResult) -> Result<String, DbError> {
    let physical = result
        .rows
        .iter()
        .find(|row| matches!(row.first(), Some(Value::Text(key)) if key == "physical_plan"));

    match physical.or(result.rows.first()).and_then(|row| row.get(1)) {
        Some(Value::Text(text) | Value::Json(text)) => Ok(text.clone()),
        _ => Err(DbError::query_failed("EXPLAIN returned no plan")),
    }
}

pub(crate) fn parse_explain_json(raw: &str) -> Result<QueryPlan, DbError> {
    let document: Json = serde_json::from_str(raw).map_err(|error| {
        DbError::query_failed(format!("EXPLAIN returned invalid JSON: {error}"))
    })?;

    let entries = document
        .as_array()
        .filter(|entries| !entries.is_empty())
        .ok_or_else(|| DbError::query_failed("EXPLAIN JSON has no plan nodes"))?;

    let roots = entries
        .iter()
        .map(parse_node)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(QueryPlan::new(roots, raw, PlanFormat::Json))
}

fn parse_node(value: &Json) -> Result<PlanNode, DbError> {
    let name = value
        .get("name")
        .and_then(Json::as_str)
        .ok_or_else(|| DbError::query_failed("EXPLAIN JSON plan node has no `name`"))?;

    let mut node = PlanNode::new(name);

    match value.get("extra_info") {
        Some(Json::Object(info)) => {
            for (key, entry) in info {
                match key.as_str() {
                    CARDINALITY_KEY => node.estimated_rows = cardinality(entry),
                    TABLE_KEY => node.relation = entry.as_str().map(str::to_string),
                    _ => node.details.push((key.clone(), detail_text(entry))),
                }
            }
        }
        Some(Json::String(text)) if !text.is_empty() => {
            node.details.push(("extra_info".to_string(), text.clone()));
        }
        _ => {}
    }

    if let Some(children) = value.get("children").and_then(Json::as_array) {
        node.children = children
            .iter()
            .map(parse_node)
            .collect::<Result<Vec<_>, _>>()?;
    }

    Ok(node)
}

/// DuckDB renders the estimate as text, with a leading `~` when approximate.
fn cardinality(value: &Json) -> Option<f64> {
    match value {
        Json::Number(number) => number.as_f64(),
        Json::String(text) => text.trim_start_matches('~').trim().parse().ok(),
        _ => None,
    }
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
