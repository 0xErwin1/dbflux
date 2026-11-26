//! Structured query plan for MySQL and MariaDB, built from `EXPLAIN FORMAT=JSON`.
//!
//! Both engines nest the same kind of containers (`query_block`,
//! `nested_loop`, `table`, operation wrappers) but name costs and row
//! estimates differently, so the walker maps either spelling and keeps every
//! other scalar as a detail.

use dbflux_core::{DbError, PlanFormat, PlanNode, QueryPlan};
use serde_json::{Map, Value as Json};

/// Keys of a `table` object mapped to `PlanNode` fields.
const TABLE_KEYS: [&str; 5] = [
    "table_name",
    "access_type",
    "key",
    "rows_examined_per_scan",
    "rows",
];

/// Builds the plan statement for `query` through the shared single-statement
/// guard; `ANALYZE` is deliberately never added.
pub(crate) fn explain_statement(query: &str) -> Result<String, DbError> {
    Ok(format!(
        "EXPLAIN FORMAT=JSON {}",
        dbflux_core::single_plan_statement(query)?
    ))
}

pub(crate) fn parse_explain_json(raw: &str) -> Result<QueryPlan, DbError> {
    let document: Json = serde_json::from_str(raw).map_err(|error| {
        DbError::query_failed(format!("EXPLAIN returned invalid JSON: {error}"))
    })?;

    let block = document
        .get("query_block")
        .ok_or_else(|| DbError::query_failed("EXPLAIN JSON has no `query_block` entry"))?;

    let root = container_node("query_block", block)?;

    Ok(QueryPlan::new(vec![root], raw, PlanFormat::Json))
}

fn container_node(key: &str, value: &Json) -> Result<PlanNode, DbError> {
    let object = value
        .as_object()
        .ok_or_else(|| DbError::query_failed(format!("EXPLAIN JSON `{key}` is not an object")))?;

    let is_table = key == "table";
    let mut node = PlanNode::new(if is_table {
        table_operation(object)
    } else {
        humanize(key)
    });

    if is_table {
        node.relation = text(object.get("table_name"));
        node.index = text(object.get("key"));
        node.estimated_rows =
            number(object.get("rows_examined_per_scan")).or_else(|| number(object.get("rows")));
    }

    let cost_info = object.get("cost_info").and_then(Json::as_object);
    node.estimated_total_cost = cost_info
        .and_then(|costs| number(costs.get("prefix_cost")).or(number(costs.get("query_cost"))))
        .or_else(|| number(object.get("cost")));

    if let Some(costs) = cost_info {
        node.details.extend(
            costs
                .iter()
                .filter(|(name, _)| !matches!(name.as_str(), "prefix_cost" | "query_cost"))
                .map(|(name, value)| (name.clone(), detail_text(value))),
        );
    }

    for (name, entry) in object {
        if name == "cost_info" || name == "cost" {
            continue;
        }

        if is_table && TABLE_KEYS.contains(&name.as_str()) {
            continue;
        }

        if is_structure(entry) {
            node.children.extend(child_nodes(name, entry)?);
        } else {
            node.details.push((name.clone(), detail_text(entry)));
        }
    }

    Ok(node)
}

fn table_operation(object: &Map<String, Json>) -> String {
    text(object.get("access_type")).unwrap_or_else(|| "Table".to_string())
}

/// An object, or a non-empty array of objects, is a nested plan structure;
/// everything else is a plain attribute.
fn is_structure(value: &Json) -> bool {
    match value {
        Json::Object(_) => true,
        Json::Array(items) => !items.is_empty() && items.iter().all(Json::is_object),
        _ => false,
    }
}

fn child_nodes(key: &str, value: &Json) -> Result<Vec<PlanNode>, DbError> {
    if key == "nested_loop" {
        return nested_loop(value);
    }

    match value {
        Json::Array(items) => items.iter().map(|item| container_node(key, item)).collect(),
        other => Ok(vec![container_node(key, other)?]),
    }
}

/// A `nested_loop` is a list of single-key wrappers (`{"table": ...}`); one
/// entry needs no wrapper node.
fn nested_loop(value: &Json) -> Result<Vec<PlanNode>, DbError> {
    let mut nodes = Vec::new();

    for item in value.as_array().into_iter().flatten() {
        for (name, entry) in item.as_object().into_iter().flatten() {
            if is_structure(entry) {
                nodes.extend(child_nodes(name, entry)?);
            }
        }
    }

    if nodes.len() == 1 {
        return Ok(nodes);
    }

    let mut wrapper = PlanNode::new("Nested loop");
    wrapper.children = nodes;
    Ok(vec![wrapper])
}

fn humanize(key: &str) -> String {
    let spaced = key.replace('_', " ");
    let mut characters = spaced.chars();

    match characters.next() {
        Some(first) => first.to_uppercase().chain(characters).collect(),
        None => spaced,
    }
}

fn text(value: Option<&Json>) -> Option<String> {
    value.and_then(Json::as_str).map(str::to_string)
}

/// MySQL reports costs and filters as strings, MariaDB as numbers.
fn number(value: Option<&Json>) -> Option<f64> {
    match value? {
        Json::Number(number) => number.as_f64(),
        Json::String(text) => text.parse().ok(),
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
