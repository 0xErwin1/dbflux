//! Structured query plan for MySQL and MariaDB, built from `EXPLAIN FORMAT=JSON`.
//!
//! Both engines nest the same kind of containers (`query_block`,
//! `nested_loop`, `table`, operation wrappers) but name costs and row
//! estimates differently, so the walker maps either spelling and keeps every
//! other scalar as a detail.

use dbflux_core::{DbError, PlanCondition, PlanFormat, PlanNode, QueryPlan};
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

    node.key_condition = key_condition(object, is_table);
    node.description = describe(&node.operation).map(str::to_string);

    let cost_info = object.get("cost_info").and_then(Json::as_object);
    node.estimated_total_cost = cost_info
        .and_then(|costs| number(costs.get("prefix_cost")).or(number(costs.get("query_cost"))))
        .or_else(|| number(object.get("cost")));

    if let Some(costs) = cost_info {
        node.details.extend(
            costs
                .iter()
                .filter(|(name, _)| name.as_str() != "query_cost")
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

/// The attached condition when the engine gives one, otherwise the key parts
/// a table access uses.
fn key_condition(object: &Map<String, Json>, is_table: bool) -> Option<PlanCondition> {
    let attached = ("attached_condition", "Attached condition");
    let key_parts = ("used_key_parts", "Used key parts");
    let candidates: &[(&str, &str)] = if is_table {
        &[attached, key_parts]
    } else {
        &[attached]
    };

    candidates.iter().find_map(|(key, label)| {
        let value = detail_text(object.get(*key)?);
        (!value.is_empty()).then(|| PlanCondition {
            label: (*label).to_string(),
            value,
        })
    })
}

/// Glossary of access types and operation containers, keyed by the operation
/// name the node carries.
const GLOSSARY: &[(&str, &str)] = &[
    ("ALL", "Reads every row of the table."),
    (
        "index",
        "Reads every entry of an index instead of the table rows.",
    ),
    (
        "range",
        "Reads the rows whose index values fall in a range.",
    ),
    (
        "ref",
        "Finds the rows whose index value equals the value it is compared with.",
    ),
    (
        "eq_ref",
        "Finds the one row that matches each row of the previous table through a unique index.",
    ),
    (
        "ref_or_null",
        "Finds the rows whose index value equals the value it is compared with, plus the rows where it is NULL.",
    ),
    (
        "const",
        "Reads the single row that matches a constant through a unique index.",
    ),
    ("system", "Reads a table that has a single row."),
    ("fulltext", "Finds the rows through a full-text index."),
    (
        "index_merge",
        "Combines the rows found through several indexes.",
    ),
    (
        "unique_subquery",
        "Looks the value up in a unique index to evaluate an IN subquery.",
    ),
    (
        "index_subquery",
        "Looks the value up in an index to evaluate an IN subquery.",
    ),
    ("Table", "Reads rows from a table."),
    (
        "Query block",
        "A SELECT of the statement, with the operations it runs.",
    ),
    (
        "Nested loop",
        "For each row from the first table, looks up the matching rows in the next.",
    ),
    ("Ordering operation", "Orders the rows by the sort key."),
    (
        "Grouping operation",
        "Groups the rows and computes the aggregates of each group.",
    ),
    ("Duplicates removal", "Removes duplicate rows."),
    ("Windowing", "Computes window functions over the rows."),
    (
        "Buffer result",
        "Stores the rows of its input in a buffer before they are used.",
    ),
    (
        "Union result",
        "Combines the rows of several SELECT statements.",
    ),
    (
        "Query specifications",
        "The SELECT statements that a union combines.",
    ),
    (
        "Materialized from subquery",
        "Computes a subquery once and stores its rows in a temporary table.",
    ),
    (
        "Read sorted file",
        "Reads the rows back from the sorted temporary file.",
    ),
    (
        "Filesort",
        "Orders the rows by the sort key, using a temporary file when they do not fit in memory.",
    ),
    (
        "Temporary table",
        "Stores the intermediate rows in a temporary table.",
    ),
];

fn describe(operation: &str) -> Option<&'static str> {
    GLOSSARY
        .iter()
        .find(|(name, _)| *name == operation)
        .map(|(_, text)| *text)
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

    // `prefix_cost` is the cost of every table up to this join position, so a
    // table's own cost is the step from the previous prefix.
    let mut previous_prefix = 0.0;

    for item in value.as_array().into_iter().flatten() {
        for (name, entry) in item.as_object().into_iter().flatten() {
            if !is_structure(entry) {
                continue;
            }

            let first_new = nodes.len();
            nodes.extend(child_nodes(name, entry)?);

            let prefix = (name == "table")
                .then(|| number(entry.get("cost_info")?.get("prefix_cost")))
                .flatten();
            if let (Some(prefix), Some(node)) = (prefix, nodes.get_mut(first_new)) {
                node.estimated_total_cost = Some(round_cost((prefix - previous_prefix).max(0.0)));
                previous_prefix = prefix;
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

fn round_cost(cost: f64) -> f64 {
    (cost * 1_000_000.0).round() / 1_000_000.0
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
