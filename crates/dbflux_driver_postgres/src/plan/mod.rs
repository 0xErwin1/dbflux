//! Structured query plan for PostgreSQL, built from `EXPLAIN (FORMAT JSON)`.

use dbflux_core::{DbError, PlanCondition, PlanFormat, PlanNode, QueryPlan, QueryResult};
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

/// Conditions that can explain a node, best first. The first one present wins
/// and keeps the engine's own label.
const KEY_CONDITION_PRIORITY: [&str; 8] = [
    "Index Cond",
    "Recheck Cond",
    "Hash Cond",
    "Merge Cond",
    "Join Filter",
    "Filter",
    "Sort Key",
    "Group Key",
];

/// Builds the plan statement for `query` through the shared single-statement
/// guard; `ANALYZE` is deliberately never added.
pub(crate) fn explain_statement(query: &str) -> Result<String, DbError> {
    Ok(format!(
        "EXPLAIN (FORMAT JSON) {}",
        dbflux_core::single_plan_statement(query)?
    ))
}

/// Extracts the JSON document `EXPLAIN (FORMAT JSON)` returned.
pub(crate) fn raw_plan_from_result(result: &QueryResult) -> Result<String, DbError> {
    dbflux_core::plan_text_from_result(result)
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

    node.key_condition = KEY_CONDITION_PRIORITY.iter().find_map(|label| {
        object.get(*label).map(|value| PlanCondition {
            label: (*label).to_string(),
            value: detail_text(value),
        })
    });
    node.description = describe(operation, object).map(str::to_string);

    if let Some(children) = object.get("Plans").and_then(Json::as_array) {
        node.children = children
            .iter()
            .map(parse_node)
            .collect::<Result<Vec<_>, _>>()?;
    }

    Ok(node)
}

/// Plain-language glossary of the node types users meet. Aggregates and
/// `ModifyTable` are described by their strategy / operation, because the JSON
/// reports the same node type for all of them.
fn describe(node_type: &str, object: &serde_json::Map<String, Json>) -> Option<&'static str> {
    let attribute = |key: &str| object.get(key).and_then(Json::as_str);

    let key = match (node_type, attribute("Strategy"), attribute("Operation")) {
        ("Aggregate", Some("Hashed"), _) => "HashAggregate",
        ("Aggregate", Some("Sorted"), _) => "GroupAggregate",
        ("ModifyTable", _, Some(operation @ ("Insert" | "Update" | "Delete"))) => operation,
        _ => node_type,
    };

    GLOSSARY
        .iter()
        .find(|(name, _)| *name == key)
        .map(|(_, text)| *text)
}

const GLOSSARY: &[(&str, &str)] = &[
    (
        "Seq Scan",
        "Reads every row of the table and keeps the ones that match the filter.",
    ),
    (
        "Index Scan",
        "Finds the matching rows through an index instead of reading the whole table.",
    ),
    (
        "Index Only Scan",
        "Reads the matching values straight from an index without visiting the table.",
    ),
    (
        "Bitmap Heap Scan",
        "Fetches the table rows that a bitmap marked as matching.",
    ),
    (
        "Bitmap Index Scan",
        "Builds a bitmap of the matching rows from an index.",
    ),
    (
        "Nested Loop",
        "For each row from the first input, looks up the matching rows in the second.",
    ),
    (
        "Hash Join",
        "Builds a hash table from one input and probes it with each row of the other to find matches.",
    ),
    (
        "Merge Join",
        "Walks two sorted inputs together and pairs the rows that match.",
    ),
    (
        "Hash",
        "Builds the in-memory hash table that a Hash Join probes.",
    ),
    ("Sort", "Orders the rows by the sort key."),
    (
        "Incremental Sort",
        "Orders the rows by the sort key, reusing the order the input already has on its first keys.",
    ),
    (
        "Aggregate",
        "Computes the aggregates over all the rows of its input.",
    ),
    (
        "HashAggregate",
        "Groups the rows with a hash table and computes the aggregates of each group.",
    ),
    (
        "GroupAggregate",
        "Computes the aggregates of each group from rows that arrive already grouped.",
    ),
    (
        "Limit",
        "Stops after the number of rows the query asks for.",
    ),
    (
        "Materialize",
        "Stores the rows of its input so they can be read again without recomputing them.",
    ),
    (
        "Memoize",
        "Caches the lookup result for each parameter value, so a repeated value is not looked up again.",
    ),
    ("Gather", "Collects the rows that parallel workers produce."),
    (
        "Gather Merge",
        "Collects the sorted rows of parallel workers and keeps their order.",
    ),
    (
        "Append",
        "Concatenates the rows of several inputs one after another.",
    ),
    ("Subquery Scan", "Reads the rows of a subquery."),
    ("CTE Scan", "Reads the rows of a common table expression."),
    ("Function Scan", "Reads the rows a function returns."),
    ("Values Scan", "Reads the rows of a VALUES list."),
    (
        "Result",
        "Returns a computed value, or passes its input through without reading a table.",
    ),
    (
        "Unique",
        "Removes consecutive duplicate rows from a sorted input.",
    ),
    (
        "WindowAgg",
        "Computes window functions over the rows of its sorted input.",
    ),
    (
        "ModifyTable",
        "Inserts, updates or deletes the rows its input returns.",
    ),
    ("Insert", "Adds the rows its input returns to the table."),
    ("Update", "Changes the rows its input returns."),
    (
        "Delete",
        "Removes the rows its input returns from the table.",
    ),
    (
        "LockRows",
        "Locks the rows its input returns, as a locking clause such as FOR UPDATE asks.",
    ),
];

fn detail_text(value: &Json) -> String {
    match value {
        Json::String(text) => text.clone(),
        Json::Array(items) => items.iter().map(detail_text).collect::<Vec<_>>().join(", "),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests;
