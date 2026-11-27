//! Structured query plan for MySQL and MariaDB, built from `EXPLAIN FORMAT=JSON`.
//!
//! Analyzed plans use each flavour's own syntax: MariaDB's `ANALYZE
//! FORMAT=JSON` is the same JSON with `r_*` actuals, MySQL 8.0.18+ only
//! reports actuals in the `EXPLAIN ANALYZE` TREE text, parsed by [`tree`].
//!
//! Both engines nest the same kind of containers (`query_block`,
//! `nested_loop`, `table`, operation wrappers) but name costs and row
//! estimates differently, so the walker maps either spelling and keeps every
//! other scalar as a detail.

use dbflux_core::{DbError, DbKind, PlanCondition, PlanFormat, PlanNode, QueryPlan};
use serde_json::{Map, Value as Json};

mod tree;
pub(crate) use tree::parse_analyze_tree;

/// Keys that carry MariaDB's actuals; they map to `PlanNode` fields.
const ACTUAL_KEYS: [&str; 6] = [
    "r_loops",
    "r_rows",
    "r_output_rows",
    "r_total_time_ms",
    "r_table_time_ms",
    "r_other_time_ms",
];

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

/// Builds the analyzed plan statement for `query` in the syntax of `kind`.
/// The statement runs, so it must pass the shared read guard and the
/// single-statement guard first; the driver also runs it inside a read-only
/// transaction.
///
/// MySQL names the TREE format explicitly from 8.0.21, so a session whose
/// `explain_format` is JSON cannot turn the analyzed plan into an error;
/// 8.0.18 to 8.0.20 reject the clause and only ever produce TREE.
pub(crate) fn analyze_statement(
    kind: DbKind,
    version: Option<&str>,
    query: &str,
) -> Result<String, DbError> {
    let statement = dbflux_core::analyze_statement(&dbflux_core::QueryLanguage::Sql, query)?;

    Ok(match kind {
        DbKind::MariaDB => format!("ANALYZE FORMAT=JSON {statement}"),
        _ if version
            .and_then(parse_version)
            .is_some_and(|v| v >= (8, 0, 21)) =>
        {
            format!("EXPLAIN ANALYZE FORMAT=TREE {statement}")
        }
        _ => format!("EXPLAIN ANALYZE {statement}"),
    })
}

/// `major.minor.patch` of a server version string; a missing patch is zero.
/// MariaDB may report a replication-compatibility prefix: `5.5.5-10.11.6-MariaDB`.
fn parse_version(version: &str) -> Option<(u32, u32, u32)> {
    let numbers = version.strip_prefix("5.5.5-").unwrap_or(version);
    let mut parts = numbers
        .split(|character: char| !character.is_ascii_digit())
        .map(|part| part.parse::<u32>().ok());
    let (Some(Some(major)), Some(Some(minor))) = (parts.next(), parts.next()) else {
        return None;
    };
    let patch = parts.next().flatten().unwrap_or(0);

    Some((major, minor, patch))
}

/// Refuses an analyzed plan on a server whose version has no analyze syntax,
/// before the statement is sent: MySQL gained `EXPLAIN ANALYZE` in 8.0.18 and
/// MariaDB has `ANALYZE FORMAT=JSON` since 10.1. A version string that cannot
/// be read does not block the request; the server answers it.
pub(crate) fn analyze_version_gate(kind: DbKind, version: &str) -> Result<(), DbError> {
    let Some((major, minor, patch)) = parse_version(version) else {
        return Ok(());
    };

    if kind == DbKind::MariaDB {
        if (major, minor) < (10, 1) {
            return Err(DbError::NotSupported(
                "Analyzed plans need MariaDB 10.1 or later".to_string(),
            ));
        }
    } else if (major, minor, patch) < (8, 0, 18) {
        return Err(DbError::NotSupported(
            "Analyzed plans need MySQL 8.0.18 or later".to_string(),
        ));
    }

    Ok(())
}

pub(crate) fn parse_explain_json(raw: &str) -> Result<QueryPlan, DbError> {
    let document: Json = serde_json::from_str(raw).map_err(|error| {
        DbError::query_failed(format!("EXPLAIN returned invalid JSON: {error}"))
    })?;

    let block = document
        .get("query_block")
        .ok_or_else(|| DbError::query_failed("EXPLAIN JSON has no `query_block` entry"))?;

    let root = container_node("query_block", block)?;

    let execution_time = root.actual_total_time;
    let mut plan = QueryPlan::new(vec![root], raw, PlanFormat::Json);
    plan.planning_time = document
        .get("query_optimization")
        .and_then(|optimization| number(optimization.get("r_total_time_ms")));
    plan.execution_time = execution_time;

    Ok(plan)
}

/// Maps MariaDB's `r_*` keys. Its times are totals over every loop, while
/// `PlanNode` keeps them per loop, as PostgreSQL and MySQL report them.
fn apply_actuals(node: &mut PlanNode, object: &Map<String, Json>) {
    node.actual_loops = object.get("r_loops").and_then(Json::as_u64);
    node.actual_rows = number(object.get("r_rows")).or_else(|| number(object.get("r_output_rows")));

    let total = number(object.get("r_total_time_ms")).or_else(|| {
        let table = number(object.get("r_table_time_ms"));
        let other = number(object.get("r_other_time_ms"));
        (table.is_some() || other.is_some())
            .then(|| table.unwrap_or_default() + other.unwrap_or_default())
    });

    node.actual_total_time = total.map(|total| match node.actual_loops {
        Some(0) => 0.0,
        Some(loops) => total / loops as f64,
        None => total,
    });
}

/// MariaDB counts `r_rows` (and estimates `rows`) before the attached
/// condition; `r_filtered` and `filtered` are the percentages that pass it.
/// `PlanNode` rows are the rows the step returns, so both are scaled by their
/// percentage, which leaves an unfiltered scan (100) unchanged.
fn apply_attached_filter(node: &mut PlanNode, object: &Map<String, Json>) {
    let Some(actual_percent) = number(object.get("r_filtered")) else {
        return;
    };

    node.actual_rows = node.actual_rows.map(|rows| rows * actual_percent / 100.0);

    if let Some(estimated_percent) = number(object.get("filtered")) {
        node.estimated_rows = node
            .estimated_rows
            .map(|rows| rows * estimated_percent / 100.0);
    }
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

    apply_actuals(&mut node, object);
    apply_attached_filter(&mut node, object);
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
        if name == "cost_info" || name == "cost" || ACTUAL_KEYS.contains(&name.as_str()) {
            continue;
        }

        if name == "r_engine_stats" {
            node.details.push((name.clone(), detail_text(entry)));
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

/// Glossary of the operation names the TREE text uses.
const TREE_GLOSSARY: &[(&str, &str)] = &[
    ("Table scan", "Reads every row of the table."),
    (
        "Index scan",
        "Reads every entry of an index instead of the table rows.",
    ),
    (
        "Covering index scan",
        "Reads every entry of an index that holds all the columns the query needs.",
    ),
    (
        "Index range scan",
        "Reads the rows whose index values fall in a range.",
    ),
    (
        "Index lookup",
        "Finds the rows whose index value equals the value it is compared with.",
    ),
    (
        "Single-row index lookup",
        "Finds the one row that matches through a unique index.",
    ),
    (
        "Covering index lookup",
        "Finds the rows through an index that holds all the columns the query needs.",
    ),
    ("Filter", "Keeps only the rows that satisfy the condition."),
    ("Sort", "Orders the rows by the sort key."),
    (
        "Limit",
        "Stops after the number of rows the query asks for.",
    ),
    (
        "Aggregate",
        "Computes the aggregates over all the rows of its input.",
    ),
    (
        "Group aggregate",
        "Computes the aggregates of each group from rows that arrive already grouped.",
    ),
    (
        "Aggregate using temporary table",
        "Groups the rows in a temporary table and computes the aggregates of each group.",
    ),
    (
        "Nested loop inner join",
        "For each row from the first input, looks up the matching rows in the second.",
    ),
    (
        "Nested loop left join",
        "For each row from the first input, looks up the matching rows in the second and keeps the row when there are none.",
    ),
    (
        "Inner hash join",
        "Builds a hash table from one input and probes it with each row of the other to find matches.",
    ),
    (
        "Left hash join",
        "Probes a hash table with each row of the left input and keeps the row when nothing matches.",
    ),
    (
        "Hash",
        "Builds the in-memory hash table that a hash join probes.",
    ),
    (
        "Materialize",
        "Stores the rows of its input so they can be read again without recomputing them.",
    ),
    (
        "Stream results",
        "Passes the rows through as they arrive, without storing them.",
    ),
    (
        "Temporary table",
        "Stores the intermediate rows in a temporary table.",
    ),
    (
        "Remove duplicates from input sorted",
        "Removes consecutive duplicate rows from a sorted input.",
    ),
];

fn describe_tree(operation: &str) -> Option<&'static str> {
    TREE_GLOSSARY
        .iter()
        .find(|(name, _)| *name == operation)
        .map(|(_, text)| *text)
}

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
    wrapper.description = describe("Nested loop").map(str::to_string);
    wrapper.actual_total_time = nodes
        .iter()
        .map(|node| Some(node.actual_total_time? * node.actual_loops.unwrap_or(1) as f64))
        .sum::<Option<f64>>();
    wrapper.actual_loops = wrapper.actual_total_time.map(|_| 1);
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
