//! Structured query plan for DuckDB, built from `EXPLAIN (FORMAT JSON)`.
//!
//! The bundled engine returns one `physical_plan` row whose value is a JSON
//! array of `{name, children, extra_info}` nodes. DuckDB reports no costs, only
//! an `Estimated Cardinality` inside `extra_info`.
//!
//! `EXPLAIN (ANALYZE, FORMAT JSON)` returns one `analyzed_plan` row instead: a
//! document with an `operator` tree of `{type, timing, intermediate_rows,
//! children, extra_info}` nodes, wrapped in the operators that run the
//! explain itself, plus the planner and query timings in seconds.

use dbflux_core::{DbError, PlanCondition, PlanFormat, PlanNode, QueryPlan, QueryResult, Value};
use serde_json::Value as Json;

const CARDINALITY_KEY: &str = "Estimated Cardinality";
const TABLE_KEY: &str = "Table";

/// `extra_info` entries that can explain an operator, best first. The first
/// non-empty one wins and keeps DuckDB's own label.
const KEY_CONDITION_PRIORITY: [&str; 4] = ["Conditions", "Filters", "Order By", "Groups"];

/// A standalone `FILTER` keeps its predicate under `Expression`.
const FILTER_OPERATOR: &str = "FILTER";
const FILTER_CONDITION_KEY: &str = "Expression";

/// Builds the plan statement for `query` through the shared single-statement
/// guard; `ANALYZE` is deliberately never added.
pub(crate) fn explain_statement(query: &str) -> Result<String, DbError> {
    Ok(format!(
        "EXPLAIN (FORMAT JSON) {}",
        dbflux_core::single_plan_statement(query)?
    ))
}

/// Builds the analyzed plan statement for `query`. The statement runs, so it
/// must pass the shared read guard and the single-statement guard first; the
/// driver also runs it inside a read-only transaction.
pub(crate) fn analyze_explain_statement(query: &str) -> Result<String, DbError> {
    Ok(format!(
        "EXPLAIN (ANALYZE, FORMAT JSON) {}",
        dbflux_core::analyze_statement(&dbflux_core::QueryLanguage::Sql, query)?
    ))
}

/// Reads the JSON of the `physical_plan` (or `analyzed_plan`) row
/// (`explain_key`, `explain_value`).
pub(crate) fn plan_text_from_result(result: &QueryResult) -> Result<String, DbError> {
    let physical = result.rows.iter().find(|row| {
        matches!(row.first(), Some(Value::Text(key)) if key == "physical_plan" || key == "analyzed_plan")
    });

    match physical.and_then(|row| row.get(1)) {
        Some(Value::Text(text) | Value::Json(text)) => Ok(text.clone()),
        _ => Err(DbError::query_failed(
            "EXPLAIN returned no physical plan; check that `explain_output` is `physical_only` or `all`",
        )),
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

/// Operators DuckDB adds around the statement to run the explain itself.
const EXPLAIN_WRAPPERS: [&str; 3] = ["RESULT_COLLECTOR", "EXECUTE", "EXPLAIN_ANALYZE"];

pub(crate) fn parse_analyzed_json(raw: &str) -> Result<QueryPlan, DbError> {
    let document: Json = serde_json::from_str(raw).map_err(|error| {
        DbError::query_failed(format!("EXPLAIN ANALYZE returned invalid JSON: {error}"))
    })?;

    let operators = document
        .get("operator")
        .and_then(Json::as_array)
        .ok_or_else(|| DbError::query_failed("EXPLAIN ANALYZE JSON has no `operator` entry"))?;

    let roots = statement_operators(operators)
        .into_iter()
        .map(parse_node)
        .collect::<Result<Vec<_>, _>>()?;

    if roots.is_empty() {
        return Err(DbError::query_failed(
            "EXPLAIN ANALYZE JSON has no plan nodes",
        ));
    }

    let seconds = |section: &str, key: &str| {
        document
            .get(section)
            .and_then(|section| section.get(key))
            .and_then(Json::as_f64)
    };

    let mut plan = QueryPlan::new(roots, raw, PlanFormat::Json);
    let planning = ["planner", "optimizer", "physical_planner"]
        .iter()
        .map(|section| seconds(section, "total_time"))
        .sum::<Option<f64>>();
    plan.planning_time = planning.map(|seconds| seconds * 1000.0);
    // `query.total_time` covers the whole statement, planning included.
    plan.execution_time = seconds("query", "total_time")
        .zip(planning)
        .map(|(total, planning)| ((total - planning) * 1000.0).max(0.0));

    Ok(plan)
}

/// The operators of the explained statement, past the wrappers that run the
/// explain.
fn statement_operators(entries: &[Json]) -> Vec<&Json> {
    entries
        .iter()
        .flat_map(|entry| {
            let wrapped = entry
                .get("type")
                .and_then(Json::as_str)
                .is_some_and(|name| EXPLAIN_WRAPPERS.contains(&name));

            if wrapped {
                let children = entry.get("children").and_then(Json::as_array);
                statement_operators(children.map(Vec::as_slice).unwrap_or_default())
            } else {
                vec![entry]
            }
        })
        .collect()
}

fn parse_node(value: &Json) -> Result<PlanNode, DbError> {
    let name = value
        .get("name")
        .or_else(|| value.get("type"))
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

    if let Some(Json::Object(info)) = value.get("extra_info") {
        let filter_key = (name == FILTER_OPERATOR).then_some(FILTER_CONDITION_KEY);
        node.key_condition = KEY_CONDITION_PRIORITY
            .iter()
            .copied()
            .chain(filter_key)
            .find_map(|label| {
                let text = detail_text(info.get(label)?);
                (!text.is_empty()).then(|| PlanCondition {
                    label: label.to_string(),
                    value: text,
                })
            });
    }
    node.description = describe(name).map(str::to_string);

    if let Some(children) = value.get("children").and_then(Json::as_array) {
        node.children = children
            .iter()
            .map(parse_node)
            .collect::<Result<Vec<_>, _>>()?;
    }

    apply_actuals(&mut node, value);

    Ok(node)
}

/// Maps an analyzed operator. DuckDB's `timing` is the seconds spent in the
/// operator alone, so the node keeps an inclusive total in milliseconds (its
/// own time plus its children's), which `actual_self_time` turns back into the
/// operator's own time. It is CPU time summed over threads, so a parallel
/// operator can report more than the wall clock.
fn apply_actuals(node: &mut PlanNode, value: &Json) {
    let Some(timing) = value.get("timing").and_then(Json::as_f64) else {
        return;
    };

    let children = node
        .children
        .iter()
        .map(|child| child.actual_total_time.unwrap_or_default())
        .sum::<f64>();

    node.actual_total_time = Some(timing * 1000.0 + children);
    node.actual_loops = Some(1);
    node.actual_rows = value.get("intermediate_rows").and_then(Json::as_f64);

    for key in [
        "rows_scanned",
        "row_groups_scanned",
        "total_row_groups_to_scan",
    ] {
        if let Some(counter) = value.get(key) {
            node.details.push((key.to_string(), detail_text(counter)));
        }
    }
}

/// Glossary of DuckDB's physical operator names.
const GLOSSARY: &[(&str, &str)] = &[
    (
        "SEQ_SCAN",
        "Reads every row of the table, skipping the parts that cannot match the filters.",
    ),
    (
        "TABLE_SCAN",
        "Reads the rows of a table or table function, applying any pushed-down filters.",
    ),
    ("FILTER", "Keeps only the rows that satisfy the condition."),
    ("PROJECTION", "Computes the output columns of each row."),
    (
        "HASH_JOIN",
        "Builds a hash table from one input and probes it with each row of the other to find matches.",
    ),
    (
        "NESTED_LOOP_JOIN",
        "For each row from the first input, compares it with the rows of the second.",
    ),
    (
        "BLOCKWISE_NL_JOIN",
        "Compares blocks of rows of one input with the other, for joins without an equality condition.",
    ),
    (
        "PIECEWISE_MERGE_JOIN",
        "Sorts both inputs in pieces and merges them to find the matching rows.",
    ),
    ("IE_JOIN", "Joins the inputs on inequality conditions."),
    (
        "ASOF_JOIN",
        "Pairs each row with the closest matching row of the other input.",
    ),
    (
        "POSITIONAL_JOIN",
        "Pairs the rows of both inputs by their position.",
    ),
    (
        "CROSS_PRODUCT",
        "Pairs every row of the first input with every row of the second.",
    ),
    (
        "HASH_GROUP_BY",
        "Groups the rows with a hash table and computes the aggregates of each group.",
    ),
    (
        "PERFECT_HASH_GROUP_BY",
        "Groups the rows with a direct-lookup table and computes the aggregates of each group.",
    ),
    (
        "UNGROUPED_AGGREGATE",
        "Computes the aggregates over all the rows of its input.",
    ),
    (
        "SIMPLE_AGGREGATE",
        "Computes the aggregates over all the rows of its input.",
    ),
    ("ORDER_BY", "Orders the rows by the sort key."),
    (
        "TOP_N",
        "Keeps only the first rows of the sort order, without sorting the whole input.",
    ),
    (
        "LIMIT",
        "Stops after the number of rows the query asks for.",
    ),
    (
        "STREAMING_LIMIT",
        "Stops after the number of rows the query asks for.",
    ),
    ("DISTINCT", "Removes duplicate rows."),
    (
        "WINDOW",
        "Computes window functions over the rows of its input.",
    ),
    (
        "STREAMING_WINDOW",
        "Computes window functions as the rows stream through.",
    ),
    (
        "UNION",
        "Appends the rows of several inputs one after another.",
    ),
    (
        "CTE",
        "Computes a common table expression once so it can be read several times.",
    ),
    ("CTE_SCAN", "Reads the rows of a common table expression."),
    (
        "RECURSIVE_CTE",
        "Repeats a query over its own previous results until no new rows appear.",
    ),
    (
        "DUMMY_SCAN",
        "Produces a single row without reading any table.",
    ),
    ("EMPTY_RESULT", "Returns no rows."),
    (
        "COLUMN_DATA_SCAN",
        "Reads rows held in memory, such as a VALUES list.",
    ),
    ("UNNEST", "Expands a list into one row per element."),
    ("INSERT", "Adds the rows its input returns to the table."),
    ("UPDATE", "Changes the rows its input returns."),
    (
        "DELETE",
        "Removes the rows its input returns from the table.",
    ),
];

fn describe(name: &str) -> Option<&'static str> {
    GLOSSARY
        .iter()
        .find(|(operator, _)| *operator == name)
        .map(|(_, text)| *text)
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
