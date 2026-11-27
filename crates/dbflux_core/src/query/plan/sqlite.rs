//! Parser for SQLite's `EXPLAIN QUERY PLAN`, shared by every driver that
//! speaks that dialect (SQLite and Turso).

use std::collections::HashSet;

use crate::{DbError, PlanCondition, PlanFormat, PlanNode, QueryPlan, QueryResult, Value};

/// Statements SQLite can plan without side effects. Anything else, a `PRAGMA`
/// above all, may act while the plan statement is prepared, because `EXPLAIN`
/// does not stop that.
const PLANNABLE_KEYWORDS: [&str; 7] = [
    "SELECT", "WITH", "VALUES", "INSERT", "REPLACE", "UPDATE", "DELETE",
];

/// Returns the one statement a SQLite-dialect plan covers: the shared
/// single-statement guard, plus a refusal of statements that are not plain
/// queries or writes.
pub fn sqlite_plan_statement(query: &str) -> Result<String, DbError> {
    let statement = crate::single_plan_statement(query)?;
    let first_word = crate::strip_leading_comments(&statement)
        .split(|character: char| !character.is_ascii_alphabetic())
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();

    if PLANNABLE_KEYWORDS.contains(&first_word.as_str()) {
        return Ok(statement);
    }

    Err(DbError::query_failed(
        "A query plan covers SELECT, INSERT, UPDATE, DELETE, REPLACE, VALUES and WITH statements; any other statement can change the session while its plan is prepared, so it is not planned.",
    ))
}

struct PlanRow {
    id: i64,
    parent: i64,
    detail: String,
}

/// Builds a plan from the `(id, parent, notused, detail)` rows of
/// `EXPLAIN QUERY PLAN`. The engine reports no costs or row estimates.
pub fn parse_sqlite_query_plan(result: &QueryResult) -> Result<QueryPlan, DbError> {
    let rows = result
        .rows
        .iter()
        .map(|row| plan_row(row))
        .collect::<Result<Vec<_>, _>>()?;

    if rows.is_empty() {
        // SQLite reports no steps for some statements, such as a DELETE
        // without WHERE, which it runs as a table truncate.
        let mut plan = QueryPlan::new(Vec::new(), String::new(), PlanFormat::Text);
        plan.warnings
            .push("The engine reported no plan steps for this statement.".to_string());
        return Ok(plan);
    }

    let raw = rows
        .iter()
        .map(|row| format!("{}\t{}\t{}", row.id, row.parent, row.detail))
        .collect::<Vec<_>>()
        .join("\n");

    let mut visited = HashSet::new();
    let mut roots = Vec::new();

    for row in &rows {
        let is_root = row.parent == row.id || !rows.iter().any(|other| other.id == row.parent);

        if is_root && visited.insert(row.id) {
            roots.push(build_node(row, &rows, &mut visited));
        }
    }

    Ok(QueryPlan::new(roots, raw, PlanFormat::Text))
}

fn plan_row(row: &[Value]) -> Result<PlanRow, DbError> {
    let malformed = || DbError::query_failed("EXPLAIN QUERY PLAN returned an unexpected row");

    match row {
        [Value::Int(id), Value::Int(parent), _, Value::Text(detail)] => Ok(PlanRow {
            id: *id,
            parent: *parent,
            detail: detail.clone(),
        }),
        _ => Err(malformed()),
    }
}

fn build_node(row: &PlanRow, rows: &[PlanRow], visited: &mut HashSet<i64>) -> PlanNode {
    let mut node = parse_detail(&row.detail);

    for child in rows.iter().filter(|child| child.parent == row.id) {
        if visited.insert(child.id) {
            node.children.push(build_node(child, rows, visited));
        }
    }

    node
}

fn parse_detail(detail: &str) -> PlanNode {
    let mut node = parse_detail_fields(detail);
    node.description = describe(detail).map(str::to_string);
    node
}

fn parse_detail_fields(detail: &str) -> PlanNode {
    let (operation, rest) = match detail.split_once(' ') {
        Some((verb @ ("SCAN" | "SEARCH"), rest)) => (verb, rest),
        _ => return PlanNode::new(detail),
    };

    let rest = rest.strip_prefix("TABLE ").unwrap_or(rest);
    let (rest, left_join) = match rest.strip_suffix(" LEFT-JOIN") {
        Some(rest) => (rest, true),
        None => (rest, false),
    };
    let (rest, virtual_index) = match rest.split_once(" VIRTUAL TABLE INDEX ") {
        Some((target, index)) => (target, Some(index)),
        None => (rest, None),
    };
    let (target, using) = match rest.split_once(" USING ") {
        Some((target, using)) => (target, Some(using)),
        None => (rest, None),
    };
    let (relation, alias) = match target.split_once(" AS ") {
        Some((relation, alias)) => (relation, Some(alias)),
        None => (target, None),
    };

    if relation.starts_with("SUBQUERY") || relation.starts_with("CONSTANT ROW") {
        return PlanNode::new(detail);
    }

    let mut node = PlanNode::new(operation);
    node.relation = Some(relation.to_string());

    if let Some(alias) = alias {
        node.details.push(("Alias".to_string(), alias.to_string()));
    }

    if let Some(index) = virtual_index {
        node.details
            .push(("Virtual table index".to_string(), index.to_string()));
    }

    if left_join {
        node.details
            .push(("Join".to_string(), "LEFT-JOIN".to_string()));
    }

    if let Some(using) = using {
        apply_using(&mut node, using);
    }

    node
}

fn apply_using(node: &mut PlanNode, using: &str) {
    let (access, condition) = match using.split_once(" (") {
        Some((access, condition)) => (access, Some(format!("({condition}"))),
        None => (using, None),
    };

    let mut access = access;
    if let Some(rest) = access.strip_prefix("AUTOMATIC ") {
        node.details
            .push(("Automatic".to_string(), "true".to_string()));
        access = rest;
    }
    if let Some(rest) = access.strip_prefix("COVERING ") {
        node.details
            .push(("Covering".to_string(), "true".to_string()));
        access = rest;
    }

    match access.strip_prefix("INDEX") {
        Some(name) => {
            let name = name.trim();
            if !name.is_empty() {
                node.index = Some(name.to_string());
            }
        }
        None => node.details.push(("Key".to_string(), access.to_string())),
    }

    if let Some(condition) = condition {
        node.key_condition = Some(PlanCondition {
            label: "Condition".to_string(),
            value: condition.clone(),
        });
        node.details.push(("Condition".to_string(), condition));
    }
}

/// Glossary of `EXPLAIN QUERY PLAN` steps, matched on whole leading words so
/// `SCAN` does not capture `SCAN SUBQUERY 1`. More specific entries come first.
const GLOSSARY: &[(&str, &str)] = &[
    (
        "SCAN CONSTANT ROW",
        "Produces a single row without reading any table.",
    ),
    ("SCAN SUBQUERY", "Reads the rows a subquery produced."),
    ("SCAN", "Reads every row of the table."),
    (
        "SEARCH",
        "Looks rows up through an index instead of scanning the whole table.",
    ),
    (
        "USE TEMP B-TREE",
        "Builds a temporary index to group or order the rows.",
    ),
    (
        "CO-ROUTINE",
        "Runs a subquery step by step, feeding its rows to the outer query as they are produced.",
    ),
    (
        "MATERIALIZE",
        "Computes a subquery once and stores its rows in a temporary table.",
    ),
    (
        "COMPOUND QUERY",
        "Combines the results of several SELECT statements.",
    ),
    (
        "LEFT-MOST SUBQUERY",
        "Runs the first SELECT of a compound query.",
    ),
    (
        "UNION ALL",
        "Appends the rows of the next SELECT of a compound query.",
    ),
    (
        "UNION",
        "Combines the rows of the SELECT statements of a compound query.",
    ),
    (
        "MERGE",
        "Merges the sorted results of the SELECT statements of a compound query.",
    ),
    (
        "MULTI-INDEX OR",
        "Looks rows up through one index for each OR term and combines the results.",
    ),
    (
        "INDEX",
        "Looks rows up through one index of a multi-index search.",
    ),
    (
        "LIST SUBQUERY",
        "Computes the list of values an IN clause compares against.",
    ),
    (
        "CORRELATED SCALAR SUBQUERY",
        "Runs a subquery once for each row of the outer query to get a single value.",
    ),
    (
        "SCALAR SUBQUERY",
        "Runs a subquery once to get a single value.",
    ),
];

fn describe(detail: &str) -> Option<&'static str> {
    GLOSSARY
        .iter()
        .find(|(prefix, _)| {
            detail
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with(' '))
        })
        .map(|(_, text)| *text)
}
