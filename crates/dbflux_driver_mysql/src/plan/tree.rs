//! Parser for MySQL's `EXPLAIN ANALYZE` TREE text.
//!
//! Every node is one line: `-> <description>  (cost=C rows=R) (actual
//! time=S..T rows=N loops=L)`, indented four spaces per level. MySQL reports
//! the actual time and rows per loop, which is what `PlanNode` stores. A node
//! that did not run ends in `(never executed)` instead.

use dbflux_core::{DbError, PlanCondition, PlanFormat, PlanNode, QueryPlan};

const INDENT_WIDTH: usize = 4;

/// Markers that start the cost/actual part of a node line. The description
/// ends at the first one: two spaces then an opening parenthesis.
const METRIC_MARKERS: [&str; 3] = ["  (cost=", "  (actual time=", "  (never executed)"];

pub(crate) fn parse_analyze_tree(raw: &str) -> Result<QueryPlan, DbError> {
    let mut roots: Vec<PlanNode> = Vec::new();
    let mut open: Vec<(usize, PlanNode)> = Vec::new();

    for line in raw.lines() {
        let trimmed = line.trim_start();
        let Some(body) = trimmed.strip_prefix("-> ") else {
            if !trimmed.is_empty()
                && let Some((_, node)) = open.last_mut()
            {
                node.details.push(("Note".to_string(), trimmed.to_string()));
            }
            continue;
        };

        let depth = (line.len() - trimmed.len()) / INDENT_WIDTH;
        close_deeper(&mut open, &mut roots, depth);
        open.push((depth, parse_line(body)));
    }

    close_deeper(&mut open, &mut roots, 0);

    if roots.is_empty() {
        return Err(DbError::query_failed(
            "EXPLAIN ANALYZE returned no plan tree",
        ));
    }

    let mut plan = QueryPlan::new(roots, raw, PlanFormat::Text);
    plan.execution_time = plan
        .roots
        .iter()
        .map(|root| Some(root.actual_total_time? * root.actual_loops.unwrap_or(1) as f64))
        .sum::<Option<f64>>();

    Ok(plan)
}

/// Attaches every open node at `depth` or deeper to its parent, or to the
/// roots when it has none.
fn close_deeper(open: &mut Vec<(usize, PlanNode)>, roots: &mut Vec<PlanNode>, depth: usize) {
    while open
        .last()
        .is_some_and(|(open_depth, _)| *open_depth >= depth)
    {
        let Some((_, node)) = open.pop() else {
            return;
        };

        match open.last_mut() {
            Some((_, parent)) => parent.children.push(node),
            None => roots.push(node),
        }
    }
}

fn parse_line(body: &str) -> PlanNode {
    let split = METRIC_MARKERS
        .iter()
        .filter_map(|marker| body.find(marker))
        .min();

    let (description, metrics) = match split {
        Some(index) => {
            let (description, metrics) = body.split_at(index);
            (description.trim(), metrics.trim())
        }
        None => (body.trim(), ""),
    };

    let mut node = parse_description(description);
    node.description = super::describe_tree(&node.operation).map(str::to_string);
    apply_metrics(&mut node, metrics);
    node
}

fn parse_description(description: &str) -> PlanNode {
    let split = [": ", " on "]
        .iter()
        .filter_map(|marker| description.find(marker).map(|index| (index, *marker)))
        .min_by_key(|(index, _)| *index);

    match split {
        Some((index, ": ")) => {
            let (operation, rest) = description.split_at(index);
            argument_node(operation, rest.trim_start_matches(": "))
        }
        Some((index, _)) => {
            let (operation, rest) = description.split_at(index);
            access_node(operation, rest.trim_start_matches(" on "))
        }
        None => join_or_plain_node(description),
    }
}

/// `Filter: (cond)`, `Sort: key`, `Limit: 3 row(s)`: the text after the colon.
fn argument_node(operation: &str, argument: &str) -> PlanNode {
    let mut node = PlanNode::new(operation);

    match operation {
        "Filter" => node.key_condition = Some(condition("Filter", argument)),
        "Sort" => node.key_condition = Some(condition("Sort Key", argument)),
        _ => node
            .details
            .push(("Detail".to_string(), argument.to_string())),
    }

    node
}

/// `Table scan on t`, `Index lookup on t using idx (a=b)`, `Index range scan
/// on t using idx over (...), with index condition: (...)`.
fn access_node(operation: &str, target: &str) -> PlanNode {
    let mut node = PlanNode::new(operation);

    let (relation, after_relation) = target.split_once(' ').unwrap_or((target, ""));
    node.relation = Some(relation.to_string());

    let mut rest = after_relation.trim();

    if let Some(after_using) = rest.strip_prefix("using ") {
        let (index, after_index) = after_using.split_once(' ').unwrap_or((after_using, ""));
        node.index = Some(index.to_string());
        rest = after_index.trim();
    }

    let (scan, index_condition) = match rest.split_once(", with index condition: ") {
        Some((scan, index_condition)) => (scan.trim(), Some(index_condition.trim())),
        None => (rest, None),
    };

    if let Some(index_condition) = index_condition {
        node.key_condition = Some(condition("Index Cond", index_condition));
    }

    if scan.starts_with('(') {
        let lookup = condition("Index Cond", scan);
        match node.key_condition {
            None => node.key_condition = Some(lookup),
            Some(_) => node.details.push(("Lookup".to_string(), scan.to_string())),
        }
    } else if let Some(range) = scan.strip_prefix("over ") {
        node.details
            .push(("Range".to_string(), format!("over {range}")));
    } else if !scan.is_empty() {
        node.details.push(("Detail".to_string(), scan.to_string()));
    }

    node
}

/// `Inner hash join (a = b)` carries its condition in trailing parentheses;
/// every other line is just an operation name.
fn join_or_plain_node(description: &str) -> PlanNode {
    match description.split_once(" (") {
        Some((operation, condition_text)) if description.ends_with(')') => {
            let mut node = PlanNode::new(operation);
            node.key_condition = Some(condition("Join Cond", &format!("({condition_text}")));
            node
        }
        _ => PlanNode::new(description),
    }
}

fn condition(label: &str, value: &str) -> PlanCondition {
    PlanCondition {
        label: label.to_string(),
        value: value.to_string(),
    }
}

fn apply_metrics(node: &mut PlanNode, metrics: &str) {
    if metrics.contains("(never executed)") {
        node.actual_startup_time = Some(0.0);
        node.actual_total_time = Some(0.0);
        node.actual_rows = Some(0.0);
        node.actual_loops = Some(0);
    }

    if let Some(cost) = group(metrics, "(cost=") {
        node.estimated_total_cost = number_after(cost, "cost=");
        node.estimated_rows = number_after(cost, "rows=");
    }

    if let Some(actual) = group(metrics, "(actual time=") {
        let times = actual
            .strip_prefix("actual time=")
            .and_then(|text| text.split_whitespace().next())
            .and_then(|range| range.split_once(".."));

        if let Some((startup, total)) = times {
            node.actual_startup_time = startup.parse().ok();
            node.actual_total_time = total.parse().ok();
        }

        node.actual_rows = number_after(actual, "rows=");
        node.actual_loops = token_after(actual, "loops=").and_then(|loops| loops.parse().ok());
    }
}

/// The text of the parenthesised group that starts with `opening`, without
/// its parentheses.
fn group<'text>(metrics: &'text str, opening: &str) -> Option<&'text str> {
    let start = metrics.find(opening)? + 1;
    let tail = metrics.get(start..)?;
    let end = tail.find(')')?;
    tail.get(..end)
}

fn token_after<'text>(text: &'text str, key: &str) -> Option<&'text str> {
    let start = text.find(key)? + key.len();
    text.get(start..)?.split_whitespace().next()
}

fn number_after(text: &str, key: &str) -> Option<f64> {
    token_after(text, key)?.parse().ok()
}
