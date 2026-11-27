//! The plan as the views read it: a flat list of nodes with the numbers the
//! views show already worked out.
//!
//! Everything here is pure. The driver's [`QueryPlan`] is a tree of optional
//! numbers; the diagram, the tree and the inspector all need the same derived
//! facts (a node's self cost, its share of the plan, which node is the
//! costliest, which columns have any data), so they are computed once here
//! instead of in each view.

use dbflux_core::{PlanCondition, PlanFormat, PlanNode, QueryPlan};

/// Position of a node in the plan, in reading order (a node comes before its
/// children, roots first). It is the node's identity in every view and the
/// number the inspector shows.
pub(crate) type NodeId = usize;

/// One operation of the plan, with its derived numbers.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlanNodeView {
    pub id: NodeId,
    pub parent: Option<NodeId>,
    pub depth: usize,
    pub children: Vec<NodeId>,
    pub operation: String,
    pub description: Option<String>,
    pub relation: Option<String>,
    pub index: Option<String>,
    pub key_condition: Option<PlanCondition>,
    pub startup_cost: Option<f64>,
    pub total_cost: Option<f64>,
    pub rows: Option<f64>,
    /// Cost of this node alone, never below zero.
    pub self_cost: Option<f64>,
    /// `self_cost` as a fraction of the plan total (0 to 1). `None` when the
    /// node or the plan has no cost to compare.
    pub self_share: Option<f64>,
    /// The node with the highest self cost of the plan.
    pub is_costliest: bool,
    pub details: Vec<(String, String)>,
    pub warnings: Vec<String>,
    /// Rows the edge out of this node carries: the actual rows over all loops
    /// when the plan ran, the estimate otherwise.
    pub edge_rows: Option<f64>,
    /// Rows returned per loop. Analyzed plans only.
    pub actual_rows: Option<f64>,
    /// Milliseconds until the first row, per loop.
    pub actual_startup_time: Option<f64>,
    /// Milliseconds spent in the node and its children, over all loops.
    pub actual_time: Option<f64>,
    pub loops: Option<u64>,
    /// Milliseconds spent in this node alone, over all loops.
    pub self_time: Option<f64>,
    /// `self_time` as a fraction of the execution time (0 to 1).
    pub self_time_share: Option<f64>,
    /// The share the views draw: the self time share of an analyzed plan, the
    /// self cost share otherwise.
    pub share: Option<f64>,
    /// The node with the most self time of the plan.
    pub is_slowest: bool,
    /// How far the actual rows are from the estimate, as a factor of at least
    /// ten. Set on the one node with the largest gap.
    pub estimate_gap: Option<f64>,
    /// Actual rows per loop against the estimate.
    pub row_drift: Option<RowDrift>,
}

/// Actual rows per loop compared with the estimate, as a factor of at least
/// one. Both numbers count as at least one row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum RowDrift {
    More(f64),
    Fewer(f64),
    Same,
}

impl PlanNodeView {
    /// Whether the node reports anything the engine measured.
    pub(crate) fn has_actuals(&self) -> bool {
        self.actual_rows.is_some() || self.actual_time.is_some() || self.loops.is_some()
    }

    /// The node the plan marks as its hot spot: the costliest of an estimate,
    /// the slowest of an analyzed plan.
    pub(crate) fn is_marked(&self) -> bool {
        self.is_costliest || self.is_slowest
    }
}

/// The columns of the Tree view that carry data for this plan. A column with
/// nothing to show is left out, so a plan without costs (SQLite) is not a
/// table of dashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlanColumns {
    pub index: bool,
    pub cost: bool,
    pub self_cost: bool,
    pub rows: bool,
    pub actual_rows: bool,
    pub time: bool,
    pub loops: bool,
    pub self_time: bool,
}

/// The plan, ready to draw.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlanModel {
    pub nodes: Vec<PlanNodeView>,
    pub roots: Vec<NodeId>,
    pub total_cost: Option<f64>,
    pub total_rows: Option<f64>,
    /// The query ran: the plan carries what the engine measured.
    pub analyzed: bool,
    /// Milliseconds the engine spent planning. Analyzed plans only.
    pub planning_time: Option<f64>,
    /// Milliseconds the engine spent executing. Analyzed plans only.
    pub execution_time: Option<f64>,
    /// The plan's own warnings, then those of its nodes, without repeats.
    pub warnings: Vec<String>,
    pub columns: PlanColumns,
    pub raw: String,
    pub raw_format: PlanFormat,
}

impl PlanModel {
    pub(crate) fn from_plan(plan: &QueryPlan) -> Self {
        let mut nodes = Vec::new();
        let mut roots = Vec::with_capacity(plan.roots.len());
        for root in &plan.roots {
            roots.push(flatten(root, None, 0, &mut nodes));
        }

        let total_cost = plan_total_cost(&nodes, &roots);
        let analyzed = plan.planning_time.is_some()
            || plan.execution_time.is_some()
            || nodes.iter().any(PlanNodeView::has_actuals);
        let total_rows = if analyzed {
            actual_total_rows(&nodes, &roots)
        } else {
            None
        }
        .or_else(|| {
            roots
                .iter()
                .map(|&root| nodes.get(root).and_then(|node| node.rows))
                .sum::<Option<f64>>()
                .filter(|_| !roots.is_empty())
        });

        apply_shares(&mut nodes, total_cost);
        let time_basis = analyzed && apply_time_shares(&mut nodes, &roots, plan.execution_time);
        for node in nodes.iter_mut() {
            node.share = if time_basis {
                node.self_time_share
            } else {
                node.self_share
            };
            if time_basis {
                node.is_costliest = false;
            }
            if analyzed {
                apply_actual_rows(node);
            }
        }

        let mut warnings: Vec<String> = Vec::new();
        for warning in plan
            .warnings
            .iter()
            .chain(nodes.iter().flat_map(|node| node.warnings.iter()))
        {
            if !warnings.contains(warning) {
                warnings.push(warning.clone());
            }
        }
        if let Some(warning) = mark_estimate_gap(plan, &mut nodes, &roots) {
            warnings.push(warning);
        }

        let columns = PlanColumns {
            index: nodes.iter().any(|node| node.index.is_some()),
            cost: nodes.iter().any(|node| node.total_cost.is_some()),
            self_cost: !time_basis && nodes.iter().any(|node| node.self_share.is_some()),
            rows: nodes.iter().any(|node| node.rows.is_some()),
            actual_rows: nodes.iter().any(|node| node.actual_rows.is_some()),
            time: nodes.iter().any(|node| node.actual_time.is_some()),
            loops: nodes.iter().any(|node| node.loops.is_some()),
            self_time: time_basis,
        };

        Self {
            nodes,
            roots,
            total_cost,
            total_rows,
            analyzed,
            planning_time: plan.planning_time,
            execution_time: plan.execution_time,
            warnings,
            columns,
            raw: display_raw(&plan.raw, plan.raw_format),
            raw_format: plan.raw_format,
        }
    }

    pub(crate) fn node(&self, id: NodeId) -> Option<&PlanNodeView> {
        self.nodes.get(id)
    }

    /// The node the keyboard lands on first: the slowest or costliest one,
    /// otherwise the first root.
    pub(crate) fn entry_node(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .find(|node| node.is_marked())
            .map(|node| node.id)
            .or_else(|| self.roots.first().copied())
    }

    /// The largest row estimate of the plan, the scale the edge widths use.
    pub(crate) fn max_rows(&self) -> Option<f64> {
        self.nodes
            .iter()
            .filter_map(|node| node.edge_rows)
            .fold(None, |max: Option<f64>, rows| {
                Some(max.map_or(rows, |current| current.max(rows)))
            })
    }
}

fn flatten(
    node: &PlanNode,
    parent: Option<NodeId>,
    depth: usize,
    nodes: &mut Vec<PlanNodeView>,
) -> NodeId {
    let id = nodes.len();
    nodes.push(PlanNodeView {
        id,
        parent,
        depth,
        children: Vec::new(),
        operation: node.operation.clone(),
        description: node.description.clone(),
        relation: node.relation.clone(),
        index: node.index.clone(),
        key_condition: node.key_condition.clone(),
        startup_cost: node.estimated_startup_cost,
        total_cost: node.estimated_total_cost,
        rows: node.estimated_rows,
        self_cost: node.estimated_self_cost(),
        self_share: None,
        is_costliest: false,
        details: node.details.clone(),
        warnings: node.warnings.clone(),
        edge_rows: node.estimated_rows,
        actual_rows: node.actual_rows,
        actual_startup_time: node.actual_startup_time,
        actual_time: node
            .actual_total_time
            .map(|time| time * node.actual_loops.unwrap_or(1) as f64),
        loops: node.actual_loops,
        self_time: node.actual_self_time(),
        self_time_share: None,
        share: None,
        is_slowest: false,
        estimate_gap: None,
        row_drift: None,
    });

    let children: Vec<NodeId> = node
        .children
        .iter()
        .map(|child| flatten(child, Some(id), depth + 1, nodes))
        .collect();
    if let Some(entry) = nodes.get_mut(id) {
        entry.children = children;
    }

    id
}

/// The cost of the whole plan: the total cost of its roots, when every root
/// reports one.
fn plan_total_cost(nodes: &[PlanNodeView], roots: &[NodeId]) -> Option<f64> {
    if roots.is_empty() {
        return None;
    }

    roots
        .iter()
        .map(|&root| nodes.get(root).and_then(|node| node.total_cost))
        .sum::<Option<f64>>()
}

/// Fills each node's share of the plan total and marks the costliest node.
fn apply_shares(nodes: &mut [PlanNodeView], total_cost: Option<f64>) {
    let Some(total) = total_cost.filter(|total| *total > 0.0) else {
        return;
    };

    for node in nodes.iter_mut() {
        node.self_share = node.self_cost.map(|cost| (cost / total).clamp(0.0, 1.0));
    }

    let costliest = nodes
        .iter()
        .filter_map(|node| node.self_cost.map(|cost| (node.id, cost)))
        .filter(|(_, cost)| *cost > 0.0)
        .fold(None, |best: Option<(NodeId, f64)>, (id, cost)| match best {
            Some((_, best_cost)) if best_cost >= cost => best,
            _ => Some((id, cost)),
        });

    if let Some((id, _)) = costliest
        && let Some(node) = nodes.get_mut(id)
    {
        node.is_costliest = true;
    }
}

/// The rows the plan returned, as its roots report them over all loops.
fn actual_total_rows(nodes: &[PlanNodeView], roots: &[NodeId]) -> Option<f64> {
    if roots.is_empty() {
        return None;
    }

    roots
        .iter()
        .map(|&root| {
            let node = nodes.get(root)?;
            Some(node.actual_rows? * node.loops.unwrap_or(1) as f64)
        })
        .sum::<Option<f64>>()
}

/// Fills each node's share of the execution time and marks the slowest node.
/// `true` when any node got a share.
fn apply_time_shares(
    nodes: &mut [PlanNodeView],
    roots: &[NodeId],
    execution_time: Option<f64>,
) -> bool {
    let total = execution_time
        .or_else(|| {
            roots
                .iter()
                .map(|&root| nodes.get(root).and_then(|node| node.actual_time))
                .sum::<Option<f64>>()
        })
        .filter(|total| *total > 0.0);
    let Some(total) = total else {
        return false;
    };

    for node in nodes.iter_mut() {
        node.self_time_share = node.self_time.map(|time| (time / total).clamp(0.0, 1.0));
    }

    let slowest = nodes
        .iter()
        .filter_map(|node| node.self_time.map(|time| (node.id, time)))
        .filter(|(_, time)| *time > 0.0)
        .fold(None, |best: Option<(NodeId, f64)>, (id, time)| match best {
            Some((_, best_time)) if best_time >= time => best,
            _ => Some((id, time)),
        });

    if let Some((id, _)) = slowest
        && let Some(node) = nodes.get_mut(id)
    {
        node.is_slowest = true;
    }

    nodes.iter().any(|node| node.self_time_share.is_some())
}

/// Weights the edge by the rows the node returned over all its loops, and
/// compares them per loop with the estimate.
fn apply_actual_rows(node: &mut PlanNodeView) {
    if let Some(rows) = node.actual_rows {
        node.edge_rows = Some(rows * node.loops.unwrap_or(1) as f64);
    }

    if node.loops == Some(0) {
        return;
    }
    let (Some(estimated), Some(actual)) = (node.rows, node.actual_rows) else {
        return;
    };
    let (estimated, actual) = (estimated.max(1.0), actual.max(1.0));
    let factor = estimated.max(actual) / estimated.min(actual);

    node.row_drift = Some(if format!("{factor:.1}") == "1.0" {
        RowDrift::Same
    } else if actual > estimated {
        RowDrift::More(factor)
    } else {
        RowDrift::Fewer(factor)
    });
}

/// Marks the node whose actual rows are furthest from the estimate (the
/// core's rule: ten times or more) and words the warning that says so.
fn mark_estimate_gap(
    plan: &QueryPlan,
    nodes: &mut [PlanNodeView],
    roots: &[NodeId],
) -> Option<String> {
    let gap = plan.largest_estimate_gap()?;

    let mut path = gap.path.iter();
    let mut id = *roots.get(*path.next()?)?;
    for index in path {
        id = *nodes.get(id)?.children.get(*index)?;
    }

    let node = nodes.get_mut(id)?;
    node.estimate_gap = Some(gap.ratio);

    let name = match &node.relation {
        Some(relation) => format!(
            "{} {} {relation}",
            node.operation,
            dbflux_i18n::t!("document.query_plan.tree.on_relation")
        ),
        None => node.operation.clone(),
    };

    Some(dbflux_i18n::t!(
        "document.query_plan.warnings.estimate_gap",
        node = name,
        actual = format_row_count(gap.node.actual_rows?),
        estimated = format_row_count(gap.node.estimated_rows?),
        ratio = format_ratio(gap.ratio)
    ))
}

/// A cost as the planner prints it: two decimals, thousands separated
/// (`9,639.43`).
pub(crate) fn format_cost(cost: f64) -> String {
    let rounded = (cost.max(0.0) * 100.0).round() / 100.0;
    let whole = rounded.trunc();
    let cents = ((rounded - whole) * 100.0).round() as u32;

    format!("{}.{cents:02}", group_thousands(whole as u64))
}

/// A row estimate as a whole number, thousands separated (`3,100`).
pub(crate) fn format_rows(rows: f64) -> String {
    group_thousands(rows.max(0.0).round() as u64)
}

/// A share of the plan as a percentage with one decimal (`85.2%`). A node
/// that spends something, but less than a tenth of a percent, reads `<0.1%`
/// rather than a misleading `0.0%`.
pub(crate) fn format_share(share: f64) -> String {
    let percent = share.clamp(0.0, 1.0) * 100.0;

    if percent > 0.0 && percent < 0.05 {
        "<0.1%".to_string()
    } else {
        format!("{percent:.1}%")
    }
}

/// A factor between estimate and actual rows: one decimal below a hundred
/// times (`15.5×`), none from there on (`750×`).
pub(crate) fn format_ratio(ratio: f64) -> String {
    if (ratio * 10.0).round() / 10.0 >= 100.0 {
        format!("{ratio:.0}×")
    } else {
        format!("{ratio:.1}×")
    }
}

/// A row count worded for its number: `1 row`, `3,100 rows`.
pub(crate) fn format_row_count(rows: f64) -> String {
    let count = group_thousands(rows.max(0.0).round() as u64);

    if rows.round() == 1.0 {
        dbflux_i18n::t!("document.query_plan.rows.one", count = count)
    } else {
        dbflux_i18n::t!("document.query_plan.rows.many", count = count)
    }
}

/// A count of loops, thousands separated (`48,200`).
pub(crate) fn format_loops(loops: u64) -> String {
    group_thousands(loops)
}

/// A duration given in milliseconds: three decimals at most under a
/// millisecond (`0.041 ms`), one decimal up to a second (`412.6 ms`), seconds
/// above (`1.50 s`).
pub(crate) fn format_time(milliseconds: f64) -> String {
    let milliseconds = milliseconds.max(0.0);

    if milliseconds >= 1000.0 {
        format!("{:.2} s", milliseconds / 1000.0)
    } else if milliseconds >= 1.0 {
        format!("{milliseconds:.1} ms")
    } else {
        let text = format!("{milliseconds:.3}");
        let trimmed = text.trim_end_matches('0').trim_end_matches('.');
        format!("{} ms", if trimmed.is_empty() { "0" } else { trimmed })
    }
}

fn group_thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);

    for (position, digit) in digits.chars().enumerate() {
        if position > 0 && (digits.len() - position).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }

    grouped
}

/// The raw plan as it is shown and copied: JSON re-indented with two spaces
/// (the engines return it on one line), everything else as returned. Text that
/// does not parse as JSON stays as it came.
fn display_raw(raw: &str, format: PlanFormat) -> String {
    if format != PlanFormat::Json {
        return raw.to_string();
    }

    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|value| serde_json::to_string_pretty(&value).ok())
        .unwrap_or_else(|| raw.to_string())
}
