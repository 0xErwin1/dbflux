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
}

/// The plan, ready to draw.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlanModel {
    pub nodes: Vec<PlanNodeView>,
    pub roots: Vec<NodeId>,
    pub total_cost: Option<f64>,
    pub total_rows: Option<f64>,
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
        let total_rows = roots
            .iter()
            .map(|&root| nodes.get(root).and_then(|node| node.rows))
            .sum::<Option<f64>>()
            .filter(|_| !roots.is_empty());

        apply_shares(&mut nodes, total_cost);
        let columns = PlanColumns {
            index: nodes.iter().any(|node| node.index.is_some()),
            cost: nodes.iter().any(|node| node.total_cost.is_some()),
            self_cost: nodes.iter().any(|node| node.self_share.is_some()),
            rows: nodes.iter().any(|node| node.rows.is_some()),
        };

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

        Self {
            nodes,
            roots,
            total_cost,
            total_rows,
            warnings,
            columns,
            raw: display_raw(&plan.raw, plan.raw_format),
            raw_format: plan.raw_format,
        }
    }

    pub(crate) fn node(&self, id: NodeId) -> Option<&PlanNodeView> {
        self.nodes.get(id)
    }

    /// The node the keyboard lands on first: the costliest one, otherwise the
    /// first root.
    pub(crate) fn entry_node(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .find(|node| node.is_costliest)
            .map(|node| node.id)
            .or_else(|| self.roots.first().copied())
    }

    /// The largest row estimate of the plan, the scale the edge widths use.
    pub(crate) fn max_rows(&self) -> Option<f64> {
        self.nodes
            .iter()
            .filter_map(|node| node.rows)
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
