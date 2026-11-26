//! Layouts for a graph of boxes.
//!
//! A diagram is described as [`LayoutNode`]s (a size and a sort key) and
//! [`LayoutEdge`]s (a direction and a weight) addressed by position in the input
//! slices. Every layout returns a [`DiagramLayout`] whose nodes keep the input
//! order and whose edges keep the input order too, so a consumer maps the result
//! back to its own ids by index.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::f32::consts::PI as PI_F32;

use petgraph::Direction as EdgeDirection;
use petgraph::prelude::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;

/// Horizontal gutter between two layers of the layered layout. Each layer starts
/// after the widest node of the previous one, so a wide node can never overlap
/// its neighbour.
const LAYER_GUTTER_X: f32 = 96.0;
/// Vertical gutter between two stacked nodes.
const NODE_SPACING_Y: f32 = 64.0;
const CELL_WIDTH: f32 = 360.0;
const COMPACT_CELL_SPACING_X: f32 = 48.0;
const COMPACT_CELL_SPACING_Y: f32 = 48.0;

/// A box to place: its size and the key that breaks every ordering tie.
#[derive(Clone, Debug, PartialEq)]
pub struct LayoutNode {
    pub width: f32,
    pub height: f32,
    /// Nodes with the same depth, and nodes the crossing reduction cannot tell
    /// apart, are ordered by this key, so the same input always draws the same
    /// way.
    pub sort_key: String,
}

/// A directed edge between two nodes, by index into the node slice.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LayoutEdge {
    pub from: usize,
    pub to: usize,
    /// How much the edge carries (rows, cost, ...). The layout does not read it;
    /// it is handed back on the matching [`PlacedEdge`] so the canvas can draw
    /// the edge with a width.
    pub weight: f32,
}

/// Where one node ended up.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlacedNode {
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
}

/// Where one edge attaches: the middle of the right side of its source and the
/// middle of the left side of its target.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlacedEdge {
    pub from: usize,
    pub to: usize,
    pub from_anchor: (f32, f32),
    pub to_anchor: (f32, f32),
    pub weight: f32,
}

/// The result of a layout.
#[derive(Clone, Debug, PartialEq)]
pub struct DiagramLayout {
    /// One entry per input node, in input order.
    pub nodes: Vec<PlacedNode>,
    /// One entry per input edge whose ends exist, in input order.
    pub edges: Vec<PlacedEdge>,
    pub total_width: f32,
    pub total_height: f32,
}

impl DiagramLayout {
    fn empty() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            total_width: 0.0,
            total_height: 0.0,
        }
    }
}

/// The input graph with the node order and the edge order of the caller, which is
/// what the layouts' tie-breaking relies on.
struct Graph<'a> {
    nodes: &'a [LayoutNode],
    edges: &'a [LayoutEdge],
    graph: DiGraph<(), f32>,
}

impl<'a> Graph<'a> {
    fn new(nodes: &'a [LayoutNode], edges: &'a [LayoutEdge]) -> Self {
        let mut graph = DiGraph::with_capacity(nodes.len(), edges.len());
        for _ in nodes {
            graph.add_node(());
        }
        for edge in edges {
            if edge.from < nodes.len() && edge.to < nodes.len() {
                graph.add_edge(
                    NodeIndex::new(edge.from),
                    NodeIndex::new(edge.to),
                    edge.weight,
                );
            }
        }
        Self {
            nodes,
            edges,
            graph,
        }
    }

    fn node_count(&self) -> usize {
        self.nodes.len()
    }

    fn key(&self, idx: NodeIndex) -> &str {
        self.nodes
            .get(idx.index())
            .map(|node| node.sort_key.as_str())
            .unwrap_or("")
    }

    fn size(&self, idx: NodeIndex) -> (f32, f32) {
        self.nodes
            .get(idx.index())
            .map(|node| (node.width, node.height))
            .unwrap_or((0.0, 0.0))
    }

    fn sort_by_key(&self, nodes: &mut [NodeIndex]) {
        nodes.sort_by(|a, b| self.key(*a).cmp(self.key(*b)));
    }

    /// Every node as a `PlacedNode`, in input order, from the positions chosen
    /// (a node no position was chosen for sits at the origin).
    fn into_layout(
        self,
        positions: &HashMap<NodeIndex, (f32, f32)>,
        total_width: f32,
        total_height: f32,
    ) -> DiagramLayout {
        let placed: Vec<PlacedNode> = (0..self.node_count())
            .map(|position| {
                let idx = NodeIndex::new(position);
                let (x, y) = positions.get(&idx).copied().unwrap_or_default();
                let (width, height) = self.size(idx);
                PlacedNode {
                    x,
                    y,
                    width,
                    height,
                }
            })
            .collect();

        let edges = self
            .edges
            .iter()
            .filter_map(|edge| {
                let from = placed.get(edge.from)?;
                let to = placed.get(edge.to)?;
                Some(PlacedEdge {
                    from: edge.from,
                    to: edge.to,
                    from_anchor: (from.x + from.width, from.y + from.height / 2.0),
                    to_anchor: (to.x, to.y + to.height / 2.0),
                    weight: edge.weight,
                })
            })
            .collect();

        DiagramLayout {
            nodes: placed,
            edges,
            total_width,
            total_height,
        }
    }
}

/// Assigns every node to a layer by dependency depth, cutting cycles open as it
/// goes.
///
/// Condensing a cycle into one layer kept the diagram from collapsing into a
/// grid, but it also merged every node of the cycle into a single column. This
/// walks the graph from its roots instead, and when it reaches a node whose
/// remaining predecessors are all inside a cycle, it cuts there and keeps going,
/// so a cycle costs one reversed edge rather than a lost layout.
fn assign_layers(graph: &Graph<'_>) -> BTreeMap<usize, Vec<NodeIndex>> {
    let mut pending_incoming: HashMap<NodeIndex, usize> = HashMap::new();
    for idx in graph.graph.node_indices() {
        pending_incoming.insert(
            idx,
            graph
                .graph
                .edges_directed(idx, EdgeDirection::Incoming)
                .count(),
        );
    }

    // The sort key decides every tie, so the same input always draws the same way.
    let mut remaining: Vec<NodeIndex> = graph.graph.node_indices().collect();
    graph.sort_by_key(&mut remaining);

    let mut layer: HashMap<NodeIndex, usize> =
        remaining.iter().map(|&idx| (idx, 0_usize)).collect();
    let mut placed: HashSet<NodeIndex> = HashSet::new();
    let mut queue: VecDeque<NodeIndex> = remaining
        .iter()
        .copied()
        .filter(|idx| pending_incoming.get(idx).copied().unwrap_or_default() == 0)
        .collect();

    while placed.len() < graph.node_count() {
        while let Some(node) = queue.pop_front() {
            if !placed.insert(node) {
                continue;
            }
            let node_layer = layer.get(&node).copied().unwrap_or_default();
            for successor in graph
                .graph
                .neighbors_directed(node, EdgeDirection::Outgoing)
            {
                if placed.contains(&successor) {
                    continue;
                }
                let successor_layer = layer.entry(successor).or_default();
                *successor_layer = (*successor_layer).max(node_layer + 1);

                if let Some(count) = pending_incoming.get_mut(&successor) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        queue.push_back(successor);
                    }
                }
            }
        }

        // No source left: everything unplaced sits inside a cycle. Cut it at the
        // first key still standing and treat the edges that closed the cycle as
        // back edges.
        let Some(&cut) = remaining.iter().find(|idx| !placed.contains(*idx)) else {
            break;
        };
        pending_incoming.insert(cut, 0);
        queue.push_back(cut);
    }

    let mut layers: BTreeMap<usize, Vec<NodeIndex>> = BTreeMap::new();
    for idx in graph.graph.node_indices() {
        let depth = layer.get(&idx).copied().unwrap_or_default();
        layers.entry(depth).or_default().push(idx);
    }

    for nodes in layers.values_mut() {
        graph.sort_by_key(nodes);
    }

    layers
}

/// Reorders the nodes inside each layer to reduce edge crossings.
///
/// This is the barycenter pass of a Sugiyama layout: repeatedly place every node
/// next to the average position of the neighbours it connects to in the layer
/// before it (then after it, sweeping back up). Ties break on the sort key, so
/// the same input always produces the same diagram.
fn reduce_crossings(graph: &Graph<'_>, layers: &mut BTreeMap<usize, Vec<NodeIndex>>) {
    const SWEEPS: usize = 4;
    if layers.len() < 2 {
        return;
    }

    let keys: Vec<usize> = layers.keys().copied().collect();

    for _ in 0..SWEEPS {
        // Sweep top-down, then bottom-up, so both orders inform each other.
        let passes: [Box<dyn Iterator<Item = &usize>>; 2] =
            [Box::new(keys.iter()), Box::new(keys.iter().rev())];

        for pass in passes {
            for key in pass {
                let Some(current) = layers.get(key).cloned() else {
                    continue;
                };
                let Some(&previous_key) = keys.iter().filter(|k| *k < key).max() else {
                    continue;
                };
                let positions = positions_of(layers.get(&previous_key).map(Vec::as_slice));

                let mut scored: Vec<(f32, String, NodeIndex)> = current
                    .iter()
                    .map(|&idx| {
                        let neighbours: Vec<f32> = graph
                            .graph
                            .neighbors_directed(idx, EdgeDirection::Incoming)
                            .filter_map(|neighbor| positions.get(&neighbor).copied())
                            .collect();
                        let barycenter = if neighbours.is_empty() {
                            f32::MAX
                        } else {
                            neighbours.iter().sum::<f32>() / neighbours.len() as f32
                        };
                        (barycenter, graph.key(idx).to_owned(), idx)
                    })
                    .collect();

                scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
                layers.insert(*key, scored.into_iter().map(|(_, _, idx)| idx).collect());
            }
        }
    }
}

/// Position of every node inside one layer, used as the barycenter input.
fn positions_of(layer: Option<&[NodeIndex]>) -> HashMap<NodeIndex, f32> {
    layer
        .unwrap_or_default()
        .iter()
        .enumerate()
        .map(|(position, &idx)| (idx, position as f32))
        .collect()
}

/// Layered layout, left to right.
///
/// Edges point from the left layer to the right one: a node sits one layer to
/// the right of the deepest node that points at it, so a tree whose edges run
/// from parent to child puts the root on the left. Cycles are cut open rather
/// than refused, layers are ordered to reduce crossings, and every layer starts
/// after the widest node of the previous one.
pub fn layered_left_right(nodes: &[LayoutNode], edges: &[LayoutEdge]) -> DiagramLayout {
    let graph = Graph::new(nodes, edges);
    if graph.node_count() == 0 {
        return DiagramLayout::empty();
    }

    let mut layers = assign_layers(&graph);
    reduce_crossings(&graph, &mut layers);

    let mut positions: HashMap<NodeIndex, (f32, f32)> = HashMap::new();

    // Layers are stacked left to right, each one starting after the widest node of
    // the previous layer. A fixed stride let a wide node overlap the next layer,
    // which is part of why the diagram read as one solid block.
    let mut layer_x = 0.0_f32;
    for node_ids in layers.values() {
        let layer_width = node_ids
            .iter()
            .map(|&idx| graph.size(idx).0)
            .fold(0.0_f32, f32::max);

        let mut y_cursor = 0.0_f32;
        for &idx in node_ids {
            positions.insert(idx, (layer_x, y_cursor));
            y_cursor += graph.size(idx).1 + NODE_SPACING_Y;
        }

        layer_x += layer_width + LAYER_GUTTER_X;
    }

    let total_width = if layers.is_empty() {
        0.0_f32
    } else {
        (layer_x - LAYER_GUTTER_X).max(0.0_f32)
    };
    let total_height = layers
        .values()
        .map(|ids| {
            ids.iter().map(|&idx| graph.size(idx).1).sum::<f32>()
                + (ids.len().saturating_sub(1) as f32) * NODE_SPACING_Y
        })
        .max_by(|a, b| a.total_cmp(b))
        .unwrap_or(0.0_f32);

    graph.into_layout(&positions, total_width, total_height)
}

/// Compact grid layout: nodes sorted by key and packed into a square-ish grid
/// with tight spacing.
pub fn compact_grid(nodes: &[LayoutNode], edges: &[LayoutEdge]) -> DiagramLayout {
    let graph = Graph::new(nodes, edges);
    let count = graph.node_count();
    if count == 0 {
        return DiagramLayout::empty();
    }

    let cols = ((count as f32).sqrt().ceil() as usize).max(1);
    let rows = count.div_ceil(cols);

    let mut sorted: Vec<NodeIndex> = graph.graph.node_indices().collect();
    graph.sort_by_key(&mut sorted);

    // First pass: per-row max heights and the widest node.
    let mut row_max_heights = vec![0.0_f32; rows];
    let mut max_node_width = 0.0_f32;
    for (i, &idx) in sorted.iter().enumerate() {
        let (width, height) = graph.size(idx);
        if let Some(slot) = row_max_heights.get_mut(i / cols) {
            *slot = slot.max(height);
        }
        max_node_width = max_node_width.max(width);
    }

    // Cumulative row y offsets.
    let mut row_y_offsets = vec![0.0_f32; rows];
    for r in 1..rows {
        if let (Some(&prev_offset), Some(&prev_height)) =
            (row_y_offsets.get(r - 1), row_max_heights.get(r - 1))
            && let Some(slot) = row_y_offsets.get_mut(r)
        {
            *slot = prev_offset + prev_height + COMPACT_CELL_SPACING_Y;
        }
    }

    // Cell width based on the widest node.
    let cell_width = max_node_width.max(CELL_WIDTH);

    let mut positions: HashMap<NodeIndex, (f32, f32)> = HashMap::new();
    for (i, &idx) in sorted.iter().enumerate() {
        let col = i % cols;
        let row = i / cols;
        let x = col as f32 * (cell_width + COMPACT_CELL_SPACING_X);
        let Some(&y) = row_y_offsets.get(row) else {
            continue;
        };
        positions.insert(idx, (x, y));
    }

    let total_width = cols as f32 * (cell_width + COMPACT_CELL_SPACING_X);
    let total_height = row_y_offsets
        .last()
        .map(|&y| y + row_max_heights.last().copied().unwrap_or(0.0))
        .unwrap_or(0.0_f32);

    graph.into_layout(&positions, total_width, total_height)
}

/// Radial layout: the `focal` node at the centre, the nodes it shares an edge
/// with on a circle around it, every other node in a grid below. A `focal` that
/// is not in `nodes` falls back to [`layered_left_right`].
pub fn radial(nodes: &[LayoutNode], edges: &[LayoutEdge], focal: usize) -> DiagramLayout {
    if focal >= nodes.len() {
        return layered_left_right(nodes, edges);
    }

    let graph = Graph::new(nodes, edges);
    let focal_idx = NodeIndex::new(focal);

    // Direct neighbours in either direction.
    let mut neighbor_indices: Vec<NodeIndex> = Vec::new();
    for edge in graph
        .graph
        .edges_directed(focal_idx, EdgeDirection::Outgoing)
    {
        neighbor_indices.push(edge.target());
    }
    for edge in graph
        .graph
        .edges_directed(focal_idx, EdgeDirection::Incoming)
    {
        neighbor_indices.push(edge.source());
    }
    graph.sort_by_key(&mut neighbor_indices);

    let neighbor_count = neighbor_indices.len();

    // The focal node sits at a fixed centre.
    let cx = 500.0_f32;
    let cy = 400.0_f32;

    let (focal_width, focal_height) = graph.size(focal_idx);
    let mut positions: HashMap<NodeIndex, (f32, f32)> = HashMap::new();
    positions.insert(focal_idx, (cx - focal_width / 2.0, cy - focal_height / 2.0));

    let radius = 300.0_f32.max(neighbor_count as f32 * 80.0);
    let angle_step = if neighbor_count > 0 {
        2.0 * PI_F32 / neighbor_count as f32
    } else {
        0.0
    };

    for (i, &neighbor_idx) in neighbor_indices.iter().enumerate() {
        // Start from the top.
        let angle = angle_step * i as f32 - PI_F32 / 2.0;
        let (width, height) = graph.size(neighbor_idx);
        positions.insert(
            neighbor_idx,
            (
                cx + radius * angle.cos() - width / 2.0,
                cy + radius * angle.sin() - height / 2.0,
            ),
        );
    }

    // Nodes outside the neighbourhood go in a grid below the circle.
    let mut other_indices: Vec<NodeIndex> = graph
        .graph
        .node_indices()
        .filter(|&idx| idx != focal_idx && !neighbor_indices.contains(&idx))
        .collect();
    graph.sort_by_key(&mut other_indices);

    let grid_start_y = cy + radius + 150.0;
    let grid_cols = 4;
    for (i, &idx) in other_indices.iter().enumerate() {
        let height = graph.size(idx).1;
        let col = i % grid_cols;
        let row = i / grid_cols;
        positions.insert(
            idx,
            (
                col as f32 * (CELL_WIDTH + COMPACT_CELL_SPACING_X),
                grid_start_y + row as f32 * (height + COMPACT_CELL_SPACING_Y),
            ),
        );
    }

    // Bounding box of every node plus a 100px margin.
    let rects: Vec<(f32, f32, f32, f32)> = positions
        .iter()
        .map(|(&idx, &(x, y))| {
            let (width, height) = graph.size(idx);
            (x, y, width, height)
        })
        .collect();
    let min_x = rects.iter().map(|r| r.0).fold(0.0_f32, f32::min);
    let min_y = rects.iter().map(|r| r.1).fold(0.0_f32, f32::min);
    let max_x = rects.iter().map(|r| r.0 + r.2).fold(0.0_f32, f32::max);
    let max_y = rects.iter().map(|r| r.1 + r.3).fold(0.0_f32, f32::max);

    graph.into_layout(&positions, max_x - min_x + 100.0, max_y - min_y + 100.0)
}

#[cfg(test)]
mod tests;
