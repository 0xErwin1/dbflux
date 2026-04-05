use std::collections::HashMap;

use petgraph::algo::is_cyclic_directed;
use petgraph::prelude::NodeIndex;
use petgraph::visit::EdgeRef;

use crate::graph::SchemaGraph;

const NODE_WIDTH: f32 = 220.0;
const NODE_HEADER_HEIGHT: f32 = 28.0;
const NODE_ROW_HEIGHT: f32 = 22.0;
const LAYER_SPACING_X: f32 = 280.0;
const NODE_SPACING_Y: f32 = 40.0;
const CELL_WIDTH: f32 = 300.0;
const CELL_HEIGHT: f32 = 200.0;

/// Layout information for a single node.
#[derive(Clone, Debug)]
pub struct NodeLayout {
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
}

/// Layout information for a single edge.
#[derive(Clone, Debug)]
pub struct EdgeLayout {
    pub from_node: NodeIndex,
    pub to_node: NodeIndex,
    pub from_anchor: (f32, f32),
    pub to_anchor: (f32, f32),
}

/// Result of a layout computation.
#[derive(Clone, Debug)]
pub struct LayoutResult {
    pub nodes: HashMap<NodeIndex, NodeLayout>,
    pub edges: Vec<EdgeLayout>,
    pub total_width: f32,
    pub total_height: f32,
}

/// Compute a layout for the given `SchemaGraph`.
pub fn compute_layout(graph: &SchemaGraph) -> LayoutResult {
    if graph.node_count() == 0 {
        return LayoutResult {
            nodes: HashMap::new(),
            edges: Vec::new(),
            total_width: 0.0,
            total_height: 0.0,
        };
    }

    if is_cyclic_directed(&graph.graph) {
        return grid_layout(graph);
    }

    layered_layout(graph)
}

/// Layered layout for acyclic graphs.
fn layered_layout(graph: &SchemaGraph) -> LayoutResult {
    // Find a root node: one with no incoming edges, or the first node.
    let root_idx = graph
        .graph
        .externals(petgraph::Direction::Incoming)
        .next()
        .or_else(|| graph.graph.externals(petgraph::Direction::Outgoing).next())
        .unwrap_or_else(|| NodeIndex::new(0));

    // BFS from root to assign layers.
    let mut layer: HashMap<NodeIndex, usize> = HashMap::new();
    let mut queue: Vec<(NodeIndex, usize)> = vec![(root_idx, 0)];
    layer.insert(root_idx, 0);

    while !queue.is_empty() {
        let (current, depth) = queue.remove(0);

        for edge in graph
            .graph
            .edges_directed(current, petgraph::Direction::Outgoing)
        {
            let neighbor = edge.target();
            if layer.insert(neighbor, depth + 1).is_none() {
                queue.push((neighbor, depth + 1));
            }
        }
    }

    // Handle disconnected nodes: assign them to layer 0.
    for idx in graph.graph.node_indices() {
        layer.entry(idx).or_insert(0);
    }

    // Group nodes by layer.
    let mut layers: HashMap<usize, Vec<NodeIndex>> = HashMap::new();
    for (idx, &l) in &layer {
        layers.entry(l).or_default().push(*idx);
    }

    // Sort nodes within each layer by table name for determinism.
    for nodes in layers.values_mut() {
        nodes.sort_by(|a, b| {
            let name_a = &graph.graph.node_weight(*a).unwrap().id.name;
            let name_b = &graph.graph.node_weight(*b).unwrap().id.name;
            name_a.cmp(name_b)
        });
    }

    let max_layer = layers.keys().max().copied().unwrap_or(0);
    let mut nodes: HashMap<NodeIndex, NodeLayout> = HashMap::new();

    for (l, node_ids) in &layers {
        let x = *l as f32 * LAYER_SPACING_X;

        for (pos, &idx) in node_ids.iter().enumerate() {
            let node_weight = graph.graph.node_weight(idx).unwrap();
            let col_count = node_weight.columns.len().max(1) as f32;
            let height = NODE_HEADER_HEIGHT + col_count * NODE_ROW_HEIGHT;

            let y = pos as f32 * (height + NODE_SPACING_Y);

            nodes.insert(
                idx,
                NodeLayout {
                    x,
                    y,
                    width: NODE_WIDTH,
                    height,
                },
            );
        }
    }

    // Build edge layouts.
    let edges: Vec<EdgeLayout> = graph
        .graph
        .edge_indices()
        .filter_map(|edge_idx| {
            let (source, target) = graph.graph.edge_endpoints(edge_idx)?;
            let from_layout = nodes.get(&source)?;
            let to_layout = nodes.get(&target)?;

            let from_anchor = (
                from_layout.x + NODE_WIDTH,
                from_layout.y + from_layout.height / 2.0,
            );
            let to_anchor = (to_layout.x, to_layout.y + to_layout.height / 2.0);

            Some(EdgeLayout {
                from_node: source,
                to_node: target,
                from_anchor,
                to_anchor,
            })
        })
        .collect();

    // Compute total bounds.
    let total_width = (max_layer as f32 + 1.0) * LAYER_SPACING_X;
    let total_height = layers.values().map(|ids| ids.len()).max().unwrap_or(0) as f32
        * (CELL_HEIGHT + NODE_SPACING_Y);

    LayoutResult {
        nodes,
        edges,
        total_width,
        total_height,
    }
}

/// Grid layout for cyclic graphs.
fn grid_layout(graph: &SchemaGraph) -> LayoutResult {
    let n = graph.node_count();
    let cols = ((n as f32).sqrt().ceil() as usize).max(1);

    let mut nodes: HashMap<NodeIndex, NodeLayout> = HashMap::new();

    for (i, idx) in graph.graph.node_indices().enumerate() {
        let node_weight = graph.graph.node_weight(idx).unwrap();
        let col_count = node_weight.columns.len().max(1) as f32;
        let height = NODE_HEADER_HEIGHT + col_count * NODE_ROW_HEIGHT;

        let x = (i % cols) as f32 * CELL_WIDTH;
        let y = (i / cols) as f32 * CELL_HEIGHT;

        nodes.insert(
            idx,
            NodeLayout {
                x,
                y,
                width: NODE_WIDTH,
                height,
            },
        );
    }

    let edges: Vec<EdgeLayout> = graph
        .graph
        .edge_indices()
        .filter_map(|edge_idx| {
            let (source, target) = graph.graph.edge_endpoints(edge_idx)?;
            let from_layout = nodes.get(&source)?;
            let to_layout = nodes.get(&target)?;

            let from_anchor = (
                from_layout.x + NODE_WIDTH,
                from_layout.y + from_layout.height / 2.0,
            );
            let to_anchor = (to_layout.x, to_layout.y + to_layout.height / 2.0);

            Some(EdgeLayout {
                from_node: source,
                to_node: target,
                from_anchor,
                to_anchor,
            })
        })
        .collect();

    let rows = (n + cols - 1) / cols;
    let total_width = cols as f32 * CELL_WIDTH;
    let total_height = rows as f32 * CELL_HEIGHT;

    LayoutResult {
        nodes,
        edges,
        total_width,
        total_height,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::SchemaGraph;
    use dbflux_core::{ColumnInfo, ForeignKeyInfo, TableInfo};

    fn col(name: &str, type_name: &str, pk: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_owned(),
            type_name: type_name.to_owned(),
            nullable: !pk,
            is_primary_key: pk,
            default_value: None,
            enum_values: None,
        }
    }

    fn table(name: &str, cols: Vec<ColumnInfo>, fks: Vec<ForeignKeyInfo>) -> TableInfo {
        TableInfo {
            name: name.to_owned(),
            schema: None,
            columns: Some(cols),
            indexes: None,
            foreign_keys: if fks.is_empty() { None } else { Some(fks) },
            constraints: None,
            sample_fields: None,
        }
    }

    fn fk(name: &str, cols: Vec<&str>, ref_table: &str, ref_cols: Vec<&str>) -> ForeignKeyInfo {
        ForeignKeyInfo {
            name: name.to_owned(),
            columns: cols.into_iter().map(String::from).collect(),
            referenced_table: ref_table.to_owned(),
            referenced_schema: None,
            referenced_columns: ref_cols.into_iter().map(String::from).collect(),
            on_delete: None,
            on_update: None,
        }
    }

    // ── 9.7: layout deterministic ────────────────────────────────────────────

    #[test]
    fn test_layout_deterministic() {
        let tables = vec![
            table(
                "users",
                vec![col("id", "integer", true), col("name", "text", false)],
                vec![],
            ),
            table(
                "orders",
                vec![col("id", "integer", true), col("user_id", "integer", false)],
                vec![fk("fk_orders_users", vec!["user_id"], "users", vec!["id"])],
            ),
        ];

        let graph = SchemaGraph::build(&tables);
        let layout1 = compute_layout(&graph);
        let layout2 = compute_layout(&graph);

        assert_eq!(layout1.nodes.len(), layout2.nodes.len());
        for (idx, node_layout) in &layout1.nodes {
            let other = layout2.nodes.get(idx).unwrap();
            assert_eq!(node_layout.x, other.x, "x differs for node {idx:?}");
            assert_eq!(node_layout.y, other.y, "y differs for node {idx:?}");
        }
    }

    // ── 9.8: grid fallback — no overlap ─────────────────────────────────────

    #[test]
    fn test_grid_no_overlap() {
        // A → B → C → A (cycle)
        let tables = vec![
            table(
                "a",
                vec![col("id", "integer", true)],
                vec![fk("fk_a_b", vec!["id"], "b", vec!["id"])],
            ),
            table(
                "b",
                vec![col("id", "integer", true)],
                vec![fk("fk_b_c", vec!["id"], "c", vec!["id"])],
            ),
            table(
                "c",
                vec![col("id", "integer", true)],
                vec![fk("fk_c_a", vec!["id"], "a", vec!["id"])],
            ),
        ];

        let graph = SchemaGraph::build(&tables);
        let layout = compute_layout(&graph);

        // Check all nodes have non-overlapping bounding boxes.
        let node_list: Vec<(&NodeIndex, &NodeLayout)> = layout.nodes.iter().collect();
        for i in 0..node_list.len() {
            for j in (i + 1)..node_list.len() {
                let (_, a) = node_list[i];
                let (_, b) = node_list[j];

                let a_right = a.x + a.width;
                let a_bottom = a.y + a.height;
                let b_right = b.x + b.width;
                let b_bottom = b.y + b.height;

                let overlaps_x = a.x < b_right && a_right > b.x;
                let overlaps_y = a.y < b_bottom && a_bottom > b.y;

                assert!(
                    !(overlaps_x && overlaps_y),
                    "Nodes overlap: {:?} vs {:?}",
                    a,
                    b
                );
            }
        }

        // All three nodes should be present.
        assert_eq!(layout.nodes.len(), 3);
        // Total width/height should be positive.
        assert!(layout.total_width > 0.0);
        assert!(layout.total_height > 0.0);
    }
}
