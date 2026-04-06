use std::collections::{HashMap, VecDeque};

use petgraph::algo::is_cyclic_directed;
use petgraph::prelude::NodeIndex;
use petgraph::visit::EdgeRef;

use crate::graph::SchemaGraph;

const NODE_HEADER_HEIGHT: f32 = 28.0;
const NODE_ROW_HEIGHT: f32 = 22.0;
const LAYER_SPACING_X: f32 = 320.0;
const NODE_SPACING_Y: f32 = 40.0;
const CELL_WIDTH: f32 = 360.0;

/// Compute the width for a node based on its content.
fn compute_node_width(node: &crate::graph::TableNode) -> f32 {
    let chars_per_px = 7.0_f32;
    let h_padding = 40.0_f32;

    let header_text = match &node.id.schema {
        Some(s) => format!("{} · {}", s, node.id.name),
        None => node.id.name.clone(),
    };
    let header_width = header_text.len() as f32 * chars_per_px + h_padding;

    let body_width = node
        .columns
        .iter()
        .map(|col| {
            let type_label = if col.is_pk {
                format!("{} [pk]", col.type_name)
            } else if col.is_fk {
                format!("{} [fk]", col.type_name)
            } else {
                col.type_name.clone()
            };
            let name_w = col.name.len() as f32 * chars_per_px;
            let type_w = type_label.len() as f32 * chars_per_px;
            name_w + type_w + h_padding + 8.0
        })
        .fold(0.0_f32, f32::max);

    header_width.max(body_width).max(180.0).min(400.0)
}

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
        .unwrap_or_else(|| {
            graph
                .graph
                .node_indices()
                .next()
                .expect("layered_layout requires a non-empty graph")
        });

    // BFS from root to assign layers.
    let mut layer: HashMap<NodeIndex, usize> = HashMap::new();
    let mut queue: VecDeque<(NodeIndex, usize)> = VecDeque::from([(root_idx, 0)]);
    layer.insert(root_idx, 0);

    while let Some((current, depth)) = queue.pop_front() {
        for edge in graph
            .graph
            .edges_directed(current, petgraph::Direction::Outgoing)
        {
            let neighbor = edge.target();
            if layer.insert(neighbor, depth + 1).is_none() {
                queue.push_back((neighbor, depth + 1));
            }
        }
    }

    // Handle disconnected nodes: assign them to an isolated layer beyond max_layer.
    let max_layer = layer.values().max().copied().unwrap_or(0);
    for idx in graph.graph.node_indices() {
        layer.entry(idx).or_insert(max_layer + 1);
    }

    // Group nodes by layer.
    let mut layers: HashMap<usize, Vec<NodeIndex>> = HashMap::new();
    for (idx, &l) in &layer {
        layers.entry(l).or_default().push(*idx);
    }

    // Sort nodes within each layer by table name for determinism.
    for nodes in layers.values_mut() {
        nodes.sort_by(|a, b| {
            let name_a = &graph
                .graph
                .node_weight(*a)
                .expect("invariant: a is a node index from the same graph")
                .id
                .name;
            let name_b = &graph
                .graph
                .node_weight(*b)
                .expect("invariant: b is a node index from the same graph")
                .id
                .name;
            name_a.cmp(name_b)
        });
    }

    let computed_max_layer = layers.keys().max().copied().unwrap_or(0);
    let mut nodes: HashMap<NodeIndex, NodeLayout> = HashMap::new();

    for (layer_num, node_ids) in &layers {
        let x = *layer_num as f32 * LAYER_SPACING_X;

        let mut y_cursor = 0.0_f32;
        for &idx in node_ids {
            let node_weight = graph
                .graph
                .node_weight(idx)
                .expect("invariant: idx is a node index from the same graph");
            let col_count = node_weight.columns.len().max(1) as f32;
            let height = NODE_HEADER_HEIGHT + 4.0 + col_count * NODE_ROW_HEIGHT;
            let width = compute_node_width(node_weight);

            nodes.insert(
                idx,
                NodeLayout {
                    x,
                    y: y_cursor,
                    width,
                    height,
                },
            );
            y_cursor += height + NODE_SPACING_Y;
        }
    }

    let edges: Vec<EdgeLayout> = graph
        .graph
        .edge_indices()
        .filter_map(|edge_idx| {
            let (source, target) = graph.graph.edge_endpoints(edge_idx)?;
            let from_layout = nodes.get(&source)?;
            let to_layout = nodes.get(&target)?;

            let from_anchor = (
                from_layout.x + from_layout.width,
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

    let total_width = computed_max_layer as f32 * LAYER_SPACING_X
        + nodes.values().map(|n| n.width).fold(0.0_f32, f32::max);
    let total_height = layers
        .values()
        .map(|ids| {
            ids.iter()
                .map(|&idx| {
                    let node_weight = graph
                        .graph
                        .node_weight(idx)
                        .expect("invariant: idx is from the same graph");
                    let col_count = node_weight.columns.len().max(1) as f32;
                    NODE_HEADER_HEIGHT + 4.0 + col_count * NODE_ROW_HEIGHT
                })
                .sum::<f32>()
                + (ids.len().saturating_sub(1) as f32) * NODE_SPACING_Y
        })
        .max_by(|a, b| a.total_cmp(b))
        .unwrap_or(0.0_f32);

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
    let rows = n.div_ceil(cols);

    // Sort nodes deterministically by table name before laying out.
    let mut sorted_indices: Vec<NodeIndex> = graph.graph.node_indices().collect();
    sorted_indices.sort_by_key(|&idx| {
        graph
            .graph
            .node_weight(idx)
            .map(|n| n.id.name.as_str())
            .unwrap_or("")
    });

    // First pass: compute per-row max heights and collect all widths.
    let mut row_max_heights = vec![0.0_f32; rows];
    let mut all_widths: Vec<f32> = Vec::with_capacity(sorted_indices.len());
    for (i, &idx) in sorted_indices.iter().enumerate() {
        let node_weight = graph
            .graph
            .node_weight(idx)
            .expect("invariant: idx is from node_indices() on the same graph");
        let col_count = node_weight.columns.len().max(1) as f32;
        let height = NODE_HEADER_HEIGHT + 4.0 + col_count * NODE_ROW_HEIGHT;
        let width = compute_node_width(node_weight);
        row_max_heights[i / cols] = row_max_heights[i / cols].max(height);
        all_widths.push(width);
    }

    // Compute cumulative row y offsets.
    let mut row_y_offsets = vec![0.0_f32; rows];
    for r in 1..rows {
        row_y_offsets[r] = row_y_offsets[r - 1] + row_max_heights[r - 1] + NODE_SPACING_Y;
    }

    // Cell width based on maximum node width across all nodes.
    let max_node_width = all_widths.iter().fold(0.0_f32, |acc, &w| acc.max(w));
    let cell_width = max_node_width.max(CELL_WIDTH);

    // Second pass: place nodes using row y offsets.
    let mut nodes: HashMap<NodeIndex, NodeLayout> = HashMap::new();
    for (i, &idx) in sorted_indices.iter().enumerate() {
        let node_weight = graph
            .graph
            .node_weight(idx)
            .expect("invariant: idx is from node_indices() on the same graph");
        let col_count = node_weight.columns.len().max(1) as f32;
        let height = NODE_HEADER_HEIGHT + 4.0 + col_count * NODE_ROW_HEIGHT;
        let width = compute_node_width(node_weight);

        let col = i % cols;
        let row = i / cols;
        let x = col as f32 * cell_width;
        let y = row_y_offsets[row];

        nodes.insert(
            idx,
            NodeLayout {
                x,
                y,
                width,
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
                from_layout.x + from_layout.width,
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

    let total_width = cols as f32 * cell_width;
    let total_height = row_y_offsets
        .last()
        .map(|&y| y + row_max_heights.last().copied().unwrap_or(0.0))
        .unwrap_or(0.0_f32);

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
            let other = layout2
                .nodes
                .get(idx)
                .expect("test: node should exist in layout2");
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

    // ── 9.9: layered layout — chain A→B→C has increasing x ──────────────────

    #[test]
    fn test_layered_layout_layer_positions() {
        // A → B → C
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
            table("c", vec![col("id", "integer", true)], vec![]),
        ];

        let graph = SchemaGraph::build(&tables);
        let layout = compute_layout(&graph);

        // Find node indices by name.
        let idx_by_name = |name: &str| -> NodeIndex {
            graph
                .nodes()
                .find(|(_, n)| n.id.name == name)
                .map(|(i, _)| i)
                .expect("test: node should exist")
        };

        let idx_a = idx_by_name("a");
        let idx_b = idx_by_name("b");
        let idx_c = idx_by_name("c");

        let x_a = layout
            .nodes
            .get(&idx_a)
            .map(|l| l.x)
            .expect("test: idx_a should be in layout");
        let x_b = layout
            .nodes
            .get(&idx_b)
            .map(|l| l.x)
            .expect("test: idx_b should be in layout");
        let x_c = layout
            .nodes
            .get(&idx_c)
            .map(|l| l.x)
            .expect("test: idx_c should be in layout");

        assert!(x_a < x_b, "A (x={x_a}) should be left of B (x={x_b})");
        assert!(x_b < x_c, "B (x={x_b}) should be left of C (x={x_c})");
    }

    // ── 9.10: cyclic graph uses grid layout (multiple x values) ──────────────

    #[test]
    fn test_grid_fallback_chosen_for_cyclic() {
        // A → B → C → A
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

        // Collect x values from all nodes and check uniqueness.
        let mut xs: Vec<_> = layout.nodes.values().map(|l| l.x).collect();
        xs.sort_by(|a, b| a.total_cmp(b));
        xs.dedup();
        // Grid layout should produce at least 2 different x values.
        assert!(
            xs.len() >= 2,
            "Cyclic graph should use grid layout with multiple columns; got x values: {xs:?}"
        );

        // Grid x positions must be multiples of CELL_WIDTH.
        for (_, node_layout) in &layout.nodes {
            let col = (node_layout.x / CELL_WIDTH).round();
            let expected_x = col * CELL_WIDTH;
            assert!(
                (node_layout.x - expected_x).abs() < 0.01,
                "x={} is not a multiple of CELL_WIDTH={}",
                node_layout.x,
                CELL_WIDTH
            );
        }
    }
}
