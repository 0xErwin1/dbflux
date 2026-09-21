use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::f32::consts::PI as PI_F32;

use petgraph::prelude::NodeIndex;
use petgraph::visit::EdgeRef;

use crate::graph::SchemaGraph;

/// Layout algorithm to use when computing positions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum LayoutFormat {
    /// Layered left-to-right. FK source tables on the left, referenced tables on the right.
    /// This is the default.
    #[default]
    LeftRight,
    /// Snowflake: focal table centered, neighbors arranged in a circle around it.
    /// Falls back to LeftRight when there is no focal table (Global mode).
    Snowflake,
    /// Compact grid: tighter spacing, maximizes number of visible tables.
    Compact,
}

// T5: promoted public layout constants.
// NODE_HEADER_PX / NODE_ROW_PX used both by layout math and by the UI renderer
// so both sides agree on anchor positions without duplicating magic numbers.
pub const NODE_HEADER_PX: f32 = 30.0;
pub const NODE_BODY_TOP_PX: f32 = 2.0;
pub const NODE_ROW_PX: f32 = 22.0;
pub const NODE_INDEX_HEADER_PX: f32 = 20.0;
pub const NODE_INDEX_ROW_PX: f32 = 18.0;

/// Horizontal gutter between two layers of the layered layout. Each layer starts
/// after the widest node of the previous one, so a wide table can never overlap
/// its neighbour.
const LAYER_GUTTER_X: f32 = 96.0;
/// Vertical gutter between two stacked nodes.
const NODE_SPACING_Y: f32 = 64.0;
const CELL_WIDTH: f32 = 360.0;
const COMPACT_CELL_SPACING_X: f32 = 48.0;
const COMPACT_CELL_SPACING_Y: f32 = 48.0;

/// Compute the pixel height of a node given its column count and toggle state.
///
/// Formula: NODE_HEADER_PX + NODE_BODY_TOP_PX + cols * NODE_ROW_PX
///          + (when show_indexes and indexes exist) NODE_INDEX_HEADER_PX + n * NODE_INDEX_ROW_PX
fn compute_node_height(node: &crate::graph::TableNode, show_indexes: bool) -> f32 {
    let col_count = node.columns.len().max(1) as f32;
    let base = NODE_HEADER_PX + NODE_BODY_TOP_PX + col_count * NODE_ROW_PX;

    let index_section = if show_indexes && !node.indexes.is_empty() {
        NODE_INDEX_HEADER_PX + node.indexes.len() as f32 * NODE_INDEX_ROW_PX
    } else {
        0.0
    };

    base + index_section
}

/// Compute the width for a node based on its content.
///
/// `show_types` controls whether type labels contribute to width.
/// `show_indexes` controls whether index label widths are considered.
/// Badge cluster contributes a fixed slot, and the type column another one.
/// Result is clamped to [200, 520].
pub fn compute_node_width(
    node: &crate::graph::TableNode,
    show_types: bool,
    show_indexes: bool,
) -> f32 {
    let chars_per_px = 7.0_f32;
    let bold_chars_per_px = 8.0_f32;
    let h_padding = 40.0_f32;
    // Header chrome: 10px each side padding, table icon (~16px), gaps (~8px),
    // and the right-aligned "N cols" label (~7 chars * 7px).
    let header_chrome = 10.0 + 16.0 + 8.0 + 8.0 + 49.0 + 10.0;

    let header_text = match &node.id.schema {
        Some(s) => format!("{}.{}", s, node.id.name),
        None => node.id.name.clone(),
    };
    let header_width = header_text.len() as f32 * bold_chars_per_px + header_chrome;

    // Body width must mirror the actual rendered row layout:
    // [container px(10)] [name flex_1] [gap 8] [badge slot 56] {[gap 8] [type slot 56]} [container px(10)]
    let body_padding = 10.0 + 10.0;
    let badge_slot_w = 56.0;
    let row_chrome = if show_types {
        body_padding + 8.0 + badge_slot_w + 8.0 + 56.0
    } else {
        body_padding + 8.0 + badge_slot_w
    };
    let body_width = node
        .columns
        .iter()
        .map(|col| {
            let name_w = col.name.len() as f32 * chars_per_px;
            name_w + row_chrome
        })
        .fold(0.0_f32, f32::max);

    let index_width = if show_indexes && !node.indexes.is_empty() {
        node.indexes
            .iter()
            .map(|idx| {
                let col_part = idx.columns.join(", ");
                let label = format!("{} ({})", idx.name, col_part);
                label.len() as f32 * chars_per_px + h_padding
            })
            .fold(0.0_f32, f32::max)
    } else {
        0.0
    };

    header_width
        .max(body_width)
        .max(index_width)
        .clamp(200.0, 520.0)
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
///
/// `show_types` and `show_indexes` are threaded into per-node width/height
/// computation so that layout dimensions match the rendered node sizes exactly.
pub fn compute_layout(
    graph: &SchemaGraph,
    format: LayoutFormat,
    focal: Option<(&str, Option<&str>)>,
    show_types: bool,
    show_indexes: bool,
) -> LayoutResult {
    if graph.node_count() == 0 {
        return LayoutResult {
            nodes: HashMap::new(),
            edges: Vec::new(),
            total_width: 0.0,
            total_height: 0.0,
        };
    }

    match format {
        LayoutFormat::LeftRight => layered_layout(graph, show_types, show_indexes),
        LayoutFormat::Compact => compact_layout(graph, show_types, show_indexes),
        LayoutFormat::Snowflake => {
            if let Some((focal_name, focal_schema)) = focal {
                snowflake_layout(graph, focal_name, focal_schema, show_types, show_indexes)
            } else {
                // No focal table: the layered layout places the most connected
                // table first, which is the closest thing to a focal node.
                layered_layout(graph, show_types, show_indexes)
            }
        }
    }
}

/// Assigns every node to a layer by dependency depth, cutting cycles open as it
/// goes.
///
/// Condensing a cycle into one layer kept the diagram from collapsing into a grid,
/// but it also merged every table of the cycle into a single column: a schema whose
/// tables reference each other in a ring drew one tall stripe. This walks the graph
/// from its roots instead, and when it reaches a node whose remaining predecessors
/// are all inside a cycle, it cuts there and keeps going, so a cycle costs one
/// reversed edge rather than a lost layout.
fn assign_layers(graph: &SchemaGraph) -> BTreeMap<usize, Vec<NodeIndex>> {
    let mut pending_incoming: HashMap<NodeIndex, usize> = HashMap::new();
    for idx in graph.graph.node_indices() {
        pending_incoming.insert(
            idx,
            graph
                .graph
                .edges_directed(idx, petgraph::Direction::Incoming)
                .count(),
        );
    }

    // Name order decides every tie, so the same schema always draws the same way.
    let mut remaining: Vec<NodeIndex> = graph.graph.node_indices().collect();
    remaining.sort_by_key(|&idx| table_name(graph, idx));

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
                .neighbors_directed(node, petgraph::Direction::Outgoing)
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
        // first name still standing and treat the edges that closed the cycle as
        // back edges.
        let Some(&cut) = remaining.iter().find(|idx| !placed.contains(*idx)) else {
            break;
        };
        pending_incoming.insert(cut, 0);
        queue.push_back(cut);
    }

    let mut layers: BTreeMap<usize, Vec<NodeIndex>> = BTreeMap::new();
    for (&idx, &depth) in &layer {
        layers.entry(depth).or_default().push(idx);
    }

    for nodes in layers.values_mut() {
        sort_by_table_name(graph, nodes);
    }

    layers
}

/// Reorders the nodes inside each layer to reduce edge crossings.
///
/// This is the barycenter pass of a Sugiyama layout: repeatedly place every node
/// next to the average position of the neighbours it connects to in the layer
/// before it (then after it, sweeping back up). Ties break on the table name, so
/// the same schema always produces the same diagram.
fn reduce_crossings(graph: &SchemaGraph, layers: &mut BTreeMap<usize, Vec<NodeIndex>>) {
    const SWEEPS: usize = 4;
    if layers.len() < 2 {
        return;
    }

    let keys: Vec<usize> = layers.keys().copied().collect();

    for _ in 0..SWEEPS {
        // Swipe top-down, then bottom-up, so both orders inform each other.
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
                            .neighbors_directed(idx, petgraph::Direction::Incoming)
                            .filter_map(|neighbor| positions.get(&neighbor).copied())
                            .collect();
                        let barycenter = if neighbours.is_empty() {
                            f32::MAX
                        } else {
                            neighbours.iter().sum::<f32>() / neighbours.len() as f32
                        };
                        (barycenter, table_name(graph, idx), idx)
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

fn table_name(graph: &SchemaGraph, idx: NodeIndex) -> String {
    graph
        .graph
        .node_weight(idx)
        .map(|node| node.id.name.clone())
        .unwrap_or_default()
}

fn sort_by_table_name(graph: &SchemaGraph, nodes: &mut [NodeIndex]) {
    nodes.sort_by_key(|&idx| table_name(graph, idx));
}

/// Layered layout, left to right.
///
/// Cyclic schemas are the norm — a pair of tables referencing each other is
/// enough — and the previous implementation gave up on them and silently drew a
/// grid instead, which is why choosing "Left-Right" produced a compact-looking
/// diagram. Layers are now assigned on the condensation of the graph, so the
/// cycle is collapsed into one layer and everything else still flows left to
/// right.
fn layered_layout(graph: &SchemaGraph, show_types: bool, show_indexes: bool) -> LayoutResult {
    if graph.node_count() == 0 {
        return LayoutResult {
            nodes: HashMap::new(),
            edges: Vec::new(),
            total_width: 0.0,
            total_height: 0.0,
        };
    }

    let mut layers = assign_layers(graph);
    reduce_crossings(graph, &mut layers);

    let mut nodes: HashMap<NodeIndex, NodeLayout> = HashMap::new();

    // Layers are stacked left to right, each one starting after the widest node of
    // the previous layer. A fixed stride let a wide node overlap the
    // next layer, which is part of why the diagram read as one solid block.
    let mut layer_x = 0.0_f32;
    for node_ids in layers.values() {
        let layer_width = node_ids
            .iter()
            .filter_map(|&idx| graph.graph.node_weight(idx))
            .map(|node_weight| compute_node_width(node_weight, show_types, show_indexes))
            .fold(0.0_f32, f32::max);

        let mut y_cursor = 0.0_f32;
        for &idx in node_ids {
            let Some(node_weight) = graph.graph.node_weight(idx) else {
                continue;
            };
            let height = compute_node_height(node_weight, show_indexes);
            let width = compute_node_width(node_weight, show_types, show_indexes);

            nodes.insert(
                idx,
                NodeLayout {
                    x: layer_x,
                    y: y_cursor,
                    width,
                    height,
                },
            );
            y_cursor += height + NODE_SPACING_Y;
        }

        layer_x += layer_width + LAYER_GUTTER_X;
    }

    let edges = build_edges(graph, &nodes);

    let total_width = if layers.is_empty() {
        0.0_f32
    } else {
        (layer_x - LAYER_GUTTER_X).max(0.0_f32)
    };
    let total_height = layers
        .values()
        .map(|ids| {
            ids.iter()
                .filter_map(|&idx| {
                    graph
                        .graph
                        .node_weight(idx)
                        .map(|node_weight| compute_node_height(node_weight, show_indexes))
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

/// Compact grid layout with tighter spacing than grid_layout.
fn compact_layout(graph: &SchemaGraph, show_types: bool, show_indexes: bool) -> LayoutResult {
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
        let Some(node_weight) = graph.graph.node_weight(idx) else {
            continue;
        };
        let height = compute_node_height(node_weight, show_indexes);
        let width = compute_node_width(node_weight, show_types, show_indexes);
        if let Some(slot) = row_max_heights.get_mut(i / cols) {
            *slot = slot.max(height);
        }
        all_widths.push(width);
    }

    // Compute cumulative row y offsets.
    let mut row_y_offsets = vec![0.0_f32; rows];
    for r in 1..rows {
        if let (Some(&prev_offset), Some(&prev_height)) =
            (row_y_offsets.get(r - 1), row_max_heights.get(r - 1))
            && let Some(slot) = row_y_offsets.get_mut(r)
        {
            *slot = prev_offset + prev_height + COMPACT_CELL_SPACING_Y;
        }
    }

    // Cell width based on maximum node width across all nodes.
    let max_node_width = all_widths.iter().fold(0.0_f32, |acc, &w| acc.max(w));
    let cell_width = max_node_width.max(CELL_WIDTH);

    // Second pass: place nodes using row y offsets.
    let mut nodes: HashMap<NodeIndex, NodeLayout> = HashMap::new();
    for (i, &idx) in sorted_indices.iter().enumerate() {
        let Some(node_weight) = graph.graph.node_weight(idx) else {
            continue;
        };
        let height = compute_node_height(node_weight, show_indexes);
        let width = compute_node_width(node_weight, show_types, show_indexes);

        let col = i % cols;
        let row = i / cols;
        let x = col as f32 * (cell_width + COMPACT_CELL_SPACING_X);
        let Some(&y) = row_y_offsets.get(row) else {
            continue;
        };

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

    let edges = build_edges(graph, &nodes);
    let total_width = cols as f32 * (cell_width + COMPACT_CELL_SPACING_X);
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

/// Snowflake layout: focal table at center, neighbors arranged in a circle.
fn snowflake_layout(
    graph: &SchemaGraph,
    focal_name: &str,
    focal_schema: Option<&str>,
    show_types: bool,
    show_indexes: bool,
) -> LayoutResult {
    // Find focal node index by name + schema.
    let focal_id = crate::graph::TableNodeId {
        schema: focal_schema.map(String::from),
        name: focal_name.to_owned(),
    };

    let Some(&focal_idx) = graph.node_index_by_id.get(&focal_id) else {
        // Focal table not in the graph: the layered layout is still the best
        // answer, and it no longer refuses cyclic schemas.
        return layered_layout(graph, show_types, show_indexes);
    };

    // Collect all neighbor indices (direct FK connections in either direction).
    let mut neighbor_indices: Vec<NodeIndex> = Vec::new();

    for edge in graph
        .graph
        .edges_directed(focal_idx, petgraph::Direction::Outgoing)
    {
        neighbor_indices.push(edge.target());
    }
    for edge in graph
        .graph
        .edges_directed(focal_idx, petgraph::Direction::Incoming)
    {
        neighbor_indices.push(edge.source());
    }

    // Sort neighbors by name for determinism.
    neighbor_indices.sort_by_key(|&idx| {
        graph
            .graph
            .node_weight(idx)
            .map(|n| n.id.name.as_str())
            .unwrap_or("")
    });

    let neighbor_count = neighbor_indices.len();

    // Place focal node at center: use a fixed center for now.
    let cx = 500.0_f32;
    let cy = 400.0_f32;

    let Some(focal_node_weight) = graph.graph.node_weight(focal_idx) else {
        return LayoutResult {
            nodes: HashMap::new(),
            edges: Vec::new(),
            total_width: 0.0,
            total_height: 0.0,
        };
    };
    let focal_width = compute_node_width(focal_node_weight, show_types, show_indexes);
    let focal_height = compute_node_height(focal_node_weight, show_indexes);

    let mut nodes: HashMap<NodeIndex, NodeLayout> = HashMap::new();

    // Focal node at center.
    nodes.insert(
        focal_idx,
        NodeLayout {
            x: cx - focal_width / 2.0,
            y: cy - focal_height / 2.0,
            width: focal_width,
            height: focal_height,
        },
    );

    // Place neighbors on a circle around the focal node.
    let radius = 300.0_f32.max(neighbor_count as f32 * 80.0);
    let angle_step = if neighbor_count > 0 {
        2.0 * PI_F32 / neighbor_count as f32
    } else {
        0.0
    };

    for (i, &neighbor_idx) in neighbor_indices.iter().enumerate() {
        let angle = angle_step * i as f32 - PI_F32 / 2.0; // Start from top
        let Some(node_weight) = graph.graph.node_weight(neighbor_idx) else {
            continue;
        };
        let width = compute_node_width(node_weight, show_types, show_indexes);
        let height = compute_node_height(node_weight, show_indexes);

        let node_x = cx + radius * angle.cos() - width / 2.0;
        let node_y = cy + radius * angle.sin() - height / 2.0;

        nodes.insert(
            neighbor_idx,
            NodeLayout {
                x: node_x,
                y: node_y,
                width,
                height,
            },
        );
    }

    // Nodes not in the immediate neighborhood use grid_layout, placed below.
    // Collect nodes that aren't the focal or a neighbor.
    let mut other_indices: Vec<NodeIndex> = graph
        .graph
        .node_indices()
        .filter(|&idx| idx != focal_idx && !neighbor_indices.contains(&idx))
        .collect();
    other_indices.sort_by_key(|&idx| {
        graph
            .graph
            .node_weight(idx)
            .map(|n| n.id.name.as_str())
            .unwrap_or("")
    });

    // Place other nodes in a grid below the snowflake.
    // Use a fixed y offset for the grid start.
    let grid_start_y = cy + radius + 150.0;
    let grid_cols = 4;
    for (i, &idx) in other_indices.iter().enumerate() {
        let Some(node_weight) = graph.graph.node_weight(idx) else {
            continue;
        };
        let width = compute_node_width(node_weight, show_types, show_indexes);
        let height = compute_node_height(node_weight, show_indexes);

        let col = i % grid_cols;
        let row = i / grid_cols;
        let x = col as f32 * (CELL_WIDTH + COMPACT_CELL_SPACING_X);
        let y = grid_start_y + row as f32 * (height + COMPACT_CELL_SPACING_Y);

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

    let edges = build_edges(graph, &nodes);

    // Compute bounding box of all nodes + 100px margin.
    let all_nodes: Vec<&NodeLayout> = nodes.values().collect();
    let min_x = all_nodes.iter().map(|n| n.x).fold(0.0_f32, f32::min);
    let min_y = all_nodes.iter().map(|n| n.y).fold(0.0_f32, f32::min);
    let max_x = all_nodes
        .iter()
        .map(|n| n.x + n.width)
        .fold(0.0_f32, f32::max);
    let max_y = all_nodes
        .iter()
        .map(|n| n.y + n.height)
        .fold(0.0_f32, f32::max);
    let total_width = max_x - min_x + 100.0;
    let total_height = max_y - min_y + 100.0;

    LayoutResult {
        nodes,
        edges,
        total_width,
        total_height,
    }
}

/// Compute edge layouts for all edges in the graph.
fn build_edges(graph: &SchemaGraph, nodes: &HashMap<NodeIndex, NodeLayout>) -> Vec<EdgeLayout> {
    graph
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
        .collect()
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
            storage_hints: None,
            presentation: dbflux_core::CollectionPresentation::default(),
            child_items: None,
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

    // ── T8: compute_node_width matrix (show_types, show_indexes) ────────────

    fn fixture_node_with_index() -> (SchemaGraph, petgraph::prelude::NodeIndex) {
        use dbflux_core::{IndexData, IndexInfo};

        let mut t = table(
            "orders",
            vec![
                col("id", "integer", true),
                col("user_id", "integer", false),
                col("status", "text", false),
            ],
            vec![],
        );
        t.indexes = Some(IndexData::Relational(vec![IndexInfo {
            name: "orders_user_idx".to_owned(),
            columns: vec!["user_id".to_owned()],
            is_unique: false,
            is_primary: false,
        }]));

        let g = SchemaGraph::build(&[t]);
        let idx = g.graph.node_indices().next().expect("one node");
        (g, idx)
    }

    #[test]
    fn test_compute_node_width_matrix() {
        let (graph, idx) = fixture_node_with_index();
        let node = graph.graph.node_weight(idx).unwrap();

        let tt = compute_node_width(node, true, true);
        let tf = compute_node_width(node, true, false);
        let ft = compute_node_width(node, false, true);
        let ff = compute_node_width(node, false, false);

        // All results must be within the clamp range [200, 520]
        for (label, w) in [("TT", tt), ("TF", tf), ("FT", ft), ("FF", ff)] {
            assert!(
                (200.0..=520.0).contains(&w),
                "compute_node_width({label}) = {w} outside [200, 520]"
            );
        }
    }

    // ── T9: compute_layout height invariants ────────────────────────────────

    #[test]
    fn test_compute_layout_height_invariants() {
        let tables = vec![table(
            "users",
            vec![col("id", "integer", true), col("name", "text", false)],
            vec![],
        )];
        let graph = SchemaGraph::build(&tables);
        let idx = graph.graph.node_indices().next().unwrap();

        // With and without show_types=false — node width should still be clamped [180..400].
        let layout_with = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);
        let layout_without = compute_layout(&graph, LayoutFormat::LeftRight, None, false, false);

        let w_with = layout_with.nodes.get(&idx).map(|n| n.width).unwrap();
        let w_without = layout_without.nodes.get(&idx).map(|n| n.width).unwrap();

        assert!(
            (200.0..=520.0).contains(&w_with),
            "width with types out of range: {w_with}"
        );
        assert!(
            (200.0..=520.0).contains(&w_without),
            "width without types out of range: {w_without}"
        );

        // Both layouts should have the same node count.
        assert_eq!(
            layout_with.nodes.len(),
            layout_without.nodes.len(),
            "node count must be equal regardless of toggle"
        );
    }

    // ── T10: compute_layout index-section delta ──────────────────────────────

    #[test]
    fn test_compute_layout_index_section_delta() {
        use dbflux_core::{IndexData, IndexInfo};

        let mut t = table(
            "items",
            vec![col("id", "integer", true), col("name", "text", false)],
            vec![],
        );
        t.indexes = Some(IndexData::Relational(vec![
            IndexInfo {
                name: "items_name_idx".to_owned(),
                columns: vec!["name".to_owned()],
                is_unique: false,
                is_primary: false,
            },
            IndexInfo {
                name: "items_id_pkey".to_owned(),
                columns: vec!["id".to_owned()],
                is_unique: true,
                is_primary: true,
            },
        ]));

        let graph = SchemaGraph::build(&[t]);
        let idx = graph.graph.node_indices().next().unwrap();

        let layout_no_idx = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);
        let layout_with_idx = compute_layout(&graph, LayoutFormat::LeftRight, None, true, true);

        let h_no = layout_no_idx.nodes.get(&idx).map(|n| n.height).unwrap();
        let h_with = layout_with_idx.nodes.get(&idx).map(|n| n.height).unwrap();

        // Expected delta: NODE_INDEX_HEADER_PX + n_indexes * NODE_INDEX_ROW_PX
        let n_indexes = 2_f32;
        let expected_delta = NODE_INDEX_HEADER_PX + n_indexes * NODE_INDEX_ROW_PX;

        let actual_delta = h_with - h_no;
        assert!(
            (actual_delta - expected_delta).abs() < 0.1,
            "index section delta: expected {expected_delta}, got {actual_delta}"
        );
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
        let layout1 = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);
        let layout2 = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);

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
        let layout = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);

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
        let layout = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);

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
    fn test_cyclic_schema_keeps_dependency_depths() {
        // a → b → c → b: b and c reference each other, which used to push the whole
        // diagram into a grid and silently ignore the Left-Right choice.
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
                vec![fk("fk_c_b", vec!["id"], "b", vec!["id"])],
            ),
        ];

        let graph = SchemaGraph::build(&tables);
        let layout = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);

        let x_of = |name: &str| {
            graph
                .node_index_by_id
                .get(&crate::graph::TableNodeId {
                    schema: None,
                    name: name.to_owned(),
                })
                .and_then(|idx| layout.nodes.get(idx))
                .map(|node| node.x)
                .unwrap_or_default()
        };

        // a is outside the cycle and drives it, so it takes the first layer.
        assert!(
            x_of("a") < x_of("b"),
            "a must sit left of the table it references, got a={} b={}",
            x_of("a"),
            x_of("b")
        );
        // b and c reference each other: the cycle is cut open, so each keeps its own
        // depth instead of collapsing into one column.
        assert!(
            x_of("b") < x_of("c"),
            "a cycle is cut, not flattened, got b={} c={}",
            x_of("b"),
            x_of("c")
        );
    }

    #[test]
    fn test_layered_layout_orders_layers_to_avoid_crossings() {
        // Two tables pointing at two tables, crossed: alphabetical order draws the
        // edges across each other, the barycenter pass flips the second layer.
        let tables = vec![
            table(
                "left_a",
                vec![col("id", "integer", true), col("b_id", "integer", false)],
                vec![fk("fk_left_a_right_b", vec!["b_id"], "right_b", vec!["id"])],
            ),
            table(
                "left_c",
                vec![col("id", "integer", true), col("b_id", "integer", false)],
                vec![fk("fk_left_c_right_a", vec!["b_id"], "right_a", vec!["id"])],
            ),
            table("right_a", vec![col("id", "integer", true)], vec![]),
            table("right_b", vec![col("id", "integer", true)], vec![]),
        ];

        let graph = SchemaGraph::build(&tables);
        let layout = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);

        let y_of = |name: &str| {
            graph
                .node_index_by_id
                .get(&crate::graph::TableNodeId {
                    schema: None,
                    name: name.to_owned(),
                })
                .and_then(|idx| layout.nodes.get(idx))
                .map(|node| node.y)
                .unwrap_or_default()
        };

        assert!(
            y_of("right_b") < y_of("right_a"),
            "the referenced table must line up with the table that points at it, got right_b={} right_a={}",
            y_of("right_b"),
            y_of("right_a")
        );
    }
}
