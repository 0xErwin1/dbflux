use std::collections::HashMap;

use dbflux_diagram::{DiagramLayout, LayoutEdge, LayoutNode};
use petgraph::prelude::NodeIndex;

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
pub const NODE_BODY_TOP_PX: f32 = 0.0;
pub const NODE_ROW_PX: f32 = 24.0;
pub const NODE_INDEX_HEADER_PX: f32 = 20.0;
pub const NODE_INDEX_ROW_PX: f32 = 18.0;

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
/// The placement itself is the domain-free layout of `dbflux_diagram`; this
/// function only sizes the tables and maps the result back to graph indices.
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

    let (nodes, edges) = diagram_input(graph, show_types, show_indexes);

    let placed = match format {
        LayoutFormat::LeftRight => dbflux_diagram::layered_left_right(&nodes, &edges),
        LayoutFormat::Compact => dbflux_diagram::compact_grid(&nodes, &edges),
        // No focal table, or one that is not in the graph: the layered layout
        // places the most connected table first, which is the closest thing to a
        // focal node.
        LayoutFormat::Snowflake => match focal_index(graph, focal) {
            Some(focal_idx) => dbflux_diagram::radial(&nodes, &edges, focal_idx.index()),
            None => dbflux_diagram::layered_left_right(&nodes, &edges),
        },
    };

    into_result(graph, placed)
}

/// The tables as sized boxes and the foreign keys as edges, in graph order, so
/// that position `i` in the result is `NodeIndex::new(i)`.
fn diagram_input(
    graph: &SchemaGraph,
    show_types: bool,
    show_indexes: bool,
) -> (Vec<LayoutNode>, Vec<LayoutEdge>) {
    let nodes = graph
        .nodes()
        .map(|(_, node)| LayoutNode {
            width: compute_node_width(node, show_types, show_indexes),
            height: compute_node_height(node, show_indexes),
            sort_key: node.id.name.clone(),
        })
        .collect();

    let edges = graph
        .edge_indices()
        .filter_map(|edge_idx| {
            let (source, target) = graph.edge_endpoints(edge_idx)?;
            Some(LayoutEdge {
                from: source.index(),
                to: target.index(),
                weight: 1.0,
            })
        })
        .collect();

    (nodes, edges)
}

fn focal_index(graph: &SchemaGraph, focal: Option<(&str, Option<&str>)>) -> Option<NodeIndex> {
    let (name, schema) = focal?;
    let focal_id = crate::graph::TableNodeId {
        schema: schema.map(String::from),
        name: name.to_owned(),
    };
    graph.node_index_by_id.get(&focal_id).copied()
}

fn into_result(graph: &SchemaGraph, placed: DiagramLayout) -> LayoutResult {
    let nodes = graph
        .nodes()
        .filter_map(|(idx, _)| {
            let node = placed.nodes.get(idx.index())?;
            Some((
                idx,
                NodeLayout {
                    x: node.x,
                    y: node.y,
                    width: node.width,
                    height: node.height,
                },
            ))
        })
        .collect();

    let edges = placed
        .edges
        .iter()
        .map(|edge| EdgeLayout {
            from_node: NodeIndex::new(edge.from),
            to_node: NodeIndex::new(edge.to),
            from_anchor: edge.from_anchor,
            to_anchor: edge.to_anchor,
        })
        .collect();

    LayoutResult {
        nodes,
        edges,
        total_width: placed.total_width,
        total_height: placed.total_height,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
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
            pseudo_columns: Box::default(),
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
