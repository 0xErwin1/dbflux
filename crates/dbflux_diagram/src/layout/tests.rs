use super::*;

fn node(key: &str, width: f32, height: f32) -> LayoutNode {
    LayoutNode {
        width,
        height,
        sort_key: key.to_owned(),
    }
}

fn edge(from: usize, to: usize) -> LayoutEdge {
    LayoutEdge {
        from,
        to,
        weight: 1.0,
    }
}

/// root -> (left, right), left -> leaf. Node heights differ, as plan nodes with a
/// different number of rows do.
fn small_tree() -> (Vec<LayoutNode>, Vec<LayoutEdge>) {
    (
        vec![
            node("root", 100.0, 40.0),
            node("left", 120.0, 80.0),
            node("right", 90.0, 30.0),
            node("leaf", 60.0, 50.0),
        ],
        vec![edge(0, 1), edge(0, 2), edge(1, 3)],
    )
}

#[test]
fn a_tree_is_laid_out_layer_by_layer_from_the_root() {
    let (nodes, edges) = small_tree();

    let layout = layered_left_right(&nodes, &edges);

    // Layer 0 starts at x=0 and is 100 wide, layer 1 after the 96 gutter, 120 wide,
    // layer 2 after another gutter.
    assert_eq!(
        layout.nodes,
        vec![
            PlacedNode {
                x: 0.0,
                y: 0.0,
                width: 100.0,
                height: 40.0
            },
            PlacedNode {
                x: 196.0,
                y: 0.0,
                width: 120.0,
                height: 80.0
            },
            PlacedNode {
                x: 196.0,
                y: 144.0,
                width: 90.0,
                height: 30.0
            },
            PlacedNode {
                x: 412.0,
                y: 0.0,
                width: 60.0,
                height: 50.0
            },
        ]
    );
    assert_eq!(layout.total_width, 472.0);
    // The tallest layer is left (80) + gutter (64) + right (30).
    assert_eq!(layout.total_height, 174.0);
}

#[test]
fn edges_anchor_on_the_facing_sides_and_keep_input_order() {
    let (nodes, edges) = small_tree();

    let layout = layered_left_right(&nodes, &edges);

    assert_eq!(layout.edges.len(), 3);
    assert_eq!((layout.edges[0].from, layout.edges[0].to), (0, 1));
    assert_eq!(layout.edges[0].from_anchor, (100.0, 20.0));
    assert_eq!(layout.edges[0].to_anchor, (196.0, 40.0));
    assert_eq!((layout.edges[2].from, layout.edges[2].to), (1, 3));
    assert_eq!(layout.edges[2].from_anchor, (316.0, 40.0));
    assert_eq!(layout.edges[2].to_anchor, (412.0, 25.0));
}

#[test]
fn edge_weight_is_handed_back_on_the_placed_edge() {
    let nodes = vec![
        node("a", 10.0, 10.0),
        node("b", 10.0, 10.0),
        node("c", 10.0, 10.0),
    ];
    let edges = vec![
        LayoutEdge {
            from: 0,
            to: 1,
            weight: 0.0,
        },
        LayoutEdge {
            from: 0,
            to: 2,
            weight: 123_456.0,
        },
    ];

    let layered = layered_left_right(&nodes, &edges);
    let compact = compact_grid(&nodes, &edges);
    let around = radial(&nodes, &edges, 0);

    for layout in [layered, compact, around] {
        let weights: Vec<f32> = layout.edges.iter().map(|placed| placed.weight).collect();
        assert_eq!(weights, vec![0.0, 123_456.0]);
    }
}

#[test]
fn a_diamond_puts_the_join_one_layer_after_its_deepest_input() {
    // a -> b -> d and a -> c -> e -> d: d depends on the longer path.
    let nodes = vec![
        node("a", 50.0, 20.0),
        node("b", 50.0, 20.0),
        node("c", 50.0, 20.0),
        node("d", 50.0, 20.0),
        node("e", 50.0, 20.0),
    ];
    let edges = vec![edge(0, 1), edge(0, 2), edge(1, 3), edge(2, 4), edge(4, 3)];

    let layout = layered_left_right(&nodes, &edges);

    let x: Vec<f32> = layout.nodes.iter().map(|placed| placed.x).collect();
    let stride = 50.0 + LAYER_GUTTER_X;
    assert_eq!(x, vec![0.0, stride, stride, 3.0 * stride, 2.0 * stride]);
}

#[test]
fn layers_are_ordered_to_avoid_crossing_edges() {
    // l1 points at r2 and l2 at r1: alphabetical order would cross the edges.
    let nodes = vec![
        node("l1", 50.0, 20.0),
        node("l2", 50.0, 20.0),
        node("r1", 50.0, 20.0),
        node("r2", 50.0, 20.0),
    ];
    let edges = vec![edge(0, 3), edge(1, 2)];

    let layout = layered_left_right(&nodes, &edges);

    assert!(layout.nodes[3].y < layout.nodes[2].y);
}

#[test]
fn a_cycle_is_cut_open_instead_of_collapsing_into_one_layer() {
    let nodes = vec![
        node("a", 50.0, 20.0),
        node("b", 50.0, 20.0),
        node("c", 50.0, 20.0),
    ];
    let edges = vec![edge(0, 1), edge(1, 2), edge(2, 1)];

    let layout = layered_left_right(&nodes, &edges);

    assert!(layout.nodes[0].x < layout.nodes[1].x);
    assert!(layout.nodes[1].x < layout.nodes[2].x);
}

#[test]
fn the_same_input_always_lays_out_the_same_way() {
    let (nodes, edges) = small_tree();

    assert_eq!(
        layered_left_right(&nodes, &edges),
        layered_left_right(&nodes, &edges)
    );
}

#[test]
fn an_empty_graph_has_an_empty_layout() {
    for layout in [
        layered_left_right(&[], &[]),
        compact_grid(&[], &[]),
        radial(&[], &[], 0),
    ] {
        assert!(layout.nodes.is_empty());
        assert!(layout.edges.is_empty());
        assert_eq!((layout.total_width, layout.total_height), (0.0, 0.0));
    }
}

#[test]
fn an_edge_to_a_missing_node_is_dropped() {
    let nodes = vec![node("a", 10.0, 10.0)];

    let layout = layered_left_right(&nodes, &[edge(0, 7)]);

    assert_eq!(layout.nodes.len(), 1);
    assert!(layout.edges.is_empty());
}

#[test]
fn the_compact_grid_packs_nodes_by_key_into_a_square() {
    let nodes = vec![
        node("d", 100.0, 20.0),
        node("a", 100.0, 40.0),
        node("c", 100.0, 20.0),
        node("b", 100.0, 20.0),
    ];

    let layout = compact_grid(&nodes, &[]);

    // Two columns; a, b on the first row, c, d on the second. The cell is 360 wide.
    let cell = 360.0 + COMPACT_CELL_SPACING_X;
    assert_eq!((layout.nodes[1].x, layout.nodes[1].y), (0.0, 0.0));
    assert_eq!((layout.nodes[3].x, layout.nodes[3].y), (cell, 0.0));
    assert_eq!((layout.nodes[2].x, layout.nodes[2].y), (0.0, 40.0 + 48.0));
    assert_eq!((layout.nodes[0].x, layout.nodes[0].y), (cell, 40.0 + 48.0));
    assert_eq!(layout.total_width, 2.0 * cell);
    assert_eq!(layout.total_height, 40.0 + 48.0 + 20.0);
}

#[test]
fn the_radial_layout_centres_the_focal_node() {
    let nodes = vec![
        node("hub", 100.0, 40.0),
        node("a", 50.0, 20.0),
        node("b", 50.0, 20.0),
        node("far", 50.0, 20.0),
    ];
    let edges = vec![edge(0, 1), edge(2, 0)];

    let layout = radial(&nodes, &edges, 0);

    assert_eq!((layout.nodes[0].x, layout.nodes[0].y), (450.0, 380.0));
    // The first neighbour starts at the top of the circle.
    let first = layout.nodes[1];
    assert_eq!(first.x + first.width / 2.0, 500.0);
    assert_eq!(first.y + first.height / 2.0, 100.0);
    // A node outside the neighbourhood goes in the grid below the circle.
    assert_eq!((layout.nodes[3].x, layout.nodes[3].y), (0.0, 850.0));
}

#[test]
fn the_radial_layout_without_that_focal_node_is_the_layered_one() {
    let (nodes, edges) = small_tree();

    assert_eq!(
        radial(&nodes, &edges, 99),
        layered_left_right(&nodes, &edges)
    );
}

#[test]
fn equal_sort_keys_keep_input_order_in_a_layer() {
    let nodes = vec![
        node("orders", 210.0, 50.0),
        node("orders", 220.0, 50.0),
        node("orders", 230.0, 50.0),
    ];

    for _ in 0..16 {
        let placed = layered_left_right(&nodes, &[]);
        assert!(placed.nodes[0].y < placed.nodes[1].y);
        assert!(placed.nodes[1].y < placed.nodes[2].y);
    }
}

#[test]
fn parallel_edges_to_one_neighbour_take_one_radial_slot() {
    let nodes = vec![
        node("hub", 100.0, 40.0),
        node("a", 50.0, 20.0),
        node("b", 50.0, 20.0),
    ];
    let single = radial(&nodes, &[edge(0, 1), edge(0, 2)], 0);
    let parallel = radial(&nodes, &[edge(0, 1), edge(0, 1), edge(0, 2)], 0);

    assert_eq!(parallel.nodes, single.nodes);
    assert_eq!(parallel.edges.len(), 3);
}

#[test]
fn a_self_edge_on_the_focal_node_leaves_it_at_the_centre() {
    let nodes = vec![node("hub", 100.0, 40.0), node("a", 50.0, 20.0)];
    let with_loop = radial(&nodes, &[edge(0, 1), edge(0, 0)], 0);
    let without = radial(&nodes, &[edge(0, 1)], 0);

    assert_eq!((with_loop.nodes[0].x, with_loop.nodes[0].y), (450.0, 380.0));
    assert_eq!(with_loop.nodes, without.nodes);
    assert_eq!(with_loop.edges.len(), 2);
}

fn tree_node(height: f32) -> LayoutNode {
    node("n", 100.0, height)
}

/// 0 -> (1, 2); 2 -> (3, 4); 4 -> 5: the shape of a join with a hash side.
fn join_tree() -> (Vec<LayoutNode>, Vec<Option<usize>>) {
    (
        (0..6).map(|_| tree_node(60.0)).collect(),
        vec![None, Some(0), Some(0), Some(2), Some(2), Some(4)],
    )
}

#[test]
fn a_tree_layout_puts_each_child_exactly_one_column_left_of_its_parent() {
    let (nodes, parents) = join_tree();
    let layout = layered_tree_right(&nodes, &parents);

    for (child, parent) in parents.iter().enumerate() {
        let Some(parent) = parent else { continue };
        let (c, p) = (layout.nodes[child], layout.nodes[*parent]);
        assert!(c.x + c.width < p.x, "node {child} is left of {parent}");
        assert_eq!(layout.edges.iter().filter(|e| e.from == child).count(), 1);
    }
    // Siblings share a column, and the root is the rightmost.
    assert_eq!(layout.nodes[1].x, layout.nodes[2].x);
    assert_eq!(layout.nodes[3].x, layout.nodes[4].x);
    assert!(layout.nodes.iter().all(|n| n.x <= layout.nodes[0].x));
    // A leaf at depth 1 does not sit in the first column.
    assert!(layout.nodes[5].x < layout.nodes[3].x);
    assert!(layout.nodes[1].x > layout.nodes[5].x);
}

#[test]
fn no_tree_edge_skips_a_column() {
    let (nodes, parents) = join_tree();
    let layout = layered_tree_right(&nodes, &parents);
    let mut columns: Vec<f32> = layout.nodes.iter().map(|n| n.x).collect();
    columns.sort_by(f32::total_cmp);
    columns.dedup();

    for edge in &layout.edges {
        let from = columns.iter().position(|x| *x == layout.nodes[edge.from].x);
        let to = columns.iter().position(|x| *x == layout.nodes[edge.to].x);
        assert_eq!(from.map(|c| c + 1), to, "{edge:?}");
    }
}

#[test]
fn a_tree_parent_is_centred_on_its_first_and_last_child() {
    let (nodes, parents) = join_tree();
    let layout = layered_tree_right(&nodes, &parents);
    let centre = |i: usize| layout.nodes[i].y + layout.nodes[i].height / 2.0;

    assert!((centre(0) - (centre(1) + centre(2)) / 2.0).abs() < 0.01);
    assert!((centre(2) - (centre(3) + centre(4)) / 2.0).abs() < 0.01);
    assert!(
        (centre(4) - centre(5)).abs() < 0.01,
        "an only child is level"
    );
}

#[test]
fn tree_nodes_in_one_column_never_overlap_even_with_different_heights() {
    let nodes = vec![
        tree_node(200.0),
        tree_node(40.0),
        tree_node(160.0),
        tree_node(90.0),
        tree_node(30.0),
    ];
    let parents = vec![None, Some(0), Some(0), Some(1), Some(2)];
    let layout = layered_tree_right(&nodes, &parents);

    for a in 0..nodes.len() {
        for b in (a + 1)..nodes.len() {
            let (p, q) = (layout.nodes[a], layout.nodes[b]);
            if (p.x - q.x).abs() < 0.01 {
                assert!(
                    p.y + p.height <= q.y || q.y + q.height <= p.y,
                    "{a} and {b} overlap: {p:?} {q:?}"
                );
            }
        }
    }
}

#[test]
fn a_deep_tree_chain_runs_one_column_per_level() {
    let nodes: Vec<LayoutNode> = (0..9).map(|_| tree_node(50.0)).collect();
    let parents: Vec<Option<usize>> = (0..9_usize).map(|i| i.checked_sub(1)).collect();
    let layout = layered_tree_right(&nodes, &parents);

    for i in 1..9 {
        assert!(layout.nodes[i].x < layout.nodes[i - 1].x);
        assert_eq!(layout.nodes[i].y, layout.nodes[0].y, "a chain is one row");
    }
    assert_eq!(layout.nodes[0].x + 100.0, layout.total_width);
}

#[test]
fn an_empty_tree_is_an_empty_layout() {
    assert_eq!(layered_tree_right(&[], &[]), DiagramLayout::empty());
}
