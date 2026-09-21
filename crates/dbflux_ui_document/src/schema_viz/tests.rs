use super::routing::{NodeBounds, RoutePoint, route_foreign_key};
/// Tests for SchemaVizDocument logic (pure unit tests — no GPUI harness required).
///
/// Imports are kept minimal (no `super::*`) to avoid triggering GPUI proc-macro
/// expansion across the full parent module during test compilation.
use super::{SchemaVizDocument, pixel_aligned_diagram_pan};
use dbflux_core::{ColumnInfo, ForeignKeyInfo, SchemaForeignKeyInfo, TableInfo};
use dbflux_schema_viz::{
    graph::SchemaGraph,
    layout::{LayoutFormat, compute_layout},
};
use gpui::{Pixels, Point, px};

fn route_midpoint(route: &super::routing::OrthogonalRoute) -> RoutePoint {
    let mut total = 0.0_f32;
    for pair in route.points.windows(2) {
        if let [a, b] = pair {
            total += ((b.x - a.x).powi(2) + (b.y - a.y).powi(2)).sqrt();
        }
    }

    let mut remaining = total / 2.0;
    for pair in route.points.windows(2) {
        if let [a, b] = pair {
            let length = ((b.x - a.x).powi(2) + (b.y - a.y).powi(2)).sqrt();
            if length >= remaining && length > 0.0 {
                let ratio = remaining / length;
                return RoutePoint {
                    x: a.x + (b.x - a.x) * ratio,
                    y: a.y + (b.y - a.y) * ratio,
                };
            }
            remaining -= length;
        }
    }

    route.end()
}

/// Every segment of an orthogonal route shares an axis with the next point.
fn assert_axis_aligned(route: &super::routing::OrthogonalRoute) {
    for pair in route.points.windows(2) {
        if let [a, b] = pair {
            assert!(
                (a.x - b.x).abs() < 0.01 || (a.y - b.y).abs() < 0.01,
                "segment {a:?} -> {b:?} is diagonal; routes must be axis-aligned"
            );
        }
    }
}

/// x of the vertical leg, i.e. the middle vertices of a four-point route.
fn lane_x(route: &super::routing::OrthogonalRoute) -> f32 {
    route.points.get(1).map(|p| p.x).unwrap_or_default()
}

fn make_table(name: &str, n_cols: usize) -> TableInfo {
    TableInfo {
        name: name.to_owned(),
        schema: None,
        columns: Some(
            (0..n_cols)
                .map(|i| ColumnInfo {
                    name: format!("col_{}", i),
                    type_name: "text".to_owned(),
                    nullable: i > 0,
                    is_primary_key: i == 0,
                    default_value: None,
                    enum_values: None,
                })
                .collect(),
        ),
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
        presentation: dbflux_core::CollectionPresentation::default(),
        child_items: None,
        storage_hints: None,
    }
}

fn make_fk_table(name: &str, ref_table: &str) -> TableInfo {
    TableInfo {
        name: name.to_owned(),
        schema: None,
        columns: Some(vec![
            ColumnInfo {
                name: "id".to_owned(),
                type_name: "integer".to_owned(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                enum_values: None,
            },
            ColumnInfo {
                name: format!("{}_id", ref_table),
                type_name: "integer".to_owned(),
                nullable: true,
                is_primary_key: false,
                default_value: None,
                enum_values: None,
            },
        ]),
        indexes: None,
        foreign_keys: Some(vec![ForeignKeyInfo {
            name: format!("fk_{}_{}", name, ref_table),
            columns: vec![format!("{}_id", ref_table)],
            referenced_table: ref_table.to_owned(),
            referenced_schema: None,
            referenced_columns: vec!["id".to_owned()],
            on_delete: None,
            on_update: None,
        }]),
        constraints: None,
        sample_fields: None,
        presentation: dbflux_core::CollectionPresentation::default(),
        child_items: None,
        storage_hints: None,
    }
}

fn make_schema_fk(
    table: &str,
    referenced_table: &str,
    referenced_schema: Option<&str>,
) -> SchemaForeignKeyInfo {
    SchemaForeignKeyInfo {
        name: format!("fk_{}_{}", table, referenced_table),
        table_name: table.to_owned(),
        columns: vec![format!("{}_id", referenced_table)],
        referenced_schema: referenced_schema.map(str::to_owned),
        referenced_table: referenced_table.to_owned(),
        referenced_columns: vec!["id".to_owned()],
        on_delete: None,
        on_update: None,
    }
}

#[test]
fn inbound_neighbours_include_tables_that_only_reference_the_focal_one() {
    // The focal table's own foreign keys never name the tables that point at it:
    // those are declared by the children, which is what the schema's key list has.
    let foreign_keys = vec![
        make_schema_fk("comments", "documents", None),
        make_schema_fk("attachments", "documents", None),
        make_schema_fk("document_revisions", "documents", None),
        make_schema_fk("documents", "folders", None),
        make_schema_fk("documents", "documents", None),
        make_schema_fk("comments", "comment_links", None),
    ];

    assert_eq!(
        SchemaVizDocument::inbound_neighbor_names(&foreign_keys, "documents", None),
        vec![
            "attachments".to_owned(),
            "comments".to_owned(),
            "document_revisions".to_owned(),
        ]
    );
}

#[test]
fn inbound_neighbours_are_deduplicated_across_a_composite_key() {
    let foreign_keys = vec![
        make_schema_fk("document_links", "documents", None),
        make_schema_fk("document_links", "documents", None),
    ];

    assert_eq!(
        SchemaVizDocument::inbound_neighbor_names(&foreign_keys, "documents", None),
        vec!["document_links".to_owned()]
    );
}

#[test]
fn inbound_neighbours_respect_the_schema_when_both_sides_have_one() {
    let foreign_keys = vec![
        make_schema_fk("comments", "documents", Some("public")),
        make_schema_fk("archive_comments", "documents", Some("archive")),
    ];

    assert_eq!(
        SchemaVizDocument::inbound_neighbor_names(&foreign_keys, "documents", Some("public")),
        vec!["comments".to_owned()]
    );
}

#[test]
fn inbound_neighbours_treat_a_missing_schema_as_the_same_namespace() {
    // SQLite reports no schema anywhere; the sidebar always passes one where the
    // driver has one. A node without a schema and a key without one are the same
    // namespace, not different ones.
    assert!(SchemaVizDocument::same_namespace(None, None));
    assert!(SchemaVizDocument::same_namespace(Some("public"), None));
    assert!(SchemaVizDocument::same_namespace(None, Some("public")));
    assert!(!SchemaVizDocument::same_namespace(
        Some("public"),
        Some("archive")
    ));
}

// ── T23: set_show_types toggles the field and recomputes layout ──────────
//
// We cannot instantiate SchemaVizDocument without a GPUI context, so we
// test the underlying behavior directly: calling compute_layout with
// show_types=true vs show_types=false produces the same number of nodes,
// confirming that toggle-driven recomputation would succeed.
#[test]
fn test_show_types_toggle_recomputes_layout() {
    let tables = vec![make_table("users", 3)];
    let graph = SchemaGraph::build(&tables);

    let layout_with = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);
    let layout_without = compute_layout(&graph, LayoutFormat::LeftRight, None, false, false);

    assert_eq!(
        layout_with.nodes.len(),
        layout_without.nodes.len(),
        "node count is stable after show_types toggle"
    );

    // Both layouts should contain the same node indices.
    let keys_with: std::collections::HashSet<_> = layout_with.nodes.keys().collect();
    let keys_without: std::collections::HashSet<_> = layout_without.nodes.keys().collect();
    assert_eq!(keys_with, keys_without, "same nodes present after toggle");
}

// ── T24: layout_label returns correct strings per variant ────────────────

#[test]
fn test_layout_label_left_right() {
    assert_eq!(
        SchemaVizDocument::layout_label(LayoutFormat::LeftRight),
        "Left-Right"
    );
}

#[test]
fn test_layout_label_snowflake() {
    assert_eq!(
        SchemaVizDocument::layout_label(LayoutFormat::Snowflake),
        "Snowflake"
    );
}

#[test]
fn test_layout_label_compact() {
    assert_eq!(
        SchemaVizDocument::layout_label(LayoutFormat::Compact),
        "Compact"
    );
}

// ── T25: counter values match layout.nodes.len() / edges.len() ───────────

#[test]
fn test_counter_matches_layout_nodes_and_edges() {
    // Build a graph with 2 tables and 1 FK edge: orders -> users
    let users = make_table("users", 2);
    let orders = make_fk_table("orders", "users");

    let graph = SchemaGraph::build(&[users, orders]);
    let layout = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);

    let n_tables = layout.nodes.len();
    let n_relations = layout.edges.len();

    assert_eq!(n_tables, 2, "2 tables should be in layout");
    assert_eq!(n_relations, 1, "1 FK relation should be in layout");

    // This mirrors the toolbar counter formula exactly:
    //   format!("{} tables · {} relations", n_tables, n_relations)
    let counter = format!("{} tables · {} relations", n_tables, n_relations);
    assert_eq!(counter, "2 tables · 1 relations");
}

#[test]
fn test_initial_global_layout_uses_left_right_for_fk_chain() {
    let graph = SchemaGraph::build(&[
        make_fk_table("a", "b"),
        make_fk_table("b", "c"),
        make_fk_table("c", "d"),
        make_table("d", 2),
    ]);
    let actual = SchemaVizDocument::initial_global_layout(&graph);
    let expected = compute_layout(&graph, LayoutFormat::LeftRight, None, true, false);

    assert_eq!(actual.nodes.len(), expected.nodes.len());
    for (node, expected_node) in &expected.nodes {
        let actual_node = actual
            .nodes
            .get(node)
            .expect("initial layout must retain every table");
        assert_eq!(
            (actual_node.x, actual_node.y),
            (expected_node.x, expected_node.y),
            "initial global layout must follow the selected LeftRight format"
        );
    }
}

#[test]
fn test_route_left_to_right_uses_facing_ports() {
    let source = NodeBounds {
        x: 0.0,
        y: 20.0,
        width: 100.0,
        height: 80.0,
    };
    let target = NodeBounds {
        x: 300.0,
        y: 40.0,
        width: 100.0,
        height: 80.0,
    };

    let route = route_foreign_key(source, target, 30.0, 30.0, 0, 1);

    assert!(route.start().x > source.x + source.width);
    assert!(route.end().x < target.x);
    assert_axis_aligned(&route);
}

#[test]
fn test_route_right_to_left_uses_facing_ports_and_corridor_controls() {
    let source = NodeBounds {
        x: 300.0,
        y: 20.0,
        width: 100.0,
        height: 80.0,
    };
    let target = NodeBounds {
        x: 0.0,
        y: 40.0,
        width: 100.0,
        height: 80.0,
    };

    let route = route_foreign_key(source, target, 30.0, 30.0, 0, 1);

    assert!(
        route.start().x < source.x,
        "source must exit from its left port"
    );
    assert!(
        route.end().x > target.x + target.width,
        "target must enter through its right port"
    );
    let lane = lane_x(&route);
    assert!(
        lane > target.x + target.width && lane < source.x,
        "the vertical leg must run in the corridor between the two columns, got {lane}"
    );
    assert_axis_aligned(&route);
}

#[test]
fn test_route_self_reference_equal_rows_stays_outside_node() {
    let node = NodeBounds {
        x: 100.0,
        y: 200.0,
        width: 120.0,
        height: 80.0,
    };

    let route = route_foreign_key(node, node, 40.0, 40.0, 0, 1);
    let midpoint = route_midpoint(&route);

    assert_ne!(
        route.start(),
        route.end(),
        "self-reference needs a visible loop"
    );
    assert_axis_aligned(&route);
    assert!(
        midpoint.x < node.x
            || midpoint.x > node.x + node.width
            || midpoint.y < node.y
            || midpoint.y > node.y + node.height,
        "self-reference loop must not pass through its table"
    );
}

#[test]
fn test_route_parallel_foreign_keys_have_deterministic_distinct_lanes() {
    let source = NodeBounds {
        x: 0.0,
        y: 0.0,
        width: 100.0,
        height: 80.0,
    };
    let target = NodeBounds {
        x: 300.0,
        y: 0.0,
        width: 100.0,
        height: 80.0,
    };

    let first = route_foreign_key(source, target, 40.0, 40.0, 0, 2);
    let second = route_foreign_key(source, target, 40.0, 40.0, 1, 2);

    assert_ne!(first, second, "parallel FKs need separate lanes");
    assert_ne!(
        lane_x(&first),
        lane_x(&second),
        "parallel FKs need distinct corridors"
    );
    assert_axis_aligned(&first);
    assert_eq!(first, route_foreign_key(source, target, 40.0, 40.0, 0, 2));
}

#[test]
fn test_route_overlapping_tables_uses_same_side_exterior_ports() {
    let source = NodeBounds {
        x: 0.0,
        y: 0.0,
        width: 120.0,
        height: 80.0,
    };
    let target = NodeBounds {
        x: 20.0,
        y: 240.0,
        width: 120.0,
        height: 80.0,
    };

    let route = route_foreign_key(source, target, 40.0, 40.0, 0, 1);

    assert!(
        route.start().x < source.x,
        "source must exit on the exterior side"
    );
    assert!(
        route.end().x < target.x,
        "target must enter on the exterior side"
    );
    assert_axis_aligned(&route);
}

#[test]
fn test_route_translation_preserves_shape() {
    let source = NodeBounds {
        x: 10.0,
        y: 20.0,
        width: 100.0,
        height: 80.0,
    };
    let target = NodeBounds {
        x: 310.0,
        y: 140.0,
        width: 100.0,
        height: 80.0,
    };
    let translation = RoutePoint {
        x: 500.0,
        y: -300.0,
    };
    let translated_source = NodeBounds {
        x: source.x + translation.x,
        y: source.y + translation.y,
        ..source
    };
    let translated_target = NodeBounds {
        x: target.x + translation.x,
        y: target.y + translation.y,
        ..target
    };

    let route = route_foreign_key(source, target, 30.0, 50.0, 0, 1);
    let translated = route_foreign_key(translated_source, translated_target, 30.0, 50.0, 0, 1);

    let expected: Vec<RoutePoint> = route
        .points
        .iter()
        .map(|point| RoutePoint {
            x: point.x + translation.x,
            y: point.y + translation.y,
        })
        .collect();

    assert_eq!(translated.points, expected);
}

#[test]
fn test_route_reverse_translation_preserves_shape() {
    let source = NodeBounds {
        x: 300.0,
        y: 20.0,
        width: 100.0,
        height: 80.0,
    };
    let target = NodeBounds {
        x: 0.0,
        y: 140.0,
        width: 100.0,
        height: 80.0,
    };
    let translation = RoutePoint { x: -80.0, y: 250.0 };
    let translated_source = NodeBounds {
        x: source.x + translation.x,
        y: source.y + translation.y,
        ..source
    };
    let translated_target = NodeBounds {
        x: target.x + translation.x,
        y: target.y + translation.y,
        ..target
    };

    let route = route_foreign_key(source, target, 30.0, 50.0, 0, 1);
    let translated = route_foreign_key(translated_source, translated_target, 30.0, 50.0, 0, 1);

    let expected: Vec<RoutePoint> = route
        .points
        .iter()
        .map(|point| RoutePoint {
            x: point.x + translation.x,
            y: point.y + translation.y,
        })
        .collect();

    assert_eq!(
        translated,
        super::routing::OrthogonalRoute { points: expected }
    );
}

#[test]
fn test_route_self_reference_distinct_rows_stays_outside_node() {
    let node = NodeBounds {
        x: 100.0,
        y: 200.0,
        width: 120.0,
        height: 100.0,
    };

    let route = route_foreign_key(node, node, 30.0, 70.0, 0, 1);

    assert!(
        route.points.iter().all(|point| point.x < node.x),
        "a self reference must loop outside its own node"
    );
    assert!(route_midpoint(&route).x < node.x);
    assert_axis_aligned(&route);
}

#[test]
fn test_route_zero_lane_count_is_safe_and_uses_single_lane() {
    let source = NodeBounds {
        x: 0.0,
        y: 0.0,
        width: 100.0,
        height: 80.0,
    };
    let target = NodeBounds {
        x: 300.0,
        y: 0.0,
        width: 100.0,
        height: 80.0,
    };

    assert_eq!(
        route_foreign_key(source, target, 40.0, 40.0, 0, 0),
        route_foreign_key(source, target, 40.0, 40.0, 0, 1)
    );
}

fn point(x: f32, y: f32) -> Point<Pixels> {
    Point::new(px(x), px(y))
}

fn final_matrix_translation(pan: Point<Pixels>, origin: Point<Pixels>, zoom: f32) -> Point<Pixels> {
    Point::new(
        pan.x + origin.x * (1.0 - zoom),
        pan.y + origin.y * (1.0 - zoom),
    )
}

fn assert_physical_grid_alignment(
    effective_pan: Point<Pixels>,
    raw_pan: Point<Pixels>,
    origin: Point<Pixels>,
    zoom: f32,
    scale_factor: f32,
) {
    let translation = final_matrix_translation(effective_pan, origin, zoom);
    for coordinate in [translation.x, translation.y] {
        let logical: f32 = coordinate.into();
        let physical = logical * scale_factor;
        assert!((physical - physical.round()).abs() < 0.0001);
    }
    for correction in [effective_pan.x - raw_pan.x, effective_pan.y - raw_pan.y] {
        let logical: f32 = correction.into();
        assert!(logical.abs() * scale_factor <= 0.5001);
    }
}

#[test]
fn pixel_aligned_pan_rounds_half_pixels_at_display_scale_one() {
    let raw_pan = point(0.5, -0.5);
    let effective_pan = pixel_aligned_diagram_pan(raw_pan, point(0.0, 0.0), 1.0, 1.0);

    assert_eq!(effective_pan, point(1.0, -1.0));
    assert_physical_grid_alignment(effective_pan, raw_pan, point(0.0, 0.0), 1.0, 1.0);
}

#[test]
fn pixel_aligned_pan_keeps_fractional_origin_at_identity_zoom() {
    let raw_pan = point(0.0, 0.0);
    let effective_pan = pixel_aligned_diagram_pan(raw_pan, point(0.25, -0.25), 1.0, 1.0);

    assert_eq!(effective_pan, raw_pan);
    assert_physical_grid_alignment(effective_pan, raw_pan, point(0.25, -0.25), 1.0, 1.0);
}

#[test]
fn pixel_aligned_pan_accounts_for_origin_at_two_x_zoom() {
    let raw_pan = point(0.0, 0.0);
    let origin = point(0.25, 0.25);
    let effective_pan = pixel_aligned_diagram_pan(raw_pan, origin, 2.0, 1.0);

    assert_eq!(effective_pan, point(0.25, 0.25));
    assert_physical_grid_alignment(effective_pan, raw_pan, origin, 2.0, 1.0);
}

#[test]
fn pixel_aligned_pan_rounds_subpixels_at_display_scale_two() {
    let raw_pan = point(0.3, 0.3);
    let effective_pan = pixel_aligned_diagram_pan(raw_pan, point(0.0, 0.0), 1.0, 2.0);

    assert_eq!(effective_pan, point(0.5, 0.5));
    assert_physical_grid_alignment(effective_pan, raw_pan, point(0.0, 0.0), 1.0, 2.0);
}

#[test]
fn pixel_aligned_pan_aligns_non_integer_dpi_with_bounded_correction() {
    let raw_pan = point(0.3, 0.0);
    let origin = point(0.2, 0.0);
    let effective_pan = pixel_aligned_diagram_pan(raw_pan, origin, 1.5, 1.25);

    assert_eq!(effective_pan, point(0.1, 0.0));
    assert_physical_grid_alignment(effective_pan, raw_pan, origin, 1.5, 1.25);
}

#[test]
fn pixel_aligned_pan_is_a_no_op_when_already_aligned() {
    let raw_pan = point(0.5, -1.0);
    let effective_pan = pixel_aligned_diagram_pan(raw_pan, point(0.25, 0.75), 1.0, 2.0);

    assert_eq!(effective_pan, raw_pan);
    assert_physical_grid_alignment(effective_pan, raw_pan, point(0.25, 0.75), 1.0, 2.0);
}

#[test]
fn pixel_aligned_pan_preserves_raw_small_delta_accumulation() {
    let raw_pan = point(0.2 + 0.2 + 0.2, 0.0);
    let effective_pan = pixel_aligned_diagram_pan(raw_pan, point(0.0, 0.0), 1.0, 1.0);

    assert!((f32::from(raw_pan.x) - 0.6).abs() < 0.0001);
    assert_eq!(effective_pan, point(1.0, 0.0));
}

#[test]
fn pixel_aligned_pan_aligns_negative_pan_at_non_integer_dpi() {
    let raw_pan = point(-0.3, 0.47);
    let origin = point(0.2, -0.35);
    let effective_pan = pixel_aligned_diagram_pan(raw_pan, origin, 1.5, 1.25);

    assert_physical_grid_alignment(effective_pan, raw_pan, origin, 1.5, 1.25);
}

#[test]
fn pixel_aligned_pan_keeps_raw_pan_for_invalid_display_scale() {
    let raw_pan = point(0.3, -0.7);

    for invalid_scale in [0.0, -1.0, f32::INFINITY, f32::NAN] {
        assert_eq!(
            pixel_aligned_diagram_pan(raw_pan, point(0.2, -0.1), 1.5, invalid_scale),
            raw_pan
        );
    }
}

#[test]
fn test_route_segments_are_axis_aligned_in_every_configuration() {
    let source = NodeBounds {
        x: 300.0,
        y: 20.0,
        width: 120.0,
        height: 90.0,
    };
    let target_is_right = NodeBounds {
        x: 600.0,
        y: 40.0,
        width: 120.0,
        height: 90.0,
    };
    let target_is_left = NodeBounds {
        x: 0.0,
        y: 40.0,
        width: 120.0,
        height: 90.0,
    };
    let target_overlaps = NodeBounds {
        x: 320.0,
        y: 300.0,
        width: 120.0,
        height: 90.0,
    };

    for (target, label) in [
        (target_is_right, "target to the right"),
        (target_is_left, "target to the left"),
        (target_overlaps, "target overlapping the source column"),
    ] {
        for (lane_rank, lane_count) in [(0, 1), (0, 3), (1, 3), (2, 3)] {
            let route = route_foreign_key(source, target, 30.0, 50.0, lane_rank, lane_count);
            assert!(
                route.points.len() >= 2,
                "{label} with {lane_rank}/{lane_count}: a route needs at least one segment"
            );
            assert_axis_aligned(&route);
        }
    }
}
