use super::routing::{NodeBounds, RoutePoint, route_foreign_key};
/// Tests for SchemaVizDocument logic (pure unit tests — no GPUI harness required).
///
/// Imports are kept minimal (no `super::*`) to avoid triggering GPUI proc-macro
/// expansion across the full parent module during test compilation.
use super::{LoadedSchemaData, SchemaVizDocument, pixel_aligned_diagram_pan};
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

    let route = route_foreign_key(source, target, 30.0, 30.0, 0, 1, &[]);

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

    let route = route_foreign_key(source, target, 30.0, 30.0, 0, 1, &[]);

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

    let route = route_foreign_key(node, node, 40.0, 40.0, 0, 1, &[]);
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

    let first = route_foreign_key(source, target, 40.0, 40.0, 0, 2, &[]);
    let second = route_foreign_key(source, target, 40.0, 40.0, 1, 2, &[]);

    assert_ne!(first, second, "parallel FKs need separate lanes");
    assert_ne!(
        lane_x(&first),
        lane_x(&second),
        "parallel FKs need distinct corridors"
    );
    assert_axis_aligned(&first);
    assert_eq!(
        first,
        route_foreign_key(source, target, 40.0, 40.0, 0, 2, &[])
    );
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

    let route = route_foreign_key(source, target, 40.0, 40.0, 0, 1, &[]);

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

    let route = route_foreign_key(source, target, 30.0, 50.0, 0, 1, &[]);
    let translated = route_foreign_key(translated_source, translated_target, 30.0, 50.0, 0, 1, &[]);

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

    let route = route_foreign_key(source, target, 30.0, 50.0, 0, 1, &[]);
    let translated = route_foreign_key(translated_source, translated_target, 30.0, 50.0, 0, 1, &[]);

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

    let route = route_foreign_key(node, node, 30.0, 70.0, 0, 1, &[]);

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
        route_foreign_key(source, target, 40.0, 40.0, 0, 0, &[]),
        route_foreign_key(source, target, 40.0, 40.0, 0, 1, &[])
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
            let route = route_foreign_key(source, target, 30.0, 50.0, lane_rank, lane_count, &[]);
            assert!(
                route.points.len() >= 2,
                "{label} with {lane_rank}/{lane_count}: a route needs at least one segment"
            );
            assert_axis_aligned(&route);
        }
    }
}

#[test]
fn test_drag_snapping_lands_on_the_grid_lattice() {
    // The dot grid and the drag snap share one lattice, so a dropped table lines
    // up with the background and with its neighbours.
    for (input, expected) in [
        (0.0_f32, 0.0_f32),
        (11.9, 0.0),
        (12.1, 24.0),
        (-13.0, -24.0),
        (100.0, 96.0),
    ] {
        let snapped = super::snap_to_lattice(input);
        assert!(
            (snapped - expected).abs() < 0.01,
            "snap_to_lattice({input}) = {snapped}, expected {expected}"
        );
        assert!(
            (snapped % super::GRID_LATTICE).abs() < 0.01
                || (snapped % super::GRID_LATTICE - super::GRID_LATTICE).abs() < 0.01,
            "{snapped} is not a multiple of the lattice"
        );
    }
}

#[test]
fn test_route_between_nearly_touching_tables_does_not_panic() {
    // Tables dragged by hand, or placed radially, can sit closer together than the
    // anchor gap. That leaves a corridor narrower than the anchors need, and the
    // lane clamp used to panic on the inverted range.
    for gap in [0.0_f32, 2.0, 8.0, 11.0, 12.0, 64.0] {
        let source = NodeBounds {
            x: -600.0,
            y: 0.0,
            width: 100.0,
            height: 80.0,
        };
        let target = NodeBounds {
            x: -500.0 + gap,
            y: 40.0,
            width: 100.0,
            height: 80.0,
        };

        let route = route_foreign_key(source, target, 30.0, 30.0, 0, 3, &[]);

        assert_axis_aligned(&route);
        let lane = lane_x(&route);
        assert!(
            lane.is_finite(),
            "gap {gap}: the vertical leg must land on a real coordinate, got {lane}"
        );
    }
}

#[test]
fn test_route_lane_steps_around_a_table_in_the_corridor() {
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
    // A third table parked in the middle of the corridor, right where the preferred
    // lane (the midpoint, x = 200) would run.
    let blocking = NodeBounds {
        x: 180.0,
        y: 0.0,
        width: 60.0,
        height: 200.0,
    };

    let route = route_foreign_key(source, target, 40.0, 40.0, 0, 1, &[blocking]);
    let lane = lane_x(&route);

    assert!(
        lane <= 180.0 || lane >= 240.0,
        "the vertical leg must not run through the table standing in the corridor, lane={lane}"
    );
    assert_axis_aligned(&route);
}

#[test]
fn test_type_names_are_truncated_to_their_row_slot() {
    // The row reserves a fixed slot for the type, so long names are cut short
    // rather than widening every table.
    assert_eq!(super::truncate_type_name("text"), "text");
    assert_eq!(super::truncate_type_name("uuid"), "uuid");
    assert_eq!(super::truncate_type_name("timestamptz"), "timesta…");
    assert_eq!(
        super::truncate_type_name("timestamp with time zone"),
        "timesta…"
    );
    assert_eq!(
        super::truncate_type_name("timestamp with time zone")
            .chars()
            .count(),
        super::TYPE_NAME_CHARS
    );
}

// ---------------------------------------------------------------------------
// Metadata loader tests: bulk path, per-table fallback, relation-set discipline.
//
// The fake below implements the `MetadataSource` seam the loader consumes and
// records every call so the tests can assert which path ran, not only what
// came out the other end.
// ---------------------------------------------------------------------------

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::{MetadataSource, SchemaVizMode};
use dbflux_core::{
    CancelToken, CollectionPresentation, DatabaseCategory, DbError, DbSchemaInfo,
    DriverCapabilities, DriverMetadata, DriverMetadataBuilder, IndexData, IndexInfo, QueryLanguage,
    SchemaColumnInfo, SchemaIndexInfo,
};

#[derive(Clone, Debug, Default)]
struct FakeCalls {
    table_details: Vec<(Option<String>, String)>,
    schema_columns: Vec<Option<String>>,
    schema_indexes: Vec<Option<String>>,
    schema_foreign_keys: Vec<Option<String>>,
    schema_for_database: Vec<String>,
}

struct FakeSource {
    metadata: DriverMetadata,
    columns: HashMap<Option<String>, Vec<SchemaColumnInfo>>,
    indexes: HashMap<Option<String>, Vec<SchemaIndexInfo>>,
    foreign_keys: HashMap<Option<String>, Vec<SchemaForeignKeyInfo>>,
    tables: HashMap<(Option<String>, String), TableInfo>,
    schemas: HashMap<String, DbSchemaInfo>,
    /// When set, `schema_indexes` and `schema_foreign_keys` fail after
    /// recording the call, so the loader's log-only degradation arms run.
    fail_indexes_and_fks: bool,
    calls: Mutex<FakeCalls>,
}

impl FakeSource {
    fn new() -> Self {
        Self {
            metadata: DriverMetadataBuilder::new(
                "fake",
                "Fake",
                DatabaseCategory::Relational,
                QueryLanguage::Sql,
            )
            .capabilities(DriverCapabilities::FOREIGN_KEYS)
            .build(),
            columns: HashMap::new(),
            indexes: HashMap::new(),
            foreign_keys: HashMap::new(),
            tables: HashMap::new(),
            schemas: HashMap::new(),
            fail_indexes_and_fks: false,
            calls: Mutex::new(FakeCalls::default()),
        }
    }

    fn calls(&self) -> FakeCalls {
        self.calls.lock().expect("fake calls mutex").clone()
    }
}

impl MetadataSource for FakeSource {
    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
    }

    fn table_details(
        &self,
        _database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        self.calls
            .lock()
            .expect("fake calls mutex")
            .table_details
            .push((schema.map(str::to_owned), table.to_owned()));
        self.tables
            .get(&(schema.map(str::to_owned), table.to_owned()))
            .cloned()
            .ok_or_else(|| DbError::NotSupported(format!("no table {}", table)))
    }

    fn schema_columns(
        &self,
        _database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaColumnInfo>, DbError> {
        self.calls
            .lock()
            .expect("fake calls mutex")
            .schema_columns
            .push(schema.map(str::to_owned));
        self.columns
            .get(&schema.map(str::to_owned))
            .cloned()
            .ok_or_else(|| DbError::NotSupported("no bulk column path".to_owned()))
    }

    fn schema_indexes(
        &self,
        _database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaIndexInfo>, DbError> {
        self.calls
            .lock()
            .expect("fake calls mutex")
            .schema_indexes
            .push(schema.map(str::to_owned));
        if self.fail_indexes_and_fks {
            return Err(DbError::NotSupported("no bulk index path".to_owned()));
        }
        Ok(self
            .indexes
            .get(&schema.map(str::to_owned))
            .cloned()
            .unwrap_or_default())
    }

    fn schema_foreign_keys(
        &self,
        _database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        self.calls
            .lock()
            .expect("fake calls mutex")
            .schema_foreign_keys
            .push(schema.map(str::to_owned));
        if self.fail_indexes_and_fks {
            return Err(DbError::NotSupported("no bulk foreign key path".to_owned()));
        }
        Ok(self
            .foreign_keys
            .get(&schema.map(str::to_owned))
            .cloned()
            .unwrap_or_default())
    }

    fn schema_for_database(&self, database: &str) -> Result<DbSchemaInfo, DbError> {
        self.calls
            .lock()
            .expect("fake calls mutex")
            .schema_for_database
            .push(database.to_owned());
        self.schemas
            .get(database)
            .cloned()
            .ok_or_else(|| DbError::NotSupported(format!("no schema for {}", database)))
    }
}

fn bulk_column(table: &str, name: &str, is_primary_key: bool) -> SchemaColumnInfo {
    SchemaColumnInfo {
        table_name: table.to_owned(),
        column: ColumnInfo {
            name: name.to_owned(),
            type_name: "integer".to_owned(),
            nullable: !is_primary_key,
            is_primary_key,
            default_value: None,
            enum_values: None,
        },
    }
}

fn bulk_index(table: &str, name: &str, columns: &[&str]) -> SchemaIndexInfo {
    SchemaIndexInfo {
        name: name.to_owned(),
        table_name: table.to_owned(),
        columns: columns.iter().map(|c| (*c).to_owned()).collect(),
        is_unique: false,
        is_primary: false,
    }
}

fn plain_column(name: &str, is_primary_key: bool) -> ColumnInfo {
    ColumnInfo {
        name: name.to_owned(),
        type_name: "integer".to_owned(),
        nullable: !is_primary_key,
        is_primary_key,
        default_value: None,
        enum_values: None,
    }
}

fn per_table_index(name: &str, columns: &[&str]) -> IndexInfo {
    IndexInfo {
        name: name.to_owned(),
        columns: columns.iter().map(|c| (*c).to_owned()).collect(),
        is_unique: false,
        is_primary: false,
    }
}

/// A per-table `table_details` response shaped exactly like what the bulk
/// path fabricates, so the two paths can be compared for the same input.
fn per_table_response(
    name: &str,
    schema: Option<&str>,
    columns: Vec<ColumnInfo>,
    indexes: Vec<IndexInfo>,
    foreign_keys: Vec<ForeignKeyInfo>,
) -> TableInfo {
    TableInfo {
        name: name.to_owned(),
        schema: schema.map(str::to_owned),
        columns: Some(columns),
        indexes: Some(IndexData::Relational(indexes)),
        foreign_keys: Some(foreign_keys),
        constraints: None,
        sample_fields: None,
        presentation: CollectionPresentation::DataGrid,
        child_items: None,
        storage_hints: None,
    }
}

fn users_from_bulk() -> Vec<SchemaColumnInfo> {
    vec![
        bulk_column("users", "id", true),
        bulk_column("posts", "id", true),
        bulk_column("posts", "user_id", false),
    ]
}

fn users_focused_load(source: &FakeSource) -> (Vec<TableInfo>, bool, usize) {
    let result = SchemaVizDocument::load_focused_schema_blocking(
        Some("app".to_owned()),
        SchemaVizMode::Focused {
            table: "users".to_owned(),
            schema: Some("public".to_owned()),
        },
        source,
        Arc::new(CancelToken::new()),
    )
    .expect("focused load should succeed");
    (result.tables, result.capped, result.tables_loaded)
}

fn sorted_table_summaries(
    tables: &[TableInfo],
) -> Vec<(String, Vec<String>, Vec<String>, Vec<String>)> {
    let mut summaries: Vec<(String, Vec<String>, Vec<String>, Vec<String>)> = tables
        .iter()
        .map(|table| {
            let columns = table
                .columns
                .as_ref()
                .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
                .unwrap_or_default();
            let index_names = table
                .indexes
                .as_ref()
                .map(|data| match data {
                    IndexData::Relational(indexes) => {
                        indexes.iter().map(|i| i.name.clone()).collect()
                    }
                    IndexData::Document(indexes) => {
                        indexes.iter().map(|i| i.name.clone()).collect()
                    }
                })
                .unwrap_or_default();
            let foreign_keys = table
                .foreign_keys
                .as_ref()
                .map(|fks| fks.iter().map(|fk| fk.referenced_table.clone()).collect())
                .unwrap_or_default();
            (table.name.clone(), columns, index_names, foreign_keys)
        })
        .collect();
    summaries.sort_by(|a, b| a.0.cmp(&b.0));
    summaries
}

fn sorted_table_details_calls(calls: &[(Option<String>, String)]) -> Vec<(String, String)> {
    let mut sorted: Vec<(String, String)> = calls
        .iter()
        .map(|(schema, table)| (schema.clone().unwrap_or_default(), table.clone()))
        .collect();
    sorted.sort();
    sorted
}

#[test]
fn loader_bulk_path_fabricates_tables_without_table_details() {
    let mut source = FakeSource::new();
    source
        .columns
        .insert(Some("public".to_owned()), users_from_bulk());
    source.indexes.insert(
        Some("public".to_owned()),
        vec![bulk_index("users", "users_pkey", &["id"])],
    );
    source.foreign_keys.insert(
        Some("public".to_owned()),
        vec![make_schema_fk("posts", "users", None)],
    );

    let (tables, capped, tables_loaded) = users_focused_load(&source);

    assert_eq!(tables_loaded, 2);
    assert!(!capped);

    let users = tables
        .iter()
        .find(|t| t.name == "users")
        .expect("users node");
    assert_eq!(users.schema.as_deref(), Some("public"));
    let user_columns = users.columns.as_ref().expect("users columns");
    assert_eq!(user_columns.len(), 1);
    assert!(user_columns[0].is_primary_key);
    match &users.indexes {
        Some(IndexData::Relational(indexes)) => {
            assert_eq!(indexes.len(), 1);
            assert_eq!(indexes[0].name, "users_pkey");
        }
        other => panic!("users should carry relational indexes, got {other:?}"),
    }
    let user_fks = users.foreign_keys.as_ref().expect("users fks");
    assert!(user_fks.is_empty());

    let posts = tables
        .iter()
        .find(|t| t.name == "posts")
        .expect("posts node");
    let post_fks = posts.foreign_keys.as_ref().expect("posts fks");
    assert_eq!(post_fks.len(), 1);
    assert_eq!(post_fks[0].name, "fk_posts_users");
    assert_eq!(post_fks[0].columns, vec!["users_id".to_owned()]);
    assert_eq!(post_fks[0].referenced_table, "users");
    assert_eq!(post_fks[0].referenced_schema, None);
    assert_eq!(post_fks[0].referenced_columns, vec!["id".to_owned()]);
    assert_eq!(post_fks[0].on_delete, None);
    assert_eq!(post_fks[0].on_update, None);

    let calls = source.calls();
    assert!(
        calls.table_details.is_empty(),
        "table_details must not run on the bulk path, got {calls:?}"
    );
    assert_eq!(calls.schema_columns, vec![Some("public".to_owned())]);
    assert_eq!(calls.schema_indexes, vec![Some("public".to_owned())]);
    assert_eq!(calls.schema_foreign_keys, vec![Some("public".to_owned())]);
    assert!(calls.schema_for_database.is_empty());
}

#[test]
fn loader_fallback_path_matches_bulk_path_result() {
    // Same scenario twice: bulk supported, then `schema_columns` unsupported
    // (no `columns` entry) so every relation goes through `table_details`.
    let mut bulk_source = FakeSource::new();
    bulk_source
        .columns
        .insert(Some("public".to_owned()), users_from_bulk());
    bulk_source.indexes.insert(
        Some("public".to_owned()),
        vec![bulk_index("users", "users_pkey", &["id"])],
    );
    bulk_source.foreign_keys.insert(
        Some("public".to_owned()),
        vec![make_schema_fk("posts", "users", None)],
    );

    let (bulk_tables, _, _) = users_focused_load(&bulk_source);

    let mut fallback_source = FakeSource::new();
    fallback_source.foreign_keys.insert(
        Some("public".to_owned()),
        vec![make_schema_fk("posts", "users", None)],
    );
    fallback_source.tables.insert(
        (Some("public".to_owned()), "users".to_owned()),
        per_table_response(
            "users",
            Some("public"),
            vec![plain_column("id", true)],
            vec![per_table_index("users_pkey", &["id"])],
            vec![],
        ),
    );
    fallback_source.tables.insert(
        (Some("public".to_owned()), "posts".to_owned()),
        per_table_response(
            "posts",
            Some("public"),
            vec![plain_column("id", true), plain_column("user_id", false)],
            vec![],
            vec![ForeignKeyInfo {
                name: "fk_posts_users".to_owned(),
                columns: vec!["users_id".to_owned()],
                referenced_table: "users".to_owned(),
                referenced_schema: None,
                referenced_columns: vec!["id".to_owned()],
                on_delete: None,
                on_update: None,
            }],
        ),
    );

    let (fallback_tables, capped, tables_loaded) = users_focused_load(&fallback_source);

    // One relation comes from bulk and one from `table_details`; both count.
    assert_eq!(
        tables_loaded, 2,
        "relations assembled from either path must be counted"
    );
    assert!(!capped);
    assert_eq!(
        sorted_table_summaries(&bulk_tables),
        sorted_table_summaries(&fallback_tables)
    );

    let calls = fallback_source.calls();
    assert_eq!(
        sorted_table_details_calls(&calls.table_details),
        vec![
            ("public".to_owned(), "posts".to_owned()),
            ("public".to_owned(), "users".to_owned()),
        ]
    );
    assert_eq!(calls.schema_columns, vec![Some("public".to_owned())]);
    assert!(
        calls.schema_indexes.is_empty(),
        "the fallback path must not bulk-fetch indexes"
    );
    assert_eq!(calls.schema_foreign_keys, vec![Some("public".to_owned())]);
}

#[test]
fn loader_bulk_result_extra_relation_becomes_no_node() {
    let mut source = FakeSource::new();
    let mut columns = users_from_bulk();
    // A serial primary key leaves a sequence behind; the bulk result names it
    // but nothing in the name distinguishes it from a table.
    columns.push(bulk_column("feed_id_seq", "last_value", false));
    source.columns.insert(Some("public".to_owned()), columns);
    source.foreign_keys.insert(
        Some("public".to_owned()),
        vec![make_schema_fk("posts", "users", None)],
    );

    let (tables, _, _) = users_focused_load(&source);

    let mut names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    names.sort();
    assert_eq!(names, vec!["posts", "users"]);

    let calls = source.calls();
    assert!(
        calls.table_details.is_empty(),
        "the extra relation must not even be fetched, got {calls:?}"
    );
}

#[test]
fn loader_returns_unfiltered_bulk_maps_for_cache_seeding() {
    let mut source = FakeSource::new();
    let mut columns = users_from_bulk();
    // A serial primary key leaves a sequence behind: the diagram excludes it
    // from its nodes, but the cache seed must still carry it — a populated
    // schema key means the whole schema is loaded.
    columns.push(bulk_column("feed_id_seq", "last_value", false));
    source.columns.insert(Some("public".to_owned()), columns);
    source.indexes.insert(
        Some("public".to_owned()),
        vec![
            bulk_index("users", "users_pkey", &["id"]),
            bulk_index("feed_id_seq", "feed_id_seq_pkey", &["last_value"]),
        ],
    );
    source.foreign_keys.insert(
        Some("public".to_owned()),
        vec![make_schema_fk("posts", "users", None)],
    );

    let result = SchemaVizDocument::load_focused_schema_blocking(
        Some("app".to_owned()),
        SchemaVizMode::Focused {
            table: "users".to_owned(),
            schema: Some("public".to_owned()),
        },
        &source,
        Arc::new(CancelToken::new()),
    )
    .expect("focused load should succeed");

    // The diagram itself filtered the sequence out of its nodes...
    let mut names: Vec<&str> = result.tables.iter().map(|t| t.name.as_str()).collect();
    names.sort();
    assert_eq!(names, vec!["posts", "users"]);

    // ...but the returned bulk maps must carry every relation the fake's bulk
    // response contained, including the one the diagram excluded.
    assert_eq!(result.bulk.len(), 1, "one entry per fetched schema");
    let seed = &result.bulk[0];
    assert_eq!(seed.database, "app");
    assert_eq!(seed.schema.as_deref(), Some("public"));

    let mut column_tables: Vec<&str> = seed
        .columns
        .as_ref()
        .expect("bulk columns fetched")
        .iter()
        .map(|c| c.table_name.as_str())
        .collect();
    column_tables.sort();
    column_tables.dedup();
    assert_eq!(
        column_tables,
        vec!["feed_id_seq", "posts", "users"],
        "the column seed is unfiltered: the sequence the diagram dropped is still there"
    );

    let mut index_tables: Vec<&str> = seed
        .indexes
        .as_ref()
        .expect("bulk indexes fetched")
        .iter()
        .map(|i| i.table_name.as_str())
        .collect();
    index_tables.sort();
    assert_eq!(
        index_tables,
        vec!["feed_id_seq", "users"],
        "the index seed is unfiltered"
    );

    let fk_tables: Vec<&str> = seed
        .foreign_keys
        .as_ref()
        .expect("bulk foreign keys fetched")
        .iter()
        .map(|fk| fk.table_name.as_str())
        .collect();
    assert_eq!(fk_tables, vec!["posts"]);

    // Degraded seams must arrive as `None` so the cache is never seeded with
    // an empty stand-in for a real result.
    let mut failed_seams = FakeSource::new();
    failed_seams.fail_indexes_and_fks = true;
    failed_seams.schemas.insert(
        "app".to_owned(),
        DbSchemaInfo {
            name: "app".to_owned(),
            tables: vec![TableInfo {
                name: "users".to_owned(),
                schema: Some("public".to_owned()),
                columns: None,
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: None,
                presentation: CollectionPresentation::DataGrid,
                child_items: None,
                storage_hints: None,
            }],
            views: Vec::new(),
            custom_types: None,
        },
    );
    failed_seams.columns.insert(
        Some("public".to_owned()),
        vec![bulk_column("users", "id", true)],
    );

    let failed = SchemaVizDocument::load_focused_schema_blocking(
        Some("app".to_owned()),
        SchemaVizMode::Global,
        &failed_seams,
        Arc::new(CancelToken::new()),
    )
    .expect("global load should succeed despite the failed seams");

    assert_eq!(failed.bulk.len(), 1);
    let failed_seed = &failed.bulk[0];
    assert!(
        failed_seed.columns.is_some(),
        "the successful column seam is seeded"
    );
    assert!(
        failed_seed.indexes.is_none(),
        "a failed index seam must not seed an empty stand-in"
    );
    assert!(
        failed_seed.foreign_keys.is_none(),
        "a failed foreign-key seam must not seed an empty stand-in"
    );
}

#[test]
fn loader_cross_schema_focused_load_bulks_each_schema_once() {
    let mut source = FakeSource::new();
    // Two relations per schema: with only one relation per schema, calling
    // the bulk seams once per relation would produce the same call vector as
    // calling them once per schema.
    source.columns.insert(
        Some("public".to_owned()),
        vec![
            bulk_column("users", "id", true),
            bulk_column("profiles", "id", true),
        ],
    );
    source.columns.insert(
        Some("billing".to_owned()),
        vec![
            bulk_column("invoices", "id", true),
            bulk_column("invoices", "user_id", false),
            bulk_column("audit_entries", "id", true),
        ],
    );
    // The focal table declares outbound foreign keys into another schema, and
    // a second public relation declares an inbound one back at the focal table.
    source.foreign_keys.insert(
        Some("public".to_owned()),
        vec![
            make_schema_fk("users", "invoices", Some("billing")),
            make_schema_fk("users", "audit_entries", Some("billing")),
            make_schema_fk("profiles", "users", None),
        ],
    );

    let (tables, _, tables_loaded) = users_focused_load(&source);

    assert_eq!(tables_loaded, 4);
    let mut names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["audit_entries", "invoices", "profiles", "users"]
    );
    let invoices = tables
        .iter()
        .find(|t| t.name == "invoices")
        .expect("cross-schema node");
    assert_eq!(invoices.schema.as_deref(), Some("billing"));
    let invoice_columns = invoices.columns.as_ref().expect("invoices columns");
    assert_eq!(invoice_columns.len(), 2);

    let users = tables
        .iter()
        .find(|t| t.name == "users")
        .expect("users node");
    let user_fks = users.foreign_keys.as_ref().expect("users fks");
    assert_eq!(user_fks[0].referenced_schema.as_deref(), Some("billing"));

    let calls = source.calls();
    assert!(calls.table_details.is_empty());
    let mut column_calls = calls.schema_columns.clone();
    column_calls.sort();
    assert_eq!(
        column_calls,
        vec![Some("billing".to_owned()), Some("public".to_owned())],
        "one bulk attempt per distinct schema"
    );
}

#[test]
fn loader_missing_relation_falls_back_per_relation() {
    let mut source = FakeSource::new();
    // Bulk result covers users but not posts.
    source.columns.insert(
        Some("public".to_owned()),
        vec![bulk_column("users", "id", true)],
    );
    source.foreign_keys.insert(
        Some("public".to_owned()),
        vec![make_schema_fk("posts", "users", None)],
    );
    source.tables.insert(
        (Some("public".to_owned()), "posts".to_owned()),
        per_table_response(
            "posts",
            Some("public"),
            vec![plain_column("id", true), plain_column("user_id", false)],
            vec![],
            vec![ForeignKeyInfo {
                name: "fk_posts_users".to_owned(),
                columns: vec!["users_id".to_owned()],
                referenced_table: "users".to_owned(),
                referenced_schema: None,
                referenced_columns: vec!["id".to_owned()],
                on_delete: None,
                on_update: None,
            }],
        ),
    );

    let (tables, _, tables_loaded) = users_focused_load(&source);

    // users arrives via the bulk path and posts via `table_details`; both
    // count toward `tables_loaded`.
    assert_eq!(
        tables_loaded, 2,
        "relations assembled from either path must be counted"
    );
    let posts = tables
        .iter()
        .find(|t| t.name == "posts")
        .expect("posts node");
    let post_columns = posts.columns.as_ref().expect("posts columns");
    assert_eq!(post_columns.len(), 2);
    let users = tables
        .iter()
        .find(|t| t.name == "users")
        .expect("users node");
    assert_eq!(users.columns.as_ref().expect("users columns").len(), 1);

    let calls = source.calls();
    assert_eq!(
        sorted_table_details_calls(&calls.table_details),
        vec![("public".to_owned(), "posts".to_owned())],
        "only the relation missing from the bulk result is fetched per table"
    );
    assert_eq!(calls.schema_columns, vec![Some("public".to_owned())]);
}

#[test]
fn loader_global_bulks_each_schema_once_without_table_details() {
    let mut source = FakeSource::new();
    source.schemas.insert(
        "app".to_owned(),
        DbSchemaInfo {
            name: "app".to_owned(),
            tables: vec![
                TableInfo {
                    name: "users".to_owned(),
                    schema: Some("public".to_owned()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: CollectionPresentation::DataGrid,
                    child_items: None,
                    storage_hints: None,
                },
                TableInfo {
                    name: "sessions".to_owned(),
                    schema: Some("public".to_owned()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: CollectionPresentation::DataGrid,
                    child_items: None,
                    storage_hints: None,
                },
                TableInfo {
                    name: "audit_log".to_owned(),
                    schema: Some("audit".to_owned()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: CollectionPresentation::DataGrid,
                    child_items: None,
                    storage_hints: None,
                },
                TableInfo {
                    name: "audit_archive".to_owned(),
                    schema: Some("audit".to_owned()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: CollectionPresentation::DataGrid,
                    child_items: None,
                    storage_hints: None,
                },
            ],
            views: Vec::new(),
            custom_types: None,
        },
    );
    source.columns.insert(
        Some("public".to_owned()),
        vec![
            bulk_column("users", "id", true),
            bulk_column("sessions", "id", true),
            // Present in the bulk result but not in the relation set.
            bulk_column("users_session_seq", "last_value", false),
        ],
    );
    source.columns.insert(
        Some("audit".to_owned()),
        vec![
            bulk_column("audit_log", "id", true),
            bulk_column("audit_archive", "id", true),
        ],
    );

    let LoadedSchemaData {
        tables,
        capped,
        tables_loaded,
        ..
    } = SchemaVizDocument::load_focused_schema_blocking(
        Some("app".to_owned()),
        SchemaVizMode::Global,
        &source,
        Arc::new(CancelToken::new()),
    )
    .expect("global load should succeed");

    assert_eq!(tables_loaded, 4);
    assert!(!capped);
    let mut names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["audit_archive", "audit_log", "sessions", "users"]
    );

    let calls = source.calls();
    assert!(calls.table_details.is_empty());
    let mut column_calls = calls.schema_columns.clone();
    column_calls.sort();
    assert_eq!(
        column_calls,
        vec![Some("audit".to_owned()), Some("public".to_owned())]
    );
}

#[test]
fn loader_same_name_in_two_schemas_keeps_schema_scoped_maps() {
    let mut source = FakeSource::new();
    // The same relation name exists in two schemas with deliberately
    // different columns and indexes: merging the per-schema bulk maps into
    // one table-keyed map would cross-contaminate both nodes.
    source.columns.insert(
        Some("public".to_owned()),
        vec![
            bulk_column("orders", "id", true),
            bulk_column("orders", "customer_id", false),
        ],
    );
    source.columns.insert(
        Some("billing".to_owned()),
        vec![
            bulk_column("orders", "id", true),
            bulk_column("orders", "invoice_id", false),
        ],
    );
    source.indexes.insert(
        Some("public".to_owned()),
        vec![bulk_index("orders", "orders_pkey", &["id"])],
    );
    source.indexes.insert(
        Some("billing".to_owned()),
        vec![bulk_index(
            "orders",
            "billing_orders_invoice_idx",
            &["invoice_id"],
        )],
    );
    source.foreign_keys.insert(
        Some("public".to_owned()),
        vec![make_schema_fk("orders", "orders", Some("billing"))],
    );

    let LoadedSchemaData {
        tables,
        capped,
        tables_loaded,
        ..
    } = SchemaVizDocument::load_focused_schema_blocking(
        Some("app".to_owned()),
        SchemaVizMode::Focused {
            table: "orders".to_owned(),
            schema: Some("public".to_owned()),
        },
        &source,
        Arc::new(CancelToken::new()),
    )
    .expect("focused load should succeed");

    assert_eq!(tables_loaded, 2);
    assert!(!capped);
    assert_eq!(tables.len(), 2);

    let public_orders = tables
        .iter()
        .find(|t| t.name == "orders" && t.schema.as_deref() == Some("public"))
        .expect("public orders node");
    let public_columns: Vec<&str> = public_orders
        .columns
        .as_ref()
        .expect("public orders columns")
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(public_columns, vec!["id", "customer_id"]);
    match &public_orders.indexes {
        Some(IndexData::Relational(indexes)) => {
            let names: Vec<&str> = indexes.iter().map(|i| i.name.as_str()).collect();
            assert_eq!(names, vec!["orders_pkey"]);
        }
        other => panic!("public orders should carry relational indexes, got {other:?}"),
    }

    let billing_orders = tables
        .iter()
        .find(|t| t.name == "orders" && t.schema.as_deref() == Some("billing"))
        .expect("billing orders node");
    let billing_columns: Vec<&str> = billing_orders
        .columns
        .as_ref()
        .expect("billing orders columns")
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(billing_columns, vec!["id", "invoice_id"]);
    match &billing_orders.indexes {
        Some(IndexData::Relational(indexes)) => {
            let names: Vec<&str> = indexes.iter().map(|i| i.name.as_str()).collect();
            assert_eq!(names, vec!["billing_orders_invoice_idx"]);
        }
        other => panic!("billing orders should carry relational indexes, got {other:?}"),
    }

    let calls = source.calls();
    assert!(
        calls.table_details.is_empty(),
        "same-named relations must resolve from their own schema's bulk data, got {calls:?}"
    );
    let mut column_calls = calls.schema_columns.clone();
    column_calls.sort();
    assert_eq!(
        column_calls,
        vec![Some("billing".to_owned()), Some("public".to_owned())]
    );
}

#[test]
fn loader_global_not_supported_columns_fall_back_per_relation() {
    let mut source = FakeSource::new();
    // No `columns` entries: every `schema_columns` attempt returns
    // NotSupported, so the Global loop must fall back to per-relation
    // `table_details` and drop nothing.
    source.schemas.insert(
        "app".to_owned(),
        DbSchemaInfo {
            name: "app".to_owned(),
            tables: vec![
                TableInfo {
                    name: "users".to_owned(),
                    schema: Some("public".to_owned()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: CollectionPresentation::DataGrid,
                    child_items: None,
                    storage_hints: None,
                },
                TableInfo {
                    name: "audit_log".to_owned(),
                    schema: Some("audit".to_owned()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: CollectionPresentation::DataGrid,
                    child_items: None,
                    storage_hints: None,
                },
            ],
            views: Vec::new(),
            custom_types: None,
        },
    );
    source.tables.insert(
        (Some("public".to_owned()), "users".to_owned()),
        per_table_response(
            "users",
            Some("public"),
            vec![plain_column("id", true)],
            vec![],
            vec![],
        ),
    );
    source.tables.insert(
        (Some("audit".to_owned()), "audit_log".to_owned()),
        per_table_response(
            "audit_log",
            Some("audit"),
            vec![plain_column("id", true), plain_column("event", false)],
            vec![],
            vec![],
        ),
    );

    let LoadedSchemaData {
        tables,
        capped,
        tables_loaded,
        ..
    } = SchemaVizDocument::load_focused_schema_blocking(
        Some("app".to_owned()),
        SchemaVizMode::Global,
        &source,
        Arc::new(CancelToken::new()),
    )
    .expect("global load should succeed");

    assert_eq!(tables_loaded, 2);
    assert!(!capped);
    let mut names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    names.sort();
    assert_eq!(names, vec!["audit_log", "users"], "no relation is dropped");
    let users = tables
        .iter()
        .find(|t| t.name == "users")
        .expect("users node");
    assert_eq!(users.columns.as_ref().expect("users columns").len(), 1);
    let audit_log = tables
        .iter()
        .find(|t| t.name == "audit_log")
        .expect("audit_log node");
    assert_eq!(
        audit_log.columns.as_ref().expect("audit_log columns").len(),
        2
    );

    let calls = source.calls();
    assert_eq!(
        sorted_table_details_calls(&calls.table_details),
        vec![
            ("audit".to_owned(), "audit_log".to_owned()),
            ("public".to_owned(), "users".to_owned()),
        ],
        "every relation must arrive via table_details"
    );
    let mut column_calls = calls.schema_columns.clone();
    column_calls.sort();
    assert_eq!(
        column_calls,
        vec![Some("audit".to_owned()), Some("public".to_owned())],
        "the bulk attempt still runs once per distinct schema"
    );
}

#[test]
fn loader_global_succeeds_with_empty_indexes_and_fks_when_bulk_seams_fail() {
    let mut source = FakeSource::new();
    source.fail_indexes_and_fks = true;
    // `schema_columns` succeeds while `schema_indexes` and
    // `schema_foreign_keys` both fail: the load degrades to empty indexes and
    // foreign keys but must not fail or drop relations.
    source.schemas.insert(
        "app".to_owned(),
        DbSchemaInfo {
            name: "app".to_owned(),
            tables: vec![
                TableInfo {
                    name: "users".to_owned(),
                    schema: Some("public".to_owned()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: CollectionPresentation::DataGrid,
                    child_items: None,
                    storage_hints: None,
                },
                TableInfo {
                    name: "posts".to_owned(),
                    schema: Some("public".to_owned()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: CollectionPresentation::DataGrid,
                    child_items: None,
                    storage_hints: None,
                },
            ],
            views: Vec::new(),
            custom_types: None,
        },
    );
    source.columns.insert(
        Some("public".to_owned()),
        vec![
            bulk_column("users", "id", true),
            bulk_column("posts", "id", true),
            bulk_column("posts", "user_id", false),
        ],
    );
    // Would have been used had the seams not failed.
    source.indexes.insert(
        Some("public".to_owned()),
        vec![bulk_index("users", "users_pkey", &["id"])],
    );
    source.foreign_keys.insert(
        Some("public".to_owned()),
        vec![make_schema_fk("posts", "users", None)],
    );

    let LoadedSchemaData {
        tables,
        capped,
        tables_loaded,
        ..
    } = SchemaVizDocument::load_focused_schema_blocking(
        Some("app".to_owned()),
        SchemaVizMode::Global,
        &source,
        Arc::new(CancelToken::new()),
    )
    .expect("global load should succeed despite index and FK failures");

    assert_eq!(tables_loaded, 2);
    assert!(!capped);
    let users = tables
        .iter()
        .find(|t| t.name == "users")
        .expect("users node");
    let user_columns: Vec<&str> = users
        .columns
        .as_ref()
        .expect("users columns")
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(user_columns, vec!["id"]);
    match &users.indexes {
        Some(IndexData::Relational(indexes)) => {
            assert!(indexes.is_empty(), "failed index seam degrades to empty");
        }
        other => panic!("users should carry relational indexes, got {other:?}"),
    }
    assert!(
        users.foreign_keys.as_ref().expect("users fks").is_empty(),
        "failed foreign-key seam degrades to empty"
    );

    let posts = tables
        .iter()
        .find(|t| t.name == "posts")
        .expect("posts node");
    let post_columns: Vec<&str> = posts
        .columns
        .as_ref()
        .expect("posts columns")
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(post_columns, vec!["id", "user_id"]);

    let calls = source.calls();
    assert_eq!(calls.schema_columns, vec![Some("public".to_owned())]);
    assert_eq!(
        calls.schema_indexes,
        vec![Some("public".to_owned())],
        "the failing index seam was attempted"
    );
    assert_eq!(
        calls.schema_foreign_keys,
        vec![Some("public".to_owned())],
        "the failing foreign-key seam was attempted"
    );
}
