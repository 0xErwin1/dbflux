use dbflux_diagram::{NodeBounds, route_edge};
use gpui::black;

use super::*;

fn node(x: f32, y: f32) -> NodeBounds {
    NodeBounds {
        x,
        y,
        width: 100.0,
        height: 60.0,
    }
}

fn straight_edge() -> CanvasEdge {
    let route = route_edge(node(0.0, 0.0), node(300.0, 0.0), 30.0, 30.0, 0, 1, &[]);
    CanvasEdge::new(route, black())
}

#[test]
fn a_new_edge_is_drawn_at_the_default_width() {
    assert_eq!(straight_edge().stroke_width(), DEFAULT_EDGE_WIDTH);
}

#[test]
fn the_width_set_on_an_edge_reaches_every_stroke() {
    let edge = straight_edge().width(7.0).ends(EdgeEnds::Cardinality);

    assert_eq!(EdgeGeometry::new(&edge, (0.0, 0.0)).stroke_width, 7.0);
}

#[test]
fn two_edges_keep_their_own_widths() {
    let thin = straight_edge().width(1.0);
    let thick = straight_edge().width(9.5);

    assert_eq!(EdgeGeometry::new(&thin, (0.0, 0.0)).stroke_width, 1.0);
    assert_eq!(EdgeGeometry::new(&thick, (0.0, 0.0)).stroke_width, 9.5);
}

#[test]
fn the_polyline_follows_the_route_offset_by_the_canvas_origin() {
    let edge = straight_edge();

    let geometry = EdgeGeometry::new(&edge, (10.0, 20.0));

    assert_eq!(geometry.polyline.len(), edge.route.points.len());
    let first = edge.route.start();
    assert_eq!(geometry.polyline[0], (first.x + 10.0, first.y + 20.0));
    let last = edge.route.end();
    assert_eq!(geometry.arrow[0], (last.x + 10.0, last.y + 20.0));
}

#[test]
fn an_arrow_edge_has_no_cardinality_markers() {
    let geometry = EdgeGeometry::new(&straight_edge(), (0.0, 0.0));

    assert!(geometry.foot.is_none());
    assert!(geometry.tick.is_none());
}

#[test]
fn a_cardinality_edge_has_three_prongs_and_a_tick() {
    let edge = straight_edge().ends(EdgeEnds::Cardinality);

    let geometry = EdgeGeometry::new(&edge, (0.0, 0.0));

    assert_eq!(geometry.foot.map(|prongs| prongs.len()), Some(3));
    assert!(geometry.tick.is_some());
}

#[test]
fn the_arrowhead_points_along_the_last_segment() {
    // The route enters the target moving right, so the base sits left of the tip.
    let geometry = EdgeGeometry::new(&straight_edge(), (0.0, 0.0));

    let [tip, left, right] = geometry.arrow;
    assert!(left.0 < tip.0 && right.0 < tip.0);
    assert!((left.1 - right.1).abs() > 1.0);
}

#[test]
fn nothing_to_draw_yields_no_overlay() {
    assert!(edge_overlay(Vec::new()).is_none());
}
