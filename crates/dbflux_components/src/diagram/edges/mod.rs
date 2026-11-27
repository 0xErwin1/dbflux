//! Drawing of routed edges.
//!
//! A consumer routes each edge with `dbflux_diagram::route_edge`, describes how
//! it looks as a [`CanvasEdge`] (colour, stroke width, dashing, end markers) and
//! hands the lot to [`edge_overlay`], which paints every edge on one canvas
//! element. The geometry of one edge is computed by [`EdgeGeometry`], free of any
//! window, so it can be tested without painting.

use dbflux_diagram::OrthogonalRoute;
use gpui::prelude::*;
use gpui::{Div, Hsla, PathBuilder, Window, canvas, div, point, px};

/// Stroke width an edge has unless the consumer sets one.
pub const DEFAULT_EDGE_WIDTH: f32 = 1.5;

/// Side of the square canvas the edges are painted on, in graph coordinates.
const OVERLAY_EXTENT: f32 = 10000.0;

const ARROW_LENGTH: f32 = 8.0;
const ARROW_HALF_BASE: f32 = 3.0;
const FOOT_LENGTH: f32 = 9.0;
const FOOT_SPREAD: f32 = 4.5;
const TICK_OFFSET: f32 = 6.0;
const TICK_HALF: f32 = 5.0;

/// What an edge draws at its ends, besides the line itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum EdgeEnds {
    /// An arrowhead at the target.
    #[default]
    Arrow,
    /// An arrowhead at the target, plus the cardinality notation of an IDEF1X
    /// diagram: a crow's foot at the source (many) and a tick at the target
    /// (one).
    Cardinality,
}

/// One edge to draw.
#[derive(Clone, Debug)]
pub struct CanvasEdge {
    route: OrthogonalRoute,
    color: Hsla,
    width: f32,
    dashed: bool,
    ends: EdgeEnds,
}

impl CanvasEdge {
    /// An edge along `route` in `color`, drawn solid at [`DEFAULT_EDGE_WIDTH`]
    /// with an arrowhead.
    pub fn new(route: OrthogonalRoute, color: Hsla) -> Self {
        Self {
            route,
            color,
            width: DEFAULT_EDGE_WIDTH,
            dashed: false,
            ends: EdgeEnds::default(),
        }
    }

    /// The stroke width, in pixels, of the line and of the end markers. The
    /// canvas draws exactly this width; mapping a quantity (rows, cost) to a
    /// width is the consumer's call.
    pub fn width(mut self, width: f32) -> Self {
        self.width = width;
        self
    }

    pub fn dashed(mut self, dashed: bool) -> Self {
        self.dashed = dashed;
        self
    }

    pub fn ends(mut self, ends: EdgeEnds) -> Self {
        self.ends = ends;
        self
    }

    pub fn stroke_width(&self) -> f32 {
        self.width
    }
}

/// A line from one point to another, in window coordinates.
pub type Segment = ((f32, f32), (f32, f32));

/// Every shape of one edge, in window coordinates.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeGeometry {
    /// The route, drawn as one stroked path.
    pub polyline: Vec<(f32, f32)>,
    /// Stroke width of the polyline and the markers.
    pub stroke_width: f32,
    /// Tip, left and right corner of the arrowhead, a filled triangle.
    pub arrow: [(f32, f32); 3],
    /// The three prongs of the crow's foot, for [`EdgeEnds::Cardinality`].
    pub foot: Option<[Segment; 3]>,
    /// The tick across the line near the target, for [`EdgeEnds::Cardinality`].
    pub tick: Option<Segment>,
}

impl EdgeGeometry {
    /// The shapes of `edge` with the canvas element's top-left corner at
    /// `origin`: `paint_path` works in window-absolute coordinates, so every
    /// point of the route is offset by it.
    pub fn new(edge: &CanvasEdge, origin: (f32, f32)) -> Self {
        let (origin_x, origin_y) = origin;
        let route = &edge.route;
        let start = route.start();
        let end = route.end();
        let (start_x, start_y) = (start.x + origin_x, start.y + origin_y);
        let (end_x, end_y) = (end.x + origin_x, end.y + origin_y);

        let polyline = route
            .points
            .iter()
            .map(|route_point| (route_point.x + origin_x, route_point.y + origin_y))
            .collect();

        // Arrowhead: a small filled triangle at the target, along the direction
        // the last segment enters with.
        let (ux, uy) = route.end_direction();
        let base_center_x = end_x - ux * ARROW_LENGTH;
        let base_center_y = end_y - uy * ARROW_LENGTH;
        let arrow = [
            (end_x, end_y),
            (
                base_center_x - uy * ARROW_HALF_BASE,
                base_center_y + ux * ARROW_HALF_BASE,
            ),
            (
                base_center_x + uy * ARROW_HALF_BASE,
                base_center_y - ux * ARROW_HALF_BASE,
            ),
        ];

        let (foot, tick) = match edge.ends {
            EdgeEnds::Arrow => (None, None),
            EdgeEnds::Cardinality => {
                let (sx_dir, sy_dir) = route.start_direction();
                let (perp_x, perp_y) = (-sy_dir, sx_dir);
                let base_x = start_x + sx_dir * FOOT_LENGTH;
                let base_y = start_y + sy_dir * FOOT_LENGTH;
                let foot = [-FOOT_SPREAD, 0.0, FOOT_SPREAD].map(|offset| {
                    (
                        (base_x, base_y),
                        (start_x + perp_x * offset, start_y + perp_y * offset),
                    )
                });

                let tick_x = end_x - ux * TICK_OFFSET;
                let tick_y = end_y - uy * TICK_OFFSET;
                let tick = (
                    (tick_x + uy * TICK_HALF, tick_y - ux * TICK_HALF),
                    (tick_x - uy * TICK_HALF, tick_y + ux * TICK_HALF),
                );

                (Some(foot), Some(tick))
            }
        };

        Self {
            polyline,
            stroke_width: edge.width,
            arrow,
            foot,
            tick,
        }
    }
}

fn paint_edge(window: &mut Window, edge: &CanvasEdge, origin: (f32, f32)) {
    let geometry = EdgeGeometry::new(edge, origin);
    let color = edge.color;

    let mut builder = PathBuilder::stroke(px(geometry.stroke_width));
    if edge.dashed {
        // Dash on/off lengths are diagram stroke geometry, not UI spacing.
        builder = builder.dash_array(&[px(6.0), px(4.0)]); // guardrail-allow: diagram stroke geometry
    }
    for (position, &(x, y)) in geometry.polyline.iter().enumerate() {
        if position == 0 {
            builder.move_to(point(px(x), px(y)));
        } else {
            builder.line_to(point(px(x), px(y)));
        }
    }
    if let Ok(path) = builder.build() {
        window.paint_path(path, color);
    }

    let [tip, left, right] = geometry.arrow;
    let mut arrow_builder = PathBuilder::fill();
    arrow_builder.move_to(point(px(tip.0), px(tip.1)));
    arrow_builder.line_to(point(px(left.0), px(left.1)));
    arrow_builder.line_to(point(px(right.0), px(right.1)));
    arrow_builder.close();
    if let Ok(arrow_path) = arrow_builder.build() {
        window.paint_path(arrow_path, color);
    }

    if let Some(prongs) = geometry.foot {
        let mut foot_builder = PathBuilder::stroke(px(geometry.stroke_width));
        for (from, to) in prongs {
            foot_builder.move_to(point(px(from.0), px(from.1)));
            foot_builder.line_to(point(px(to.0), px(to.1)));
        }
        if let Ok(foot_path) = foot_builder.build() {
            window.paint_path(foot_path, color);
        }
    }

    if let Some((from, to)) = geometry.tick {
        let mut tick_builder = PathBuilder::stroke(px(geometry.stroke_width));
        tick_builder.move_to(point(px(from.0), px(from.1)));
        tick_builder.line_to(point(px(to.0), px(to.1)));
        if let Ok(tick_path) = tick_builder.build() {
            window.paint_path(tick_path, color);
        }
    }
}

/// Paints every edge on a single canvas element covering the diagram, in the
/// order given (later edges draw over earlier ones). `None` when there is
/// nothing to draw. Place the result among the diagram surface's children.
pub fn edge_overlay(edges: Vec<CanvasEdge>) -> Option<Div> {
    if edges.is_empty() {
        return None;
    }

    let canvas_element = canvas(
        |_bounds, _, _cx| {},
        move |bounds, _, window, _cx| {
            let origin = (bounds.origin.x.into(), bounds.origin.y.into());
            for edge in &edges {
                paint_edge(window, edge, origin);
            }
        },
    )
    .absolute()
    .top_0()
    .left_0()
    .w(px(OVERLAY_EXTENT))
    .h(px(OVERLAY_EXTENT));

    Some(div().absolute().top_0().left_0().child(canvas_element))
}

#[cfg(test)]
mod tests;
