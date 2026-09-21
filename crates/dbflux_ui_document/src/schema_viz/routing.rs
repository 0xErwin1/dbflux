//! Pure world-space geometry for schema foreign-key edges.
//!
//! Routes are axis-aligned polylines. A camera-free ER diagram is read by
//! following a line from a column to the table it references, so the router
//! keeps every edge inside the gutters between nodes instead of drawing a curve
//! across the canvas: it leaves the source sideways, travels in a vertical lane
//! placed in the gutter between the two columns, and enters the target sideways.
//! Edges that would have to cross a node (the target sits behind the source, or
//! both share a column) are routed around the outside of the pair.

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct NodeBounds {
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct RoutePoint {
    pub x: f32,
    pub y: f32,
}

/// An axis-aligned polyline from the source anchor to the target anchor.
///
/// Consecutive points always share an x or a y, so the renderer can draw it with
/// straight segments and the UI can read the entry direction off the last one.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct OrthogonalRoute {
    pub points: Vec<RoutePoint>,
}

impl OrthogonalRoute {
    fn two_ended(points: Vec<RoutePoint>) -> Self {
        Self { points }
    }

    pub fn start(&self) -> RoutePoint {
        self.points
            .first()
            .copied()
            .unwrap_or(RoutePoint { x: 0.0, y: 0.0 })
    }

    pub fn end(&self) -> RoutePoint {
        self.points
            .last()
            .copied()
            .unwrap_or(RoutePoint { x: 0.0, y: 0.0 })
    }

    /// Unit direction of the final segment, which is what the arrowhead follows.
    pub fn end_direction(&self) -> (f32, f32) {
        let count = self.points.len();
        if count < 2 {
            return (0.0, 0.0);
        }
        let (Some(from), Some(to)) = (self.points.get(count - 2), self.points.get(count - 1))
        else {
            return (0.0, 0.0);
        };
        unit_between(*from, *to)
    }

    /// Unit direction the edge leaves the source with, used for the crow's foot.
    pub fn start_direction(&self) -> (f32, f32) {
        let (Some(from), Some(to)) = (self.points.first(), self.points.get(1)) else {
            return (0.0, 0.0);
        };
        unit_between(*from, *to)
    }
}

fn unit_between(from: RoutePoint, to: RoutePoint) -> (f32, f32) {
    let dx = to.x - from.x;
    let dy = to.y - from.y;
    let magnitude = (dx * dx + dy * dy).sqrt();
    if magnitude <= f32::EPSILON {
        return (0.0, 0.0);
    }
    (dx / magnitude, dy / magnitude)
}

/// Distance kept between a node edge and the first or last segment.
const EDGE_ANCHOR_GAP: f32 = 6.0;
/// Separation between two parallel edges joining the same pair of tables.
const LANE_SPACING: f32 = 14.0;
/// How far a route that goes around a node pair stays outside the node border.
const CORRIDOR_CLEARANCE: f32 = 24.0;
/// Vertical split applied to a self reference whose ends land on the same row.
const SELF_ROW_SPLIT: f32 = 5.0;

impl NodeBounds {
    fn left(self) -> f32 {
        self.x
    }

    fn right(self) -> f32 {
        self.x + self.width
    }

    fn center_x(self) -> f32 {
        self.x + self.width / 2.0
    }
}

/// Computes the world-space polyline for one foreign-key edge.
///
/// `source_row_y` and `target_row_y` are offsets inside each node, so the edge
/// leaves and enters at the row of the referencing and referenced columns.
pub(super) fn route_foreign_key(
    source: NodeBounds,
    target: NodeBounds,
    source_row_y: f32,
    target_row_y: f32,
    lane_rank: usize,
    lane_count: usize,
) -> OrthogonalRoute {
    let source_y = source.y + source_row_y;
    let target_y = target.y + target_row_y;
    let lane_offset = lane_offset(lane_rank, lane_count);

    if source == target {
        return self_route(source, source_y, target_y, lane_offset);
    }

    if source.right() <= target.left() {
        return between_columns_route(source, target, source_y, target_y, lane_offset, true);
    }
    if target.right() <= source.left() {
        return between_columns_route(source, target, source_y, target_y, lane_offset, false);
    }

    let use_left_side = source.center_x() <= target.center_x();
    exterior_route(
        source,
        target,
        source_y,
        target_y,
        lane_offset,
        use_left_side,
    )
}

/// Signed offset for one edge of a bundle, centred on zero so a single edge runs
/// straight down the middle of its corridor.
fn lane_offset(lane_rank: usize, lane_count: usize) -> f32 {
    let count = lane_count.max(1);
    let rank = lane_rank.min(count.saturating_sub(1));
    (rank as f32 - (count.saturating_sub(1) as f32 / 2.0)) * LANE_SPACING
}

/// Route between two nodes that face each other, running the vertical leg in the
/// gutter that separates their columns.
fn between_columns_route(
    source: NodeBounds,
    target: NodeBounds,
    source_y: f32,
    target_y: f32,
    lane_offset: f32,
    target_is_right: bool,
) -> OrthogonalRoute {
    let (exit_x, entry_x, corridor_low, corridor_high) = if target_is_right {
        (
            source.right() + EDGE_ANCHOR_GAP,
            target.left() - EDGE_ANCHOR_GAP,
            source.right() + EDGE_ANCHOR_GAP,
            target.left() - EDGE_ANCHOR_GAP,
        )
    } else {
        (
            source.left() - EDGE_ANCHOR_GAP,
            target.right() + EDGE_ANCHOR_GAP,
            target.right() + EDGE_ANCHOR_GAP,
            source.left() - EDGE_ANCHOR_GAP,
        )
    };

    // Keep the lane inside the corridor: a bundle wider than the gutter would
    // otherwise push its outermost edges into a node.
    let lane_x = ((exit_x + entry_x) / 2.0 + lane_offset).clamp(corridor_low, corridor_high);

    OrthogonalRoute::two_ended(vec![
        RoutePoint {
            x: exit_x,
            y: source_y,
        },
        RoutePoint {
            x: lane_x,
            y: source_y,
        },
        RoutePoint {
            x: lane_x,
            y: target_y,
        },
        RoutePoint {
            x: entry_x,
            y: target_y,
        },
    ])
}

/// Route between two nodes that overlap horizontally: leave and enter on the same
/// side, travelling around the outside of the pair.
fn exterior_route(
    source: NodeBounds,
    target: NodeBounds,
    source_y: f32,
    target_y: f32,
    lane_offset: f32,
    use_left_side: bool,
) -> OrthogonalRoute {
    let outside_x = if use_left_side {
        source.left().min(target.left()) - CORRIDOR_CLEARANCE - lane_offset.abs()
    } else {
        source.right().max(target.right()) + CORRIDOR_CLEARANCE + lane_offset.abs()
    };
    let (exit_x, entry_x) = if use_left_side {
        (
            source.left() - EDGE_ANCHOR_GAP,
            target.left() - EDGE_ANCHOR_GAP,
        )
    } else {
        (
            source.right() + EDGE_ANCHOR_GAP,
            target.right() + EDGE_ANCHOR_GAP,
        )
    };

    OrthogonalRoute::two_ended(vec![
        RoutePoint {
            x: exit_x,
            y: source_y,
        },
        RoutePoint {
            x: outside_x,
            y: source_y,
        },
        RoutePoint {
            x: outside_x,
            y: target_y,
        },
        RoutePoint {
            x: entry_x,
            y: target_y,
        },
    ])
}

/// Self reference: leave the left border, loop away from the node and come back
/// to the other row.
fn self_route(node: NodeBounds, source_y: f32, target_y: f32, lane_offset: f32) -> OrthogonalRoute {
    let split = if (source_y - target_y).abs() < f32::EPSILON {
        SELF_ROW_SPLIT
    } else {
        0.0
    };
    let anchor_x = node.left() - EDGE_ANCHOR_GAP;
    let outside_x = node.left() - CORRIDOR_CLEARANCE - lane_offset.abs();

    OrthogonalRoute::two_ended(vec![
        RoutePoint {
            x: anchor_x,
            y: source_y - split,
        },
        RoutePoint {
            x: outside_x,
            y: source_y - split,
        },
        RoutePoint {
            x: outside_x,
            y: target_y + split,
        },
        RoutePoint {
            x: anchor_x,
            y: target_y + split,
        },
    ])
}
