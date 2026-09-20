//! Pure world-space geometry for schema foreign-key edges.

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct CubicRoute {
    pub start: RoutePoint,
    pub control1: RoutePoint,
    pub control2: RoutePoint,
    pub end: RoutePoint,
}

const EDGE_ANCHOR_GAP: f32 = 6.0;
const LOOP_CLEARANCE: f32 = 36.0;
const SELF_ROW_SPLIT: f32 = 5.0;
const LANE_SPACING: f32 = 14.0;

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

/// Computes the world-space cubic route for one foreign-key edge.
pub(super) fn route_foreign_key(
    source: NodeBounds,
    target: NodeBounds,
    source_row_y: f32,
    target_row_y: f32,
    lane_rank: usize,
    lane_count: usize,
) -> CubicRoute {
    let source_y = source.y + source_row_y;
    let target_y = target.y + target_row_y;
    let lane_offset = lane_offset(lane_rank, lane_count);

    if source == target {
        return self_route(source, source_y, target_y, lane_offset);
    }

    if source.right() <= target.left() {
        return facing_route(source, target, source_y, target_y, 1.0, lane_offset);
    }
    if target.right() <= source.left() {
        return facing_route(source, target, source_y, target_y, -1.0, lane_offset);
    }

    exterior_route(source, target, source_y, target_y, lane_offset)
}

fn lane_offset(lane_rank: usize, lane_count: usize) -> f32 {
    let count = lane_count.max(1);
    let rank = lane_rank.min(count.saturating_sub(1));
    (rank as f32 - (count.saturating_sub(1) as f32 / 2.0)) * LANE_SPACING
}

fn facing_route(
    source: NodeBounds,
    target: NodeBounds,
    source_y: f32,
    target_y: f32,
    direction: f32,
    lane_offset: f32,
) -> CubicRoute {
    let start = RoutePoint {
        x: if direction > 0.0 {
            source.right() + EDGE_ANCHOR_GAP
        } else {
            source.left() - EDGE_ANCHOR_GAP
        },
        y: source_y,
    };
    let end = RoutePoint {
        x: if direction > 0.0 {
            target.left() - EDGE_ANCHOR_GAP
        } else {
            target.right() + EDGE_ANCHOR_GAP
        },
        y: target_y,
    };
    let control_offset = (end.x - start.x).abs() / 2.0;

    CubicRoute {
        start,
        control1: RoutePoint {
            x: start.x + direction * control_offset,
            y: start.y + lane_offset,
        },
        control2: RoutePoint {
            x: end.x - direction * control_offset,
            y: end.y + lane_offset,
        },
        end,
    }
}

fn exterior_route(
    source: NodeBounds,
    target: NodeBounds,
    source_y: f32,
    target_y: f32,
    lane_offset: f32,
) -> CubicRoute {
    let use_left_side = source.center_x() <= target.center_x();
    let exterior_x = if use_left_side {
        source.left().min(target.left()) - LOOP_CLEARANCE
    } else {
        source.right().max(target.right()) + LOOP_CLEARANCE
    };
    let start = RoutePoint {
        x: if use_left_side {
            source.left() - EDGE_ANCHOR_GAP
        } else {
            source.right() + EDGE_ANCHOR_GAP
        },
        y: source_y,
    };
    let end = RoutePoint {
        x: if use_left_side {
            target.left() - EDGE_ANCHOR_GAP
        } else {
            target.right() + EDGE_ANCHOR_GAP
        },
        y: target_y,
    };

    CubicRoute {
        start,
        control1: RoutePoint {
            x: exterior_x,
            y: start.y + lane_offset,
        },
        control2: RoutePoint {
            x: exterior_x,
            y: end.y + lane_offset,
        },
        end,
    }
}

fn self_route(node: NodeBounds, source_y: f32, target_y: f32, lane_offset: f32) -> CubicRoute {
    let split_rows = (source_y - target_y).abs() < f32::EPSILON;
    let start = RoutePoint {
        x: node.left() - EDGE_ANCHOR_GAP,
        y: source_y - if split_rows { SELF_ROW_SPLIT } else { 0.0 },
    };
    let end = RoutePoint {
        x: node.left() - EDGE_ANCHOR_GAP,
        y: target_y + if split_rows { SELF_ROW_SPLIT } else { 0.0 },
    };
    let exterior_x = node.left() - LOOP_CLEARANCE - lane_offset.abs();

    CubicRoute {
        start,
        control1: RoutePoint {
            x: exterior_x,
            y: start.y + lane_offset,
        },
        control2: RoutePoint {
            x: exterior_x,
            y: end.y + lane_offset,
        },
        end,
    }
}
