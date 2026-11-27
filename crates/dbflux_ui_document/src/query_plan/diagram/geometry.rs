//! Where the diagram puts each node and edge, and how the camera frames
//! them. Pure geometry over the [`PlanModel`]; the drawing is in the parent
//! module.

use dbflux_diagram::{
    Direction, LayoutNode, NodeBounds, OrthogonalRoute, layered_tree_right, next_in_direction,
    route_edge,
};
use gpui::{Pixels, Point, Size, px};

use super::super::model::{NodeId, PlanModel, PlanNodeView};

/// Width of a node card, in graph pixels.
pub(crate) const NODE_WIDTH: f32 = 248.0;
/// Height of the card header.
pub(crate) const HEADER_PX: f32 = 28.0;
/// Height of one body row.
pub(crate) const ROW_PX: f32 = 20.0;
/// Space under the last row.
pub(crate) const BODY_PADDING_PX: f32 = 6.0;

/// Thinnest and thickest edge, in pixels. The width follows the child's row
/// estimate on a log scale between them.
pub(crate) const MIN_EDGE_WIDTH: f32 = 1.0;
pub(crate) const MAX_EDGE_WIDTH: f32 = 6.0;
/// Edge widths snap to this step, so two nearly equal estimates draw alike.
const EDGE_WIDTH_STEP: f32 = 0.5;

/// Smallest zoom Fit picks. Below it the card text is too small to read, so
/// Fit stops here and the user pans the rest.
pub(crate) const MIN_FIT_ZOOM: f32 = 0.85;

/// The body rows a node card draws. Each is present only when the node has
/// the data for it, so a plan without costs draws short cards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CardRows {
    pub relation: bool,
    pub index: bool,
    pub condition: bool,
    pub cost: bool,
    /// Actual time, loops and rows of an analyzed node.
    pub actual: bool,
    pub share: bool,
}

impl CardRows {
    pub(crate) fn of(node: &PlanNodeView) -> Self {
        Self {
            relation: node.relation.is_some(),
            index: node.index.is_some(),
            condition: node.key_condition.is_some(),
            cost: node.total_cost.is_some() || node.rows.is_some(),
            actual: node.actual_time.is_some() || node.actual_rows.is_some(),
            share: node.share.is_some(),
        }
    }

    fn count(self) -> usize {
        [
            self.relation,
            self.index,
            self.condition,
            self.cost,
            self.actual,
            self.share,
        ]
        .into_iter()
        .filter(|present| *present)
        .count()
    }

    /// Height of the whole card.
    pub(crate) fn height(self) -> f32 {
        HEADER_PX + self.count() as f32 * ROW_PX + BODY_PADDING_PX
    }
}

/// One edge: from a child to the node that consumes its rows.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DiagramEdge {
    pub from: NodeId,
    pub to: NodeId,
    pub rows: Option<f64>,
    /// Whether `rows` were counted by running the query; otherwise they are
    /// the engine's estimate, even in an analyzed plan.
    pub rows_are_actual: bool,
    pub width: f32,
    pub route: OrthogonalRoute,
    /// Where the row-count label sits, in graph space.
    pub label_at: (f32, f32),
}

/// The placed diagram.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DiagramGeometry {
    /// One entry per node, indexed by [`NodeId`].
    pub nodes: Vec<NodeBounds>,
    pub edges: Vec<DiagramEdge>,
}

impl DiagramGeometry {
    /// Lays the plan out left to right, leaves first and the root last, and
    /// routes an edge from every child to its parent.
    pub(crate) fn build(model: &PlanModel) -> Self {
        let layout_nodes: Vec<LayoutNode> = model
            .nodes
            .iter()
            .map(|node| LayoutNode {
                width: NODE_WIDTH,
                height: CardRows::of(node).height(),
                sort_key: format!("{:08}", node.id),
            })
            .collect();

        let max_rows = model.max_rows();
        let parents: Vec<Option<usize>> = model.nodes.iter().map(|node| node.parent).collect();

        let layout = layered_tree_right(&layout_nodes, &parents);
        let nodes: Vec<NodeBounds> = layout
            .nodes
            .iter()
            .map(|placed| NodeBounds {
                x: placed.x,
                y: placed.y,
                width: placed.width,
                height: placed.height,
            })
            .collect();

        let edges = model
            .nodes
            .iter()
            .filter_map(|node| {
                let parent = node.parent?;
                let source = *nodes.get(node.id)?;
                let target = *nodes.get(parent)?;
                let route = route_edge(
                    source,
                    target,
                    source.height / 2.0,
                    target.height / 2.0,
                    0,
                    1,
                    &nodes,
                );

                Some(DiagramEdge {
                    from: node.id,
                    to: parent,
                    rows: node.edge_rows,
                    rows_are_actual: node.actual_rows.is_some(),
                    width: edge_width(node.edge_rows, max_rows),
                    label_at: label_anchor(&route),
                    route,
                })
            })
            .collect();

        Self { nodes, edges }
    }

    /// The bounding box `(min_x, min_y, max_x, max_y)` of every node.
    pub(crate) fn bounds(&self) -> Option<(f32, f32, f32, f32)> {
        dbflux_components::diagram::content_bounds(
            self.nodes
                .iter()
                .map(|node| (node.x, node.y, node.width, node.height)),
        )
    }
}

/// Stroke width of an edge carrying `rows`, on a log scale against the plan's
/// largest estimate: the busiest edge is [`MAX_EDGE_WIDTH`], one row is
/// [`MIN_EDGE_WIDTH`]. Without an estimate, or without anything to compare it
/// to, every edge draws at the same default width so the diagram never
/// suggests a volume it does not know.
pub(crate) fn edge_width(rows: Option<f64>, max_rows: Option<f64>) -> f32 {
    let (Some(rows), Some(max_rows)) = (rows, max_rows) else {
        return dbflux_components::diagram::DEFAULT_EDGE_WIDTH;
    };
    if max_rows <= 0.0 {
        return dbflux_components::diagram::DEFAULT_EDGE_WIDTH;
    }

    let fraction = ((1.0 + rows.max(0.0)).ln() / (1.0 + max_rows).ln()).clamp(0.0, 1.0) as f32;
    let width = MIN_EDGE_WIDTH + fraction * (MAX_EDGE_WIDTH - MIN_EDGE_WIDTH);

    (width / EDGE_WIDTH_STEP).round() * EDGE_WIDTH_STEP
}

/// Where an edge's row-count label sits: the middle of the route's longest
/// horizontal segment. That is clear of the arrowhead (which is at the end of
/// the last segment) and of the vertical lane, unlike the middle of the whole
/// route, which can land on either.
pub(crate) fn label_anchor(route: &OrthogonalRoute) -> (f32, f32) {
    let longest = route
        .points
        .windows(2)
        .filter_map(|pair| match pair {
            [from, to] if (from.y - to.y).abs() < 0.01 => {
                Some(((to.x - from.x).abs(), (from.x + to.x) / 2.0, from.y))
            }
            _ => None,
        })
        .fold(None, |best: Option<(f32, f32, f32)>, segment| match best {
            Some(current) if current.0 >= segment.0 => Some(current),
            _ => Some(segment),
        });

    match longest {
        Some((_, x, y)) => (x, y),
        None => {
            let start = route.start();
            (start.x, start.y)
        }
    }
}

/// Zoom and pan that frame the diagram in the viewport, never below
/// [`MIN_FIT_ZOOM`]: a big plan is shown at a readable size, anchored at its
/// top-left corner, and the user pans for the rest. `max_zoom` caps how far a
/// small plan is blown up.
pub(crate) fn fit_view(
    bounds: (f32, f32, f32, f32),
    viewport: Size<Pixels>,
    max_zoom: f32,
) -> Option<(f32, Point<Pixels>)> {
    let (zoom, pan) = dbflux_components::diagram::fit_diagram(bounds, viewport, max_zoom)?;

    if zoom >= MIN_FIT_ZOOM {
        return Some((zoom, pan));
    }

    let (min_x, min_y, _, _) = bounds;
    let margin = dbflux_components::diagram::FIT_MARGIN;
    Some((
        MIN_FIT_ZOOM,
        Point::new(
            px(margin - min_x * MIN_FIT_ZOOM),
            px(margin - min_y * MIN_FIT_ZOOM),
        ),
    ))
}

/// The node an arrow key selects from `current`.
///
/// Left and Right use the shared navigation over every node. Up and Down use
/// it over the node's own column first: the diagram reads left to right, so
/// "down" means the sibling below in the same layer, not whichever node of
/// another layer happens to sit lower. A node alone in its column falls back
/// to every node, so the key still goes somewhere.
pub(crate) fn navigate(
    nodes: &[NodeBounds],
    current: NodeId,
    direction: Direction,
) -> Option<NodeId> {
    let all: Vec<(NodeId, NodeBounds)> = nodes.iter().copied().enumerate().collect();

    let in_column = matches!(direction, Direction::Up | Direction::Down)
        .then(|| {
            let x = nodes.get(current)?.x;
            let column: Vec<_> = all
                .iter()
                .copied()
                .filter(|(_, bounds)| (bounds.x - x).abs() < 1.0)
                .collect();
            (column.len() > 1).then_some(column)
        })
        .flatten();

    next_in_direction(in_column.as_deref().unwrap_or(&all), current, direction)
}
