//! Domain-free geometry for node-and-edge diagrams.
//!
//! The crate knows boxes with a size, directed edges between them and nothing
//! else: no GPUI, no schema, no query plan. A consumer describes its diagram as
//! [`layout::LayoutNode`]s and [`layout::LayoutEdge`]s, places them with one of
//! the layouts, routes the edges with [`routing`] and moves a selection between
//! them with [`navigation`]. The GPUI canvas that draws the result lives in
//! `dbflux_components::diagram`.

pub mod layout;
pub mod navigation;
pub mod routing;

pub use layout::{
    DiagramLayout, LayoutEdge, LayoutNode, PlacedEdge, PlacedNode, compact_grid,
    layered_left_right, radial,
};
pub use navigation::{Direction, next_in_direction, spatial_order};
pub use routing::{NodeBounds, OrthogonalRoute, RoutePoint, route_edge};
