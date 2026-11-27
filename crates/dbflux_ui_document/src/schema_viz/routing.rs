//! Edge routing now lives in `dbflux_diagram::routing`; the schema diagram routes
//! foreign keys with it and keeps these names for its tests.

pub(super) use dbflux_diagram::routing::{
    NodeBounds, OrthogonalRoute, RoutePoint, route_edge as route_foreign_key,
};
