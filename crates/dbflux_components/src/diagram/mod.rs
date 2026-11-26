//! A domain-free canvas for node-and-edge diagrams.
//!
//! The canvas knows boxes, routed edges, a camera and a selection; it knows
//! nothing about what a node means. A consumer lays its graph out with
//! `dbflux_diagram`, then builds the canvas from these parts inside a viewport
//! it owns:
//!
//! - [`viewport`]: pan, zoom, fit and drag math over the consumer's own state.
//! - [`surface`]: the transformed layer, the dot grid and the bounds probe.
//! - [`edges`]: edges drawn with a colour, a stroke width and end markers.
//! - [`node`]: the node card and its header / body / footer slots; the content of
//!   the slots, and the handlers on the card, come from the consumer.
//!
//! Selection and keyboard navigation between nodes are in
//! `dbflux_diagram::navigation`; the consumer keeps the selected id.

pub mod edges;
pub mod node;
pub mod surface;
pub mod viewport;

pub use edges::{CanvasEdge, DEFAULT_EDGE_WIDTH, EdgeEnds, EdgeGeometry, edge_overlay};
pub use node::{NodeCard, node_body, node_header};
pub use surface::{diagram_surface, dot_grid, measure_bounds};
pub use viewport::{
    FIT_MARGIN, FIT_MAX_ZOOM, GRID_LATTICE, OPEN_FIT_MAX_ZOOM, ZOOM_MAX, ZOOM_MIN, ZOOM_STEP,
    can_zoom_in, can_zoom_out, center_on, content_bounds, dragged_node_position, fit_diagram,
    pixel_aligned_pan, snap_to_lattice, viewport_local_position, wheel_zoom, zoom_in, zoom_out,
};
