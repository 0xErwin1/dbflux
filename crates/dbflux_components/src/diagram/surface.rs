//! The surface a diagram is drawn on: the transformed layer that holds the dot
//! grid, the edges and the nodes, and a probe that reports the viewport's size.

use gpui::prelude::*;
use gpui::{
    AnyElement, App, Bounds, Div, ElementTransform, Hsla, Pixels, Point, Window, canvas, div, px,
    size,
};

use super::viewport::GRID_LATTICE;
use crate::tokens::SchemaMetrics;

/// Side of the square the dot grid covers, in graph coordinates.
const DOT_EXTENT: f32 = 3000.0;

/// The dot grid: a [`SchemaMetrics::DOT`] square at every lattice intersection,
/// painted by a single canvas element. Put it first among the surface's children.
pub fn dot_grid(color: Hsla) -> AnyElement {
    let dot_size = SchemaMetrics::DOT;
    canvas(
        |_bounds, _, _cx| {},
        move |bounds, _, window, _cx| {
            let origin_x: f32 = bounds.origin.x.into();
            let origin_y: f32 = bounds.origin.y.into();
            let steps = (DOT_EXTENT / GRID_LATTICE) as usize + 1;
            for column in 0..steps {
                for row in 0..steps {
                    let dot_x = column as f32 * GRID_LATTICE + origin_x;
                    let dot_y = row as f32 * GRID_LATTICE + origin_y;
                    let dot_bounds = Bounds::centered_at(
                        Point::new(px(dot_x), px(dot_y)),
                        gpui::Size {
                            width: dot_size,
                            height: dot_size,
                        },
                    );
                    window.paint_quad(gpui::fill(dot_bounds, color));
                }
            }
        },
    )
    .absolute()
    .top_0()
    .left_0()
    .w(px(DOT_EXTENT))
    .h(px(DOT_EXTENT))
    .into_any_element()
}

/// The layer nodes and edges are placed on: `children` in graph coordinates,
/// translated by `pan` and scaled by `zoom`. `pan` is the effective pan from
/// [`super::viewport::pixel_aligned_pan`].
pub fn diagram_surface(
    background: Hsla,
    pan: Point<Pixels>,
    zoom: f32,
    children: Vec<AnyElement>,
) -> Div {
    div()
        .relative()
        .size_full()
        .bg(background)
        .transform(
            ElementTransform::default()
                .translate(pan)
                .scale(size(zoom, zoom)),
        )
        .children(children)
}

/// An invisible element that fills its parent and calls `on_bounds` with the
/// bounds it was laid out at, before paint. A viewport uses it to learn its origin
/// and size; a panel uses it to learn where it sits in the window.
pub fn measure_bounds(
    on_bounds: impl FnOnce(Bounds<Pixels>, &mut App) + 'static,
) -> impl IntoElement {
    canvas(
        move |bounds: Bounds<Pixels>, _: &mut Window, cx: &mut App| on_bounds(bounds, cx),
        |_: Bounds<Pixels>, _, _, _| {},
    )
    .absolute()
    .size_full()
}
