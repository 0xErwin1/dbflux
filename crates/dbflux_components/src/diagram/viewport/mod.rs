//! Pan, zoom and fit math for a diagram canvas.
//!
//! Everything here is a pure function over values the consumer keeps in its own
//! state (`zoom`, `pan_offset`, the measured viewport). The canvas has no state of
//! its own, so a consumer can keep those fields where its tests and commands
//! already look for them.
//!
//! Two coordinate systems are involved. Graph space is where nodes are placed.
//! Viewport-local space is graph space after `screen = graph * zoom + pan`,
//! measured from the viewport's top-left corner.

use gpui::{Pixels, Point, Size, px};

/// Smallest zoom the canvas allows.
pub const ZOOM_MIN: f32 = 0.25;

/// Largest zoom the canvas allows.
pub const ZOOM_MAX: f32 = 4.0;

/// Factor one zoom-in or zoom-out step multiplies the zoom by.
pub const ZOOM_STEP: f32 = 1.25;

/// Factor one scroll-wheel notch zooms in by; out is its mirror.
const WHEEL_ZOOM_IN: f32 = 1.1;
const WHEEL_ZOOM_OUT: f32 = 0.9;

/// Largest zoom the Fit button picks, so a small diagram is not blown up.
pub const FIT_MAX_ZOOM: f32 = 1.5;

/// Largest zoom the fit on open picks: a diagram that already fits opens at
/// its natural size.
pub const OPEN_FIT_MAX_ZOOM: f32 = 1.0;

/// Screen margin kept around a fitted diagram, on every side.
pub const FIT_MARGIN: f32 = 48.0;

/// Spacing of the diagram's dot grid, in graph coordinates. Node drags and
/// keyboard nudges land on this lattice, so nodes line up instead of drifting.
pub const GRID_LATTICE: f32 = 24.0;

/// Zoom after one zoom-in step, never above [`ZOOM_MAX`].
pub fn zoom_in(zoom: f32) -> f32 {
    (zoom * ZOOM_STEP).min(ZOOM_MAX)
}

/// Zoom after one zoom-out step, never below [`ZOOM_MIN`].
pub fn zoom_out(zoom: f32) -> f32 {
    (zoom / ZOOM_STEP).max(ZOOM_MIN)
}

/// Whether a zoom-in step would change anything.
pub fn can_zoom_in(zoom: f32) -> bool {
    zoom < ZOOM_MAX
}

/// Whether a zoom-out step would change anything.
pub fn can_zoom_out(zoom: f32) -> bool {
    zoom > ZOOM_MIN
}

/// Rounds a graph-space coordinate onto the diagram lattice.
pub fn snap_to_lattice(value: f32) -> f32 {
    (value / GRID_LATTICE).round() * GRID_LATTICE
}

/// Zoom and pan after a scroll-wheel step at `mouse`, which keeps the graph point
/// under the cursor fixed. `delta_y > 0` zooms in. `None` when the zoom would not
/// change (already at a limit).
///
/// `pan` is the effective pan (see [`pixel_aligned_pan`]) the diagram is drawn
/// with, so the point under the cursor is the one the user sees.
pub fn wheel_zoom(
    mouse: Point<Pixels>,
    pan: Point<Pixels>,
    zoom: f32,
    delta_y: Pixels,
) -> Option<(f32, Point<Pixels>)> {
    let factor = if delta_y > px(0.0) {
        WHEEL_ZOOM_IN
    } else {
        WHEEL_ZOOM_OUT
    };
    let new_zoom = (zoom * factor).clamp(ZOOM_MIN, ZOOM_MAX);

    if (new_zoom - zoom).abs() < 0.001 {
        return None;
    }

    // graph_pt = (screen - pan) / old_zoom
    // new_pan  = screen - graph_pt * new_zoom
    let graph_x = (mouse.x - pan.x) / zoom;
    let graph_y = (mouse.y - pan.y) / zoom;
    let new_pan = Point::new(mouse.x - graph_x * new_zoom, mouse.y - graph_y * new_zoom);

    Some((new_zoom, new_pan))
}

/// Zoom and pan that fit the graph-space box `(min_x, min_y, max_x, max_y)`
/// inside `viewport` with [`FIT_MARGIN`] on every side, anchored at the
/// top-left margin, at no more than `max_zoom`. `None` for an empty box or an
/// unmeasured viewport.
pub fn fit_diagram(
    (min_x, min_y, max_x, max_y): (f32, f32, f32, f32),
    viewport: Size<Pixels>,
    max_zoom: f32,
) -> Option<(f32, Point<Pixels>)> {
    if min_x > max_x || min_y > max_y {
        return None;
    }

    let viewport_width: f32 = viewport.width.into();
    let viewport_height: f32 = viewport.height.into();
    if viewport_width <= 0.0 || viewport_height <= 0.0 {
        return None;
    }

    let content_width = (max_x - min_x).max(1.0);
    let content_height = (max_y - min_y).max(1.0);
    let available_width = (viewport_width - 2.0 * FIT_MARGIN).max(1.0);
    let available_height = (viewport_height - 2.0 * FIT_MARGIN).max(1.0);

    let zoom = (available_width / content_width)
        .min(available_height / content_height)
        .clamp(ZOOM_MIN, max_zoom.max(ZOOM_MIN));

    let pan = Point::new(px(FIT_MARGIN - min_x * zoom), px(FIT_MARGIN - min_y * zoom));

    Some((zoom, pan))
}

/// The bounding box `(min_x, min_y, max_x, max_y)` of graph-space rectangles
/// `(x, y, width, height)`, or `None` when there are none. Feed it to
/// [`fit_diagram`].
pub fn content_bounds(
    rects: impl IntoIterator<Item = (f32, f32, f32, f32)>,
) -> Option<(f32, f32, f32, f32)> {
    rects
        .into_iter()
        .fold(None, |bounds, (x, y, width, height)| {
            let (min_x, min_y, max_x, max_y) =
                bounds.unwrap_or((f32::MAX, f32::MAX, f32::MIN, f32::MIN));
            Some((
                min_x.min(x),
                min_y.min(y),
                max_x.max(x + width),
                max_y.max(y + height),
            ))
        })
}

/// Pan that centres graph-space point `(graph_x, graph_y)` in the viewport at
/// `zoom`. An unmeasured viewport centres in an 800 x 600 one.
pub fn center_on(graph_x: f32, graph_y: f32, viewport: Size<Pixels>, zoom: f32) -> Point<Pixels> {
    let viewport_width: f32 = if viewport.width > px(0.0) {
        viewport.width.into()
    } else {
        800.0
    };
    let viewport_height: f32 = if viewport.height > px(0.0) {
        viewport.height.into()
    } else {
        600.0
    };

    Point::new(
        px(viewport_width / 2.0 - graph_x * zoom),
        px(viewport_height / 2.0 - graph_y * zoom),
    )
}

/// A window-absolute position relative to the viewport's top-left corner.
pub fn viewport_local_position(
    window_position: Point<Pixels>,
    viewport_origin: Point<Pixels>,
) -> Point<Pixels> {
    Point::new(
        window_position.x - viewport_origin.x,
        window_position.y - viewport_origin.y,
    )
}

/// The graph-space position a dragged node's top-left corner takes when the
/// pointer is at `local` and grabbed the node `drag_offset` from its corner.
pub fn dragged_node_position(
    local: Point<Pixels>,
    drag_offset: Point<Pixels>,
    pan: Point<Pixels>,
    zoom: f32,
) -> (f32, f32) {
    let screen_x: f32 = local.x.into();
    let screen_y: f32 = local.y.into();
    let offset_x: f32 = drag_offset.x.into();
    let offset_y: f32 = drag_offset.y.into();
    let pan_x: f32 = pan.x.into();
    let pan_y: f32 = pan.y.into();

    (
        (screen_x - offset_x - pan_x) / zoom,
        (screen_y - offset_y - pan_y) / zoom,
    )
}

/// The pan the diagram is drawn with: `pan_offset` moved so the transform's
/// translation lands on a physical pixel, which keeps node borders and text
/// sharp. The viewport is the transform's pivot, so its origin enters the math.
/// An invalid display scale leaves the pan as it is.
pub fn pixel_aligned_pan(
    pan_offset: Point<Pixels>,
    viewport_origin: Point<Pixels>,
    zoom: f32,
    scale_factor: f32,
) -> Point<Pixels> {
    if !scale_factor.is_finite() || scale_factor <= 0.0 {
        return pan_offset;
    }

    let pivot_translation = viewport_origin * (1.0 - zoom);
    let matrix_translation = pan_offset + pivot_translation;
    Point::new(
        px((f32::from(matrix_translation.x) * scale_factor).round() / scale_factor)
            - pivot_translation.x,
        px((f32::from(matrix_translation.y) * scale_factor).round() / scale_factor)
            - pivot_translation.y,
    )
}

#[cfg(test)]
mod tests;
