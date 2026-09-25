use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use gpui::prelude::*;
use gpui::{
    App, Bounds, DispatchPhase, ElementId, Hitbox, HitboxBehavior, Hsla, MouseButton,
    MouseDownEvent, MouseMoveEvent, MouseUpEvent, PathBuilder, Pixels, Point, Rgba, Window, canvas,
    point, px,
};

use crate::tokens::{Anim, Borders};

/// Which corners of the rectangle are cut at 45°.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ChamferCorners {
    /// Top-left and bottom-right corners are cut; the other two stay square.
    #[default]
    TopLeftBottomRight,
    /// Only the top-left corner is cut (document tabs, the main action of a
    /// split button).
    TopLeft,
    /// Only the bottom-right corner is cut (the menu segment of a split
    /// button).
    BottomRight,
}

/// A straight edge painted along the top, bottom or left side of the shape, inside
/// its bounds and following the shape outline where the side meets a cut.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChamferEdge {
    pub color: Hsla,
    pub thickness: Pixels,
}

/// How a control's rest fill reads, which decides where its focus ring sits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChamferFillKind {
    /// Solid accent or danger fill: the ring sits outside, so it stays
    /// visible against the fill.
    Filled,
    /// Surface, bordered, or transparent fill: the ring sits inside.
    Surface,
}

/// A ring stroked along the full outline of the shape, diagonals included.
///
/// `offset` has the meaning of CSS `outline-offset`: the distance from the
/// shape's edge to the ring's inner side, positive outward. A negative offset
/// equal to the thickness keeps the whole ring inside the bounds. A ring
/// outside the bounds paints past the element's box, so an ancestor that
/// clips its overflow also clips the ring.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChamferRing {
    pub color: Hsla,
    pub thickness: Pixels,
    pub offset: Pixels,
}

impl ChamferRing {
    /// Keyboard focus ring: `Borders::FOCUS_RING` thick, fully inside the
    /// bounds.
    pub fn focus(color: impl Into<Hsla>) -> Self {
        Self {
            color: color.into(),
            thickness: Borders::FOCUS_RING,
            offset: -Borders::FOCUS_RING,
        }
    }

    /// Keyboard focus ring for a control of the given fill kind: inside the
    /// bounds for surfaces, `Borders::MEDIUM` outside them for filled
    /// controls.
    pub fn focus_for(color: impl Into<Hsla>, kind: ChamferFillKind) -> Self {
        let ring = Self::focus(color);

        match kind {
            ChamferFillKind::Filled => ring.outside(Borders::MEDIUM),
            ChamferFillKind::Surface => ring,
        }
    }

    /// The same ring moved `offset` outside the bounds.
    pub fn outside(mut self, offset: Pixels) -> Self {
        self.offset = offset;
        self
    }
}

/// Fill and border colors of a chamfered shape.
///
/// `fill_hover` and `fill_active` only take effect when the shape is made
/// interactive with [`Chamfer::interactive`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChamferColors {
    pub fill: Hsla,
    pub fill_hover: Option<Hsla>,
    pub fill_active: Option<Hsla>,
    pub border: Option<Hsla>,
}

impl ChamferColors {
    /// Resolves the fill for an interaction state. Pressed wins over hovered,
    /// and a missing state color falls back to the next less specific one.
    pub fn fill_for(&self, hovered: bool, pressed: bool) -> Hsla {
        let hover_fill = self.fill_hover.unwrap_or(self.fill);

        if pressed {
            return self.fill_active.unwrap_or(hover_fill);
        }

        if hovered {
            return hover_fill;
        }

        self.fill
    }
}

impl Default for ChamferColors {
    fn default() -> Self {
        Self {
            fill: gpui::transparent_black(),
            fill_hover: None,
            fill_active: None,
            border: None,
        }
    }
}

/// Clamps a cut depth to at most half of the smaller side of `size`, and to
/// zero for empty or negative sizes, so the polygon never self-intersects.
pub fn clamp_cut(size: gpui::Size<Pixels>, cut: Pixels) -> Pixels {
    let width = f32::from(size.width);
    let height = f32::from(size.height);

    if width <= 0.0 || height <= 0.0 {
        return Pixels::ZERO;
    }

    let limit = width.min(height) / 2.0;
    px(f32::from(cut).clamp(0.0, limit))
}

/// Outline of the chamfered shape in the same coordinate space as `bounds`,
/// clockwise from the top edge. Returns no points for empty bounds and never
/// repeats a vertex, so a zero cut yields the plain rectangle.
pub fn chamfer_points(
    bounds: Bounds<Pixels>,
    cut: Pixels,
    corners: ChamferCorners,
) -> Vec<Point<Pixels>> {
    to_pixel_points(&shape_polygon(bounds, cut, corners))
}

/// Pieces of a 1 px-style inset border of `thickness` covering the straight
/// sides of the shape only: the diagonal cuts get no border, which matches an
/// inset box-shadow clipped by the shape. The four pieces do not overlap, so
/// translucent border colors stay uniform.
pub fn chamfer_border_polygons(
    bounds: Bounds<Pixels>,
    cut: Pixels,
    corners: ChamferCorners,
    thickness: Pixels,
) -> Vec<Vec<Point<Pixels>>> {
    let shape = shape_polygon(bounds, cut, corners);
    let thickness = f32::from(thickness);

    if shape.is_empty() || thickness <= 0.0 {
        return Vec::new();
    }

    let (left, top, right, bottom) = edges_of(bounds);
    let thickness = thickness
        .min((right - left) / 2.0)
        .min((bottom - top) / 2.0);

    let bands = [
        (left, top, right, top + thickness),
        (left, bottom - thickness, right, bottom),
        (left, top + thickness, left + thickness, bottom - thickness),
        (
            right - thickness,
            top + thickness,
            right,
            bottom - thickness,
        ),
    ];

    bands
        .into_iter()
        .filter_map(|band| clip_to_band(&shape, band))
        .map(|polygon| to_pixel_points(&polygon))
        .collect()
}

/// Region of the shape within `thickness` of its top side. It starts where the
/// top side meets the cut and follows the diagonal down, like an inset
/// box-shadow on the top edge clipped by the shape.
pub fn chamfer_top_edge_polygon(
    bounds: Bounds<Pixels>,
    cut: Pixels,
    corners: ChamferCorners,
    thickness: Pixels,
) -> Vec<Point<Pixels>> {
    let (left, top, right, bottom) = edges_of(bounds);
    let band_bottom = (top + f32::from(thickness)).min(bottom);

    band_polygon(bounds, cut, corners, (left, top, right, band_bottom))
}

/// Region of the shape within `thickness` of its bottom side.
pub fn chamfer_bottom_edge_polygon(
    bounds: Bounds<Pixels>,
    cut: Pixels,
    corners: ChamferCorners,
    thickness: Pixels,
) -> Vec<Point<Pixels>> {
    let (left, top, right, bottom) = edges_of(bounds);
    let band_top = (bottom - f32::from(thickness)).max(top);

    band_polygon(bounds, cut, corners, (left, band_top, right, bottom))
}

/// Region of the shape within `thickness` of its left side. Where the left
/// side meets the top-left cut it follows the diagonal, like an inset
/// box-shadow on the left edge clipped by the shape.
pub fn chamfer_left_edge_polygon(
    bounds: Bounds<Pixels>,
    cut: Pixels,
    corners: ChamferCorners,
    thickness: Pixels,
) -> Vec<Point<Pixels>> {
    let (left, top, right, bottom) = edges_of(bounds);
    let band_right = (left + f32::from(thickness)).min(right);

    band_polygon(bounds, cut, corners, (left, top, band_right, bottom))
}

/// Centerline of a [`ChamferRing`] around the shape: the outline moved
/// outward by `offset + thickness / 2` (inward when negative), keeping every
/// diagonal parallel to the shape's own cut. Stroking this closed polygon with
/// the ring's thickness yields the ring. Returns no points when the moved
/// outline collapses.
pub fn chamfer_ring_points(
    bounds: Bounds<Pixels>,
    cut: Pixels,
    corners: ChamferCorners,
    ring: &ChamferRing,
) -> Vec<Point<Pixels>> {
    let distance = f32::from(ring.offset) + f32::from(ring.thickness) / 2.0;
    let (left, top, right, bottom) = edges_of(bounds);

    if right <= left || bottom <= top {
        return Vec::new();
    }

    let moved_bounds = Bounds::new(
        point(px(left - distance), px(top - distance)),
        gpui::size(
            px(right - left + 2.0 * distance),
            px(bottom - top + 2.0 * distance),
        ),
    );

    let cut = f32::from(clamp_cut(bounds.size, cut));
    let moved_cut = if cut > 0.0 {
        (cut + distance * (2.0 - std::f32::consts::SQRT_2)).max(0.0)
    } else {
        0.0
    };

    chamfer_points(moved_bounds, px(moved_cut), corners)
}

/// Control points of the Foundations motion curve, CSS
/// `cubic-bezier(.2, .8, .2, 1)`.
const MOTION_CURVE: (f32, f32, f32, f32) = (0.2, 0.8, 0.2, 1.0);

/// One coordinate of a unit cubic Bézier (end points at 0 and 1) at curve
/// parameter `t`, given its two inner control values.
fn bezier_coordinate(first: f32, second: f32, t: f32) -> f32 {
    let inverse = 1.0 - t;

    3.0 * inverse * inverse * t * first + 3.0 * inverse * t * t * second + t * t * t
}

/// Derivative of [`bezier_coordinate`] with respect to `t`.
fn bezier_slope(first: f32, second: f32, t: f32) -> f32 {
    let inverse = 1.0 - t;

    3.0 * inverse * inverse * first
        + 6.0 * inverse * t * (second - first)
        + 3.0 * t * t * (1.0 - second)
}

/// Eased progress for a linear `progress` in `0..=1`, following the
/// Foundations motion curve `cubic-bezier(.2, .8, .2, 1)` the way CSS does:
/// the curve parameter whose x equals `progress` is found first (Newton steps,
/// then bisection when the slope is too flat), and its y is returned.
pub fn motion_ease(progress: f32) -> f32 {
    let (x1, y1, x2, y2) = MOTION_CURVE;
    let progress = progress.clamp(0.0, 1.0);

    if progress <= 0.0 || progress >= 1.0 {
        return progress;
    }

    let mut parameter = progress;

    for _ in 0..8 {
        let error = bezier_coordinate(x1, x2, parameter) - progress;

        if error.abs() < 1e-6 {
            return bezier_coordinate(y1, y2, parameter);
        }

        let slope = bezier_slope(x1, x2, parameter);

        if slope.abs() < 1e-6 {
            break;
        }

        parameter = (parameter - error / slope).clamp(0.0, 1.0);
    }

    let (mut low, mut high) = (0.0_f32, 1.0_f32);
    parameter = progress;

    for _ in 0..32 {
        let x = bezier_coordinate(x1, x2, parameter);

        if (x - progress).abs() < 1e-6 {
            break;
        }

        if x < progress {
            low = parameter;
        } else {
            high = parameter;
        }

        parameter = (low + high) / 2.0;
    }

    bezier_coordinate(y1, y2, parameter)
}

/// Color at `elapsed` into a fill transition eased by [`motion_ease`],
/// interpolated in RGBA so hue does not swing through unrelated colors. A zero
/// duration returns `to`.
pub fn transition_color(from: Hsla, to: Hsla, elapsed: Duration, duration: Duration) -> Hsla {
    if duration.is_zero() || elapsed >= duration {
        return to;
    }

    let progress = motion_ease(elapsed.as_secs_f32() / duration.as_secs_f32());
    let from = Rgba::from(from);
    let to = Rgba::from(to);
    let mix = |start: f32, end: f32| start + (end - start) * progress;

    Hsla::from(Rgba {
        r: mix(from.r, to.r),
        g: mix(from.g, to.g),
        b: mix(from.b, to.b),
        a: mix(from.a, to.a),
    })
}

/// Snaps a logical-pixel rectangle to the device pixel grid
/// (each side rounded to the nearest device pixel), so axis-aligned edges of
/// the painted shape land on whole device pixels.
pub fn snap_bounds_to_device(bounds: Bounds<Pixels>, scale_factor: f32) -> Bounds<Pixels> {
    if scale_factor <= 0.0 {
        return bounds;
    }

    let (left, top, right, bottom) = edges_of(bounds);
    let snap = |value: f32| (value * scale_factor).round() / scale_factor;

    let left = snap(left);
    let top = snap(top);
    let right = snap(right).max(left);
    let bottom = snap(bottom).max(top);

    Bounds::new(
        point(px(left), px(top)),
        gpui::size(px(right - left), px(bottom - top)),
    )
}

/// Rounds an offset to the nearest device pixel.
fn snap_offset_to_device(offset: Pixels, scale_factor: f32) -> Pixels {
    if scale_factor <= 0.0 {
        return offset;
    }

    px((f32::from(offset) * scale_factor).round() / scale_factor)
}

/// Rounds a positive length to a whole number of device pixels, never below one
/// device pixel, so thin edges stay crisp at fractional scale factors.
pub fn snap_length_to_device(length: Pixels, scale_factor: f32) -> Pixels {
    let length = f32::from(length);

    if scale_factor <= 0.0 || length <= 0.0 {
        return px(length.max(0.0));
    }

    px((length * scale_factor).round().max(1.0) / scale_factor)
}

/// Paints a chamfered shape behind a control's children.
///
/// GPUI has no clip-path, so the shape is painted with a canvas. Place it as
/// the first child of a `.relative()` container; it is absolutely positioned
/// over the container's full box, so it does not affect layout and the
/// children paint on top of it. Hit testing stays rectangular.
///
/// Interaction contract: a static shape (no [`Chamfer::interactive`]) always
/// paints `fill`. An interactive shape inserts a non-blocking hitbox over its
/// bounds, reads hover from that hitbox (so an occluding overlay suppresses
/// it, like a `div` hover style) and keeps its pressed flag in element state
/// keyed by the given id. When the hover or pressed state changes it notifies
/// the view that rendered it, so callers need no `on_hover` bookkeeping. Fill
/// changes of an interactive shape fade over `Anim::FAST_MS` along the
/// Foundations easing curve (instantly when the app asks for reduced motion),
/// driven by animation frames of that view.
#[derive(IntoElement)]
pub struct Chamfer {
    cut: Pixels,
    corners: ChamferCorners,
    colors: ChamferColors,
    top_edge: Option<ChamferEdge>,
    bottom_edge: Option<ChamferEdge>,
    left_edge: Option<ChamferEdge>,
    ring: Option<ChamferRing>,
    interaction_id: Option<ElementId>,
    held: bool,
}

impl Chamfer {
    /// A transparent shape with the standard top-left/bottom-right cut. Use a
    /// `tokens::ChamferCut` constant for `cut`.
    pub fn new(cut: Pixels) -> Self {
        Self {
            cut,
            corners: ChamferCorners::default(),
            colors: ChamferColors::default(),
            top_edge: None,
            bottom_edge: None,
            left_edge: None,
            ring: None,
            interaction_id: None,
            held: false,
        }
    }

    pub fn corners(mut self, corners: ChamferCorners) -> Self {
        self.corners = corners;
        self
    }

    pub fn colors(mut self, colors: ChamferColors) -> Self {
        self.colors = colors;
        self
    }

    pub fn fill(mut self, color: impl Into<Hsla>) -> Self {
        self.colors.fill = color.into();
        self
    }

    pub fn fill_hover(mut self, color: impl Into<Hsla>) -> Self {
        self.colors.fill_hover = Some(color.into());
        self
    }

    pub fn fill_active(mut self, color: impl Into<Hsla>) -> Self {
        self.colors.fill_active = Some(color.into());
        self
    }

    /// 1 px inset border on the straight sides; the cuts stay borderless.
    pub fn border(mut self, color: impl Into<Hsla>) -> Self {
        self.colors.border = Some(color.into());
        self
    }

    pub fn top_edge(mut self, color: impl Into<Hsla>, thickness: Pixels) -> Self {
        self.top_edge = Some(ChamferEdge {
            color: color.into(),
            thickness,
        });
        self
    }

    pub fn bottom_edge(mut self, color: impl Into<Hsla>, thickness: Pixels) -> Self {
        self.bottom_edge = Some(ChamferEdge {
            color: color.into(),
            thickness,
        });
        self
    }

    /// Kind stripe along the left side, as used by banners and toasts.
    pub fn left_edge(mut self, color: impl Into<Hsla>, thickness: Pixels) -> Self {
        self.left_edge = Some(ChamferEdge {
            color: color.into(),
            thickness,
        });
        self
    }

    /// Strokes a ring along the full outline, painted above everything else.
    pub fn ring(mut self, ring: ChamferRing) -> Self {
        self.ring = Some(ring);
        self
    }

    /// Tracks hover and pressed state so `fill_hover` / `fill_active` apply.
    /// `id` keys the pressed state and must be unique among its siblings.
    pub fn interactive(mut self, id: impl Into<ElementId>) -> Self {
        self.interaction_id = Some(id.into());
        self
    }

    /// Shows the pressed fill regardless of the pointer, for a control held
    /// down from the keyboard (Enter or Space on a focused button). Only takes
    /// effect on an interactive shape.
    pub fn held(mut self, held: bool) -> Self {
        self.held = held;
        self
    }

    /// Paints the shape into `bounds` with the given fill. The border is drawn
    /// on top of the fill, the top, bottom and left edges over the border, and
    /// the ring last. Bounds, cut, border and edges snap to the device pixel grid.
    pub fn paint(&self, bounds: Bounds<Pixels>, fill: Hsla, window: &mut Window) {
        let scale_factor = window.scale_factor();
        let bounds = snap_bounds_to_device(bounds, scale_factor);
        let cut = snap_offset_to_device(self.cut, scale_factor);

        paint_polygon(&chamfer_points(bounds, cut, self.corners), fill, window);

        if let Some(border) = self.colors.border {
            let thickness = snap_length_to_device(Borders::THIN, scale_factor);

            for polygon in chamfer_border_polygons(bounds, cut, self.corners, thickness) {
                paint_polygon(&polygon, border, window);
            }
        }

        if let Some(edge) = self.top_edge {
            let thickness = snap_length_to_device(edge.thickness, scale_factor);
            let polygon = chamfer_top_edge_polygon(bounds, cut, self.corners, thickness);
            paint_polygon(&polygon, edge.color, window);
        }

        if let Some(edge) = self.bottom_edge {
            let thickness = snap_length_to_device(edge.thickness, scale_factor);
            let polygon = chamfer_bottom_edge_polygon(bounds, cut, self.corners, thickness);
            paint_polygon(&polygon, edge.color, window);
        }

        if let Some(edge) = self.left_edge {
            let thickness = snap_length_to_device(edge.thickness, scale_factor);
            let polygon = chamfer_left_edge_polygon(bounds, cut, self.corners, thickness);
            paint_polygon(&polygon, edge.color, window);
        }

        if let Some(ring) = self.ring {
            let outline = chamfer_ring_points(bounds, cut, self.corners, &ring);
            stroke_closed_polygon(&outline, ring.thickness, ring.color, window);
        }
    }
}

/// A fill fade in progress, or settled once `started + duration` has passed.
#[derive(Clone, Copy)]
struct FillTransition {
    from: Hsla,
    to: Hsla,
    started: Instant,
}

/// Interaction state of an interactive shape, kept across frames in element
/// state.
#[derive(Default)]
struct InteractionState {
    pressed: Cell<bool>,
    fill: Cell<Option<FillTransition>>,
}

impl InteractionState {
    /// Fill to paint now for `target`, starting a new fade from the currently
    /// shown color whenever the target changes. The second value is `true`
    /// while the fade still needs frames.
    fn displayed_fill(&self, target: Hsla, now: Instant, duration: Duration) -> (Hsla, bool) {
        let transition = match self.fill.get() {
            None => FillTransition {
                from: target,
                to: target,
                started: now,
            },
            Some(current) if current.to == target => current,
            Some(current) => FillTransition {
                from: transition_color(
                    current.from,
                    current.to,
                    now.saturating_duration_since(current.started),
                    duration,
                ),
                to: target,
                started: now,
            },
        };
        self.fill.set(Some(transition));

        let elapsed = now.saturating_duration_since(transition.started);
        let color = transition_color(transition.from, transition.to, elapsed, duration);
        let animating = transition.from != transition.to && elapsed < duration;

        (color, animating)
    }
}

/// State carried from the canvas prepaint to its paint.
struct ChamferInteraction {
    hitbox: Hitbox,
    state: Rc<InteractionState>,
}

impl RenderOnce for Chamfer {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        let interaction_id = self.interaction_id.clone();

        canvas(
            move |bounds, window, _cx| {
                let id = interaction_id?;
                let hitbox = window.insert_hitbox(bounds, HitboxBehavior::Normal);
                let state = window.with_global_id(id, |global_id, window| {
                    window.with_element_state(
                        global_id,
                        |state: Option<Rc<InteractionState>>, _| {
                            let state = state.unwrap_or_default();
                            (state.clone(), state)
                        },
                    )
                });

                Some(ChamferInteraction { hitbox, state })
            },
            move |bounds, interaction, window, cx| {
                let Some(interaction) = interaction else {
                    self.paint(bounds, self.colors.fill, window);
                    return;
                };

                let hovered = interaction.hitbox.is_hovered(window);
                let pressed = self.held || interaction.state.pressed.get();
                let target = self.colors.fill_for(hovered, pressed);
                let duration = if cx.reduce_motion() {
                    Duration::ZERO
                } else {
                    Duration::from_millis(Anim::FAST_MS)
                };
                let (fill, animating) =
                    interaction
                        .state
                        .displayed_fill(target, Instant::now(), duration);

                self.paint(bounds, fill, window);

                if animating {
                    window.request_animation_frame();
                }

                register_interaction_listeners(&self.colors, interaction, hovered, window);
            },
        )
        .absolute()
        .inset_0()
    }
}

/// Notifies the rendering view when hover or pressed state changes, so the
/// next frame repaints with the matching fill. Listeners are only registered
/// for the states that have their own color.
fn register_interaction_listeners(
    colors: &ChamferColors,
    interaction: ChamferInteraction,
    was_hovered: bool,
    window: &mut Window,
) {
    let current_view = window.current_view();
    let ChamferInteraction { hitbox, state } = interaction;

    if colors.fill_hover.is_some() {
        let hitbox = hitbox.clone();
        window.on_mouse_event(move |_: &MouseMoveEvent, phase, window, cx| {
            if phase == DispatchPhase::Capture && hitbox.is_hovered(window) != was_hovered {
                cx.notify(current_view);
            }
        });
    }

    if colors.fill_active.is_some() {
        let state_on_down = state.clone();
        window.on_mouse_event(move |event: &MouseDownEvent, phase, window, cx| {
            if phase == DispatchPhase::Bubble
                && event.button == MouseButton::Left
                && hitbox.is_hovered(window)
            {
                state_on_down.pressed.set(true);
                cx.notify(current_view);
            }
        });

        window.on_mouse_event(move |_: &MouseUpEvent, phase, _window, cx| {
            if phase == DispatchPhase::Capture && state.pressed.replace(false) {
                cx.notify(current_view);
            }
        });
    }
}

fn paint_polygon(points: &[Point<Pixels>], color: Hsla, window: &mut Window) {
    if points.len() < 3 || color.a <= 0.0 {
        return;
    }

    let mut builder = PathBuilder::fill();
    builder.move_to(points[0]);
    for point in &points[1..] {
        builder.line_to(*point);
    }
    builder.close();

    if let Ok(path) = builder.build() {
        window.paint_path(path, color);
    }
}

/// Strokes a closed polygon. Lyon's default miter join keeps the 90° and 135°
/// corners of a chamfer sharp without spikes.
fn stroke_closed_polygon(
    points: &[Point<Pixels>],
    thickness: Pixels,
    color: Hsla,
    window: &mut Window,
) {
    if points.len() < 3 || color.a <= 0.0 || thickness <= Pixels::ZERO {
        return;
    }

    let mut builder = PathBuilder::stroke(thickness);
    builder.move_to(points[0]);
    for point in &points[1..] {
        builder.line_to(*point);
    }
    builder.close();

    if let Ok(path) = builder.build() {
        window.paint_path(path, color);
    }
}

type Band = (f32, f32, f32, f32);

fn edges_of(bounds: Bounds<Pixels>) -> Band {
    let left = f32::from(bounds.origin.x);
    let top = f32::from(bounds.origin.y);

    (
        left,
        top,
        left + f32::from(bounds.size.width),
        top + f32::from(bounds.size.height),
    )
}

fn band_polygon(
    bounds: Bounds<Pixels>,
    cut: Pixels,
    corners: ChamferCorners,
    band: Band,
) -> Vec<Point<Pixels>> {
    let shape = shape_polygon(bounds, cut, corners);

    clip_to_band(&shape, band)
        .map(|polygon| to_pixel_points(&polygon))
        .unwrap_or_default()
}

fn shape_polygon(bounds: Bounds<Pixels>, cut: Pixels, corners: ChamferCorners) -> Vec<Point<f32>> {
    let (left, top, right, bottom) = edges_of(bounds);

    if right <= left || bottom <= top {
        return Vec::new();
    }

    let cut = f32::from(clamp_cut(bounds.size, cut));

    let outline = match corners {
        ChamferCorners::TopLeftBottomRight => vec![
            point(left + cut, top),
            point(right, top),
            point(right, bottom - cut),
            point(right - cut, bottom),
            point(left, bottom),
            point(left, top + cut),
        ],
        ChamferCorners::TopLeft => vec![
            point(left + cut, top),
            point(right, top),
            point(right, bottom),
            point(left, bottom),
            point(left, top + cut),
        ],
        ChamferCorners::BottomRight => vec![
            point(left, top),
            point(right, top),
            point(right, bottom - cut),
            point(right - cut, bottom),
            point(left, bottom),
        ],
    };

    without_repeated_vertices(outline)
}

/// Intersects a convex polygon with an axis-aligned band (Sutherland–Hodgman
/// against its four sides). Returns `None` when nothing with area remains.
fn clip_to_band(polygon: &[Point<f32>], band: Band) -> Option<Vec<Point<f32>>> {
    let (left, top, right, bottom) = band;

    if right <= left || bottom <= top || polygon.len() < 3 {
        return None;
    }

    let clipped = clip_half_plane(polygon, |p| p.x >= left, |a, b| at_x(a, b, left));
    let clipped = clip_half_plane(&clipped, |p| p.x <= right, |a, b| at_x(a, b, right));
    let clipped = clip_half_plane(&clipped, |p| p.y >= top, |a, b| at_y(a, b, top));
    let clipped = clip_half_plane(&clipped, |p| p.y <= bottom, |a, b| at_y(a, b, bottom));
    let clipped = without_repeated_vertices(clipped);

    (clipped.len() >= 3 && polygon_area(&clipped) > f32::EPSILON).then_some(clipped)
}

fn clip_half_plane(
    polygon: &[Point<f32>],
    inside: impl Fn(Point<f32>) -> bool,
    intersection: impl Fn(Point<f32>, Point<f32>) -> Point<f32>,
) -> Vec<Point<f32>> {
    let mut output = Vec::with_capacity(polygon.len() + 2);

    for (index, &current) in polygon.iter().enumerate() {
        let previous = polygon[(index + polygon.len() - 1) % polygon.len()];

        match (inside(previous), inside(current)) {
            (true, true) => output.push(current),
            (true, false) => output.push(intersection(previous, current)),
            (false, true) => {
                output.push(intersection(previous, current));
                output.push(current);
            }
            (false, false) => {}
        }
    }

    output
}

fn at_x(a: Point<f32>, b: Point<f32>, x: f32) -> Point<f32> {
    let ratio = (x - a.x) / (b.x - a.x);
    point(x, a.y + (b.y - a.y) * ratio)
}

fn at_y(a: Point<f32>, b: Point<f32>, y: f32) -> Point<f32> {
    let ratio = (y - a.y) / (b.y - a.y);
    point(a.x + (b.x - a.x) * ratio, y)
}

fn without_repeated_vertices(points: Vec<Point<f32>>) -> Vec<Point<f32>> {
    let mut unique: Vec<Point<f32>> = Vec::with_capacity(points.len());

    for candidate in points {
        if unique.last() != Some(&candidate) {
            unique.push(candidate);
        }
    }

    while unique.len() > 1 && unique.first() == unique.last() {
        unique.pop();
    }

    unique
}

fn polygon_area(polygon: &[Point<f32>]) -> f32 {
    let twice_area: f32 = polygon
        .iter()
        .zip(polygon.iter().cycle().skip(1))
        .map(|(a, b)| a.x * b.y - b.x * a.y)
        .sum();

    twice_area.abs() / 2.0
}

fn to_pixel_points(points: &[Point<f32>]) -> Vec<Point<Pixels>> {
    points.iter().map(|p| point(px(p.x), px(p.y))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::ChamferCut;
    use gpui::{Context, Entity, TestAppContext, size};

    fn rect(x: f32, y: f32, width: f32, height: f32) -> Bounds<Pixels> {
        Bounds::new(point(px(x), px(y)), size(px(width), px(height)))
    }

    fn raw(points: &[Point<Pixels>]) -> Vec<(f32, f32)> {
        points
            .iter()
            .map(|p| (f32::from(p.x), f32::from(p.y)))
            .collect()
    }

    /// Vertex set of a clipped polygon; clipping may rotate the starting vertex.
    fn vertex_set(points: &[Point<Pixels>]) -> Vec<(f32, f32)> {
        let mut vertices = raw(points);
        vertices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        vertices
    }

    fn sorted(mut vertices: Vec<(f32, f32)>) -> Vec<(f32, f32)> {
        vertices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        vertices
    }

    fn contains(polygon: &[Point<Pixels>], x: f32, y: f32) -> bool {
        let points = raw(polygon);
        let mut inside = false;

        for (index, &(ax, ay)) in points.iter().enumerate() {
            let (bx, by) = points[(index + 1) % points.len()];
            if (ay > y) != (by > y) && x < ax + (bx - ax) * (y - ay) / (by - ay) {
                inside = !inside;
            }
        }

        inside
    }

    #[test]
    fn standard_cut_trims_top_left_and_bottom_right() {
        let points = chamfer_points(
            rect(0.0, 0.0, 100.0, 30.0),
            ChamferCut::CONTROL,
            ChamferCorners::default(),
        );

        assert_eq!(
            raw(&points),
            vec![
                (6.0, 0.0),
                (100.0, 0.0),
                (100.0, 24.0),
                (94.0, 30.0),
                (0.0, 30.0),
                (0.0, 6.0),
            ]
        );
    }

    #[test]
    fn tab_cut_trims_only_top_left() {
        let points = chamfer_points(
            rect(0.0, 0.0, 100.0, 30.0),
            ChamferCut::INPUT,
            ChamferCorners::TopLeft,
        );

        assert_eq!(
            raw(&points),
            vec![
                (8.0, 0.0),
                (100.0, 0.0),
                (100.0, 30.0),
                (0.0, 30.0),
                (0.0, 8.0),
            ]
        );
    }

    #[test]
    fn menu_segment_cut_trims_only_bottom_right() {
        let points = chamfer_points(
            rect(0.0, 0.0, 26.0, 28.0),
            ChamferCut::CONTROL,
            ChamferCorners::BottomRight,
        );

        assert_eq!(
            raw(&points),
            vec![
                (0.0, 0.0),
                (26.0, 0.0),
                (26.0, 22.0),
                (20.0, 28.0),
                (0.0, 28.0),
            ]
        );
    }

    #[test]
    fn points_are_offset_by_bounds_origin() {
        let points = chamfer_points(
            rect(10.0, 20.0, 50.0, 30.0),
            ChamferCut::KEYCAP,
            ChamferCorners::default(),
        );

        assert_eq!(
            raw(&points),
            vec![
                (14.0, 20.0),
                (60.0, 20.0),
                (60.0, 46.0),
                (56.0, 50.0),
                (10.0, 50.0),
                (10.0, 24.0),
            ]
        );
    }

    #[test]
    fn cut_is_clamped_to_half_the_smaller_side() {
        assert_eq!(clamp_cut(size(px(40.0), px(10.0)), px(18.0)), px(5.0));
        assert_eq!(clamp_cut(size(px(40.0), px(10.0)), px(-3.0)), Pixels::ZERO);

        let points = chamfer_points(
            rect(0.0, 0.0, 10.0, 10.0),
            px(18.0),
            ChamferCorners::default(),
        );
        assert_eq!(
            raw(&points),
            vec![
                (5.0, 0.0),
                (10.0, 0.0),
                (10.0, 5.0),
                (5.0, 10.0),
                (0.0, 10.0),
                (0.0, 5.0)
            ]
        );
    }

    #[test]
    fn zero_cut_is_a_plain_rectangle_without_repeated_vertices() {
        let points = chamfer_points(
            rect(0.0, 0.0, 20.0, 10.0),
            Pixels::ZERO,
            ChamferCorners::default(),
        );

        assert_eq!(
            raw(&points),
            vec![(0.0, 0.0), (20.0, 0.0), (20.0, 10.0), (0.0, 10.0)]
        );
    }

    #[test]
    fn zero_or_negative_bounds_produce_nothing() {
        for bounds in [
            rect(0.0, 0.0, 0.0, 0.0),
            rect(5.0, 5.0, 0.0, 20.0),
            rect(0.0, 0.0, 20.0, -1.0),
        ] {
            assert!(
                chamfer_points(bounds, ChamferCut::CONTROL, ChamferCorners::default()).is_empty()
            );
            assert!(
                chamfer_border_polygons(
                    bounds,
                    ChamferCut::CONTROL,
                    ChamferCorners::default(),
                    Borders::THIN
                )
                .is_empty()
            );
            assert!(
                chamfer_top_edge_polygon(
                    bounds,
                    ChamferCut::CONTROL,
                    ChamferCorners::TopLeft,
                    Borders::MEDIUM
                )
                .is_empty()
            );
            assert!(
                chamfer_bottom_edge_polygon(
                    bounds,
                    ChamferCut::CONTROL,
                    ChamferCorners::default(),
                    Borders::MEDIUM
                )
                .is_empty()
            );
        }
    }

    #[test]
    fn border_covers_straight_sides_but_not_the_diagonals() {
        let bounds = rect(0.0, 0.0, 100.0, 30.0);
        let pieces = chamfer_border_polygons(
            bounds,
            ChamferCut::CONTROL,
            ChamferCorners::default(),
            Borders::THIN,
        );

        assert_eq!(pieces.len(), 4);

        let covered = |x: f32, y: f32| pieces.iter().any(|piece| contains(piece, x, y));

        assert!(covered(50.0, 0.5), "top side");
        assert!(covered(50.0, 29.5), "bottom side");
        assert!(covered(0.5, 20.0), "left side");
        assert!(covered(99.5, 10.0), "right side");

        assert!(!covered(3.0, 3.0), "top-left diagonal must stay borderless");
        assert!(
            !covered(97.0, 27.0),
            "bottom-right diagonal must stay borderless"
        );
        assert!(!covered(0.5, 0.5), "cut-away corner is outside the shape");
        assert!(!covered(50.0, 15.0), "interior is not border");

        assert_eq!(
            vertex_set(&pieces[0]),
            sorted(vec![(6.0, 0.0), (100.0, 0.0), (100.0, 1.0), (5.0, 1.0)]),
            "the top side starts where it meets the cut"
        );
    }

    #[test]
    fn tab_border_keeps_the_square_corners() {
        let bounds = rect(0.0, 0.0, 100.0, 30.0);
        let pieces = chamfer_border_polygons(
            bounds,
            ChamferCut::INPUT,
            ChamferCorners::TopLeft,
            Borders::THIN,
        );
        let covered = |x: f32, y: f32| pieces.iter().any(|piece| contains(piece, x, y));

        assert!(covered(99.5, 29.5), "bottom-right corner is square");
        assert!(covered(99.5, 0.5), "top-right corner is square");
        assert!(!covered(4.0, 4.0), "top-left diagonal must stay borderless");
    }

    #[test]
    fn top_edge_follows_the_cut() {
        let bounds = rect(10.0, 0.0, 100.0, 36.0);
        let polygon = chamfer_top_edge_polygon(
            bounds,
            ChamferCut::INPUT,
            ChamferCorners::TopLeft,
            Borders::MEDIUM,
        );

        assert_eq!(
            vertex_set(&polygon),
            sorted(vec![(18.0, 0.0), (110.0, 0.0), (110.0, 2.0), (16.0, 2.0)])
        );
    }

    #[test]
    fn left_edge_follows_the_top_left_cut() {
        let bounds = rect(0.0, 0.0, 100.0, 40.0);
        let polygon = chamfer_left_edge_polygon(
            bounds,
            ChamferCut::INPUT,
            ChamferCorners::TopLeftBottomRight,
            Borders::MEDIUM,
        );

        assert_eq!(
            vertex_set(&polygon),
            sorted(vec![(0.0, 8.0), (2.0, 6.0), (2.0, 40.0), (0.0, 40.0)])
        );
    }

    #[test]
    fn bottom_edge_spans_the_bottom_side() {
        let bounds = rect(0.0, 0.0, 40.0, 20.0);
        let square_bottom = chamfer_bottom_edge_polygon(
            bounds,
            ChamferCut::KEYCAP,
            ChamferCorners::TopLeft,
            Borders::MEDIUM,
        );
        let cut_bottom = chamfer_bottom_edge_polygon(
            bounds,
            ChamferCut::KEYCAP,
            ChamferCorners::default(),
            Borders::MEDIUM,
        );

        assert_eq!(
            vertex_set(&square_bottom),
            sorted(vec![(40.0, 18.0), (40.0, 20.0), (0.0, 20.0), (0.0, 18.0)])
        );
        assert!(contains(&cut_bottom, 1.0, 19.0));
        assert!(
            !contains(&cut_bottom, 39.0, 19.5),
            "bottom-right cut is excluded"
        );
    }

    fn assert_points_close(actual: &[Point<Pixels>], expected: &[(f32, f32)]) {
        let actual = raw(actual);
        assert_eq!(actual.len(), expected.len(), "{actual:?} vs {expected:?}");

        for (got, want) in actual.iter().zip(expected) {
            assert!(
                (got.0 - want.0).abs() < 1e-3 && (got.1 - want.1).abs() < 1e-3,
                "{actual:?} vs {expected:?}"
            );
        }
    }

    /// Perpendicular distance between the top-left diagonals of two outlines
    /// whose first and last points lie on that diagonal.
    fn top_left_diagonal_gap(inner: &[Point<Pixels>], outer: &[Point<Pixels>]) -> f32 {
        let diagonal = |points: &[Point<Pixels>]| {
            let start = raw(points)[0];
            start.0 + start.1
        };

        (diagonal(inner) - diagonal(outer)) / std::f32::consts::SQRT_2
    }

    #[test]
    fn focus_ring_traces_the_full_outline_inside_the_bounds() {
        let bounds = rect(0.0, 0.0, 100.0, 28.0);
        let ring = ChamferRing::focus(gpui::blue());
        let outline = chamfer_ring_points(
            bounds,
            ChamferCut::CONTROL,
            ChamferCorners::default(),
            &ring,
        );
        let cut = 6.0 - 0.75 * (2.0 - std::f32::consts::SQRT_2);

        assert_points_close(
            &outline,
            &[
                (0.75 + cut, 0.75),
                (99.25, 0.75),
                (99.25, 27.25 - cut),
                (99.25 - cut, 27.25),
                (0.75, 27.25),
                (0.75, 0.75 + cut),
            ],
        );

        let shape = chamfer_points(bounds, ChamferCut::CONTROL, ChamferCorners::default());
        let gap = top_left_diagonal_gap(&outline, &shape);
        assert!(
            (gap - 0.75).abs() < 1e-3,
            "the diagonal is inset like the sides: {gap}"
        );
    }

    #[test]
    fn focus_ring_placement_follows_the_fill_kind() {
        let color = gpui::blue();

        assert_eq!(
            ChamferRing::focus_for(color, ChamferFillKind::Surface),
            ChamferRing::focus(color)
        );
        assert_eq!(
            ChamferRing::focus_for(color, ChamferFillKind::Filled),
            ChamferRing {
                color,
                thickness: Borders::FOCUS_RING,
                offset: Borders::MEDIUM,
            }
        );
    }

    #[test]
    fn outside_ring_moves_every_side_and_diagonal_out_by_the_offset() {
        let bounds = rect(10.0, 10.0, 80.0, 28.0);
        let ring = ChamferRing::focus(gpui::blue()).outside(Borders::MEDIUM);
        let outline = chamfer_ring_points(
            bounds,
            ChamferCut::CONTROL,
            ChamferCorners::default(),
            &ring,
        );
        let shape = chamfer_points(bounds, ChamferCut::CONTROL, ChamferCorners::default());
        let centerline = 2.0 + 0.75;

        let points = raw(&outline);
        assert!((points[1].1 - (10.0 - centerline)).abs() < 1e-3, "top side");
        assert!(
            (points[1].0 - (90.0 + centerline)).abs() < 1e-3,
            "right side"
        );
        assert!(
            (points[4].1 - (38.0 + centerline)).abs() < 1e-3,
            "bottom side"
        );

        let gap = top_left_diagonal_gap(&shape, &outline);
        assert!((gap - centerline).abs() < 1e-3, "diagonal gap {gap}");
    }

    #[test]
    fn ring_keeps_square_corners_square_and_collapses_to_nothing() {
        let ring = ChamferRing::focus(gpui::blue());
        let tab = chamfer_ring_points(
            rect(0.0, 0.0, 60.0, 30.0),
            ChamferCut::INPUT,
            ChamferCorners::TopLeft,
            &ring,
        );
        assert_eq!(tab.len(), 5);
        assert!(
            raw(&tab).contains(&(59.25, 29.25)),
            "bottom-right stays square"
        );

        let tiny = chamfer_ring_points(
            rect(0.0, 0.0, 1.0, 1.0),
            ChamferCut::CONTROL,
            ChamferCorners::default(),
            &ring,
        );
        assert!(tiny.is_empty());
        assert!(
            chamfer_ring_points(
                rect(0.0, 0.0, 0.0, 10.0),
                ChamferCut::CONTROL,
                ChamferCorners::default(),
                &ring
            )
            .is_empty()
        );
    }

    /// Reference for the motion curve: samples the parametric Bézier densely
    /// and returns the y of the sample whose x is closest to `progress`.
    fn sampled_curve(progress: f32) -> f32 {
        let (x1, y1, x2, y2) = MOTION_CURVE;
        let steps = 200_000;

        (0..=steps)
            .map(|step| step as f32 / steps as f32)
            .map(|t| (bezier_coordinate(x1, x2, t), bezier_coordinate(y1, y2, t)))
            .min_by(|a, b| (a.0 - progress).abs().total_cmp(&(b.0 - progress).abs()))
            .map(|(_, y)| y)
            .unwrap_or(progress)
    }

    #[test]
    fn motion_ease_follows_the_foundations_curve() {
        assert_eq!(motion_ease(0.0), 0.0);
        assert_eq!(motion_ease(1.0), 1.0);
        assert_eq!(motion_ease(-0.5), 0.0);
        assert_eq!(motion_ease(1.5), 1.0);

        for step in 1..20 {
            let progress = step as f32 / 20.0;
            let eased = motion_ease(progress);

            assert!(
                (eased - sampled_curve(progress)).abs() < 1e-3,
                "motion_ease({progress}) = {eased}, curve gives {}",
                sampled_curve(progress)
            );
        }

        assert!(
            motion_ease(0.25) > 0.6,
            "the curve is ease-out: most of the change happens early"
        );

        let mut previous = 0.0;
        for step in 1..=100 {
            let eased = motion_ease(step as f32 / 100.0);
            assert!(eased >= previous, "the curve never goes backwards");
            previous = eased;
        }
    }

    #[test]
    fn transition_color_follows_the_motion_curve() {
        let duration = Duration::from_millis(Anim::FAST_MS);
        let black = gpui::black();
        let white = gpui::white();

        assert_eq!(
            transition_color(black, white, Duration::ZERO, duration),
            black
        );
        assert_eq!(transition_color(black, white, duration, duration), white);
        assert_eq!(
            transition_color(black, white, Duration::from_millis(1), Duration::ZERO),
            white
        );

        let halfway = Rgba::from(transition_color(black, white, duration / 2, duration));
        assert!((halfway.r - motion_ease(0.5)).abs() < 1e-3 && (halfway.a - 1.0).abs() < 1e-3);
    }

    #[test]
    fn displayed_fill_fades_to_a_new_target_and_retargets_from_the_shown_color() {
        let duration = Duration::from_millis(Anim::FAST_MS);
        let state = InteractionState::default();
        let start = Instant::now();

        assert_eq!(
            state.displayed_fill(gpui::black(), start, duration),
            (gpui::black(), false)
        );

        let (shown, animating) = state.displayed_fill(gpui::white(), start, duration);
        assert_eq!(shown, gpui::black());
        assert!(animating);

        let midway = start + duration / 2;
        let (shown_midway, animating) = state.displayed_fill(gpui::white(), midway, duration);
        assert!(animating);
        assert!((Rgba::from(shown_midway).r - motion_ease(0.5)).abs() < 1e-3);

        let (retargeted, _) = state.displayed_fill(gpui::black(), midway, duration);
        assert_eq!(
            retargeted, shown_midway,
            "a new target starts from the shown color"
        );

        let (settled, animating) = state.displayed_fill(gpui::black(), midway + duration, duration);
        assert_eq!(settled, gpui::black());
        assert!(!animating);

        let instant = InteractionState::default();
        instant.displayed_fill(gpui::black(), start, Duration::ZERO);
        assert_eq!(
            instant.displayed_fill(gpui::white(), start, Duration::ZERO),
            (gpui::white(), false)
        );
    }

    #[test]
    fn device_snapping_rounds_edges_and_keeps_one_device_pixel() {
        let snapped = snap_bounds_to_device(rect(10.3, 4.6, 20.4, 10.2), 2.0);
        assert_eq!(snapped, rect(10.5, 4.5, 20.0, 10.5));

        assert_eq!(snap_length_to_device(Borders::THIN, 1.5), px(2.0 / 1.5));
        assert_eq!(snap_length_to_device(px(0.1), 1.0), Borders::THIN);
        assert_eq!(snap_length_to_device(Pixels::ZERO, 2.0), Pixels::ZERO);
    }

    #[test]
    fn fill_resolution_prefers_pressed_then_hovered() {
        let base = gpui::black();
        let hover = gpui::white();
        let active = gpui::red();

        let full = ChamferColors {
            fill: base,
            fill_hover: Some(hover),
            fill_active: Some(active),
            border: None,
        };
        assert_eq!(full.fill_for(false, false), base);
        assert_eq!(full.fill_for(true, false), hover);
        assert_eq!(full.fill_for(true, true), active);

        let hover_only = ChamferColors {
            fill_active: None,
            ..full
        };
        assert_eq!(hover_only.fill_for(false, true), hover);

        let plain = ChamferColors {
            fill_hover: None,
            fill_active: None,
            ..full
        };
        assert_eq!(plain.fill_for(true, true), base);
    }

    struct ChamferHost {
        renders: usize,
    }

    impl Render for ChamferHost {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            self.renders += 1;

            gpui::div()
                .id("chamfer-host")
                .debug_selector(|| "chamfer-host".to_string())
                .relative()
                .w(px(120.0))
                .h(px(28.0))
                .child(
                    Chamfer::new(ChamferCut::CONTROL)
                        .fill(gpui::black())
                        .fill_hover(gpui::white())
                        .fill_active(gpui::red())
                        .border(gpui::blue())
                        .ring(ChamferRing::focus(gpui::blue()))
                        .bottom_edge(gpui::green(), Borders::MEDIUM)
                        .interactive("chamfer"),
                )
                .child("Run")
        }
    }

    fn renders(host: &Entity<ChamferHost>, cx: &mut gpui::VisualTestContext) -> usize {
        cx.update(|_, cx| host.read(cx).renders)
    }

    #[gpui::test]
    fn interactive_chamfer_repaints_on_hover_and_press(cx: &mut TestAppContext) {
        let (host, window) = cx.add_window_view(|_, _| ChamferHost { renders: 0 });
        window.run_until_parked();

        let bounds = window
            .debug_bounds("chamfer-host")
            .expect("the chamfer host should be laid out");
        let inside = bounds.center();
        let outside = point(bounds.right() + px(40.0), bounds.bottom() + px(40.0));

        window.simulate_mouse_move(outside, None, gpui::Modifiers::default());
        window.run_until_parked();
        let before_hover = renders(&host, window);

        window.simulate_mouse_move(inside, None, gpui::Modifiers::default());
        window.run_until_parked();
        let after_hover = renders(&host, window);
        assert!(
            after_hover > before_hover,
            "entering the shape must repaint it"
        );

        window.simulate_mouse_down(inside, MouseButton::Left, gpui::Modifiers::default());
        window.run_until_parked();
        let after_press = renders(&host, window);
        assert!(after_press > after_hover, "pressing must repaint it");

        window.simulate_mouse_up(inside, MouseButton::Left, gpui::Modifiers::default());
        window.run_until_parked();
        assert!(
            renders(&host, window) > after_press,
            "releasing must repaint it"
        );
    }
}
