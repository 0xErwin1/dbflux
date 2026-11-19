//! When a hovered row opens its flyout, and when leaving one keeps it open.
//!
//! A diagonal journey toward a child must not close it, so the open is delayed
//! (sweeping across a row spawns nothing) and the close is delayed and
//! suppressed while the pointer is inside the triangle between the row it left
//! and the flyout it heads for.

use crate::composites::menu_tree::placement::FlyoutSide;
use gpui::{Bounds, Pixels, Point};
use std::time::{Duration, Instant};

/// How long the pointer must rest on a row before its flyout opens.
pub const HOVER_OPEN_DELAY: Duration = Duration::from_millis(150);

/// How long a flyout stays open after the pointer leaves it, which absorbs the
/// gap between the parent panel and the flyout.
pub const HOVER_CLOSE_GRACE: Duration = Duration::from_millis(120);

/// What a hover timer decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoverTick {
    /// The pointer rested long enough on this row: open its flyout.
    Open(usize, usize),
    /// The pointer left this level long enough ago: close its flyout.
    CloseLevel(usize),
    /// Nothing is due yet.
    Nothing,
}

/// Hover state for one open menu.
#[derive(Clone, Debug, Default)]
pub struct HoverIntent {
    pending: Option<(usize, usize)>,
    pending_since: Option<Instant>,
    open: Option<(usize, usize)>,
    left: Option<(usize, Instant)>,
    in_corridor: bool,
}

impl HoverIntent {
    pub fn new() -> Self {
        Self::default()
    }

    /// The pointer entered `index` of `level`.
    pub fn enter(&mut self, level: usize, index: usize, now: Instant) {
        self.left = None;

        if self.open == Some((level, index)) {
            self.pending = None;
            self.pending_since = None;
            return;
        }

        if self.pending != Some((level, index)) {
            self.pending = Some((level, index));
            self.pending_since = Some(now);
        }
    }

    /// The pointer left `level`'s frame.
    pub fn leave(&mut self, level: usize, now: Instant) {
        self.pending = None;
        self.pending_since = None;

        if self.open.is_some_and(|(open_level, _)| open_level == level) {
            self.left = Some((level, now));
        }
    }

    /// Records which row's flyout is open, so hovering it again is a no-op.
    pub fn mark_open(&mut self, level: usize, index: Option<usize>) {
        self.open = index.map(|index| (level, index));
        self.pending = None;
        self.pending_since = None;
        self.left = None;
    }

    /// The row a timer is running for, if any.
    pub fn pending(&self) -> Option<(usize, usize)> {
        self.pending
    }

    /// Whether the pointer is inside the safe triangle between the row it left
    /// and the flyout it is heading for.
    pub fn in_corridor(&self) -> bool {
        self.in_corridor
    }

    /// Tracks the pointer against the corridor of the last open flyout.
    pub fn pointer_moved(
        &mut self,
        pointer: Point<Pixels>,
        row: Bounds<Pixels>,
        flyout: Bounds<Pixels>,
        side: FlyoutSide,
    ) {
        self.in_corridor = in_corridor(pointer, row, flyout, side);

        if self.in_corridor {
            self.left = None;
        }
    }

    /// Reports what is due at `now`.
    pub fn tick(&mut self, now: Instant) -> HoverTick {
        if let (Some(target), Some(since)) = (self.pending, self.pending_since)
            && now.saturating_duration_since(since) >= HOVER_OPEN_DELAY
        {
            self.pending = None;
            self.pending_since = None;
            self.open = Some(target);

            return HoverTick::Open(target.0, target.1);
        }

        if let Some((level, since)) = self.left
            && !self.in_corridor
            && now.saturating_duration_since(since) >= HOVER_CLOSE_GRACE
        {
            self.left = None;
            self.open = None;

            return HoverTick::CloseLevel(level);
        }

        HoverTick::Nothing
    }
}

/// Whether `pointer` sits on the way from `row` to `flyout`, so crossing that
/// space must not close the flyout.
///
/// The region is the triangle between the row's leading edge and the flyout's
/// near top corner, plus the two rects themselves: a pointer anywhere inside the
/// row or the flyout is obviously still on the menu.
pub fn in_corridor(
    pointer: Point<Pixels>,
    row: Bounds<Pixels>,
    flyout: Bounds<Pixels>,
    side: FlyoutSide,
) -> bool {
    if inside(pointer, row) || inside(pointer, flyout) {
        return true;
    }

    let (near_row, near_flyout) = match side {
        FlyoutSide::Right => (row.right(), flyout.left()),
        FlyoutSide::Left => (row.left(), flyout.right()),
    };

    inside_triangle(
        (pointer.x, pointer.y),
        (near_row, row.top()),
        (near_row, row.bottom()),
        (near_flyout, flyout.top()),
    )
}

/// Whether a point is inside a rect, edges included.
fn inside(point: Point<Pixels>, bounds: Bounds<Pixels>) -> bool {
    point.x >= bounds.left()
        && point.x <= bounds.right()
        && point.y >= bounds.top()
        && point.y <= bounds.bottom()
}

/// Whether `point` is inside the triangle `a`, `b`, `c`.
fn inside_triangle(point: Corner, a: Corner, b: Corner, c: Corner) -> bool {
    let edges = [edge(a, b, point), edge(b, c, point), edge(c, a, point)];

    let outside_one_way = edges.iter().any(|area| *area < 0.0);
    let outside_the_other = edges.iter().any(|area| *area > 0.0);

    !(outside_one_way && outside_the_other)
}

/// A corner of the corridor triangle, in the space the geometry is compared in.
type Corner = (Pixels, Pixels);

/// Twice the signed area of the triangle `a`, `b`, `c`: its sign says which side
/// of the line `a`-`b` the point `c` falls on.
fn edge(a: Corner, b: Corner, c: Corner) -> f32 {
    let (ax, ay) = (f32::from(a.0), f32::from(a.1));
    let (bx, by) = (f32::from(b.0), f32::from(b.1));
    let (cx, cy) = (f32::from(c.0), f32::from(c.1));

    (bx - ax) * (cy - ay) - (by - ay) * (cx - ax)
}
