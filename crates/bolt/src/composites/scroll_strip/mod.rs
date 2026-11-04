//! `scroll_strip` — a row that keeps its parent's width and scrolls sideways.
//!
//! A tab row or a segmented control wider than its pane would be cut by the
//! pane edge. Wrapped in a strip it stays inside the pane, scrolls on the
//! wheel and the trackpad, draws no scrollbar, and [`StripReveal`] scrolls the
//! selected child into view when the selection or the pane's width changes.

use std::cell::RefCell;
use std::ops::Range;

use gpui::prelude::*;
use gpui::{Div, ElementId, Pixels, ScrollHandle, Stateful, div, point};

use crate::primitives::SegmentBounds;

#[cfg(test)]
mod tests;

/// A horizontally scrollable viewport `id`, at most as wide as its parent,
/// following `handle`. Add the children to scroll as children of the strip;
/// the strip draws no scrollbar.
pub fn scroll_strip(id: impl Into<ElementId>, handle: &ScrollHandle) -> Stateful<Div> {
    div()
        .id(id)
        .flex()
        .flex_none()
        .max_w_full()
        .overflow_x_scroll()
        .track_scroll(handle)
}

/// What a strip last revealed, so a reveal runs again only when the target or
/// the pane's width changes and keeps a scroll the user made until then.
///
/// Call the reveal from the host's `render`, every frame, with the pane's
/// current width: the strip's own scroll runs in the same frame.
#[derive(Default)]
pub struct StripReveal(RefCell<Option<(RevealKey, Pixels)>>);

/// What a reveal aimed at. A segment is known by its id, not by its measured
/// bounds: those were painted before the current scroll offset and shift when
/// the user scrolls.
#[derive(Clone, PartialEq)]
enum RevealKey {
    Range(Pixels, Pixels),
    Segment(String),
}

impl StripReveal {
    // One key for ranges and segments: separate keys would skip a reveal when
    // the target goes range(A), segment(B), range(A).
    fn due(&self, key: RevealKey, width: Pixels) -> bool {
        let mut last = self.0.borrow_mut();
        let current = last
            .as_ref()
            .is_some_and(|(last_key, last_width)| *last_key == key && *last_width == width);
        if !current {
            *last = Some((key, width));
        }
        !current
    }

    /// Scrolls the strip `handle` follows so its child `index` shows, by the
    /// least amount. A child wider than the strip is aligned to its start.
    /// Waits, without remembering anything, until the child has been laid out.
    ///
    /// This measures the child itself rather than using
    /// [`ScrollHandle::scroll_to_item`], which is dropped when requested
    /// before the strip's first prepaint.
    pub fn child(&self, handle: &ScrollHandle, index: usize, width: Pixels) {
        let Some(bounds) = handle.bounds_for_item(index) else {
            return;
        };
        let origin = handle.bounds().left();
        self.range(
            handle,
            bounds.left() - origin..bounds.right() - origin,
            width,
        );
    }

    /// Scrolls the strip so `range` shows, by the least amount. `range` is in
    /// the strip's content coordinates: the distance from the content's start,
    /// so it does not move as the strip scrolls. A range wider than the strip
    /// is aligned to its start. Nothing happens, and nothing is remembered,
    /// until the strip has been laid out.
    pub fn range(&self, handle: &ScrollHandle, range: Range<Pixels>, width: Pixels) {
        let viewport = handle.bounds().size.width;
        if viewport <= Pixels::ZERO {
            return;
        }
        if !self.due(RevealKey::Range(range.start, range.end), width) {
            return;
        }
        Self::reveal_range(handle, range, viewport);
    }

    fn reveal_range(handle: &ScrollHandle, range: Range<Pixels>, viewport: Pixels) {
        let offset = handle.offset();
        let shown_start = -offset.x;
        let shown_end = shown_start + viewport;
        let target = if range.end - range.start > viewport || range.start < shown_start {
            range.start
        } else if range.end > shown_end {
            range.end - viewport
        } else {
            return;
        };
        handle.set_offset(point(-target.max(Pixels::ZERO), offset.y));
    }

    /// Scrolls the strip so the segment `id`, measured by `segments`, shows
    /// exactly, when `id` or `width` differs from the last call and keeps the
    /// user's scroll otherwise. Waits, without remembering anything, until the
    /// strip has been laid out and the segment painted once.
    pub fn segment(
        &self,
        handle: &ScrollHandle,
        segments: &SegmentBounds,
        id: &str,
        width: Pixels,
    ) {
        let viewport = handle.bounds().size.width;
        if viewport <= Pixels::ZERO {
            return;
        }
        let Some(bounds) = segments.get(id) else {
            return;
        };
        if !self.due(RevealKey::Segment(id.to_owned()), width) {
            return;
        }
        let origin = handle.bounds().left() + handle.offset().x;
        Self::reveal_range(
            handle,
            bounds.left() - origin..bounds.right() - origin,
            viewport,
        );
    }
}
