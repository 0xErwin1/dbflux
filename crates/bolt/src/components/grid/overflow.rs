//! Horizontal overflow for the grid.
//!
//! A grid draws as wide as its columns add up to. Put it in a pane narrower
//! than that and the columns past the edge are cut, with no way to reach
//! them. [`GridOverflow`] wraps the grid's header row and its row list in one
//! viewport: the two scroll sideways together on a shared
//! [`gpui::ScrollHandle`], a horizontal scrollbar draws along the bottom of
//! the viewport, and the wheel keeps scrolling the row list vertically
//! because the viewport claims only the horizontal axis.
//!
//! While the columns overflow, the row list ends one
//! [`GridMetrics::SCROLLBAR_WIDTH`] above the viewport's edge, so the last row
//! stays reachable at the bottom of the vertical scroll instead of sitting
//! under the bar. A grid that fits its viewport draws exactly as it draws
//! without this wrapper: the content fills the pane and keeps the full height,
//! because no bar is shown and no strip is reserved.
//!
//! The component owns no widths and no scroll offset: the host keeps the
//! column widths, sums them into `total_width`, and keeps the handle. The
//! reserve, and with it the bar, follow the width the grid is given: the
//! viewport is built from its own measured size in the same frame, so a pane
//! that grows or shrinks is laid out correctly without a frame of lag.
//!
//! Column resize keeps working while the grid is scrolled: the header's drag
//! handles move with the header, and [`crate::components::grid::track_column_resize`]
//! resizes by pointer delta rather than by position, so the scroll offset
//! never enters the width.
//!
//! The default viewport takes its height from the pane it is given, which is
//! what a grid filling a document wants. A page that lays out to its content's
//! height — a settings page, a form section, a document that scrolls as a
//! whole — has no such height, so there the viewport would lay out 0 px tall
//! and the rows would be clipped away. [`GridOverflow::content_height`] is the
//! opt-in for those pages: the viewport takes the height of its header and its
//! rows instead, still scrolls them sideways on the host's handle, still
//! reserves the strip only while the columns overflow, and lets a vertical
//! wheel scroll the page around it, because the rows do not scroll vertically
//! on their own in that mode. A grid that fits draws as it does unwrapped,
//! exactly as in the default mode.
//!
//! The default mode measures the width of its viewport during layout, with a
//! [`gpui::container_query`], so the strip is reserved in the frame the width
//! changes. A content-height viewport has no height for that query to be
//! given, so it compares the total width with the width the handle says its
//! viewport had in the frame just drawn, and asks for one more frame while
//! that width differs from the one it drew with. The strip therefore settles
//! in the frame after the page's width first reaches the grid, the same frame
//! gpui's own scrollbar settles in, and a grid that fits draws as it does
//! unwrapped on every frame.

use gpui::prelude::*;
use gpui::{
    AnyElement, App, Div, ElementId, IntoElement, Pixels, RenderOnce, ScrollHandle, Stateful,
    StyleRefinement, Styled, Window, container_query, div, px,
};
use gpui_component::scroll::{Scrollbar, ScrollbarMode};

use crate::selector::id_selector;
use crate::tokens::GridMetrics;

/// A grid inside a viewport that scrolls sideways when its columns are wider
/// than it.
///
/// Compose it the way the grid composes header and rows:
/// [`GridOverflow::new`] with the id the host addresses the viewport by, the
/// shared [`ScrollHandle`] and the total width of the grid's columns (the
/// same value [`super::render_rows`] and [`super::render_header`] take), then
/// [`GridOverflow::header`] and [`GridOverflow::rows`]. It lays out as a flex
/// column that clips its overflow, so chain the sizing the grid needs
/// (`.flex_1()`, `.min_h_0()`) like on a `div`.
///
/// That sizing is for a pane with a definite height. A page that takes its
/// content's height instead takes [`GridOverflow::content_height`].
///
/// While the columns are wider than the viewport, the row list ends one
/// [`GridMetrics::SCROLLBAR_WIDTH`] above the viewport's bottom edge, so the
/// bar covers no row; a grid that fits keeps the full height of its viewport.
///
/// The host keeps the handle across frames: a `ScrollHandle` created on every
/// render starts the grid back at the left. The id is recorded as the
/// viewport's debug selector, and the scrollbar strip's as
/// `<id>-scrollbar`.
#[derive(IntoElement)]
pub struct GridOverflow {
    base: Div,
    id: ElementId,
    handle: ScrollHandle,
    total_width: f32,
    content_height: bool,
    header: Option<AnyElement>,
    rows: Option<AnyElement>,
}

impl GridOverflow {
    pub fn new(id: impl Into<ElementId>, handle: &ScrollHandle, total_width: f32) -> Self {
        Self {
            base: div().relative().flex().flex_col().overflow_hidden(),
            id: id.into(),
            handle: handle.clone(),
            total_width,
            content_height: false,
            header: None,
            rows: None,
        }
    }

    /// Takes the height of the header and the rows instead of the height of
    /// the pane the grid is given.
    ///
    /// For a page that lays out to its content's height, where a viewport that
    /// fills its pane has no pane to fill and would lay out 0 px tall. The
    /// grid still scrolls sideways on the same handle, and still reserves the
    /// strip the bar draws in only while the columns overflow; the rows do not
    /// scroll vertically on their own, so a vertical wheel goes on to the page
    /// around the grid.
    pub fn content_height(mut self) -> Self {
        self.content_height = true;
        self
    }

    /// The header row above the rows, drawn at `total_width` so it scrolls
    /// with them.
    pub fn header(mut self, header: impl IntoElement) -> Self {
        self.header = Some(header.into_any_element());
        self
    }

    /// The row list under the header: it keeps its own vertical scroll
    /// inside the viewport.
    pub fn rows(mut self, rows: impl IntoElement) -> Self {
        self.rows = Some(rows.into_any_element());
        self
    }
}

impl Styled for GridOverflow {
    fn style(&mut self) -> &mut StyleRefinement {
        self.base.style()
    }
}

impl RenderOnce for GridOverflow {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        let scrollbar_id = ElementId::Name(format!("{}-scrollbar", self.id).into());
        let strip_id = scrollbar_id.clone();
        let viewport_id = self.id;
        let handle = self.handle.clone();
        let content_height = self.content_height;
        let content = div()
            .flex()
            .flex_col()
            // The content fills a viewport with a pane's height, so a grid that
            // fits spans the pane as it does unwrapped, and grows past it only
            // when the columns need it. In content-height mode the viewport
            // takes its height from this content instead.
            .when(!content_height, |content| content.h_full())
            .w_full()
            .min_w(px(self.total_width))
            .children(self.header)
            .children(self.rows)
            .into_any_element();

        if content_height {
            // Without a pane's height there is no height to measure against, so
            // the container query the default mode uses cannot size this
            // viewport. The width the viewport was given is read from the
            // handle the viewport tracks instead, which carries the last
            // frame's layout; a frame drawn with a width that has since
            // changed asks for one more, so the strip settles in the frame
            // after the page's width first reaches the grid.
            let drawn_width = self.handle.bounds().size.width;
            // Until the first layout reaches the viewport there is no width to
            // compare against, and a grid drawn as if it fitted is the grid it
            // would draw unwrapped.
            let overflowing =
                drawn_width > Pixels::ZERO && self.total_width > f32::from(drawn_width);
            let settled_handle = self.handle.clone();

            return self
                .base
                .on_children_prepainted(move |_, window, _cx| {
                    if settled_handle.bounds().size.width != drawn_width {
                        window.request_animation_frame();
                    }
                })
                .child(viewport(viewport_id, &handle, overflowing, true, content))
                .child(strip(strip_id, scrollbar_id, &self.handle));
        }

        let total_width = self.total_width;
        self.base
            // The viewport is built from the width it is given, in the same
            // frame, so the strip is reserved exactly while the columns
            // overflow it and a grid that fits keeps its full height.
            .child(
                container_query(move |size, _window, _cx| {
                    let overflowing = total_width > f32::from(size.width);

                    viewport(viewport_id, &handle, overflowing, false, content)
                })
                .flex_1()
                .min_h_0()
                .w_full(),
            )
            .child(strip(strip_id, scrollbar_id, &self.handle))
    }
}

/// The sideways viewport the header and the rows scroll in.
///
/// `content_height` leaves the height to the content instead of filling the
/// pane the viewport is given, which is what a page that lays out to its
/// content's height needs.
fn viewport(
    id: ElementId,
    handle: &ScrollHandle,
    overflowing: bool,
    content_height: bool,
    content: AnyElement,
) -> Stateful<Div> {
    div()
        .debug_selector(id_selector(&id))
        .id(id)
        .when(content_height, |viewport| viewport.w_full())
        .when(!content_height, |viewport| viewport.size_full())
        .overflow_x_scroll()
        // A vertical wheel belongs to the row list, never to the sideways
        // scroll, and in content-height mode it belongs to the page around
        // the grid.
        .restrict_scroll_to_axis()
        .track_scroll(handle)
        .when(overflowing, |viewport| {
            viewport.pb(GridMetrics::SCROLLBAR_WIDTH)
        })
        .child(content)
}

/// The strip along the viewport's bottom edge, where the bar draws while the
/// columns overflow, and where it draws nothing when they fit.
fn strip(strip_id: ElementId, scrollbar_id: ElementId, handle: &ScrollHandle) -> Stateful<Div> {
    div()
        .debug_selector(id_selector(&strip_id))
        .id(strip_id)
        .absolute()
        .left_0()
        .right_0()
        .bottom_0()
        .h(GridMetrics::SCROLLBAR_WIDTH)
        .child(
            Scrollbar::horizontal(handle)
                .id(scrollbar_id)
                // The scrollbar hides itself when the columns fit the
                // viewport, so it can always be present.
                .mode(ScrollbarMode::Always)
                .viewport_from_layout(),
        )
}
