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

use gpui::prelude::*;
use gpui::{
    AnyElement, App, Div, ElementId, IntoElement, RenderOnce, ScrollHandle, StyleRefinement,
    Styled, Window, container_query, div, px,
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
            header: None,
            rows: None,
        }
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
        let total_width = self.total_width;
        let content = div()
            .flex()
            .flex_col()
            .h_full()
            // The content fills the viewport, so a grid that fits spans the
            // pane as it does unwrapped, and grows past it only when the
            // columns need it.
            .w_full()
            .min_w(px(total_width))
            .children(self.header)
            .children(self.rows)
            .into_any_element();

        self.base
            // The viewport is built from the width it is given, in the same
            // frame, so the strip is reserved exactly while the columns
            // overflow it and a grid that fits keeps its full height.
            .child(
                container_query(move |size, _window, _cx| {
                    let overflowing = total_width > f32::from(size.width);

                    div()
                        .debug_selector(id_selector(&viewport_id))
                        .id(viewport_id)
                        .size_full()
                        .overflow_x_scroll()
                        // A vertical wheel belongs to the row list, never to
                        // the sideways scroll.
                        .restrict_scroll_to_axis()
                        .track_scroll(&handle)
                        .when(overflowing, |viewport| {
                            viewport.pb(GridMetrics::SCROLLBAR_WIDTH)
                        })
                        .child(content)
                })
                .flex_1()
                .min_h_0()
                .w_full(),
            )
            // The bar sits in the strip the row list leaves free while the
            // columns overflow, and draws nothing when they fit.
            .child(
                div()
                    .debug_selector(id_selector(&strip_id))
                    .id(strip_id)
                    .absolute()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .h(GridMetrics::SCROLLBAR_WIDTH)
                    .child(
                        Scrollbar::horizontal(&self.handle)
                            .id(scrollbar_id)
                            // The scrollbar hides itself when the columns fit
                            // the viewport, so it can always be present.
                            .mode(ScrollbarMode::Always)
                            .viewport_from_layout(),
                    ),
            )
    }
}
