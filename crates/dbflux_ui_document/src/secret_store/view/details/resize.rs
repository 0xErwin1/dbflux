//! Resizing the details pane by its left edge, with the design system's
//! `Splitter`: the gutter drags, steps with the arrow keys while focused, and
//! is a splitter to assistive technology. The width lives for the session of
//! the view, like the sidebar's: neither is persisted.

use super::SecretView;
use dbflux_components::composites::{SplitAxis, Splitter};
use dbflux_components::tokens::{SecretStoreMetrics, SplitterMetrics};
use gpui::prelude::*;
use gpui::*;

/// The widest the pane grows; the listing keeps the rest.
pub(in crate::secret_store) const DETAILS_MAX_WIDTH: Pixels = px(900.0); // guardrail-allow: layout clamp of the pane, no token carries it

impl SecretView {
    /// The pane's width: what the user dragged it to, or the default.
    pub(super) fn details_width(&self) -> Pixels {
        self.details_width
            .unwrap_or(SecretStoreMetrics::DETAILS_WIDTH)
    }

    /// The gutter's focus handle, for the tests that press its keys.
    #[cfg(test)]
    pub(in crate::secret_store) fn details_grip_focus(&self) -> &FocusHandle {
        &self.details_grip_focus
    }

    fn set_details_width(&mut self, width: Pixels, cx: &mut Context<Self>) {
        let width = width.clamp(SecretStoreMetrics::DETAILS_MIN_WIDTH, DETAILS_MAX_WIDTH);

        if self.details_width != Some(width) {
            self.details_width = Some(width);
            cx.notify();
        }
    }

    /// The gutter on the pane's left edge. A drag reports the pointer, so the
    /// width is the distance from it to the pane's right edge, which the
    /// last frame measured; an arrow key moves the edge by its step, so left
    /// widens the pane.
    pub(super) fn render_details_grip(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let on_drag = cx.entity().downgrade();
        let on_step = on_drag.clone();
        let right_edge = self.details_right.clone();

        Splitter::new("secret-details-grip", SplitAxis::Horizontal)
            .track_focus(&self.details_grip_focus)
            .aria_label(dbflux_i18n::t!("document.secret_store.details.resize"))
            .on_drag(move |position, _, cx| {
                let width = right_edge.get() - position - SplitterMetrics::GUTTER / 2.0;

                on_drag
                    .update(cx, |view, cx| view.set_details_width(width, cx))
                    .ok();
            })
            .on_step(move |delta, _, cx| {
                on_step
                    .update(cx, |view, cx| {
                        let width = view.details_width() - delta;
                        view.set_details_width(width, cx);
                    })
                    .ok();
            })
    }

    /// Records where the pane's right edge is, as the last frame painted it.
    pub(super) fn measure_details_edge(&self) -> impl IntoElement {
        let right_edge = self.details_right.clone();

        canvas(
            move |bounds, _, _| right_edge.set(bounds.right()),
            |_, _, _, _| {},
        )
        .absolute()
        .size_full()
    }
}
