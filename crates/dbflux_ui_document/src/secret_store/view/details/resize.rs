//! Dragging the left edge of the details pane to resize it, the way the
//! sidebar's seam resizes the sidebar. The width lives for the session of the
//! view, like the sidebar's: neither is persisted.

use super::SecretView;
use dbflux_components::tokens::SecretStoreMetrics;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// The widest the pane grows; the listing keeps the rest.
const DETAILS_MAX_WIDTH: Pixels = px(900.0);

/// How far the grip reaches to each side of the pane's edge.
const GRIP_REACH: Pixels = px(4.0); // guardrail-allow: a hit target on the pane's edge, not spacing

/// Width of the line drawn on the edge while the grip is hovered or dragged.
const GRIP_LINE_WIDTH: Pixels = px(2.0);

/// A drag in flight: where the pointer and the pane's width were at the press.
#[derive(Clone, Copy, Debug)]
pub(in crate::secret_store) struct DetailsResize {
    start_x: Pixels,
    start_width: Pixels,
}

impl SecretView {
    /// The pane's width: what the user dragged it to, or the default.
    pub(super) fn details_width(&self) -> Pixels {
        self.details_width
            .unwrap_or(SecretStoreMetrics::DETAILS_WIDTH)
    }

    pub(in crate::secret_store) fn begin_details_resize(
        &mut self,
        position_x: Pixels,
        cx: &mut Context<Self>,
    ) {
        self.details_resize = Some(DetailsResize {
            start_x: position_x,
            start_width: self.details_width(),
        });
        cx.notify();
    }

    /// Dragging left widens the pane, because its edge is its left one.
    pub(in crate::secret_store) fn drag_details_resize(
        &mut self,
        position_x: Pixels,
        cx: &mut Context<Self>,
    ) {
        let Some(resize) = self.details_resize else {
            return;
        };

        let width = resize.start_width + (resize.start_x - position_x);
        self.details_width =
            Some(width.clamp(SecretStoreMetrics::DETAILS_MIN_WIDTH, DETAILS_MAX_WIDTH));
        cx.notify();
    }

    pub(in crate::secret_store) fn finish_details_resize(&mut self, cx: &mut Context<Self>) {
        if self.details_resize.take().is_some() {
            cx.notify();
        }
    }

    pub(in crate::secret_store) fn is_resizing_details(&self) -> bool {
        self.details_resize.is_some()
    }

    /// The grip on the pane's left edge. While a drag is in flight the
    /// window-level listeners follow the pointer wherever it goes, because
    /// element listeners stop seeing the drag once the pointer leaves the
    /// grip; they must be registered during paint, hence the canvas.
    pub(super) fn render_details_grip(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let line_color = cx.theme().accent.opacity(0.6);
        let dragging = self.is_resizing_details();
        let view = cx.entity().clone();

        div()
            .id("secret-details-grip")
            .group("secret-details-grip")
            .absolute()
            .top_0()
            .bottom_0()
            .left(-GRIP_REACH)
            .w(GRIP_REACH * 2.0)
            .cursor_col_resize()
            .child(
                div()
                    .absolute()
                    .top_0()
                    .bottom_0()
                    .left(GRIP_REACH - GRIP_LINE_WIDTH / 2.0)
                    .w(GRIP_LINE_WIDTH)
                    .group_hover("secret-details-grip", move |line| line.bg(line_color))
                    .when(dragging, move |line| line.bg(line_color)),
            )
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, event: &MouseDownEvent, _, cx| {
                    this.begin_details_resize(event.position.x, cx);
                }),
            )
            .when(dragging, |grip| {
                grip.child(
                    canvas(
                        |_, _, _| {},
                        move |_, _, window, _| {
                            window.on_mouse_event({
                                let view = view.clone();
                                move |event: &MouseMoveEvent, phase, _, cx| {
                                    if phase.bubble() {
                                        view.update(cx, |view, cx| {
                                            view.drag_details_resize(event.position.x, cx);
                                        });
                                    }
                                }
                            });

                            window.on_mouse_event({
                                let view = view.clone();
                                move |_: &MouseUpEvent, phase, _, cx| {
                                    if phase.bubble() {
                                        view.update(cx, |view, cx| view.finish_details_resize(cx));
                                    }
                                }
                            });
                        },
                    )
                    .absolute()
                    .size_full(),
                )
            })
    }
}
