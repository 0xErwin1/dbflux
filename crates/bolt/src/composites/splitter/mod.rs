//! `Splitter` — the resize gutter between two panes: a thin strip with a
//! three-dot grip that drags, steps with the arrow keys and collapses on a
//! double click or Enter.

use std::rc::Rc;

use gpui::prelude::*;
use gpui::{
    App, ElementId, FocusHandle, KeyDownEvent, Orientation, Pixels, Render, Role, Window, div,
};
use gpui_component::ActiveTheme;

use crate::primitives::is_focus_visible;
use crate::selector::id_selector;
use crate::tokens::{ChromeColors, SplitterMetrics};

/// The direction the two panes are laid out in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitAxis {
    /// Panes side by side: a vertical gutter that resizes columns.
    Horizontal,
    /// Panes stacked: a horizontal gutter that resizes rows.
    Vertical,
}

impl SplitAxis {
    /// The step an arrow key asks for, or `None` for keys across the axis.
    fn key_step(self, key: &str, step: Pixels) -> Option<Pixels> {
        match (self, key) {
            (Self::Horizontal, "left") | (Self::Vertical, "up") => Some(-step),
            (Self::Horizontal, "right") | (Self::Vertical, "down") => Some(step),
            _ => None,
        }
    }
}

/// The value a splitter drag carries, so a drag move reaches only the
/// splitter that started it.
struct SplitterDrag(ElementId);

/// The view drawn under the pointer while a splitter drags: nothing, the
/// gutter itself shows the drag.
struct NoDragPreview;

impl Render for NoDragPreview {
    fn render(&mut self, _window: &mut Window, _cx: &mut gpui::Context<Self>) -> impl IntoElement {
        gpui::Empty
    }
}

type PositionHandler = Rc<dyn Fn(Pixels, &mut Window, &mut App)>;
type ToggleHandler = Rc<dyn Fn(&mut Window, &mut App)>;

/// A resize gutter between two panes.
///
/// The owner keeps the pane size: the splitter reports the pointer position
/// along the axis while dragging ([`Self::on_drag`], window coordinates), a
/// signed step for the arrow keys ([`Self::on_step`], `KEY_STEP` by
/// default), and a collapse toggle for a double click or Enter
/// ([`Self::on_toggle_collapse`]). Keys work while the handle passed to
/// [`Self::track_focus`] holds focus.
///
/// Drawn as `SplitterMetrics::GUTTER` across the split, spanning the other
/// axis, with a three-dot muted grip in the middle; the accent wash at
/// `HOVER_ALPHA` on hover and the tint while dragging or keyboard-focused,
/// like the sidebar and inspector grips. Its role is a splitter.
#[derive(IntoElement)]
pub struct Splitter {
    id: ElementId,
    axis: SplitAxis,
    step: Pixels,
    active: bool,
    focus_handle: Option<FocusHandle>,
    on_drag: Option<PositionHandler>,
    on_step: Option<PositionHandler>,
    on_toggle_collapse: Option<ToggleHandler>,
}

impl Splitter {
    pub fn new(id: impl Into<ElementId>, axis: SplitAxis) -> Self {
        Self {
            id: id.into(),
            axis,
            step: SplitterMetrics::KEY_STEP,
            active: false,
            focus_handle: None,
            on_drag: None,
            on_step: None,
            on_toggle_collapse: None,
        }
    }

    /// The distance one arrow key asks for.
    pub fn step(mut self, step: Pixels) -> Self {
        self.step = step;
        self
    }

    /// Draws the gutter as being dragged, for owners that know a drag is
    /// under way.
    pub fn active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }

    /// Makes the gutter focusable through `handle`, which enables the keys.
    pub fn track_focus(mut self, handle: &FocusHandle) -> Self {
        self.focus_handle = Some(handle.clone());
        self
    }

    pub fn on_drag(mut self, handler: impl Fn(Pixels, &mut Window, &mut App) + 'static) -> Self {
        self.on_drag = Some(Rc::new(handler));
        self
    }

    pub fn on_step(mut self, handler: impl Fn(Pixels, &mut Window, &mut App) + 'static) -> Self {
        self.on_step = Some(Rc::new(handler));
        self
    }

    pub fn on_toggle_collapse(mut self, handler: impl Fn(&mut Window, &mut App) + 'static) -> Self {
        self.on_toggle_collapse = Some(Rc::new(handler));
        self
    }

    fn render_grip(&self, color: gpui::Hsla) -> impl IntoElement {
        let dot = || div().size(SplitterMetrics::DOT).bg(color);
        let grip_id = self.id.clone();

        let grip = div()
            .debug_selector(move || format!("{grip_id}-grip"))
            .flex()
            .items_center()
            .justify_center()
            .gap(SplitterMetrics::DOT_GAP);

        match self.axis {
            SplitAxis::Horizontal => grip.flex_col().h(SplitterMetrics::GRIP_LENGTH),
            SplitAxis::Vertical => grip.flex_row().w(SplitterMetrics::GRIP_LENGTH),
        }
        .child(dot())
        .child(dot())
        .child(dot())
    }
}

impl RenderOnce for Splitter {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let hover = theme.accent.opacity(SplitterMetrics::HOVER_ALPHA);
        let tint = ChromeColors::tint(theme);
        let focused = self
            .focus_handle
            .as_ref()
            .is_some_and(|handle| is_focus_visible(handle, window));
        let grip = self.render_grip(theme.muted_foreground);

        let axis = self.axis;
        let step = self.step;
        let drag_id = self.id.clone();
        let on_drag = self.on_drag.clone();
        let on_step = self.on_step.clone();
        let on_key_toggle = self.on_toggle_collapse.clone();
        let on_click_toggle = self.on_toggle_collapse.clone();

        div()
            .debug_selector(id_selector(&self.id))
            .id(self.id.clone())
            .role(Role::Splitter)
            .aria_orientation(match axis {
                SplitAxis::Horizontal => Orientation::Vertical,
                SplitAxis::Vertical => Orientation::Horizontal,
            })
            .flex()
            .flex_none()
            .items_center()
            .justify_center()
            .map(|gutter| match axis {
                SplitAxis::Horizontal => gutter
                    .w(SplitterMetrics::GUTTER)
                    .h_full()
                    .cursor_col_resize(),
                SplitAxis::Vertical => gutter
                    .h(SplitterMetrics::GUTTER)
                    .w_full()
                    .cursor_row_resize(),
            })
            .hover(move |gutter| gutter.bg(hover))
            .when(self.active || focused, |gutter| gutter.bg(tint))
            .when_some(self.focus_handle.as_ref(), |gutter, handle| {
                gutter.track_focus(handle)
            })
            .child(grip)
            .on_drag(SplitterDrag(self.id.clone()), |_, _, _, cx| {
                cx.new(|_| NoDragPreview)
            })
            .on_drag_move::<SplitterDrag>(move |event, window, cx| {
                if event.drag(cx).0 != drag_id {
                    return;
                }

                if let Some(on_drag) = on_drag.as_ref() {
                    let position = match axis {
                        SplitAxis::Horizontal => event.event.position.x,
                        SplitAxis::Vertical => event.event.position.y,
                    };
                    on_drag(position, window, cx);
                }
            })
            .on_key_down(move |event: &KeyDownEvent, window, cx| {
                let keystroke = &event.keystroke;
                if keystroke.modifiers.modified() {
                    return;
                }

                if let Some(delta) = axis.key_step(keystroke.key.as_str(), step) {
                    if let Some(on_step) = on_step.as_ref() {
                        cx.stop_propagation();
                        on_step(delta, window, cx);
                    }
                } else if keystroke.key == "enter"
                    && let Some(toggle) = on_key_toggle.as_ref()
                {
                    cx.stop_propagation();
                    toggle(window, cx);
                }
            })
            .on_click(move |event, window, cx| {
                if event.click_count() == 2
                    && let Some(toggle) = on_click_toggle.as_ref()
                {
                    toggle(window, cx);
                }
            })
    }
}

#[cfg(test)]
mod tests;
