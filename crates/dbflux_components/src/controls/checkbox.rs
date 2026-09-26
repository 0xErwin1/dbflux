use std::rc::Rc;

use gpui::prelude::*;
use gpui::{
    App, ElementId, FocusHandle, MouseButton, Role, SharedString, Toggled, Window, div, relative,
};
use gpui_component::ActiveTheme;

use crate::icons::AppIcon;
use crate::primitives::{Chamfer, ChamferFillKind, ChamferRing, Icon};
use crate::tokens::{ChamferCut, ChromeColors, Fields};

type CheckboxHandler = Rc<dyn Fn(&bool, &mut Window, &mut App)>;

/// A checkbox drawn as a 16 px chamfered box.
///
/// Checked, the box is the byzantine fill with a white check mark; unchecked,
/// it is an inset 1.5 px line. The label sits to the right. The row is one
/// focusable accessibility node with the checkbox role: Enter and Space
/// toggle it, a pointer press toggles it without moving focus, and keyboard
/// focus draws the tint ring around the box (outside the fill when checked,
/// inside the line when unchecked).
#[derive(IntoElement)]
pub struct Checkbox {
    id: ElementId,
    checked: bool,
    disabled: bool,
    label: Option<SharedString>,
    aria_label: Option<SharedString>,
    on_click: Option<CheckboxHandler>,
}

impl Checkbox {
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            checked: false,
            disabled: false,
            label: None,
            aria_label: None,
            on_click: None,
        }
    }

    pub fn checked(mut self, checked: bool) -> Self {
        self.checked = checked;
        self
    }

    /// Dims the checkbox and ignores activation.
    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    /// Sets the visible label, which also names the checkbox for assistive
    /// technology unless [`Self::aria_label`] overrides it.
    pub fn label(mut self, label: impl Into<SharedString>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Sets the accessible name of the checkbox, normally the visible text of
    /// the row it toggles.
    ///
    /// Use it when that text is drawn next to the checkbox instead of through
    /// [`Self::label`]. A checkbox with neither has no name, so a screen reader
    /// announces it as a bare checkbox and UI automation can only find it by id.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }

    /// Called with the requested checked value when the checkbox is
    /// activated. The owner stores the value and re-renders.
    pub fn on_click(mut self, handler: impl Fn(&bool, &mut Window, &mut App) + 'static) -> Self {
        self.on_click = Some(Rc::new(handler));
        self
    }

    fn focus_handle(&self, window: &mut Window, cx: &mut App) -> FocusHandle {
        window
            .use_keyed_state(self.id.clone(), cx, |_, cx| cx.focus_handle())
            .read(cx)
            .clone()
    }
}

impl RenderOnce for Checkbox {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let focus_handle = self.focus_handle(window, cx);
        let focused = !self.disabled && focus_handle.is_focused(window);
        let theme = cx.theme();

        let checked = self.checked;
        let opacity = if self.disabled {
            Fields::DISABLED_OPACITY
        } else {
            1.0
        };

        let box_shape = if checked {
            Chamfer::new(ChamferCut::KEYCAP).fill(theme.primary.opacity(opacity))
        } else {
            Chamfer::new(ChamferCut::KEYCAP)
                .ring(ChamferRing::outline(theme.input.opacity(opacity)))
        };

        let fill_kind = if checked {
            ChamferFillKind::Filled
        } else {
            ChamferFillKind::Surface
        };

        let indicator = div()
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .justify_center()
            .size(Fields::CHECKBOX_SIZE)
            .child(box_shape)
            .when(checked, |this| {
                this.child(
                    Icon::new(AppIcon::Check)
                        .size(Fields::CHECK_MARK)
                        .color(theme.primary_foreground.opacity(opacity)),
                )
            })
            .when(focused, |this| {
                this.child(
                    Chamfer::new(ChamferCut::KEYCAP)
                        .ring(ChamferRing::focus_for(ChromeColors::tint(theme), fill_kind)),
                )
            });

        let accessible_name = self.aria_label.or_else(|| self.label.clone());
        let label_color = if self.disabled {
            theme.muted_foreground
        } else {
            theme.foreground
        };
        let on_click = self.on_click.filter(|_| !self.disabled);

        div()
            .id(self.id)
            .role(Role::CheckBox)
            .aria_toggled(if checked {
                Toggled::True
            } else {
                Toggled::False
            })
            .when_some(accessible_name, |this, name| this.aria_label(name))
            .when(!self.disabled, |this| {
                this.track_focus(&focus_handle.clone().tab_index(0).tab_stop(true))
            })
            .flex()
            .items_start()
            .gap(Fields::CHECKBOX_GAP)
            .line_height(relative(1.))
            .when(!self.disabled, |this| this.cursor_pointer())
            .when(self.disabled, |this| this.cursor_not_allowed())
            .on_mouse_down(MouseButton::Left, |_, window, _| {
                window.prevent_default();
            })
            .when_some(on_click, |this, on_click| {
                this.on_click(move |_, window, cx| {
                    on_click(&!checked, window, cx);
                })
            })
            .child(indicator)
            .when_some(self.label, |this, label| {
                this.child(
                    div()
                        .min_h(Fields::CHECKBOX_SIZE)
                        .flex()
                        .items_center()
                        .text_color(label_color)
                        .child(label),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use gpui::{
        AccessibilityFrame, Context, FrameObserver, IntoElement, ParentElement as _, Render, Role,
        Styled as _, TestAppContext, Window, div,
    };

    use super::Checkbox;

    /// Keeps the latest rendered accessibility frame of the window it observes.
    #[derive(Default)]
    struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

    impl FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    struct Row {
        named: bool,
    }

    impl Render for Row {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            let checkbox = Checkbox::new("vim-mode");
            let checkbox = if self.named {
                checkbox.aria_label("Enable Vim mode")
            } else {
                checkbox
            };

            div().size_full().child(checkbox).child("Enable Vim mode")
        }
    }

    /// Renders one checkbox with its text drawn beside it and returns the
    /// checkbox's accessible name.
    fn render_checkbox_name(named: bool, cx: &mut TestAppContext) -> Option<String> {
        cx.update(gpui_component::init);

        let capture = Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();
        let (_view, visual) = cx.add_window_view(move |window, _cx| {
            window.observe_frames(&capture_for_window);
            window.refresh();
            Row { named }
        });
        visual.run_until_parked();

        let frame = capture
            .0
            .lock()
            .expect("frame capture lock")
            .clone()
            .expect("the window rendered a frame");

        let (_, node) = frame
            .nodes()
            .find(|(_, node)| node.id() == "vim-mode")
            .expect("the checkbox is found by its id");
        let accessible = frame
            .accessibility_node(node)
            .expect("the checkbox is an accessible node");
        assert_eq!(accessible.role(), Role::CheckBox);

        accessible.label().map(ToOwned::to_owned)
    }

    #[gpui::test]
    fn aria_label_names_the_checkbox(cx: &mut TestAppContext) {
        assert_eq!(
            render_checkbox_name(true, cx).as_deref(),
            Some("Enable Vim mode")
        );
    }

    #[gpui::test]
    fn text_drawn_beside_the_checkbox_does_not_name_it(cx: &mut TestAppContext) {
        assert_eq!(render_checkbox_name(false, cx), None);
    }
}
