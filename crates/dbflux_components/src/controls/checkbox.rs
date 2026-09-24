use gpui::{AnyElement, App, ElementId, IntoElement, SharedString, Window};
use gpui_component::checkbox::Checkbox as GpuiCheckbox;
use gpui_component::text::Text;

/// Thin wrapper around `gpui_component::checkbox::Checkbox` that applies
/// DBFlux design system defaults.
pub struct Checkbox {
    inner: GpuiCheckbox,
}

impl Checkbox {
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            inner: GpuiCheckbox::new(id),
        }
    }

    pub fn checked(mut self, checked: bool) -> Self {
        self.inner = self.inner.checked(checked);
        self
    }

    pub fn label(mut self, label: impl Into<Text>) -> Self {
        self.inner = self.inner.label(label);
        self
    }

    /// Sets the accessible name of the checkbox, normally the visible text of
    /// the row it toggles.
    ///
    /// Use it when that text is drawn next to the checkbox instead of through
    /// [`Self::label`]. A checkbox with neither has no name, so a screen reader
    /// announces it as a bare checkbox and UI automation can only find it by id.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.inner = self.inner.accessibility_label(label);
        self
    }

    pub fn on_click(mut self, handler: impl Fn(&bool, &mut Window, &mut App) + 'static) -> Self {
        self.inner = self.inner.on_click(handler);
        self
    }
}

impl IntoElement for Checkbox {
    type Element = AnyElement;

    fn into_element(self) -> Self::Element {
        self.inner.into_any_element()
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
