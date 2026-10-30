//! Accessible-name tests for the generic [`Button`], pinned on the product
//! side because they read the accessibility frames through this crate's
//! `accessibility` helpers (which move with the design system later) and use
//! the product icon catalog.
//!
//! The button itself lives in `bolt` and is re-exported at this crate's
//! `controls` path; its generic rendering, focus and size tests live there.

#[cfg(test)]
mod tests {
    use super::super::Button;
    use crate::accessibility::{
        FrameCapture, accessibility_node, accessible_name, unnamed_clickables,
    };
    use crate::icons::AppIcon;
    use gpui::{IntoElement, ParentElement as _, Render, Styled as _, Toggled, Window, div};

    /// A window whose whole body is the buttons under test.
    ///
    /// The buttons come from a factory because a button is a one-shot element:
    /// a repaint of the window needs to build them again.
    struct ButtonRow(std::sync::Arc<dyn Fn() -> Vec<Button>>);

    impl Render for ButtonRow {
        fn render(
            &mut self,
            _window: &mut Window,
            _cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            div().size_full().children((self.0)())
        }
    }

    /// Renders the buttons of `build` and returns the window's frame.
    fn frame_with_buttons(
        build: impl Fn() -> Vec<Button> + 'static,
        cx: &mut gpui::TestAppContext,
    ) -> gpui::AccessibilityFrame {
        cx.update(crate::theme::init);

        let capture = std::sync::Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();
        let (_, window) = cx.add_window_view(move |window, _cx| {
            window.observe_frames(&capture_for_window);
            window.refresh();
            ButtonRow(std::sync::Arc::new(build))
        });
        window.run_until_parked();

        capture.frame(window)
    }

    fn name_of(frame: &gpui::AccessibilityFrame, id: &str) -> Option<String> {
        accessible_name(frame, id)
    }

    #[gpui::test]
    fn a_label_button_is_named_by_its_visible_text(cx: &mut gpui::TestAppContext) {
        let frame = frame_with_buttons(|| vec![Button::new("save", "Save")], cx);

        assert_eq!(name_of(&frame, "save").as_deref(), Some("Save"));
        assert_eq!(unnamed_clickables(&frame), Vec::<String>::new());
    }

    /// An icon-only button draws no text, so its label — which becomes the
    /// tooltip — names it for assistive technology.
    #[gpui::test]
    fn an_icon_only_button_is_named_by_its_label(cx: &mut gpui::TestAppContext) {
        let frame = frame_with_buttons(
            || {
                vec![
                    Button::new("save", "Save file")
                        .icon_only()
                        .icon(AppIcon::Save),
                ]
            },
            cx,
        );

        assert_eq!(name_of(&frame, "save").as_deref(), Some("Save file"));
    }

    /// An icon-only button may carry no label (the caller names the action
    /// with `aria_label` or a tooltip instead), and it may also be disabled.
    #[gpui::test]
    fn an_icon_only_button_is_named_by_its_aria_label_or_tooltip(cx: &mut gpui::TestAppContext) {
        let frame = frame_with_buttons(
            || {
                vec![
                    Button::new("close", "")
                        .icon_only()
                        .icon(AppIcon::CircleX)
                        .aria_label("Close the dialog"),
                    Button::new("remove", "")
                        .icon_only()
                        .icon(AppIcon::Delete)
                        .tooltip("Delete the row")
                        .disabled(true),
                ]
            },
            cx,
        );

        assert_eq!(
            name_of(&frame, "close").as_deref(),
            Some("Close the dialog")
        );
        assert_eq!(name_of(&frame, "remove").as_deref(), Some("Delete the row"));
    }

    /// A toolbar toggle says whether it is on; its fill alone cannot carry
    /// that to assistive technology.
    #[gpui::test]
    fn a_toggled_button_announces_its_state(cx: &mut gpui::TestAppContext) {
        let frame = frame_with_buttons(
            || {
                vec![
                    Button::new("stats", "Stats").selected(true).toggled(true),
                    Button::new("run", "Run"),
                ]
            },
            cx,
        );

        let on = accessibility_node(&frame, "stats").expect("the toggle is an accessible node");
        assert_eq!(on.toggled(), Some(Toggled::True));

        let plain = accessibility_node(&frame, "run").expect("the action is an accessible node");
        assert_eq!(
            plain.toggled(),
            None,
            "a plain action carries no toggle state"
        );
    }

    /// `aria_label` wins over the label the button draws, for a control whose
    /// visible text is an abbreviation of the action.
    #[gpui::test]
    fn aria_label_overrides_the_drawn_text(cx: &mut gpui::TestAppContext) {
        let frame = frame_with_buttons(
            || vec![Button::new("zoom", "1:1").aria_label("Reset the zoom to 100%")],
            cx,
        );

        assert_eq!(
            name_of(&frame, "zoom").as_deref(),
            Some("Reset the zoom to 100%")
        );
    }
}
