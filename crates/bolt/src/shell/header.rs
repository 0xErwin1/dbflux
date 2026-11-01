//! The header strip: the fixed-height row at the top of a master list or
//! panel, on the island's top edge.
//!
//! A title as an uppercase label, optional leading elements before it and
//! optional trailing actions after it, with a hairline along the bottom. The
//! host wires its own handlers onto the elements it passes into the slots.

use gpui::{App, IntoElement, ParentElement, RenderOnce, SharedString, Styled, Window, div};
use gpui_component::ActiveTheme;

use crate::primitives::Text;
use crate::tokens::HeaderMetrics;

/// A panel header: `HeaderMetrics::PANEL_HEIGHT` tall, the title as an
/// uppercase `Label`, leading elements before it, trailing actions after it,
/// and a line along the bottom.
#[derive(IntoElement)]
pub struct HeaderStrip {
    title: SharedString,
    leading: Vec<gpui::AnyElement>,
    trailing: Vec<gpui::AnyElement>,
}

impl HeaderStrip {
    pub fn new(title: impl Into<SharedString>) -> Self {
        Self {
            title: title.into(),
            leading: Vec::new(),
            trailing: Vec::new(),
        }
    }

    /// An element drawn before the title, such as a back or collapse button.
    pub fn leading(mut self, element: impl IntoElement) -> Self {
        self.leading.push(element.into_any_element());
        self
    }

    /// A trailing action, drawn right-aligned after the title.
    pub fn trailing(mut self, element: impl IntoElement) -> Self {
        self.trailing.push(element.into_any_element());
        self
    }
}

impl RenderOnce for HeaderStrip {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(HeaderMetrics::PANEL_GAP)
            .h(HeaderMetrics::PANEL_HEIGHT)
            .pl(HeaderMetrics::PANEL_PADDING_LEFT)
            .pr(HeaderMetrics::PANEL_PADDING_RIGHT)
            .border_b_1()
            .border_color(cx.theme().border)
            .children(self.leading)
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .child(Text::label(self.title)),
            )
            .children(self.trailing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controls::Button;
    use crate::tokens::{BASE_REM, HeaderMetrics};
    use gpui::prelude::*;
    use gpui::px;

    fn install_test_theme(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(|cx| {
            crate::theme::install(
                crate::brand::Brand::neutral(),
                crate::palette::PaletteMode::Dark,
                crate::density::Density::Default,
                cx,
            );
        });
    }

    struct HeaderHarness;

    impl Render for HeaderHarness {
        fn render(
            &mut self,
            _window: &mut Window,
            _cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            div()
                .size_full()
                .flex()
                .flex_col()
                .child(
                    div()
                        .debug_selector(|| "header-under-test".to_string())
                        .child(
                            HeaderStrip::new("Connections").trailing(
                                div()
                                    .debug_selector(|| "header-actions".to_string())
                                    .child(Button::new("header-action", "Add")),
                            ),
                        ),
                )
                .child(div().flex_1())
        }
    }

    fn open_header(cx: &mut gpui::TestAppContext) -> &mut gpui::VisualTestContext {
        install_test_theme(cx);

        let (_, window) = cx.add_window_view(|window, cx| {
            let harness = cx.new(|_| HeaderHarness);
            gpui_component::Root::new(harness, window, cx)
        });
        window.run_until_parked();
        window
    }

    #[gpui::test]
    fn strip_renders_at_the_panel_header_height(cx: &mut gpui::TestAppContext) {
        let window = open_header(cx);

        let header = window
            .debug_bounds("header-under-test")
            .expect("the header strip is laid out");

        assert_eq!(
            header.size.height,
            HeaderMetrics::PANEL_HEIGHT.to_pixels(px(BASE_REM)),
            "the strip is one panel header tall"
        );
    }

    #[gpui::test]
    fn trailing_actions_sit_right_of_the_title(cx: &mut gpui::TestAppContext) {
        let window = open_header(cx);

        let header = window
            .debug_bounds("header-under-test")
            .expect("the header strip is laid out");
        let action = window
            .debug_bounds("header-actions")
            .expect("the trailing action is laid out");

        assert!(
            action.left() > header.left(),
            "the action renders inside the strip"
        );
        assert!(
            action.right() <= header.right(),
            "the action is right-aligned inside the strip, not past it"
        );
    }
}
