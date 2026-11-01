//! The master-detail frame: a fixed-width list region beside a detail
//! region, the split both products build by hand around their sidebars and
//! settings pages.
//!
//! The list region takes a fixed width and never shrinks or grows; the
//! detail region takes the rest. A hairline divider runs along the list's
//! right edge unless `.divider(false)` turns it off, for hosts that separate
//! the two regions another way (island gaps, the dock's own edge).
//!
//! The frame is static: it does not drag-resize. Only one of the two products
//! resizes its master list today, and it does that in its own dock layer
//! with a grip, minimum and maximum widths, and persistence — none of which
//! both products share. A resizable split belongs to a later slice once a
//! second product asks for it.

use gpui::prelude::*;
use gpui::{App, ElementId, IntoElement, ParentElement, Pixels, RenderOnce, Styled, Window, div};
use gpui_component::ActiveTheme;

use crate::tokens::{Borders, ShellMetrics};

/// The default master-list width: the width of the main window's sidebar.
pub const DEFAULT_LIST_WIDTH: Pixels = ShellMetrics::SIDEBAR_WIDTH;

/// A fixed-width list region beside a detail region that takes the rest.
#[derive(IntoElement)]
pub struct MasterDetail {
    id: ElementId,
    list_width: Pixels,
    divider: bool,
    list: gpui::AnyElement,
    detail: gpui::AnyElement,
}

impl MasterDetail {
    pub fn new(list_width: Pixels, list: impl IntoElement, detail: impl IntoElement) -> Self {
        Self {
            id: ElementId::Name("master-detail".into()),
            list_width,
            divider: true,
            list: list.into_any_element(),
            detail: detail.into_any_element(),
        }
    }

    /// Names the frame, used for the divider's element id.
    pub fn id(mut self, id: impl Into<ElementId>) -> Self {
        self.id = id.into();
        self
    }

    /// Whether the hairline divider runs along the list's right edge
    /// (default `true`).
    pub fn divider(mut self, divider: bool) -> Self {
        self.divider = divider;
        self
    }
}

impl RenderOnce for MasterDetail {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let divider_id = match &self.id {
            ElementId::Name(name) => Some(ElementId::Name(format!("{name}-divider").into())),
            _ => None,
        };

        let list = div()
            .relative()
            .flex_none()
            .flex()
            .flex_col()
            .w(self.list_width)
            .h_full()
            .when(self.divider, |region| {
                let border = cx.theme().border;
                fn paint<E: Styled>(el: E, border: gpui::Hsla) -> E {
                    el.absolute()
                        .top_0()
                        .bottom_0()
                        .right_0()
                        .w(Borders::THIN)
                        .bg(border)
                }
                let line = match divider_id {
                    Some(id) => paint(div().id(id), border)
                        .debug_selector(|| "master-detail-divider".to_string())
                        .into_any_element(),
                    None => paint(div(), border).into_any_element(),
                };
                region.child(line)
            })
            .child(self.list);

        let detail = div()
            .flex_1()
            .min_w_0()
            .h_full()
            .flex()
            .flex_col()
            .child(self.detail);

        let mut frame = div().id(self.id.clone());
        if let ElementId::Name(name) = &self.id {
            let selector = name.to_string();
            frame = frame.debug_selector(move || selector);
        }

        frame.flex().min_h_0().child(list).child(detail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    struct FrameHarness {
        list_width: Pixels,
        divider: bool,
    }

    impl Render for FrameHarness {
        fn render(
            &mut self,
            _window: &mut Window,
            _cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            let list = div()
                .debug_selector(|| "frame-list".to_string())
                .size_full()
                .bg(gpui::transparent_black());
            let detail = div()
                .debug_selector(|| "frame-detail".to_string())
                .size_full()
                .bg(gpui::transparent_black());

            div().size_full().child(
                MasterDetail::new(self.list_width, list, detail)
                    .id("frame")
                    .divider(self.divider),
            )
        }
    }

    fn open_frame(
        cx: &mut gpui::TestAppContext,
        list_width: Pixels,
        divider: bool,
    ) -> &mut gpui::VisualTestContext {
        install_test_theme(cx);

        let (_, window) = cx.add_window_view(move |window, cx| {
            let harness = cx.new(|_| FrameHarness {
                list_width,
                divider,
            });
            gpui_component::Root::new(harness, window, cx)
        });
        window.run_until_parked();
        window
    }

    #[gpui::test]
    fn list_region_keeps_its_fixed_width_and_detail_takes_the_rest(cx: &mut gpui::TestAppContext) {
        let window = open_frame(cx, DEFAULT_LIST_WIDTH, true);

        let frame = window.debug_bounds("frame").expect("the frame is laid out");
        let list = window
            .debug_bounds("frame-list")
            .expect("the list region is laid out");
        let detail = window
            .debug_bounds("frame-detail")
            .expect("the detail region is laid out");

        assert_eq!(
            list.size.width, DEFAULT_LIST_WIDTH,
            "the list region is exactly its configured width"
        );
        assert_eq!(list.size.height, frame.size.height);
        assert_eq!(
            detail.size.width,
            frame.size.width - list.size.width,
            "the detail region takes the rest of the row"
        );
        assert_eq!(detail.left(), list.right());
    }

    #[gpui::test]
    fn divider_runs_along_the_list_edge_and_can_be_turned_off(cx: &mut gpui::TestAppContext) {
        let window = open_frame(cx, DEFAULT_LIST_WIDTH, true);

        let divider = window
            .debug_bounds("master-detail-divider")
            .expect("the divider is laid out");
        let list = window
            .debug_bounds("frame-list")
            .expect("the list is laid out");

        assert_eq!(
            divider.size.width,
            Borders::THIN,
            "the divider is one hairline wide"
        );
        assert_eq!(
            divider.right(),
            list.right(),
            "it hugs the list's right edge"
        );

        let window = open_frame(cx, DEFAULT_LIST_WIDTH, false);
        assert!(
            window.debug_bounds("master-detail-divider").is_none(),
            "divider(false) draws no divider"
        );
    }

    #[test]
    fn default_list_width_is_the_sidebar_width_token() {
        assert_eq!(DEFAULT_LIST_WIDTH, ShellMetrics::SIDEBAR_WIDTH);
    }
}
