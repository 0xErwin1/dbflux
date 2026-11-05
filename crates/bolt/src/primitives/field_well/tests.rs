use gpui::{Context, IntoElement, Render, TestAppContext, VisualTestContext, Window, div, px};

use super::{FieldWell, inspect_field_well};
use crate::accessibility::FrameCapture;
use crate::primitives::CodeView;
use crate::tokens::{ChamferCut, ChromeColorSlot};

fn install_test_theme(cx: &mut TestAppContext) {
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

#[test]
fn a_field_well_fills_with_the_ground_behind_a_line_and_the_field_cut() {
    let inspection = inspect_field_well();

    assert_eq!(inspection.fill, ChromeColorSlot::Background);
    assert_eq!(inspection.border, ChromeColorSlot::Border);
    assert_eq!(inspection.cut, ChamferCut::CONTROL);
}

/// A well in a 480 px column draws its id and holds its content inside its
/// own bounds.
struct Harness;

impl Render for Harness {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        use gpui::{ParentElement as _, Styled as _};

        div().size_full().p(px(20.0)).child(
            div().flex().flex_col().w(px(480.0)).child(
                FieldWell::new()
                    .id("document-text")
                    .child(CodeView::new("document", "one\ntwo\n").line_numbers(true)),
            ),
        )
    }
}

fn well(cx: &mut TestAppContext) -> (&mut VisualTestContext, gpui::AccessibilityFrame) {
    install_test_theme(cx);
    let (_, window) = cx.add_window_view(move |_, _| Harness);
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    (window, frame)
}

/// The well carries its id and its content sits inside it, so a host frames a
/// block by placing it in a well.
#[gpui::test]
fn a_field_well_holds_its_content_inside_its_id(cx: &mut TestAppContext) {
    let (window, frame) = well(cx);

    assert!(
        frame.nodes().any(|(_, node)| node.id() == "document-text"),
        "the well carries the id the host gave it"
    );

    let well = window
        .debug_bounds("document-text")
        .expect("the well is laid out");
    let line = window
        .debug_bounds("document-line-1")
        .expect("the content is laid out");
    let last = window
        .debug_bounds("document-line-2")
        .expect("the content is laid out");

    assert!(line.left() >= well.left(), "content starts inside the well");
    assert!(
        last.bottom() <= well.bottom(),
        "content ends inside the well"
    );
    assert_eq!(well.size.width, px(480.0), "the well fills its column");
}
