use gpui::{AccessibilityFrame, IntoElement as _, TestAppContext, VisualTestContext, px};

use super::CodeView;
use crate::accessibility::{FrameCapture, accessible_name};
use crate::tokens::{BASE_REM, ModalMetrics};

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

const CODE: &str = "curl -X POST\n  --data '{}'\n";

/// A code view in a 480 px well, with the first frame.
fn code_window(
    cx: &mut TestAppContext,
    build: fn(&mut gpui::App) -> gpui::AnyElement,
) -> (AccessibilityFrame, &mut VisualTestContext) {
    struct Surface(fn(&mut gpui::App) -> gpui::AnyElement);

    impl gpui::Render for Surface {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            use gpui::{ParentElement as _, Styled as _};

            gpui::div()
                .size_full()
                .child(gpui::div().w(px(480.0)).child((self.0)(cx)))
        }
    }

    install_test_theme(cx);
    let (_, window) = cx.add_window_view(move |_, _| Surface(build));
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    (frame, window)
}

fn numbered(_cx: &mut gpui::App) -> gpui::AnyElement {
    CodeView::new("body", CODE)
        .highlights(std::iter::once(0..4))
        .line_numbers(true)
        .aria_label("Response body")
        .into_any_element()
}

fn plain(_cx: &mut gpui::App) -> gpui::AnyElement {
    CodeView::new("body", CODE).into_any_element()
}

/// Numbered, every line carries `<id>-line-<n>` named by its text, the view
/// carries the caller's name, and the rows stack at the line height.
#[gpui::test]
fn numbered_lines_carry_their_ids_and_text(cx: &mut TestAppContext) {
    let (frame, window) = code_window(cx, numbered);

    assert_eq!(
        accessible_name(&frame, "body").as_deref(),
        Some("Response body")
    );
    assert_eq!(
        accessible_name(&frame, "body-line-1").as_deref(),
        Some("curl -X POST")
    );
    assert_eq!(
        accessible_name(&frame, "body-line-2").as_deref(),
        Some("--data '{}'"),
        "the name is trimmed text; the drawn line keeps its indent"
    );

    let first = window.debug_bounds("body-line-1").expect("line 1 is drawn");
    let second = window.debug_bounds("body-line-2").expect("line 2 is drawn");
    let line_height = ModalMetrics::CODE_LINE_HEIGHT.to_pixels(px(BASE_REM));

    assert_eq!(first.size.height, line_height, "a line is 20 px tall");
    assert_eq!(second.top(), first.bottom(), "lines stack without a gap");
}

/// Without line numbers the view draws one label node with the whole code and
/// no line rows.
#[gpui::test]
fn an_unnumbered_view_draws_no_line_rows(cx: &mut TestAppContext) {
    let (frame, _window) = code_window(cx, plain);

    assert_eq!(
        accessible_name(&frame, "body").as_deref(),
        Some(CODE.trim())
    );
    assert!(
        frame
            .nodes()
            .all(|(_, node)| !node.id().starts_with("body-line-")),
        "no line rows without the gutter"
    );
}

fn long_numbered(_cx: &mut gpui::App) -> gpui::AnyElement {
    let long = format!("SELECT {} FROM t\nshort\n", "column_name, ".repeat(40));
    CodeView::new("long", long)
        .line_numbers(true)
        .into_any_element()
}

/// A line wider than the view scrolls the code column sideways while the
/// gutter stays where it is, and a vertical wheel does not scroll it.
#[gpui::test]
fn a_long_line_scrolls_the_code_column_beside_a_fixed_gutter(cx: &mut TestAppContext) {
    use gpui::{Modifiers, ScrollDelta, ScrollWheelEvent, point};

    let (_frame, window) = code_window(cx, long_numbered);

    let column = window
        .debug_bounds("long-lines")
        .expect("the column is drawn");
    let line = window.debug_bounds("long-line-1").expect("line 1 is drawn");
    assert!(
        line.size.width > column.size.width,
        "the long line keeps its full width inside the column"
    );

    window.simulate_event(ScrollWheelEvent {
        position: column.center(),
        delta: ScrollDelta::Pixels(point(px(0.0), px(-200.0))),
        modifiers: Modifiers::default(),
        ..Default::default()
    });
    window.run_until_parked();

    let after_vertical = window.debug_bounds("long-line-1").expect("line 1 is drawn");
    assert_eq!(
        after_vertical.left(),
        line.left(),
        "a vertical wheel is left to the host"
    );

    window.simulate_event(ScrollWheelEvent {
        position: column.center(),
        delta: ScrollDelta::Pixels(point(px(-200.0), px(0.0))),
        modifiers: Modifiers::default(),
        ..Default::default()
    });
    window.run_until_parked();

    let scrolled = window.debug_bounds("long-line-1").expect("line 1 is drawn");
    let gutter_after = window
        .debug_bounds("long-lines")
        .expect("the column is drawn");
    assert!(scrolled.left() < line.left(), "the line moved left");
    assert_eq!(
        gutter_after.left(),
        column.left(),
        "the gutter did not move"
    );
}

/// The highlight a code view draws the caller's ranges in stays bold tint, so
/// a block inside a dialog and a view inside a page mark a range the same way.
#[gpui::test]
fn the_caller_ranges_draw_bold_in_the_tint(cx: &mut TestAppContext) {
    install_test_theme(cx);

    cx.update(|cx| {
        let theme = gpui_component::ActiveTheme::theme(cx);
        let expected = gpui::HighlightStyle {
            color: Some(crate::tokens::ChromeColors::tint(theme)),
            font_weight: Some(gpui::FontWeight::BOLD),
            ..Default::default()
        };

        assert_eq!(
            super::highlight_styles(std::iter::once(2..5), cx),
            vec![(2..5, expected)]
        );
    });
}

/// The caller's ranges reach the text sorted and non-overlapping: a range
/// pair given out of order, and one that overlaps its neighbour, still color
/// the bytes they name.
#[gpui::test]
fn unordered_and_overlapping_highlights_are_normalized(cx: &mut TestAppContext) {
    install_test_theme(cx);

    cx.update(|cx| {
        let tint = gpui::HighlightStyle {
            color: Some(crate::tokens::ChromeColors::tint(
                gpui_component::ActiveTheme::theme(cx),
            )),
            font_weight: Some(gpui::FontWeight::BOLD),
            ..Default::default()
        };

        assert_eq!(
            CodeView::new("body", "abcdefgh")
                .highlights(vec![4..6, 0..2])
                .styled_runs(cx),
            vec![(0..2, tint), (4..6, tint)],
            "an out-of-order pair is drawn in byte order"
        );
        assert_eq!(
            CodeView::new("body", "abcdefgh")
                .highlights(vec![0..4, 2..6])
                .styled_runs(cx),
            vec![(0..6, tint)],
            "an overlap of one style is drawn once"
        );
    });
}

/// A numbered view draws its rows at its own code font, so a host with a
/// larger text size cannot make the lines outgrow their 20 px rows.
#[gpui::test]
fn a_numbered_view_draws_its_rows_at_the_code_font(cx: &mut TestAppContext) {
    let mut lines = Vec::new();

    for text_size in [crate::tokens::ui(12.5), crate::tokens::ui(30.0)] {
        install_test_theme(cx);
        let (_, window) = cx.add_window_view(move |_, _| ProbeFont { text_size });
        let capture = FrameCapture::observe(window);
        let _frame = capture.frame(window);
        lines.push(
            window
                .debug_bounds("probe-code-line-1")
                .expect("line 1 is drawn"),
        );
    }

    assert_eq!(
        lines[0].size.width, lines[1].size.width,
        "the host's text size does not stretch the code"
    );
    assert_eq!(
        lines[1].size.height,
        ModalMetrics::CODE_LINE_HEIGHT.to_pixels(px(BASE_REM)),
        "a numbered line is one line tall at any host size"
    );
}

struct ProbeFont {
    text_size: gpui::Rems,
}

impl gpui::Render for ProbeFont {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        _cx: &mut gpui::Context<Self>,
    ) -> impl gpui::IntoElement {
        use gpui::{ParentElement as _, Styled as _};

        gpui::div().size_full().child(
            gpui::div()
                .w(px(480.0))
                .text_size(self.text_size)
                .child(CodeView::new("probe-code", "SELECT id\nFROM users\n").line_numbers(true)),
        )
    }
}
