use super::{CodeLine, ModalCode, split_lines};
use crate::accessibility::{FrameCapture, accessible_name};
use gpui::{AccessibilityFrame, App, IntoElement as _, TestAppContext, VisualTestContext};

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

/// `build` drawn in a themed 480 px window, with its first frame.
fn code_window(
    cx: &mut TestAppContext,
    build: fn(&mut App) -> gpui::AnyElement,
) -> (AccessibilityFrame, &mut VisualTestContext) {
    struct Surface(fn(&mut App) -> gpui::AnyElement);

    impl gpui::Render for Surface {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            use gpui::{ParentElement as _, Styled as _};

            gpui::div()
                .size_full()
                .child(gpui::div().w(gpui::px(480.0)).child((self.0)(cx)))
        }
    }

    install_test_theme(cx);
    let (_, window) = cx.add_window_view(move |_, _| Surface(build));
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    (frame, window)
}

const CODE: &str = "curl -X POST\n  --data '{}'\n";

fn numbered(_cx: &mut App) -> gpui::AnyElement {
    ModalCode::new("export-code", CODE)
        .highlights(std::iter::once(0..4))
        .line_numbers(true)
        .aria_label("Generated code")
        .into_any_element()
}

fn plain(_cx: &mut App) -> gpui::AnyElement {
    ModalCode::new("export-code", CODE).into_any_element()
}

#[test]
fn lines_split_at_each_break_without_the_break_or_a_trailing_empty_line() {
    let lines = split_lines("a\r\nbc\n", &[]);

    assert_eq!(
        lines,
        vec![
            CodeLine {
                text: 0..1,
                highlights: vec![],
            },
            CodeLine {
                text: 3..5,
                highlights: vec![],
            },
        ]
    );
}

#[test]
fn empty_code_is_one_empty_line() {
    assert_eq!(
        split_lines("", &[]),
        vec![CodeLine {
            text: 0..0,
            highlights: vec![],
        }]
    );
}

#[test]
fn a_highlight_is_clipped_to_each_line_it_covers_relative_to_the_line() {
    // "abc\ndef\nghi": the range covers "bc", the break, "def", the break
    // and "g".
    let lines = split_lines("abc\ndef\nghi", std::slice::from_ref(&(1..9)));

    assert_eq!(lines[0].highlights, vec![1..3]);
    assert_eq!(lines[1].highlights, vec![0..3]);
    assert_eq!(lines[2].highlights, vec![0..1]);
}

#[test]
fn a_highlight_on_the_line_break_alone_draws_nowhere() {
    let lines = split_lines("ab\ncd", std::slice::from_ref(&(2..3)));

    assert!(lines.iter().all(|line| line.highlights.is_empty()));
}

/// Numbered, every line carries `<id>-line-<n>` named by its text, the
/// block carries the caller's name, and the number sits before the code.
#[gpui::test]
fn numbered_lines_carry_their_ids_and_text(cx: &mut TestAppContext) {
    let (frame, window) = code_window(cx, numbered);

    assert_eq!(
        accessible_name(&frame, "export-code").as_deref(),
        Some("Generated code")
    );
    assert_eq!(
        accessible_name(&frame, "export-code-line-1").as_deref(),
        Some("curl -X POST")
    );
    assert_eq!(
        accessible_name(&frame, "export-code-line-2").as_deref(),
        Some("--data '{}'"),
        "the name is trimmed text; the drawn line keeps its indent"
    );
    assert!(
        frame
            .nodes()
            .all(|(_, node)| node.id() != "export-code-line-3"),
        "a trailing line break opens no empty line"
    );

    let first = window
        .debug_bounds("export-code-line-1")
        .expect("line 1 is laid out");
    let second = window
        .debug_bounds("export-code-line-2")
        .expect("line 2 is laid out");
    let line_height =
        crate::tokens::ModalMetrics::CODE_LINE_HEIGHT.to_pixels(gpui::px(crate::tokens::BASE_REM));
    assert_eq!(first.size.height, line_height, "a line is 20 px tall");
    assert_eq!(second.top(), first.bottom(), "lines stack without a gap");
}

/// Without line numbers the block draws its code as one text and carries
/// no line nodes; with no caller name it announces the code.
#[gpui::test]
fn an_unnumbered_block_draws_no_line_rows(cx: &mut TestAppContext) {
    let (frame, _window) = code_window(cx, plain);

    assert_eq!(
        accessible_name(&frame, "export-code").as_deref(),
        Some(CODE.trim())
    );
    assert!(
        frame
            .nodes()
            .all(|(_, node)| !node.id().starts_with("export-code-line-")),
        "no line rows without the gutter"
    );
}
