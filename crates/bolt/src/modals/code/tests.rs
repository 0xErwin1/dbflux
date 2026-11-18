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

/// The per-line scan `split_lines` replaced, kept as the reference its
/// single pass must match.
fn split_lines_by_scan(code: &str, highlights: &[std::ops::Range<usize>]) -> Vec<CodeLine> {
    let mut lines = Vec::new();
    let mut start = 0;

    for segment in code.split_inclusive('\n') {
        let content = segment
            .strip_suffix('\n')
            .map(|line| line.strip_suffix('\r').unwrap_or(line))
            .unwrap_or(segment);
        let text = start..start + content.len();
        let line_highlights = highlights
            .iter()
            .filter_map(|range| {
                let clipped_start = range.start.max(text.start);
                let clipped_end = range.end.min(text.end);
                (clipped_start < clipped_end)
                    .then(|| clipped_start - text.start..clipped_end - text.start)
            })
            .collect();
        lines.push(CodeLine {
            text,
            highlights: line_highlights,
        });
        start += segment.len();
    }

    if lines.is_empty() {
        lines.push(CodeLine {
            text: 0..0,
            highlights: Vec::new(),
        });
    }

    lines
}

#[test]
fn the_single_pass_matches_the_per_line_scan_on_mixed_ranges() {
    let code = "SELECT id\r\nFROM users\n\nWHERE id = 1\n  AND name = 'x'";
    let cases: Vec<Vec<std::ops::Range<usize>>> = vec![
        vec![],
        vec![0..6, 11..15, 24..29],
        // Unsorted, overlapping, crossing breaks, empty and past the end.
        vec![
            24..29,
            2..14,
            0..6,
            7..7,
            9..11,
            20..50,
            13..26,
            40..44,
            60..70,
        ],
        vec![0..code.len()],
        vec![10..11, 22..23, 23..24],
    ];

    for highlights in cases {
        assert_eq!(
            split_lines(code, &highlights),
            split_lines_by_scan(code, &highlights),
            "highlights {highlights:?}"
        );
    }
}

fn long_numbered(_cx: &mut App) -> gpui::AnyElement {
    let long = format!("SELECT {} FROM t\nshort\n", "column_name, ".repeat(40));
    ModalCode::new("long-code", long)
        .line_numbers(true)
        .into_any_element()
}

/// A line wider than the block scrolls the code column sideways; the
/// gutter stays where it is.
#[gpui::test]
fn a_long_line_scrolls_the_code_column(cx: &mut TestAppContext) {
    use gpui::{Modifiers, ScrollDelta, ScrollWheelEvent, point, px};

    let (_frame, window) = code_window(cx, long_numbered);

    let column = window
        .debug_bounds("long-code-lines")
        .expect("the code column is laid out");
    let line = window
        .debug_bounds("long-code-line-1")
        .expect("line 1 is laid out");
    assert!(
        line.size.width > column.size.width,
        "the long line keeps its full width inside the column"
    );

    window.simulate_event(ScrollWheelEvent {
        position: column.center(),
        delta: ScrollDelta::Pixels(point(px(-200.0), px(0.0))),
        modifiers: Modifiers::default(),
        ..Default::default()
    });
    window.run_until_parked();

    let scrolled = window
        .debug_bounds("long-code-line-1")
        .expect("line 1 is laid out");
    let column_after = window
        .debug_bounds("long-code-lines")
        .expect("the code column is laid out");
    assert!(scrolled.left() < line.left(), "the line moved left");
    assert_eq!(
        column_after.left(),
        column.left(),
        "the gutter did not move"
    );
}

/// A vertical wheel over the code column leaves it in place, so it reaches
/// the scrolling dialog body instead of turning into a sideways scroll.
#[gpui::test]
fn a_vertical_wheel_does_not_scroll_the_code_sideways(cx: &mut TestAppContext) {
    use gpui::{ScrollDelta, ScrollWheelEvent, point, px};

    let (_frame, window) = code_window(cx, long_numbered);
    let column = window
        .debug_bounds("long-code-lines")
        .expect("the code column is laid out");
    let line = window
        .debug_bounds("long-code-line-1")
        .expect("line 1 is laid out");

    window.simulate_event(ScrollWheelEvent {
        position: column.center(),
        delta: ScrollDelta::Pixels(point(px(0.0), px(-200.0))),
        ..Default::default()
    });
    window.run_until_parked();

    let after = window
        .debug_bounds("long-code-line-1")
        .expect("line 1 is laid out");
    assert_eq!(after.left(), line.left());
}

fn tint_bold(cx: &App) -> gpui::HighlightStyle {
    gpui::HighlightStyle {
        color: Some(crate::tokens::ChromeColors::tint(
            gpui_component::ActiveTheme::theme(cx),
        )),
        font_weight: Some(gpui::FontWeight::BOLD),
        ..Default::default()
    }
}

fn color(color: gpui::Hsla) -> gpui::HighlightStyle {
    gpui::HighlightStyle {
        color: Some(color),
        ..Default::default()
    }
}

/// With a language the code takes its syntax runs, and a highlight replaces
/// the syntax style over its own range.
#[gpui::test]
fn a_language_colors_the_code_under_the_highlights(cx: &mut TestAppContext) {
    install_test_theme(cx);
    crate::syntax::register_bundled();
    let colors = crate::tokens::SyntaxColors::dark();

    cx.update(|cx| {
        let runs = ModalCode::new("json-code", r#"{"key": 1}"#)
            .language("json")
            .highlights(std::iter::once(1..4))
            .styled_runs(cx);

        assert_eq!(
            runs,
            vec![
                (0..1, color(colors.operator)),
                (1..4, tint_bold(cx)),
                (4..6, color(colors.property)),
                (6..7, color(colors.operator)),
                (8..9, color(colors.number)),
                (9..10, color(colors.operator)),
            ]
        );
    });
}

#[gpui::test]
fn an_unknown_language_leaves_only_the_highlights(cx: &mut TestAppContext) {
    install_test_theme(cx);

    cx.update(|cx| {
        let runs = ModalCode::new("plain-code", r#"{"key": 1}"#)
            .language("probe-unknown-language")
            .highlights(std::iter::once(1..4))
            .styled_runs(cx);

        assert_eq!(runs, vec![(1..4, tint_bold(cx))]);
    });
}

/// Numbered, each line carries its own runs relative to the line start.
#[test]
fn styled_runs_split_per_line_relative_to_the_line() {
    let code = "{\n  \"k\": 1\n}";
    let runs = vec![(0..1, 'a'), (4..7, 'b'), (9..10, 'c'), (11..12, 'a')];

    let lines = super::split_runs(code, &runs);

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0].1, vec![(0..1, 'a')]);
    assert_eq!(lines[1].1, vec![(2..5, 'b'), (7..8, 'c')]);
    assert_eq!(lines[2].1, vec![(0..1, 'a')]);
}

/// A toned highlight draws bold in its tone's color, beside the tint ones,
/// and over the syntax runs.
#[gpui::test]
fn toned_highlights_draw_in_their_tone_over_the_syntax(cx: &mut TestAppContext) {
    use crate::primitives::BadgeTone;

    install_test_theme(cx);
    crate::syntax::register_bundled();
    let colors = crate::tokens::SyntaxColors::dark();

    cx.update(|cx| {
        let theme = gpui_component::ActiveTheme::theme(cx);
        let bold = |color: gpui::Hsla| gpui::HighlightStyle {
            color: Some(color),
            font_weight: Some(gpui::FontWeight::BOLD),
            ..Default::default()
        };
        let warning = bold(BadgeTone::Warning.text_color(theme));

        let runs = ModalCode::new("toned-code", r#"{"key": 1}"#)
            .language("json")
            .highlights(std::iter::once(2..5))
            .highlights_with_tone(std::iter::once(8..9), BadgeTone::Warning)
            .styled_runs(cx);

        assert_eq!(
            runs,
            vec![
                (0..1, color(colors.operator)),
                (1..2, color(colors.property)),
                (2..5, tint_bold(cx)),
                (5..6, color(colors.property)),
                (6..7, color(colors.operator)),
                (8..9, warning),
                (9..10, color(colors.operator)),
            ]
        );
    });
}

/// Without a language the toned and tint highlights are the only runs.
#[gpui::test]
fn toned_highlights_without_a_language(cx: &mut TestAppContext) {
    use crate::primitives::BadgeTone;

    install_test_theme(cx);

    cx.update(|cx| {
        let warning = gpui::HighlightStyle {
            color: Some(BadgeTone::Warning.text_color(gpui_component::ActiveTheme::theme(cx))),
            font_weight: Some(gpui::FontWeight::BOLD),
            ..Default::default()
        };

        let runs = ModalCode::new("toned-plain", "{{a}} {{b}}")
            .highlights(std::iter::once(0..5))
            .highlights_with_tone(std::iter::once(6..11), BadgeTone::Warning)
            .styled_runs(cx);

        assert_eq!(runs, vec![(0..5, tint_bold(cx)), (6..11, warning)]);
    });
}
