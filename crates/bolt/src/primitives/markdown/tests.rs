use gpui::prelude::*;
use gpui::{
    AnyElement, App, IntoElement, Modifiers, MouseButton, TestAppContext, VisualTestContext, point,
};
use gpui_base::TextSelection;
use gpui_component::ActiveTheme;

use crate::accessibility::{FrameCapture, accessibility_node, accessible_name};
use crate::fonts;
use crate::primitives::MarkdownView;
use crate::tokens::{FontSizes, MarkdownMetrics, Spacing};
use gpui::AccessibilityFrame;

const VIEW_WIDTH: f32 = 420.0;

/// A document with one block of every kind the view renders: a heading, a
/// paragraph, a sub-heading, a list, inline code, a link, a fenced code block
/// and a table.
const DOCUMENT: &str = r#"# Release notes

alpha beta gamma

## Second heading

- one
- two

Inline `code` and a [link](https://example.com).

```
fn main() {}
```

| key | value |
|---|---|
| a | 1 |
"#;

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

fn view_window(
    cx: &mut TestAppContext,
    build: fn(&mut App) -> AnyElement,
) -> (AccessibilityFrame, &mut VisualTestContext) {
    struct Surface(fn(&mut App) -> AnyElement);

    impl gpui::Render for Surface {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            gpui::div().size_full().child(
                gpui::div()
                    .flex()
                    .w(gpui::px(VIEW_WIDTH))
                    .child((self.0)(cx)),
            )
        }
    }

    install_test_theme(cx);
    let (_, window) = cx.add_window_view(move |window, cx| {
        let surface = cx.new(|_| Surface(build));

        gpui_component::Root::new(surface, window, cx)
    });
    window.run_until_parked();
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    (frame, window)
}

fn document(_cx: &mut App) -> AnyElement {
    MarkdownView::new("docs", DOCUMENT).into_any_element()
}

fn unselectable(_cx: &mut App) -> AnyElement {
    MarkdownView::new("docs", DOCUMENT)
        .selectable(false)
        .into_any_element()
}

fn welled(_cx: &mut App) -> AnyElement {
    MarkdownView::new("docs", DOCUMENT)
        .well(true)
        .aria_label("Release notes")
        .into_any_element()
}

/// Drags across the whole document, from its first line to its last.
fn select_everything(window: &mut VisualTestContext) {
    let body = window
        .debug_bounds("docs-body")
        .expect("the document is laid out");
    let start = point(body.left() + gpui::px(1.0), body.top() + gpui::px(1.0));
    let end = point(body.right() - gpui::px(1.0), body.bottom() - gpui::px(1.0));

    window.simulate_mouse_down(start, MouseButton::Left, Modifiers::default());
    window.simulate_mouse_move(end, Some(MouseButton::Left), Modifiers::default());
    window.simulate_mouse_up(end, MouseButton::Left, Modifiers::default());
    window.run_until_parked();
}

fn selected_text(window: &mut VisualTestContext) -> String {
    window.update(TextSelection::selected_text)
}

/// Every block kind renders as its rendered text, and the document can be
/// selected: dragging across it copies the text the view draws, not the
/// markdown source.
#[gpui::test]
fn a_document_renders_and_selects_as_rendered_text(cx: &mut TestAppContext) {
    let (_frame, window) = view_window(cx, document);

    assert!(
        window
            .debug_bounds("docs-body")
            .is_some_and(|bounds| bounds.size.height > gpui::px(0.0)),
        "the document is laid out inside the view"
    );

    select_everything(window);
    let selected = selected_text(window);

    for drawn in [
        "Release notes",
        "alpha beta gamma",
        "Second heading",
        "one",
        "two",
        "Inline",
        "code",
        "link",
        "fn main() {}",
        "key",
        "value",
    ] {
        assert!(
            selected.contains(drawn),
            "the selection is missing {drawn:?}: {selected:?}"
        );
    }

    for markup in ["#", "`", "[", "](", "|", "---"] {
        assert!(
            !selected.contains(markup),
            "the selection carries the markdown {markup:?} it should have parsed away: {selected:?}"
        );
    }
}

/// A view asked not to be selectable draws the same document and selects
/// nothing.
#[gpui::test]
fn an_unselectable_view_selects_nothing(cx: &mut TestAppContext) {
    let (_frame, window) = view_window(cx, unselectable);

    select_everything(window);

    assert_eq!(selected_text(window), "");
}

/// The sizes a document draws with come from the tokens: the body on
/// `FontSizes::BASE`, code one step down in mono, and headings from the title
/// down, one step per level.
#[gpui::test]
fn the_typography_comes_from_the_tokens(cx: &mut TestAppContext) {
    cx.update(|cx| {
        let typography = MarkdownView::typography(cx);

        assert_eq!(typography.body, fonts::ui_px(cx, FontSizes::BASE));
        assert_eq!(
            typography.code,
            fonts::ui_px(cx, MarkdownMetrics::CODE_FONT)
        );
        assert_eq!(typography.paragraph_gap, MarkdownMetrics::PARAGRAPH_GAP);
        assert_eq!(
            typography.headings,
            [
                fonts::ui_px(cx, FontSizes::TITLE),
                fonts::ui_px(cx, FontSizes::XL),
                fonts::ui_px(cx, FontSizes::LG),
                fonts::ui_px(cx, FontSizes::BASE),
                fonts::ui_px(cx, FontSizes::XS),
                fonts::ui_px(cx, FontSizes::LABEL),
            ]
        );
        assert!(
            typography.headings.windows(2).all(|pair| pair[0] > pair[1]),
            "each heading level is smaller than the one above it: {:?}",
            typography.headings
        );
        assert!(typography.code < typography.body);
    });
}

/// The well is what puts the document on the inset ground: with it the
/// document is inset by the well's padding, without it the document starts at
/// the view's own edge.
#[gpui::test]
fn the_well_insets_the_document_only_when_asked(cx: &mut TestAppContext) {
    let (_frame, window) = view_window(cx, welled);
    let view = window.debug_bounds("docs").expect("the view is laid out");
    let body = window
        .debug_bounds("docs-body")
        .expect("the document is laid out");

    assert_eq!(view.size.width, gpui::px(VIEW_WIDTH));
    assert_eq!(body.left(), view.left() + Spacing::MD);
    assert_eq!(body.right(), view.right() - Spacing::MD);
    assert_eq!(body.top(), view.top() + Spacing::SM);

    let (_frame, window) = view_window(cx, document);
    let view = window.debug_bounds("docs").expect("the view is laid out");
    let body = window
        .debug_bounds("docs-body")
        .expect("the document is laid out");

    assert_eq!(body.left(), view.left());
    assert_eq!(body.right(), view.right());
}

/// A table's header row sits on the raised fill, the same ground as the code
/// blocks and the inline code around it. The renderer paints the table itself,
/// so the frame cannot show the fill: the test pins the style the renderer is
/// given, which the component folds onto the theme's own table style.
#[gpui::test]
fn a_table_header_sits_on_the_raised_fill(cx: &mut TestAppContext) {
    install_test_theme(cx);
    cx.update(|cx| {
        let role = crate::colors(cx);
        let style = super::text_style(
            &MarkdownView::typography(cx),
            cx.theme(),
            role,
            fonts::editor_family(cx),
        );

        assert_eq!(style.table_head.background, Some(role.raised.into()));
    });
}

/// A named view is a group that announces its name; an unnamed one carries no
/// name of its own.
#[gpui::test]
fn the_view_announces_the_name_it_is_given(cx: &mut TestAppContext) {
    let (frame, _window) = view_window(cx, welled);
    assert_eq!(
        accessible_name(&frame, "docs").as_deref(),
        Some("Release notes")
    );
    assert_eq!(
        accessibility_node(&frame, "docs").map(|node| node.role()),
        Some(gpui::Role::Group)
    );

    let (frame, _window) = view_window(cx, document);
    assert_eq!(accessible_name(&frame, "docs"), None);
}
