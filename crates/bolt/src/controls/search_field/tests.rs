use gpui::{
    AccessibilityFrame, AppContext as _, Context, Entity, IntoElement, Render, TestAppContext,
    VisualTestContext, Window, div,
};

use super::{SearchCount, SearchField};
use crate::accessibility::{FrameCapture, accessible_name};
use crate::controls::InputState;
use crate::primitives::{BadgeTone, Kbd};
use crate::tokens::Fields;

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

/// A search field in a 300 px window, with the count the test asks for.
struct Harness {
    input: Entity<InputState>,
    count: Option<&'static str>,
}

impl Render for Harness {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        use gpui::{ParentElement as _, Styled as _};

        let field = SearchField::new("body-search", &self.input)
            .aria_label("Search in body")
            .count(
                self.count
                    .map(|label| SearchCount::new(label).tone(BadgeTone::Accent)),
            )
            .trailing(Kbd::new("F3").id("body-search-next"));

        div()
            .size_full()
            .child(div().flex().w(gpui::px(300.0)).child(field))
    }
}

fn open<'a>(
    cx: &'a mut TestAppContext,
    count: Option<&'static str>,
) -> (AccessibilityFrame, &'a mut VisualTestContext) {
    install_test_theme(cx);
    let (_, window) = cx.add_window_view(move |window, cx| {
        let input = cx.new(|cx| InputState::new(window, cx));
        Harness { input, count }
    });
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    (frame, window)
}

/// The count is a label under `<id>-count` named by the caller's text, and
/// it sits after the input and before the trailing controls.
#[gpui::test]
fn the_count_carries_its_id_and_text_after_the_input(cx: &mut TestAppContext) {
    let (frame, window) = open(cx, Some("3 of 12"));

    assert_eq!(
        accessible_name(&frame, "body-search-count").as_deref(),
        Some("3 of 12")
    );

    let field = window
        .debug_bounds("body-search")
        .expect("the field is laid out");
    let count = window
        .debug_bounds("body-search-count")
        .expect("the count is laid out");
    let next = window
        .debug_bounds("body-search-next")
        .expect("the trailing control is laid out");

    assert_eq!(
        field.size.height,
        Fields::HEIGHT.to_pixels(gpui::px(crate::tokens::BASE_REM))
    );
    assert!(
        count.left() > field.center().x,
        "the count sits at the field's end"
    );
    assert!(
        next.left() >= count.right(),
        "trailing controls follow the count"
    );
    assert!(next.right() <= field.right(), "everything stays inside");
}

/// The input carries `<id>-input` and the caller's name.
#[gpui::test]
fn the_input_carries_its_id_and_name(cx: &mut TestAppContext) {
    let (frame, _window) = open(cx, None);

    assert_eq!(
        accessible_name(&frame, "body-search-input").as_deref(),
        Some("Search in body")
    );
}

/// Without a count the field draws none.
#[gpui::test]
fn a_field_without_a_count_draws_none(cx: &mut TestAppContext) {
    let (frame, window) = open(cx, None);

    assert!(
        frame
            .nodes()
            .all(|(_, node)| node.id() != "body-search-count")
    );
    assert!(window.debug_bounds("body-search-count").is_none());
}

/// A failed field rings in danger whether or not it holds focus; holding
/// focus it keeps the tint focus ring and draws its border in danger, so
/// keyboard focus stays visible on a failed search.
#[gpui::test]
fn a_failed_field_still_shows_focus(cx: &mut TestAppContext) {
    use super::field_frame;
    use crate::primitives::ChamferRing;
    use gpui_component::ActiveTheme as _;

    install_test_theme(cx);
    cx.update(|cx| {
        let theme = cx.theme().clone();
        let tint = crate::tokens::ChromeColors::tint(&theme);

        assert_eq!(
            field_frame(true, false, &theme),
            (theme.border, Some(ChamferRing::outline(theme.danger))),
            "an unfocused failed field always shows the danger ring"
        );
        assert_eq!(
            field_frame(true, true, &theme),
            (theme.danger, Some(ChamferRing::focus(tint))),
            "a focused failed field shows the focus ring inside a danger border"
        );
        assert_eq!(
            field_frame(false, true, &theme),
            (theme.border, Some(ChamferRing::focus(tint)))
        );
        assert_eq!(field_frame(false, false, &theme), (theme.border, None));
    });
}

/// A very long count stops at its maximum width and the input keeps its
/// minimum width; nothing paints past the field.
#[gpui::test]
fn a_long_count_is_bounded_and_the_input_keeps_its_width(cx: &mut TestAppContext) {
    let (_frame, window) = open(
        cx,
        Some("1234567 of 98765432 matches across every loaded response body"),
    );
    let rem = gpui::px(crate::tokens::BASE_REM);

    let field = window.debug_bounds("body-search").expect("field laid out");
    let count = window
        .debug_bounds("body-search-count")
        .expect("count laid out");
    let next = window
        .debug_bounds("body-search-next")
        .expect("trailing control laid out");

    // The input sits between the leading icon and the count.
    let input_start =
        field.left() + Fields::PADDING_X + Fields::LEADING_ICON.to_pixels(rem) + Fields::GAP;
    let input_width = count.left() - Fields::GAP - input_start;

    assert!(
        count.size.width <= Fields::COUNT_MAX_WIDTH.to_pixels(rem),
        "the count stops at its maximum width"
    );
    assert!(
        input_width >= Fields::SEARCH_INPUT_MIN_WIDTH.to_pixels(rem),
        "the input keeps its minimum width"
    );
    assert!(
        next.right() <= field.right(),
        "nothing paints past the field"
    );
}
