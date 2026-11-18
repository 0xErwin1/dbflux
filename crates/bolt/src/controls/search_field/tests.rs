use std::cell::Cell;
use std::rc::Rc;

use gpui::{
    AccessibilityFrame, AppContext as _, Context, Entity, IntoElement, Modifiers, Render, Role,
    TestAppContext, VisualTestContext, Window, div, px,
};

use super::{SearchCount, SearchField, SearchTrigger};
use crate::accessibility::{FrameCapture, accessibility_node, accessible_name};
use crate::controls::InputState;
use crate::primitives::{BadgeTone, Kbd};
use crate::tokens::{BASE_REM, Fields};

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

/// A closed field in a 300 px row, reporting the opens it is clicked for.
struct TriggerHarness {
    opens: Rc<Cell<usize>>,
    shortcut: Option<&'static str>,
    width: Option<gpui::Rems>,
}

impl Render for TriggerHarness {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        use gpui::{ParentElement as _, Styled as _};

        let opens = self.opens.clone();
        let field = SearchTrigger::new("body-search", "Search in body")
            .on_open(move |_, _, _| opens.set(opens.get() + 1));
        let field = match self.shortcut {
            Some(shortcut) => field.shortcut(shortcut),
            None => field,
        };
        let field = match self.width {
            Some(width) => field.width(width),
            None => field,
        };

        div()
            .size_full()
            .child(div().flex().w(px(300.0)).child(field))
    }
}

fn closed<'a>(
    cx: &'a mut TestAppContext,
    shortcut: Option<&'static str>,
    width: Option<gpui::Rems>,
) -> (
    Rc<Cell<usize>>,
    AccessibilityFrame,
    &'a mut VisualTestContext,
) {
    install_test_theme(cx);
    let opens = Rc::new(Cell::new(0));
    let reported = opens.clone();
    let (_, window) = cx.add_window_view(move |_, _| TriggerHarness {
        opens: reported,
        shortcut,
        width,
    });
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    (opens, frame, window)
}

/// A closed field draws the same frame and height as the open one, fills the
/// row it is given and draws no input and no count.
#[gpui::test]
fn a_closed_field_draws_the_field_metrics_and_no_input(cx: &mut TestAppContext) {
    let (_opens, frame, window) = closed(cx, Some("Ctrl F"), None);

    let field = window
        .debug_bounds("body-search")
        .expect("the closed field is laid out");

    assert_eq!(field.size.height, Fields::HEIGHT.to_pixels(px(BASE_REM)));
    assert_eq!(
        field.size.width,
        px(300.0),
        "without a width it fills the row"
    );
    assert!(
        frame
            .nodes()
            .all(|(_, node)| node.id() != "body-search-input"),
        "a closed field has no input"
    );
    assert!(
        frame
            .nodes()
            .all(|(_, node)| node.id() != "body-search-count"),
        "a closed field draws no count"
    );
}

/// The closed field is a button named by its placeholder, so a reader and
/// automation find the control that opens the search.
#[gpui::test]
fn a_closed_field_is_a_button_named_by_its_placeholder(cx: &mut TestAppContext) {
    let (_opens, frame, _window) = closed(cx, Some("Ctrl F"), None);

    assert_eq!(
        accessible_name(&frame, "body-search").as_deref(),
        Some("Search in body")
    );
    assert_eq!(
        accessibility_node(&frame, "body-search").map(|node| node.role()),
        Some(Role::Button)
    );
}

/// The placeholder is drawn between the icon and the shortcut keycap, and
/// the keycap carries the chord it shows.
#[gpui::test]
fn a_closed_field_draws_the_placeholder_and_the_shortcut_keycap(cx: &mut TestAppContext) {
    let (_opens, frame, window) = closed(cx, Some("Ctrl F"), None);

    assert_eq!(
        accessible_name(&frame, "body-search-placeholder").as_deref(),
        Some("Search in body")
    );
    assert_eq!(
        accessible_name(&frame, "body-search-key").as_deref(),
        Some("Ctrl F")
    );

    let field = window
        .debug_bounds("body-search")
        .expect("the closed field is laid out");
    let placeholder = window
        .debug_bounds("body-search-placeholder")
        .expect("the placeholder is laid out");
    let key = window
        .debug_bounds("body-search-key")
        .expect("the keycap is laid out");

    assert!(placeholder.left() > field.left(), "the icon leads the text");
    assert!(
        key.left() >= placeholder.right(),
        "the keycap closes the field"
    );
    assert!(key.right() <= field.right(), "everything stays inside");
}

/// Clicking the closed field asks the host to open it.
#[gpui::test]
fn clicking_a_closed_field_opens_it(cx: &mut TestAppContext) {
    let (opens, _frame, window) = closed(cx, Some("Ctrl F"), None);
    let field = window
        .debug_bounds("body-search")
        .expect("the closed field is laid out");

    window.simulate_click(field.center(), Modifiers::default());
    window.run_until_parked();

    assert_eq!(opens.get(), 1);
}

/// Without a shortcut the closed field draws no keycap, and a fixed width
/// holds instead of filling the row.
#[gpui::test]
fn a_closed_field_without_a_shortcut_or_a_fixed_width(cx: &mut TestAppContext) {
    let (_opens, frame, window) = closed(cx, None, Some(crate::tokens::ui(220.0)));

    assert!(
        frame
            .nodes()
            .all(|(_, node)| node.id() != "body-search-key"),
        "no keycap without a shortcut"
    );

    let field = window
        .debug_bounds("body-search")
        .expect("the closed field is laid out");
    assert_eq!(
        field.size.width,
        crate::tokens::ui(220.0).to_pixels(px(BASE_REM))
    );
}

/// A closed field narrower than its contents keeps its parts in order inside
/// the frame: the placeholder truncates first, the icon and the keycap keep
/// their size, and the keycap never paints past the frame's own width.
#[gpui::test]
fn a_narrow_closed_field_keeps_its_parts_inside_the_frame(cx: &mut TestAppContext) {
    let rem = px(BASE_REM);
    let inset = Fields::PADDING_X + Fields::LEADING_ICON.to_pixels(rem) + Fields::GAP;
    let mut keycap_width = None;

    for width in [60.0_f32, 80.0, 90.0, 100.0, 140.0] {
        install_test_theme(cx);
        let (_, window) = cx.add_window_view(move |_, _| ProbeTrigger { width });
        let capture = FrameCapture::observe(window);
        let _frame = capture.frame(window);
        let field = window.debug_bounds("probe-trigger").expect("field");
        let placeholder = window
            .debug_bounds("probe-trigger-placeholder")
            .expect("placeholder");
        let key = window.debug_bounds("probe-trigger-key").expect("keycap");

        assert!(
            placeholder.left() >= field.left() + inset,
            "at {width} px the icon keeps its width and the text starts after it"
        );
        assert!(
            placeholder.right() <= key.left(),
            "at {width} px the placeholder truncates before the keycap"
        );
        let expected = *keycap_width.get_or_insert(key.size.width);
        assert_eq!(
            key.size.width, expected,
            "at {width} px the keycap keeps its size"
        );

        if width >= 100.0 {
            assert!(
                key.right() <= field.right(),
                "at {width} px the keycap stays inside the frame"
            );
        }
    }
}

struct ProbeTrigger {
    width: f32,
}

impl Render for ProbeTrigger {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        use gpui::{ParentElement as _, Styled as _};

        div().size_full().child(
            div().flex().w(px(self.width)).child(
                SearchTrigger::new("probe-trigger", "Search the whole document body")
                    .shortcut("Ctrl F"),
            ),
        )
    }
}
