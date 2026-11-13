use std::time::Duration;

use super::{TimelineBar, TimelineStrip, bar_extent};
use crate::accessibility::{FrameCapture, accessibility_node, accessible_name};
use crate::primitives::BadgeTone;
use crate::tokens::TimelineMetrics;
use gpui::{AccessibilityFrame, App, IntoElement as _, Role, TestAppContext, VisualTestContext};

const STRIP_WIDTH: f32 = 400.0;

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

fn strip_window(
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

            gpui::div().size_full().child(
                gpui::div()
                    .flex()
                    .w(gpui::px(STRIP_WIDTH))
                    .child((self.0)(cx)),
            )
        }
    }

    install_test_theme(cx);
    let (_, window) = cx.add_window_view(move |_, _| Surface(build));
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);
    (frame, window)
}

fn millis(value: u64) -> Duration {
    Duration::from_millis(value)
}

fn phases(_cx: &mut App) -> gpui::AnyElement {
    TimelineStrip::new("hop-1", millis(200))
        .aria_label("Hop 1")
        .bar(
            TimelineBar::new("hop-1-lookup", "Lookup: 50 ms", millis(0), millis(50))
                .tone(BadgeTone::Info),
        )
        .bar(
            TimelineBar::new(
                "hop-1-transfer",
                "Transfer: 100 ms",
                millis(100),
                millis(100),
            )
            .tone(BadgeTone::Success),
        )
        .bar(TimelineBar::new(
            "hop-1-blip",
            "Blip: 0 ms",
            millis(60),
            millis(0),
        ))
        .into_any_element()
}

#[test]
fn a_bar_is_placed_by_its_start_and_sized_by_its_duration() {
    assert_eq!(
        bar_extent(millis(50), millis(100), millis(200)),
        (0.25, 0.5)
    );
}

#[test]
fn a_bar_running_past_the_span_is_cut_at_the_end() {
    assert_eq!(
        bar_extent(millis(150), millis(100), millis(200)),
        (0.75, 0.25)
    );
    assert_eq!(bar_extent(millis(300), millis(10), millis(200)), (1.0, 0.0));
}

#[test]
fn a_zero_span_draws_every_bar_at_the_start() {
    assert_eq!(bar_extent(millis(5), millis(5), Duration::ZERO), (0.0, 0.0));
}

#[test]
fn an_unbounded_duration_does_not_overflow() {
    assert_eq!(
        bar_extent(millis(100), Duration::MAX, millis(200)),
        (0.5, 0.5)
    );
}

/// Each bar is a label under its own id, named by the caller's label, and
/// the strip is a group named by its own label.
#[gpui::test]
fn the_bars_carry_their_ids_and_names(cx: &mut TestAppContext) {
    let (frame, _window) = strip_window(cx, phases);

    assert_eq!(
        accessible_name(&frame, "hop-1-lookup").as_deref(),
        Some("Lookup: 50 ms")
    );
    assert_eq!(
        accessible_name(&frame, "hop-1-transfer").as_deref(),
        Some("Transfer: 100 ms")
    );
    assert_eq!(accessible_name(&frame, "hop-1").as_deref(), Some("Hop 1"));
    assert_eq!(
        accessibility_node(&frame, "hop-1").map(|node| node.role()),
        Some(Role::Group)
    );
}

/// The bars sit on the strip's axis: a phase from 100 ms to 200 ms on a
/// 200 ms span covers the second half of the track, and a zero-length
/// phase still draws at the minimum width.
#[gpui::test]
fn the_bars_share_the_strips_axis(cx: &mut TestAppContext) {
    let (_frame, window) = strip_window(cx, phases);

    let strip = window.debug_bounds("hop-1").expect("the strip is laid out");
    let lookup = window
        .debug_bounds("hop-1-lookup")
        .expect("the lookup bar is laid out");
    let transfer = window
        .debug_bounds("hop-1-transfer")
        .expect("the transfer bar is laid out");
    let blip = window
        .debug_bounds("hop-1-blip")
        .expect("the zero-length bar is laid out");

    assert_eq!(strip.size.width, gpui::px(STRIP_WIDTH));
    assert_eq!(strip.size.height, TimelineMetrics::STRIP_HEIGHT);
    assert_eq!(lookup.left(), strip.left());
    assert_eq!(lookup.size.width, gpui::px(STRIP_WIDTH / 4.0));
    assert_eq!(transfer.left(), strip.left() + gpui::px(STRIP_WIDTH / 2.0));
    assert_eq!(transfer.right(), strip.right());
    assert_eq!(blip.size.width, TimelineMetrics::MIN_BAR);
    assert_eq!(blip.size.height, strip.size.height);
}
