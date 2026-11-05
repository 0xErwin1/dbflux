use gpui::prelude::*;
use gpui::{
    AccessibilityFrame, AnyElement, App, IntoElement, Pixels, TestAppContext, VisualTestContext, px,
};

use super::{TimelineBucket, TimelineHistogram, TimelineLegend, bucket_heights, peak};
use crate::accessibility::{FrameCapture, accessibility_node, accessible_name};
use crate::primitives::BadgeTone;
use crate::tokens::{Borders, DocumentMetrics};

const HISTOGRAM_WIDTH: f32 = 400.0;

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

fn histogram_window(
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
                    .flex_col()
                    .w(px(HISTOGRAM_WIDTH))
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

/// Four buckets rising to a peak of four, with an empty one at the end.
fn counts(_cx: &mut App) -> AnyElement {
    TimelineHistogram::new("events")
        .aria_label("Events per hour")
        .buckets([
            TimelineBucket::new(1, 0),
            TimelineBucket::new(2, 1),
            TimelineBucket::new(4, 2),
            TimelineBucket::new(0, 0),
        ])
        .into_any_element()
}

fn with_legend(_cx: &mut App) -> AnyElement {
    TimelineHistogram::new("events")
        .bucket(TimelineBucket::new(4, 1))
        .legend([
            TimelineLegend::new("calls", BadgeTone::Accent),
            TimelineLegend::new("refused", BadgeTone::Danger),
        ])
        .into_any_element()
}

fn bar_height(count: i64) -> Pixels {
    DocumentMetrics::TIMELINE_BAR_HEIGHT * (count as f32 / 4.0)
}

#[test]
fn a_bucket_is_sized_by_its_share_of_the_peak() {
    assert_eq!(
        bucket_heights(TimelineBucket::new(1, 0), 4),
        (bar_height(1), px(0.0))
    );
    assert_eq!(
        bucket_heights(TimelineBucket::new(4, 2), 4),
        (bar_height(2), bar_height(2))
    );
    assert_eq!(
        bucket_heights(TimelineBucket::new(4, 4), 4),
        (px(0.0), DocumentMetrics::TIMELINE_BAR_HEIGHT)
    );
}

#[test]
fn a_bucket_with_events_is_at_least_a_hairline_and_an_empty_one_draws_nothing() {
    assert_eq!(
        bucket_heights(TimelineBucket::new(1, 0), 400),
        (Borders::THIN, px(0.0))
    );
    assert_eq!(
        bucket_heights(TimelineBucket::new(0, 0), 4),
        (px(0.0), px(0.0))
    );
}

#[test]
fn a_histogram_without_events_is_flat() {
    assert_eq!(
        bucket_heights(TimelineBucket::new(0, 0), 0),
        (px(0.0), px(0.0))
    );
    assert_eq!(peak(&[]), 1);
    assert_eq!(peak(&[TimelineBucket::new(0, 0)]), 1);
}

#[test]
fn the_highlight_can_never_grow_past_its_bucket_and_a_negative_count_is_no_events() {
    assert_eq!(
        bucket_heights(TimelineBucket::new(2, 5), 2),
        (px(0.0), DocumentMetrics::TIMELINE_BAR_HEIGHT)
    );
    assert_eq!(
        bucket_heights(TimelineBucket::new(-3, -1), 2),
        (px(0.0), px(0.0))
    );
    assert_eq!(TimelineBucket::new(7, 3).highlighted(), 3);
    assert_eq!(TimelineBucket::new(7, 3).total(), 7);
}

/// A bucket's minimum height is the bucket's, not each of its parts: a bucket
/// whose two parts both round to nothing is as tall as the same bucket with
/// nothing highlighted, and the highlight is a share of that one height.
#[test]
fn a_mixed_bucket_is_as_tall_as_its_own_count() {
    let (rest, highlighted) = bucket_heights(TimelineBucket::new(2, 1), 10_000);
    let (plain, _) = bucket_heights(TimelineBucket::new(2, 0), 10_000);

    assert_eq!(rest + highlighted, plain);
    assert_eq!(rest + highlighted, Borders::THIN);
    assert_eq!(rest, highlighted);
    assert_eq!(
        bucket_heights(TimelineBucket::new(2, 2), 10_000),
        (px(0.0), Borders::THIN)
    );
}

#[test]
fn the_peak_is_the_busiest_bucket() {
    assert_eq!(
        peak(&[
            TimelineBucket::new(1, 0),
            TimelineBucket::new(7, 3),
            TimelineBucket::new(2, 0),
        ]),
        7
    );
}

/// One bar per bucket: every bar is `TIMELINE_BAR_HEIGHT` tall,
/// `TIMELINE_BAR_GAP` from its neighbour, they share their widths, and the
/// highlighted part is drawn over the rest of its bucket, both ending at the
/// bar's bottom.
#[gpui::test]
fn the_bars_share_the_strips_axis_with_the_highlight_over_the_rest(cx: &mut TestAppContext) {
    let (_frame, window) = histogram_window(cx, counts);

    let histogram = window
        .debug_bounds("events")
        .expect("the histogram is laid out");
    assert_eq!(histogram.size.width, px(HISTOGRAM_WIDTH));

    let first = window.debug_bounds("events-bar-0").expect("the first bar");
    let second = window.debug_bounds("events-bar-1").expect("the second bar");
    let third = window.debug_bounds("events-bar-2").expect("the third bar");
    let empty = window.debug_bounds("events-bar-3").expect("the empty bar");

    assert_eq!(first.size.height, DocumentMetrics::TIMELINE_BAR_HEIGHT);
    assert_eq!(
        second.left(),
        first.right() + DocumentMetrics::TIMELINE_BAR_GAP
    );
    assert_eq!(
        third.left(),
        second.right() + DocumentMetrics::TIMELINE_BAR_GAP
    );
    assert_eq!(
        empty.left(),
        third.right() + DocumentMetrics::TIMELINE_BAR_GAP
    );
    assert_eq!(first.size.width, third.size.width);
    // An empty bucket keeps its slot on the axis while there is room.
    assert_eq!(empty.size.width, first.size.width);
    assert_eq!(
        third.bottom(),
        histogram.bottom() - DocumentMetrics::TIMELINE_PADDING_Y
    );
    assert_eq!(
        first.top(),
        histogram.top() + DocumentMetrics::TIMELINE_PADDING_Y
    );

    // A bucket of four events out of a peak of four fills its bar; two of them
    // are highlighted over the other two.
    let rest = window
        .debug_bounds("events-bar-2-rest")
        .expect("the rest of the bucket");
    let highlighted = window
        .debug_bounds("events-bar-2-highlight")
        .expect("the highlighted part");

    assert_eq!(rest.size.height, bar_height(2));
    assert_eq!(highlighted.size.height, bar_height(2));
    assert_eq!(rest.bottom(), third.bottom());
    assert_eq!(highlighted.bottom(), rest.top());
    assert_eq!(highlighted.top(), third.top());
    assert_eq!(highlighted.size.width, third.size.width);

    // The bucket with one event, none of them highlighted, draws a quarter of
    // the bar and nothing above it.
    assert_eq!(
        window
            .debug_bounds("events-bar-0-rest")
            .expect("the rest of the first bucket")
            .size
            .height,
        bar_height(1)
    );
    assert_eq!(
        window
            .debug_bounds("events-bar-0-highlight")
            .expect("the first bucket has nothing highlighted")
            .size
            .height,
        px(0.0)
    );
    assert_eq!(
        window
            .debug_bounds("events-bar-3-rest")
            .expect("the empty bucket")
            .size
            .height,
        px(0.0)
    );
}

/// The legend is optional: without entries the bars fill the width on their
/// own, with them each entry is a swatch and its label, `TIMELINE_LEGEND_GAP`
/// to the right of the bars and `TIMELINE_LEGEND_ROW_GAP` apart.
#[gpui::test]
fn the_legend_is_drawn_only_when_it_is_given(cx: &mut TestAppContext) {
    let (_frame, window) = histogram_window(cx, counts);
    assert!(window.debug_bounds("events-swatch-0").is_none());
    assert!(window.debug_bounds("events-legend-0").is_none());
    // Without a legend the bars fill the whole width on their own.
    assert_eq!(
        window
            .debug_bounds("events-bar-3")
            .expect("the last bar")
            .right(),
        px(HISTOGRAM_WIDTH)
    );

    let (_frame, window) = histogram_window(cx, with_legend);
    let bar = window.debug_bounds("events-bar-0").expect("the first bar");
    let calls = window
        .debug_bounds("events-swatch-0")
        .expect("the first swatch");
    let refused = window
        .debug_bounds("events-swatch-1")
        .expect("the second swatch");

    assert_eq!(
        calls.size,
        gpui::size(
            DocumentMetrics::TIMELINE_SWATCH,
            DocumentMetrics::TIMELINE_SWATCH
        )
    );
    assert_eq!(
        window
            .debug_bounds("events-legend-0")
            .expect("the first legend entry")
            .left(),
        bar.right() + DocumentMetrics::TIMELINE_LEGEND_GAP
    );
    assert_eq!(refused.left(), calls.left());

    // The entries stack `TIMELINE_LEGEND_ROW_GAP` apart, and the column ends at
    // the histogram's own right edge.
    let first_entry = window
        .debug_bounds("events-legend-0")
        .expect("the first legend entry");
    let second_entry = window
        .debug_bounds("events-legend-1")
        .expect("the second legend entry");
    let column = window.debug_bounds("events-legend").expect("the legend");
    assert_eq!(
        second_entry.top(),
        first_entry.bottom() + DocumentMetrics::TIMELINE_LEGEND_ROW_GAP
    );
    assert_eq!(column.right(), px(HISTOGRAM_WIDTH));
}

/// A long legend label cannot take the bars' width: the legend is bounded and
/// its label truncates, so the bars keep a real share of the strip.
#[gpui::test]
fn a_long_legend_label_cannot_take_the_bars_width(cx: &mut TestAppContext) {
    fn long_label(_cx: &mut App) -> AnyElement {
        TimelineHistogram::new("events")
            .bucket(TimelineBucket::new(4, 1))
            .legend([TimelineLegend::new(
                "a refused call, with the reason it was refused and the rule that refused it",
                BadgeTone::Danger,
            )])
            .into_any_element()
    }

    let (_frame, window) = histogram_window(cx, long_label);
    let histogram = window
        .debug_bounds("events")
        .expect("the histogram is laid out");
    let legend = window.debug_bounds("events-legend").expect("the legend");
    let bar = window.debug_bounds("events-bar-0").expect("the first bar");

    assert!(
        legend.size.width <= DocumentMetrics::TIMELINE_LEGEND_MAX_WIDTH,
        "the legend stays within its own width: {:?}",
        legend.size.width
    );
    assert_eq!(bar.left(), histogram.left());
    assert_eq!(
        bar.right(),
        legend.left() - DocumentMetrics::TIMELINE_LEGEND_GAP
    );
    assert!(
        bar.size.width >= px(HISTOGRAM_WIDTH / 2.0),
        "the bars keep half the strip: {:?}",
        bar.size.width
    );
}

/// Enough buckets to need more gap than the strip is wide: the gaps give way
/// so every bucket with events still draws a hairline, and the row still fits.
#[gpui::test]
fn many_buckets_keep_a_hairline_each(cx: &mut TestAppContext) {
    const BUCKETS: usize = 288;

    fn dense(_cx: &mut App) -> AnyElement {
        TimelineHistogram::new("events")
            .buckets((0..BUCKETS).map(|_| TimelineBucket::new(1, 0)))
            .into_any_element()
    }

    let (_frame, window) = histogram_window(cx, dense);
    let histogram = window
        .debug_bounds("events")
        .expect("the histogram is laid out");
    let first = window.debug_bounds("events-bar-0").expect("the first bar");
    let second = window.debug_bounds("events-bar-1").expect("the second bar");
    // The id of the last of the `BUCKETS` bars.
    let last = window.debug_bounds("events-bar-287").expect("the last bar");

    assert!(
        first.size.width >= Borders::THIN,
        "a populated bucket keeps a hairline: {:?}",
        first.size.width
    );
    assert!(
        second.left() - first.right() < DocumentMetrics::TIMELINE_BAR_GAP,
        "the gaps shrink when they do not fit: {:?}",
        second.left() - first.right()
    );
    assert!(
        last.right() <= histogram.right(),
        "the burst still fits the strip: {:?} > {:?}",
        last.right(),
        histogram.right()
    );
}

/// The histogram is a group named by the caller, so a screen can announce
/// what the bars count.
#[gpui::test]
fn the_histogram_announces_its_name(cx: &mut TestAppContext) {
    let (frame, _window) = histogram_window(cx, counts);

    assert_eq!(
        accessible_name(&frame, "events").as_deref(),
        Some("Events per hour")
    );
    assert_eq!(
        accessibility_node(&frame, "events").map(|node| node.role()),
        Some(gpui::Role::Group)
    );
}
