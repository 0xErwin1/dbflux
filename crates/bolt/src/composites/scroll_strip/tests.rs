use gpui::{
    Bounds, Context, InteractiveElement as _, IntoElement, Modifiers, ParentElement as _, Pixels,
    Render, ScrollDelta, ScrollHandle, ScrollWheelEvent, Styled as _, TestAppContext, TouchPhase,
    VisualTestContext, Window, div, point, px,
};

use super::{StripReveal, scroll_strip};

const PANE: f32 = 200.0;
const CHILD: f32 = 120.0;

struct Host {
    handle: ScrollHandle,
    reveal: StripReveal,
    children: usize,
    pane: Pixels,
    target: Option<usize>,
}

impl Render for Host {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(index) = self.target {
            self.reveal.child(&self.handle, index, self.pane);
        }

        div()
            .w(self.pane)
            .child(
                scroll_strip("strip", &self.handle).children((0..self.children).map(|index| {
                    div()
                        .debug_selector(move || format!("child-{index}"))
                        .flex_none()
                        .w(px(CHILD))
                        .h(px(30.0))
                })),
            )
    }
}

fn open(
    children: usize,
    target: Option<usize>,
    cx: &mut TestAppContext,
) -> (gpui::Entity<Host>, &mut VisualTestContext) {
    let (host, window) = cx.add_window_view(move |_, _| Host {
        handle: ScrollHandle::new(),
        reveal: StripReveal::default(),
        children,
        pane: px(PANE),
        target,
    });
    window.run_until_parked();
    (host, window)
}

fn bounds(window: &mut VisualTestContext, selector: &'static str) -> Bounds<Pixels> {
    window
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("{selector} is laid out"))
}

fn wheel(window: &mut VisualTestContext, host: &gpui::Entity<Host>, dy: Pixels) {
    let position = window.update(|_, cx| host.read(cx).handle.bounds().center());
    window.simulate_event(ScrollWheelEvent {
        position,
        delta: ScrollDelta::Pixels(point(px(0.0), dy)),
        modifiers: Modifiers::none(),
        touch_phase: TouchPhase::Moved,
    });
    window.run_until_parked();
}

#[gpui::test]
fn children_wider_than_the_strip_stay_inside_its_bounds(cx: &mut TestAppContext) {
    let (host, window) = open(3, None, cx);
    let strip = window.update(|_, cx| host.read(cx).handle.bounds());

    assert_eq!(strip.size.width, px(PANE));
    assert!(
        window.update(|_, cx| host.read(cx).handle.max_offset().x) > px(0.0),
        "three children of {CHILD} px overflow a {PANE} px strip"
    );
    assert!(bounds(window, "child-0").right() <= strip.right());
}

#[gpui::test]
fn the_wheel_scrolls_the_children(cx: &mut TestAppContext) {
    let (host, window) = open(3, None, cx);
    let before = bounds(window, "child-0").left();

    wheel(window, &host, px(-40.0));

    assert!(bounds(window, "child-0").left() < before);
    assert!(window.update(|_, cx| host.read(cx).handle.offset().x) < px(0.0));
}

#[gpui::test]
fn revealing_the_last_child_brings_it_fully_into_view(cx: &mut TestAppContext) {
    let (host, window) = open(3, Some(2), cx);
    window.update(|_, cx| host.update(cx, |_, cx| cx.notify()));
    window.run_until_parked();

    let strip = window.update(|_, cx| host.read(cx).handle.bounds());
    let last = bounds(window, "child-2");

    assert!(last.left() >= strip.left());
    assert!(last.right() <= strip.right() + px(0.5));
}

#[gpui::test]
fn nothing_scrolls_when_the_children_fit(cx: &mut TestAppContext) {
    let (host, window) = open(1, Some(0), cx);
    let before = bounds(window, "child-0").left();

    wheel(window, &host, px(-40.0));

    assert_eq!(bounds(window, "child-0").left(), before);
    assert_eq!(
        window.update(|_, cx| host.read(cx).handle.max_offset().x),
        px(0.0)
    );
}

#[gpui::test]
fn revealing_a_range_scrolls_the_least_that_shows_it(cx: &mut TestAppContext) {
    let (host, window) = open(3, None, cx);
    let handle = window.update(|_, cx| host.read(cx).handle.clone());
    let reveal = StripReveal::default();

    reveal.range(&handle, px(250.0)..px(300.0), px(PANE));
    assert_eq!(handle.offset().x, px(-100.0), "the range ends at the edge");

    reveal.range(&handle, px(60.0)..px(90.0), px(PANE));
    assert_eq!(handle.offset().x, px(-60.0), "the range starts at the edge");

    reveal.range(&handle, px(70.0)..px(110.0), px(PANE));
    assert_eq!(
        handle.offset().x,
        px(-60.0),
        "a range in view does not move it"
    );
}

#[gpui::test]
fn a_reveal_keeps_the_users_scroll_until_the_target_or_width_changes(cx: &mut TestAppContext) {
    let (host, window) = open(3, Some(2), cx);
    window.update(|_, cx| host.update(cx, |_, cx| cx.notify()));
    window.run_until_parked();

    wheel(window, &host, px(40.0));
    let scrolled = window.update(|_, cx| host.read(cx).handle.offset().x);
    window.update(|_, cx| host.update(cx, |_, cx| cx.notify()));
    window.run_until_parked();

    assert_eq!(
        window.update(|_, cx| host.read(cx).handle.offset().x),
        scrolled,
        "a repeated reveal of the same child at the same width leaves the scroll alone"
    );
}

struct SegmentHost {
    handle: ScrollHandle,
    reveal: StripReveal,
    segments: crate::primitives::SegmentBounds,
}

impl Render for SegmentHost {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        self.reveal
            .segment(&self.handle, &self.segments, "last", px(120.0));

        div().w(px(120.0)).child(
            scroll_strip("segment-strip", &self.handle).child(
                crate::primitives::SegmentedControl::new(
                    ["first", "second", "third", "last"]
                        .map(|id| crate::primitives::SegmentedItem::new(id, id))
                        .to_vec(),
                    "last",
                    |_, _, _| {},
                )
                .segment_bounds(&self.segments),
            ),
        )
    }
}

#[gpui::test]
fn a_measured_segment_is_revealed_exactly(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(|cx| {
        crate::theme::install(
            crate::brand::Brand::neutral(),
            crate::palette::PaletteMode::Dark,
            crate::density::Density::Default,
            cx,
        );
    });
    let (host, window) = cx.add_window_view(|_, _| SegmentHost {
        handle: ScrollHandle::new(),
        reveal: StripReveal::default(),
        segments: Default::default(),
    });
    window.run_until_parked();
    window.update(|_, cx| host.update(cx, |_, cx| cx.notify()));
    window.run_until_parked();

    let (strip, last) = window.update(|_, cx| {
        let host = host.read(cx);
        (host.handle.bounds(), host.segments.get("last"))
    });
    let last = last.expect("the last segment was painted");

    assert!(
        window.update(|_, cx| host.read(cx).handle.max_offset().x) > px(0.0),
        "the control is wider than the strip"
    );
    assert!(last.left() >= strip.left());
    assert!(last.right() <= strip.right() + px(0.5));
}
