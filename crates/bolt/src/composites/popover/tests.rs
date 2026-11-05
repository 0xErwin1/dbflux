//! The popover's tests: the placement math, the frame's dialog parts, focus
//! moving in and back, dismissal on Escape and on a click away, the hover
//! intent under the test clock, and a dropdown list opened inside the surface.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use gpui::prelude::*;
use gpui::{
    Bounds, Context, Entity, FocusHandle, IntoElement, Modifiers, Pixels, Point, Render, Size,
    TestAppContext, VisualTestContext, Window, div, point, px, size,
};

use super::{
    HOVER_DELAY, HOVER_GRACE, Popover, PopoverPlacement, PopoverSide, PopoverState, place_popover,
};
use crate::accessibility::{FrameCapture, accessibility_node, accessible_name};
use crate::controls::{Button, Dropdown, DropdownItem};
use crate::tokens::{Fields, PopoverMetrics, Spacing};

/// The rectangle the host anchors its popover to, in window coordinates.
fn anchor() -> Bounds<Pixels> {
    Bounds::new(point(px(120.0), px(120.0)), size(px(140.0), px(20.0)))
}

/// A 300 by 180 px surface, as a form popover draws it.
fn frame_size() -> Size<Pixels> {
    size(px(300.0), px(180.0))
}

fn viewport() -> Size<Pixels> {
    size(px(800.0), px(600.0))
}

// ---------------------------------------------------------------------------
// The placement math
// ---------------------------------------------------------------------------

/// An anchor near the top with room under it keeps the surface below, left
/// aligned and one gap away.
#[test]
fn a_surface_with_room_under_its_anchor_lands_below_it() {
    let placement = place_popover(
        anchor(),
        frame_size(),
        viewport(),
        PopoverMetrics::ANCHOR_GAP,
        PopoverMetrics::WINDOW_MARGIN,
        PopoverSide::Below,
    );

    assert_eq!(
        placement,
        PopoverPlacement {
            origin: point(px(120.0), px(146.0)),
            side: PopoverSide::Below,
        }
    );
}

/// An anchor near the bottom edge has no room under it, so the surface flips
/// above the anchor instead of covering it.
#[test]
fn a_surface_with_no_room_below_flips_above_its_anchor() {
    let low = Bounds::new(point(px(120.0), px(520.0)), size(px(140.0), px(20.0)));

    let placement = place_popover(
        low,
        frame_size(),
        viewport(),
        PopoverMetrics::ANCHOR_GAP,
        PopoverMetrics::WINDOW_MARGIN,
        PopoverSide::Below,
    );

    assert_eq!(placement.side, PopoverSide::Above);
    assert_eq!(
        placement.origin,
        point(
            px(120.0),
            px(520.0) - px(180.0) - PopoverMetrics::ANCHOR_GAP
        )
    );
}

/// A surface that fits on neither side keeps the side it asked for and starts
/// at the window margin instead of hanging off the window.
#[test]
fn a_surface_that_fits_on_neither_side_is_snapped_inside_the_window() {
    let tall = size(px(300.0), px(560.0));

    let below = place_popover(
        anchor(),
        tall,
        viewport(),
        PopoverMetrics::ANCHOR_GAP,
        PopoverMetrics::WINDOW_MARGIN,
        PopoverSide::Below,
    );
    let above = place_popover(
        anchor(),
        tall,
        viewport(),
        PopoverMetrics::ANCHOR_GAP,
        PopoverMetrics::WINDOW_MARGIN,
        PopoverSide::Above,
    );

    assert_eq!(below.side, PopoverSide::Below);
    assert_eq!(above.side, PopoverSide::Above);
    assert_eq!(
        below.origin.y,
        px(146.0).min(px(600.0) - px(560.0) - PopoverMetrics::WINDOW_MARGIN)
    );
    assert_eq!(
        above.origin.y,
        PopoverMetrics::WINDOW_MARGIN,
        "a surface taller than the room starts at the top margin"
    );
}

/// An anchor near the right edge pulls the surface left so it stays inside the
/// window, keeping the margin.
#[test]
fn a_surface_past_the_right_edge_is_pulled_inside() {
    let right = Bounds::new(point(px(700.0), px(100.0)), size(px(60.0), px(20.0)));

    let placement = place_popover(
        right,
        frame_size(),
        viewport(),
        PopoverMetrics::ANCHOR_GAP,
        PopoverMetrics::WINDOW_MARGIN,
        PopoverSide::Below,
    );

    assert_eq!(
        placement.origin.x,
        px(800.0) - px(300.0) - PopoverMetrics::WINDOW_MARGIN
    );
}

/// An anchor near the top has no room over it, so a surface that prefers the
/// space above it lands under the anchor instead.
#[test]
fn a_surface_that_prefers_above_flips_below_when_there_is_no_room() {
    let placement = place_popover(
        anchor(),
        frame_size(),
        viewport(),
        PopoverMetrics::ANCHOR_GAP,
        PopoverMetrics::WINDOW_MARGIN,
        PopoverSide::Above,
    );

    assert_eq!(placement.side, PopoverSide::Below);
    assert_eq!(
        placement.origin,
        point(px(120.0), px(140.0) + PopoverMetrics::ANCHOR_GAP)
    );
}

/// A surface wider than the window starts at the left margin rather than
/// hanging off the left side.
#[test]
fn a_surface_wider_than_the_window_starts_at_the_left_margin() {
    let left = Bounds::new(point(px(10.0), px(100.0)), size(px(20.0), px(20.0)));

    let placement = place_popover(
        left,
        size(px(900.0), px(180.0)),
        viewport(),
        PopoverMetrics::ANCHOR_GAP,
        PopoverMetrics::WINDOW_MARGIN,
        PopoverSide::Below,
    );

    assert_eq!(placement.origin.x, PopoverMetrics::WINDOW_MARGIN);
}

// ---------------------------------------------------------------------------
// A rendered host
// ---------------------------------------------------------------------------

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

/// A screen with a trigger, a field inside the popover's body and a dropdown
/// whose list opens above the window, as a host composes them.
struct Host {
    state: Entity<PopoverState>,
    /// The rectangle the surface points at; `None` renders it in place.
    anchor: Option<Bounds<Pixels>>,
    trigger: FocusHandle,
    field: FocusHandle,
    dropdown: Entity<Dropdown>,
    dismissed: Rc<RefCell<usize>>,
}

impl Render for Host {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // A host keeps the anchor's rectangle fresh from the element it points
        // at: `set_anchor` takes the bounds that element laid out.
        self.state
            .update(cx, |state, _cx| state.set_anchor(self.anchor));

        let trigger = div()
            .id("popover-trigger")
            .track_focus(&self.trigger)
            .absolute()
            .left(px(120.0))
            .top(px(120.0))
            .w(px(140.0))
            .h(px(20.0))
            .cursor_pointer()
            .on_click(cx.listener(|this, _event, window, cx| {
                this.state.update(cx, |state, cx| match this.anchor {
                    Some(anchor) => state.open_at(anchor, window, cx),
                    None => state.open(window, cx),
                });
            }));

        div().size_full().child(trigger).child(
            Popover::new("form-popover", self.state.clone())
                .title("New workspace")
                .body(
                    div()
                        .id("popover-body")
                        .debug_selector(|| "popover-body".to_owned())
                        .flex()
                        .flex_col()
                        .gap(Spacing::XXS)
                        .child(
                            div()
                                .id("popover-field")
                                .track_focus(&self.field)
                                .h(px(30.0))
                                .child("Name"),
                        )
                        .child(self.dropdown.clone()),
                )
                .footer(vec![
                    Button::new("popover-cancel", "Cancel"),
                    Button::new("popover-create", "Create"),
                ]),
        )
    }
}

struct Harness {
    host: Entity<Host>,
    state: Entity<PopoverState>,
    trigger: FocusHandle,
    field: FocusHandle,
    dropdown: Entity<Dropdown>,
    dismissed: Rc<RefCell<usize>>,
}

fn open(cx: &mut TestAppContext) -> (Harness, &mut VisualTestContext) {
    install_test_theme(cx);

    let dismissed = Rc::new(RefCell::new(0));
    let (view, window) = cx.add_window_view({
        let dismissed = dismissed.clone();
        move |_window, cx| {
            let field = cx.focus_handle();
            let state = cx.new(|cx| PopoverState::new(cx).hover_intent(true));

            state.update(cx, |state, _cx| {
                state.set_initial_focus(Some(field.clone()));
                state.on_dismiss({
                    let dismissed = dismissed.clone();
                    move |_window, _cx| *dismissed.borrow_mut() += 1
                });
            });

            let dropdown = cx.new(|_cx| {
                Dropdown::new("popover-target").items(vec![
                    DropdownItem::new("Local"),
                    DropdownItem::new("Remote"),
                    DropdownItem::new("Staging"),
                    DropdownItem::new("Production"),
                ])
            });

            Host {
                state,
                anchor: Some(anchor()),
                trigger: cx.focus_handle(),
                field,
                dropdown,
                dismissed,
            }
        }
    });

    let (state, trigger, field, dropdown, dismissed) = window.update(|_window, cx| {
        let host = view.read(cx);

        (
            host.state.clone(),
            host.trigger.clone(),
            host.field.clone(),
            host.dropdown.clone(),
            host.dismissed.clone(),
        )
    });
    (
        Harness {
            host: view,
            state,
            trigger,
            field,
            dropdown,
            dismissed,
        },
        window,
    )
}

fn is_open(state: &Entity<PopoverState>, window: &mut VisualTestContext) -> bool {
    window.update(|_window, cx| state.read(cx).is_open())
}

fn click(window: &mut VisualTestContext, position: Point<Pixels>) {
    window.simulate_click(position, Modifiers::default());
    window.run_until_parked();
}

fn open_at_anchor(harness: &Harness, window: &mut VisualTestContext) {
    harness.state.update_in(window, |state, window, cx| {
        state.open_at(anchor(), window, cx)
    });
    window.run_until_parked();
}

fn set_anchor(harness: &Harness, window: &mut VisualTestContext, anchor: Option<Bounds<Pixels>>) {
    window.update(|_window, cx| {
        harness.host.update(cx, |host, cx| {
            host.anchor = anchor;
            cx.notify();
        });
    });
}

fn advance(window: &mut VisualTestContext, duration: Duration) {
    window.executor().advance_clock(duration);
    window.run_until_parked();
}

// ---------------------------------------------------------------------------
// The frame's parts and its placement
// ---------------------------------------------------------------------------

#[gpui::test]
fn a_closed_popover_draws_nothing(cx: &mut TestAppContext) {
    let (_harness, window) = open(cx);

    assert!(window.debug_bounds("form-popover").is_none());
}

#[gpui::test]
fn the_frame_draws_its_title_body_and_footer_under_the_anchor(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    open_at_anchor(&harness, window);

    let frame = window
        .debug_bounds("form-popover")
        .expect("the open frame is laid out");

    let rem_size = window.update(|window, _| window.rem_size());
    assert_eq!(frame.size.width, PopoverMetrics::WIDTH.to_pixels(rem_size));
    assert_eq!(frame.origin.x, anchor().origin.x);
    assert!(
        frame.origin.y >= anchor().bottom(),
        "the frame sits under its anchor, not over it"
    );
    assert!(window.debug_bounds("popover-body").is_some());
    assert!(window.debug_bounds("form-popover-title").is_some());
    assert!(window.debug_bounds("form-popover-footer").is_some());
    assert!(window.debug_bounds("popover-cancel").is_some());
    assert!(window.debug_bounds("popover-create").is_some());
}

#[gpui::test]
fn the_frame_is_a_named_dialog(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    open_at_anchor(&harness, window);

    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    let node = accessibility_node(&frame, "form-popover").expect("the frame's node");
    assert_eq!(node.role(), gpui::Role::Dialog);
    assert_eq!(
        accessible_name(&frame, "form-popover").as_deref(),
        Some("New workspace")
    );
}

#[gpui::test]
fn the_frame_flips_above_an_anchor_near_the_bottom_edge(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    let viewport = window.update(|window, _| window.viewport_size());
    let low = Bounds::new(
        point(px(120.0), viewport.height - px(40.0)),
        size(px(140.0), px(20.0)),
    );

    set_anchor(&harness, window, Some(low));
    window.run_until_parked();

    harness
        .state
        .update_in(window, |state, window, cx| state.open_at(low, window, cx));
    window.run_until_parked();

    let frame = window
        .debug_bounds("form-popover")
        .expect("the open frame is laid out");

    let expected = place_popover(
        low,
        frame.size,
        viewport,
        PopoverMetrics::ANCHOR_GAP,
        PopoverMetrics::WINDOW_MARGIN,
        PopoverSide::Below,
    );

    assert_eq!(
        expected.side,
        PopoverSide::Above,
        "an anchor with no room below it flips the surface"
    );
    // The anchored layer rounds its offset to whole pixels.
    assert!(
        (frame.origin.x - expected.origin.x).abs() <= px(1.0)
            && (frame.origin.y - expected.origin.y).abs() <= px(1.0),
        "the surface takes the flipped placement, not a snapped one: frame {frame:?}, expected {expected:?}, anchor {low:?}, viewport {viewport:?}"
    );
    assert!(
        frame.bottom() <= low.top(),
        "the flipped surface sits over its anchor: frame {frame:?}, anchor {low:?}"
    );
}

#[gpui::test]
fn a_popover_without_an_anchor_stays_where_the_host_renders_it(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    set_anchor(&harness, window, None);
    harness
        .state
        .update_in(window, |state, window, cx| state.open(window, cx));
    window.run_until_parked();

    let frame = window
        .debug_bounds("form-popover")
        .expect("the open frame is laid out");

    assert_eq!(frame.origin, point(px(0.0), px(0.0)));
}

// ---------------------------------------------------------------------------
// Focus and dismissal
// ---------------------------------------------------------------------------

#[gpui::test]
fn opening_moves_focus_in_and_closing_gives_it_back(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    window.update(|window, cx| harness.trigger.focus(window, cx));
    window.run_until_parked();

    open_at_anchor(&harness, window);

    assert_eq!(
        window.update(|window, cx| window.focused(cx).map(|handle| handle == harness.field)),
        Some(true),
        "focus moves to the field the popover was opened for"
    );

    window.simulate_keystrokes("escape");
    window.run_until_parked();

    assert!(
        !is_open(&harness.state, window),
        "Escape closes the popover"
    );
    assert_eq!(
        window.update(|window, cx| window.focused(cx).map(|handle| handle == harness.trigger)),
        Some(true),
        "closing gives focus back to the trigger"
    );
    assert_eq!(*harness.dismissed.borrow(), 1);
}

#[gpui::test]
fn a_click_outside_closes_it_and_a_click_on_it_does_not(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    open_at_anchor(&harness, window);

    let body = window
        .debug_bounds("popover-body")
        .expect("the body is laid out");
    click(window, body.center());
    assert!(
        is_open(&harness.state, window),
        "a click inside the surface keeps it open"
    );

    click(window, point(px(700.0), px(560.0)));
    assert!(
        !is_open(&harness.state, window),
        "a click away dismisses the surface"
    );
}

#[gpui::test]
fn a_click_in_a_dropdown_list_opened_inside_keeps_the_popover_open(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    open_at_anchor(&harness, window);

    harness
        .dropdown
        .update_in(window, |dropdown, _window, cx| dropdown.open(cx));
    window.run_until_parked();

    let list = window
        .debug_bounds("popover-target-menu")
        .expect("the dropdown list is drawn above the window");

    // The last row of the list, which the layer draws past the popover's own
    // rectangle: the list pads its rows by `Fields::MENU_PADDING_Y` and a row is
    // `Fields::MENU_ROW_HEIGHT` tall.
    let rem_size = window.update(|window, _| window.rem_size());
    let row_height = Fields::MENU_ROW_HEIGHT.to_pixels(rem_size);
    let frame = window
        .debug_bounds("form-popover")
        .expect("the open frame is laid out");
    let row = point(
        list.origin.x + list.size.width / 2.0,
        list.origin.y + Fields::MENU_PADDING_Y + row_height * 3.5,
    );

    assert!(
        !frame.contains(&row),
        "the row is drawn outside the popover's own rectangle: row {row:?}, frame {frame:?}, list {list:?}"
    );

    click(window, row);

    assert!(
        is_open(&harness.state, window),
        "a click in a dropdown list opened from inside is a click inside"
    );
    assert_eq!(
        window.update(|_window, cx| harness.dropdown.read(cx).selected()),
        Some(3),
        "the row the click landed on is the row the dropdown selected"
    );
}

// ---------------------------------------------------------------------------
// Hover intent
// ---------------------------------------------------------------------------

#[gpui::test]
fn resting_on_the_anchor_opens_after_the_delay(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    harness.state.update_in(window, |state, window, cx| {
        state.hover_anchor(true, window, cx)
    });
    advance(window, HOVER_DELAY - Duration::from_millis(1));

    assert!(
        !is_open(&harness.state, window),
        "not yet: the delay has not elapsed"
    );

    advance(window, Duration::from_millis(1));

    assert!(
        is_open(&harness.state, window),
        "the hover opens after the delay"
    );
}

#[gpui::test]
fn a_pointer_that_leaves_the_anchor_before_the_delay_never_opens(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    harness.state.update_in(window, |state, window, cx| {
        state.hover_anchor(true, window, cx)
    });
    advance(window, Duration::from_millis(200));
    harness.state.update_in(window, |state, window, cx| {
        state.hover_anchor(false, window, cx)
    });
    advance(window, HOVER_DELAY * 2);

    assert!(!is_open(&harness.state, window));
}

#[gpui::test]
fn the_pointer_over_the_surface_holds_it_open(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    harness.state.update_in(window, |state, window, cx| {
        state.hover_anchor(true, window, cx)
    });
    advance(window, HOVER_DELAY);

    let body = window
        .debug_bounds("popover-body")
        .expect("the body is laid out");
    window.simulate_mouse_move(body.center(), None, Modifiers::default());
    advance(window, HOVER_GRACE * 4);

    assert!(
        is_open(&harness.state, window),
        "the surface stays open while the pointer is over it"
    );
}

#[gpui::test]
fn the_surface_closes_after_the_grace_when_the_pointer_leaves_both(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    harness.state.update_in(window, |state, window, cx| {
        state.hover_anchor(true, window, cx)
    });
    advance(window, HOVER_DELAY);
    assert!(is_open(&harness.state, window));

    window.simulate_mouse_move(point(px(700.0), px(560.0)), None, Modifiers::default());
    advance(window, HOVER_GRACE - Duration::from_millis(1));
    assert!(
        is_open(&harness.state, window),
        "the grace keeps it open for a moment"
    );

    advance(window, Duration::from_millis(1));
    assert!(!is_open(&harness.state, window), "then it closes");
}

/// A popover that was opened by a hover does not reopen on its own after it is
/// dismissed while the pointer still rests on the anchor.
#[gpui::test]
fn a_dismissed_hover_popover_waits_for_the_pointer_to_leave(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    harness.state.update_in(window, |state, window, cx| {
        state.hover_anchor(true, window, cx)
    });
    advance(window, HOVER_DELAY);
    assert!(is_open(&harness.state, window));

    window.simulate_keystrokes("escape");
    window.run_until_parked();
    advance(window, HOVER_DELAY * 2);

    assert!(
        !is_open(&harness.state, window),
        "the pointer has to leave the anchor and come back"
    );
}
