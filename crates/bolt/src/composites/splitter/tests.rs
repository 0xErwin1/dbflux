use std::cell::RefCell;
use std::rc::Rc;

use gpui::{
    Context, FocusHandle, IntoElement, Modifiers, MouseButton, MouseDownEvent, MouseUpEvent,
    ParentElement as _, Pixels, Render, Styled as _, TestAppContext, VisualTestContext, Window,
    div, point, px,
};

use super::{SplitAxis, Splitter};
use crate::tokens::SplitterMetrics;

fn named_splitter(_cx: &mut gpui::App) -> gpui::AnyElement {
    div()
        .flex()
        .w(px(400.0))
        .h(px(200.0))
        .child(
            Splitter::new("split", SplitAxis::Horizontal)
                .aria_label("Resize request and response")
                .value_percent(62.4),
        )
        .into_any_element()
}

#[gpui::test]
fn the_gutter_reads_its_name_and_value_in_percent(cx: &mut TestAppContext) {
    use crate::accessibility::{accessibility_node, accessible_name, render_frame};

    let frame = render_frame(cx, named_splitter);

    let node = accessibility_node(&frame, "split").expect("splitter node");
    assert_eq!(node.role(), gpui::Role::Splitter);
    assert_eq!(
        accessible_name(&frame, "split").as_deref(),
        Some("Resize request and response")
    );
    assert_eq!(node.value(), Some("62%"));
    assert_eq!(node.numeric_value(), Some(62.4));
    assert_eq!(node.min_numeric_value(), Some(0.0));
    assert_eq!(node.max_numeric_value(), Some(100.0));
}

#[derive(Debug, Clone, PartialEq)]
enum Seen {
    Drag(Pixels),
    Step(Pixels),
    Toggle,
}

struct Host {
    axis: SplitAxis,
    focus_handle: FocusHandle,
    seen: Rc<RefCell<Vec<Seen>>>,
}

impl Render for Host {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        let drags = self.seen.clone();
        let steps = self.seen.clone();
        let toggles = self.seen.clone();

        let splitter = Splitter::new("split", self.axis)
            .track_focus(&self.focus_handle)
            .on_drag(move |position, _, _| drags.borrow_mut().push(Seen::Drag(position)))
            .on_step(move |step, _, _| steps.borrow_mut().push(Seen::Step(step)))
            .on_toggle_collapse(move |_, _| toggles.borrow_mut().push(Seen::Toggle));

        let row = match self.axis {
            SplitAxis::Horizontal => div().flex().flex_row().w(px(400.0)).h(px(200.0)),
            SplitAxis::Vertical => div().flex().flex_col().w(px(400.0)).h(px(200.0)),
        };

        div().size_full().child(
            row.child(div().flex_1().child("left"))
                .child(splitter)
                .child(div().flex_1().child("right")),
        )
    }
}

fn open(
    axis: SplitAxis,
    cx: &mut TestAppContext,
) -> (Rc<RefCell<Vec<Seen>>>, FocusHandle, &mut VisualTestContext) {
    cx.update(gpui_component::init);
    cx.update(|cx| {
        crate::theme::install(
            crate::brand::Brand::neutral(),
            crate::palette::PaletteMode::Dark,
            crate::density::Density::Default,
            cx,
        );
    });

    let seen = Rc::new(RefCell::new(Vec::new()));
    let (view, window) = cx.add_window_view({
        let seen = seen.clone();
        move |_, cx| Host {
            axis,
            focus_handle: cx.focus_handle(),
            seen,
        }
    });
    let focus_handle = window.update(|_, cx| view.read(cx).focus_handle.clone());
    window.run_until_parked();
    (seen, focus_handle, window)
}

#[gpui::test]
fn the_gutter_spans_the_cross_axis_and_draws_its_grip(cx: &mut TestAppContext) {
    let (_seen, _focus, window) = open(SplitAxis::Horizontal, cx);

    let gutter = window
        .debug_bounds("split")
        .expect("the gutter is laid out");
    assert_eq!(gutter.size.width, SplitterMetrics::GUTTER);
    assert_eq!(gutter.size.height, px(200.0));

    let grip = window
        .debug_bounds("split-grip")
        .expect("the grip is drawn");
    assert!(gutter.contains(&grip.center()));
    assert_eq!(grip.size.height, SplitterMetrics::GRIP_LENGTH);

    let (_seen, _focus, window) = open(SplitAxis::Vertical, cx);
    let gutter = window
        .debug_bounds("split")
        .expect("the gutter is laid out");
    assert_eq!(gutter.size.height, SplitterMetrics::GUTTER);
    assert_eq!(gutter.size.width, px(400.0));
}

#[gpui::test]
fn dragging_reports_the_pointer_along_the_axis(cx: &mut TestAppContext) {
    let (seen, _focus, window) = open(SplitAxis::Horizontal, cx);
    let gutter = window
        .debug_bounds("split")
        .expect("the gutter is laid out");
    let start = gutter.center();

    window.simulate_mouse_down(start, MouseButton::Left, Modifiers::default());
    window.simulate_mouse_move(
        point(start.x + px(20.0), start.y),
        MouseButton::Left,
        Modifiers::default(),
    );
    window.simulate_mouse_move(
        point(start.x + px(40.0), start.y + px(5.0)),
        MouseButton::Left,
        Modifiers::default(),
    );
    window.simulate_mouse_up(
        point(start.x + px(40.0), start.y + px(5.0)),
        MouseButton::Left,
        Modifiers::default(),
    );
    window.run_until_parked();

    let drags: Vec<Seen> = seen
        .borrow()
        .iter()
        .filter(|seen| matches!(seen, Seen::Drag(_)))
        .cloned()
        .collect();
    assert_eq!(drags.last(), Some(&Seen::Drag(start.x + px(40.0))));
}

#[gpui::test]
fn arrow_keys_step_and_enter_toggles_while_focused(cx: &mut TestAppContext) {
    let (seen, focus, window) = open(SplitAxis::Horizontal, cx);
    window.update(|window, cx| focus.focus(window, cx));
    window.run_until_parked();

    window.simulate_keystrokes("right left down enter");

    assert_eq!(
        *seen.borrow(),
        vec![
            Seen::Step(SplitterMetrics::KEY_STEP),
            Seen::Step(-SplitterMetrics::KEY_STEP),
            Seen::Toggle,
        ],
        "a side-by-side split moves with Left and Right only"
    );

    let (seen, focus, window) = open(SplitAxis::Vertical, cx);
    window.update(|window, cx| focus.focus(window, cx));
    window.run_until_parked();

    window.simulate_keystrokes("down up right");

    assert_eq!(
        *seen.borrow(),
        vec![
            Seen::Step(SplitterMetrics::KEY_STEP),
            Seen::Step(-SplitterMetrics::KEY_STEP),
        ]
    );
}

#[gpui::test]
fn a_double_click_toggles_and_a_single_click_does_not(cx: &mut TestAppContext) {
    let (seen, _focus, window) = open(SplitAxis::Horizontal, cx);
    let center = window
        .debug_bounds("split")
        .expect("the gutter is laid out")
        .center();

    window.simulate_click(center, Modifiers::default());
    window.run_until_parked();
    assert!(!seen.borrow().contains(&Seen::Toggle));

    for click_count in [1, 2] {
        window.simulate_event(MouseDownEvent {
            button: MouseButton::Left,
            position: center,
            modifiers: Modifiers::default(),
            click_count,
            first_mouse: false,
        });
        window.simulate_event(MouseUpEvent {
            button: MouseButton::Left,
            position: center,
            modifiers: Modifiers::default(),
            click_count,
        });
    }
    window.run_until_parked();

    assert_eq!(
        seen.borrow()
            .iter()
            .filter(|seen| **seen == Seen::Toggle)
            .count(),
        1
    );
}
