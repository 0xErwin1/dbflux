use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

use gpui::{
    AppContext as _, Context, Entity, IntoElement, Modifiers, ParentElement as _, Render, Role,
    Styled as _, TestAppContext, VisualTestContext, Window, div, px,
};

use super::SelectTrigger;
use crate::controls::{Dropdown, DropdownItem};
use crate::icon::IconSource;

#[derive(Default)]
struct FrameCapture(std::sync::Mutex<Option<gpui::AccessibilityFrame>>);

impl gpui::FrameObserver for FrameCapture {
    fn accessibility_updated(&self, frame: &gpui::AccessibilityFrame) {
        *self.0.lock().expect("frame capture lock") = Some(frame.clone());
    }
}

struct Host {
    dropdown: Entity<Dropdown>,
    clicks: Rc<Cell<usize>>,
}

impl Render for Host {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        let clicks = self.clicks.clone();
        let icon = IconSource::Svg("icons/folder.svg".into());

        div()
            .size_full()
            .flex()
            .flex_col()
            .child(
                div().w(px(200.0)).child(
                    SelectTrigger::new("pick", "One")
                        .leading_icon(icon)
                        .on_click(move |_, _, _| clicks.set(clicks.get() + 1)),
                ),
            )
            .child(div().w(px(200.0)).child(self.dropdown.clone()))
            .child(
                div()
                    .w(px(200.0))
                    .child(SelectTrigger::new("pick-disabled", "Off").disabled(true)),
            )
    }
}

fn open(cx: &mut TestAppContext) -> (Rc<Cell<usize>>, Arc<FrameCapture>, &mut VisualTestContext) {
    cx.update(gpui_component::init);
    cx.update(|cx| {
        crate::theme::install(
            crate::brand::Brand::neutral(),
            crate::palette::PaletteMode::Dark,
            crate::density::Density::Default,
            cx,
        );
    });

    let clicks = Rc::new(Cell::new(0));
    let capture = Arc::new(FrameCapture::default());
    let (_view, window) = cx.add_window_view({
        let clicks = clicks.clone();
        let capture = capture.clone();
        move |window, cx| {
            window.observe_frames(&capture);
            window.refresh();
            Host {
                dropdown: cx.new(|_| {
                    Dropdown::new("drop")
                        .items(vec![DropdownItem::new("One")])
                        .selected_index(Some(0))
                        .leading_icon(IconSource::Svg("icons/folder.svg".into()))
                }),
                clicks,
            }
        }
    });
    window.run_until_parked();
    (clicks, capture, window)
}

#[gpui::test]
fn the_trigger_is_drawn_like_a_dropdown_trigger(cx: &mut TestAppContext) {
    let (_clicks, _capture, window) = open(cx);

    let trigger = window
        .debug_bounds("pick")
        .expect("the trigger is laid out");
    let dropdown = window
        .debug_bounds("drop")
        .expect("the dropdown is laid out");

    assert_eq!(trigger.size, dropdown.size);
}

#[gpui::test]
fn clicking_the_trigger_runs_its_handler_and_a_disabled_one_does_not(cx: &mut TestAppContext) {
    let (clicks, _capture, window) = open(cx);

    let trigger = window
        .debug_bounds("pick")
        .expect("the trigger is laid out");
    window.simulate_click(trigger.center(), Modifiers::default());
    window.run_until_parked();
    assert_eq!(clicks.get(), 1);

    let disabled = window
        .debug_bounds("pick-disabled")
        .expect("the disabled trigger is laid out");
    window.simulate_click(disabled.center(), Modifiers::default());
    window.run_until_parked();
    assert_eq!(clicks.get(), 1);
}

#[gpui::test]
fn the_trigger_is_a_button_named_by_its_label(cx: &mut TestAppContext) {
    let (_clicks, capture, _window) = open(cx);

    let frame = capture
        .0
        .lock()
        .expect("frame capture lock")
        .clone()
        .expect("the window rendered a frame");
    let (_, node) = frame
        .nodes()
        .find(|(_, node)| node.id() == "pick")
        .expect("the trigger is in the frame");
    let node = frame
        .accessibility_node(node)
        .expect("the trigger is accessible");

    assert_eq!(node.role(), Role::Button);
    assert_eq!(node.label(), Some("One"));
}
