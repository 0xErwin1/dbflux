//! Whether a platform file drag would be taken by the element under it, which the X11 backend
//! reports in `XdndStatus`. These live here because `gpui-pre`'s own test suite does not build
//! outside Zed's tree; `cargo test -p gpui-pre-linux --lib` runs them on GPUI's test platform.

use std::{cell::RefCell, path::PathBuf, rc::Rc};

use gpui::{
    AnyWindowHandle, AppContext as _, Context, DispatchEventResult, ExternalPaths, FileDropEvent,
    InputEvent as _, InteractiveElement as _, IntoElement, MouseMoveEvent, ParentElement as _,
    Pixels, Point, Render, Styled as _, TestAppContext, Window, div, point, px,
};

type Seen = Rc<RefCell<Vec<Vec<PathBuf>>>>;

/// Left half takes file drops, right half is plain, bottom left refuses them in `can_drop`, and
/// the bottom right takes them in a child nested in a refusing parent.
struct DropTargets {
    asked: Seen,
}

impl Render for DropTargets {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        let asked = self.asked.clone();
        div()
            .size_full()
            .child(
                div()
                    .absolute()
                    .left(px(0.))
                    .top(px(0.))
                    .w(px(100.))
                    .h(px(100.))
                    .on_drop::<ExternalPaths>(|_, _, _| {})
                    .can_drop(move |value, _, _| {
                        if let Some(paths) = value.downcast_ref::<ExternalPaths>() {
                            asked.borrow_mut().push(paths.paths().to_vec());
                        }
                        true
                    }),
            )
            .child(
                div()
                    .absolute()
                    .left(px(0.))
                    .top(px(200.))
                    .w(px(100.))
                    .h(px(100.))
                    .on_drop::<ExternalPaths>(|_, _, _| {})
                    .can_drop(|_, _, _| false),
            )
            .child(
                div()
                    .absolute()
                    .left(px(300.))
                    .top(px(200.))
                    .w(px(100.))
                    .h(px(100.))
                    .on_drop::<ExternalPaths>(|_, _, _| {})
                    .can_drop(|_, _, _| false)
                    .child(
                        div()
                            .size_full()
                            .on_drop::<ExternalPaths>(|_, _, _| {})
                            .can_drop(|_, _, _| true),
                    ),
            )
            .child(
                div()
                    .absolute()
                    .left(px(500.))
                    .top(px(200.))
                    .w(px(100.))
                    .h(px(100.))
                    .on_drop::<String>(|_, _, _| {}),
            )
    }
}

const ACCEPTING: Point<Pixels> = point(px(50.), px(50.));
const PLAIN: Point<Pixels> = point(px(700.), px(50.));
const REFUSING: Point<Pixels> = point(px(50.), px(250.));
const NESTED: Point<Pixels> = point(px(350.), px(250.));
const OTHER_TYPE: Point<Pixels> = point(px(550.), px(250.));

fn paths() -> ExternalPaths {
    ExternalPaths([PathBuf::from("/tmp/dragged")].into_iter().collect())
}

fn window(cx: &mut TestAppContext) -> (AnyWindowHandle, Seen) {
    let asked = Seen::default();
    let window: AnyWindowHandle = cx
        .add_window({
            let asked = asked.clone();
            move |_, _| DropTargets { asked }
        })
        .into();
    (window, asked)
}

fn dispatch(
    cx: &mut TestAppContext,
    window: AnyWindowHandle,
    event: impl FnOnce() -> gpui::PlatformInput,
) -> DispatchEventResult {
    cx.update_window(window, |_, window, cx| {
        window.draw(cx).clear(cx);
        window.dispatch_event(event(), cx)
    })
    .expect("the window is open")
}

fn entered(position: Point<Pixels>) -> gpui::PlatformInput {
    FileDropEvent::Entered {
        position,
        paths: paths(),
    }
    .to_platform_input()
}

fn pending(position: Point<Pixels>) -> gpui::PlatformInput {
    FileDropEvent::Pending { position }.to_platform_input()
}

#[gpui::test]
fn an_element_with_an_external_paths_drop_listener_accepts(cx: &mut TestAppContext) {
    let (window, _) = window(cx);
    let result = dispatch(cx, window, || entered(ACCEPTING));
    assert_eq!(result.external_drop_accepted, Some(true));
}

#[gpui::test]
fn the_predicate_sees_the_dragged_paths(cx: &mut TestAppContext) {
    let (window, asked) = window(cx);
    dispatch(cx, window, || entered(ACCEPTING));
    assert_eq!(
        asked.borrow().last(),
        Some(&vec![PathBuf::from("/tmp/dragged")])
    );
}

#[gpui::test]
fn an_element_without_a_drop_listener_refuses(cx: &mut TestAppContext) {
    let (window, _) = window(cx);
    let result = dispatch(cx, window, || entered(PLAIN));
    assert_eq!(result.external_drop_accepted, Some(false));
}

#[gpui::test]
fn can_drop_returning_false_refuses(cx: &mut TestAppContext) {
    let (window, _) = window(cx);
    let result = dispatch(cx, window, || entered(REFUSING));
    assert_eq!(result.external_drop_accepted, Some(false));
}

#[gpui::test]
fn a_listener_for_another_drag_type_refuses(cx: &mut TestAppContext) {
    let (window, _) = window(cx);
    let result = dispatch(cx, window, || entered(OTHER_TYPE));
    assert_eq!(result.external_drop_accepted, Some(false));
}

#[gpui::test]
fn the_innermost_listener_decides_like_a_real_drop(cx: &mut TestAppContext) {
    let (window, _) = window(cx);
    let result = dispatch(cx, window, || entered(NESTED));
    assert_eq!(result.external_drop_accepted, Some(true));
}

#[gpui::test]
fn moving_over_the_window_updates_the_answer(cx: &mut TestAppContext) {
    let (window, _) = window(cx);
    assert_eq!(
        dispatch(cx, window, || entered(PLAIN)).external_drop_accepted,
        Some(false)
    );
    assert_eq!(
        dispatch(cx, window, || pending(ACCEPTING)).external_drop_accepted,
        Some(true)
    );
    assert_eq!(
        dispatch(cx, window, || pending(REFUSING)).external_drop_accepted,
        Some(false)
    );
}

#[gpui::test]
fn ordinary_pointer_events_report_no_answer(cx: &mut TestAppContext) {
    let (window, _) = window(cx);
    let result = dispatch(cx, window, || {
        MouseMoveEvent {
            position: ACCEPTING,
            pressed_button: None,
            modifiers: Default::default(),
        }
        .to_platform_input()
    });
    assert_eq!(result.external_drop_accepted, None);
}
