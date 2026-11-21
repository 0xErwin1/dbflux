// Explicit imports rather than the parent glob: combining `use super::*`
// with `#[gpui::test]` sends the gpui_macros expansion into unbounded
// recursion.
use super::{
    CONFIRM_CANCEL_ID, CONFIRM_CONFIRM_ID, ConfirmOutcome, ConfirmRequest, ModalConfirm,
    ModalVariant,
};
use crate::modals::modal::bind_modal_keys_for_tests;
use gpui::{
    AccessibilityFrame, AppContext as _, Bounds, Context, Entity, FocusHandle, FrameObserver,
    InteractiveElement as _, IntoElement, Modifiers, ParentElement as _, Pixels, Render,
    Styled as _, TestAppContext, VisualTestContext, Window, div,
};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

/// Keeps the latest rendered frame of the window it observes.
#[derive(Default)]
struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

impl FrameObserver for FrameCapture {
    fn accessibility_updated(&self, frame: &AccessibilityFrame) {
        *self.0.lock().expect("frame capture lock") = Some(frame.clone());
    }
}

impl FrameCapture {
    fn bounds_of(&self, id: &str) -> Option<Bounds<Pixels>> {
        let frame = self.0.lock().expect("frame capture lock").clone()?;
        frame
            .nodes()
            .find(|(_, node)| node.id() == id)
            .map(|(_, node)| node.bounds())
    }
}

type Log = Rc<RefCell<Vec<ConfirmOutcome>>>;

/// Hosts the confirmation over a focusable shell, so focus hand-back has a
/// place to return to.
struct Host {
    modal: Entity<ModalConfirm>,
    focus: FocusHandle,
}

impl Render for Host {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .track_focus(&self.focus)
            .child(self.modal.clone())
    }
}

fn danger_request() -> ConfirmRequest {
    ConfirmRequest {
        title: "Reset the installation id?".to_string(),
        body: "A new random id replaces the current one.".to_string(),
        variant: ModalVariant::Danger,
        confirm_label: "Reset id".to_string(),
    }
}

/// Builds a host window with a subscribed confirmation, focuses the shell,
/// then opens the confirmation.
fn setup(
    cx: &mut TestAppContext,
) -> (
    Entity<Host>,
    Entity<ModalConfirm>,
    Arc<FrameCapture>,
    Log,
    &mut VisualTestContext,
) {
    bind_modal_keys_for_tests(cx);
    cx.update(|cx| {
        gpui_component::init(cx);
        crate::theme::init(cx);
    });

    let capture = Arc::new(FrameCapture::default());
    let log: Log = Rc::new(RefCell::new(Vec::new()));

    let slot = Rc::new(RefCell::new(None));
    let (host, window) = cx.add_window_view({
        let slot = slot.clone();
        let capture_for_view = capture.clone();
        move |window, cx| {
            window.observe_frames(&capture_for_view);
            let modal = cx.new(ModalConfirm::new);
            slot.borrow_mut().replace(modal.clone());
            Host {
                modal,
                focus: cx.focus_handle(),
            }
        }
    });
    window.run_until_parked();

    let modal = slot.borrow().clone().expect("confirmation built");

    // The shell holds focus before the confirmation opens, so the modal
    // records a real focus to hand back to. The outcome subscription is
    // attached on `App`, where the house three-argument closure applies.
    let sink = log.clone();
    window.update(|window, cx| {
        cx.subscribe(&modal, move |_, outcome: &ConfirmOutcome, _| {
            sink.borrow_mut().push(*outcome);
        })
        .detach();

        let focus = host.read(cx).focus.clone();
        focus.focus(window, cx);
    });
    window.run_until_parked();
    window.update(|_, cx| {
        modal.update(cx, |confirm, cx| confirm.open(danger_request(), cx));
    });
    window.run_until_parked();

    (host, modal, capture, log, window)
}

fn events(log: &Log) -> Vec<ConfirmOutcome> {
    log.borrow().clone()
}

#[gpui::test]
fn both_buttons_render_and_the_cancel_button_cancels(cx: &mut TestAppContext) {
    let (_host, modal, capture, log, window) = setup(cx);

    let cancel = capture
        .bounds_of(CONFIRM_CANCEL_ID)
        .expect("the confirmation renders a Cancel button");
    let confirm = capture
        .bounds_of(CONFIRM_CONFIRM_ID)
        .expect("the confirmation renders a primary button");
    assert_ne!(cancel, confirm, "the two buttons are distinct elements");

    window.simulate_click(cancel.center(), Modifiers::default());
    window.run_until_parked();

    assert_eq!(events(&log), [ConfirmOutcome::Cancelled]);
    assert!(
        !window.update(|_, cx| modal.read(cx).is_visible()),
        "cancelling closes the confirmation"
    );
}

#[gpui::test]
fn the_confirm_button_confirms(cx: &mut TestAppContext) {
    let (_host, modal, capture, log, window) = setup(cx);

    let confirm = capture
        .bounds_of(CONFIRM_CONFIRM_ID)
        .expect("the confirmation renders a primary button");
    window.simulate_click(confirm.center(), Modifiers::default());
    window.run_until_parked();

    assert_eq!(events(&log), [ConfirmOutcome::Confirmed]);
    assert!(
        !window.update(|_, cx| modal.read(cx).is_visible()),
        "confirming closes the confirmation"
    );
}

#[gpui::test]
fn resolving_an_already_closed_confirmation_emits_once(cx: &mut TestAppContext) {
    let (_host, modal, _capture, log, window) = setup(cx);

    window.update(|_, cx| {
        modal.update(cx, |confirm, cx| {
            confirm.confirm(cx);
            // A second Enter and a late Cancel click after the card is gone
            // must not resolve the same confirmation twice.
            confirm.confirm(cx);
            confirm.cancel(cx);
        });
    });
    window.run_until_parked();

    assert_eq!(events(&log), [ConfirmOutcome::Confirmed]);
}

#[gpui::test]
fn current_body_tracks_the_open_request(cx: &mut TestAppContext) {
    let confirm = cx.new(ModalConfirm::new);

    assert_eq!(
        cx.update(|cx| confirm.read(cx).current_body().map(str::to_string)),
        None,
        "nothing opened yet"
    );

    cx.update(|cx| {
        confirm.update(cx, |modal, cx| modal.open(danger_request(), cx));
    });
    assert_eq!(
        cx.update(|cx| confirm.read(cx).current_body().map(str::to_string)),
        Some(danger_request().body),
        "the open request is visible to the host"
    );

    cx.update(|cx| {
        confirm.update(cx, |modal, cx| modal.cancel(cx));
    });
    assert_eq!(
        cx.update(|cx| confirm.read(cx).current_body().map(str::to_string)),
        None,
        "closing clears the request"
    );
}

#[gpui::test]
fn escape_cancels(cx: &mut TestAppContext) {
    let (_host, modal, _capture, log, window) = setup(cx);

    window.simulate_keystrokes("escape");
    window.run_until_parked();

    assert_eq!(events(&log), [ConfirmOutcome::Cancelled]);
    assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
}

#[gpui::test]
fn opening_moves_focus_into_the_confirmation(cx: &mut TestAppContext) {
    let (_host, modal, _capture, _log, window) = setup(cx);

    let focused = window.update(|window, cx| modal.read(cx).contains_focus(window, cx));
    assert!(focused, "opening the confirmation moves focus into it");
}

#[gpui::test]
fn cancelling_gives_focus_back_to_what_had_it(cx: &mut TestAppContext) {
    let (host, modal, _capture, _log, window) = setup(cx);

    window.update(|_, cx| {
        modal.update(cx, |confirm, cx| confirm.cancel(cx));
    });
    window.run_until_parked();

    let restored = window.update(|window, cx| host.read(cx).focus.contains_focused(window, cx));
    assert!(restored, "focus returns to what held it before the open");
}

#[gpui::test]
fn the_callers_labels_are_shown(cx: &mut TestAppContext) {
    let (_host, _modal, capture, _log, _window) = setup(cx);

    // The primary button carries the caller's label, not a shared one.
    let frame = capture.0.lock().expect("frame capture lock").clone();
    let frame = frame.expect("a frame was rendered");
    let caller_label_rendered = frame
        .nodes()
        .any(|(_, node)| node.content_text().contains("Reset id"));
    assert!(
        caller_label_rendered,
        "the caller's primary label reaches the rendered button"
    );
}

#[test]
fn confirm_shared_labels_resolve_in_every_locale() {
    for key in ["modals.confirm.cancel", "modals.confirm.confirm"] {
        for locale in ["en", "es", "ko", "zh_Hans"] {
            let value = dbflux_i18n::t!(key, locale = locale);
            assert!(!value.is_empty(), "{key} resolved empty for {locale}");
            assert_ne!(
                value,
                format!("{locale}.{key}"),
                "{key} missing in {locale}"
            );
        }
    }
}

#[test]
fn confirm_shared_labels_are_translated_outside_english() {
    for key in ["modals.confirm.cancel", "modals.confirm.confirm"] {
        let en = dbflux_i18n::t!(key, locale = "en");
        for locale in ["es", "ko", "zh_Hans"] {
            assert_ne!(
                dbflux_i18n::t!(key, locale = locale),
                en,
                "English fallback for {key} in {locale}"
            );
        }
    }
}
