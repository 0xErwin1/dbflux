//! `KeyCapture` — a field-shaped control that catches the next chord.
//!
//! A host that lets users rebind a command puts its row into capturing
//! state: the control shows the host's own hint and the next chord the user
//! presses is reported to the host instead of reaching the app's keymap.
//! A bare Escape cancels; a modified Escape is a chord like any other, and
//! a bare modifier keeps the capture listening.

use std::rc::Rc;

use gpui::prelude::*;
use gpui::{
    App, Context, ElementId, Keystroke, Render, Role, SharedString, Subscription, Window, div,
};
use gpui_component::ActiveTheme;

use crate::primitives::{Chamfer, ChamferRing};
use crate::selector::id_selector;
use crate::tokens::{ChamferCut, ChromeColors, Fields};

/// The handler a host supplies to [`KeyCapture::start`]: it is invoked once
/// with the captured chord and the string a config file can store — the same
/// syntax [`Keystroke::parse`] and `KeyBinding::new` take (`ctrl-alt-k`).
pub type KeyCaptureHandler = Rc<dyn Fn(&Keystroke, SharedString, &mut App)>;

/// A control that catches the next chord for a host that rebinds keys.
///
/// Keep it in an [`Entity`](gpui::Entity): [`KeyCapture::start`] opens the
/// capture (from a click on the row around it, for example) and
/// [`KeyCapture::stop`] cancels it. While capturing, the control shows the
/// host's hint; the first non-modifier key is reported to the handler and
/// never dispatched to the app's keymap. A bare Escape cancels instead of
/// being reported; a modified Escape (`ctrl-escape`) is a real chord and is
/// reported like any other.
pub struct KeyCapture {
    id: ElementId,
    label: SharedString,
    state: KeyCaptureState,
}

enum KeyCaptureState {
    Idle,
    Capturing {
        hint: SharedString,
        handler: KeyCaptureHandler,
        _interceptor: Subscription,
    },
}

impl KeyCapture {
    /// Starts an idle control under `id` showing `label` — the value the row
    /// currently holds, usually a chord string.
    pub fn new(id: impl Into<ElementId>, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            state: KeyCaptureState::Idle,
        }
    }

    /// The label shown while idle.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Replaces the idle label (the host showing a freshly captured chord).
    pub fn set_label(&mut self, label: impl Into<SharedString>, cx: &mut Context<Self>) {
        let label = label.into();
        if label != self.label {
            self.label = label;
            cx.notify();
        }
    }

    /// Whether the control is capturing the next chord.
    pub fn is_capturing(&self) -> bool {
        matches!(self.state, KeyCaptureState::Capturing { .. })
    }

    /// Opens the capture with the host's `hint` drawn in the field and
    /// `handler` waiting for the next chord. Capturing again replaces the
    /// previous capture.
    pub fn start(
        &mut self,
        hint: impl Into<SharedString>,
        handler: KeyCaptureHandler,
        cx: &mut Context<Self>,
    ) {
        self.stop(cx);
        let this = cx.weak_entity();
        let interceptor =
            cx.intercept_keystrokes(move |event: &gpui::KeystrokeEvent, _window, cx| {
                let keystroke = &event.keystroke;
                if is_modifier_key(&keystroke.key) {
                    // A bare modifier is not a chord yet: leave it to the
                    // app and keep listening.
                    return;
                }
                let Some(this) = this.upgrade() else {
                    return;
                };
                if keystroke.key == "escape" && !keystroke.modifiers.modified() {
                    // Only a bare Escape cancels: a modified Escape is a
                    // chord the host can rebind to.
                    cx.stop_propagation();
                    this.update(cx, |capture, cx| capture.stop(cx));
                } else {
                    let chord = SharedString::from(keystroke.unparse());
                    cx.stop_propagation();
                    let mut report = None;
                    this.update(cx, |capture, cx| {
                        if let KeyCaptureState::Capturing { handler, .. } = &capture.state {
                            report = Some(handler.clone());
                        }
                        capture.stop(cx);
                    });
                    if let Some(handler) = report {
                        handler(keystroke, chord, cx);
                    }
                }
            });
        self.state = KeyCaptureState::Capturing {
            hint: hint.into(),
            handler,
            _interceptor: interceptor,
        };
        cx.notify();
    }

    /// Cancels the capture; nothing is reported.
    pub fn stop(&mut self, cx: &mut Context<Self>) {
        if matches!(self.state, KeyCaptureState::Capturing { .. }) {
            self.state = KeyCaptureState::Idle;
            cx.notify();
        }
    }
}

/// The keys a pressed modifier reports before it is part of a chord. A
/// capture keeps listening when one arrives.
fn is_modifier_key(key: &str) -> bool {
    matches!(
        key,
        "shift"
            | "control"
            | "alt"
            | "platform"
            | "function"
            | "command"
            | "super"
            | "win"
            | "meta"
    )
}

impl Render for KeyCapture {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let (text, capturing) = match &self.state {
            KeyCaptureState::Idle => (self.label.clone(), false),
            KeyCaptureState::Capturing { hint, .. } => (hint.clone(), true),
        };

        let mut shape = Chamfer::new(ChamferCut::INPUT)
            .fill(theme.secondary)
            .border(theme.border);
        if capturing {
            shape = shape.ring(ChamferRing::focus(ChromeColors::tint(theme)));
        }

        div()
            .id(self.id.clone())
            .debug_selector(id_selector(&self.id))
            .role(Role::Label)
            .aria_label(text.clone())
            .flex()
            .items_center()
            .h(Fields::HEIGHT)
            .px(Fields::PADDING_X)
            .font_family(crate::fonts::ui_family(cx))
            .text_size(Fields::TEXT)
            .text_color(if capturing {
                theme.muted_foreground
            } else {
                theme.accent_foreground
            })
            .child(shape)
            .child(div().truncate().child(text))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    use gpui::{
        AccessibilityFrame, AppContext as _, Context, Entity, FrameObserver, IntoElement,
        Keystroke, ParentElement as _, Render, Styled as _, TestAppContext, VisualTestContext,
        Window, div,
    };

    use super::{KeyCapture, KeyCaptureHandler};

    /// The (keystroke, chord string) pairs a capture reported.
    type Reports = Rc<RefCell<Vec<(Keystroke, String)>>>;

    fn recording_handler(reports: &Reports) -> KeyCaptureHandler {
        let reports = reports.clone();
        Rc::new(move |keystroke, chord, _cx| {
            reports
                .borrow_mut()
                .push((keystroke.clone(), chord.to_string()));
        })
    }

    /// Keeps the latest rendered accessibility frame of the window it
    /// observes.
    #[derive(Default)]
    struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

    impl FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    struct Host {
        capture: Entity<KeyCapture>,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .size_full()
                .flex()
                .flex_col()
                .child(self.capture.clone())
        }
    }

    const LABEL: &str = "ctrl-alt-k";
    const HINT: &str = "listening… press the new keys, Esc cancels";

    /// Opens a window with one idle `KeyCapture` under `"rebind"`, and its
    /// accessibility frame capture.
    fn open(cx: &mut TestAppContext) -> (Entity<KeyCapture>, Arc<FrameCapture>, VisualTestContext) {
        cx.update(gpui_component::init);
        cx.update(|cx| {
            crate::theme::install(
                crate::brand::Brand::neutral(),
                crate::palette::PaletteMode::Dark,
                crate::density::Density::Default,
                cx,
            );
        });

        let capture = Arc::new(FrameCapture::default());
        let frame_capture = capture.clone();
        let (host, window) = cx.add_window_view(move |window, cx| {
            window.observe_frames(&frame_capture);
            window.refresh();
            Host {
                capture: cx.new(|_| KeyCapture::new("rebind", LABEL)),
            }
        });
        window.run_until_parked();
        let capture_entity = window.update(|_, cx| host.read(cx).capture.clone());
        (capture_entity, capture, window.clone())
    }

    /// Starts a capture that reports into `reports`.
    fn start_capturing(
        capture: &Entity<KeyCapture>,
        reports: &Reports,
        window: &mut VisualTestContext,
    ) {
        window.update(|_, cx| {
            capture.update(cx, |capture, cx| {
                capture.start(HINT, recording_handler(reports), cx);
            });
        });
    }

    /// The accessible name the control currently draws.
    fn drawn_text(capture: &Arc<FrameCapture>) -> String {
        let frame = capture
            .0
            .lock()
            .expect("frame capture lock")
            .clone()
            .expect("the window rendered a frame");
        let (_, node) = frame
            .nodes()
            .find(|(_, node)| node.id() == "rebind")
            .expect("the capture control is in the frame");
        frame
            .accessibility_node(node)
            .expect("the capture control is accessible")
            .label()
            .expect("the capture control names its text")
            .to_owned()
    }

    #[gpui::test]
    fn a_captured_chord_is_reported_with_its_string(cx: &mut TestAppContext) {
        let (capture, _frames, mut window) = open(cx);
        let reports = Reports::default();
        start_capturing(&capture, &reports, &mut window);

        window.simulate_keystrokes("ctrl-alt-k");

        let reported = reports.borrow();
        let (keystroke, chord) = reported
            .first()
            .expect("the pressed chord was reported once")
            .clone();
        assert_eq!(reported.len(), 1);
        assert_eq!(chord, "ctrl-alt-k");
        assert_eq!(keystroke.key, "k");
        assert!(keystroke.modifiers.control && keystroke.modifiers.alt);
        assert!(!keystroke.modifiers.shift);
        drop(reported);

        assert!(
            !capture.read_with(cx, |capture, _| capture.is_capturing()),
            "the capture ends when a chord is reported"
        );

        reports.borrow_mut().clear();

        window.simulate_keystrokes("b");
        assert!(
            reports.borrow().is_empty(),
            "a closed capture reports nothing more"
        );
    }

    #[gpui::test]
    fn escape_cancels_without_reporting(cx: &mut TestAppContext) {
        let (capture, _frames, mut window) = open(cx);
        let reports = Reports::default();
        start_capturing(&capture, &reports, &mut window);

        window.simulate_keystrokes("escape");

        assert!(
            !capture.read_with(cx, |capture, _| capture.is_capturing()),
            "escape closes the capture"
        );
        assert!(
            reports.borrow().is_empty(),
            "a cancelled capture reports nothing"
        );

        window.simulate_keystrokes("b");
        assert!(
            reports.borrow().is_empty(),
            "a cancelled capture is really closed"
        );
    }

    #[gpui::test]
    fn a_modified_escape_is_reported_as_a_chord(cx: &mut TestAppContext) {
        let (capture, _frames, mut window) = open(cx);
        let reports = Reports::default();
        start_capturing(&capture, &reports, &mut window);

        window.simulate_keystrokes("ctrl-escape");

        let reported = reports.borrow();
        let (keystroke, chord) = reported
            .first()
            .expect("the modified escape was reported once")
            .clone();
        assert_eq!(reported.len(), 1);
        assert_eq!(chord, "ctrl-escape");
        assert_eq!(keystroke.key, "escape");
        assert!(keystroke.modifiers.control);
        drop(reported);

        assert!(
            !capture.read_with(cx, |capture, _| capture.is_capturing()),
            "reporting a chord closes the capture"
        );
    }

    #[gpui::test]
    fn a_modifier_only_press_keeps_listening(cx: &mut TestAppContext) {
        let (capture, _frames, mut window) = open(cx);
        let reports = Reports::default();
        start_capturing(&capture, &reports, &mut window);

        window.simulate_keystrokes("shift");

        assert!(
            reports.borrow().is_empty(),
            "a bare modifier is not a chord"
        );
        assert!(
            capture.read_with(cx, |capture, _| capture.is_capturing()),
            "a bare modifier keeps the capture open"
        );

        window.simulate_keystrokes("ctrl-m");
        assert_eq!(reports.borrow().len(), 1, "the capture is still listening");
    }

    #[gpui::test]
    fn an_idle_control_renders_its_label_and_no_capture_hint(cx: &mut TestAppContext) {
        let (capture, frames, _window) = open(cx);

        assert!(!capture.read_with(cx, |capture, _| capture.is_capturing()));
        assert_eq!(drawn_text(&frames), LABEL);
        assert_ne!(drawn_text(&frames), HINT, "an idle control shows no hint");
    }

    #[gpui::test]
    fn a_capturing_control_renders_the_callers_hint(cx: &mut TestAppContext) {
        let (capture, frames, mut window) = open(cx);
        let reports = Reports::default();
        start_capturing(&capture, &reports, &mut window);
        window.run_until_parked();

        assert_eq!(
            drawn_text(&frames),
            HINT,
            "the hint is the caller's, drawn while capturing"
        );
    }
}
