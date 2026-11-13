use gpui::{AppContext as _, Context, Entity, Render, TestAppContext, Window};

use super::{Toast, ToastAction, ToastHost};
use crate::accessibility::{FrameCapture, accessible_name};

/// A neutral brand at the default palette and density, so the tests
/// render against the same theme engine the products install.
fn install_test_theme(cx: &mut gpui::TestAppContext) {
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

/// A window whose whole body is the host under test.
struct HostView(Entity<ToastHost>);

impl Render for HostView {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl gpui::IntoElement {
        self.0.clone()
    }
}

/// A themed window showing one error toast (no auto-dismiss timer) with
/// the one action under test. The first pushed toast carries id 1, so
/// its first action button is `toast-action-1-0`.
fn host_window(cx: &mut TestAppContext, action: ToastAction) -> &mut gpui::VisualTestContext {
    install_test_theme(cx);

    let host = cx.new(|_| ToastHost::new());
    host.update(cx, |host, cx| {
        host.push_rich(Toast::error("Paste undone").action(action), cx);
    });

    let (_view, window) = cx.add_window_view(move |_, _| HostView(host.clone()));
    window.run_until_parked();
    window
}

const ACTION_BUTTON: &str = "toast-action-1-0";
const ACTION_KEYCAP: &str = "toast-action-1-0-key";

#[gpui::test]
fn a_hint_draws_the_keycap_inside_its_button(cx: &mut TestAppContext) {
    let window = host_window(
        cx,
        ToastAction::new("undo", "Paste undone").key_hint("ctrl-z"),
    );

    let button = window.debug_bounds(ACTION_BUTTON).expect("action laid out");
    let keycap = window
        .debug_bounds(ACTION_KEYCAP)
        .expect("the keycap carries the action id");
    assert!(button.contains(&keycap.center()));
}

#[gpui::test]
fn a_hint_reads_the_chord_it_was_given(cx: &mut TestAppContext) {
    let window = host_window(
        cx,
        ToastAction::new("undo", "Paste undone").key_hint("ctrl-z"),
    );
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    assert_eq!(
        accessible_name(&frame, ACTION_KEYCAP).as_deref(),
        Some("Ctrl Z")
    );
}

/// The keycap is for the eye: the chord stays out of the accessible
/// name, which keeps reading just the label.
#[gpui::test]
fn a_hint_stays_out_of_the_action_accessible_name(cx: &mut TestAppContext) {
    let window = host_window(
        cx,
        ToastAction::new("undo", "Paste undone").key_hint("ctrl-z"),
    );
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    assert_eq!(
        accessible_name(&frame, ACTION_BUTTON).as_deref(),
        Some("Paste undone"),
        "the keycap must not join the accessible name"
    );
}

/// An action without a hint renders as before: no keycap, and the
/// button keeps the label as its accessible name.
#[gpui::test]
fn an_action_without_a_hint_renders_as_before(cx: &mut TestAppContext) {
    let window = host_window(cx, ToastAction::new("undo", "Paste undone"));
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    assert!(
        window.debug_bounds(ACTION_KEYCAP).is_none(),
        "an action without a hint draws no keycap"
    );
    assert_eq!(
        accessible_name(&frame, ACTION_BUTTON).as_deref(),
        Some("Paste undone")
    );
}
