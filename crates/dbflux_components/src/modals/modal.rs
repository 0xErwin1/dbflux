//! Keymap and copy extensions over bolt's generic [`Modal`].
//!
//! The modal itself — its card, scrim, focus trap, key scrolling and the
//! [`ModalSeams`] key seam — lives in `bolt` and is re-exported here so call
//! sites keep compiling. What stays here is the product half: the keymap
//! actions a modal answers, the deferred commands, the Escape keycap read
//! from the keymap, the close icon and its translated name.

pub use bolt::modals::modal::{
    AttachModalKeys, MODAL_BACKDROP_ID, MODAL_BODY_SELECTOR, MODAL_CLOSE_ID, MODAL_FOOTER_SELECTOR,
    MODAL_KEY_CONTEXT, Modal, ModalFocus, ModalKey, ModalKeyHandler, ModalSeams, ModalVariant,
    ScrollKey, on_modal_key,
};

use dbflux_core::keymap_types::{Command, ContextId};
use gpui::{App, Div, Stateful, Window};

use crate::actions::{self, RunCommand, shortcut_label};
use crate::icons::AppIcon;

/// Installs the seams the moved modal reads: the keymap actions bound in the
/// `Modal` key context, the commands a deferring modal runs, the Escape
/// keycap and the close button's icon and name.
///
/// Call it once at startup, before the first window; `theme::init` does. A
/// modal rendered before it runs answers no keys.
pub fn install_modal_seams(cx: &mut App) {
    cx.set_global(ModalSeams {
        attach_keys: attach_modal_keys,
        defer_to_owner: run_owner_command,
        escape_label: |cx| shortcut_label(cx, ContextId::Modal.id(), Command::Cancel.id(), "Esc"),
        close_icon: AppIcon::CircleX.into(),
        close_label: || dbflux_i18n::t!("modals.close").into(),
    });
}

/// Registers the backdrop handlers of the actions the keymap binds in the
/// `Modal` key context.
fn attach_modal_keys(backdrop: Stateful<Div>, handler: ModalKeyHandler) -> Stateful<Div> {
    let backdrop = on_modal_key::<actions::Cancel>(backdrop, ModalKey::Cancel, &handler);
    let backdrop = on_modal_key::<actions::Execute>(backdrop, ModalKey::Execute, &handler);
    let backdrop =
        on_modal_key::<actions::ScrollUp>(backdrop, ModalKey::Scroll(ScrollKey::LineUp), &handler);
    let backdrop = on_modal_key::<actions::ScrollDown>(
        backdrop,
        ModalKey::Scroll(ScrollKey::LineDown),
        &handler,
    );
    let backdrop = on_modal_key::<actions::ScrollPageUp>(
        backdrop,
        ModalKey::Scroll(ScrollKey::PageUp),
        &handler,
    );
    let backdrop = on_modal_key::<actions::ScrollPageDown>(
        backdrop,
        ModalKey::Scroll(ScrollKey::PageDown),
        &handler,
    );
    let backdrop =
        on_modal_key::<actions::ScrollToTop>(backdrop, ModalKey::Scroll(ScrollKey::Top), &handler);
    on_modal_key::<actions::ScrollToBottom>(backdrop, ModalKey::Scroll(ScrollKey::Bottom), &handler)
}

/// Runs the keymap's Cancel or Execute command for a modal that leaves those
/// keys to its owner.
fn run_owner_command(key: ModalKey, window: &mut Window, cx: &mut App) {
    let command = match key {
        ModalKey::Cancel => Command::Cancel,
        ModalKey::Execute => Command::Execute,
        ModalKey::Scroll(_) => return,
    };

    window.dispatch_action(Box::new(RunCommand::new(command.id())), cx);
}

/// The modal keys the app keymap binds in the `Modal` context (see the modal
/// layer in `dbflux_ui_base::keymap`), for component tests that run without
/// the app keymap. Also installs [`install_modal_seams`], which the app does
/// at startup.
#[cfg(test)]
pub(crate) fn bind_modal_keys_for_tests(cx: &mut gpui::TestAppContext) {
    use gpui::KeyBinding;

    let context = Some(MODAL_KEY_CONTEXT);
    cx.update(|cx| {
        install_modal_seams(cx);
        cx.bind_keys([
            KeyBinding::new("escape", actions::Cancel, context),
            KeyBinding::new("enter", actions::Execute, context),
            KeyBinding::new("up", actions::ScrollUp, context),
            KeyBinding::new("down", actions::ScrollDown, context),
            KeyBinding::new("pageup", actions::ScrollPageUp, context),
            KeyBinding::new("pagedown", actions::ScrollPageDown, context),
            KeyBinding::new("home", actions::ScrollToTop, context),
            KeyBinding::new("end", actions::ScrollToBottom, context),
        ]);
    });
}

#[cfg(test)]
mod tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{MODAL_CLOSE_ID, Modal};
    use crate::actions::RunCommand;
    use gpui::{
        AccessibilityFrame, Context, FocusHandle, FrameObserver, InteractiveElement as _,
        IntoElement, ParentElement as _, Render, SharedString, Styled as _, TestAppContext,
        VisualTestContext, Window, div,
    };
    use std::sync::{Arc, Mutex};

    /// Keeps the latest rendered frame of the window it observes.
    #[derive(Default)]
    struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

    impl FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    type Log = Arc<Mutex<Vec<SharedString>>>;

    /// A modal under an owner that records the commands reaching it.
    struct Harness {
        focus: FocusHandle,
        defer_keys: bool,
        log: Log,
    }

    impl Render for Harness {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            let cancel_log = self.log.clone();
            let confirm_log = self.log.clone();

            let mut modal = Modal::new("Harness")
                .body(div())
                .footer(div())
                .focus_handle(&self.focus)
                .on_close(move |_, _| {
                    cancel_log.lock().expect("log lock").push("cancel".into());
                })
                .on_confirm(move |_, _| {
                    confirm_log.lock().expect("log lock").push("confirm".into());
                });

            if self.defer_keys {
                modal = modal.defer_keys_to_owner();
            }

            div()
                .size_full()
                .on_action(cx.listener(|this, action: &RunCommand, _, _| {
                    this.log
                        .lock()
                        .expect("log lock")
                        .push(action.command.clone());
                }))
                .child(modal)
        }
    }

    struct Setup<'a> {
        window: &'a mut VisualTestContext,
        capture: Arc<FrameCapture>,
        log: Log,
    }

    fn setup(cx: &mut TestAppContext, defer_keys: bool) -> Setup<'_> {
        cx.update(gpui_component::init);
        super::bind_modal_keys_for_tests(cx);

        let log: Log = Arc::default();
        let capture = Arc::new(FrameCapture::default());
        let (view, window) = cx.add_window_view({
            let log = log.clone();
            let capture = capture.clone();
            move |window, cx| {
                window.observe_frames(&capture);
                Harness {
                    focus: cx.focus_handle(),
                    defer_keys,
                    log,
                }
            }
        });
        window.run_until_parked();

        let focus = window.update(|_, cx| view.read(cx).focus.clone());
        window.update(|window, cx| focus.focus(window, cx));
        window.run_until_parked();

        Setup {
            window,
            capture,
            log,
        }
    }

    impl Setup<'_> {
        fn events(&self) -> Vec<SharedString> {
            self.log.lock().expect("log lock").clone()
        }
    }

    /// The close button announces the translated name the product supplies.
    #[gpui::test]
    fn the_close_button_is_named(cx: &mut TestAppContext) {
        let setup = setup(cx, false);
        let frame = setup
            .capture
            .0
            .lock()
            .expect("frame capture lock")
            .clone()
            .expect("a frame");
        let node = frame
            .nodes()
            .find(|(_, node)| node.id() == MODAL_CLOSE_ID)
            .and_then(|(_, node)| frame.accessibility_node(node))
            .expect("the close button is an accessible node");
        let expected = dbflux_i18n::t!("modals.close").to_string();

        assert_eq!(node.role(), gpui::Role::Button);
        assert_eq!(node.label(), Some(expected.as_str()));
    }

    #[gpui::test]
    fn the_keymap_actions_cancel_and_confirm(cx: &mut TestAppContext) {
        let setup = setup(cx, false);

        setup.window.simulate_keystrokes("escape");
        setup.window.simulate_keystrokes("enter");

        assert_eq!(setup.events(), ["cancel", "confirm"]);
    }

    #[gpui::test]
    fn a_deferring_modal_runs_the_owner_commands(cx: &mut TestAppContext) {
        use dbflux_core::keymap_types::Command;

        let setup = setup(cx, true);

        setup.window.simulate_keystrokes("escape");
        setup.window.simulate_keystrokes("enter");

        assert_eq!(
            setup.events(),
            [Command::Cancel.id(), Command::Execute.id()]
        );
    }
}
