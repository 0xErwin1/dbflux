//! The native command console under the key list and value pane
//! (`Ctrl+\``), shown when the connection's driver advertises
//! `DriverCapabilities::NATIVE_CONSOLE`. Execution, confirmation, audit and
//! history live in `crate::console`; this file only docks it and keeps the
//! document's focus bookkeeping in step with it.

use super::{KeyValueDocument, KeyValueFocusMode};
use crate::console::{NativeConsole, NativeConsoleEvent, NativeConsoleTarget};
use crate::handle::DocumentEvent;
use dbflux_app::keymap::ContextId;
use dbflux_ui_base::AppStateEntity;
use gpui::*;
use uuid::Uuid;

impl KeyValueDocument {
    /// The console for `profile_id`, or `None` when its driver offers none.
    pub(super) fn build_console(
        profile_id: Uuid,
        database: &str,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<(Entity<NativeConsole>, Subscription)> {
        let profile = app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .and_then(|connected| connected.connection.metadata().native_console())?;

        let target = NativeConsoleTarget {
            profile_id,
            database: Some(database.to_string()),
            label: super::parsing::database_label(database),
        };

        let console = cx.new(|cx| {
            NativeConsole::new(target, profile, ContextId::KeyValue, app_state, window, cx)
        });

        let subscription = cx.subscribe(&console, Self::on_console_event);

        Some((console, subscription))
    }

    fn on_console_event(
        &mut self,
        _console: Entity<NativeConsole>,
        event: &NativeConsoleEvent,
        cx: &mut Context<Self>,
    ) {
        match event {
            NativeConsoleEvent::InputFocused => {
                self.focus_mode = KeyValueFocusMode::TextInput;
                cx.emit(DocumentEvent::RequestFocus);
                cx.notify();
            }
            // A command may have changed the open key or the key list; the
            // value is cheap to re-read.
            NativeConsoleEvent::Executed { succeeded, .. } => {
                if *succeeded {
                    self.reload_selected_value(cx);
                }
            }
        }
    }

    /// Shows or hides the console (`Ctrl+\``), moving focus into it when it
    /// opens and back to the key list when it closes. Returns false when the
    /// connection has no console.
    pub(super) fn toggle_console(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        let Some(console) = self.console.clone() else {
            return false;
        };

        let open = console.update(cx, |console, cx| console.toggle(window, cx));

        if open {
            self.focus_mode = KeyValueFocusMode::TextInput;
        } else {
            self.focus_mode = KeyValueFocusMode::List;
            self.focus_handle.focus(window, cx);
        }

        cx.notify();
        true
    }

    pub(super) fn console_has_pending(&self, cx: &App) -> bool {
        self.console
            .as_ref()
            .is_some_and(|console| console.read(cx).has_pending())
    }

    pub(super) fn confirm_console_command(&mut self, cx: &mut Context<Self>) {
        if let Some(console) = self.console.clone() {
            console.update(cx, |console, cx| console.confirm_pending(cx));
        }
    }

    pub(super) fn cancel_console_command(&mut self, cx: &mut Context<Self>) {
        if let Some(console) = self.console.clone() {
            console.update(cx, |console, cx| console.cancel_pending(cx));
        }
    }

    /// The docked console, under the key context existing predicates name.
    pub(super) fn render_console(&self) -> Option<Div> {
        let console = self.console.clone()?;

        Some(
            div()
                .key_context(dbflux_components::key_contexts::KEY_VALUE_CONSOLE)
                .flex()
                .flex_col()
                .flex_none()
                .child(console),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::key_value::KeyValueDocument;
    use crate::keyboard_test_support::{host_document, init_keyboard_runtime};
    use dbflux_core::{DbKind, QueryResult};
    use dbflux_test_support::fake_driver::FakeDriver;
    use gpui::{AppContext as _, Entity, Focusable as _, TestAppContext, VisualTestContext};
    use std::time::Duration;

    /// A key-value document over a connected fake Redis profile.
    fn open(cx: &mut TestAppContext) -> (Entity<KeyValueDocument>, &mut VisualTestContext) {
        init_keyboard_runtime(cx);

        let driver = FakeDriver::new(DbKind::Redis)
            .with_default_result(QueryResult::text("OK".to_string(), Duration::ZERO));
        let (app_state, profile_id) =
            crate::keyboard_test_support::connected_app_state(cx, &driver, "cache");

        let (host, window) = host_document(
            cx,
            move |window, cx| {
                cx.new(|cx| {
                    KeyValueDocument::new(profile_id, "0".to_string(), app_state, window, cx)
                })
            },
            |document, cx| document.active_context(cx),
            KeyValueDocument::dispatch_command,
        );
        let document = window.update(|_, cx| host.read(cx).document.clone());

        window.update(|window, cx| document.update(cx, |doc, cx| doc.focus(window, cx)));
        window.run_until_parked();

        (document, window)
    }

    fn console_open(document: &Entity<KeyValueDocument>, window: &mut VisualTestContext) -> bool {
        window.update(|_, cx| {
            document
                .read(cx)
                .console
                .as_ref()
                .is_some_and(|console| console.read(cx).is_open())
        })
    }

    /// The console toggle works from the document and from the console
    /// input, and a letter the document binds is typed into a text field
    /// instead of running.
    #[gpui::test]
    fn key_value_keys_run_from_the_keymap(cx: &mut TestAppContext) {
        let (document, window) = open(cx);

        window.simulate_keystrokes("ctrl-`");
        assert!(console_open(&document, window), "Ctrl+` opens the console");

        let console_focused = window.update(|window, cx| {
            document
                .read(cx)
                .console
                .as_ref()
                .expect("console")
                .read(cx)
                .input
                .read(cx)
                .focus_handle(cx)
                .is_focused(window)
        });
        assert!(console_focused, "the console input takes focus");

        window.simulate_keystrokes("ctrl-`");
        assert!(
            !console_open(&document, window),
            "Ctrl+` closes it again from its input"
        );

        window.update(|window, cx| {
            let filter = document.read(cx).filter_input.clone();
            filter.update(cx, |state, cx| state.focus(window, cx));
        });
        window.run_until_parked();
        window.simulate_keystrokes("t");

        assert_eq!(
            window.update(|_, cx| document.read(cx).filter_input.read(cx).value().to_string()),
            "t",
            "`t` in the filter is text, not the expiry editor"
        );
        assert!(
            window.update(|_, cx| document.read(cx).expiry_editor.is_none()),
            "the expiry editor stays closed"
        );
    }

    /// Enter in the empty console field answers a pending confirmation with
    /// Run anyway, and Escape answers it with Cancel.
    #[gpui::test]
    fn enter_and_escape_answer_a_pending_console_command(cx: &mut TestAppContext) {
        let (document, window) = open(cx);

        window.simulate_keystrokes("ctrl-`");
        window.simulate_input("DEL user:1 user:2");
        window.simulate_keystrokes("enter");
        window.run_until_parked();

        let pending = |window: &mut VisualTestContext| {
            window.update(|_, cx| {
                document
                    .read(cx)
                    .console
                    .as_ref()
                    .is_some_and(|console| console.read(cx).has_pending())
            })
        };
        assert!(pending(window), "a multi-key delete asks first");

        window.simulate_keystrokes("enter");
        window.run_until_parked();
        assert!(!pending(window), "Enter answers the confirmation");

        let cancelled = dbflux_i18n::t!("document.console.cancelled");
        let was_cancelled = window.update(|_, cx| {
            document
                .read(cx)
                .console
                .as_ref()
                .expect("console")
                .read(cx)
                .transcript
                .iter()
                .any(|entry| entry.output.iter().any(|line| line.text == cancelled))
        });
        assert!(
            !was_cancelled,
            "Enter runs the command rather than cancelling it"
        );

        window.simulate_input("DEL user:3 user:4");
        window.simulate_keystrokes("enter");
        window.run_until_parked();
        assert!(pending(window));

        window.update(|window, cx| {
            document.update(cx, |doc, cx| {
                doc.dispatch_command(dbflux_app::keymap::Command::Cancel, window, cx)
            })
        });
        assert!(!pending(window), "Cancel drops the pending command");
    }
}
