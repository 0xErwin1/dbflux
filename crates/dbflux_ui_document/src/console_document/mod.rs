//! A native console in its own tab, opened from a database in the sidebar
//! ("Open console") for connections whose driver advertises
//! `DriverCapabilities::NATIVE_CONSOLE`. It runs commands against that
//! database without a table, collection or key browser open, with the same
//! checks, audit and history as the consoles docked under documents.

mod pane;

use crate::console::{NativeConsole, NativeConsoleEvent, NativeConsoleTarget};
use crate::handle::DocumentEvent;
use crate::types::{DocumentId, DocumentState};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_core::NativeConsoleProfile;
use dbflux_ui_base::AppStateEntity;
use gpui::*;
use uuid::Uuid;

pub struct ConsoleDocument {
    id: DocumentId,
    title: String,
    profile_id: Uuid,
    database: Option<String>,
    console: Entity<NativeConsole>,
    focus_handle: FocusHandle,
    _subscription: Subscription,
}

impl ConsoleDocument {
    /// A console tab for `database` (the connection's own when `None`) of
    /// `profile_id`, presented as `profile` describes.
    pub fn new(
        profile_id: Uuid,
        database: Option<String>,
        profile: NativeConsoleProfile,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let connection_name = app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| connected.profile.name.clone())
            .unwrap_or_default();

        let label = database.clone().unwrap_or_else(|| connection_name.clone());
        let title = dbflux_i18n::t!(
            "document.console.tab_title",
            connection = connection_name,
            database = label.clone()
        );

        let target = NativeConsoleTarget {
            profile_id,
            database: database.clone(),
            label,
        };

        let console = cx.new(|cx| {
            NativeConsole::new(target, profile, ContextId::Global, app_state, window, cx).in_tab()
        });

        let subscription = cx.subscribe(&console, |_, _, event: &NativeConsoleEvent, cx| {
            if let NativeConsoleEvent::InputFocused = event {
                cx.emit(DocumentEvent::RequestFocus);
            }
        });

        Self {
            id: DocumentId::new(),
            title,
            profile_id,
            database,
            console,
            focus_handle: cx.focus_handle(),
            _subscription: subscription,
        }
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        self.title.clone()
    }

    pub fn state(&self) -> DocumentState {
        DocumentState::Clean
    }

    pub fn can_close(&self) -> bool {
        true
    }

    pub fn connection_id(&self) -> Option<Uuid> {
        Some(self.profile_id)
    }

    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    /// Focusing the tab puts the keyboard in the console input.
    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.console
            .update(cx, |console, cx| console.focus_input(window, cx));
    }

    /// The tab is a text field: bare letters are typed, never run as
    /// commands.
    pub fn active_context(&self, _cx: &App) -> ContextId {
        ContextId::TextInput
    }

    /// `Ctrl+\`` returns the keyboard to the input; Enter (outside the input)
    /// and Escape answer a command waiting for confirmation.
    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match cmd {
            Command::ToggleConsole => {
                self.focus(window, cx);
                true
            }
            Command::Execute | Command::Cancel => {
                let console = self.console.read(cx);
                let answers = console.has_pending()
                    && !(cmd == Command::Execute && console.input_has_focus());

                if !answers {
                    return false;
                }

                self.console.update(cx, |console, cx| {
                    if cmd == Command::Execute {
                        console.confirm_pending(cx);
                    } else {
                        console.cancel_pending(cx);
                    }
                });
                true
            }
            _ => false,
        }
    }
}

impl EventEmitter<DocumentEvent> for ConsoleDocument {}

impl Render for ConsoleDocument {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("console-document")
            .size_full()
            .track_focus(&self.focus_handle)
            .child(self.console.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::ConsoleDocument;
    use crate::dedup::DocumentKey;
    use crate::keyboard_test_support::{connected_app_state, init_keyboard_runtime};
    use dbflux_app::keymap::Command;
    use dbflux_core::{DbDriver as _, DbKind, QueryResult};
    use dbflux_test_support::fake_driver::FakeDriver;
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::time::Duration;

    fn open(cx: &mut TestAppContext) -> (Entity<ConsoleDocument>, &mut VisualTestContext) {
        init_keyboard_runtime(cx);

        let driver = FakeDriver::new(DbKind::Redis)
            .with_default_result(QueryResult::text("OK".to_string(), Duration::ZERO));
        let profile = driver.metadata().native_console().expect("console profile");
        let (app_state, profile_id) = connected_app_state(cx, &driver, "cache");

        let (document, window) = cx.add_window_view(move |window, cx| {
            ConsoleDocument::new(
                profile_id,
                Some("0".to_string()),
                profile,
                app_state,
                window,
                cx,
            )
        });

        (document, window)
    }

    #[gpui::test]
    fn the_tab_console_is_open_and_takes_the_keyboard(cx: &mut TestAppContext) {
        let (document, window) = open(cx);

        window.update(|window, cx| {
            document.update(cx, |document, cx| {
                document.dispatch_command(Command::ToggleConsole, window, cx)
            })
        });

        let (open, focused) = window.update(|_, cx| {
            let console = document.read(cx).console.read(cx);
            (console.is_open(), console.input_has_focus())
        });
        assert!(open, "a tab console is always open");
        assert!(focused, "Ctrl+` puts the keyboard in the input");

        window.update(|window, cx| {
            document.update(cx, |document, cx| {
                document.dispatch_command(Command::ToggleConsole, window, cx)
            })
        });
        assert!(
            window.update(|_, cx| document.read(cx).console.read(cx).is_open()),
            "a second Ctrl+` does not collapse it"
        );
    }

    #[gpui::test]
    fn escape_drops_a_command_waiting_for_confirmation(cx: &mut TestAppContext) {
        let (document, window) = open(cx);

        window.update(|window, cx| {
            let console = document.read(cx).console.clone();
            console.update(cx, |console, cx| {
                console.input.update(cx, |state, cx| {
                    state.set_value("DEL user:1 user:2".to_string(), window, cx)
                });
                console.submit(window, cx);
            });
        });
        assert!(window.update(|_, cx| document.read(cx).console.read(cx).has_pending()));

        let handled = window.update(|window, cx| {
            document.update(cx, |document, cx| {
                document.dispatch_command(Command::Cancel, window, cx)
            })
        });
        assert!(handled);
        assert!(!window.update(|_, cx| document.read(cx).console.read(cx).has_pending()));
    }

    #[gpui::test]
    fn the_pane_deduplicates_by_connection_and_database(cx: &mut TestAppContext) {
        let (document, window) = open(cx);

        let (matches_same, matches_other) = window.update(|_, cx| {
            let profile_id = document.read(cx).connection_id().expect("connection");
            let pane = ConsoleDocument::into_pane(document.clone(), cx);
            (
                pane.matches_dedup_key(
                    &DocumentKey::Console {
                        profile_id,
                        database: Some("0".to_string()),
                    },
                    cx,
                ),
                pane.matches_dedup_key(
                    &DocumentKey::Console {
                        profile_id,
                        database: Some("1".to_string()),
                    },
                    cx,
                ),
            )
        });

        assert!(matches_same);
        assert!(!matches_other);
    }
}
