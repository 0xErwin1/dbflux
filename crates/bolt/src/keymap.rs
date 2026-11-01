//! The keymap seam: how a product tells Bolt which of its actions mean what.
//!
//! A product's keymap is its own; Bolt never sees it. What a product
//! installs here is one [`CommandSet`] at startup: a table mapping the
//! semantic commands Bolt's components answer — [`Command`] — onto the
//! product's action ids, with both lookups, plus the key context its keymap
//! binds those keys in and the monomorphised hook that attaches the
//! product's action listener to a component's element.
//!
//! There is one table per app, installed once at startup from the product's
//! own command enum, so every component reads the same ids: a running app
//! cannot have one component with one table and another with a different
//! one.

use gpui::{Action, App, Context, Div, InteractiveElement, Keystroke, SharedString, Stateful};

use crate::controls::Dropdown;

/// The semantic commands Bolt's components answer, in Bolt's own vocabulary.
///
/// The set is enumerated from the real consumers — the dropdown's menu keys
/// ([`crate::controls::Dropdown`]) and an editor surface's leader commands —
/// not from a product's full command catalogue, which stays product-side. A
/// product maps each command it uses onto one of its action ids in its
/// [`CommandSet`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Command {
    /// Moves the highlight down.
    SelectNext,
    /// Moves the highlight up.
    SelectPrev,
    /// Confirms the highlighted choice.
    Execute,
    /// Opens what is collapsed, and confirms while it is open.
    ExpandCollapse,
    /// Closes what is open, or gives focus back.
    Cancel,
    /// Puts the cursor in the surface's own search — the vim leader search
    /// command of an editor.
    FocusSearch,
    /// Saves or confirms the surface hosting an editor, as its primary
    /// button does — the vim leader save command a dialog answers.
    Save,
}

/// The keymap action a product dispatches: the action id it carries names
/// the command a key binding runs (see [`CommandSet`]).
pub trait KeyCommand: Action {
    /// The action id the action carries — the product's `Command::id`.
    fn key_command_id(&self) -> &str;
}

/// The type of [`CommandSet::attach`]: registers the product's action
/// listener on a component's element.
pub type AttachKeyCommands = fn(Stateful<Div>, &mut Context<Dropdown>) -> Stateful<Div>;

/// Registers the component's key handler for the product's keymap action
/// `A` on `element`: the element answers the dispatched action's action id
/// (see [`KeyCommand`]) and propagates the keys it does not own.
///
/// The hook is monomorphised on the product's action type on purpose:
/// gpui-pre's `on_boxed_action` invokes the listener with the prototype
/// clone, never the dispatched action, so a `Box<dyn Any>` handler would
/// receive the wrong value.
pub fn attach_key_commands<A>(element: Stateful<Div>, cx: &mut Context<Dropdown>) -> Stateful<Div>
where
    A: KeyCommand,
{
    element.on_action(cx.listener(|this, action: &A, window, cx| {
        this.handle_key_command(action.key_command_id(), window, cx);
    }))
}

/// The product-installed table of semantic commands: for each [`Command`]
/// the product uses, the action id its keymap dispatches, with both
/// lookups.
///
/// Install one value per app at startup, before the first window. A
/// component rendered without one answers none of the commands.
pub struct CommandSet {
    /// The key context the product's keymap binds these commands' keys in; a
    /// component applies it to its own element so the product's bindings
    /// match on its keys.
    pub key_context: SharedString,
    /// Registers the typed action listener for the product's keymap action
    /// (see [`attach_key_commands`]); monomorphised on the product's
    /// [`KeyCommand`] type so the component stays non-generic.
    pub attach: AttachKeyCommands,
    entries: Vec<(Command, SharedString)>,
}

impl gpui::Global for CommandSet {}

impl CommandSet {
    /// Starts a set bound in the key context `key_context`, attaching the
    /// product's action listener with `attach`. It answers no commands
    /// until [`CommandSet::map`] adds them.
    pub fn new(key_context: impl Into<SharedString>, attach: AttachKeyCommands) -> Self {
        Self {
            key_context: key_context.into(),
            attach,
            entries: Vec::new(),
        }
    }

    /// Maps `command` onto the product's action id `action_id`, replacing
    /// any id mapped for it before.
    pub fn map(mut self, command: Command, action_id: impl Into<SharedString>) -> Self {
        let action_id = action_id.into();
        if let Some(entry) = self.entries.iter_mut().find(|(known, _)| *known == command) {
            entry.1 = action_id;
        } else {
            self.entries.push((command, action_id));
        }
        self
    }

    /// The action id the product maps `command` to, or `None` when it does
    /// not use the command.
    pub fn action_id(&self, command: Command) -> Option<&str> {
        self.entries
            .iter()
            .find(|(known, _)| *known == command)
            .map(|(_, id)| id.as_ref())
    }

    /// The semantic command whose action id is `action_id`, or `None` when
    /// the product maps no command to it.
    pub fn command(&self, action_id: &str) -> Option<Command> {
        self.entries
            .iter()
            .find(|(_, id)| id.as_ref() == action_id)
            .map(|(command, _)| *command)
    }
}

/// The last key pressed in any window, and the interceptor that records it.
#[derive(Default)]
struct LastKeystroke(Option<Keystroke>, Option<gpui::Subscription>);

impl gpui::Global for LastKeystroke {}

/// Starts recording the last key pressed in any window, for
/// [`last_keystroke`]. The keymap calls it once at startup.
pub fn record_last_keystroke(cx: &mut App) {
    let recorder = cx.intercept_keystrokes(|event, _window, cx| {
        cx.default_global::<LastKeystroke>().0 = Some(event.keystroke.clone());
    });
    cx.default_global::<LastKeystroke>().1 = Some(recorder);
}

/// The key whose binding is being dispatched: interceptors run before key
/// bindings, so while a binding's action runs this is the keystroke that
/// matched it (the last one of a key sequence).
pub fn last_keystroke(cx: &App) -> Option<Keystroke> {
    cx.try_global::<LastKeystroke>()
        .and_then(|recorded| recorded.0.clone())
}

#[cfg(test)]
mod tests {
    use gpui::{IntoElement, ParentElement, Render, TestAppContext, Window, div};

    use super::*;

    /// A no-op stand-in for a product's attach hook: the lookups under test
    /// never run it.
    fn test_attach(element: Stateful<Div>, _cx: &mut Context<Dropdown>) -> Stateful<Div> {
        element
    }

    fn test_set() -> CommandSet {
        CommandSet::new("TestDropdown", test_attach)
            .map(Command::SelectNext, "test_next")
            .map(Command::SelectPrev, "test_prev")
            .map(Command::Execute, "test_execute")
            .map(Command::ExpandCollapse, "test_expand")
            .map(Command::Cancel, "test_cancel")
            .map(Command::FocusSearch, "test_search")
            .map(Command::Save, "test_save")
    }

    #[test]
    fn a_set_answers_both_lookups_for_every_command_it_maps() {
        let set = test_set();

        for (command, action_id) in [
            (Command::SelectNext, "test_next"),
            (Command::SelectPrev, "test_prev"),
            (Command::Execute, "test_execute"),
            (Command::ExpandCollapse, "test_expand"),
            (Command::Cancel, "test_cancel"),
            (Command::FocusSearch, "test_search"),
            (Command::Save, "test_save"),
        ] {
            assert_eq!(set.action_id(command), Some(action_id), "{command:?}");
            assert_eq!(set.command(action_id), Some(command), "{action_id}");
        }

        assert_eq!(set.key_context, "TestDropdown");
    }

    #[test]
    fn an_unknown_action_id_names_no_command() {
        let set = test_set();

        assert_eq!(set.command("something_else"), None);
        assert_eq!(set.command(""), None);
    }

    #[test]
    fn a_command_the_product_does_not_use_names_no_action_id() {
        // A product maps only the commands its surfaces answer; the rest
        // stay unmapped for a consumer that asks.
        let set =
            CommandSet::new("TestDropdown", test_attach).map(Command::SelectNext, "test_next");

        assert_eq!(set.action_id(Command::FocusSearch), None);
        assert_eq!(set.action_id(Command::Save), None);
        assert_eq!(set.command("test_search"), None);
    }

    #[test]
    fn an_empty_set_answers_no_lookups() {
        let set = CommandSet::new("TestDropdown", test_attach);

        assert_eq!(set.action_id(Command::SelectNext), None);
        assert_eq!(set.command("test_next"), None);
        assert_eq!(set.key_context, "TestDropdown");
    }

    #[test]
    fn remapping_a_command_replaces_its_action_id() {
        let set = test_set().map(Command::SelectNext, "other_next");

        assert_eq!(set.action_id(Command::SelectNext), Some("other_next"));
        assert_eq!(set.command("test_next"), None);
        assert_eq!(set.command("other_next"), Some(Command::SelectNext));
    }

    #[gpui::test]
    fn an_app_without_an_installed_set_has_no_global(cx: &mut TestAppContext) {
        cx.update(|cx| assert!(cx.try_global::<CommandSet>().is_none()));
    }

    #[gpui::test]
    fn without_a_recorder_no_last_keystroke_is_reported(cx: &mut TestAppContext) {
        cx.update(|cx| assert_eq!(last_keystroke(cx), None));
    }

    #[gpui::test]
    fn the_recorded_last_keystroke_is_the_one_dispatched(cx: &mut TestAppContext) {
        struct Host {
            focus: gpui::FocusHandle,
        }

        impl Render for Host {
            fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
                div().id("host").track_focus(&self.focus).child("")
            }
        }

        cx.update(|cx| {
            record_last_keystroke(cx);
            assert_eq!(last_keystroke(cx), None, "nothing pressed yet");
        });

        let (host, window) = cx.add_window_view(|_, cx| Host {
            focus: cx.focus_handle(),
        });
        window.update(|window, cx| {
            let focus = host.read(cx).focus.clone();
            focus.focus(window, cx);
        });
        window.simulate_keystrokes("a");
        window.update(|_, cx| {
            let keystroke = last_keystroke(cx).expect("a key was pressed");
            assert_eq!(keystroke.key, "a");
        });

        window.simulate_keystrokes("shift-b");
        window.update(|_, cx| {
            let keystroke = last_keystroke(cx).expect("another key was pressed");
            assert_eq!(keystroke.key, "b");
            assert!(keystroke.modifiers.shift);
        });
    }

    #[gpui::test]
    fn an_app_without_a_recorder_never_reports_a_keystroke(cx: &mut TestAppContext) {
        struct Host {
            focus: gpui::FocusHandle,
        }

        impl Render for Host {
            fn render(&mut self, _: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
                div().id("host").track_focus(&self.focus).child("")
            }
        }

        let (host, window) = cx.add_window_view(|_, cx| Host {
            focus: cx.focus_handle(),
        });
        window.update(|window, cx| {
            let focus = host.read(cx).focus.clone();
            focus.focus(window, cx);
        });
        window.simulate_keystrokes("a");
        window.update(|_, cx| assert_eq!(last_keystroke(cx), None));
    }

    #[gpui::test]
    fn the_installed_set_is_readable_as_a_global(cx: &mut TestAppContext) {
        cx.update(|cx| cx.set_global(test_set()));

        cx.update(|cx| {
            let set = cx.try_global::<CommandSet>().expect("the set is installed");
            assert_eq!(set.command("test_save"), Some(Command::Save));
            assert_eq!(set.action_id(Command::Save), Some("test_save"));
        });
    }
}
