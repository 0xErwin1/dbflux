use gpui::{Action, SharedString, actions};

actions!(
    dbflux,
    [
        Cancel,
        // List navigation
        SelectNext,
        SelectPrev,
        Execute,
        // CRUD / item actions
        Delete,
        ToggleFavorite,
        Rename,
        // Search / saved queries
        FocusSearch,
        SaveQuery,
        // Save the value of a modal editor (cell editor, document preview)
        SaveEdit,
        // Scroll a modal body from the keyboard
        ScrollUp,
        ScrollDown,
        ScrollPageUp,
        ScrollPageDown,
        ScrollToTop,
        ScrollToBottom,
    ]
);

/// Runs a keymap command.
///
/// Every keybinding generated from the keymap for a context owned by a
/// window root dispatches this action, carrying the command's action id
/// (`Command::action_id`). The window root handles it; an element that owns
/// some commands handles it first and propagates the ones it does not own.
#[derive(Clone, Debug, PartialEq, Eq, Action)]
#[action(namespace = dbflux, no_json)]
pub struct RunCommand {
    pub command: SharedString,
    /// The binding is one the user changed. An element that takes keys of
    /// its own ahead of the default bindings (the code editor with Vim
    /// editing) leaves the key to a user binding.
    pub from_user_binding: bool,
}

impl RunCommand {
    pub fn new(command: impl Into<SharedString>) -> Self {
        Self {
            command: command.into(),
            from_user_binding: false,
        }
    }

    /// The action of a binding the user changed.
    pub fn from_user_binding(command: impl Into<SharedString>) -> Self {
        Self {
            command: command.into(),
            from_user_binding: true,
        }
    }
}

/// Answers "which keys run this command here?" for keycaps drawn by
/// components, which cannot read the app keymap themselves. The keymap
/// installs it at startup (see `dbflux_ui_base::keymap::init_keymap`).
///
/// The arguments are a context id and a command id
/// (`ContextId::id`, `Command::id`); the answer is the label of the keys, or
/// `None` when the command has no shortcut there.
pub struct ShortcutLabels(pub Box<dyn Fn(&str, &str) -> Option<SharedString>>);

impl gpui::Global for ShortcutLabels {}

/// The label of the keys that run `command` in `context`.
///
/// Without an installed [`ShortcutLabels`] (component tests) the default
/// label `fallback` is returned; with one, `None` means the user removed the
/// shortcut, so the keycap is left out.
pub fn shortcut_label(
    cx: &gpui::App,
    context: &str,
    command: &str,
    fallback: &str,
) -> Option<SharedString> {
    match cx.try_global::<ShortcutLabels>() {
        Some(labels) => (labels.0)(context, command),
        None => Some(SharedString::from(fallback.to_string())),
    }
}
