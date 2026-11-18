//! Opt-in modal editing (Vim Normal/Insert/Visual) for multi-line editors.
//!
//! The engine lives in `bolt::vim` and is re-exported here at the old paths.
//! This module keeps the product's pieces: the `dbflux`-namespaced leader
//! action the keymap dispatches (its action id is persisted in users'
//! keybinding overrides, so the type stays here), the translated mode labels,
//! and the installer for the product's [`VimSeams`] the moved engine reads.

pub use bolt::vim::{
    LeaderCommandHandler, RunCommandHandler, VIM_MODE_KEY, VimBinding, VimHost, VimMode, VimSeams,
    VimSettingGlobal, machine, set_vim_seams, vim_enabled,
};

use gpui::{Action, App, InteractiveElement as _, SharedString, Window};

use crate::actions::RunCommand;

/// Runs a keymap command from a Vim leader sequence (`<leader> a`).
///
/// The editor's Vim wrapper takes it and runs the command as a
/// `RunCommand` of its own, or inside a dialog hands it to the dialog
/// (`VimHost::vim_dialog_command`), so the sequence's last key never reaches
/// Vim or the editor, even when nothing handles the command.
#[derive(Clone, Debug, PartialEq, Eq, Action)]
#[action(namespace = dbflux, no_json)]
pub struct LeaderCommand {
    /// The command's action id (`Command::action_id`).
    pub command: SharedString,
}

impl LeaderCommand {
    pub fn new(command: impl Into<SharedString>) -> Self {
        Self {
            command: command.into(),
        }
    }
}

/// Mode indicator shown under an editor while Vim mode is enabled.
pub fn vim_mode_label(mode: VimMode) -> String {
    match mode {
        VimMode::Normal => dbflux_i18n::t!("document.code.vim.normal"),
        VimMode::Insert => dbflux_i18n::t!("document.code.vim.insert"),
        VimMode::Replace => dbflux_i18n::t!("document.code.vim.replace"),
        VimMode::Visual => dbflux_i18n::t!("document.code.vim.visual"),
        VimMode::VisualLine => dbflux_i18n::t!("document.code.vim.visual_line"),
        VimMode::VisualBlock => dbflux_i18n::t!("document.code.vim.visual_block"),
    }
}

/// Installs the product's Vim seams: the leader and run-command actions, the
/// Vim and modal key contexts, and the translated mode labels.
///
/// Call it once at startup, before the first window; `theme::init` does,
/// right after the keymap seams. [`set_vim_enabled`] also installs it, so an
/// app or a component test that switches Vim on without the startup path
/// still gets the seams.
pub fn install_vim_seams(cx: &mut App) {
    if cx.has_global::<VimSeams>() {
        return;
    }

    set_vim_seams(
        cx,
        VimSeams {
            leader_key_context: dbflux_core::keymap_types::ContextId::VimNormal.as_gpui_context(),
            modal_key_context: crate::modals::MODAL_KEY_CONTEXT,
            mode_label: |mode| vim_mode_label(mode).into(),
            make_run_command_action: |command| Box::new(RunCommand::from_user_binding(command)),
            capture_leader_action: |element, on_command| {
                element.capture_action(
                    move |action: &LeaderCommand, window: &mut Window, cx: &mut App| {
                        on_command(&action.command, window, cx);
                        cx.stop_propagation();
                    },
                )
            },
            capture_run_command: |element, on_command| {
                element.capture_action(
                    move |action: &RunCommand, window: &mut Window, cx: &mut App| {
                        on_command(action, action.from_user_binding, window, cx);
                    },
                )
            },
        },
    );
}

/// Publishes the Vim mode setting, installing the product's seams first so
/// an app or a component test that switches Vim on without the startup path
/// still gets them. Observers of the global run only when the value changes.
pub fn set_vim_enabled(cx: &mut App, enabled: bool) {
    install_vim_seams(cx);
    bolt::vim::set_vim_enabled(cx, enabled);
}
