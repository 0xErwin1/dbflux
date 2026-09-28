//! The code editor as a Vim host. The Vim logic itself is
//! `dbflux_components::vim`; this module supplies the document's gates and
//! keeps the document methods the rest of the crate and the tests call.

use super::*;
use dbflux_components::vim::{VimBinding, VimHost};

pub use dbflux_components::vim::VimMode;

impl VimHost for CodeDocument {
    fn vim(&self) -> Option<&VimBinding> {
        Some(&self.vim)
    }

    fn vim_mut(&mut self) -> Option<&mut VimBinding> {
        Some(&mut self.vim)
    }

    fn vim_read_only(&self, _cx: &App) -> bool {
        self.read_only
    }

    /// Focus inside one of the editor's own overlays, such as the find
    /// panel's query field, or in another pane of the document leaves the
    /// keys to them.
    fn vim_accepts_focus(&self) -> bool {
        self.focus_mode == SqlQueryFocus::Editor
    }

    fn vim_text_changed(&mut self, cx: &mut Context<Self>) {
        if self.read_only {
            return;
        }

        self.mark_dirty(cx);
        self.schedule_auto_save(cx);
        self.schedule_diagnostic_refresh(cx);
        self.editor.last_change_length = self.editor.input_state.read(cx).text().len();
    }
}

impl CodeDocument {
    /// Turns Vim mode on or off for this document. Enabling always starts in Normal mode.
    pub fn set_vim_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        VimBinding::set_enabled(self, enabled, cx);
    }

    /// Follows the persisted Vim mode setting. A no-op while it is unchanged, so
    /// unrelated app-state events keep the document's current mode.
    pub(super) fn sync_vim_setting(&mut self, cx: &mut Context<Self>) {
        let enabled = self.app_state.read(cx).general_settings().vim_mode;
        self.set_vim_enabled(enabled, cx);
    }

    /// The current Vim mode, or `None` when Vim mode is disabled.
    pub fn vim_mode(&self) -> Option<VimMode> {
        self.vim.mode()
    }

    pub(super) fn clear_vim_count_and_notify(&mut self, cx: &mut Context<Self>) {
        self.vim.clear_pending(cx);
    }

    pub(super) fn editor_menu_open(&self, cx: &App) -> bool {
        self.vim.menu_open(cx)
    }

    pub(super) fn close_change_group_on_blur(&mut self, cx: &mut Context<Self>) {
        VimBinding::blur(self, cx);
    }

    pub(super) fn finish_replace_once(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        VimBinding::input_changed(self, window, cx);
    }

    /// Returns focus to the editor input on the next tick, while the editor
    /// pane holds the document's keyboard.
    pub(super) fn schedule_editor_refocus(&self, window: &mut Window, cx: &mut Context<Self>) {
        if self.focus_mode != SqlQueryFocus::Editor {
            return;
        }

        self.vim.refocus_input(window, cx);
    }
}

#[cfg(test)]
mod tests;
