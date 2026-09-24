//! Opt-in modal editing (Vim Normal/Insert) for the code editor.
//!
//! Normal mode is enforced by two layers, both scoped to this document's editor:
//!
//! 1. The editor input is switched to read-only. gpui-component then rejects every
//!    user-originated text change, whatever route it takes: text typed or committed
//!    by an IME through the platform input handler, keyboard and context-menu paste,
//!    cut, and the Enter, Tab and deletion actions, whose handlers are only registered
//!    while the input is editable. Programmatic edits (`set_value`, `replace`) are not
//!    limited by read-only, which is how `x` deletes.
//! 2. A capture-phase key listener on the editor container turns the command keys into
//!    editor operations. It is only on the dispatch path while focus is inside this
//!    editor, so other inputs never see it, and it runs before the bubble-phase
//!    workspace keymap. Modified keys always pass through, so application shortcuts
//!    keep working in both modes.

use super::*;
use dbflux_core::LogErr;
use gpui_component::input::Undo;

/// Editing mode of a code editor while Vim mode is enabled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum VimMode {
    #[default]
    Normal,
    Insert,
}

/// Per-document Vim state. Inert while `enabled` is false.
#[derive(Default)]
pub(super) struct VimState {
    enabled: bool,
    mode: VimMode,
    /// Set while a Normal-mode `u` temporarily lifts the read-only lock so the
    /// component's own undo handler is registered for one dispatch.
    undo_unlocked: bool,
}

/// One line of the buffer, without its line terminator.
struct LineView {
    start: usize,
    content: String,
}

impl LineView {
    fn read(text: &Rope, row: usize) -> Self {
        let line = text.slice_line(row).to_string();
        let content = line.strip_suffix('\r').unwrap_or(&line).to_string();

        Self {
            start: text.line_start_offset(row),
            content,
        }
    }

    /// Byte column of the last character, which is the right-most Normal-mode position.
    fn last_char_column(&self) -> usize {
        self.content
            .char_indices()
            .last()
            .map(|(column, _)| column)
            .unwrap_or(0)
    }

    fn clamp_normal(&self, column: usize) -> usize {
        column.min(self.last_char_column())
    }
}

impl CodeDocument {
    /// Turns Vim mode on or off for this document. Enabling always starts in Normal mode.
    pub fn set_vim_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if self.vim.enabled == enabled {
            return;
        }

        self.vim.enabled = enabled;
        self.vim.mode = VimMode::Normal;
        self.vim.undo_unlocked = false;
        self.sync_editor_lock(cx);

        if enabled {
            self.clamp_cursor_for_normal(cx);
        }

        cx.notify();
    }

    /// The current Vim mode, or `None` when Vim mode is disabled.
    pub fn vim_mode(&self) -> Option<VimMode> {
        self.vim.enabled.then_some(self.vim.mode)
    }

    /// Whether the editor must reject user text changes right now.
    pub(super) fn editor_input_locked(&self) -> bool {
        self.read_only
            || (self.vim.enabled && self.vim.mode == VimMode::Normal && !self.vim.undo_unlocked)
    }

    /// Applies the lock immediately. Render applies it again every frame, but text
    /// can reach the input handler before the next frame (an IME commit), so a mode
    /// change must not wait for it.
    fn sync_editor_lock(&mut self, cx: &mut Context<Self>) {
        let locked = self.editor_input_locked();
        self.editor
            .input_state
            .update(cx, |state, cx| state.set_readonly(locked, cx));
    }

    /// Handles a key before the editor and the workspace keymap see it.
    ///
    /// Returns true when the key was consumed and must not propagate.
    pub(super) fn handle_vim_key_down(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.vim.enabled || self.focus_mode != SqlQueryFocus::Editor {
            return false;
        }

        let keystroke = &event.keystroke;
        let modifiers = keystroke.modifiers;
        if modifiers.control || modifiers.alt || modifiers.platform || modifiers.function {
            return false;
        }

        let key = keystroke.key.as_str();

        match self.vim.mode {
            VimMode::Insert => {
                if key != "escape" || modifiers.shift {
                    return false;
                }

                self.vim_escape_from_insert(window, cx);
                true
            }
            VimMode::Normal => {
                // Tab and Shift+Tab do nothing in Normal mode. Without this they would
                // reach the workspace keymap and move focus out of the editor.
                if key == "tab" {
                    return true;
                }

                if modifiers.shift {
                    return false;
                }

                match key {
                    "h" => self.vim_move_horizontal(false, cx),
                    "l" => self.vim_move_horizontal(true, cx),
                    "j" | "enter" => self.vim_move_vertical(1, cx),
                    "k" => self.vim_move_vertical(-1, cx),
                    "i" => self.vim_enter_insert(cx),
                    "x" => self.vim_delete_char(window, cx),
                    "u" => self.vim_undo(window, cx),
                    _ => return false,
                }

                true
            }
        }
    }

    /// Handles the editor's Escape action in its capture phase, before the
    /// component's own handler runs.
    ///
    /// With a completion or code-action menu open in Insert mode, Esc closes the
    /// menu and stays in Insert mode. The component would close the menu too, but
    /// it lets the key propagate afterwards, which would then leave Insert mode.
    /// Returns true when the action was consumed.
    pub(super) fn handle_vim_escape_action(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.vim.enabled || self.vim.mode != VimMode::Insert || !self.editor_menu_open(cx) {
            return false;
        }

        self.dismiss_editor_menus(cx);
        self.schedule_editor_refocus(window, cx);
        true
    }

    fn dismiss_editor_menus(&mut self, cx: &mut Context<Self>) {
        self.editor.input_state.update(cx, |state, cx| {
            state.dismiss_completion_overlay(cx);
            state.dismiss_code_action_overlay(cx);
        });
    }

    fn editor_menu_open(&self, cx: &App) -> bool {
        let state = self.editor.input_state.read(cx);
        state.completion_menu_state().open || state.code_action_menu_state().open
    }

    fn vim_enter_insert(&mut self, cx: &mut Context<Self>) {
        self.vim.mode = VimMode::Insert;
        self.sync_editor_lock(cx);
        cx.notify();
    }

    fn vim_escape_from_insert(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.editor_menu_open(cx) {
            self.dismiss_editor_menus(cx);
        } else {
            self.vim.mode = VimMode::Normal;
            self.sync_editor_lock(cx);

            // Leaving Insert mode steps the cursor back onto the character it
            // was after, as Vim does.
            self.vim_move_horizontal(false, cx);
        }

        self.schedule_editor_refocus(window, cx);
        cx.notify();
    }

    fn cursor_line(&self, cx: &App) -> (usize, LineView, usize) {
        let state = self.editor.input_state.read(cx);
        let text = state.text();
        let cursor = state.cursor();
        let row = text.offset_to_point(cursor).row;
        let line = LineView::read(text, row);
        let column = cursor.saturating_sub(line.start).min(line.content.len());

        (row, line, column)
    }

    fn set_editor_cursor(&mut self, offset: usize, cx: &mut Context<Self>) {
        self.editor
            .input_state
            .update(cx, |state, cx| state.set_selected_range(offset..offset, cx));
    }

    fn clamp_cursor_for_normal(&mut self, cx: &mut Context<Self>) {
        let (_, line, column) = self.cursor_line(cx);
        let clamped = line.clamp_normal(column);

        if clamped != column {
            self.set_editor_cursor(line.start + clamped, cx);
        }
    }

    fn vim_move_horizontal(&mut self, forward: bool, cx: &mut Context<Self>) {
        let (_, line, column) = self.cursor_line(cx);

        let target = if forward {
            let next = line.content[column..]
                .chars()
                .next()
                .map(|character| column + character.len_utf8())
                .unwrap_or(column);
            line.clamp_normal(next)
        } else {
            line.content[..column]
                .chars()
                .next_back()
                .map(|character| column - character.len_utf8())
                .unwrap_or(column)
        };

        self.set_editor_cursor(line.start + target, cx);
    }

    fn vim_move_vertical(&mut self, delta: isize, cx: &mut Context<Self>) {
        let (row, line, column) = self.cursor_line(cx);
        let line_count = self.editor.input_state.read(cx).text().lines_len();

        let Some(target_row) = row
            .checked_add_signed(delta)
            .filter(|target| *target < line_count)
        else {
            return;
        };

        let desired_chars = line.content[..column].chars().count();
        let target = LineView::read(self.editor.input_state.read(cx).text(), target_row);
        let target_column = target
            .content
            .char_indices()
            .nth(desired_chars)
            .map(|(byte, _)| byte)
            .unwrap_or(target.content.len());

        self.set_editor_cursor(target.start + target.clamp_normal(target_column), cx);
    }

    fn vim_delete_char(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let (_, line, column) = self.cursor_line(cx);

        let Some(character) = line.content[column..].chars().next() else {
            return;
        };

        let start = line.start + column;
        let range = start..start + character.len_utf8();

        self.editor.input_state.update(cx, |state, cx| {
            state.set_selected_range(range, cx);
            state.replace("", window, cx);
        });

        self.clamp_cursor_for_normal(cx);
    }

    /// Runs the component's undo while in Normal mode.
    ///
    /// The undo handler is only registered on frames painted while the input is
    /// editable, so the lock is lifted, one frame is drawn to register it, the
    /// action is dispatched to the editor, and the lock is restored. Everything
    /// happens in one deferred callback, before any further platform input can
    /// reach the unlocked editor.
    fn vim_undo(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.vim.undo_unlocked = true;
        self.sync_editor_lock(cx);

        let input = self.editor.input_state.clone();
        let document = cx.entity().downgrade();

        window.defer(cx, move |window, cx| {
            window.draw(cx).clear(cx);

            let focus_handle = input.read(cx).focus_handle(cx);
            focus_handle.dispatch_action(&Undo, window, cx);

            document
                .update(cx, |document, cx| {
                    document.vim.undo_unlocked = false;
                    document.sync_editor_lock(cx);
                    document.clamp_cursor_for_normal(cx);
                    cx.notify();
                })
                .log_err();
        });
    }

    /// Returns focus to the editor input on the next tick.
    ///
    /// gpui-component's completion menu hides itself on Esc but never restores
    /// focus to the editor input. Synchronously the input still owns focus when
    /// Esc is observed, but the menu's notify and the resulting re-render reset
    /// the window focus before the next paint, so the refocus waits a tick.
    pub(super) fn schedule_editor_refocus(&self, window: &mut Window, cx: &mut Context<Self>) {
        if self.focus_mode != SqlQueryFocus::Editor {
            return;
        }

        let input = self.editor.input_state.clone();
        cx.spawn_in(window, async move |_this, cx| {
            cx.update(|window, cx| {
                input.update(cx, |state, cx| state.focus(window, cx));
            })
            .ok();
        })
        .detach();
    }
}

#[cfg(test)]
mod tests;
