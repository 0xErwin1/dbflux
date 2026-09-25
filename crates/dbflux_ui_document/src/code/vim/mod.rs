//! Opt-in modal editing (Vim Normal/Insert) for the code editor.
//!
//! `machine` decides what a key means and where the cursor goes; this module is
//! the document-owned adapter that applies those decisions to gpui-component's
//! `InputState` through its public API. Every `CodeDocument` language goes through
//! the same adapter.
//!
//! Normal mode is enforced by two layers, both scoped to this document's editor:
//!
//! 1. The editor input is switched to read-only. gpui-component then rejects every
//!    user-originated text change, whatever route it takes: text typed or committed
//!    by an IME through the platform input handler, keyboard and context-menu paste,
//!    cut, and the Enter, Tab and deletion actions, whose handlers are only registered
//!    while the input is editable. Programmatic edits (`set_value`, `replace`) are not
//!    limited by read-only, which is how `x` deletes and how completion, formatting
//!    and script output keep working.
//! 2. A capture-phase key listener on the editor container turns the command keys into
//!    editor operations. It is only on the dispatch path while focus is inside this
//!    editor, so other inputs never see it, and it runs before the bubble-phase
//!    workspace keymap. Keys with Ctrl, Alt, Cmd or Fn always pass through, so
//!    application shortcuts keep working in both modes.

mod machine;

use super::*;
use dbflux_core::LogErr;
use gpui_component::input::{Redo, Undo};
use machine::{VimCommand, VimKey};

pub use machine::VimMode;

/// Which history action a Normal-mode undo shortcut runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum HistoryStep {
    Undo,
    Redo,
}

/// Per-document Vim state. Inert while `enabled` is false.
#[derive(Default)]
pub(super) struct VimState {
    enabled: bool,
    mode: VimMode,
    /// Set while a Normal-mode undo or redo temporarily lifts the read-only lock
    /// so the component's own handler is registered for one dispatch.
    history_unlocked: bool,
    /// Where the last vertical move left the cursor and the character column it
    /// aimed for. The goal is reused only while the cursor is still there, so
    /// any other cursor change (a click, an arrow key, an edit) resets it.
    vertical_goal: Option<(usize, usize)>,
    count: Option<usize>,
    visual_anchor: Option<usize>,
    visual_cursor: Option<usize>,
}

impl CodeDocument {
    /// Turns Vim mode on or off for this document. Enabling always starts in Normal mode.
    pub fn set_vim_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if self.vim.enabled == enabled {
            return;
        }

        if !enabled
            && matches!(
                self.vim.mode,
                VimMode::Visual | VimMode::VisualLine | VimMode::VisualBlock
            )
        {
            let cursor = self
                .vim
                .visual_cursor
                .unwrap_or_else(|| self.editor_cursor(cx));
            self.editor
                .input_state
                .update(cx, |state, cx| state.set_selected_range(cursor..cursor, cx));
        }

        self.vim = VimState {
            enabled,
            ..VimState::default()
        };
        self.sync_editor_lock(cx);
        self.sync_editor_cursor_shape(cx);

        if enabled {
            self.clamp_cursor_for_normal(cx);
        }

        cx.notify();
    }

    /// Follows the persisted Vim mode setting. A no-op while it is unchanged, so
    /// unrelated app-state events keep the document's current mode.
    pub(super) fn sync_vim_setting(&mut self, cx: &mut Context<Self>) {
        let enabled = self.app_state.read(cx).general_settings().vim_mode;
        self.set_vim_enabled(enabled, cx);
    }

    /// The current Vim mode, or `None` when Vim mode is disabled.
    pub fn vim_mode(&self) -> Option<VimMode> {
        self.vim.enabled.then_some(self.vim.mode)
    }

    /// Whether the editor must reject user text changes right now.
    pub(super) fn editor_input_locked(&self) -> bool {
        self.read_only
            || (self.vim.enabled && self.vim.mode != VimMode::Insert && !self.vim.history_unlocked)
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

    fn sync_editor_cursor_shape(&mut self, cx: &mut Context<Self>) {
        use gpui_base::input::InputCursorShape;

        let shape = if self.vim.enabled && self.vim.mode != VimMode::Insert {
            InputCursorShape::Block
        } else {
            InputCursorShape::Bar
        };
        self.editor
            .input_state
            .update(cx, |state, cx| state.set_cursor_shape(shape, cx));
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

        let modifiers = event.keystroke.modifiers;
        let key = VimKey {
            key: event.keystroke.key.as_str(),
            shift: modifiers.shift,
            command_modifier: modifiers.control
                || modifiers.alt
                || modifiers.platform
                || modifiers.function,
        };

        let block_key = modifiers.control
            && !modifiers.alt
            && !modifiers.platform
            && !modifiers.function
            && !modifiers.shift
            && key.key == "v"
            && self.vim.mode != VimMode::Insert;
        let command = if block_key {
            Some(if self.vim.mode == VimMode::VisualBlock {
                VimCommand::LeaveVisual
            } else {
                VimCommand::EnterVisualBlock
            })
        } else {
            machine::command_for(self.vim.mode, key)
        };
        let Some(command) = command else {
            self.vim.count = None;
            return false;
        };

        if let VimCommand::Digit(digit) = command {
            if digit != 0 || self.vim.count.is_some() {
                self.vim.count = Some(
                    self.vim
                        .count
                        .unwrap_or(0)
                        .saturating_mul(10)
                        .saturating_add(digit as usize),
                );
                return true;
            }
        }
        self.apply_vim_command(command, window, cx);
        true
    }

    fn apply_vim_command(
        &mut self,
        command: VimCommand,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let count = self.vim.count.take().unwrap_or(1);
        match command {
            VimCommand::Digit(0) => self.move_cursor_with(machine::line_start, cx),
            VimCommand::Digit(_) => {}
            VimCommand::MoveLeft => self.repeat_cursor(machine::step_left, count, cx),
            VimCommand::MoveRight => self.repeat_cursor(machine::step_right, count, cx),
            VimCommand::MoveUp => self.repeat_vertical(-1, count, cx),
            VimCommand::MoveDown => self.repeat_vertical(1, count, cx),
            VimCommand::WordEnd(big) => self.move_word(machine::WordMotion::End, big, count, cx),
            VimCommand::WordForward(big) => {
                self.move_word(machine::WordMotion::Forward, big, count, cx)
            }
            VimCommand::WordBackward(big) => {
                self.move_word(machine::WordMotion::Backward, big, count, cx)
            }
            VimCommand::Append => {
                self.move_cursor_with(machine::append_after, cx);
                self.set_vim_mode(VimMode::Insert, cx);
            }
            VimCommand::AppendLine => {
                self.move_cursor_with(machine::line_end, cx);
                self.set_vim_mode(VimMode::Insert, cx);
            }
            VimCommand::InsertLine => {
                self.move_cursor_with(machine::line_first_nonblank, cx);
                self.set_vim_mode(VimMode::Insert, cx);
            }
            VimCommand::EnterInsert => self.set_vim_mode(VimMode::Insert, cx),
            VimCommand::EnterVisual
            | VimCommand::EnterVisualLine
            | VimCommand::EnterVisualBlock => {
                let cursor = self
                    .vim
                    .visual_cursor
                    .unwrap_or_else(|| self.editor_cursor(cx));
                if self.vim.visual_anchor.is_none() {
                    self.vim.visual_anchor = Some(cursor);
                }
                self.vim.visual_cursor = Some(cursor);
                self.set_vim_mode(
                    match command {
                        VimCommand::EnterVisual => VimMode::Visual,
                        VimCommand::EnterVisualLine => VimMode::VisualLine,
                        _ => VimMode::VisualBlock,
                    },
                    cx,
                );
                self.update_visual_selection(cx);
            }
            VimCommand::LeaveVisual => {
                let cursor = self
                    .vim
                    .visual_cursor
                    .unwrap_or_else(|| self.editor_cursor(cx));
                self.vim.visual_anchor = None;
                self.vim.visual_cursor = None;
                self.set_vim_mode(VimMode::Normal, cx);
                self.set_editor_cursor(cursor, cx);
                self.schedule_editor_refocus(window, cx);
            }
            VimCommand::LeaveInsert => self.leave_insert(window, cx),
            // A read-only document keeps its text: motions work, edits do nothing.
            VimCommand::DeleteChar if !self.read_only => self.delete_chars(count, window, cx),
            VimCommand::Undo if !self.read_only => {
                self.run_history_in_normal_mode(HistoryStep::Undo, count, window, cx)
            }
            VimCommand::DeleteChar | VimCommand::Undo | VimCommand::Swallow => {}
        }
    }

    pub(super) fn clear_vim_count(&mut self) {
        self.vim.count = None;
    }

    fn set_vim_mode(&mut self, mode: VimMode, cx: &mut Context<Self>) {
        self.vim.mode = mode;
        self.vim.vertical_goal = None;
        self.sync_editor_lock(cx);
        self.sync_editor_cursor_shape(cx);
        cx.notify();
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
        if !self.vim.enabled || self.focus_mode != SqlQueryFocus::Editor {
            return false;
        }
        if matches!(
            self.vim.mode,
            VimMode::Visual | VimMode::VisualLine | VimMode::VisualBlock
        ) {
            self.apply_vim_command(VimCommand::LeaveVisual, window, cx);
            return true;
        }
        if self.vim.mode != VimMode::Insert || !self.editor_menu_open(cx) {
            return false;
        }

        self.dismiss_editor_menus(cx);
        self.schedule_editor_refocus(window, cx);
        true
    }

    /// Whether the editor's Tab and Shift+Tab indent actions must be swallowed.
    ///
    /// In Normal mode the locked input does not register its indent handlers, so
    /// the keys would fall through to the root's focus navigation and move focus
    /// out of the editor. Checked in the actions' capture phase, because key
    /// bindings are dispatched before any key listener runs.
    pub(super) fn vim_swallows_indent_action(&mut self) -> bool {
        self.clear_vim_count();
        self.vim.enabled
            && self.focus_mode == SqlQueryFocus::Editor
            && machine::command_for(
                self.vim.mode,
                VimKey {
                    key: "tab",
                    shift: false,
                    command_modifier: false,
                },
            ) == Some(VimCommand::Swallow)
    }

    fn editor_menu_open(&self, cx: &App) -> bool {
        let state = self.editor.input_state.read(cx);
        state.completion_menu_state().open || state.code_action_menu_state().open
    }

    fn dismiss_editor_menus(&mut self, cx: &mut Context<Self>) {
        self.editor.input_state.update(cx, |state, cx| {
            state.dismiss_completion_overlay(cx);
            state.dismiss_code_action_overlay(cx);
        });
    }

    fn leave_insert(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.editor_menu_open(cx) {
            // Normally consumed by `handle_vim_escape_action` already; kept so a
            // menu opened some other way still closes before the mode changes.
            self.dismiss_editor_menus(cx);
        } else {
            self.set_vim_mode(
                machine::mode_after(self.vim.mode, VimCommand::LeaveInsert),
                cx,
            );

            // Leaving Insert mode steps the cursor back onto the character it
            // was after, as Vim does.
            self.move_cursor_with(machine::step_left, cx);
        }

        self.schedule_editor_refocus(window, cx);
    }

    fn editor_cursor(&self, cx: &App) -> usize {
        self.editor.input_state.read(cx).cursor()
    }

    fn set_editor_cursor(&mut self, offset: usize, cx: &mut Context<Self>) {
        if matches!(
            self.vim.mode,
            VimMode::Visual | VimMode::VisualLine | VimMode::VisualBlock
        ) {
            self.vim.visual_cursor = Some(offset);
            self.update_visual_selection(cx);
        } else {
            self.editor
                .input_state
                .update(cx, |state, cx| state.set_selected_range(offset..offset, cx));
        }
    }

    fn update_visual_selection(&mut self, cx: &mut Context<Self>) {
        let (Some(anchor), Some(cursor)) = (self.vim.visual_anchor, self.vim.visual_cursor) else {
            return;
        };
        self.editor.input_state.update(cx, |state, cx| {
            let range = machine::visual_range(
                state.text(),
                anchor,
                cursor,
                self.vim.mode == VimMode::VisualLine,
            );
            if self.vim.mode == VimMode::VisualBlock {
                state.set_columnar_selection(anchor, cursor, cx);
            } else {
                state.set_selected_range(range, cx);
            }
        });
    }

    fn motion_cursor(&self, cx: &App) -> usize {
        self.vim
            .visual_cursor
            .unwrap_or_else(|| self.editor_cursor(cx))
    }

    fn move_cursor_with(&mut self, step: fn(&Rope, usize) -> usize, cx: &mut Context<Self>) {
        let target = step(
            self.editor.input_state.read(cx).text(),
            self.motion_cursor(cx),
        );

        self.vim.vertical_goal = None;
        self.set_editor_cursor(target, cx);
    }

    fn repeat_cursor(
        &mut self,
        step: fn(&Rope, usize) -> usize,
        count: usize,
        cx: &mut Context<Self>,
    ) {
        let mut target = self.motion_cursor(cx);
        {
            let text = self.editor.input_state.read(cx);
            for _ in 0..count.min(text.text().len().saturating_add(1)) {
                let next = step(text.text(), target);
                if next == target {
                    break;
                }
                target = next;
            }
        }
        self.vim.vertical_goal = None;
        self.set_editor_cursor(target, cx);
    }

    fn move_word(
        &mut self,
        motion: machine::WordMotion,
        big: bool,
        count: usize,
        cx: &mut Context<Self>,
    ) {
        let mut target = self.motion_cursor(cx);
        {
            let text = self.editor.input_state.read(cx);
            let chars = machine::word_offsets(text.text());
            for _ in 0..count.min(chars.len().saturating_add(1)) {
                let next = machine::step_word(text.text(), &chars, target, motion, big);
                if next == target {
                    break;
                }
                target = next;
            }
        }
        self.vim.vertical_goal = None;
        self.set_editor_cursor(target, cx);
    }

    fn repeat_vertical(&mut self, delta: isize, count: usize, cx: &mut Context<Self>) {
        for _ in 0..count.min(self.editor.input_state.read(cx).text().lines_len()) {
            let before = self.motion_cursor(cx);
            self.move_cursor_vertically(delta, cx);
            if self.motion_cursor(cx) == before {
                break;
            }
        }
    }

    fn move_cursor_vertically(&mut self, delta: isize, cx: &mut Context<Self>) {
        let cursor = self.motion_cursor(cx);
        let goal = self
            .vim
            .vertical_goal
            .filter(|(offset, _)| *offset == cursor)
            .map(|(_, column)| column);

        let Some(step) =
            machine::step_vertical(self.editor.input_state.read(cx).text(), cursor, delta, goal)
        else {
            return;
        };

        self.set_editor_cursor(step.offset, cx);
        self.vim.vertical_goal = Some((step.offset, step.goal_column));
    }

    fn clamp_cursor_for_normal(&mut self, cx: &mut Context<Self>) {
        let cursor = self.editor_cursor(cx);
        let clamped = machine::clamp_to_character(self.editor.input_state.read(cx).text(), cursor);

        if clamped != cursor {
            self.set_editor_cursor(clamped, cx);
        }
    }

    fn delete_chars(&mut self, count: usize, window: &mut Window, cx: &mut Context<Self>) {
        let range = {
            let text = self.editor.input_state.read(cx);
            let Some(range) =
                machine::counted_character_range(text.text(), self.editor_cursor(cx), count)
            else {
                return;
            };
            range
        };

        self.vim.vertical_goal = None;
        self.editor.input_state.update(cx, |state, cx| {
            state.set_selected_range(range, cx);
            state.replace("", window, cx);
        });

        self.clamp_cursor_for_normal(cx);
    }

    /// Handles the editor's Undo and Redo actions (`Ctrl+Z`, `Ctrl+Y`, ...) in
    /// their capture phase. The locked Normal-mode input does not register its
    /// own handlers, so without this those shortcuts would stop working while
    /// Vim mode is on. Returns true when the action was consumed.
    pub(super) fn handle_vim_history_action(
        &mut self,
        step: HistoryStep,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.vim.enabled
            || self.vim.mode != VimMode::Normal
            || self.vim.history_unlocked
            || self.read_only
            || self.focus_mode != SqlQueryFocus::Editor
        {
            return false;
        }

        self.clear_vim_count();
        self.run_history_in_normal_mode(step, 1, window, cx);
        true
    }

    /// Runs the component's undo or redo while in Normal mode.
    ///
    /// The handlers are only registered on frames painted while the input is
    /// editable, so the lock is lifted, one frame is drawn to register them, the
    /// action is dispatched to the editor, and the lock is restored. Everything
    /// happens in one deferred callback, before any further platform input can
    /// reach the unlocked editor.
    fn run_history_in_normal_mode(
        &mut self,
        step: HistoryStep,
        count: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.vim.history_unlocked = true;
        self.vim.vertical_goal = None;
        self.sync_editor_lock(cx);

        let input = self.editor.input_state.clone();
        let document = cx.entity().downgrade();

        window.defer(cx, move |window, cx| {
            window.draw(cx).clear(cx);

            let focus_handle = input.read(cx).focus_handle(cx);
            // History depth is not exposed by the editor; cap pathological counts.
            for _ in 0..count.min(10_000) {
                match step {
                    HistoryStep::Undo => focus_handle.dispatch_action(&Undo, window, cx),
                    HistoryStep::Redo => focus_handle.dispatch_action(&Redo, window, cx),
                }
            }

            document
                .update(cx, |document, cx| {
                    document.vim.history_unlocked = false;
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
