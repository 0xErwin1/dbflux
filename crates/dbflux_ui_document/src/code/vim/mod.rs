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
use gpui_base::input::{EditAnchor, EditAnchorAffinity};
use gpui_component::input::{Redo, Undo};
use machine::{VimCommand, VimKey};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CHANGE_GROUP: AtomicU64 = AtomicU64::new(1);

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
    pub(super) search_open: bool,
    last_search: Option<String>,
    /// Set while a Normal-mode undo or redo temporarily lifts the read-only lock
    /// so the component's own handler is registered for one dispatch.
    history_unlocked: bool,
    /// Where the last vertical move left the cursor and the character column it
    /// aimed for. The goal is reused only while the cursor is still there, so
    /// any other cursor change (a click, an arrow key, an edit) resets it.
    vertical_goal: Option<(usize, usize)>,
    count: Option<usize>,
    /// Raw command keys, bounded independently of the saturating numeric count.
    pub(super) pending_keys: String,
    pending_g: bool,
    pending_mark: Option<char>,
    marks: [Option<EditAnchor>; 26],
    pending_operator: Option<(char, Option<usize>)>,
    change_group: Option<u64>,
    replace_once: Option<PendingReplace>,
    /// Characters overwritten in this Replace session, innermost last: where the
    /// typed character starts and what it covered (`None` when it was appended).
    replaced: Vec<(usize, Option<String>)>,
    visual_anchor: Option<usize>,
    visual_cursor: Option<usize>,
}

struct PendingReplace {
    start: usize,
    count: usize,
    original: String,
}

/// What `r` writes when its key is not delivered as typed text.
#[derive(Clone, Copy)]
enum ReplaceOnceText {
    LineBreak,
    Character(char),
}

/// Registers the interceptor that sees every key before GPUI resolves key
/// bindings.
///
/// While `r` waits for its character the editor is editable, so the platform
/// input handler and an IME can deliver it. Editor actions bound to keys such
/// as Backspace, Delete, the arrows or Tab run before key listeners, so only an
/// interceptor can stop them from editing or moving the cursor first.
pub(super) fn intercept_vim_keystrokes(cx: &mut Context<CodeDocument>) -> Subscription {
    let document = cx.entity().downgrade();
    cx.intercept_keystrokes(move |event, window, cx| {
        let Some(document) = document.upgrade() else {
            return;
        };
        let vim = &document.read(cx).vim;
        if vim.replace_once.is_none() && !(vim.enabled && vim.mode == VimMode::Replace) {
            return;
        }

        let consumed = document.update(cx, |document, cx| {
            document.intercept_vim_keystroke(&event.keystroke, window, cx)
        });
        if consumed {
            cx.stop_propagation();
        }
    })
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

        if !enabled {
            self.vim.replace_once = None;
            self.close_change_group(cx);
            let marks = std::mem::take(&mut self.vim.marks);
            self.editor.input_state.update(cx, |state, _cx| {
                for anchor in marks.into_iter().flatten() {
                    state.remove_edit_anchor(anchor);
                }
            });
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
            || (self.vim.enabled && !self.vim.mode.accepts_text() && !self.vim.history_unlocked)
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

        let shape = if self.vim.enabled && !self.vim.mode.accepts_text() {
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
            self.clear_vim_count_and_notify(cx);
            return false;
        }
        if self.vim.search_open {
            return false;
        }

        if self.vim.replace_once.is_some() && event.keystroke.key == "escape" {
            self.cancel_replace_once(cx);
            return true;
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
            && !self.vim.mode.accepts_text();
        let command = if block_key {
            Some(if self.vim.mode == VimMode::VisualBlock {
                VimCommand::LeaveVisual
            } else {
                VimCommand::EnterVisualBlock
            })
        } else {
            machine::command_for(self.vim.mode, key)
        };
        if let Some(prefix) = self.vim.pending_mark.take() {
            self.vim.pending_keys.clear();
            cx.notify();
            if !key.command_modifier && !key.shift && key.key.len() == 1 {
                let letter = key.key.as_bytes()[0];
                if letter.is_ascii_lowercase() {
                    let index = usize::from(letter - b'a');
                    if prefix == 'm' {
                        let cursor = self.editor_cursor(cx);
                        self.editor.input_state.update(cx, |state, _| {
                            if let Some(previous) = self.vim.marks[index].take() {
                                state.remove_edit_anchor(previous);
                            }
                            self.vim.marks[index] =
                                state.create_edit_anchor(cursor, EditAnchorAffinity::Right);
                        });
                    } else {
                        let destination = {
                            let target = self.editor.input_state.read(cx);
                            self.vim.marks[index]
                                .and_then(|anchor| target.resolve_edit_anchor(anchor))
                                .map(|offset| {
                                    if prefix == '\'' {
                                        machine::line_first_nonblank(target.text(), offset)
                                    } else {
                                        machine::clamp_to_character(target.text(), offset)
                                    }
                                })
                        };
                        if let Some(destination) = destination {
                            self.vim.vertical_goal = None;
                            self.set_editor_cursor(destination, cx);
                        }
                    }
                }
            }
            self.clear_vim_count_and_notify(cx);
            return !key.command_modifier && key.key != "escape" && key.key != "tab";
        }
        let Some(command) = command else {
            self.clear_vim_count_and_notify(cx);
            return false;
        };
        if let VimCommand::PendingMark(prefix) = command {
            let interrupted = self.vim.pending_g || self.vim.pending_operator.is_some();
            self.clear_vim_count_and_notify(cx);
            self.vim.pending_mark = Some(if interrupted { '\0' } else { prefix });
            if !interrupted {
                self.push_pending_key(prefix, cx);
            }
            return true;
        }
        if self.vim.pending_g {
            self.vim.pending_g = false;
            self.vim.pending_keys.clear();
            cx.notify();
            if command == VimCommand::PendingG {
                if let Some((operator, prefix)) = self.vim.pending_operator.take() {
                    let count = prefix
                        .unwrap_or(1)
                        .saturating_mul(self.vim.count.take().unwrap_or(1));
                    self.apply_absolute_operator(operator, count.saturating_sub(1), window, cx);
                } else {
                    self.apply_vim_command(VimCommand::FirstLine, window, cx);
                }
                return true;
            }
            self.clear_vim_count_and_notify(cx);
            return true;
        }
        if command == VimCommand::PendingG {
            self.vim.pending_g = true;
            self.push_pending_key('g', cx);
            return true;
        }

        if let VimCommand::Digit(digit) = command
            && (digit != 0 || self.vim.count.is_some() || self.vim.pending_operator.is_some())
        {
            self.push_pending_key(char::from(b'0' + digit), cx);
            self.vim.count = Some(
                self.vim
                    .count
                    .unwrap_or(0)
                    .saturating_mul(10)
                    .saturating_add(digit as usize),
            );
            return true;
        }
        if let Some((operator, prefix)) = self.vim.pending_operator.take() {
            self.vim.pending_keys.clear();
            cx.notify();
            let inner = self.vim.count.take();
            let count = prefix.unwrap_or(1).saturating_mul(inner.unwrap_or(1));
            if command == VimCommand::LastLine {
                let target = if prefix.is_some() || inner.is_some() {
                    count.saturating_sub(1)
                } else {
                    usize::MAX
                };
                self.apply_absolute_operator(operator, target, window, cx);
                return true;
            }
            if let VimCommand::Operator(repeated) = command
                && operator == repeated
            {
                self.apply_line_operator(operator, count, window, cx);
                return true;
            }
            let motion = match command {
                VimCommand::WordEnd(big) => Some((machine::WordMotion::End, big)),
                VimCommand::WordForward(big) => Some((machine::WordMotion::Forward, big)),
                VimCommand::WordBackward(big) => Some((machine::WordMotion::Backward, big)),
                _ => None,
            };
            if let Some((motion, big)) = motion {
                self.apply_word_operator(operator, motion, big, count, window, cx);
                return true;
            }
            if let Some((range, linewise)) = {
                let state = self.editor.input_state.read(cx);
                let text = state.text();
                let cursor = state.cursor();
                match command {
                    VimCommand::MoveLeft => {
                        machine::horizontal_operator_range(text, cursor, false, count)
                            .or_else(|| (operator == 'c').then_some(cursor..cursor))
                            .map(|range| (range, false))
                    }
                    VimCommand::MoveRight => {
                        let range = if operator == 'c' {
                            machine::change_horizontal_right_range(text, cursor, count)
                        } else {
                            machine::horizontal_operator_range(text, cursor, true, count)
                        };
                        range.map(|range| (range, false))
                    }
                    VimCommand::MoveUp | VimCommand::MoveDown => {
                        let down = command == VimCommand::MoveDown;
                        let row = text.offset_to_point(cursor).row;
                        let target = if down {
                            row.saturating_add(count)
                                .min(text.lines_len().saturating_sub(1))
                        } else {
                            row.saturating_sub(count)
                        };
                        if operator == 'c' && row == target {
                            None
                        } else {
                            Some((
                                machine::vertical_operator_range(text, cursor, down, count),
                                true,
                            ))
                        }
                    }
                    _ => None,
                }
            } {
                self.apply_motion_operator(operator, range, linewise, window, cx);
                return true;
            }
            // An interrupted operator does not turn its next key into a command.
            return true;
        }

        if !matches!(command, VimCommand::Operator(_)) {
            self.vim.pending_keys.clear();
            cx.notify();
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
        let explicit_count = self.vim.count.take();
        let count = explicit_count.unwrap_or(1);
        match command {
            VimCommand::Digit(0) => self.move_cursor_with(machine::line_start, cx),
            VimCommand::Digit(_) | VimCommand::PendingG | VimCommand::PendingMark(_) => {}
            VimCommand::FirstLine | VimCommand::LastLine => {
                let target = {
                    let state = self.editor.input_state.read(cx);
                    let row = if command == VimCommand::FirstLine || explicit_count.is_some() {
                        count.saturating_sub(1)
                    } else {
                        state.text().lines_len().saturating_sub(1)
                    };
                    machine::absolute_line(state.text(), row)
                };
                self.vim.vertical_goal = None;
                self.set_editor_cursor(target, cx);
            }
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
            VimCommand::EnterReplace if !self.read_only => self.enter_replace(cx),
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
            VimCommand::VisualDelete if !self.read_only => {
                self.apply_visual_operator(true, window, cx)
            }
            VimCommand::VisualYank => self.apply_visual_operator(false, window, cx),
            VimCommand::VisualChange if !self.read_only => {
                self.apply_visual_change(window, cx);
            }
            VimCommand::VisualDelete | VimCommand::VisualChange => {}
            // A read-only document keeps its text: motions work, edits do nothing.
            VimCommand::Operator(operator) => {
                self.vim.pending_operator = Some((operator, explicit_count));
                self.push_pending_key(operator, cx);
            }
            VimCommand::DeleteChar if !self.read_only => self.delete_chars(count, window, cx),
            VimCommand::ReplaceOnce if !self.read_only => self.start_replace_once(count, cx),
            VimCommand::Undo if !self.read_only => {
                self.run_history_in_normal_mode(HistoryStep::Undo, count, window, cx)
            }
            VimCommand::OpenSearch => {
                self.vim.search_open = true;
                self.vim_search_input.update(cx, |state, cx| {
                    state.set_value("", window, cx);
                    state.focus(window, cx);
                });
                cx.notify();
            }
            VimCommand::RepeatSearch(reverse) => {
                self.repeat_vim_search(
                    if reverse {
                        machine::SearchDirection::Backward
                    } else {
                        machine::SearchDirection::Forward
                    },
                    count,
                    cx,
                );
            }
            VimCommand::DeleteChar
            | VimCommand::ReplaceOnce
            | VimCommand::EnterReplace
            | VimCommand::Undo
            | VimCommand::Swallow => {}
        }
    }

    pub(super) fn focus_vim_search_prompt(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.vim.search_open {
            return false;
        }
        self.vim_search_input
            .update(cx, |state, cx| state.focus(window, cx));
        true
    }

    pub(super) fn accept_vim_search(
        &mut self,
        query: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.vim.search_open {
            return;
        }
        self.vim.search_open = false;
        if !query.is_empty() {
            self.vim.last_search = Some(query.clone());
            self.editor
                .input_state
                .update(cx, |state, cx| state.set_search_query(query, false, cx));
            self.repeat_vim_search(machine::SearchDirection::Forward, 1, cx);
        }
        self.schedule_editor_refocus(window, cx);
        cx.notify();
    }

    pub(super) fn cancel_vim_search(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.vim.search_open {
            return false;
        }
        self.vim.search_open = false;
        self.schedule_editor_refocus(window, cx);
        cx.notify();
        true
    }

    fn repeat_vim_search(
        &mut self,
        direction: machine::SearchDirection,
        count: usize,
        cx: &mut Context<Self>,
    ) {
        let Some(query) = self.vim.last_search.clone() else {
            return;
        };
        let (matches, mut cursor) = self.editor.input_state.update(cx, |state, cx| {
            state.set_search_query(query, false, cx);
            (
                state.search_session().matcher.matched_ranges(),
                state.cursor(),
            )
        });
        for _ in 0..count.min(10_000) {
            let Some(range) = machine::cursor_relative_match(&matches, cursor, direction) else {
                return;
            };
            cursor = range.start;
        }
        self.vim.vertical_goal = None;
        self.set_editor_cursor(cursor, cx);
    }

    fn apply_absolute_operator(
        &mut self,
        operator: char,
        target_row: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let range = {
            let state = self.editor.input_state.read(cx);
            machine::absolute_operator_range(state.text(), state.cursor(), target_row)
        };
        self.apply_motion_operator(operator, range, true, window, cx);
    }

    fn push_pending_key(&mut self, key: char, cx: &mut Context<Self>) {
        if self.vim.pending_keys.len() < 32 {
            self.vim.pending_keys.push(key);
        }
        cx.notify();
    }

    pub(super) fn clear_vim_count(&mut self) {
        self.vim.count = None;
        self.vim.pending_g = false;
        self.vim.pending_mark = None;
        self.vim.pending_operator = None;
        self.vim.pending_keys.clear();
    }

    pub(super) fn clear_vim_count_and_notify(&mut self, cx: &mut Context<Self>) {
        if !self.vim.pending_keys.is_empty() {
            self.clear_vim_count();
            cx.notify();
        } else {
            self.clear_vim_count();
        }
    }

    fn set_vim_mode(&mut self, mode: VimMode, cx: &mut Context<Self>) {
        self.clear_vim_count_and_notify(cx);
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
        self.clear_vim_count_and_notify(cx);
        if self.vim.replace_once.is_some() {
            self.cancel_replace_once(cx);
            return true;
        }
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
        if !self.vim.mode.accepts_text() || !self.editor_menu_open(cx) {
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
    pub(super) fn vim_swallows_indent_action(&mut self, cx: &mut Context<Self>) -> bool {
        if self.vim.search_open {
            return true;
        }
        self.clear_vim_count_and_notify(cx);
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
            self.close_change_group(cx);
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
                state.set_visual_caret(Some(cursor), cx);
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

    fn apply_visual_change(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let (range, clipboard) = {
            let state = self.editor.input_state.read(cx);
            let content = state.text().to_string();
            let selected = state.selected_range();
            let clipboard = if self.vim.mode == VimMode::VisualLine {
                machine::line_yank_text(&content, selected.clone())
            } else {
                content.get(selected.clone())
            };
            let range = if self.vim.mode == VimMode::VisualLine && !selected.is_empty() {
                machine::change_line_range(state.text(), selected)
            } else {
                selected
            };
            (
                range,
                clipboard.filter(|text| !text.is_empty()).map(str::to_owned),
            )
        };
        let anchor = self.vim.visual_anchor.unwrap_or(range.start);
        self.editor.input_state.update(cx, |state, cx| {
            state.set_selected_range(anchor..anchor, cx);
        });
        self.apply_change(range, clipboard, window, cx);
        self.vim.visual_anchor = None;
        self.vim.visual_cursor = None;
        self.schedule_editor_refocus(window, cx);
    }

    fn apply_visual_operator(&mut self, delete: bool, window: &mut Window, cx: &mut Context<Self>) {
        let mode = self.vim.mode;
        let (selected, delete_range) = {
            let state = self.editor.input_state.read(cx);
            let content = state.text().to_string();
            let ranges = state.selected_nonempty_ranges();
            let range = state.selected_range();
            let selected = match mode {
                VimMode::VisualBlock => ranges
                    .iter()
                    .filter_map(|range| content.get(range.clone()))
                    .collect::<Vec<_>>()
                    .join("\n"),
                VimMode::VisualLine => machine::line_yank_text(&content, range.clone())
                    .unwrap_or_default()
                    .to_string(),
                _ => content.get(range.clone()).unwrap_or_default().to_string(),
            };
            let delete_range = (mode == VimMode::VisualLine)
                .then(|| machine::line_delete_range(state.text(), range));
            (selected, delete_range)
        };
        let has_selection = !selected.is_empty();
        if has_selection {
            cx.write_to_clipboard(gpui::ClipboardItem::new_string(selected));
        }
        if delete && has_selection {
            self.editor.input_state.update(cx, |state, cx| {
                if let Some(range) = delete_range {
                    state.set_selected_range(range, cx);
                }
                // The native multi-cursor replace applies disjoint block fragments
                // in one history transaction; never collapse them into one span.
                state.replace("", window, cx);
            });
        }
        let cursor = self
            .vim
            .visual_anchor
            .unwrap_or(0)
            .min(self.vim.visual_cursor.unwrap_or(0));
        self.vim.visual_anchor = None;
        self.vim.visual_cursor = None;
        self.set_vim_mode(VimMode::Normal, cx);
        self.editor.input_state.update(cx, |state, cx| {
            let cursor = machine::clamp_to_character(state.text(), cursor.min(state.text().len()));
            state.set_selected_range(cursor..cursor, cx);
        });
        self.schedule_editor_refocus(window, cx);
    }

    fn apply_line_operator(
        &mut self,
        operator: char,
        count: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if operator == 'c' {
            if self.read_only {
                return;
            }
            let range = {
                let state = self.editor.input_state.read(cx);
                let start = machine::line_start(state.text(), state.cursor());
                let end_row = state
                    .text()
                    .offset_to_point(start)
                    .row
                    .saturating_add(count);
                let end = if end_row < state.text().lines_len() {
                    state.text().line_start_offset(end_row)
                } else {
                    state.text().len()
                };
                let content = state.text().to_string();
                let selected = content.get(start..end).unwrap_or_default().to_string();
                let terminator = if selected.ends_with("\r\n") {
                    2
                } else if selected.ends_with('\n') {
                    1
                } else {
                    0
                };
                (start..end.saturating_sub(terminator).max(start), selected)
            };
            self.apply_change(range.0, Some(range.1), window, cx);
            return;
        }
        let range = {
            let state = self.editor.input_state.read(cx);
            machine::counted_line_range(state.text(), self.editor_cursor(cx), count)
        };
        let content = self.editor.input_state.read(cx).text().to_string();
        let selected = if operator == 'y' {
            machine::line_yank_text(&content, range.clone())
        } else {
            content.get(range.clone())
        }
        .unwrap_or_default()
        .to_string();
        if operator == 'd' && self.read_only {
            return;
        }
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(selected));
        if operator == 'd' {
            let delete_range = {
                let state = self.editor.input_state.read(cx);
                machine::line_delete_range(state.text(), range)
            };
            if delete_range.is_empty() {
                return;
            }
            self.vim.vertical_goal = None;
            self.editor.input_state.update(cx, |state, cx| {
                state.set_selected_range(delete_range, cx);
                state.replace("", window, cx);
            });
            self.clamp_cursor_for_normal(cx);
        }
    }

    fn apply_motion_operator(
        &mut self,
        operator: char,
        range: std::ops::Range<usize>,
        linewise: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if matches!(operator, 'd' | 'c') && self.read_only {
            return;
        }
        let selected = {
            let state = self.editor.input_state.read(cx);
            let content = state.text().to_string();
            if operator == 'y' && linewise {
                machine::line_yank_text(&content, range.clone())
            } else {
                content.get(range.clone())
            }
            .map(str::to_owned)
        };
        let Some(selected) = selected else { return };
        if operator == 'c' {
            let delete_range = if linewise {
                machine::change_line_range(self.editor.input_state.read(cx).text(), range)
            } else {
                range
            };
            self.apply_change(
                delete_range,
                (!selected.is_empty()).then_some(selected),
                window,
                cx,
            );
            return;
        }
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(selected));
        if operator == 'd' {
            let delete_range = if linewise {
                machine::line_delete_range(self.editor.input_state.read(cx).text(), range)
            } else {
                range
            };
            if !delete_range.is_empty() {
                self.vim.vertical_goal = None;
                self.editor.input_state.update(cx, |state, cx| {
                    state.set_selected_range(delete_range, cx);
                    state.replace("", window, cx);
                });
                self.clamp_cursor_for_normal(cx);
            }
        }
    }

    fn apply_word_operator(
        &mut self,
        operator: char,
        motion: machine::WordMotion,
        big: bool,
        count: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if matches!(operator, 'd' | 'c') && self.read_only {
            return;
        }
        if operator == 'c' {
            let range = {
                let state = self.editor.input_state.read(cx);
                if motion == machine::WordMotion::Forward {
                    machine::change_word_range_with_class(state.text(), state.cursor(), count, big)
                } else {
                    machine::word_operator_range(state.text(), state.cursor(), motion, big, count)
                }
            };
            if let Some(range) = range {
                self.apply_change(range, None, window, cx);
            }
            return;
        }
        let (range, selected) = {
            let state = self.editor.input_state.read(cx);
            let Some(range) = machine::word_operator_range(
                state.text(),
                self.editor_cursor(cx),
                motion,
                big,
                count,
            ) else {
                return;
            };
            let content = state.text().to_string();
            let Some(selected) = content.get(range.clone()) else {
                return;
            };
            (range, selected.to_string())
        };
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(selected));
        if operator == 'd' {
            self.vim.vertical_goal = None;
            self.editor.input_state.update(cx, |state, cx| {
                state.set_selected_range(range, cx);
                state.replace("", window, cx);
            });
            self.clamp_cursor_for_normal(cx);
        }
    }

    fn apply_change(
        &mut self,
        range: std::ops::Range<usize>,
        clipboard: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.read_only {
            return;
        }
        let content = self.editor.input_state.read(cx).text().to_string();
        let Some(selected) = content.get(range.clone()) else {
            return;
        };
        let group = NEXT_CHANGE_GROUP.fetch_add(1, Ordering::Relaxed);
        let started = self
            .editor
            .input_state
            .update(cx, |state, _| state.begin_edit_group(group));
        if !started {
            return;
        }
        if !selected.is_empty() || clipboard.is_some() {
            cx.write_to_clipboard(gpui::ClipboardItem::new_string(
                clipboard.unwrap_or_else(|| selected.to_string()),
            ));
        }
        self.vim.change_group = Some(group);
        self.vim.vertical_goal = None;
        if !range.is_empty() {
            self.editor.input_state.update(cx, |state, cx| {
                state.set_selected_range(range, cx);
                state.replace("", window, cx);
            });
        }
        self.set_vim_mode(VimMode::Insert, cx);
    }

    pub(super) fn close_change_group_on_blur(&mut self, cx: &mut Context<Self>) {
        if self.vim.replace_once.is_some() {
            self.cancel_replace_once(cx);
            return;
        }
        if self.vim.change_group.is_some() {
            self.close_change_group(cx);
            self.set_vim_mode(VimMode::Normal, cx);
        }
    }

    fn close_change_group(&mut self, cx: &mut Context<Self>) {
        if let Some(group) = self.vim.change_group.take() {
            self.editor.input_state.update(cx, |state, _| {
                state.request_end_edit_group(group);
            });
        }
    }

    fn start_replace_once(&mut self, count: usize, cx: &mut Context<Self>) {
        let pending = {
            let state = self.editor.input_state.read(cx);
            let start = state.cursor();
            let Some(range) = machine::counted_character_range(state.text(), start, count) else {
                return;
            };
            if state.text().slice(range).chars().count() != count {
                return;
            }
            PendingReplace {
                start,
                count,
                original: state.text().to_string(),
            }
        };
        let group = NEXT_CHANGE_GROUP.fetch_add(1, Ordering::Relaxed);
        let started = self
            .editor
            .input_state
            .update(cx, |state, _| state.begin_edit_group(group));
        if !started {
            return;
        }
        self.vim.change_group = Some(group);
        self.editor.input_state.update(cx, |state, cx| {
            state.set_selected_range(pending.start..pending.start, cx);
        });
        self.vim.replace_once = Some(pending);
        self.set_vim_mode(VimMode::Insert, cx);
    }

    /// Decides a key while `r` waits for its character. Returns true when the key
    /// is consumed before any binding or input handler sees it.
    ///
    /// Text keys go on to the native input so an IME or a dead key can compose
    /// the character, and `finish_replace_once` completes the replacement from
    /// the edit. Enter and Tab replace directly, a shortcut cancels and still
    /// reaches the application, and the other editing and movement keys cancel
    /// without moving or editing.
    fn intercept_vim_keystroke(
        &mut self,
        keystroke: &gpui::Keystroke,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        {
            let state = self.editor.input_state.read(cx);
            if !state.focus_handle(cx).is_focused(window) || state.has_active_composition() {
                return false;
            }
        }

        let modifiers = keystroke.modifiers;
        let printable = keystroke
            .key_char
            .as_deref()
            .is_some_and(|text| !text.is_empty() && !text.chars().any(char::is_control));
        let shortcut = modifiers.control
            || modifiers.platform
            || ((modifiers.alt || modifiers.function) && !printable);
        if self.vim.replace_once.is_none() {
            return self.intercept_replace_mode_key(keystroke, printable && !shortcut, window, cx);
        }
        if shortcut {
            self.cancel_replace_once(cx);
            return false;
        }

        match (keystroke.key.as_str(), modifiers.shift) {
            ("escape", _) => false,
            ("enter", false) => {
                self.replace_once_directly(ReplaceOnceText::LineBreak, window, cx);
                true
            }
            ("tab", false) => {
                self.replace_once_directly(ReplaceOnceText::Character('\t'), window, cx);
                true
            }
            (key, _) if machine::is_editing_key(key) => {
                self.cancel_replace_once(cx);
                true
            }
            _ => false,
        }
    }

    /// Replace mode: a typed character first selects the character under the
    /// cursor, so the native insertion overwrites it, and Backspace restores what
    /// this session overwrote. Every other key keeps its Insert-mode behavior.
    fn intercept_replace_mode_key(
        &mut self,
        keystroke: &gpui::Keystroke,
        typed_character: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if keystroke.key == "backspace" && !typed_character {
            self.replace_mode_backspace(window, cx);
            return true;
        }
        if !typed_character || machine::is_editing_key(&keystroke.key) {
            return false;
        }

        let (cursor, covered) = {
            let state = self.editor.input_state.read(cx);
            let selection = state.selected_range();
            if !selection.is_empty() {
                return false;
            }
            let covered = machine::counted_character_range(state.text(), selection.start, 1);
            (selection.start, covered)
        };
        let original = covered.map(|range| {
            let content = self
                .editor
                .input_state
                .read(cx)
                .text()
                .slice(range.clone())
                .to_string();
            self.editor
                .input_state
                .update(cx, |state, cx| state.set_selected_range(range, cx));
            content
        });
        self.vim.replaced.push((cursor, original));
        false
    }

    fn replace_mode_backspace(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let (cursor, previous) = {
            let state = self.editor.input_state.read(cx);
            let cursor = state.cursor();
            (cursor, machine::step_left(state.text(), cursor))
        };

        let restorable = self
            .vim
            .replaced
            .last()
            .is_some_and(|(start, _)| *start == previous && previous < cursor);
        if !restorable {
            self.vim.replaced.clear();
            self.set_editor_cursor(previous, cx);
            return;
        }

        if let Some((start, original)) = self.vim.replaced.pop() {
            self.editor.input_state.update(cx, |state, cx| {
                state.set_selected_range(start..cursor, cx);
                state.replace(original.unwrap_or_default(), window, cx);
            });
            self.set_editor_cursor(start, cx);
        }
    }

    fn enter_replace(&mut self, cx: &mut Context<Self>) {
        let group = NEXT_CHANGE_GROUP.fetch_add(1, Ordering::Relaxed);
        let started = self
            .editor
            .input_state
            .update(cx, |state, _| state.begin_edit_group(group));
        if !started {
            return;
        }

        self.vim.change_group = Some(group);
        self.vim.replaced.clear();
        self.set_vim_mode(VimMode::Replace, cx);
    }

    fn replace_once_directly(
        &mut self,
        text: ReplaceOnceText,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(pending) = self.vim.replace_once.take() else {
            return;
        };

        let edit = {
            let state = self.editor.input_state.read(cx);
            let content = state.text();
            machine::counted_character_range(content, pending.start, pending.count)
                .filter(|range| content.slice(range.clone()).chars().count() == pending.count)
                .map(|range| {
                    let replacement = match text {
                        ReplaceOnceText::LineBreak => {
                            machine::line_break_with_indent(content, pending.start)
                        }
                        ReplaceOnceText::Character(character) => {
                            character.to_string().repeat(pending.count)
                        }
                    };
                    (range, replacement)
                })
        };

        if let Some((range, replacement)) = edit {
            let cursor = match text {
                ReplaceOnceText::LineBreak => range.start + replacement.len(),
                ReplaceOnceText::Character(_) => range.start,
            };
            self.editor.input_state.update(cx, |state, cx| {
                state.set_selected_range(range, cx);
                state.replace(replacement, window, cx);
            });
            self.set_editor_cursor(cursor, cx);
            if matches!(text, ReplaceOnceText::LineBreak) {
                // Vim leaves the cursor where Esc would after typing the break.
                self.move_cursor_with(machine::step_left, cx);
            }
        }

        self.close_change_group(cx);
        self.set_vim_mode(VimMode::Normal, cx);
    }

    fn cancel_replace_once(&mut self, cx: &mut Context<Self>) {
        let pending = self.vim.replace_once.take();
        let marked_text_changed = pending.is_some_and(|pending| {
            let state = self.editor.input_state.read(cx);
            state.has_active_composition() && *state.text() != pending.original
        });
        self.close_change_group(cx);
        self.set_vim_mode(VimMode::Normal, cx);
        if marked_text_changed && !self.read_only {
            self.mark_dirty(cx);
            self.schedule_auto_save(cx);
            self.schedule_diagnostic_refresh(cx);
            self.editor.last_change_length = self.editor.input_state.read(cx).text().len();
        }
    }

    pub(super) fn finish_replace_once(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.vim.replace_once.is_none() {
            return;
        };
        if self.read_only {
            self.cancel_replace_once(cx);
            return;
        }
        if self.editor.input_state.read(cx).has_active_composition() {
            return;
        }
        let current = self.editor.input_state.read(cx).text().to_string();
        if self
            .vim
            .replace_once
            .as_ref()
            .is_some_and(|pending| pending.original == current)
        {
            return;
        }
        let Some(pending) = self.vim.replace_once.take() else {
            return;
        };
        let Some(prefix) = pending.original.get(..pending.start) else {
            self.cancel_replace_once(cx);
            return;
        };
        let Some(suffix) = pending.original.get(pending.start..) else {
            self.cancel_replace_once(cx);
            return;
        };
        let Some(inserted) = current
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(suffix))
        else {
            self.cancel_replace_once(cx);
            return;
        };
        if inserted.chars().count() != 1 || inserted.contains(['\n', '\r']) {
            self.cancel_replace_once(cx);
            return;
        }
        let Some(range) = machine::counted_character_range(
            self.editor.input_state.read(cx).text(),
            pending.start + inserted.len(),
            pending.count,
        ) else {
            self.cancel_replace_once(cx);
            return;
        };
        let replacement = inserted.repeat(pending.count.saturating_sub(1));
        self.editor.input_state.update(cx, |state, cx| {
            let text = state.text().to_string();
            let start = text[..range.start].encode_utf16().count();
            let end = start + text[range].encode_utf16().count();
            state.replace_text_in_range(Some(start..end), &replacement, window, cx);
        });
        self.set_editor_cursor(pending.start, cx);
        self.close_change_group(cx);
        self.set_vim_mode(VimMode::Normal, cx);
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
        if self.vim.search_open {
            return false;
        }
        self.clear_vim_count_and_notify(cx);
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
