//! The editor-side half of Vim mode: applies `machine` decisions to an
//! `EditorState` through its public API, for any view that hosts one.
//!
//! Normal mode is enforced by two layers, both scoped to the host's editor:
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
//!
//! A host owns a [`VimBinding`] and implements [`VimHost`]; [`VimBinding::wire`]
//! installs the listeners on the element that wraps the editor, and
//! [`VimBinding::editor`] builds the editor element with the lock applied,
//! because the element re-applies its read-only flag to the state every frame.

use super::machine::{self, VimCommand, VimKey};
use super::{VimMode, VimSettingGlobal, vim_enabled, vim_mode_label};
use crate::actions::{RunCommand, last_keystroke};
use crate::controls::{Rope, RopeExt};
use crate::primitives::Text;
use crate::tokens::{Heights, Spacing};
use dbflux_core::LogErr;
use gpui::prelude::FluentBuilder as _;
use gpui::{
    Action, App, Context, Div, Entity, EntityInputHandler as _, Focusable as _,
    InteractiveElement as _, KeyDownEvent, ParentElement as _, SharedString, Stateful, Styled as _,
    Subscription, Window, div,
};
use gpui_base::input::{EditAnchor, EditAnchorAffinity, InputCursorShape};
use gpui_component::ActiveTheme as _;
use gpui_component::input::{Editor, EditorState, InputEvent, Redo, Undo};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CHANGE_GROUP: AtomicU64 = AtomicU64::new(1);

/// A view that hosts an editor with Vim mode.
pub trait VimHost: 'static + Sized {
    /// The binding of the host's editor, or `None` while the host has no
    /// editor (a buffer that is still loading).
    fn vim(&self) -> Option<&VimBinding>;
    fn vim_mut(&mut self) -> Option<&mut VimBinding>;

    /// Whether the host keeps the text unchanged: motions and yanks work,
    /// edits do nothing.
    fn vim_read_only(&self, _cx: &App) -> bool {
        false
    }

    /// An extra gate on top of "the editor has focus", such as the host's own
    /// notion of which of its panes owns the keyboard.
    fn vim_accepts_focus(&self) -> bool {
        true
    }

    /// The text changed without an `InputEvent::Change`: an IME composition
    /// left behind by a cancelled `r`.
    fn vim_text_changed(&mut self, _cx: &mut Context<Self>) {}
}

/// Which history action a Normal-mode undo shortcut runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoryStep {
    Undo,
    Redo,
}

/// Vim state of one editor. Inert while `enabled` is false.
pub struct VimBinding {
    input: Entity<EditorState>,
    enabled: bool,
    mode: VimMode,
    /// The host's read-only and focus gates, refreshed from [`VimHost`] at
    /// every entry point.
    host_read_only: bool,
    host_accepts_focus: bool,
    /// Set when the text changed without a Change event; the entry point
    /// then tells the host.
    text_changed: bool,
    /// Set while a Normal-mode undo or redo temporarily lifts the read-only lock
    /// so the component's own handler is registered for one dispatch.
    history_unlocked: bool,
    /// Where the last vertical move left the cursor and the character column it
    /// aimed for. The goal is reused only while the cursor is still there, so
    /// any other cursor change (a click, an arrow key, an edit) resets it.
    vertical_goal: Option<(usize, usize)>,
    count: Option<usize>,
    /// Raw command keys, bounded independently of the saturating numeric count.
    pending_keys: String,
    pending_g: bool,
    pending_mark: Option<char>,
    marks: [Option<EditAnchor>; 26],
    pending_operator: Option<(char, Option<usize>)>,
    change_group: Option<u64>,
    replace_once: Option<PendingReplace>,
    /// Characters overwritten in this Replace session, innermost last: where the
    /// typed character starts and what it covered (`None` when it was appended).
    replaced: Vec<(usize, Option<String>)>,
    block_change: Option<BlockChange>,
    visual_anchor: Option<usize>,
    visual_cursor: Option<usize>,
    /// The key interceptor `r` and Replace mode need, registered the first
    /// time either starts so idle editors add nothing to every key press.
    keystroke_interceptor: Option<Subscription>,
    _focus_out: Subscription,
    _input_changes: Subscription,
    _setting: Option<Subscription>,
}

struct PendingReplace {
    start: usize,
    count: usize,
    original: String,
}

/// A Visual Block `c` waiting for Insert to end, when the text typed on its
/// first row is copied to the others.
struct BlockChange {
    /// Row where Insert started, the byte column there, and that row's length
    /// and the buffer's line count right after the block was deleted.
    row: usize,
    column: usize,
    line_len: usize,
    lines: usize,
    /// The other block rows and the byte column each receives the text at.
    targets: Vec<(usize, usize)>,
}

/// What `r` writes when its key is not delivered as typed text.
#[derive(Clone, Copy)]
enum ReplaceOnceText {
    LineBreak,
    Character(char),
}

impl VimBinding {
    /// A disabled binding for `input`. Leaving the input ends an open change
    /// group, as leaving Insert mode would, and every edit of the input
    /// completes a pending `r`.
    pub fn new<H: VimHost>(
        input: Entity<EditorState>,
        window: &mut Window,
        cx: &mut Context<H>,
    ) -> Self {
        let focus_handle = input.read(cx).focus_handle(cx);
        let focus_out = cx.on_focus_out(&focus_handle, window, |host, _, _, cx| {
            Self::blur(host, cx);
        });
        let input_changes =
            cx.subscribe_in(&input, window, |host, _, event: &InputEvent, window, cx| {
                if matches!(event, InputEvent::Change) {
                    Self::input_changed(host, window, cx);
                }
            });

        Self {
            input,
            enabled: false,
            mode: VimMode::Normal,
            host_read_only: false,
            host_accepts_focus: true,
            text_changed: false,
            history_unlocked: false,
            vertical_goal: None,
            count: None,
            pending_keys: String::new(),
            pending_g: false,
            pending_mark: None,
            marks: Default::default(),
            pending_operator: None,
            change_group: None,
            replace_once: None,
            replaced: Vec::new(),
            block_change: None,
            visual_anchor: None,
            visual_cursor: None,
            keystroke_interceptor: None,
            _focus_out: focus_out,
            _input_changes: input_changes,
            _setting: None,
        }
    }

    /// Makes the host follow [`VimSettingGlobal`] from now on, starting with
    /// its current value. Hosts that read the setting from somewhere else call
    /// [`VimBinding::set_enabled`] themselves instead.
    pub fn follow_setting<H: VimHost>(host: &mut H, cx: &mut Context<H>) {
        let subscription = cx.observe_global::<VimSettingGlobal>(|host, cx| {
            let enabled = vim_enabled(cx);
            Self::set_enabled(host, enabled, cx);
        });
        let Some(binding) = host.vim_mut() else {
            return;
        };
        binding._setting = Some(subscription);

        let enabled = vim_enabled(cx);
        Self::set_enabled(host, enabled, cx);
    }

    /// The editor this binding drives.
    pub fn input(&self) -> &Entity<EditorState> {
        &self.input
    }

    /// Copies the host's gates into the binding before it acts.
    fn refresh_host<H: VimHost>(host: &mut H, cx: &App) {
        let read_only = host.vim_read_only(cx);
        let accepts_focus = host.vim_accepts_focus();
        let Some(binding) = host.vim_mut() else {
            return;
        };
        binding.host_read_only = read_only;
        binding.host_accepts_focus = accepts_focus;
    }

    /// Tells the host about a text change that raised no Change event.
    fn report_text_change<H: VimHost>(host: &mut H, cx: &mut Context<H>) {
        let changed = host
            .vim_mut()
            .is_some_and(|binding| std::mem::take(&mut binding.text_changed));
        if changed {
            host.vim_text_changed(cx);
        }
    }

    /// Turns Vim mode on or off for the host's editor. Enabling always starts
    /// in Normal mode.
    pub fn set_enabled<H: VimHost>(host: &mut H, enabled: bool, cx: &mut Context<H>) {
        Self::refresh_host(host, cx);
        if let Some(binding) = host.vim_mut() {
            binding.set_vim_enabled(enabled, cx);
        }
    }

    /// Whether Vim mode is on.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// The current Vim mode, or `None` when Vim mode is disabled.
    pub fn mode(&self) -> Option<VimMode> {
        self.enabled.then_some(self.mode)
    }

    /// Command keys typed so far (a count, an operator, `g`).
    pub fn pending_keys(&self) -> &str {
        &self.pending_keys
    }

    /// The edit anchor of mark `index` (`0` is `a`).
    pub fn mark(&self, index: usize) -> Option<EditAnchor> {
        self.marks.get(index).copied().flatten()
    }

    /// Where a Visual selection's moving end is, while one is active.
    pub fn visual_cursor(&self) -> Option<usize> {
        self.visual_cursor
    }

    /// Whether the editor must reject user text changes right now, given
    /// whether the host itself is read-only.
    pub fn locked(&self, host_read_only: bool) -> bool {
        host_read_only || (self.enabled && !self.mode.accepts_text() && !self.history_unlocked)
    }

    /// The editor element for this binding's input, with the lock applied.
    /// Chain styling on it, but never another `readonly`: the element
    /// re-applies its flag to the state every frame.
    pub fn editor(&self, host_read_only: bool) -> Editor {
        Editor::new(&self.input).readonly(self.locked(host_read_only))
    }

    /// The key context entry reporting the Vim mode, left out while the find
    /// panel is open because its fields take every key as typed text.
    pub fn key_context_entry(&self, cx: &App) -> Option<(SharedString, SharedString)> {
        let mode = self.mode()?;

        if self.input.read(cx).search_session().open {
            return None;
        }

        Some((super::VIM_MODE_KEY.into(), mode.context_id().into()))
    }

    /// The mode indicator row, with the pending command keys, while Vim mode
    /// is on.
    pub fn render_indicator(&self, cx: &App) -> Option<Stateful<Div>> {
        let mode = self.mode()?;
        let theme = cx.theme();

        Some(
            div()
                .id("vim-mode-indicator")
                .flex()
                .flex_none()
                .items_center()
                .h(Heights::ROW_COMPACT)
                .px(Spacing::SM)
                .border_t_1()
                .border_color(theme.border)
                .bg(theme.tab_bar)
                .child(Text::caption(vim_mode_label(mode)))
                .when(!self.pending_keys.is_empty(), |el| {
                    el.child(
                        div()
                            .id("vim-pending-command")
                            .ml(Spacing::SM)
                            .child(self.pending_keys.clone()),
                    )
                }),
        )
    }

    /// Installs Vim's listeners on the element that wraps the editor: the
    /// editor's Escape, Undo and Redo actions in their capture phase, and the
    /// command keys ahead of the editor.
    pub fn wire<H: VimHost>(element: Div, cx: &mut Context<H>) -> Div {
        element
            .capture_action(
                cx.listener(|host, _: &gpui_component::input::Escape, window, cx| {
                    if Self::escape_action(host, window, cx) {
                        cx.stop_propagation();
                    }
                }),
            )
            .capture_action(cx.listener(|host, _: &Undo, window, cx| {
                if Self::history_action(host, HistoryStep::Undo, window, cx) {
                    cx.stop_propagation();
                }
            }))
            .capture_action(cx.listener(|host, _: &Redo, window, cx| {
                if Self::history_action(host, HistoryStep::Redo, window, cx) {
                    cx.stop_propagation();
                }
            }))
            .capture_key_down(cx.listener(|host, event: &KeyDownEvent, window, cx| {
                if Self::key_down(host, event, window, cx) {
                    cx.stop_propagation();
                }
            }))
    }

    /// Hands the keymap's `RunCommand` to Vim first, for hosts without a
    /// `RunCommand` capture of their own. A binding the user made wins over
    /// Vim, as in [`VimBinding::route_binding`].
    pub fn capture_run_command<H: VimHost>(element: Div, cx: &mut Context<H>) -> Div {
        element.capture_action(cx.listener(|host, action: &RunCommand, window, cx| {
            if action.from_user_binding {
                if let Some(binding) = host.vim_mut() {
                    binding.clear_vim_count_and_notify(cx);
                }
                return;
            }

            if Self::route_bound_key(host, action, window, cx) {
                cx.stop_propagation();
            }
        }))
    }

    /// Hands a host action bound to a key (a modal's Cancel or Execute, a
    /// scroll binding) to Vim first, so Vim sees keys the host binds too.
    pub fn capture_action<A: Action, H: VimHost>(element: Div, cx: &mut Context<H>) -> Div {
        element.capture_action(cx.listener(|host, action: &A, window, cx| {
            if Self::route_bound_key(host, action, window, cx) {
                cx.stop_propagation();
            }
        }))
    }

    /// Replays the last key to Vim when `action` runs because that key is
    /// bound to it here, and not because something dispatched it directly.
    /// Returns true when Vim consumed the key.
    fn route_bound_key<H: VimHost>(
        host: &mut H,
        action: &dyn Action,
        window: &mut Window,
        cx: &mut Context<H>,
    ) -> bool {
        let Some(keystroke) = last_keystroke(cx) else {
            return false;
        };

        // Every binding of the action, shadowed ones included: the action
        // usually runs because a deeper binding for the same key (the
        // editor's own Escape or Enter) let the key through.
        let bound = cx
            .key_bindings()
            .borrow()
            .bindings_for_action(action)
            .any(|binding| {
                binding
                    .keystrokes()
                    .last()
                    .is_some_and(|target| keystroke.should_match(target))
            });

        if !bound {
            return false;
        }

        let consumed = Self::key_down(
            host,
            &KeyDownEvent {
                keystroke,
                is_held: false,
                prefer_character_input: false,
            },
            window,
            cx,
        );

        if !consumed && let Some(binding) = host.vim_mut() {
            binding.clear_vim_count_and_notify(cx);
        }

        consumed
    }

    /// A keymap binding runs before key listeners, so the key it matched is
    /// replayed to Vim here. Vim takes its own keys ahead of a default
    /// binding; a binding the user made wins over Vim. When Vim leaves the
    /// key, a half-typed count or operator is dropped. Returns true when Vim
    /// consumed the key.
    pub fn route_binding<H: VimHost>(
        host: &mut H,
        from_user_binding: bool,
        window: &mut Window,
        cx: &mut Context<H>,
    ) -> bool {
        if !from_user_binding
            && let Some(keystroke) = last_keystroke(cx)
            && Self::key_down(
                host,
                &KeyDownEvent {
                    keystroke,
                    is_held: false,
                    prefer_character_input: false,
                },
                window,
                cx,
            )
        {
            return true;
        }

        if let Some(binding) = host.vim_mut() {
            binding.clear_vim_count_and_notify(cx);
        }
        false
    }

    /// Handles a key before the editor and the host's keymap see it. Returns
    /// true when the key was consumed and must not propagate.
    pub fn key_down<H: VimHost>(
        host: &mut H,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<H>,
    ) -> bool {
        Self::refresh_host(host, cx);
        let consumed = host
            .vim_mut()
            .is_some_and(|binding| binding.handle_vim_key_down(event, window, cx));
        Self::report_text_change(host, cx);
        consumed
    }

    /// See `handle_vim_escape_action`.
    pub fn escape_action<H: VimHost>(
        host: &mut H,
        window: &mut Window,
        cx: &mut Context<H>,
    ) -> bool {
        Self::refresh_host(host, cx);
        let consumed = host
            .vim_mut()
            .is_some_and(|binding| binding.handle_vim_escape_action(window, cx));
        Self::report_text_change(host, cx);
        consumed
    }

    fn history_action<H: VimHost>(
        host: &mut H,
        step: HistoryStep,
        window: &mut Window,
        cx: &mut Context<H>,
    ) -> bool {
        Self::refresh_host(host, cx);
        host.vim_mut()
            .is_some_and(|binding| binding.handle_vim_history_action(step, window, cx))
    }

    /// Completes an `r` whose character arrived as typed or composed text.
    /// The binding calls it on the input's `InputEvent::Change`; a host may
    /// call it earlier in its own Change handler, the second call is a no-op.
    pub fn input_changed<H: VimHost>(host: &mut H, window: &mut Window, cx: &mut Context<H>) {
        Self::refresh_host(host, cx);
        if let Some(binding) = host.vim_mut() {
            binding.finish_replace_once(window, cx);
        }
        Self::report_text_change(host, cx);
    }

    /// Ends the open change group when the editor loses focus, and cancels a
    /// pending `r`.
    pub fn blur<H: VimHost>(host: &mut H, cx: &mut Context<H>) {
        Self::refresh_host(host, cx);
        if let Some(binding) = host.vim_mut() {
            binding.close_change_group_on_blur(cx);
        }
        Self::report_text_change(host, cx);
    }

    /// Drops a half-typed count or operator.
    pub fn clear_pending<H: VimHost>(&mut self, cx: &mut Context<H>) {
        self.clear_vim_count_and_notify(cx);
    }

    /// Whether the editor's completion or code-action menu is open.
    pub fn menu_open(&self, cx: &App) -> bool {
        self.editor_menu_open(cx)
    }

    /// Returns focus to the editor input on the next tick.
    ///
    /// gpui-component's completion menu hides itself on Esc but never restores
    /// focus to the editor input. Synchronously the input still owns focus when
    /// Esc is observed, but the menu's notify and the resulting re-render reset
    /// the window focus before the next paint, so the refocus waits a tick.
    pub fn refocus_input<H: VimHost>(&self, window: &mut Window, cx: &mut Context<H>) {
        let input = self.input.clone();
        cx.spawn_in(window, async move |_this, cx| {
            cx.update(|window, cx| {
                input.update(cx, |state, cx| state.focus(window, cx));
            })
            .ok();
        })
        .detach();
    }

    /// Registers the interceptor that sees every key before GPUI resolves key
    /// bindings, the first time `r` or Replace mode needs it.
    ///
    /// While `r` waits for its character the editor is editable, so the platform
    /// input handler and an IME can deliver it. Editor actions bound to keys such
    /// as Backspace, Delete, the arrows or Tab run before key listeners, so only an
    /// interceptor can stop them from editing or moving the cursor first.
    fn ensure_keystroke_interceptor<H: VimHost>(&mut self, cx: &mut Context<H>) {
        if self.keystroke_interceptor.is_some() {
            return;
        }

        let host = cx.entity().downgrade();
        self.keystroke_interceptor = Some(cx.intercept_keystrokes(move |event, window, cx| {
            let Some(host) = host.upgrade() else {
                return;
            };
            let waiting = host.read(cx).vim().is_some_and(|vim| {
                vim.replace_once.is_some() || (vim.enabled && vim.mode == VimMode::Replace)
            });
            if !waiting {
                return;
            }

            let consumed = host.update(cx, |host, cx| {
                Self::refresh_host(host, cx);
                let consumed = host.vim_mut().is_some_and(|binding| {
                    binding.intercept_vim_keystroke(&event.keystroke, window, cx)
                });
                Self::report_text_change(host, cx);
                consumed
            });
            if consumed {
                cx.stop_propagation();
            }
        }));
    }
}

impl VimBinding {
    /// Turns Vim mode on or off. Enabling always starts in Normal mode.
    fn set_vim_enabled<H: VimHost>(&mut self, enabled: bool, cx: &mut Context<H>) {
        if self.enabled == enabled {
            return;
        }

        if !enabled
            && matches!(
                self.mode,
                VimMode::Visual | VimMode::VisualLine | VimMode::VisualBlock
            )
        {
            let cursor = self.visual_cursor.unwrap_or_else(|| self.editor_cursor(cx));
            self.input
                .update(cx, |state, cx| state.set_selected_range(cursor..cursor, cx));
        }

        if !enabled {
            self.replace_once = None;
            self.close_change_group(cx);
            let marks = std::mem::take(&mut self.marks);
            self.input.update(cx, |state, _cx| {
                for anchor in marks.into_iter().flatten() {
                    state.remove_edit_anchor(anchor);
                }
            });
        }

        self.enabled = enabled;
        self.mode = VimMode::Normal;
        self.history_unlocked = false;
        self.vertical_goal = None;
        self.count = None;
        self.pending_keys.clear();
        self.pending_g = false;
        self.pending_mark = None;
        self.marks = Default::default();
        self.pending_operator = None;
        self.change_group = None;
        self.replace_once = None;
        self.replaced.clear();
        self.block_change = None;
        self.visual_anchor = None;
        self.visual_cursor = None;
        self.sync_editor_lock(cx);
        self.sync_editor_cursor_shape(cx);
        self.sync_search_moves_cursor(cx);

        if enabled {
            self.clamp_cursor_for_normal(cx);
        }

        cx.notify();
    }

    /// Whether Vim may treat keys and editor actions as its commands: Vim mode is
    /// on and focus is on the editor text itself. Focus inside one of the
    /// editor's own overlays, such as the find panel's query field, leaves the
    /// keys to that overlay.
    fn vim_owns_keys(&self, window: &Window, cx: &App) -> bool {
        self.enabled
            && self.host_accepts_focus
            && self.input.read(cx).focus_handle(cx).is_focused(window)
    }

    /// Whether the editor must reject user text changes right now.
    fn editor_input_locked(&self) -> bool {
        self.locked(self.host_read_only)
    }

    /// Applies the lock immediately. Render applies it again every frame, but text
    /// can reach the input handler before the next frame (an IME commit), so a mode
    /// change must not wait for it.
    fn sync_editor_lock<H: VimHost>(&mut self, cx: &mut Context<H>) {
        let locked = self.editor_input_locked();
        self.input
            .update(cx, |state, cx| state.set_readonly(locked, cx));
    }

    /// Lets the find panel's navigation move the cursor onto the match while
    /// Vim mode is on, as `/` does in Vim. Visual modes keep the selection
    /// they own, so navigation only marks the match there.
    fn sync_search_moves_cursor<H: VimHost>(&mut self, cx: &mut Context<H>) {
        let moves_cursor = self.enabled
            && !matches!(
                self.mode,
                VimMode::Visual | VimMode::VisualLine | VimMode::VisualBlock
            );
        self.input
            .update(cx, |state, _| state.set_search_moves_cursor(moves_cursor));
    }

    fn sync_editor_cursor_shape<H: VimHost>(&mut self, cx: &mut Context<H>) {
        let shape = if self.enabled && !self.mode.accepts_text() {
            InputCursorShape::Block
        } else {
            InputCursorShape::Bar
        };
        self.input
            .update(cx, |state, cx| state.set_cursor_shape(shape, cx));
    }

    /// Handles a key before the editor and the workspace keymap see it.
    ///
    /// Returns true when the key was consumed and must not propagate.
    fn handle_vim_key_down<H: VimHost>(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<H>,
    ) -> bool {
        if !self.vim_owns_keys(window, cx) {
            self.clear_vim_count_and_notify(cx);
            return false;
        }

        if self.replace_once.is_some() && event.keystroke.key == "escape" {
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
            && !self.mode.accepts_text();
        let command = if block_key {
            Some(if self.mode == VimMode::VisualBlock {
                VimCommand::LeaveVisual
            } else {
                VimCommand::EnterVisualBlock
            })
        } else {
            machine::command_for(self.mode, key)
        };
        if let Some(prefix) = self.pending_mark.take() {
            self.pending_keys.clear();
            cx.notify();
            if !key.command_modifier && !key.shift && key.key.len() == 1 {
                let letter = key.key.as_bytes()[0];
                if letter.is_ascii_lowercase() {
                    let index = usize::from(letter - b'a');
                    if prefix == 'm' {
                        let cursor = self.editor_cursor(cx);
                        self.input.update(cx, |state, _| {
                            if let Some(previous) = self.marks[index].take() {
                                state.remove_edit_anchor(previous);
                            }
                            self.marks[index] =
                                state.create_edit_anchor(cursor, EditAnchorAffinity::Right);
                        });
                    } else {
                        let destination = {
                            let target = self.input.read(cx);
                            self.marks[index]
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
                            self.vertical_goal = None;
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
            let interrupted = self.pending_g || self.pending_operator.is_some();
            self.clear_vim_count_and_notify(cx);
            self.pending_mark = Some(if interrupted { '\0' } else { prefix });
            if !interrupted {
                self.push_pending_key(prefix, cx);
            }
            return true;
        }
        if self.pending_g {
            self.pending_g = false;
            self.pending_keys.clear();
            cx.notify();
            if command == VimCommand::PendingG {
                if let Some((operator, prefix)) = self.pending_operator.take() {
                    let count = prefix
                        .unwrap_or(1)
                        .saturating_mul(self.count.take().unwrap_or(1));
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
            self.pending_g = true;
            self.push_pending_key('g', cx);
            return true;
        }

        if let VimCommand::Digit(digit) = command
            && (digit != 0 || self.count.is_some() || self.pending_operator.is_some())
        {
            self.push_pending_key(char::from(b'0' + digit), cx);
            self.count = Some(
                self.count
                    .unwrap_or(0)
                    .saturating_mul(10)
                    .saturating_add(digit as usize),
            );
            return true;
        }
        if let Some((operator, prefix)) = self.pending_operator.take() {
            self.pending_keys.clear();
            cx.notify();
            let inner = self.count.take();
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
                let state = self.input.read(cx);
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
            self.pending_keys.clear();
            cx.notify();
        }
        self.apply_vim_command(command, window, cx);
        true
    }

    fn apply_vim_command<H: VimHost>(
        &mut self,
        command: VimCommand,
        window: &mut Window,
        cx: &mut Context<H>,
    ) {
        let explicit_count = self.count.take();
        let count = explicit_count.unwrap_or(1);
        match command {
            VimCommand::Digit(0) => self.move_cursor_with(machine::line_start, cx),
            VimCommand::Digit(_) | VimCommand::PendingG | VimCommand::PendingMark(_) => {}
            VimCommand::FirstLine | VimCommand::LastLine => {
                let target = {
                    let state = self.input.read(cx);
                    let row = if command == VimCommand::FirstLine || explicit_count.is_some() {
                        count.saturating_sub(1)
                    } else {
                        state.text().lines_len().saturating_sub(1)
                    };
                    machine::absolute_line(state.text(), row)
                };
                self.vertical_goal = None;
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
            VimCommand::EnterReplace if !self.host_read_only => self.enter_replace(cx),
            VimCommand::EnterVisual
            | VimCommand::EnterVisualLine
            | VimCommand::EnterVisualBlock => {
                let cursor = self.visual_cursor.unwrap_or_else(|| self.editor_cursor(cx));
                if self.visual_anchor.is_none() {
                    self.visual_anchor = Some(cursor);
                }
                self.visual_cursor = Some(cursor);
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
                let cursor = self.visual_cursor.unwrap_or_else(|| self.editor_cursor(cx));
                self.visual_anchor = None;
                self.visual_cursor = None;
                self.set_vim_mode(VimMode::Normal, cx);
                self.set_editor_cursor(cursor, cx);
                self.schedule_editor_refocus(window, cx);
            }
            VimCommand::LeaveInsert => self.leave_insert(window, cx),
            VimCommand::VisualDelete if !self.host_read_only => {
                self.apply_visual_operator(true, window, cx)
            }
            VimCommand::VisualYank => self.apply_visual_operator(false, window, cx),
            VimCommand::VisualChange if !self.host_read_only => {
                self.apply_visual_change(window, cx);
            }
            VimCommand::VisualDelete | VimCommand::VisualChange => {}
            // A read-only document keeps its text: motions work, edits do nothing.
            VimCommand::Operator(operator) => {
                self.pending_operator = Some((operator, explicit_count));
                self.push_pending_key(operator, cx);
            }
            VimCommand::DeleteChar if !self.host_read_only => self.delete_chars(count, window, cx),
            VimCommand::ReplaceOnce if !self.host_read_only => self.start_replace_once(count, cx),
            VimCommand::Undo if !self.host_read_only => {
                self.run_history_in_normal_mode(HistoryStep::Undo, count, window, cx)
            }
            VimCommand::OpenSearch => {
                self.input
                    .update(cx, |state, cx| state.open_search(false, cx));
            }
            VimCommand::RepeatSearch(reverse) => self.repeat_native_search(reverse, count, cx),
            VimCommand::DeleteChar
            | VimCommand::ReplaceOnce
            | VimCommand::EnterReplace
            | VimCommand::Undo => {}
        }
    }

    /// Repeats the find panel's query from the cursor, `count` times, moving
    /// the cursor onto the match and making it the panel's current match.
    fn repeat_native_search<H: VimHost>(
        &mut self,
        backward: bool,
        count: usize,
        cx: &mut Context<H>,
    ) {
        self.vertical_goal = None;

        self.input.update(cx, |state, cx| {
            for _ in 0..count.min(10_000) {
                let reached = if backward {
                    state.previous_search_match(cx)
                } else {
                    state.next_search_match(cx)
                };

                if reached.is_none() {
                    break;
                }
            }
        });

        self.clamp_cursor_for_normal(cx);
    }

    fn apply_absolute_operator<H: VimHost>(
        &mut self,
        operator: char,
        target_row: usize,
        window: &mut Window,
        cx: &mut Context<H>,
    ) {
        let range = {
            let state = self.input.read(cx);
            machine::absolute_operator_range(state.text(), state.cursor(), target_row)
        };
        self.apply_motion_operator(operator, range, true, window, cx);
    }

    fn push_pending_key<H: VimHost>(&mut self, key: char, cx: &mut Context<H>) {
        if self.pending_keys.len() < 32 {
            self.pending_keys.push(key);
        }
        cx.notify();
    }

    fn clear_vim_count(&mut self) {
        self.count = None;
        self.pending_g = false;
        self.pending_mark = None;
        self.pending_operator = None;
        self.pending_keys.clear();
    }

    fn clear_vim_count_and_notify<H: VimHost>(&mut self, cx: &mut Context<H>) {
        if !self.pending_keys.is_empty() {
            self.clear_vim_count();
            cx.notify();
        } else {
            self.clear_vim_count();
        }
    }

    fn set_vim_mode<H: VimHost>(&mut self, mode: VimMode, cx: &mut Context<H>) {
        self.clear_vim_count_and_notify(cx);
        self.mode = mode;
        if mode == VimMode::Replace || self.replace_once.is_some() {
            self.ensure_keystroke_interceptor(cx);
        }
        self.vertical_goal = None;
        self.sync_editor_lock(cx);
        self.sync_editor_cursor_shape(cx);
        self.sync_search_moves_cursor(cx);
        cx.notify();
    }

    /// Handles the editor's Escape action in its capture phase, before the
    /// component's own handler runs.
    ///
    /// With a completion or code-action menu open in Insert mode, Esc closes the
    /// menu and stays in Insert mode. The component would close the menu too, but
    /// it lets the key propagate afterwards, which would then leave Insert mode.
    /// Returns true when the action was consumed.
    fn handle_vim_escape_action<H: VimHost>(
        &mut self,
        window: &mut Window,
        cx: &mut Context<H>,
    ) -> bool {
        self.clear_vim_count_and_notify(cx);
        if self.replace_once.is_some() {
            self.cancel_replace_once(cx);
            return true;
        }
        if !self.vim_owns_keys(window, cx) {
            return false;
        }
        if matches!(
            self.mode,
            VimMode::Visual | VimMode::VisualLine | VimMode::VisualBlock
        ) {
            self.apply_vim_command(VimCommand::LeaveVisual, window, cx);
            return true;
        }
        if !self.mode.accepts_text() || !self.editor_menu_open(cx) {
            return false;
        }

        self.dismiss_editor_menus(cx);
        self.schedule_editor_refocus(window, cx);
        true
    }

    fn editor_menu_open(&self, cx: &App) -> bool {
        let state = self.input.read(cx);
        state.completion_menu_state().open || state.code_action_menu_state().open
    }

    fn dismiss_editor_menus<H: VimHost>(&mut self, cx: &mut Context<H>) {
        self.input.update(cx, |state, cx| {
            state.dismiss_completion_overlay(cx);
            state.dismiss_code_action_overlay(cx);
        });
    }

    fn leave_insert<H: VimHost>(&mut self, window: &mut Window, cx: &mut Context<H>) {
        if self.editor_menu_open(cx) {
            // Normally consumed by `handle_vim_escape_action` already; kept so a
            // menu opened some other way still closes before the mode changes.
            self.dismiss_editor_menus(cx);
        } else {
            self.finish_block_change(window, cx);
            self.close_change_group(cx);
            self.set_vim_mode(machine::mode_after(self.mode, VimCommand::LeaveInsert), cx);

            // Leaving Insert mode steps the cursor back onto the character it
            // was after, as Vim does.
            self.move_cursor_with(machine::step_left, cx);
        }

        self.schedule_editor_refocus(window, cx);
    }

    fn editor_cursor(&self, cx: &App) -> usize {
        self.input.read(cx).cursor()
    }

    fn set_editor_cursor<H: VimHost>(&mut self, offset: usize, cx: &mut Context<H>) {
        if matches!(
            self.mode,
            VimMode::Visual | VimMode::VisualLine | VimMode::VisualBlock
        ) {
            self.visual_cursor = Some(offset);
            self.update_visual_selection(cx);
        } else {
            self.input
                .update(cx, |state, cx| state.set_selected_range(offset..offset, cx));
        }
    }

    fn update_visual_selection<H: VimHost>(&mut self, cx: &mut Context<H>) {
        let (Some(anchor), Some(cursor)) = (self.visual_anchor, self.visual_cursor) else {
            return;
        };
        self.input.update(cx, |state, cx| {
            let range = machine::visual_range(
                state.text(),
                anchor,
                cursor,
                self.mode == VimMode::VisualLine,
            );
            if self.mode == VimMode::VisualBlock {
                state.set_columnar_selection(anchor, cursor, cx);
            } else {
                state.set_selected_range(range, cx);
                state.set_visual_caret(Some(cursor), cx);
            }
        });
    }

    fn motion_cursor(&self, cx: &App) -> usize {
        self.visual_cursor.unwrap_or_else(|| self.editor_cursor(cx))
    }

    fn move_cursor_with<H: VimHost>(
        &mut self,
        step: fn(&Rope, usize) -> usize,
        cx: &mut Context<H>,
    ) {
        let target = step(self.input.read(cx).text(), self.motion_cursor(cx));

        self.vertical_goal = None;
        self.set_editor_cursor(target, cx);
    }

    fn repeat_cursor<H: VimHost>(
        &mut self,
        step: fn(&Rope, usize) -> usize,
        count: usize,
        cx: &mut Context<H>,
    ) {
        let mut target = self.motion_cursor(cx);
        {
            let text = self.input.read(cx);
            for _ in 0..count.min(text.text().len().saturating_add(1)) {
                let next = step(text.text(), target);
                if next == target {
                    break;
                }
                target = next;
            }
        }
        self.vertical_goal = None;
        self.set_editor_cursor(target, cx);
    }

    fn move_word<H: VimHost>(
        &mut self,
        motion: machine::WordMotion,
        big: bool,
        count: usize,
        cx: &mut Context<H>,
    ) {
        let mut target = self.motion_cursor(cx);
        {
            let text = self.input.read(cx);
            let chars = machine::word_offsets(text.text());
            for _ in 0..count.min(chars.len().saturating_add(1)) {
                let next = machine::step_word(text.text(), &chars, target, motion, big);
                if next == target {
                    break;
                }
                target = next;
            }
        }
        self.vertical_goal = None;
        self.set_editor_cursor(target, cx);
    }

    fn repeat_vertical<H: VimHost>(&mut self, delta: isize, count: usize, cx: &mut Context<H>) {
        for _ in 0..count.min(self.input.read(cx).text().lines_len()) {
            let before = self.motion_cursor(cx);
            self.move_cursor_vertically(delta, cx);
            if self.motion_cursor(cx) == before {
                break;
            }
        }
    }

    fn move_cursor_vertically<H: VimHost>(&mut self, delta: isize, cx: &mut Context<H>) {
        let cursor = self.motion_cursor(cx);
        let goal = self
            .vertical_goal
            .filter(|(offset, _)| *offset == cursor)
            .map(|(_, column)| column);

        let Some(step) = machine::step_vertical(self.input.read(cx).text(), cursor, delta, goal)
        else {
            return;
        };

        self.set_editor_cursor(step.offset, cx);
        self.vertical_goal = Some((step.offset, step.goal_column));
    }

    fn clamp_cursor_for_normal<H: VimHost>(&mut self, cx: &mut Context<H>) {
        let cursor = self.editor_cursor(cx);
        let clamped = machine::clamp_to_character(self.input.read(cx).text(), cursor);

        if clamped != cursor {
            self.set_editor_cursor(clamped, cx);
        }
    }

    fn apply_visual_change<H: VimHost>(&mut self, window: &mut Window, cx: &mut Context<H>) {
        if self.mode == VimMode::VisualBlock {
            self.apply_visual_block_change(window, cx);
            return;
        }
        let (range, clipboard) = {
            let state = self.input.read(cx);
            let content = state.text().to_string();
            let selected = state.selected_range();
            let clipboard = if self.mode == VimMode::VisualLine {
                machine::line_yank_text(&content, selected.clone())
            } else {
                content.get(selected.clone())
            };
            let range = if self.mode == VimMode::VisualLine && !selected.is_empty() {
                machine::change_line_range(state.text(), selected)
            } else {
                selected
            };
            (
                range,
                clipboard.filter(|text| !text.is_empty()).map(str::to_owned),
            )
        };
        let anchor = self.visual_anchor.unwrap_or(range.start);
        self.input.update(cx, |state, cx| {
            state.set_selected_range(anchor..anchor, cx);
        });
        self.apply_change(range, clipboard, window, cx);
        self.visual_anchor = None;
        self.visual_cursor = None;
        self.schedule_editor_refocus(window, cx);
    }

    /// Deletes the block on every row that reaches its left column and starts
    /// Insert on the first such row. Leaving Insert copies the typed text to the
    /// other rows (`finish_block_change`), all in one undo step.
    fn apply_visual_block_change<H: VimHost>(&mut self, window: &mut Window, cx: &mut Context<H>) {
        let (Some(anchor), Some(cursor)) = (self.visual_anchor, self.visual_cursor) else {
            return;
        };
        let (rows, columns, clipboard) = {
            let state = self.input.read(cx);
            let text = state.text();
            let content = text.to_string();
            let rows = machine::block_rows(text, anchor, cursor);
            let columns: Vec<(usize, usize)> = rows
                .iter()
                .map(|(row, range)| (*row, range.start - text.line_start_offset(*row)))
                .collect();
            let clipboard = rows
                .iter()
                .filter_map(|(_, range)| content.get(range.clone()))
                .collect::<Vec<_>>()
                .join("\n");
            (rows, columns, clipboard)
        };

        let group = NEXT_CHANGE_GROUP.fetch_add(1, Ordering::Relaxed);
        let started = self
            .input
            .update(cx, |state, _| state.begin_edit_group(group));
        if !started {
            return;
        }
        self.change_group = Some(group);
        self.vertical_goal = None;
        if !clipboard.is_empty() {
            cx.write_to_clipboard(gpui::ClipboardItem::new_string(clipboard));
        }

        self.input.update(cx, |state, cx| {
            for (_, range) in rows.iter().rev().filter(|(_, range)| !range.is_empty()) {
                state.set_selected_range(range.clone(), cx);
                state.replace("", window, cx);
            }
        });

        let block_change = {
            let state = self.input.read(cx);
            let text = state.text();
            columns.split_first().map(|(&(row, column), targets)| {
                (
                    text.line_start_offset(row) + column,
                    BlockChange {
                        row,
                        column,
                        line_len: machine::line_content_len(text, row),
                        lines: text.lines_len(),
                        targets: targets.to_vec(),
                    },
                )
            })
        };

        let insert_at = block_change
            .as_ref()
            .map_or_else(|| anchor.min(cursor), |(offset, _)| *offset);
        self.block_change = block_change.map(|(_, change)| change);
        self.visual_anchor = None;
        self.visual_cursor = None;
        self.set_vim_mode(VimMode::Insert, cx);
        self.input.update(cx, |state, cx| {
            state.set_selected_range(insert_at..insert_at, cx);
        });
        self.schedule_editor_refocus(window, cx);
    }

    /// Copies the text typed on the first row of a Visual Block change into the
    /// same column of the other rows. Nothing is copied when the insert added a
    /// line break or removed text, as in Vim, or while a composition is open.
    fn finish_block_change<H: VimHost>(&mut self, window: &mut Window, cx: &mut Context<H>) {
        let Some(block) = self.block_change.take() else {
            return;
        };

        let edits = {
            let state = self.input.read(cx);
            let text = state.text();
            if state.has_active_composition() || text.lines_len() != block.lines {
                return;
            }
            let line_len = machine::line_content_len(text, block.row);
            let Some(inserted_len) = line_len.checked_sub(block.line_len) else {
                return;
            };
            let line_start = text.line_start_offset(block.row);
            let content = text.to_string();
            let Some(inserted) = content
                .get(line_start + block.column..line_start + block.column + inserted_len)
                .filter(|inserted| !inserted.is_empty())
                .map(str::to_owned)
            else {
                return;
            };
            let offsets: Vec<usize> = block
                .targets
                .iter()
                .map(|(row, column)| text.line_start_offset(*row) + column)
                .collect();
            (inserted, offsets, state.selected_range())
        };

        let (inserted, offsets, selection) = edits;
        self.input.update(cx, |state, cx| {
            for offset in offsets.into_iter().rev() {
                state.set_selected_range(offset..offset, cx);
                state.replace(inserted.clone(), window, cx);
            }
            state.set_selected_range(selection, cx);
        });
    }

    fn apply_visual_operator<H: VimHost>(
        &mut self,
        delete: bool,
        window: &mut Window,
        cx: &mut Context<H>,
    ) {
        let mode = self.mode;
        let (selected, delete_range) = {
            let state = self.input.read(cx);
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
            self.input.update(cx, |state, cx| {
                if let Some(range) = delete_range {
                    state.set_selected_range(range, cx);
                }
                // The native multi-cursor replace applies disjoint block fragments
                // in one history transaction; never collapse them into one span.
                state.replace("", window, cx);
            });
        }
        let cursor = self
            .visual_anchor
            .unwrap_or(0)
            .min(self.visual_cursor.unwrap_or(0));
        self.visual_anchor = None;
        self.visual_cursor = None;
        self.set_vim_mode(VimMode::Normal, cx);
        self.input.update(cx, |state, cx| {
            let cursor = machine::clamp_to_character(state.text(), cursor.min(state.text().len()));
            state.set_selected_range(cursor..cursor, cx);
        });
        self.schedule_editor_refocus(window, cx);
    }

    fn apply_line_operator<H: VimHost>(
        &mut self,
        operator: char,
        count: usize,
        window: &mut Window,
        cx: &mut Context<H>,
    ) {
        if operator == 'c' {
            if self.host_read_only {
                return;
            }
            let range = {
                let state = self.input.read(cx);
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
            let state = self.input.read(cx);
            machine::counted_line_range(state.text(), self.editor_cursor(cx), count)
        };
        let content = self.input.read(cx).text().to_string();
        let selected = if operator == 'y' {
            machine::line_yank_text(&content, range.clone())
        } else {
            content.get(range.clone())
        }
        .unwrap_or_default()
        .to_string();
        if operator == 'd' && self.host_read_only {
            return;
        }
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(selected));
        if operator == 'd' {
            let delete_range = {
                let state = self.input.read(cx);
                machine::line_delete_range(state.text(), range)
            };
            if delete_range.is_empty() {
                return;
            }
            self.vertical_goal = None;
            self.input.update(cx, |state, cx| {
                state.set_selected_range(delete_range, cx);
                state.replace("", window, cx);
            });
            self.clamp_cursor_for_normal(cx);
        }
    }

    fn apply_motion_operator<H: VimHost>(
        &mut self,
        operator: char,
        range: std::ops::Range<usize>,
        linewise: bool,
        window: &mut Window,
        cx: &mut Context<H>,
    ) {
        if matches!(operator, 'd' | 'c') && self.host_read_only {
            return;
        }
        let selected = {
            let state = self.input.read(cx);
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
                machine::change_line_range(self.input.read(cx).text(), range)
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
                machine::line_delete_range(self.input.read(cx).text(), range)
            } else {
                range
            };
            if !delete_range.is_empty() {
                self.vertical_goal = None;
                self.input.update(cx, |state, cx| {
                    state.set_selected_range(delete_range, cx);
                    state.replace("", window, cx);
                });
                self.clamp_cursor_for_normal(cx);
            }
        }
    }

    fn apply_word_operator<H: VimHost>(
        &mut self,
        operator: char,
        motion: machine::WordMotion,
        big: bool,
        count: usize,
        window: &mut Window,
        cx: &mut Context<H>,
    ) {
        if matches!(operator, 'd' | 'c') && self.host_read_only {
            return;
        }
        if operator == 'c' {
            let range = {
                let state = self.input.read(cx);
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
            let state = self.input.read(cx);
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
            self.vertical_goal = None;
            self.input.update(cx, |state, cx| {
                state.set_selected_range(range, cx);
                state.replace("", window, cx);
            });
            self.clamp_cursor_for_normal(cx);
        }
    }

    fn apply_change<H: VimHost>(
        &mut self,
        range: std::ops::Range<usize>,
        clipboard: Option<String>,
        window: &mut Window,
        cx: &mut Context<H>,
    ) {
        if self.host_read_only {
            return;
        }
        let content = self.input.read(cx).text().to_string();
        let Some(selected) = content.get(range.clone()) else {
            return;
        };
        let group = NEXT_CHANGE_GROUP.fetch_add(1, Ordering::Relaxed);
        let started = self
            .input
            .update(cx, |state, _| state.begin_edit_group(group));
        if !started {
            return;
        }
        if !selected.is_empty() || clipboard.is_some() {
            cx.write_to_clipboard(gpui::ClipboardItem::new_string(
                clipboard.unwrap_or_else(|| selected.to_string()),
            ));
        }
        self.change_group = Some(group);
        self.vertical_goal = None;
        if !range.is_empty() {
            self.input.update(cx, |state, cx| {
                state.set_selected_range(range, cx);
                state.replace("", window, cx);
            });
        }
        self.set_vim_mode(VimMode::Insert, cx);
    }

    fn close_change_group_on_blur<H: VimHost>(&mut self, cx: &mut Context<H>) {
        if self.replace_once.is_some() {
            self.cancel_replace_once(cx);
            return;
        }
        if self.change_group.is_some() {
            self.block_change = None;
            self.close_change_group(cx);
            self.set_vim_mode(VimMode::Normal, cx);
        }
    }

    fn close_change_group<H: VimHost>(&mut self, cx: &mut Context<H>) {
        if let Some(group) = self.change_group.take() {
            self.input.update(cx, |state, _| {
                state.request_end_edit_group(group);
            });
        }
    }

    fn start_replace_once<H: VimHost>(&mut self, count: usize, cx: &mut Context<H>) {
        let pending = {
            let state = self.input.read(cx);
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
            .input
            .update(cx, |state, _| state.begin_edit_group(group));
        if !started {
            return;
        }
        self.change_group = Some(group);
        self.input.update(cx, |state, cx| {
            state.set_selected_range(pending.start..pending.start, cx);
        });
        self.replace_once = Some(pending);
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
    fn intercept_vim_keystroke<H: VimHost>(
        &mut self,
        keystroke: &gpui::Keystroke,
        window: &mut Window,
        cx: &mut Context<H>,
    ) -> bool {
        {
            let state = self.input.read(cx);
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
        if self.replace_once.is_none() {
            return !shortcut && self.intercept_replace_mode_key(keystroke, printable, window, cx);
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
    /// this session overwrote. Every other key, including shortcuts, keeps its
    /// Insert-mode behavior.
    fn intercept_replace_mode_key<H: VimHost>(
        &mut self,
        keystroke: &gpui::Keystroke,
        typed_character: bool,
        window: &mut Window,
        cx: &mut Context<H>,
    ) -> bool {
        if keystroke.key == "backspace" {
            self.replace_mode_backspace(window, cx);
            return true;
        }
        if !typed_character || machine::is_editing_key(&keystroke.key) {
            return false;
        }

        let (cursor, covered) = {
            let state = self.input.read(cx);
            let selection = state.selected_range();
            if !selection.is_empty() {
                return false;
            }
            let covered = machine::counted_character_range(state.text(), selection.start, 1);
            (selection.start, covered)
        };
        let original = covered.map(|range| {
            let content = self.input.read(cx).text().slice(range.clone()).to_string();
            self.input
                .update(cx, |state, cx| state.set_selected_range(range, cx));
            content
        });
        self.replaced.push((cursor, original));
        false
    }

    fn replace_mode_backspace<H: VimHost>(&mut self, window: &mut Window, cx: &mut Context<H>) {
        let (cursor, previous) = {
            let state = self.input.read(cx);
            let cursor = state.cursor();
            (cursor, machine::step_left(state.text(), cursor))
        };

        let restorable = self
            .replaced
            .last()
            .is_some_and(|(start, _)| *start == previous && previous < cursor);
        if !restorable {
            self.replaced.clear();
            self.set_editor_cursor(previous, cx);
            return;
        }

        if let Some((start, original)) = self.replaced.pop() {
            self.input.update(cx, |state, cx| {
                state.set_selected_range(start..cursor, cx);
                state.replace(original.unwrap_or_default(), window, cx);
            });
            self.set_editor_cursor(start, cx);
        }
    }

    fn enter_replace<H: VimHost>(&mut self, cx: &mut Context<H>) {
        let group = NEXT_CHANGE_GROUP.fetch_add(1, Ordering::Relaxed);
        let started = self
            .input
            .update(cx, |state, _| state.begin_edit_group(group));
        if !started {
            return;
        }

        self.change_group = Some(group);
        self.replaced.clear();
        self.set_vim_mode(VimMode::Replace, cx);
    }

    fn replace_once_directly<H: VimHost>(
        &mut self,
        text: ReplaceOnceText,
        window: &mut Window,
        cx: &mut Context<H>,
    ) {
        let Some(pending) = self.replace_once.take() else {
            return;
        };

        let edit = {
            let state = self.input.read(cx);
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
            self.input.update(cx, |state, cx| {
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

    fn cancel_replace_once<H: VimHost>(&mut self, cx: &mut Context<H>) {
        let pending = self.replace_once.take();
        let marked_text_changed = pending.is_some_and(|pending| {
            let state = self.input.read(cx);
            state.has_active_composition() && *state.text() != pending.original
        });
        self.close_change_group(cx);
        self.set_vim_mode(VimMode::Normal, cx);
        if marked_text_changed && !self.host_read_only {
            self.text_changed = true;
        }
    }

    fn finish_replace_once<H: VimHost>(&mut self, window: &mut Window, cx: &mut Context<H>) {
        if self.replace_once.is_none() {
            return;
        };
        if self.host_read_only {
            self.cancel_replace_once(cx);
            return;
        }
        if self.input.read(cx).has_active_composition() {
            return;
        }
        let current = self.input.read(cx).text().to_string();
        if self
            .replace_once
            .as_ref()
            .is_some_and(|pending| pending.original == current)
        {
            return;
        }
        let Some(pending) = self.replace_once.take() else {
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
            self.input.read(cx).text(),
            pending.start + inserted.len(),
            pending.count,
        ) else {
            self.cancel_replace_once(cx);
            return;
        };
        let replacement = inserted.repeat(pending.count.saturating_sub(1));
        self.input.update(cx, |state, cx| {
            let text = state.text().to_string();
            let start = text[..range.start].encode_utf16().count();
            let end = start + text[range].encode_utf16().count();
            state.replace_text_in_range(Some(start..end), &replacement, window, cx);
        });
        self.set_editor_cursor(pending.start, cx);
        self.close_change_group(cx);
        self.set_vim_mode(VimMode::Normal, cx);
    }

    fn delete_chars<H: VimHost>(&mut self, count: usize, window: &mut Window, cx: &mut Context<H>) {
        let range = {
            let text = self.input.read(cx);
            let Some(range) =
                machine::counted_character_range(text.text(), self.editor_cursor(cx), count)
            else {
                return;
            };
            range
        };

        self.vertical_goal = None;
        self.input.update(cx, |state, cx| {
            state.set_selected_range(range, cx);
            state.replace("", window, cx);
        });

        self.clamp_cursor_for_normal(cx);
    }

    /// Handles the editor's Undo and Redo actions (`Ctrl+Z`, `Ctrl+Y`, ...) in
    /// their capture phase. The locked Normal-mode input does not register its
    /// own handlers, so without this those shortcuts would stop working while
    /// Vim mode is on. Returns true when the action was consumed.
    fn handle_vim_history_action<H: VimHost>(
        &mut self,
        step: HistoryStep,
        window: &mut Window,
        cx: &mut Context<H>,
    ) -> bool {
        self.clear_vim_count_and_notify(cx);
        if !self.vim_owns_keys(window, cx)
            || self.mode != VimMode::Normal
            || self.history_unlocked
            || self.host_read_only
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
    fn run_history_in_normal_mode<H: VimHost>(
        &mut self,
        step: HistoryStep,
        count: usize,
        window: &mut Window,
        cx: &mut Context<H>,
    ) {
        self.history_unlocked = true;
        self.vertical_goal = None;
        self.sync_editor_lock(cx);

        let input = self.input.clone();
        let host = cx.entity().downgrade();

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

            host.update(cx, |host, cx| {
                Self::refresh_host(host, cx);
                if let Some(binding) = host.vim_mut() {
                    binding.history_unlocked = false;
                    binding.sync_editor_lock(cx);
                    binding.clamp_cursor_for_normal(cx);
                }
                cx.notify();
            })
            .log_err();
        });
    }

    /// Returns focus to the editor input on the next tick, while the host
    /// lets Vim own its keys (see [`VimBinding::refocus_input`]).
    fn schedule_editor_refocus<H: VimHost>(&self, window: &mut Window, cx: &mut Context<H>) {
        if !self.host_accepts_focus {
            return;
        }

        self.refocus_input(window, cx);
    }
}
