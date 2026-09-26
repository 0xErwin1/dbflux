use super::SettingsSection;
use super::SettingsSectionId;
use super::section_trait::SectionFocusEvent;
use dbflux_app::keymap::{
    BindingSlot, ContextId, KeyChord, KeySequence, KeybindingRecorder, KeymapOverrides, Modifiers,
    RecordingOutcome, RecordingState, save_keymap_overrides,
};
use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_core::LogErr;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::keymap::{
    GpuiPredicateOverlap, PredicateError, apply_keymap_overrides, key_chord_from_gpui,
    keymap_overrides, validate_predicate,
};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::*;
use gpui::*;
use std::collections::HashSet;
use std::time::Duration;

/// How long the recorder waits after the last key before it saves the
/// sequence, the same pause after which GPUI stops waiting for the next key
/// of a sequence.
const RECORDING_PAUSE: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum KeybindingsSelection {
    Context(usize),
    Binding(usize, usize),
}

impl KeybindingsSelection {
    pub(super) fn context_idx(&self) -> usize {
        match self {
            Self::Context(idx) | Self::Binding(idx, _) => *idx,
        }
    }
}

/// One row of a context group: a default binding with the keys and the
/// context predicate it has once the user's overrides apply.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct BindingEntry {
    pub(super) slot: BindingSlot,
    /// `None` when the user removed the binding's shortcut.
    pub(super) keys: Option<KeySequence>,
    /// The predicate the user gave the binding; `None` keeps the context's
    /// default predicate.
    pub(super) custom_predicate: Option<String>,
    /// The binding is declared by a parent context and reaches this one.
    pub(super) is_inherited: bool,
}

pub(super) enum KeybindingsListItem {
    ContextHeader {
        context: ContextId,
        ctx_idx: usize,
        is_expanded: bool,
        is_selected: bool,
        binding_count: usize,
    },
    Binding {
        entry: BindingEntry,
        is_selected: bool,
        is_recording: bool,
        is_overridden: bool,
        /// Context of a binding the row collides with, for its badge.
        conflict_context: Option<ContextId>,
        /// Why the binding may not behave as its keys suggest.
        warning: Option<String>,
        ctx_idx: usize,
        binding_idx: usize,
    },
    /// Decision banner under the row whose new keys collide with other
    /// bindings: cancel, or replace them.
    PendingConflict {
        keys: KeySequence,
        target: BindingSlot,
        conflicts: Vec<BindingSlot>,
    },
    /// Context predicate editor under the row whose predicate is edited.
    PredicateEditor { slot: BindingSlot },
}

/// State of the context predicate editor open under one binding row.
pub(super) struct PredicateEditing {
    pub(super) slot: BindingSlot,
    pub(super) input: Entity<InputState>,
    /// Why the last attempt to save could not be accepted.
    pub(super) error: Option<PredicateError>,
}

pub(super) struct KeybindingsSection {
    pub(super) app_state: Entity<AppStateEntity>,
    pub(super) keybindings_filter: Entity<InputState>,
    pub(super) context_filter: Entity<Dropdown>,
    /// Context chosen in the context filter; `None` shows every context.
    pub(super) context_filter_value: Option<ContextId>,
    pub(super) keybindings_expanded: HashSet<ContextId>,
    pub(super) keybindings_selection: KeybindingsSelection,
    pub(super) keybindings_editing_filter: bool,
    pub(super) keybindings_scroll_handle: ScrollHandle,
    pub(super) keybindings_pending_scroll: Option<usize>,
    pub(super) content_focused: bool,
    /// The saved overrides; the effective keymap is built from these.
    pub(super) overrides: KeymapOverrides,
    pub(super) recorder: KeybindingRecorder,
    /// Context group whose row started the recording. A binding inherited
    /// by several contexts shows the recording only in that group.
    pub(super) recording_group: Option<ContextId>,
    /// Ends the recorded sequence after a pause; replaced on every key.
    recording_pause: Option<Task<()>>,
    /// The context predicate editor, while one is open.
    pub(super) predicate_editing: Option<PredicateEditing>,
    /// Evaluates predicates for conflict detection; built once.
    pub(super) overlap: GpuiPredicateOverlap,
    _subscriptions: Vec<Subscription>,
}

impl KeybindingsSection {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let keybindings_filter = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("settings.keybindings.filter_placeholder"))
        });

        let context_filter = cx.new(|_cx| {
            Dropdown::new("keybindings-context-filter")
                .items(context_filter_items())
                .selected_index(Some(0))
                .leading_icon(AppIcon::Layers)
        });

        let context_subscription = cx.subscribe(
            &context_filter,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.context_filter_value = context_for_filter_index(event.index);
                this.keybindings_selection =
                    KeybindingsSelection::Context(this.first_visible_context(cx));
                this.keybindings_pending_scroll = Some(0);
                cx.notify();
            },
        );

        let recording_interceptor = Self::intercept_recording_keys(window, cx);

        let mut keybindings_expanded = HashSet::new();
        keybindings_expanded.insert(ContextId::Global);

        Self {
            app_state,
            keybindings_filter,
            context_filter,
            context_filter_value: None,
            keybindings_expanded,
            keybindings_selection: KeybindingsSelection::Context(0),
            keybindings_editing_filter: false,
            keybindings_scroll_handle: ScrollHandle::new(),
            keybindings_pending_scroll: None,
            content_focused: false,
            overrides: keymap_overrides(),
            recorder: KeybindingRecorder::new(),
            recording_group: None,
            recording_pause: None,
            predicate_editing: None,
            overlap: GpuiPredicateOverlap::new(),
            _subscriptions: vec![context_subscription, recording_interceptor],
        }
    }

    /// Captures every key press of this window while a shortcut is being
    /// recorded, before key bindings and key listeners see it, so keys the
    /// settings window or the app already use (Ctrl+S, Ctrl+W, Tab) can be
    /// recorded too.
    fn intercept_recording_keys(window: &mut Window, cx: &mut Context<Self>) -> Subscription {
        let section = cx.entity().downgrade();
        let window_handle = window.window_handle();

        cx.intercept_keystrokes(move |event, window, cx| {
            if window.window_handle() != window_handle {
                return;
            }

            let Some(section) = section.upgrade() else {
                return;
            };

            if !section.read(cx).recorder.is_active() {
                return;
            }

            let chord = key_chord_from_gpui(&event.keystroke);
            section.update(cx, |this, cx| this.handle_recording_key(chord, cx));
            cx.stop_propagation();
        })
    }

    /// Starts recording new keys for `slot`, from its row in the `group`
    /// context group.
    pub(super) fn start_recording(
        &mut self,
        slot: BindingSlot,
        group: ContextId,
        cx: &mut Context<Self>,
    ) {
        self.keybindings_editing_filter = false;
        self.predicate_editing = None;
        self.recording_group = Some(group);

        let predicate = self.overrides.effective_predicate(&slot).into_owned();
        self.recorder.start(slot, predicate);
        cx.notify();
    }

    pub(super) fn cancel_recording(&mut self, cx: &mut Context<Self>) {
        self.recording_pause = None;

        if self.recorder.is_active() {
            self.recorder.cancel();
            cx.notify();
        }
    }

    fn handle_recording_key(&mut self, chord: KeyChord, cx: &mut Context<Self>) {
        let is_enter = chord.key == "enter" && chord.modifiers == Modifiers::none();

        if is_enter && matches!(self.recorder.state(), RecordingState::Conflict { .. }) {
            self.replace_conflicting(cx);
            return;
        }

        let outcome = self.recorder.handle_chord(
            chord,
            dbflux_ui_base::keymap::default_keymap(),
            &self.overrides,
            &self.overlap,
        );

        self.apply_recording_outcome(outcome, cx);
    }

    /// Saves a finished sequence, or arms the pause that finishes it when
    /// the key just captured may be followed by another one.
    fn apply_recording_outcome(&mut self, outcome: RecordingOutcome, cx: &mut Context<Self>) {
        match outcome {
            RecordingOutcome::Captured => {
                self.recording_pause = Some(cx.spawn(async move |this, cx| {
                    cx.background_executor().timer(RECORDING_PAUSE).await;
                    this.update(cx, |this, cx| this.finish_recording(cx))
                        .log_err();
                }));
            }
            RecordingOutcome::Commit { slot, keys } => {
                self.recording_pause = None;
                let mut overrides = self.overrides.clone();
                overrides.set(slot, Some(keys));
                self.save_overrides(overrides, cx);
            }
            RecordingOutcome::ConflictFound | RecordingOutcome::Cancelled => {
                self.recording_pause = None;
            }
            RecordingOutcome::Ignored => {}
        }

        cx.notify();
    }

    /// Ends the sequence being recorded after the pause.
    fn finish_recording(&mut self, cx: &mut Context<Self>) {
        let outcome = self.recorder.finish(
            dbflux_ui_base::keymap::default_keymap(),
            &self.overrides,
            &self.overlap,
        );
        self.apply_recording_outcome(outcome, cx);
    }

    /// Gives the keys to the recorded binding and removes them from the
    /// bindings they collided with.
    pub(super) fn replace_conflicting(&mut self, cx: &mut Context<Self>) {
        if let Some(replacement) = self.recorder.confirm_replace() {
            let mut overrides = self.overrides.clone();
            overrides.rebind(replacement.slot, replacement.keys, &replacement.replaced);
            self.save_overrides(overrides, cx);
        }
        cx.notify();
    }

    /// Opens the context predicate editor under the row of `slot`, filled
    /// with the binding's current predicate.
    pub(super) fn start_predicate_editing(
        &mut self,
        slot: BindingSlot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.cancel_recording(cx);

        let predicate = self.overrides.effective_predicate(&slot).into_owned();
        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!(
                    "settings.keybindings.context_editor.placeholder"
                ))
                .default_value(predicate)
        });
        input.update(cx, |state, cx| state.focus(window, cx));

        self.predicate_editing = Some(PredicateEditing {
            slot,
            input,
            error: None,
        });
        cx.notify();
    }

    pub(super) fn cancel_predicate_editing(&mut self, cx: &mut Context<Self>) {
        if self.predicate_editing.take().is_some() {
            cx.emit(SectionFocusEvent::RequestFocusReturn);
            cx.notify();
        }
    }

    /// Saves the predicate typed in the editor when it parses; otherwise
    /// keeps the editor open with the parse error.
    pub(super) fn save_predicate(&mut self, cx: &mut Context<Self>) {
        let Some(editing) = self.predicate_editing.as_mut() else {
            return;
        };

        let text = editing.input.read(cx).value().trim().to_string();

        if let Err(error) = validate_predicate(&text) {
            editing.error = Some(error);
            cx.notify();
            return;
        }

        let slot = editing.slot.clone();
        self.predicate_editing = None;

        let mut overrides = self.overrides.clone();
        overrides.set_predicate(slot, Some(text));
        self.save_overrides(overrides, cx);
        cx.emit(SectionFocusEvent::RequestFocusReturn);
    }

    /// Removes the shortcut of `slot`.
    pub(super) fn remove_shortcut(&mut self, slot: BindingSlot, cx: &mut Context<Self>) {
        self.recorder.cancel();

        let mut overrides = self.overrides.clone();
        overrides.set(slot, None);
        self.save_overrides(overrides, cx);
    }

    /// Restores the default keys and predicate of `slot`.
    pub(super) fn reset_binding(&mut self, slot: &BindingSlot, cx: &mut Context<Self>) {
        if !self.overrides.is_overridden(slot) {
            return;
        }

        self.recorder.cancel();

        let mut overrides = self.overrides.clone();
        overrides.reset(slot);
        self.save_overrides(overrides, cx);
    }

    /// Restores the default keymap.
    pub(super) fn reset_all(&mut self, cx: &mut Context<Self>) {
        self.recorder.cancel();

        if self.overrides.is_empty() {
            cx.notify();
            return;
        }

        self.save_overrides(KeymapOverrides::new(), cx);
    }

    /// Persists `overrides` and, only when the write succeeds, makes them the
    /// live keymap of every window.
    fn save_overrides(&mut self, overrides: KeymapOverrides, cx: &mut Context<Self>) {
        let result = save_keymap_overrides(self.app_state.read(cx).storage_runtime(), &overrides);

        if let Err(error) = result {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("settings.keybindings.save_error", error = error),
                ),
                cx,
            );
            cx.notify();
            return;
        }

        self.overrides = overrides.clone();
        apply_keymap_overrides(overrides, cx);
        cx.notify();
    }

    /// The binding under the keyboard selection, if a binding row is selected.
    fn selected_entry(&self, cx: &Context<Self>) -> Option<BindingEntry> {
        let KeybindingsSelection::Binding(ctx_idx, binding_idx) = self.keybindings_selection else {
            return None;
        };

        let context = ContextId::all_variants().get(ctx_idx)?;
        let filter = self.keybindings_filter.read(cx).value().to_lowercase();

        self.filtered_entries(*context, &filter)
            .into_iter()
            .nth(binding_idx)
    }
}

impl EventEmitter<SectionFocusEvent> for KeybindingsSection {}

/// Items of the context filter: "All contexts", then every context.
fn context_filter_items() -> Vec<DropdownItem> {
    std::iter::once(DropdownItem::with_value(
        dbflux_i18n::t!("settings.keybindings.context_filter.all"),
        "all",
    ))
    .chain(ContextId::all_variants().iter().map(|context| {
        DropdownItem::with_value(
            crate::labels::keybinding_context_name(context),
            context.id(),
        )
    }))
    .collect()
}

/// The context picked at `index` of [`context_filter_items`].
fn context_for_filter_index(index: usize) -> Option<ContextId> {
    index
        .checked_sub(1)
        .and_then(|context_index| ContextId::all_variants().get(context_index).copied())
}

impl SettingsSection for KeybindingsSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::Keybindings
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.recorder.is_active() {
            return;
        }

        let chord = key_chord_from_gpui(&event.keystroke);

        if self.predicate_editing.is_some() {
            match (chord.key.as_str(), chord.modifiers) {
                ("enter", modifiers) if modifiers == Modifiers::none() => self.save_predicate(cx),
                ("escape", modifiers) if modifiers == Modifiers::none() => {
                    self.cancel_predicate_editing(cx)
                }
                _ => {}
            }
            return;
        }

        if self.keybindings_editing_filter {
            if chord.key == "escape" && chord.modifiers == Modifiers::none() {
                self.keybindings_editing_filter = false;
                cx.notify();
            }
            return;
        }

        if !self.content_focused {
            return;
        }

        match (chord.key.as_str(), chord.modifiers) {
            ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                self.keybindings_move_next(cx);
                self.keybindings_pending_scroll = Some(self.keybindings_flat_index(cx));
                cx.notify();
            }
            ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                self.keybindings_move_prev(cx);
                self.keybindings_pending_scroll = Some(self.keybindings_flat_index(cx));
                cx.notify();
            }
            ("g", modifiers) if modifiers == Modifiers::none() => {
                let first = self.first_visible_context(cx);
                self.keybindings_selection = KeybindingsSelection::Context(first);
                self.keybindings_pending_scroll = Some(0);
                cx.notify();
            }
            ("g", modifiers) if modifiers == Modifiers::shift() => {
                let last = self.last_visible_context(cx);
                let binding_count = self.get_visible_binding_count(last, cx);
                if binding_count > 0 {
                    self.keybindings_selection =
                        KeybindingsSelection::Binding(last, binding_count - 1);
                } else {
                    self.keybindings_selection = KeybindingsSelection::Context(last);
                }
                self.keybindings_pending_scroll = Some(self.keybindings_flat_index(cx));
                cx.notify();
            }
            ("enter", modifiers) | ("space", modifiers) if modifiers == Modifiers::none() => {
                match self.keybindings_selection {
                    KeybindingsSelection::Context(ctx_idx) => {
                        if let Some(context) = ContextId::all_variants().get(ctx_idx) {
                            if self.keybindings_expanded.contains(context) {
                                self.keybindings_expanded.remove(context);
                            } else {
                                self.keybindings_expanded.insert(*context);
                            }
                            cx.notify();
                        }
                    }
                    KeybindingsSelection::Binding(ctx_idx, _) => {
                        if let Some(entry) = self.selected_entry(cx)
                            && let Some(group) = ContextId::all_variants().get(ctx_idx)
                        {
                            self.start_recording(entry.slot, *group, cx);
                        }
                    }
                }
            }
            ("delete", modifiers) | ("backspace", modifiers) if modifiers == Modifiers::none() => {
                if let Some(entry) = self.selected_entry(cx)
                    && entry.keys.is_some()
                {
                    self.remove_shortcut(entry.slot, cx);
                }
            }
            ("r", modifiers) if modifiers == Modifiers::none() => {
                if let Some(entry) = self.selected_entry(cx) {
                    self.reset_binding(&entry.slot, cx);
                }
            }
            ("p", modifiers) if modifiers == Modifiers::none() => {
                if let Some(entry) = self.selected_entry(cx) {
                    self.start_predicate_editing(entry.slot, window, cx);
                }
            }
            ("/", modifiers) | ("f", modifiers) if modifiers == Modifiers::none() => {
                self.keybindings_editing_filter = true;
                self.keybindings_filter.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
                cx.notify();
            }
            _ => {}
        }
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        self.keybindings_editing_filter = false;
        self.predicate_editing = None;
        self.cancel_recording(cx);
        cx.notify();
    }

    fn render_footer_leading_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        self.render_footer_summary(cx)
    }

    fn render_footer_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        Some(self.render_reset_all_button(cx))
    }
}

impl Render for KeybindingsSection {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_keybindings_section(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::{context_filter_items, context_for_filter_index};
    use dbflux_app::keymap::ContextId;

    #[test]
    fn context_filter_lists_all_then_every_context() {
        let items = context_filter_items();

        assert_eq!(items.len(), ContextId::all_variants().len() + 1);
        assert_eq!(items[0].value.as_ref(), "all");
        assert_eq!(items[1].value.as_ref(), ContextId::Global.id());
    }

    #[test]
    fn context_filter_index_maps_back_to_the_context() {
        assert_eq!(context_for_filter_index(0), None);
        assert_eq!(context_for_filter_index(1), Some(ContextId::Global));
        assert_eq!(
            context_for_filter_index(ContextId::all_variants().len()),
            ContextId::all_variants().last().copied()
        );
        assert_eq!(
            context_for_filter_index(ContextId::all_variants().len() + 1),
            None
        );
    }
}
