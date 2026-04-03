use super::SettingsSection;
use super::SettingsSectionId;
use crate::keymap::{ContextId, KeyChord, Modifiers, key_chord_from_gpui};
use gpui::prelude::*;
use gpui::*;
use gpui_component::input::InputState;
use std::collections::HashSet;

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

pub(super) enum KeybindingsListItem {
    ContextHeader {
        context: ContextId,
        ctx_idx: usize,
        is_expanded: bool,
        is_selected: bool,
        binding_count: usize,
    },
    Binding {
        chord: KeyChord,
        cmd_name: String,
        is_inherited: bool,
        is_selected: bool,
        ctx_idx: usize,
        binding_idx: usize,
    },
}

pub(super) struct KeybindingsSection {
    pub(super) keybindings_filter: Entity<InputState>,
    pub(super) keybindings_expanded: HashSet<ContextId>,
    pub(super) keybindings_selection: KeybindingsSelection,
    pub(super) keybindings_editing_filter: bool,
    pub(super) keybindings_scroll_handle: ScrollHandle,
    pub(super) keybindings_pending_scroll: Option<usize>,
    pub(super) content_focused: bool,
}

impl KeybindingsSection {
    pub(super) fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let keybindings_filter =
            cx.new(|cx| InputState::new(window, cx).placeholder("Filter keybindings..."));

        let mut keybindings_expanded = HashSet::new();
        keybindings_expanded.insert(ContextId::Global);

        Self {
            keybindings_filter,
            keybindings_expanded,
            keybindings_selection: KeybindingsSelection::Context(0),
            keybindings_editing_filter: false,
            keybindings_scroll_handle: ScrollHandle::new(),
            keybindings_pending_scroll: None,
            content_focused: false,
        }
    }
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
        let chord = key_chord_from_gpui(&event.keystroke);

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
                if let KeybindingsSelection::Context(ctx_idx) = self.keybindings_selection
                    && let Some(context) = ContextId::all_variants().get(ctx_idx)
                {
                    if self.keybindings_expanded.contains(context) {
                        self.keybindings_expanded.remove(context);
                    } else {
                        self.keybindings_expanded.insert(*context);
                    }
                    cx.notify();
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
        cx.notify();
    }
}

impl Render for KeybindingsSection {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_keybindings_section(cx)
    }
}
