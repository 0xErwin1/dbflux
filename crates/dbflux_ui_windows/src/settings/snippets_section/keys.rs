use super::{SnippetFocus, SnippetFormField, SnippetsSection};
use crate::settings::form_section::FormSection;
use crate::settings::section_trait::SectionFocusEvent;
use dbflux_app::keymap::{KeyChord, Modifiers};
use dbflux_ui_base::keymap::key_chord_from_gpui;
use gpui::*;

impl SnippetsSection {
    pub(super) fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.pending_delete_id.is_some() {
            return;
        }

        if !self.content_focused && !self.editing_field {
            return;
        }

        let pressed = key_chord_from_gpui(&event.keystroke);

        if self.handle_open_language_dropdown(&pressed, cx) {
            return;
        }

        // The body is a multi-line editor: it owns Enter and Tab, so only
        // Escape leaves it.
        if self.focus == SnippetFocus::Form
            && self.editing_field
            && self.form_field == SnippetFormField::Body
        {
            if pressed.key == "escape" && pressed.modifiers == Modifiers::none() {
                self.editing_field = false;
                cx.emit(SectionFocusEvent::RequestFocusReturn);
                cx.notify();
            }
            return;
        }

        if self.handle_editing_keys(event, window, cx) {
            return;
        }

        match self.focus {
            SnippetFocus::List => self.handle_list_key(&pressed, window, cx),
            SnippetFocus::Form => self.handle_form_key(&pressed, window, cx),
        }
    }

    fn handle_open_language_dropdown(
        &mut self,
        pressed: &KeyChord,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.focus != SnippetFocus::Form
            || self.form_field != SnippetFormField::Language
            || !self.language_dropdown.read(cx).is_open()
        {
            return false;
        }

        let dropdown = self.language_dropdown.clone();

        match (pressed.key.as_str(), pressed.modifiers) {
            ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                dropdown.update(cx, |dropdown, cx| dropdown.select_next_item(cx));
            }
            ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                dropdown.update(cx, |dropdown, cx| dropdown.select_prev_item(cx));
            }
            ("enter", modifiers) if modifiers == Modifiers::none() => {
                dropdown.update(cx, |dropdown, cx| dropdown.accept_selection(cx));
            }
            ("escape", modifiers) if modifiers == Modifiers::none() => {
                dropdown.update(cx, |dropdown, cx| dropdown.close(cx));
            }
            _ => return false,
        }

        cx.notify();
        true
    }

    fn handle_list_key(&mut self, pressed: &KeyChord, window: &mut Window, cx: &mut Context<Self>) {
        let count = self.snippets.len();
        let plain = pressed.modifiers == Modifiers::none();
        let shifted = pressed.modifiers == Modifiers::shift();

        match pressed.key.as_str() {
            "j" | "down" if plain && count > 0 => {
                let next = self
                    .list_idx
                    .map_or(0, |idx| idx.saturating_add(1))
                    .min(count - 1);
                self.select_index(next, window, cx);
            }
            "k" | "up" if plain && count > 0 => {
                let previous = self.list_idx.unwrap_or(0).saturating_sub(1);
                self.select_index(previous, window, cx);
            }
            "g" if plain && count > 0 => self.select_index(0, window, cx),
            "g" if shifted && count > 0 => self.select_index(count - 1, window, cx),
            "n" if plain => {
                self.begin_new_snippet(window, cx);
                self.focus = SnippetFocus::Form;
                self.form_field = SnippetFormField::Name;
            }
            "d" if plain => {
                if let Some(id) = self.editing_id.clone() {
                    self.request_delete_snippet(id, cx);
                }
            }
            "l" | "right" | "enter"
                if plain && (self.new_form_open || self.editing_id.is_some()) =>
            {
                self.enter_form(window, cx);
            }
            _ => return,
        }

        cx.notify();
    }

    fn handle_form_key(&mut self, pressed: &KeyChord, window: &mut Window, cx: &mut Context<Self>) {
        let plain = pressed.modifiers == Modifiers::none();
        let shifted = pressed.modifiers == Modifiers::shift();

        match pressed.key.as_str() {
            "escape" | "h" if plain => self.exit_form(window, cx),
            "j" | "down" if plain => self.move_down(),
            "k" | "up" if plain => self.move_up(),
            "left" if plain => self.move_left(),
            "right" | "l" if plain => self.move_right(),
            "enter" if plain => self.activate_current_field(window, cx),
            "tab" if plain => self.tab_next(),
            "tab" if shifted => self.tab_prev(),
            "g" if plain => self.move_first(),
            "g" if shifted => self.move_last(),
            _ => return,
        }

        cx.notify();
    }
}
