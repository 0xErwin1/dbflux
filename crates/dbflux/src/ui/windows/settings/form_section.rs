use crate::keymap::{key_chord_from_gpui, Modifiers};
use gpui::{Context, Entity, EventEmitter, KeyDownEvent, Subscription, Window};
use gpui_component::input::{InputEvent, InputState};

use super::section_trait::SectionFocusEvent;

pub trait FormSection: Sized + 'static {
    type Focus: Copy + PartialEq + std::fmt::Debug;
    type FormField: Copy + PartialEq + std::fmt::Debug;

    fn focus_area(&self) -> Self::Focus;
    fn set_focus_area(&mut self, focus: Self::Focus);

    fn form_field(&self) -> Self::FormField;
    fn set_form_field(&mut self, field: Self::FormField);

    fn editing_field(&self) -> bool;
    fn set_editing_field(&mut self, editing: bool);

    fn switching_input(&self) -> bool;
    fn set_switching_input(&mut self, switching: bool);

    fn content_focused(&self) -> bool;

    fn list_focus() -> Self::Focus;
    fn form_focus() -> Self::Focus;
    fn first_form_field() -> Self::FormField;

    fn form_rows(&self) -> Vec<Vec<Self::FormField>>;
    fn is_input_field(field: Self::FormField) -> bool;

    fn focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>);
    fn activate_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>);

    fn move_down(&mut self) {
        let rows = self.form_rows();
        let current = self.form_field();

        for (row_idx, row) in rows.iter().enumerate() {
            if let Some(col_idx) = row.iter().position(|f| *f == current) {
                if row_idx + 1 < rows.len() {
                    let next_row = &rows[row_idx + 1];
                    let next_col = col_idx.min(next_row.len().saturating_sub(1));
                    self.set_form_field(next_row[next_col]);
                }
                return;
            }
        }
    }

    fn move_up(&mut self) {
        let rows = self.form_rows();
        let current = self.form_field();

        for (row_idx, row) in rows.iter().enumerate() {
            if let Some(col_idx) = row.iter().position(|f| *f == current) {
                if row_idx > 0 {
                    let prev_row = &rows[row_idx - 1];
                    let prev_col = col_idx.min(prev_row.len().saturating_sub(1));
                    self.set_form_field(prev_row[prev_col]);
                }
                return;
            }
        }
    }

    fn move_left(&mut self) {
        let rows = self.form_rows();
        let current = self.form_field();

        for row in &rows {
            if let Some(col_idx) = row.iter().position(|f| *f == current) {
                if col_idx > 0 {
                    self.set_form_field(row[col_idx - 1]);
                }
                return;
            }
        }
    }

    fn move_right(&mut self) {
        let rows = self.form_rows();
        let current = self.form_field();

        for row in &rows {
            if let Some(col_idx) = row.iter().position(|f| *f == current) {
                if col_idx + 1 < row.len() {
                    self.set_form_field(row[col_idx + 1]);
                }
                return;
            }
        }
    }

    fn move_first(&mut self) {
        let rows = self.form_rows();
        if let Some(first_row) = rows.first()
            && let Some(first_field) = first_row.first()
        {
            self.set_form_field(*first_field);
        }
    }

    fn move_last(&mut self) {
        let rows = self.form_rows();
        if let Some(last_row) = rows.last()
            && let Some(last_field) = last_row.last()
        {
            self.set_form_field(*last_field);
        }
    }

    fn tab_next(&mut self) {
        let rows = self.form_rows();
        let current = self.form_field();
        let mut found_current = false;

        for row in &rows {
            for field in row {
                if found_current {
                    self.set_form_field(*field);
                    return;
                }
                if *field == current {
                    found_current = true;
                }
            }
        }

        self.move_first();
    }

    fn tab_prev(&mut self) {
        let rows = self.form_rows();
        let current = self.form_field();
        let mut prev_field: Option<Self::FormField> = None;

        for row in &rows {
            for field in row {
                if *field == current {
                    if let Some(prev) = prev_field {
                        self.set_form_field(prev);
                    } else {
                        self.move_last();
                    }
                    return;
                }
                prev_field = Some(*field);
            }
        }
    }

    fn enter_form(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.set_focus_area(Self::form_focus());
        self.set_form_field(Self::first_form_field());
        self.set_editing_field(false);
    }

    fn exit_form(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.set_focus_area(Self::list_focus());
        self.set_editing_field(false);
    }

    fn handle_editing_keys(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool
    where
        Self: EventEmitter<SectionFocusEvent>,
    {
        if self.focus_area() != Self::form_focus() || !self.editing_field() {
            return false;
        }

        let chord = key_chord_from_gpui(&event.keystroke);

        match (chord.key.as_str(), chord.modifiers) {
            ("escape", modifiers) if modifiers == Modifiers::none() => {
                self.set_editing_field(false);
                cx.emit(SectionFocusEvent::RequestFocusReturn);
                cx.notify();
                true
            }
            ("enter", modifiers) if modifiers == Modifiers::none() => {
                self.set_editing_field(false);
                self.move_down();
                cx.notify();
                true
            }
            ("tab", modifiers) if modifiers == Modifiers::none() => {
                self.set_editing_field(false);
                self.tab_next();
                self.focus_current_field(window, cx);
                cx.notify();
                true
            }
            ("tab", modifiers) if modifiers == Modifiers::shift() => {
                self.set_editing_field(false);
                self.tab_prev();
                self.focus_current_field(window, cx);
                cx.notify();
                true
            }
            _ => false,
        }
    }

    fn validate_form_field(&mut self) {
        let rows = self.form_rows();
        let current = self.form_field();

        for row in &rows {
            if row.contains(&current) {
                return;
            }
        }

        self.set_form_field(Self::first_form_field());
    }
}

pub fn create_blur_subscription<S>(cx: &mut Context<S>, input: &Entity<InputState>) -> Subscription
where
    S: FormSection + EventEmitter<SectionFocusEvent>,
{
    cx.subscribe(input, |this, _, event: &InputEvent, cx| {
        if matches!(event, InputEvent::Blur) {
            if this.switching_input() {
                this.set_switching_input(false);
                return;
            }
            cx.emit(SectionFocusEvent::RequestFocusReturn);
        }
    })
}
