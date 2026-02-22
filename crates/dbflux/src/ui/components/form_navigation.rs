use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::input::{InputEvent, InputState};

use crate::keymap::Command;

#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub enum FormEditState {
    #[default]
    Navigating,
    Editing,
    DropdownOpen,
}

#[allow(dead_code)]
pub(crate) trait FormField: Copy + PartialEq {
    fn is_input(&self) -> bool;
}

#[allow(dead_code)]
pub(crate) trait FormNavigation: Sized + 'static {
    type Focus: FormField;

    fn edit_state(&self) -> FormEditState;
    fn set_edit_state(&mut self, state: FormEditState);

    fn form_focus(&self) -> Self::Focus;
    fn set_form_focus(&mut self, focus: Self::Focus);

    fn form_focus_handle(&self) -> &FocusHandle;

    fn focus_down(&mut self, cx: &App);
    fn focus_up(&mut self, cx: &App);
    fn focus_left(&mut self, cx: &App);
    fn focus_right(&mut self, cx: &App);

    fn activate_focused_field(&mut self, window: &mut Window, cx: &mut Context<Self>);

    fn handle_cancel_navigating(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> bool {
        false
    }

    fn exit_edit_mode(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.set_edit_state(FormEditState::Navigating);
        window.focus(self.form_focus_handle());
        cx.notify();
    }

    fn enter_edit_mode_for_field(
        &mut self,
        field: Self::Focus,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.set_form_focus(field);
        self.activate_focused_field(window, cx);
        cx.notify();
    }

    fn handle_navigating_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match command {
            Command::SelectNext => {
                self.focus_down(cx);
                cx.notify();
                true
            }
            Command::SelectPrev => {
                self.focus_up(cx);
                cx.notify();
                true
            }
            Command::FocusLeft | Command::ColumnLeft => {
                self.focus_left(cx);
                cx.notify();
                true
            }
            Command::FocusRight | Command::ColumnRight => {
                self.focus_right(cx);
                cx.notify();
                true
            }
            Command::Execute => {
                self.activate_focused_field(window, cx);
                true
            }
            Command::Cancel => self.handle_cancel_navigating(window, cx),
            _ => false,
        }
    }

    fn handle_editing_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match command {
            Command::Cancel => {
                self.exit_edit_mode(window, cx);
                true
            }
            Command::Execute => {
                self.exit_edit_mode(window, cx);
                self.focus_down(cx);
                cx.notify();
                true
            }
            _ => false,
        }
    }
}

/// Wire Enter (exit + move down) and Blur (exit) on a form input.
pub(crate) fn subscribe_form_input<T: FormNavigation>(
    cx: &mut Context<T>,
    window: &mut Window,
    input: &Entity<InputState>,
) -> Subscription {
    cx.subscribe_in(
        input,
        window,
        |this: &mut T, _, event: &InputEvent, window, cx| match event {
            InputEvent::PressEnter { secondary: false } => {
                this.exit_edit_mode(window, cx);
                this.focus_down(cx);
                cx.notify();
            }
            InputEvent::Blur => {
                this.exit_edit_mode(window, cx);
            }
            _ => {}
        },
    )
}

pub(crate) fn focus_ring(focused: bool, ring_color: Hsla) -> Div {
    div()
        .rounded(px(4.0))
        .border_2()
        .when(focused, |d| d.border_color(ring_color))
        .when(!focused, |d| d.border_color(gpui::transparent_black()))
        .p(px(2.0))
}
