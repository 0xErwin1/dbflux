//! The form-navigation vocabulary moved to `bolt::forms`; this module
//! re-exports it at the old path so the call sites keep compiling, and keeps
//! the product's own navigation trait: the commands it answers are the
//! product's keymap vocabulary (`dbflux_core::keymap_types::Command`), a
//! domain fact that cannot implement bolt's `FormCommand` here — neither the
//! trait nor the type is local to this crate, and `dbflux_core` must not
//! depend on bolt. The trait maps its commands onto bolt's [`FormNav`]
//! intents by hand and is the trait the product's forms implement.

pub use bolt::forms::{FormEditState, FormField, FormNav};

use dbflux_core::keymap_types::Command;

/// The product's form navigation: the moved [`FormEditState`], [`FormField`]
/// and [`FormNav`] vocabulary over the product's own `Command` dispatch. The
/// required methods and the command arms are exactly the ones the trait has
/// always had; only the vocabulary they speak moved.
#[allow(dead_code)]
pub trait FormNavigation: Sized + 'static {
    type Focus: FormField;

    fn edit_state(&self) -> FormEditState;
    fn set_edit_state(&mut self, state: FormEditState);

    fn form_focus(&self) -> Self::Focus;
    fn set_form_focus(&mut self, focus: Self::Focus);

    fn form_focus_handle(&self) -> &gpui::FocusHandle;

    fn focus_down(&mut self, cx: &gpui::App);
    fn focus_up(&mut self, cx: &gpui::App);
    fn focus_left(&mut self, cx: &gpui::App);
    fn focus_right(&mut self, cx: &gpui::App);

    fn activate_focused_field(&mut self, window: &mut gpui::Window, cx: &mut gpui::Context<Self>);

    fn handle_cancel_navigating(
        &mut self,
        _window: &mut gpui::Window,
        _cx: &mut gpui::Context<Self>,
    ) -> bool {
        false
    }

    fn exit_edit_mode(&mut self, window: &mut gpui::Window, cx: &mut gpui::Context<Self>) {
        self.set_edit_state(FormEditState::Navigating);
        self.form_focus_handle().focus(window, cx);
        cx.notify();
    }

    fn enter_edit_mode_for_field(
        &mut self,
        field: Self::Focus,
        window: &mut gpui::Window,
        cx: &mut gpui::Context<Self>,
    ) {
        self.set_form_focus(field);
        self.activate_focused_field(window, cx);
        cx.notify();
    }

    fn handle_navigating_command(
        &mut self,
        command: Command,
        window: &mut gpui::Window,
        cx: &mut gpui::Context<Self>,
    ) -> bool {
        match form_nav(command) {
            Some(FormNav::Down) => {
                self.focus_down(cx);
                cx.notify();
                true
            }
            Some(FormNav::Up) => {
                self.focus_up(cx);
                cx.notify();
                true
            }
            Some(FormNav::Left) => {
                self.focus_left(cx);
                cx.notify();
                true
            }
            Some(FormNav::Right) => {
                self.focus_right(cx);
                cx.notify();
                true
            }
            Some(FormNav::Activate) => {
                self.activate_focused_field(window, cx);
                true
            }
            Some(FormNav::Cancel) => self.handle_cancel_navigating(window, cx),
            None => false,
        }
    }

    fn handle_editing_command(
        &mut self,
        command: Command,
        window: &mut gpui::Window,
        cx: &mut gpui::Context<Self>,
    ) -> bool {
        match form_nav(command) {
            Some(FormNav::Cancel) => {
                self.exit_edit_mode(window, cx);
                true
            }
            Some(FormNav::Activate) => {
                self.exit_edit_mode(window, cx);
                self.focus_down(cx);
                cx.notify();
                true
            }
            _ => false,
        }
    }
}

/// The product's commands over the navigation intents the form answers: the
/// two movement vocabularies (focus and column) collapse onto one intent
/// each.
fn form_nav(command: Command) -> Option<FormNav> {
    match command {
        Command::SelectNext => Some(FormNav::Down),
        Command::SelectPrev => Some(FormNav::Up),
        Command::FocusLeft | Command::ColumnLeft => Some(FormNav::Left),
        Command::FocusRight | Command::ColumnRight => Some(FormNav::Right),
        Command::Execute => Some(FormNav::Activate),
        Command::Cancel => Some(FormNav::Cancel),
        _ => None,
    }
}

/// Wire Enter (exit + move down) and Blur (exit) on a form input. The same
/// subscription bolt's generic contract provides, over the product's trait.
pub fn subscribe_form_input<T: FormNavigation>(
    cx: &mut gpui::Context<T>,
    window: &mut gpui::Window,
    input: &gpui::Entity<crate::controls::InputState>,
) -> gpui::Subscription {
    cx.subscribe_in(
        input,
        window,
        |this: &mut T, _, event: &crate::controls::InputEvent, window, cx| match event {
            crate::controls::InputEvent::PressEnter {
                secondary: false, ..
            } => {
                this.exit_edit_mode(window, cx);
                this.focus_down(cx);
                cx.notify();
            }
            crate::controls::InputEvent::Blur => {
                this.exit_edit_mode(window, cx);
            }
            _ => {}
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{Command, FormNav, form_nav};

    /// Every arm of the navigation the product's commands drive, asserted
    /// against the mapping the trait's command handlers run: the two
    /// movement vocabularies (focus and column) collapse onto one intent
    /// each, and a command no form answers maps to none.
    #[test]
    fn product_commands_map_onto_the_navigation_intents_the_form_answers() {
        assert_eq!(form_nav(Command::SelectNext), Some(FormNav::Down));
        assert_eq!(form_nav(Command::SelectPrev), Some(FormNav::Up));
        assert_eq!(form_nav(Command::FocusLeft), Some(FormNav::Left));
        assert_eq!(form_nav(Command::ColumnLeft), Some(FormNav::Left));
        assert_eq!(form_nav(Command::FocusRight), Some(FormNav::Right));
        assert_eq!(form_nav(Command::ColumnRight), Some(FormNav::Right));
        assert_eq!(form_nav(Command::Execute), Some(FormNav::Activate));
        assert_eq!(form_nav(Command::Cancel), Some(FormNav::Cancel));
        assert_eq!(form_nav(Command::Delete), None);
        assert_eq!(form_nav(Command::ToggleFavorite), None);
    }
}
