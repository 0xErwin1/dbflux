//! Two-dimensional navigation across a form's fields: left and right within
//! a row, up and down between rows, with the row layout supplied by the
//! host's own [`FormNavigation::focus_down`] and friends.

use crate::controls::{InputEvent, InputState};
use gpui::{App, Context, Entity, FocusHandle, Subscription, Window};

#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub enum FormEditState {
    #[default]
    Navigating,
    Editing,
    DropdownOpen,
}

#[allow(dead_code)]
pub trait FormField: Copy + PartialEq {
    fn is_input(&self) -> bool;
}

/// The movement a keymap command asks of a form's navigation, in the seam's
/// own vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormNav {
    /// The next row, or the next field of the last one.
    Down,
    /// The previous row, or the previous field of the first one.
    Up,
    /// The field to the left of the focused one.
    Left,
    /// The field to the right of the focused one.
    Right,
    /// Activates the focused field: enters its input, presses its button.
    Activate,
    /// Leaves the form, or an edit of one of its fields.
    Cancel,
}

/// How a product's keymap command reaches the form navigation: the type a
/// product's command dispatch hands to [`FormNavigation::handle_navigating_command`]
/// and [`FormNavigation::handle_editing_command`], mapped onto the
/// navigation intents the form answers. Commands that mean nothing to a form
/// map to `None` and are reported as unhandled.
///
/// A product implements this for its own command type in its own crate — the
/// impl is legal there, where either the trait or the type is local. A host
/// whose command vocabulary lives in a crate that cannot depend on this one
/// keeps the mapping beside its own trait instead: the vocabulary shim of a
/// product that consumes this crate shows that shape.
pub trait FormCommand: Copy {
    fn form_nav(self) -> Option<FormNav>;
}

#[allow(dead_code)]
pub trait FormNavigation: Sized + 'static {
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
        self.form_focus_handle().focus(window, cx);
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

    /// Runs a navigating-state command: the movement it names, activation of
    /// the focused field, or the host's cancel. A command with no navigation
    /// intent is unhandled.
    fn handle_navigating_command(
        &mut self,
        command: impl FormCommand,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match command.form_nav() {
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

    /// Runs an editing-state command: leaving the edit on cancel, and
    /// leaving it onto the next row on activate. Everything else is
    /// unhandled and stays with the field being edited.
    fn handle_editing_command(
        &mut self,
        command: impl FormCommand,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match command.form_nav() {
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

/// Wire Enter (exit + move down) and Blur (exit) on a form input.
pub fn subscribe_form_input<T: FormNavigation>(
    cx: &mut Context<T>,
    window: &mut Window,
    input: &Entity<InputState>,
) -> Subscription {
    cx.subscribe_in(
        input,
        window,
        |this: &mut T, _, event: &InputEvent, window, cx| match event {
            InputEvent::PressEnter {
                secondary: false, ..
            } => {
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

#[cfg(test)]
mod tests {
    use super::{FormCommand, FormEditState, FormField, FormNav, FormNavigation};
    use gpui::{
        App, AppContext as _, Context, Entity, FocusHandle, IntoElement, Render, TestAppContext,
        VisualTestContext, Window, div,
    };

    /// The fields of a two-row test form:
    ///
    /// ```text
    /// A  B
    /// C
    /// ```
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Field {
        A,
        B,
        C,
    }

    impl FormField for Field {
        fn is_input(&self) -> bool {
            false
        }
    }

    /// A stand-in command vocabulary, mapped the way a product maps its own.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum TestCommand {
        Next,
        Prev,
        Left,
        Right,
        Execute,
        Cancel,
        Unrelated,
    }

    impl FormCommand for TestCommand {
        fn form_nav(self) -> Option<FormNav> {
            match self {
                TestCommand::Next => Some(FormNav::Down),
                TestCommand::Prev => Some(FormNav::Up),
                TestCommand::Left => Some(FormNav::Left),
                TestCommand::Right => Some(FormNav::Right),
                TestCommand::Execute => Some(FormNav::Activate),
                TestCommand::Cancel => Some(FormNav::Cancel),
                TestCommand::Unrelated => None,
            }
        }
    }

    struct TestForm {
        edit_state: FormEditState,
        focus: Field,
        focus_handle: FocusHandle,
        activated: Vec<Field>,
        cancelled: usize,
    }

    impl TestForm {
        fn new(cx: &mut Context<Self>) -> Self {
            Self {
                edit_state: FormEditState::Navigating,
                focus: Field::A,
                focus_handle: cx.focus_handle(),
                activated: Vec::new(),
                cancelled: 0,
            }
        }
    }

    impl FormNavigation for TestForm {
        type Focus = Field;

        fn edit_state(&self) -> FormEditState {
            self.edit_state
        }

        fn set_edit_state(&mut self, state: FormEditState) {
            self.edit_state = state;
        }

        fn form_focus(&self) -> Field {
            self.focus
        }

        fn set_form_focus(&mut self, focus: Field) {
            self.focus = focus;
        }

        fn form_focus_handle(&self) -> &FocusHandle {
            &self.focus_handle
        }

        fn focus_down(&mut self, _cx: &App) {
            self.focus = match self.focus {
                Field::A | Field::B => Field::C,
                Field::C => Field::A,
            };
        }

        fn focus_up(&mut self, _cx: &App) {
            self.focus = match self.focus {
                Field::C => Field::A,
                Field::A | Field::B => Field::C,
            };
        }

        fn focus_left(&mut self, _cx: &App) {
            self.focus = match self.focus {
                Field::B => Field::A,
                other => other,
            };
        }

        fn focus_right(&mut self, _cx: &App) {
            self.focus = match self.focus {
                Field::A => Field::B,
                other => other,
            };
        }

        fn activate_focused_field(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
            self.activated.push(self.focus);
        }

        fn handle_cancel_navigating(
            &mut self,
            _window: &mut Window,
            _cx: &mut Context<Self>,
        ) -> bool {
            self.cancelled += 1;
            true
        }
    }

    /// A window body holding the form under test, so the command handlers
    /// get a real `Window`.
    struct Owner {
        form: Entity<TestForm>,
    }

    impl Render for Owner {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
        }
    }

    fn setup(cx: &mut TestAppContext) -> (Entity<TestForm>, &mut VisualTestContext) {
        let (owner, window) = cx.add_window_view(|_window, cx| Owner {
            form: cx.new(TestForm::new),
        });

        let form = window.update(|_, cx| owner.read(cx).form.clone());
        (form, window)
    }

    #[gpui::test]
    fn navigating_commands_move_the_form_focus_and_report_themselves(cx: &mut TestAppContext) {
        let (form, window) = setup(cx);

        window.update(|window, cx| {
            form.update(cx, |form, cx| {
                assert!(form.handle_navigating_command(TestCommand::Right, window, cx));
                assert_eq!(form.form_focus(), Field::B);

                assert!(form.handle_navigating_command(TestCommand::Left, window, cx));
                assert_eq!(form.form_focus(), Field::A);

                assert!(form.handle_navigating_command(TestCommand::Next, window, cx));
                assert_eq!(form.form_focus(), Field::C);

                assert!(form.handle_navigating_command(TestCommand::Prev, window, cx));
                assert_eq!(form.form_focus(), Field::A);

                assert!(form.handle_navigating_command(TestCommand::Execute, window, cx));
                assert_eq!(form.activated, vec![Field::A]);

                assert!(form.handle_navigating_command(TestCommand::Cancel, window, cx));
                assert_eq!(form.cancelled, 1);
            });
        });
    }

    #[gpui::test]
    fn a_command_no_form_answers_leaves_the_form_untouched(cx: &mut TestAppContext) {
        let (form, window) = setup(cx);

        window.update(|window, cx| {
            form.update(cx, |form, cx| {
                let focus_before = form.form_focus();

                assert!(!form.handle_navigating_command(TestCommand::Unrelated, window, cx));
                assert_eq!(form.form_focus(), focus_before);
                assert!(form.activated.is_empty());
                assert_eq!(form.cancelled, 0);
                assert_eq!(form.edit_state(), FormEditState::Navigating);
            });
        });
    }

    #[gpui::test]
    fn editing_commands_exit_the_edit_and_execute_moves_to_the_next_row(cx: &mut TestAppContext) {
        let (form, window) = setup(cx);

        window.update(|window, cx| {
            form.update(cx, |form, cx| {
                form.set_edit_state(FormEditState::Editing);

                assert!(form.handle_editing_command(TestCommand::Execute, window, cx));
                assert_eq!(form.edit_state(), FormEditState::Navigating);
                assert_eq!(
                    form.form_focus(),
                    Field::C,
                    "Execute leaves the edit on the next row"
                );

                form.set_edit_state(FormEditState::Editing);
                assert!(form.handle_editing_command(TestCommand::Cancel, window, cx));
                assert_eq!(form.edit_state(), FormEditState::Navigating);

                form.set_edit_state(FormEditState::Editing);
                assert!(!form.handle_editing_command(TestCommand::Next, window, cx));
                assert_eq!(
                    form.edit_state(),
                    FormEditState::Editing,
                    "an unhandled command keeps editing"
                );
            });
        });
    }

    #[test]
    fn the_command_seam_reports_every_navigation_intent_it_carries() {
        // The mapping the product installs decides what a key means; the
        // default handlers only ever act on the intents they name here.
        for (command, expected) in [
            (TestCommand::Next, Some(FormNav::Down)),
            (TestCommand::Prev, Some(FormNav::Up)),
            (TestCommand::Left, Some(FormNav::Left)),
            (TestCommand::Right, Some(FormNav::Right)),
            (TestCommand::Execute, Some(FormNav::Activate)),
            (TestCommand::Cancel, Some(FormNav::Cancel)),
            (TestCommand::Unrelated, None),
        ] {
            assert_eq!(command.form_nav(), expected, "{command:?}");
        }
    }
}
