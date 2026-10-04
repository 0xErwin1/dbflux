use crate::actions::SaveEdit;
use crate::components::json_editor_view::{self, JsonEditorView};
use crate::icons::AppIcon;
use crate::modals::Modal;
use crate::vim::{VimBinding, VimHost};
use dbflux_core::keymap_types::{Command, ContextId};
use gpui::*;
use gpui_component::input::EditorState;

/// Width of the cell editor dialog (P1Flows).
const CELL_EDITOR_WIDTH: Pixels = px(560.0);

/// Event emitted when the modal editor saves.
#[derive(Clone)]
pub struct CellEditorSaveEvent {
    pub row: usize,
    pub col: usize,
    pub value: String,
}

/// Event emitted when the modal editor is closed.
#[derive(Clone)]
pub struct CellEditorClosedEvent;

/// Modal editor for JSON and long text values.
pub struct CellEditorModal {
    visible: bool,
    row: usize,
    col: usize,
    is_json: bool,
    /// The text the dialog opened with, to tell an edited value from one that
    /// was only looked at.
    opened_value: String,
    /// Name and type of the edited column, for the title.
    column: Option<(String, String)>,
    input: Entity<EditorState>,
    focus_handle: FocusHandle,
    validation_error: Option<String>,
    vim: VimBinding,
    /// Re-renders on every edit so the JSON status line follows the text.
    _input_observation: Subscription,
}

impl CellEditorModal {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        // soft_wrap defaults to true in 0.6.1, so the old explicit builder is gone.
        let input = cx.new(|cx| {
            EditorState::new(window, cx)
                .language("json")
                .line_number(true)
        });
        let input_observation = cx.observe(&input, |_, _, cx| cx.notify());
        let vim = VimBinding::new(input.clone(), window, cx);

        let mut modal = Self {
            visible: false,
            row: 0,
            col: 0,
            is_json: false,
            opened_value: String::new(),
            column: None,
            input,
            focus_handle: cx.focus_handle(),
            validation_error: None,
            vim,
            _input_observation: input_observation,
        };

        let input = modal.vim.input_id();
        VimBinding::follow_setting(&mut modal, input, cx);
        modal
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Opens the editor on a cell. `column` is the column's name and type,
    /// shown in the title ("Edit properties (jsonb)").
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        &mut self,
        row: usize,
        col: usize,
        value: String,
        is_json: bool,
        column: Option<(String, String)>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.row = row;
        self.col = col;
        self.is_json = is_json;
        self.column = column;
        self.visible = true;
        self.validation_error = None;

        let formatted = if is_json && !value.is_empty() {
            json_editor_view::format_json(&value).unwrap_or(value)
        } else {
            value
        };

        self.input.update(cx, |state, cx| {
            state.set_value(&formatted, window, cx);
            state.focus(window, cx);
        });
        // Read back rather than kept from `formatted`: the editor may normalize
        // the text it is given, such as its line endings.
        self.opened_value = self.input.read(cx).value().to_string();

        cx.notify();
    }

    /// Hands over the value the dialog holds when it differs from the one it
    /// opened with, and hides the dialog, as Save does.
    ///
    /// For a host that is about to close the document under the dialog, which
    /// stays reachable while it is open. `Ok(None)` when the dialog is closed
    /// or its text is unchanged. A JSON value that does not parse stays in the
    /// open dialog with its error shown, and is reported as `Err` with that
    /// error. No save or close event is emitted: the host writes the value
    /// itself and decides where focus goes.
    pub fn take_unsaved_edit(
        &mut self,
        cx: &mut Context<Self>,
    ) -> Result<Option<CellEditorSaveEvent>, String> {
        if !self.visible {
            return Ok(None);
        }

        let value = self.input.read(cx).value().to_string();
        if value == self.opened_value {
            return Ok(None);
        }

        if self.is_json
            && let Err(error) = json_editor_view::validate_json(&value, true)
        {
            self.validation_error = Some(error.clone());
            cx.notify();
            return Err(error);
        }

        self.visible = false;
        self.validation_error = None;
        cx.notify();

        Ok(Some(CellEditorSaveEvent {
            row: self.row,
            col: self.col,
            value,
        }))
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        let was_visible = self.visible;
        self.visible = false;
        self.validation_error = None;

        if was_visible {
            cx.emit(CellEditorClosedEvent);
        }

        cx.notify();
    }

    fn save(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();

        if self.is_json
            && let Err(e) = json_editor_view::validate_json(&value, true)
        {
            self.validation_error = Some(e);
            cx.notify();
            return;
        }

        cx.emit(CellEditorSaveEvent {
            row: self.row,
            col: self.col,
            value,
        });

        self.close(cx);
    }

    fn compact_json(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();
        if let Some(compact) = json_editor_view::compact_json(&value) {
            self.input.update(cx, |state, cx| {
                state.set_value(&compact, window, cx);
            });
            self.validation_error = None;
        }
    }

    fn format(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();
        if let Some(formatted) = json_editor_view::format_json(&value) {
            self.input.update(cx, |state, cx| {
                state.set_value(&formatted, window, cx);
            });
            self.validation_error = None;
        }
    }
}

impl VimHost for CellEditorModal {
    fn vim(&self, input: EntityId) -> Option<&VimBinding> {
        self.vim.for_input(input)
    }

    fn vim_mut(&mut self, input: EntityId) -> Option<&mut VimBinding> {
        self.vim.for_input_mut(input)
    }

    /// `<leader> s` runs the dialog's own save, as its primary button does.
    fn vim_dialog_command(
        &mut self,
        _input: EntityId,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if command == Command::SaveQuery {
            self.save(window, cx);
        }
    }
}

impl EventEmitter<CellEditorSaveEvent> for CellEditorModal {}
impl EventEmitter<CellEditorClosedEvent> for CellEditorModal {}

/// Vim's listeners on the editor's container. In Insert mode the modal's
/// Escape (`Cancel`) leaves Insert mode instead of closing, and in Normal
/// mode Enter (`Execute`) moves down instead of reaching the modal.
fn vim_wrapper(element: Div, input: EntityId, cx: &mut Context<CellEditorModal>) -> Div {
    let element = VimBinding::wire(element, input, cx);
    let element = VimBinding::capture_action::<crate::actions::Cancel, _>(element, input, cx);
    VimBinding::capture_action::<crate::actions::Execute, _>(element, input, cx)
}

impl Render for CellEditorModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let is_json = self.is_json;
        let entity = cx.entity().downgrade();

        let close = move |_window: &mut Window, cx: &mut App| {
            entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let mut editor = JsonEditorView::new(
            "cell-editor",
            &self.input,
            cx.listener(|this, _, window, cx| this.save(window, cx)),
            cx.listener(|this, _, _, cx| this.close(cx)),
        )
        .validation_error(self.validation_error.clone())
        .readonly(self.vim.locked(false))
        .below_editor(self.vim.render_indicator(cx))
        .min_editor_height(px(300.0));

        if is_json {
            editor = editor.show_format_buttons(
                cx.listener(|this, _, window, cx| this.format(window, cx)),
                cx.listener(|this, _, window, cx| this.compact_json(window, cx)),
            );
        }

        let title = match &self.column {
            Some((name, type_name)) => dbflux_i18n::t!(
                "modals.cell_editor.title_column",
                column = name,
                type_name = type_name
            ),
            None if is_json => dbflux_i18n::t!("modals.cell_editor.title_json"),
            None => dbflux_i18n::t!("modals.cell_editor.title_text"),
        };

        Modal::new(title)
            .id("cell-editor-modal")
            .focus_handle(&self.focus_handle)
            .on_close(close)
            .key_context(ContextId::CellEditorModal.as_gpui_context())
            .icon(AppIcon::Pencil)
            .width(CELL_EDITOR_WIDTH)
            .child(
                vim_wrapper(
                    self.vim
                        .leader_scope(div(), cx)
                        .flex_1()
                        .min_h_0()
                        .flex()
                        .flex_col()
                        .on_action(cx.listener(|this, _: &SaveEdit, window, cx| {
                            this.save(window, cx);
                        })),
                    self.vim.input_id(),
                    cx,
                )
                .child(editor.render(cx)),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn cell_editor_keys_resolve_in_both_locales() {
        let keys = [
            "modals.cell_editor.title_json",
            "modals.cell_editor.title_text",
            "modals.cell_editor.title_column",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }

    #[test]
    fn cell_editor_title_json_diverges_between_locales() {
        let en = dbflux_i18n::t!("modals.cell_editor.title_json", locale = "en");
        let es = dbflux_i18n::t!("modals.cell_editor.title_json", locale = "es");
        assert_ne!(en, es);
    }
}

#[cfg(test)]
mod keyboard_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{CellEditorModal, CellEditorSaveEvent};
    use crate::actions::RunCommand;
    use gpui::{
        AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _,
        Render, SharedString, Styled as _, TestAppContext, Window, div,
    };

    /// Stands in for the document behind the dialog: records every keymap
    /// command that bubbles out of it.
    struct Host {
        modal: Entity<CellEditorModal>,
        commands: Vec<SharedString>,
        saves: usize,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .size_full()
                .on_action(cx.listener(|this, action: &RunCommand, _, _| {
                    this.commands.push(action.command.clone());
                }))
                .child(self.modal.clone())
        }
    }

    fn host(window: &mut Window, cx: &mut Context<Host>) -> Host {
        let modal = cx.new(|cx| CellEditorModal::new(window, cx));
        cx.subscribe(&modal, |host, _, _: &CellEditorSaveEvent, _| {
            host.saves += 1
        })
        .detach();

        Host {
            modal,
            commands: Vec::new(),
            saves: 0,
        }
    }

    #[gpui::test]
    fn escape_closes_the_cell_editor(cx: &mut TestAppContext) {
        cx.update(|cx| {
            gpui_component::init(cx);
            // The escape binding the app keymap gives the cell editor (see
            // the cell editor layer in `dbflux_ui_base::keymap`).
            cx.bind_keys([gpui::KeyBinding::new(
                "escape",
                crate::actions::Cancel,
                Some(dbflux_core::keymap_types::ContextId::CellEditorModal.as_gpui_context()),
            )]);
        });

        let (host, window) = cx.add_window_view(host);
        let modal = window.update(|_, cx| host.read(cx).modal.clone());

        window.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.open(0, 0, "{\"a\": 1}".to_string(), true, None, window, cx);
            });
        });
        window.run_until_parked();
        assert!(window.update(|_, cx| modal.read(cx).is_visible()));

        window.simulate_keystrokes("escape");

        assert!(
            !window.update(|_, cx| modal.read(cx).is_visible()),
            "Escape closes the editor through its own key context"
        );
    }

    /// Opening the editor puts the keyboard in its text buffer: text typed
    /// through the window reaches the editor without a click first, and
    /// Escape still closes the dialog from there.
    #[gpui::test]
    fn typing_right_after_opening_reaches_the_editor(cx: &mut TestAppContext) {
        cx.update(|cx| {
            gpui_component::init(cx);
            cx.bind_keys([gpui::KeyBinding::new(
                "escape",
                crate::actions::Cancel,
                Some(dbflux_core::keymap_types::ContextId::CellEditorModal.as_gpui_context()),
            )]);
        });

        let (host, window) = cx.add_window_view(host);
        let modal = window.update(|_, cx| host.read(cx).modal.clone());

        window.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.open(0, 0, String::new(), false, None, window, cx);
            });
        });
        window.update(|window, _| window.refresh());
        window.run_until_parked();

        window.simulate_input("typed");
        window.run_until_parked();

        let value = window.update(|_, cx| modal.read(cx).input.read(cx).value().to_string());
        assert_eq!(value, "typed", "typing reaches the editor without a click");

        window.simulate_keystrokes("escape");

        assert!(
            !window.update(|_, cx| modal.read(cx).is_visible()),
            "Escape closes the editor while its buffer has focus"
        );
    }

    /// The save shortcut still reaches the dialog while its buffer has focus.
    #[gpui::test]
    fn the_save_shortcut_saves_from_the_focused_editor(cx: &mut TestAppContext) {
        cx.update(|cx| {
            gpui_component::init(cx);
            cx.bind_keys([gpui::KeyBinding::new(
                "ctrl-s",
                crate::actions::SaveEdit,
                Some(dbflux_core::keymap_types::ContextId::CellEditorModal.as_gpui_context()),
            )]);
        });

        let (host, window) = cx.add_window_view(host);
        let modal = window.update(|_, cx| host.read(cx).modal.clone());

        let saved = std::rc::Rc::new(std::cell::RefCell::new(None::<String>));
        let _subscription = window.update(|_, cx| {
            let saved = saved.clone();
            cx.subscribe(&modal, move |_, event: &super::CellEditorSaveEvent, _| {
                *saved.borrow_mut() = Some(event.value.clone());
            })
        });

        window.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.open(0, 0, String::new(), false, None, window, cx);
            });
        });
        window.update(|window, _| window.refresh());
        window.run_until_parked();

        window.simulate_input("typed");
        window.simulate_keystrokes("ctrl-s");
        window.run_until_parked();

        assert_eq!(saved.borrow().as_deref(), Some("typed"));
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }

    /// Opens the cell editor with the app's modal bindings for Escape and
    /// Enter and the leader sequences `space s` (Save) and `space r` (Run
    /// query), Vim mode set to `vim`, and the keyboard in the editor.
    fn open_with_vim<'a>(
        cx: &'a mut TestAppContext,
        vim: bool,
        value: &str,
    ) -> (
        Entity<Host>,
        Entity<CellEditorModal>,
        &'a mut gpui::VisualTestContext,
    ) {
        cx.update(|cx| {
            gpui_component::init(cx);
            crate::actions::record_last_keystroke(cx);
            crate::vim::set_vim_enabled(cx, vim);
            let context = dbflux_core::keymap_types::ContextId::CellEditorModal.as_gpui_context();
            let leader = dbflux_core::keymap_types::ContextId::VimNormal.default_predicate();
            cx.bind_keys([
                gpui::KeyBinding::new("escape", crate::actions::Cancel, Some(context)),
                gpui::KeyBinding::new("enter", crate::actions::Execute, Some("Modal")),
                gpui::KeyBinding::new(
                    "space s",
                    crate::vim::LeaderCommand::new("save_query"),
                    Some(leader),
                ),
                gpui::KeyBinding::new(
                    "space r",
                    crate::vim::LeaderCommand::new("run_query"),
                    Some(leader),
                ),
            ]);
        });

        let (host, window) = cx.add_window_view(host);
        let modal = window.update(|_, cx| host.read(cx).modal.clone());
        let value = value.to_string();

        window.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.open(0, 0, value, false, None, window, cx);
            });
        });
        window.run_until_parked();

        (host, modal, window)
    }

    fn text_and_cursor(
        modal: &Entity<CellEditorModal>,
        window: &mut gpui::VisualTestContext,
    ) -> (String, usize) {
        window.update(|_, cx| {
            let state = modal.read(cx).input.read(cx);
            (state.value().to_string(), state.cursor())
        })
    }

    /// With Vim mode on, the first Escape leaves Insert mode and keeps the
    /// editor open, the second closes it; Enter in Normal mode moves down.
    #[gpui::test]
    fn vim_mode_edits_the_cell_and_escape_steps_out(cx: &mut TestAppContext) {
        let (_host, modal, window) = open_with_vim(cx, true, "ab\ncd");

        window.simulate_keystrokes("enter");
        window.run_until_parked();
        assert_eq!(text_and_cursor(&modal, window), ("ab\ncd".into(), 3));

        window.simulate_keystrokes("i");
        window.simulate_input("X");
        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert_eq!(text_and_cursor(&modal, window).0, "ab\nXcd");
        assert!(
            window.update(|_, cx| modal.read(cx).is_visible()),
            "the first Escape only leaves Insert mode"
        );

        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }

    /// With Vim mode off, keys type and one Escape closes, as before.
    #[gpui::test]
    fn without_vim_mode_escape_closes_at_once(cx: &mut TestAppContext) {
        let (_host, modal, window) = open_with_vim(cx, false, "ab");

        window.simulate_input("j");
        window.simulate_keystrokes("escape");
        window.run_until_parked();

        assert_eq!(text_and_cursor(&modal, window).0, "jab");
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }

    /// `<leader> s` in Normal mode saves through the dialog's own save and
    /// closes it; nothing reaches the document behind the dialog.
    #[gpui::test]
    fn leader_s_saves_the_cell_through_the_dialog(cx: &mut TestAppContext) {
        let (host, modal, window) = open_with_vim(cx, true, "ab");

        window.simulate_keystrokes("space s");
        window.run_until_parked();

        assert_eq!(window.update(|_, cx| host.read(cx).saves), 1);
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
        assert!(
            window.update(|_, cx| host.read(cx).commands.is_empty()),
            "Save never reaches the document behind the dialog"
        );
    }

    /// A leader command with no meaning in the dialog (run the query) does
    /// nothing: the dialog stays open with its text, no key reaches Vim, and
    /// nothing reaches the document behind it.
    #[gpui::test]
    fn leader_r_does_nothing_in_the_dialog(cx: &mut TestAppContext) {
        let (host, modal, window) = open_with_vim(cx, true, "ab");

        window.simulate_keystrokes("space r");
        window.run_until_parked();

        assert!(window.update(|_, cx| modal.read(cx).is_visible()));
        assert_eq!(text_and_cursor(&modal, window), ("ab".into(), 0));
        assert_eq!(window.update(|_, cx| host.read(cx).saves), 0);
        assert!(
            window.update(|_, cx| host.read(cx).commands.is_empty()),
            "Run query never reaches the document behind the dialog"
        );
    }

    /// Opens the dialog on `value` in a host window and replaces its text
    /// with `edited`, as typing would.
    fn open_and_edit<'a>(
        cx: &'a mut TestAppContext,
        value: &str,
        is_json: bool,
        edited: Option<&str>,
    ) -> (
        Entity<Host>,
        Entity<CellEditorModal>,
        &'a mut gpui::VisualTestContext,
    ) {
        cx.update(gpui_component::init);

        let (host, window) = cx.add_window_view(host);
        let modal = window.update(|_, cx| host.read(cx).modal.clone());
        let value = value.to_string();

        window.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.open(2, 1, value, is_json, None, window, cx);
            });
        });

        if let Some(edited) = edited {
            window.update(|window, cx| {
                modal.update(cx, |modal, cx| {
                    modal
                        .input
                        .update(cx, |input, cx| input.set_value(edited, window, cx));
                });
            });
        }
        window.run_until_parked();

        (host, modal, window)
    }

    /// An edited value is handed over and the dialog hides, without a save
    /// event: the host writes the value itself.
    #[gpui::test]
    fn an_edited_value_is_handed_over_for_a_close(cx: &mut TestAppContext) {
        let (host, modal, window) = open_and_edit(cx, "old", false, Some("new"));

        let taken =
            window.update(|_, cx| modal.update(cx, |modal, cx| modal.take_unsaved_edit(cx)));
        window.run_until_parked();

        let edit = taken
            .expect("a text value is always valid")
            .expect("an edited value is handed over");
        assert_eq!((edit.row, edit.col, edit.value.as_str()), (2, 1, "new"));
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
        assert_eq!(
            window.update(|_, cx| host.read(cx).saves),
            0,
            "the hand-over emits no save event"
        );
    }

    /// A value that was only looked at is not an edit.
    #[gpui::test]
    fn an_unchanged_value_is_not_handed_over(cx: &mut TestAppContext) {
        let (_host, modal, window) = open_and_edit(cx, "{\"a\":1}", true, None);

        let taken =
            window.update(|_, cx| modal.update(cx, |modal, cx| modal.take_unsaved_edit(cx)));

        assert_eq!(taken.map(|edit| edit.map(|edit| edit.value)), Ok(None));
        assert!(
            window.update(|_, cx| modal.read(cx).is_visible()),
            "the dialog stays as it is"
        );
    }

    /// A JSON value that does not parse stays in the dialog with its error.
    #[gpui::test]
    fn an_invalid_json_value_stays_in_the_dialog(cx: &mut TestAppContext) {
        let (_host, modal, window) = open_and_edit(cx, "{\"a\":1}", true, Some("{\"a\":"));

        let taken =
            window.update(|_, cx| modal.update(cx, |modal, cx| modal.take_unsaved_edit(cx)));

        assert!(taken.is_err(), "an invalid value is refused");
        let (visible, has_error) = window.update(|_, cx| {
            let modal = modal.read(cx);
            (modal.is_visible(), modal.validation_error.is_some())
        });
        assert!(visible, "the dialog keeps the value");
        assert!(has_error, "the dialog shows why");
    }

    /// A closed dialog holds nothing.
    #[gpui::test]
    fn a_closed_dialog_hands_over_nothing(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);
        let (host, window) = cx.add_window_view(host);
        let modal = window.update(|_, cx| host.read(cx).modal.clone());

        let taken =
            window.update(|_, cx| modal.update(cx, |modal, cx| modal.take_unsaved_edit(cx)));

        assert_eq!(taken.map(|edit| edit.map(|edit| edit.value)), Ok(None));
    }
}
