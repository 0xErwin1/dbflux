use crate::actions::SaveEdit;
use crate::components::json_editor_view::{self, JsonEditorView};
use crate::icons::AppIcon;
use crate::modals::Modal;
use dbflux_core::keymap_types::ContextId;
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
    /// Name and type of the edited column, for the title.
    column: Option<(String, String)>,
    input: Entity<EditorState>,
    focus_handle: FocusHandle,
    validation_error: Option<String>,
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

        Self {
            visible: false,
            row: 0,
            col: 0,
            is_json: false,
            column: None,
            input,
            focus_handle: cx.focus_handle(),
            validation_error: None,
            _input_observation: input_observation,
        }
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

        cx.notify();
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

impl EventEmitter<CellEditorSaveEvent> for CellEditorModal {}
impl EventEmitter<CellEditorClosedEvent> for CellEditorModal {}

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
                div()
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .flex_col()
                    .on_action(cx.listener(|this, _: &SaveEdit, window, cx| {
                        this.save(window, cx);
                    }))
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
    use super::CellEditorModal;
    use gpui::{
        AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _,
        TestAppContext, Window, div,
    };

    struct Host {
        modal: Entity<CellEditorModal>,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().child(self.modal.clone())
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

        let (host, window) = cx.add_window_view(|window, cx| Host {
            modal: cx.new(|cx| CellEditorModal::new(window, cx)),
        });
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

        let (host, window) = cx.add_window_view(|window, cx| Host {
            modal: cx.new(|cx| CellEditorModal::new(window, cx)),
        });
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

        let (host, window) = cx.add_window_view(|window, cx| Host {
            modal: cx.new(|cx| CellEditorModal::new(window, cx)),
        });
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
}
