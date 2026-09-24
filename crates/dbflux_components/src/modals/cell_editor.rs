use crate::components::json_editor_view::{self, JsonEditorView};
use crate::composites::ModalFrame;
use crate::icon::IconSource;
use crate::icons::AppIcon;
use crate::primitives::Icon;
use crate::tokens::Heights;
use dbflux_core::keymap_types::ContextId;
use gpui::*;
use gpui_component::input::EditorState;

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
    input: Entity<EditorState>,
    focus_handle: FocusHandle,
    validation_error: Option<String>,
}

impl CellEditorModal {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        // soft_wrap defaults to true in 0.6.1, so the old explicit builder is gone.
        let input = cx.new(|cx| {
            EditorState::new(window, cx)
                .language("json")
                .line_number(true)
        });

        Self {
            visible: false,
            row: 0,
            col: 0,
            is_json: false,
            input,
            focus_handle: cx.focus_handle(),
            validation_error: None,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(
        &mut self,
        row: usize,
        col: usize,
        value: String,
        is_json: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.row = row;
        self.col = col;
        self.is_json = is_json;
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

        self.focus_handle.focus(window, cx);
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

        ModalFrame::new("cell-editor-modal", &self.focus_handle, close)
            .key_context(ContextId::CellEditorModal.as_gpui_context())
            .close_icon(IconSource::Svg(AppIcon::X.path().into()))
            .header_leading(Icon::new(AppIcon::Pencil).size(Heights::ICON_SM).primary())
            .title(if is_json {
                dbflux_i18n::t!("modals.cell_editor.title_json")
            } else {
                dbflux_i18n::t!("modals.cell_editor.title_text")
            })
            .width(px(900.0))
            .height(px(600.0))
            .child(editor.render(cx))
            .render(cx)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn cell_editor_keys_resolve_in_both_locales() {
        let keys = [
            "modals.cell_editor.title_json",
            "modals.cell_editor.title_text",
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
            crate::modals::register_modal_keybindings(cx);
        });

        let (host, window) = cx.add_window_view(|window, cx| Host {
            modal: cx.new(|cx| CellEditorModal::new(window, cx)),
        });
        let modal = window.update(|_, cx| host.read(cx).modal.clone());

        window.update(|window, cx| {
            modal.update(cx, |modal, cx| {
                modal.open(0, 0, "{\"a\": 1}".to_string(), true, window, cx);
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
}
