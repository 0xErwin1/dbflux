use crate::ui::components::json_editor_view::{self, JsonEditorView};
use crate::ui::components::modal_frame::ModalFrame;
use crate::ui::icons::AppIcon;
use gpui::*;
use gpui_component::input::InputState;

/// Event emitted when the modal editor saves.
#[derive(Clone)]
pub struct CellEditorSaveEvent {
    pub row: usize,
    pub col: usize,
    pub value: String,
}

/// Modal editor for JSON and long text values.
pub struct CellEditorModal {
    visible: bool,
    row: usize,
    col: usize,
    is_json: bool,
    input: Entity<InputState>,
    focus_handle: FocusHandle,
    validation_error: Option<String>,
}

impl CellEditorModal {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor("json")
                .line_number(true)
                .soft_wrap(true)
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

        self.focus_handle.focus(window);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.validation_error = None;
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
            .title(if is_json { "Edit JSON" } else { "Edit Text" })
            .icon(AppIcon::Pencil)
            .width(px(900.0))
            .height(px(600.0))
            .child(editor.render(cx))
            .render(cx)
    }
}
