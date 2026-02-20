use crate::ui::components::json_editor_view::{self, JsonEditorView};
use crate::ui::components::modal_frame::ModalFrame;
use crate::ui::icons::AppIcon;
use gpui::*;
use gpui_component::input::InputState;

/// Event emitted when the modal editor saves a document.
#[derive(Clone)]
pub struct DocumentPreviewSaveEvent {
    pub doc_index: usize,
    pub document_json: String,
}

/// Event emitted when the document preview modal is closed.
#[derive(Clone)]
pub struct DocumentPreviewClosedEvent;

/// Modal editor for viewing and editing full MongoDB documents.
pub struct DocumentPreviewModal {
    visible: bool,
    doc_index: usize,
    input: Entity<InputState>,
    focus_handle: FocusHandle,
    validation_error: Option<String>,
}

impl DocumentPreviewModal {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor("json")
                .line_number(true)
                .soft_wrap(true)
        });

        Self {
            visible: false,
            doc_index: 0,
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
        doc_index: usize,
        document_json: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.doc_index = doc_index;
        self.visible = true;
        self.validation_error = None;

        let formatted = json_editor_view::format_json(&document_json).unwrap_or(document_json);

        self.input.update(cx, |state, cx| {
            state.set_value(&formatted, window, cx);
            state.focus(window, cx);
        });

        self.focus_handle.focus(window);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        let was_visible = self.visible;
        self.visible = false;
        self.validation_error = None;

        if was_visible {
            cx.emit(DocumentPreviewClosedEvent);
        }

        cx.notify();
    }

    fn save(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();

        if let Err(e) = json_editor_view::validate_json(&value, false) {
            self.validation_error = Some(e);
            cx.notify();
            return;
        }

        cx.emit(DocumentPreviewSaveEvent {
            doc_index: self.doc_index,
            document_json: value,
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

impl EventEmitter<DocumentPreviewSaveEvent> for DocumentPreviewModal {}
impl EventEmitter<DocumentPreviewClosedEvent> for DocumentPreviewModal {}

impl Render for DocumentPreviewModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let entity = cx.entity().downgrade();

        let close = move |_window: &mut Window, cx: &mut App| {
            entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let editor = JsonEditorView::new(
            "doc-preview",
            &self.input,
            cx.listener(|this, _, window, cx| this.save(window, cx)),
            cx.listener(|this, _, _, cx| this.close(cx)),
        )
        .validation_error(self.validation_error.clone())
        .min_editor_height(px(400.0))
        .show_format_buttons(
            cx.listener(|this, _, window, cx| this.format(window, cx)),
            cx.listener(|this, _, window, cx| this.compact_json(window, cx)),
        );

        ModalFrame::new("document-preview-modal", &self.focus_handle, close)
            .title("Document Preview")
            .icon(AppIcon::Braces)
            .width(px(1000.0))
            .height(px(700.0))
            .top_offset(px(60.0))
            .block_scroll()
            .child(editor.render(cx))
            .render(cx)
    }
}
