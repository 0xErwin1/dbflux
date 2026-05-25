use crate::components::json_editor_view::{self, JsonEditorView};
use crate::composites::ModalFrame;
use crate::controls::InputState;
use crate::icon::IconSource;
use crate::icons::AppIcon;
use crate::primitives::Icon;
use crate::tokens::Heights;
use dbflux_core::keymap_types::ContextId;
use gpui::*;

/// Event emitted when the user clicks "Import" with valid JSON.
#[derive(Clone)]
pub struct ImportDashboardConfirmed {
    /// The raw CloudWatch dashboard JSON supplied by the user.
    pub json: String,
}

/// Event emitted when the user cancels the modal.
#[derive(Clone)]
pub struct ImportDashboardCancelled;

/// Modal for pasting a CloudWatch dashboard JSON and triggering an import.
///
/// Opens with an empty JSON editor. On confirm, emits `ImportDashboardConfirmed`.
/// The caller is responsible for running the actual import logic.
pub struct ModalImportDashboard {
    visible: bool,
    input: gpui::Entity<InputState>,
    focus_handle: gpui::FocusHandle,
    validation_error: Option<String>,
}

impl ModalImportDashboard {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor("json")
                .line_number(true)
                .soft_wrap(true)
        });

        Self {
            visible: false,
            input,
            focus_handle: cx.focus_handle(),
            validation_error: None,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Open the modal and focus the JSON editor.
    pub fn open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = true;
        self.validation_error = None;

        self.input.update(cx, |state, cx| {
            state.set_value("", window, cx);
            state.focus(window, cx);
        });

        self.focus_handle.focus(window);
        cx.notify();
    }

    /// Close the modal without emitting a confirmation.
    pub fn close(&mut self, cx: &mut Context<Self>) {
        if self.visible {
            self.visible = false;
            cx.emit(ImportDashboardCancelled);
        }

        cx.notify();
    }

    fn confirm(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();

        if let Err(e) = json_editor_view::validate_json(&value, false) {
            self.validation_error = Some(e);
            cx.notify();
            return;
        }

        self.visible = false;
        cx.emit(ImportDashboardConfirmed { json: value });
        cx.notify();
    }
}

impl EventEmitter<ImportDashboardConfirmed> for ModalImportDashboard {}
impl EventEmitter<ImportDashboardCancelled> for ModalImportDashboard {}

impl Render for ModalImportDashboard {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let entity = cx.entity().downgrade();

        let close = move |_window: &mut Window, cx: &mut App| {
            entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let editor = JsonEditorView::new(
            "import-dashboard",
            &self.input,
            cx.listener(|this, _, window, cx| this.confirm(window, cx)),
            cx.listener(|this, _, _, cx| this.close(cx)),
        )
        .validation_error(self.validation_error.clone())
        .min_editor_height(px(400.0))
        .show_format_buttons(
            cx.listener(|this, _, window, cx| {
                let value = this.input.read(cx).value().to_string();
                if let Some(formatted) = json_editor_view::format_json(&value) {
                    this.input.update(cx, |state, cx| {
                        state.set_value(&formatted, window, cx);
                    });
                    this.validation_error = None;
                }
            }),
            cx.listener(|this, _, window, cx| {
                let value = this.input.read(cx).value().to_string();
                if let Some(compact) = json_editor_view::compact_json(&value) {
                    this.input.update(cx, |state, cx| {
                        state.set_value(&compact, window, cx);
                    });
                    this.validation_error = None;
                }
            }),
        );

        ModalFrame::new("import-dashboard-modal", &self.focus_handle, close)
            .key_context(ContextId::SqlPreviewModal.as_gpui_context())
            .close_icon(IconSource::Svg(AppIcon::X.path().into()))
            .header_leading(
                Icon::new(AppIcon::ChartSpline)
                    .size(Heights::ICON_SM)
                    .primary(),
            )
            .title("Import CloudWatch Dashboard")
            .width(px(800.0))
            .height(px(600.0))
            .top_offset(px(60.0))
            .block_scroll()
            .child(editor.render(cx))
            .render(cx)
    }
}
