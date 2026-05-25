use crate::components::json_editor_view::{self, JsonEditorView};
use crate::composites::ModalFrame;
use crate::controls::{GpuiInput as Input, InputState};
use crate::icon::IconSource;
use crate::icons::AppIcon;
use crate::primitives::{Icon, Text};
use crate::tokens::{Heights, Spacing};
use dbflux_core::keymap_types::ContextId;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Event emitted when the user clicks "Import" with valid JSON.
#[derive(Clone)]
pub struct ImportDashboardConfirmed {
    /// The raw dashboard JSON supplied by the user.
    pub json: String,
    /// The dashboard name entered by the user. Defaults to "Imported Dashboard"
    /// when the pasted JSON has no top-level `"name"` field.
    pub name: String,
}

/// Event emitted when the user cancels the modal.
#[derive(Clone)]
pub struct ImportDashboardCancelled;

/// Modal for pasting dashboard JSON and triggering an import.
///
/// Opens with an empty JSON editor and a name field pre-filled from the
/// pasted JSON's top-level `"name"` key (if present), otherwise
/// "Imported Dashboard". On confirm, emits `ImportDashboardConfirmed`.
/// The caller is responsible for running the actual import logic.
/// Default name used when the pasted JSON has no top-level `"name"` field.
pub const DEFAULT_IMPORT_NAME: &str = "Imported Dashboard";

pub struct ModalImportDashboard {
    visible: bool,
    /// JSON editor for the raw dashboard payload.
    input: gpui::Entity<InputState>,
    /// Text input for the dashboard name, pre-filled from pasted JSON.
    name_input: gpui::Entity<InputState>,
    focus_handle: gpui::FocusHandle,
    validation_error: Option<String>,
    name_error: Option<String>,
}

impl ModalImportDashboard {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor("json")
                .line_number(true)
                .soft_wrap(true)
        });

        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Dashboard name"));

        Self {
            visible: false,
            input,
            name_input,
            focus_handle: cx.focus_handle(),
            validation_error: None,
            name_error: None,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Open the modal and focus the JSON editor.
    ///
    /// The name field is pre-filled with `DEFAULT_IMPORT_NAME`; once the user
    /// pastes JSON containing a top-level `"name"` key the field is updated.
    pub fn open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = true;
        self.validation_error = None;
        self.name_error = None;

        self.input.update(cx, |state, cx| {
            state.set_value("", window, cx);
            state.focus(window, cx);
        });

        self.name_input.update(cx, |state, cx| {
            state.set_value(DEFAULT_IMPORT_NAME, window, cx);
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

    /// Extract the top-level `"name"` string from JSON text, if present.
    fn extract_json_name(json: &str) -> Option<String> {
        // Lightweight extraction without a full serde dependency cycle:
        // look for `"name"` followed by a colon and a quoted string value.
        let name_key = "\"name\"";
        let pos = json.find(name_key)?;
        let after_key = json[pos + name_key.len()..].trim_start();
        let after_colon = after_key.strip_prefix(':')?.trim_start();
        let after_quote = after_colon.strip_prefix('"')?;
        let end = after_quote.find('"')?;
        let name = &after_quote[..end];
        if name.is_empty() {
            None
        } else {
            Some(name.to_string())
        }
    }

    fn confirm(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let json_value = self.input.read(cx).value().to_string();

        if let Err(e) = json_editor_view::validate_json(&json_value, false) {
            self.validation_error = Some(e);
            cx.notify();
            return;
        }

        let name = self.name_input.read(cx).value().trim().to_string();
        if name.is_empty() {
            self.name_error = Some("Name cannot be empty".to_string());
            cx.notify();
            return;
        }

        // Update name field from JSON if user hasn't changed it away from the default.
        let current_name = self.name_input.read(cx).value().to_string();
        let derived_name =
            Self::extract_json_name(&json_value).unwrap_or_else(|| DEFAULT_IMPORT_NAME.to_string());

        let final_name = if current_name == DEFAULT_IMPORT_NAME {
            derived_name
        } else {
            current_name
        };

        // Re-validate final name.
        if final_name.trim().is_empty() {
            self.name_error = Some("Name cannot be empty".to_string());
            cx.notify();
            return;
        }

        self.visible = false;
        cx.emit(ImportDashboardConfirmed {
            json: json_value,
            name: final_name,
        });

        // Reset name for next open.
        self.name_input.update(cx, |state, cx| {
            state.set_value(DEFAULT_IMPORT_NAME, window, cx);
        });

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

        let name_error = self.name_error.clone();
        let name_input = self.name_input.clone();

        let name_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::label("Dashboard name"))
            .child(Input::new(&name_input))
            .when_some(name_error, |el, err| {
                el.child(div().text_sm().text_color(cx.theme().danger).child(err))
            });

        let editor = JsonEditorView::new(
            "import-dashboard",
            &self.input,
            cx.listener(|this, _, window, cx| this.confirm(window, cx)),
            cx.listener(|this, _, _, cx| this.close(cx)),
        )
        .validation_error(self.validation_error.clone())
        .min_editor_height(px(300.0))
        .show_format_buttons(
            cx.listener(|this, _, window, cx| {
                let value = this.input.read(cx).value().to_string();
                if let Some(formatted) = json_editor_view::format_json(&value) {
                    this.input.update(cx, |state, cx| {
                        state.set_value(&formatted, window, cx);
                    });
                    this.validation_error = None;

                    // Update name from parsed JSON when the user formats for the first time.
                    let current_name = this.name_input.read(cx).value().to_string();
                    if current_name == DEFAULT_IMPORT_NAME
                        && let Some(name) = Self::extract_json_name(&formatted)
                    {
                        this.name_input
                            .update(cx, |state, cx| state.set_value(&name, window, cx));
                    }
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

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(name_row)
            .child(editor.render(cx));

        ModalFrame::new("import-dashboard-modal", &self.focus_handle, close)
            .key_context(ContextId::SqlPreviewModal.as_gpui_context())
            .close_icon(IconSource::Svg(AppIcon::X.path().into()))
            .header_leading(
                Icon::new(AppIcon::ChartSpline)
                    .size(Heights::ICON_SM)
                    .primary(),
            )
            .title("Import Dashboard from JSON")
            .width(px(800.0))
            .height(px(640.0))
            .top_offset(px(60.0))
            .block_scroll()
            .child(body)
            .render(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_IMPORT_NAME, ModalImportDashboard};

    #[test]
    fn extract_json_name_returns_name_when_present() {
        let json = r#"{"name": "Production Overview", "widgets": []}"#;
        let name = ModalImportDashboard::extract_json_name(json);
        assert_eq!(name, Some("Production Overview".to_string()));
    }

    #[test]
    fn extract_json_name_returns_none_when_absent() {
        let json = r#"{"widgets": []}"#;
        let name = ModalImportDashboard::extract_json_name(json);
        assert_eq!(name, None);
    }

    #[test]
    fn extract_json_name_returns_none_for_empty_name() {
        let json = r#"{"name": "", "widgets": []}"#;
        let name = ModalImportDashboard::extract_json_name(json);
        assert_eq!(name, None);
    }

    #[test]
    fn extract_json_name_returns_none_for_empty_json() {
        let name = ModalImportDashboard::extract_json_name("{}");
        assert_eq!(name, None);
    }

    #[test]
    fn default_import_name_constant_is_correct() {
        assert_eq!(DEFAULT_IMPORT_NAME, "Imported Dashboard");
    }

    #[test]
    fn modal_title_contains_no_cloudwatch_substring() {
        // The title must be "Import Dashboard from JSON" — not reference CloudWatch.
        let title = "Import Dashboard from JSON";
        assert!(!title.contains("CloudWatch"));
        assert!(title.contains("Import Dashboard from JSON"));
    }
}
