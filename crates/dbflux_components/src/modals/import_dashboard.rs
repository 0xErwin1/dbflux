use crate::components::json_editor_view;
use crate::controls::{GpuiInput as Input, InputState};
use crate::icons::AppIcon;
use crate::modals::shell::{ModalShell, ModalVariant};
use crate::primitives::{Icon, Text};
use crate::tokens::{FontSizes, Heights, Spacing};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariant, ButtonVariants};

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

/// Default name used when the pasted JSON has no top-level `"name"` field.
pub const DEFAULT_IMPORT_NAME: &str = "Imported Dashboard";

/// Modal for pasting dashboard JSON and triggering an import.
///
/// Uses the standard `ModalShell` chrome (header / scrollable body / footer
/// with top divider) so it matches the rest of the modal surfaces in the app.
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
                .placeholder("Paste dashboard JSON here…")
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
    /// formats the pasted JSON containing a top-level `"name"` key the field
    /// is updated.
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

    fn confirm(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
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

        let current_name = self.name_input.read(cx).value().to_string();
        let derived_name =
            Self::extract_json_name(&json_value).unwrap_or_else(|| DEFAULT_IMPORT_NAME.to_string());

        let final_name = if current_name == DEFAULT_IMPORT_NAME {
            derived_name
        } else {
            current_name
        };

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

        cx.notify();
    }

    fn format_json(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();
        if let Some(formatted) = json_editor_view::format_json(&value) {
            self.input.update(cx, |state, cx| {
                state.set_value(&formatted, window, cx);
            });
            self.validation_error = None;

            let current_name = self.name_input.read(cx).value().to_string();
            if current_name == DEFAULT_IMPORT_NAME
                && let Some(name) = Self::extract_json_name(&formatted)
            {
                self.name_input
                    .update(cx, |state, cx| state.set_value(&name, window, cx));
            }
        }
        cx.notify();
    }

    fn compact_json(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();
        if let Some(compact) = json_editor_view::compact_json(&value) {
            self.input.update(cx, |state, cx| {
                state.set_value(&compact, window, cx);
            });
            self.validation_error = None;
        }
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

        let theme = cx.theme();
        let entity = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let name_error = self.name_error.clone();
        let validation_error = self.validation_error.clone();

        // ---- Body --------------------------------------------------------
        // Name row
        let name_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::label("Dashboard name"))
            .child(Input::new(&self.name_input))
            .when_some(name_error, |el, err| {
                el.child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(theme.danger)
                        .child(err),
                )
            });

        // JSON editor — bordered container that fills the remaining space.
        let editor = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::label("Dashboard JSON"))
            .child(
                div()
                    .border_1()
                    .border_color(theme.border)
                    .rounded(px(4.0)) // guardrail-allow: border radius
                    .bg(theme.background)
                    .h(px(360.0))
                    .p(Spacing::SM)
                    .overflow_hidden()
                    .child(Input::new(&self.input).w_full().h_full()),
            );

        // Validation banner (only when there is an error).
        let validation_banner = validation_error.map(|err| {
            div()
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .px(Spacing::SM)
                .py(Spacing::XS)
                .bg(theme.danger.opacity(0.1))
                .border_1()
                .border_color(theme.danger.opacity(0.3))
                .rounded(px(4.0)) // guardrail-allow: border radius
                .child(
                    Icon::new(AppIcon::CircleAlert)
                        .size(Heights::ICON_SM)
                        .danger(),
                )
                .child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(theme.danger)
                        .child(err),
                )
        });

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(name_row)
            .child(editor)
            .when_some(validation_banner, |el, banner| el.child(banner));

        // ---- Footer ------------------------------------------------------
        let on_format = cx.listener(|this, _: &gpui::ClickEvent, window, cx| {
            this.format_json(window, cx);
        });
        let on_compact = cx.listener(|this, _: &gpui::ClickEvent, window, cx| {
            this.compact_json(window, cx);
        });
        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.close(cx);
        });
        let on_save = cx.listener(|this, _: &gpui::ClickEvent, window, cx| {
            this.confirm(window, cx);
        });

        let footer = div()
            .w_full()
            .flex()
            .items_center()
            .justify_between()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        Button::new("import-dashboard-format")
                            .label("Format")
                            .small()
                            .with_variant(ButtonVariant::Ghost)
                            .on_click(on_format),
                    )
                    .child(
                        Button::new("import-dashboard-compact")
                            .label("Compact")
                            .small()
                            .with_variant(ButtonVariant::Ghost)
                            .on_click(on_compact),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        Button::new("import-dashboard-cancel")
                            .label("Cancel")
                            .on_click(on_cancel),
                    )
                    .child(
                        Button::new("import-dashboard-save")
                            .label("Import")
                            .with_variant(ButtonVariant::Primary)
                            .on_click(on_save),
                    ),
            );

        ModalShell::new(
            "Import Dashboard from JSON",
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Default)
        .width(px(720.0))
        .on_close(close)
        .into_any_element()
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
        let title = "Import Dashboard from JSON";
        assert!(!title.contains("CloudWatch"));
        assert!(title.contains("Import Dashboard from JSON"));
    }
}
