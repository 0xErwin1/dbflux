use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Spacing};
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariant, ButtonVariants};
use gpui_component::input::{Input, InputState};

type ClickHandler = Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + 'static>;

/// Pretty-print a JSON string. Returns `None` if the input is not valid JSON.
pub fn format_json(s: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(s).ok()?;
    serde_json::to_string_pretty(&parsed).ok()
}

/// Compact a JSON string to a single line. Returns `None` if the input is not valid JSON.
pub fn compact_json(s: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(s).ok()?;
    serde_json::to_string(&parsed).ok()
}

/// Validate a JSON string. When `allow_empty` is true, an empty string is accepted.
pub fn validate_json(s: &str, allow_empty: bool) -> Result<(), String> {
    if s.is_empty() {
        return if allow_empty {
            Ok(())
        } else {
            Err("Document cannot be empty".to_string())
        };
    }

    serde_json::from_str::<serde_json::Value>(s)
        .map(|_| ())
        .map_err(|e| e.to_string().replace('\n', " "))
}

/// Renders a JSON/text editor area with validation banner and footer buttons.
///
/// The footer has Format/Compact buttons on the left (when `show_format_buttons` is true)
/// and Cancel/Save buttons on the right.
pub struct JsonEditorView {
    id_prefix: &'static str,
    input: Entity<InputState>,
    validation_error: Option<String>,
    show_format_buttons: bool,
    min_editor_height: Pixels,
    on_format: Option<ClickHandler>,
    on_compact: Option<ClickHandler>,
    on_save: ClickHandler,
    on_cancel: ClickHandler,
}

impl JsonEditorView {
    pub fn new(
        id_prefix: &'static str,
        input: &Entity<InputState>,
        on_save: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
        on_cancel: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        Self {
            id_prefix,
            input: input.clone(),
            validation_error: None,
            show_format_buttons: false,
            min_editor_height: px(300.0),
            on_format: None,
            on_compact: None,
            on_save: Box::new(on_save),
            on_cancel: Box::new(on_cancel),
        }
    }

    pub fn validation_error(mut self, error: Option<String>) -> Self {
        self.validation_error = error;
        self
    }

    pub fn show_format_buttons(
        mut self,
        on_format: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
        on_compact: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.show_format_buttons = true;
        self.on_format = Some(Box::new(on_format));
        self.on_compact = Some(Box::new(on_compact));
        self
    }

    pub fn min_editor_height(mut self, height: Pixels) -> Self {
        self.min_editor_height = height;
        self
    }

    pub fn render(self, cx: &App) -> AnyElement {
        let theme = cx.theme();
        let prefix = self.id_prefix;

        let mut el = div()
            .flex_1()
            .flex()
            .flex_col()
            // Editor
            .child(
                div()
                    .flex_1()
                    .p(Spacing::MD)
                    .min_h(self.min_editor_height)
                    .overflow_hidden()
                    .child(Input::new(&self.input).w_full().h_full()),
            );

        // Validation error banner
        if let Some(error_msg) = self.validation_error {
            el = el.child(
                div()
                    .px(Spacing::MD)
                    .py(Spacing::SM)
                    .bg(theme.danger.opacity(0.1))
                    .border_t_1()
                    .border_color(theme.danger.opacity(0.3))
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        svg()
                            .path(AppIcon::CircleAlert.path())
                            .size_4()
                            .text_color(theme.danger),
                    )
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .text_color(theme.danger)
                            .child(error_msg),
                    ),
            );
        }

        // Footer
        let mut left_buttons = div().flex().items_center().gap(Spacing::SM);

        if self.show_format_buttons {
            if let Some(on_format) = self.on_format {
                left_buttons = left_buttons.child(
                    Button::new(SharedString::from(format!("{}-format", prefix)))
                        .label("Format")
                        .small()
                        .with_variant(ButtonVariant::Ghost)
                        .on_click(on_format),
                );
            }
            if let Some(on_compact) = self.on_compact {
                left_buttons = left_buttons.child(
                    Button::new(SharedString::from(format!("{}-compact", prefix)))
                        .label("Compact")
                        .small()
                        .with_variant(ButtonVariant::Ghost)
                        .on_click(on_compact),
                );
            }
        }

        let right_buttons = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new(SharedString::from(format!("{}-cancel", prefix)))
                    .label("Cancel")
                    .small()
                    .with_variant(ButtonVariant::Ghost)
                    .on_click(self.on_cancel),
            )
            .child(
                Button::new(SharedString::from(format!("{}-save", prefix)))
                    .label("Save")
                    .small()
                    .with_variant(ButtonVariant::Primary)
                    .on_click(self.on_save),
            );

        el = el.child(
            div()
                .px(Spacing::MD)
                .py(Spacing::SM)
                .border_t_1()
                .border_color(theme.border)
                .flex()
                .items_center()
                .justify_between()
                .child(left_buttons)
                .child(right_buttons),
        );

        el.into_any_element()
    }
}
