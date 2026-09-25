use crate::controls::Button;
use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon};
use crate::tokens::{ChamferCut, ChromeColors, Fields, ModalMetrics};
use crate::typography::AppFonts;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::{Editor, EditorState};

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
            Err(dbflux_i18n::t!("components.json_editor.empty_error"))
        };
    }

    serde_json::from_str::<serde_json::Value>(s)
        .map(|_| ())
        .map_err(|e| e.to_string().replace('\n', " "))
}

/// Label of the save shortcut bound to [`crate::actions::SaveEdit`] inside the
/// cell editor and the document preview.
pub const SAVE_SHORTCUT_LABEL: &str = if cfg!(target_os = "macos") {
    "Cmd S"
} else {
    "Ctrl S"
};

/// Status line under a JSON editor: green "valid JSON · N lines" while the
/// text parses, the parse error in red otherwise.
fn json_status(value: &str, error: Option<String>) -> Result<String, String> {
    if let Some(error) = error {
        return Err(error);
    }

    validate_json(value, true)?;

    let lines = value.lines().count().max(1);

    Ok(if lines == 1 {
        dbflux_i18n::t!("components.json_editor.valid.one", count = lines)
    } else {
        dbflux_i18n::t!("components.json_editor.valid.many", count = lines)
    })
}

/// Renders a JSON/text editor with its status line and footer (P1Flows,
/// "Edit properties"): the editor on the ground in a cut-8 frame, a status
/// line for JSON, and a footer with Format and Compact (JSON only), Cancel
/// and Save.
pub struct JsonEditorView {
    id_prefix: &'static str,
    input: Entity<EditorState>,
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
        input: &Entity<EditorState>,
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

        let status = self.show_format_buttons.then(|| {
            let value = self.input.read(cx).value().to_string();

            let (icon, color, text) = match json_status(&value, self.validation_error.clone()) {
                Ok(text) => (AppIcon::CircleCheck, theme.success, text),
                Err(error) => (AppIcon::CircleAlert, theme.danger, error),
            };

            div()
                .flex()
                .items_center()
                .gap(ModalMetrics::FIELD_GAP)
                .text_size(ModalMetrics::FIELD_LABEL_FONT)
                .text_color(color)
                .child(Icon::new(icon).size(Fields::CHEVRON).color(color))
                .child(div().min_w_0().truncate().child(text))
        });

        let error_line = (!self.show_format_buttons)
            .then_some(self.validation_error)
            .flatten()
            .map(|error| {
                div()
                    .text_size(ModalMetrics::FIELD_LABEL_FONT)
                    .text_color(theme.danger)
                    .child(error)
            });

        let body = div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .p(ModalMetrics::PADDING)
            .child(
                div()
                    .relative()
                    .flex_1()
                    .min_h(self.min_editor_height)
                    .p(ModalMetrics::CODE_PADDING_X)
                    .overflow_hidden()
                    .child(
                        Chamfer::new(ChamferCut::INPUT)
                            .fill(theme.background)
                            .border(theme.border),
                    )
                    .child(
                        Editor::new(&self.input)
                            .w_full()
                            .h_full()
                            .font_family(AppFonts::MONO)
                            .text_size(ModalMetrics::CODE_FONT)
                            .text_color(ChromeColors::strong(theme)),
                    ),
            )
            .children(status)
            .children(error_line);

        let footer = div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .justify_end()
            .gap(ModalMetrics::FOOTER_GAP)
            .px(ModalMetrics::PADDING)
            .py(ModalMetrics::FOOTER_PADDING_Y)
            .border_t_1()
            .border_color(theme.border)
            .when_some(self.on_format, |footer, on_format| {
                footer.child(
                    Button::new(
                        SharedString::from(format!("{prefix}-format")),
                        dbflux_i18n::t!("components.json_editor.format"),
                    )
                    .icon(AppIcon::Zap)
                    .on_click(on_format),
                )
            })
            .when_some(self.on_compact, |footer, on_compact| {
                footer.child(
                    Button::new(
                        SharedString::from(format!("{prefix}-compact")),
                        dbflux_i18n::t!("components.json_editor.compact"),
                    )
                    .on_click(on_compact),
                )
            })
            .child(
                Button::new(
                    SharedString::from(format!("{prefix}-cancel")),
                    dbflux_i18n::t!("components.json_editor.cancel"),
                )
                .on_click(self.on_cancel),
            )
            .child(
                Button::new(
                    SharedString::from(format!("{prefix}-save")),
                    dbflux_i18n::t!("components.json_editor.save"),
                )
                .primary()
                .icon(AppIcon::Save)
                .kbd(SAVE_SHORTCUT_LABEL)
                .on_click(self.on_save),
            );

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .child(body)
            .child(footer)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{json_status, validate_json};

    #[test]
    fn status_counts_the_lines_of_valid_json() {
        assert_eq!(
            json_status("{\n  \"a\": 1\n}", None),
            Ok(dbflux_i18n::t!(
                "components.json_editor.valid.many",
                count = 3
            ))
        );
        assert!(json_status("{", None).is_err());
        assert_eq!(
            json_status("{}", Some("bad".to_string())),
            Err("bad".to_string())
        );
    }

    #[test]
    fn validate_json_empty_error_matches_translated_catalog() {
        let error = validate_json("", false).unwrap_err();
        assert_eq!(
            error,
            dbflux_i18n::t!("components.json_editor.empty_error", locale = "en")
        );
    }

    #[test]
    fn validate_json_empty_error_diverges_between_locales() {
        let en = dbflux_i18n::t!("components.json_editor.empty_error", locale = "en");
        let es = dbflux_i18n::t!("components.json_editor.empty_error", locale = "es");
        assert_ne!(en, es);
    }

    #[test]
    fn json_editor_keys_resolve_in_both_locales() {
        let keys = [
            "components.json_editor.empty_error",
            "components.json_editor.format",
            "components.json_editor.compact",
            "components.json_editor.cancel",
            "components.json_editor.save",
            "components.json_editor.valid.one",
            "components.json_editor.valid.many",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }
}
