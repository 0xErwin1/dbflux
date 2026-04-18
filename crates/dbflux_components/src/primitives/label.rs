use gpui::prelude::*;
use gpui::{div, App, Hsla, SharedString, Window};

use crate::primitives::Text;
use crate::typography::RequiredMarker;

/// Stateless field label with an optional required marker.
#[derive(IntoElement)]
pub struct Label {
    text: SharedString,
    required: bool,
    color_override: Option<Hsla>,
}

impl Label {
    pub fn new(text: impl Into<SharedString>) -> Self {
        Self {
            text: text.into(),
            required: false,
            color_override: None,
        }
    }

    /// Show a red asterisk after the label text.
    pub fn required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    /// Override the label color (replaces the shared field-label default).
    pub fn text_color(mut self, color: impl Into<Hsla>) -> Self {
        self.color_override = Some(color.into());
        self
    }

    /// Override the label color (replaces the shared field-label default).
    pub fn color(self, color: impl Into<Hsla>) -> Self {
        self.text_color(color)
    }

    fn build_text(text: SharedString, color_override: Option<Hsla>) -> Text {
        match color_override {
            Some(color) => Text::field_label(text).text_color(color),
            None => Text::field_label(text).muted_foreground(),
        }
    }

    #[cfg(test)]
    fn text(&self) -> Text {
        Self::build_text(self.text.clone(), self.color_override)
    }
}

impl RenderOnce for Label {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        let label = Self::build_text(self.text, self.color_override);

        let mut el = div().child(label);

        if self.required {
            el = el.child(RequiredMarker::new());
        }

        el
    }
}

#[cfg(test)]
mod tests {
    use super::Label;
    use crate::primitives::TextVariant;

    #[test]
    fn label_defaults_to_muted_field_label_typography() {
        let label = Label::new("Host").text();

        assert_eq!(
            label.role_contract(),
            TextVariant::FieldLabel.role_contract()
        );
        assert!(label.uses_muted_foreground_override());
        assert!(!label.uses_role_default_color());
    }
}
