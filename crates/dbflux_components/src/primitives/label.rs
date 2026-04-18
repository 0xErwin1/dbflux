use gpui::prelude::*;
use gpui::{App, FontWeight, Hsla, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::tokens::FontSizes;

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

    /// Override the label color (replaces the default `muted_foreground`).
    pub fn text_color(mut self, color: impl Into<Hsla>) -> Self {
        self.color_override = Some(color.into());
        self
    }

    /// Override the label color (replaces the default `muted_foreground`).
    pub fn color(self, color: impl Into<Hsla>) -> Self {
        self.text_color(color)
    }
}

impl RenderOnce for Label {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let color = self.color_override.unwrap_or(theme.muted_foreground);

        let mut el = div()
            .text_size(FontSizes::SM)
            .font_weight(FontWeight::MEDIUM)
            .text_color(color)
            .child(self.text);

        if self.required {
            el = el.child(div().text_color(theme.danger).child("*"));
        }

        el
    }
}
