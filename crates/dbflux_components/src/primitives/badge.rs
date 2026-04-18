use gpui::prelude::*;
use gpui::{App, Hsla, SharedString, Window, div, px};
use gpui_component::ActiveTheme;

use crate::tokens::{FontSizes, Radii, Spacing};

/// Semantic badge variant controlling the color scheme.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BadgeVariant {
    Info,
    Success,
    Warning,
    Danger,
    Neutral,
}

/// Stateless badge primitive. When `label` is empty renders as a small
/// colored dot; otherwise renders as a pill with text.
#[derive(IntoElement)]
pub struct Badge {
    variant: BadgeVariant,
    label: SharedString,
    dot_mode: bool,
}

impl Badge {
    pub fn new(label: impl Into<SharedString>, variant: BadgeVariant) -> Self {
        Self {
            variant,
            label: label.into(),
            dot_mode: false,
        }
    }

    /// Force dot-only mode regardless of label content.
    pub fn dot(mut self) -> Self {
        self.dot_mode = true;
        self
    }

    fn colors(&self, theme: &gpui_component::Theme) -> (Hsla, Hsla) {
        // (background, text)
        match self.variant {
            BadgeVariant::Info => {
                let bg = theme.info.opacity(0.15);
                (bg, theme.info)
            }
            BadgeVariant::Success => {
                let bg = theme.success.opacity(0.15);
                (bg, theme.success)
            }
            BadgeVariant::Warning => {
                let bg = theme.warning.opacity(0.15);
                (bg, theme.warning)
            }
            BadgeVariant::Danger => {
                let bg = theme.danger.opacity(0.15);
                (bg, theme.danger)
            }
            BadgeVariant::Neutral => (theme.secondary, theme.muted_foreground),
        }
    }
}

impl RenderOnce for Badge {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let (bg, text_color) = self.colors(theme);

        let is_dot = self.dot_mode || self.label.is_empty();

        if is_dot {
            div().size(px(8.0)).rounded_full().bg(text_color)
        } else {
            div()
                .px(Spacing::XS)
                .py(px(2.0))
                .rounded(Radii::SM)
                .bg(bg)
                .text_size(FontSizes::XS)
                .font_weight(gpui::FontWeight::MEDIUM)
                .text_color(text_color)
                .child(self.label)
        }
    }
}
