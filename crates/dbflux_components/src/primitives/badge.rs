use gpui::prelude::*;
use gpui::{App, FontWeight, Hsla, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::primitives::Chamfer;
use crate::tokens::{ChamferCut, ChromeColors, Feedback};

/// Semantic tone shared by [`Badge`] and [`EnvTag`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BadgeTone {
    Neutral,
    Accent,
    Info,
    Success,
    Warning,
    Danger,
}

impl BadgeTone {
    /// Label color for the tone. Accent resolves to the tint so it stays
    /// readable on the dark ground.
    pub fn text_color(self, theme: &gpui_component::Theme) -> Hsla {
        match self {
            Self::Neutral => theme.muted_foreground,
            Self::Accent => ChromeColors::tint(theme),
            Self::Info => theme.info,
            Self::Success => theme.success,
            Self::Warning => theme.warning,
            Self::Danger => theme.danger,
        }
    }

    /// Fill behind the label: the raised surface for Neutral, a wash of the
    /// label color at `alpha` for every other tone.
    fn fill(self, theme: &gpui_component::Theme, alpha: f32) -> Hsla {
        match self {
            Self::Neutral => theme.secondary,
            _ => self.text_color(theme).opacity(alpha),
        }
    }
}

/// Stateless badge: a short label on a 4 px chamfer, 20 px tall.
#[derive(IntoElement)]
pub struct Badge {
    tone: BadgeTone,
    label: SharedString,
}

impl Badge {
    pub fn new(label: impl Into<SharedString>, tone: BadgeTone) -> Self {
        Self {
            tone,
            label: label.into(),
        }
    }
}

impl RenderOnce for Badge {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let fill = self.tone.fill(theme, Feedback::BADGE_FILL_ALPHA);
        let text_color = self.tone.text_color(theme);

        div()
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(Feedback::BADGE_HEIGHT)
            .px(Feedback::BADGE_PADDING_X)
            .child(Chamfer::new(ChamferCut::KEYCAP).fill(fill))
            .child(
                div()
                    .whitespace_nowrap()
                    .text_size(Feedback::BADGE_FONT)
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(text_color)
                    .child(self.label),
            )
    }
}

/// Environment tag (PROD, STAGING, ...) shown next to a connection name.
///
/// Bold 10 px caps on a 4 px chamfer; Danger by default, the tone used for
/// production.
#[derive(IntoElement)]
pub struct EnvTag {
    tone: BadgeTone,
    label: SharedString,
}

impl EnvTag {
    pub fn new(label: impl Into<SharedString>) -> Self {
        Self {
            tone: BadgeTone::Danger,
            label: label.into(),
        }
    }

    pub fn tone(mut self, tone: BadgeTone) -> Self {
        self.tone = tone;
        self
    }
}

impl RenderOnce for EnvTag {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let fill = self.tone.fill(theme, Feedback::ENV_TAG_FILL_ALPHA);
        let text_color = self.tone.text_color(theme);
        let label: SharedString = self.label.to_uppercase().into();

        div()
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .px(Feedback::ENV_TAG_PADDING_X)
            .py(Feedback::ENV_TAG_PADDING_Y)
            .child(Chamfer::new(ChamferCut::KEYCAP).fill(fill))
            .child(
                div()
                    .whitespace_nowrap()
                    .text_size(Feedback::ENV_TAG_FONT)
                    .font_weight(FontWeight::BOLD)
                    .letter_spacing(Feedback::ENV_TAG_FONT * Feedback::ENV_TAG_TRACKING_EM)
                    .text_color(text_color)
                    .child(label),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neutral_badge_uses_the_raised_surface() {
        let theme = gpui_component::Theme::default();

        assert_eq!(BadgeTone::Neutral.fill(&theme, 0.14), theme.secondary);
        assert_eq!(
            BadgeTone::Neutral.text_color(&theme),
            theme.muted_foreground
        );
    }

    #[test]
    fn colored_tones_wash_their_label_color() {
        let theme = gpui_component::Theme::default();

        for tone in [
            BadgeTone::Accent,
            BadgeTone::Info,
            BadgeTone::Success,
            BadgeTone::Warning,
            BadgeTone::Danger,
        ] {
            let text = tone.text_color(&theme);
            assert_eq!(tone.fill(&theme, 0.14), text.opacity(0.14));
        }
    }

    #[test]
    fn env_tag_defaults_to_danger() {
        assert_eq!(EnvTag::new("prod").tone, BadgeTone::Danger);
        assert_eq!(
            EnvTag::new("staging").tone(BadgeTone::Warning).tone,
            BadgeTone::Warning
        );
    }
}
