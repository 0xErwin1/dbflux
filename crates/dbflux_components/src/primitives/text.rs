use gpui::prelude::*;
use gpui::{App, FontWeight, Hsla, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::tokens::FontSizes;

#[derive(Clone, Copy, Debug, PartialEq)]
enum TextColorOverride {
    Custom(Hsla),
    Danger,
    Warning,
    Success,
    Primary,
    Link,
    MutedForeground,
}

impl TextColorOverride {
    fn resolve(self, theme: &gpui_component::Theme) -> Hsla {
        match self {
            Self::Custom(color) => color,
            Self::Danger => theme.danger,
            Self::Warning => theme.warning,
            Self::Success => theme.success,
            Self::Primary => theme.primary,
            Self::Link => theme.link,
            Self::MutedForeground => theme.muted_foreground,
        }
    }
}

/// Visual variant controlling font size, weight, and default color.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextVariant {
    /// Section headings — XL, semibold, foreground.
    Heading,
    /// Body text — BASE, default weight, foreground.
    Body,
    /// Emphasized labels — BASE, medium, foreground.
    Label,
    /// Small emphasized labels — SM, medium, foreground.
    LabelSm,
    /// Page titles and brand names — TITLE, bold, foreground.
    Title,
    /// Small labels — SM, default weight, muted foreground.
    Caption,
    /// De-emphasized text — SM, default weight, muted foreground.
    Muted,
    /// Very subtle text — SM, default weight, muted foreground at 0.5 opacity.
    Dim,
    /// Slightly de-emphasized text — SM, default weight, muted foreground at 0.7 opacity.
    DimSecondary,
    /// Inline code — SM, monospace, foreground.
    Code,
}

/// Stateless text primitive. Picks font size, weight, and color from the
/// active theme based on the selected variant. Builder overrides let callers
/// replace any default.
#[derive(IntoElement)]
pub struct Text {
    variant: TextVariant,
    content: SharedString,
    color_override: Option<TextColorOverride>,
    size_override: Option<gpui::Pixels>,
    weight_override: Option<FontWeight>,
}

impl Text {
    pub fn heading(content: impl Into<SharedString>) -> Self {
        Self {
            variant: TextVariant::Heading,
            content: content.into(),
            color_override: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn body(content: impl Into<SharedString>) -> Self {
        Self {
            variant: TextVariant::Body,
            content: content.into(),
            color_override: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn label(content: impl Into<SharedString>) -> Self {
        Self {
            variant: TextVariant::Label,
            content: content.into(),
            color_override: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn label_sm(content: impl Into<SharedString>) -> Self {
        Self {
            variant: TextVariant::LabelSm,
            content: content.into(),
            color_override: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn title(content: impl Into<SharedString>) -> Self {
        Self {
            variant: TextVariant::Title,
            content: content.into(),
            color_override: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn caption(content: impl Into<SharedString>) -> Self {
        Self {
            variant: TextVariant::Caption,
            content: content.into(),
            color_override: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn muted(content: impl Into<SharedString>) -> Self {
        Self {
            variant: TextVariant::Muted,
            content: content.into(),
            color_override: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn dim(content: impl Into<SharedString>) -> Self {
        Self {
            variant: TextVariant::Dim,
            content: content.into(),
            color_override: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn dim_secondary(content: impl Into<SharedString>) -> Self {
        Self {
            variant: TextVariant::DimSecondary,
            content: content.into(),
            color_override: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn code(content: impl Into<SharedString>) -> Self {
        Self {
            variant: TextVariant::Code,
            content: content.into(),
            color_override: None,
            size_override: None,
            weight_override: None,
        }
    }

    /// Override the text color (replaces the variant default).
    pub fn text_color(mut self, color: impl Into<Hsla>) -> Self {
        self.color_override = Some(TextColorOverride::Custom(color.into()));
        self
    }

    /// Override the text color (replaces the variant default).
    pub fn color(self, color: impl Into<Hsla>) -> Self {
        self.text_color(color)
    }

    pub fn danger(mut self) -> Self {
        self.color_override = Some(TextColorOverride::Danger);
        self
    }

    pub fn warning(mut self) -> Self {
        self.color_override = Some(TextColorOverride::Warning);
        self
    }

    pub fn success(mut self) -> Self {
        self.color_override = Some(TextColorOverride::Success);
        self
    }

    pub fn primary(mut self) -> Self {
        self.color_override = Some(TextColorOverride::Primary);
        self
    }

    pub fn link(mut self) -> Self {
        self.color_override = Some(TextColorOverride::Link);
        self
    }

    pub fn muted_foreground(mut self) -> Self {
        self.color_override = Some(TextColorOverride::MutedForeground);
        self
    }

    /// Override the font size (replaces the variant default).
    pub fn font_size(mut self, size: gpui::Pixels) -> Self {
        self.size_override = Some(size);
        self
    }

    /// Override the font weight (replaces the variant default).
    pub fn font_weight(mut self, weight: FontWeight) -> Self {
        self.weight_override = Some(weight);
        self
    }
}

impl RenderOnce for Text {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();

        let (default_size, default_weight, default_color) = match self.variant {
            TextVariant::Heading => (FontSizes::XL, FontWeight::SEMIBOLD, theme.foreground),
            TextVariant::Body => (FontSizes::BASE, FontWeight::default(), theme.foreground),
            TextVariant::Label => (FontSizes::BASE, FontWeight::MEDIUM, theme.foreground),
            TextVariant::LabelSm => (FontSizes::SM, FontWeight::MEDIUM, theme.foreground),
            TextVariant::Title => (FontSizes::TITLE, FontWeight::BOLD, theme.foreground),
            TextVariant::Caption => (FontSizes::SM, FontWeight::default(), theme.muted_foreground),
            TextVariant::Muted => (FontSizes::SM, FontWeight::default(), theme.muted_foreground),
            TextVariant::Dim => (
                FontSizes::SM,
                FontWeight::default(),
                theme.muted_foreground.opacity(0.5),
            ),
            TextVariant::DimSecondary => (
                FontSizes::SM,
                FontWeight::default(),
                theme.muted_foreground.opacity(0.7),
            ),
            TextVariant::Code => (FontSizes::SM, FontWeight::default(), theme.foreground),
        };

        let size = self.size_override.unwrap_or(default_size);
        let weight = self.weight_override.unwrap_or(default_weight);
        let color = self
            .color_override
            .map(|override_color| override_color.resolve(theme))
            .unwrap_or(default_color);

        let el = div()
            .text_size(size)
            .font_weight(weight)
            .text_color(color)
            .child(self.content);

        if matches!(self.variant, TextVariant::Code) {
            el.font_family("monospace")
        } else {
            el
        }
    }
}
