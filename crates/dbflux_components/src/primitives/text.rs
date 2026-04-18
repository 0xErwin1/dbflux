use gpui::prelude::*;
use gpui::{div, font, App, FontFallbacks, FontWeight, Hsla, SharedString, Window};
use gpui_component::ActiveTheme;

use crate::tokens::FontSizes;
use crate::typography::AppFonts;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TextDefaultColor {
    Foreground,
    MutedForeground,
    MutedForegroundDim,
    MutedForegroundSecondary,
}

impl TextDefaultColor {
    fn resolve(self, theme: &gpui_component::Theme) -> Hsla {
        match self {
            Self::Foreground => theme.foreground,
            Self::MutedForeground => theme.muted_foreground,
            Self::MutedForegroundDim => theme.muted_foreground.opacity(0.5),
            Self::MutedForegroundSecondary => theme.muted_foreground.opacity(0.7),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct TextRoleContract {
    pub(crate) family: Option<&'static str>,
    pub(crate) fallbacks: &'static [&'static str],
    pub(crate) size: gpui::Pixels,
    pub(crate) weight: FontWeight,
    pub(crate) color: TextDefaultColor,
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
    /// Shared headline role — TITLE, bold, headline font, foreground.
    Headline3,
    /// Shared headline role — XL, bold, headline font, foreground.
    Headline2,
    /// Shared headline role — LG, bold, headline font, foreground.
    Headline1,
    /// Shared section label role — XS, bold, body font, muted foreground.
    SubSectionLabel,
    /// Shared sidebar label role — XS, bold, mono font, muted foreground.
    SidebarGroupLabel,
    /// Shared small body role — SM, default weight, body font, foreground.
    BodySm,
    /// Shared caption role — XS, default weight, body font, muted foreground.
    CaptionXs,
    /// Shared key hint role — XS, bold, mono font, muted foreground.
    KeyHint,
    /// Shared field label role — SM, medium, body font, foreground.
    FieldLabel,
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
    fn from_variant(variant: TextVariant, content: impl Into<SharedString>) -> Self {
        Self {
            variant,
            content: content.into(),
            color_override: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn heading(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::Heading, content)
    }

    pub fn body(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::Body, content)
    }

    pub fn label(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::Label, content)
    }

    pub fn label_sm(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::LabelSm, content)
    }

    pub fn title(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::Title, content)
    }

    pub fn caption(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::Caption, content)
    }

    pub fn muted(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::Muted, content)
    }

    pub fn dim(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::Dim, content)
    }

    pub fn dim_secondary(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::DimSecondary, content)
    }

    pub fn code(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::Code, content)
    }

    pub fn headline_3(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::Headline3, content)
    }

    pub fn headline_2(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::Headline2, content)
    }

    pub fn headline_1(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::Headline1, content)
    }

    pub fn subsection_label(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::SubSectionLabel, content)
    }

    pub fn sidebar_group_label(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::SidebarGroupLabel, content)
    }

    pub fn body_sm(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::BodySm, content)
    }

    pub fn caption_xs(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::CaptionXs, content)
    }

    pub fn key_hint(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::KeyHint, content)
    }

    pub fn field_label(content: impl Into<SharedString>) -> Self {
        Self::from_variant(TextVariant::FieldLabel, content)
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

    #[cfg(test)]
    pub(crate) fn role_contract(&self) -> TextRoleContract {
        self.variant.role_contract()
    }

    #[cfg(test)]
    pub(crate) fn uses_role_default_color(&self) -> bool {
        self.color_override.is_none()
    }

    #[cfg(test)]
    pub(crate) fn uses_muted_foreground_override(&self) -> bool {
        matches!(
            self.color_override,
            Some(TextColorOverride::MutedForeground)
        )
    }

    #[cfg(test)]
    pub(crate) fn uses_danger_override(&self) -> bool {
        matches!(self.color_override, Some(TextColorOverride::Danger))
    }
}

impl TextVariant {
    pub(crate) fn role_contract(self) -> TextRoleContract {
        match self {
            Self::Heading => TextRoleContract {
                family: None,
                fallbacks: &[],
                size: FontSizes::XL,
                weight: FontWeight::SEMIBOLD,
                color: TextDefaultColor::Foreground,
            },
            Self::Body => TextRoleContract {
                family: None,
                fallbacks: &[],
                size: FontSizes::BASE,
                weight: FontWeight::default(),
                color: TextDefaultColor::Foreground,
            },
            Self::Label => TextRoleContract {
                family: None,
                fallbacks: &[],
                size: FontSizes::BASE,
                weight: FontWeight::MEDIUM,
                color: TextDefaultColor::Foreground,
            },
            Self::LabelSm => TextRoleContract {
                family: None,
                fallbacks: &[],
                size: FontSizes::SM,
                weight: FontWeight::MEDIUM,
                color: TextDefaultColor::Foreground,
            },
            Self::Title => TextRoleContract {
                family: None,
                fallbacks: &[],
                size: FontSizes::TITLE,
                weight: FontWeight::BOLD,
                color: TextDefaultColor::Foreground,
            },
            Self::Caption => TextRoleContract {
                family: None,
                fallbacks: &[],
                size: FontSizes::SM,
                weight: FontWeight::default(),
                color: TextDefaultColor::MutedForeground,
            },
            Self::Muted => TextRoleContract {
                family: None,
                fallbacks: &[],
                size: FontSizes::SM,
                weight: FontWeight::default(),
                color: TextDefaultColor::MutedForeground,
            },
            Self::Dim => TextRoleContract {
                family: None,
                fallbacks: &[],
                size: FontSizes::SM,
                weight: FontWeight::default(),
                color: TextDefaultColor::MutedForegroundDim,
            },
            Self::DimSecondary => TextRoleContract {
                family: None,
                fallbacks: &[],
                size: FontSizes::SM,
                weight: FontWeight::default(),
                color: TextDefaultColor::MutedForegroundSecondary,
            },
            Self::Code => TextRoleContract {
                family: Some(AppFonts::MONO),
                fallbacks: &[AppFonts::MONO_FALLBACK],
                size: FontSizes::SM,
                weight: FontWeight::default(),
                color: TextDefaultColor::Foreground,
            },
            Self::Headline3 => TextRoleContract {
                family: Some(AppFonts::HEADLINE),
                fallbacks: &[],
                size: FontSizes::TITLE,
                weight: FontWeight::BOLD,
                color: TextDefaultColor::Foreground,
            },
            Self::Headline2 => TextRoleContract {
                family: Some(AppFonts::HEADLINE),
                fallbacks: &[],
                size: FontSizes::XL,
                weight: FontWeight::BOLD,
                color: TextDefaultColor::Foreground,
            },
            Self::Headline1 => TextRoleContract {
                family: Some(AppFonts::HEADLINE),
                fallbacks: &[],
                size: FontSizes::LG,
                weight: FontWeight::BOLD,
                color: TextDefaultColor::Foreground,
            },
            Self::SubSectionLabel => TextRoleContract {
                family: Some(AppFonts::BODY),
                fallbacks: &[],
                size: FontSizes::XS,
                weight: FontWeight::BOLD,
                color: TextDefaultColor::MutedForeground,
            },
            Self::SidebarGroupLabel => TextRoleContract {
                family: Some(AppFonts::SHORTCUT),
                fallbacks: &[AppFonts::MONO_FALLBACK],
                size: FontSizes::XS,
                weight: FontWeight::BOLD,
                color: TextDefaultColor::MutedForeground,
            },
            Self::BodySm => TextRoleContract {
                family: Some(AppFonts::BODY),
                fallbacks: &[],
                size: FontSizes::SM,
                weight: FontWeight::default(),
                color: TextDefaultColor::Foreground,
            },
            Self::CaptionXs => TextRoleContract {
                family: Some(AppFonts::BODY),
                fallbacks: &[],
                size: FontSizes::XS,
                weight: FontWeight::default(),
                color: TextDefaultColor::MutedForeground,
            },
            Self::KeyHint => TextRoleContract {
                family: Some(AppFonts::SHORTCUT),
                fallbacks: &[AppFonts::MONO_FALLBACK],
                size: FontSizes::XS,
                weight: FontWeight::BOLD,
                color: TextDefaultColor::MutedForeground,
            },
            Self::FieldLabel => TextRoleContract {
                family: Some(AppFonts::BODY),
                fallbacks: &[],
                size: FontSizes::SM,
                weight: FontWeight::MEDIUM,
                color: TextDefaultColor::Foreground,
            },
        }
    }
}

impl RenderOnce for Text {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let contract = self.variant.role_contract();

        let size = self.size_override.unwrap_or(contract.size);
        let weight = self.weight_override.unwrap_or(contract.weight);
        let color = self
            .color_override
            .map(|override_color| override_color.resolve(theme))
            .unwrap_or_else(|| contract.color.resolve(theme));

        let el = div()
            .text_size(size)
            .font_weight(weight)
            .text_color(color)
            .child(self.content);

        if let Some(family) = contract.family {
            if contract.fallbacks.is_empty() {
                el.font_family(family)
            } else {
                let mut text_font = font(family);
                text_font.fallbacks = Some(FontFallbacks::from_fonts(
                    contract
                        .fallbacks
                        .iter()
                        .map(|fallback| (*fallback).to_owned())
                        .collect(),
                ));

                el.font(text_font)
            }
        } else {
            el
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TextVariant;
    use crate::tokens::FontSizes;
    use crate::typography::AppFonts;
    use gpui::FontWeight;

    const NO_FALLBACKS: &[&str] = &[];

    #[test]
    fn shared_typography_roles_expose_expected_font_contracts() {
        let headline = TextVariant::Headline3.role_contract();
        assert_eq!(headline.family, Some(AppFonts::HEADLINE));
        assert_eq!(headline.fallbacks, NO_FALLBACKS);
        assert_eq!(headline.size, FontSizes::TITLE);
        assert_eq!(headline.weight, FontWeight::BOLD);

        let subsection = TextVariant::SubSectionLabel.role_contract();
        assert_eq!(subsection.family, Some(AppFonts::BODY));
        assert_eq!(subsection.fallbacks, NO_FALLBACKS);
        assert_eq!(subsection.size, FontSizes::XS);
        assert_eq!(subsection.weight, FontWeight::BOLD);

        let sidebar = TextVariant::SidebarGroupLabel.role_contract();
        assert_eq!(sidebar.family, Some(AppFonts::SHORTCUT));
        assert_eq!(sidebar.fallbacks, &[AppFonts::MONO_FALLBACK]);
        assert_eq!(sidebar.size, FontSizes::XS);
        assert_eq!(sidebar.weight, FontWeight::BOLD);
    }

    #[test]
    fn shared_body_and_mono_roles_keep_expected_defaults() {
        let body = TextVariant::BodySm.role_contract();
        assert_eq!(body.family, Some(AppFonts::BODY));
        assert_eq!(body.fallbacks, NO_FALLBACKS);
        assert_eq!(body.size, FontSizes::SM);
        assert_eq!(body.weight, FontWeight::NORMAL);

        let key_hint = TextVariant::KeyHint.role_contract();
        assert_eq!(key_hint.family, Some(AppFonts::SHORTCUT));
        assert_eq!(key_hint.fallbacks, &[AppFonts::MONO_FALLBACK]);
        assert_eq!(key_hint.size, FontSizes::XS);
        assert_eq!(key_hint.weight, FontWeight::BOLD);

        let code = TextVariant::Code.role_contract();
        assert_eq!(code.family, Some(AppFonts::MONO));
        assert_eq!(code.fallbacks, &[AppFonts::MONO_FALLBACK]);
        assert_eq!(code.size, FontSizes::SM);
        assert_eq!(code.weight, FontWeight::NORMAL);

        let field_label = TextVariant::FieldLabel.role_contract();
        assert_eq!(field_label.family, Some(AppFonts::BODY));
        assert_eq!(field_label.fallbacks, NO_FALLBACKS);
        assert_eq!(field_label.size, FontSizes::SM);
        assert_eq!(field_label.weight, FontWeight::MEDIUM);
    }
}
