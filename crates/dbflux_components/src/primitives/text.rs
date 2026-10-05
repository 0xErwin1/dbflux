use gpui::prelude::*;
use gpui::{
    AbsoluteLength, App, Font, FontFallbacks, FontWeight, Hsla, SharedString, Window, div, font,
};
use gpui_component::ActiveTheme;

use crate::density;
use crate::tokens::{ChromeColors, FontSizes};
use crate::typography::AppFonts;

/// Tracking of the uppercase `Label` role, in em.
const LABEL_TRACKING_EM: f32 = 0.14;

/// The eight text roles of the design system (DSFoundations, DSAppPlan).
///
/// Every piece of interface copy picks one of these; size, weight, family
/// and default color come from the role, and builder overrides adjust a
/// single call site.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextVariant {
    /// Window and page titles: Archivo 700, `FontSizes::TITLE`, strong.
    Title,
    /// Section and dialog headings: Archivo 700, `FontSizes::XL`, strong.
    Heading,
    /// Body copy and interface text: Archivo 500, `FontSizes::BASE`, body color.
    Body,
    /// Secondary interface text: Archivo 500, `FontSizes::XS`, body color.
    BodySm,
    /// Section labels: Archivo Expanded 800, `FontSizes::LABEL`, uppercase,
    /// 0.14em tracking, muted.
    Label,
    /// Metadata and helper text: Archivo 500, `FontSizes::XS`, muted.
    Caption,
    /// Data, identifiers and queries: JetBrains Mono 500, `FontSizes::SM`, body color.
    Code,
    /// Keyboard shortcuts: JetBrains Mono 500, `FontSizes::XS`, muted.
    KeyHint,
}

/// Default color of a role before any override.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextDefaultColor {
    Strong,
    Foreground,
    MutedForeground,
}

impl TextDefaultColor {
    fn resolve(self, theme: &gpui_component::Theme) -> Hsla {
        match self {
            Self::Strong => ChromeColors::strong(theme),
            Self::Foreground => theme.foreground,
            Self::MutedForeground => theme.muted_foreground,
        }
    }
}

/// The color a `Text` resolves to, as a theme slot rather than a value, so
/// tests can check it without a theme.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TextColorSelection {
    RoleDefault(TextDefaultColor),
    Custom(Hsla),
    Danger,
    Warning,
    Success,
    Primary,
    Link,
    MutedForeground,
}

impl TextColorSelection {
    fn resolve(self, theme: &gpui_component::Theme) -> Hsla {
        match self {
            Self::RoleDefault(color) => color.resolve(theme),
            Self::Custom(color) => color,
            Self::Danger => theme.danger,
            Self::Warning => theme.warning,
            Self::Success => theme.success,
            Self::Primary => ChromeColors::tint(theme),
            Self::Link => theme.link,
            Self::MutedForeground => theme.muted_foreground,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TextRoleContract {
    pub family: &'static str,
    pub fallbacks: &'static [&'static str],
    pub size: AbsoluteLength,
    pub weight: FontWeight,
    pub color: TextDefaultColor,
}

/// Render-free description of a `Text`, for tests of the components that
/// build one.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TextInspection {
    pub variant: TextVariant,
    pub family: &'static str,
    pub fallbacks: &'static [&'static str],
    pub size_override: Option<AbsoluteLength>,
    pub weight_override: Option<FontWeight>,
    pub color_selection: TextColorSelection,
    pub uses_role_default_color: bool,
    pub uses_muted_foreground_override: bool,
    pub has_custom_color_override: bool,
}

/// Stateless text primitive. Picks font family, size, weight and color from
/// the active theme based on its role. Builder overrides let callers replace
/// any default.
#[derive(IntoElement)]
pub struct Text {
    variant: TextVariant,
    content: SharedString,
    color_override: Option<TextColorSelection>,
    size_override: Option<AbsoluteLength>,
    weight_override: Option<FontWeight>,
}

impl Text {
    pub fn new(variant: TextVariant, content: impl Into<SharedString>) -> Self {
        let content = content.into();

        let content = if variant.is_uppercase() {
            SharedString::from(content.to_uppercase())
        } else {
            content
        };

        Self {
            variant,
            content,
            color_override: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn title(content: impl Into<SharedString>) -> Self {
        Self::new(TextVariant::Title, content)
    }

    pub fn heading(content: impl Into<SharedString>) -> Self {
        Self::new(TextVariant::Heading, content)
    }

    pub fn body(content: impl Into<SharedString>) -> Self {
        Self::new(TextVariant::Body, content)
    }

    pub fn body_sm(content: impl Into<SharedString>) -> Self {
        Self::new(TextVariant::BodySm, content)
    }

    /// Uppercase section label; pass ordinary text, the role capitalizes it.
    pub fn label(content: impl Into<SharedString>) -> Self {
        Self::new(TextVariant::Label, content)
    }

    pub fn caption(content: impl Into<SharedString>) -> Self {
        Self::new(TextVariant::Caption, content)
    }

    pub fn code(content: impl Into<SharedString>) -> Self {
        Self::new(TextVariant::Code, content)
    }

    pub fn key_hint(content: impl Into<SharedString>) -> Self {
        Self::new(TextVariant::KeyHint, content)
    }

    /// Override the text color (replaces the role default).
    pub fn text_color(mut self, color: impl Into<Hsla>) -> Self {
        self.color_override = Some(TextColorSelection::Custom(color.into()));
        self
    }

    /// Override the text color (replaces the role default).
    pub fn color(self, color: impl Into<Hsla>) -> Self {
        self.text_color(color)
    }

    pub fn danger(mut self) -> Self {
        self.color_override = Some(TextColorSelection::Danger);
        self
    }

    pub fn warning(mut self) -> Self {
        self.color_override = Some(TextColorSelection::Warning);
        self
    }

    pub fn success(mut self) -> Self {
        self.color_override = Some(TextColorSelection::Success);
        self
    }

    /// Accent text: the palette tint (`ChromeColors::tint`), not the byzantine fill.
    pub fn primary(mut self) -> Self {
        self.color_override = Some(TextColorSelection::Primary);
        self
    }

    pub fn link(mut self) -> Self {
        self.color_override = Some(TextColorSelection::Link);
        self
    }

    pub fn muted_foreground(mut self) -> Self {
        self.color_override = Some(TextColorSelection::MutedForeground);
        self
    }

    /// Override the font size (replaces the role default). Accepts pixels or
    /// rems; a `tokens::ui` rem size follows the interface scale.
    pub fn font_size(mut self, size: impl Into<AbsoluteLength>) -> Self {
        self.size_override = Some(size.into());
        self
    }

    /// Override the font weight (replaces the role default).
    pub fn font_weight(mut self, weight: FontWeight) -> Self {
        self.weight_override = Some(weight);
        self
    }

    pub fn variant(&self) -> TextVariant {
        self.variant
    }

    /// Describe the text without rendering it.
    pub fn inspect(&self) -> TextInspection {
        let contract = self.variant.role_contract();

        TextInspection {
            variant: self.variant,
            family: contract.family,
            fallbacks: contract.fallbacks,
            size_override: self.size_override,
            weight_override: self.weight_override,
            color_selection: self
                .color_override
                .unwrap_or(TextColorSelection::RoleDefault(contract.color)),
            uses_role_default_color: self.color_override.is_none(),
            uses_muted_foreground_override: matches!(
                self.color_override,
                Some(TextColorSelection::MutedForeground)
            ),
            has_custom_color_override: matches!(
                self.color_override,
                Some(TextColorSelection::Custom(_))
            ),
        }
    }
}

impl TextVariant {
    /// Resolve the font size for this role using the active density tier.
    ///
    /// Unlike `role_contract().size` (always the Default-tier constant), this
    /// reads the density global and returns the Compact-tier value when active.
    pub fn density_size(self, cx: &App) -> gpui::Rems {
        match self {
            Self::Title => density::font_title(cx),
            Self::Heading => density::font_xl(cx),
            Self::Body => density::font_base(cx),
            Self::Code => density::font_sm(cx),
            Self::BodySm | Self::Caption | Self::KeyHint => density::font_xs(cx),
            Self::Label => density::font_label(cx),
        }
    }

    /// The `Label` role renders its text in capitals, so call sites pass
    /// ordinary text.
    pub fn is_uppercase(self) -> bool {
        matches!(self, Self::Label)
    }

    /// Extra space after every character, as a fraction of the font size.
    pub fn letter_spacing_em(self) -> f32 {
        match self {
            Self::Label => LABEL_TRACKING_EM,
            _ => 0.0,
        }
    }

    /// The font this role renders with: the role's bundled family resolved
    /// to the family chosen in Settings, with the role's fallbacks.
    pub fn resolved_font(self, cx: &App) -> Font {
        let contract = self.role_contract();
        let mut text_font = font(crate::fonts::family_for(cx, contract.family));

        if !contract.fallbacks.is_empty() {
            text_font.fallbacks = Some(FontFallbacks::from_fonts(
                contract
                    .fallbacks
                    .iter()
                    .map(|fallback| (*fallback).to_owned())
                    .collect(),
            ));
        }

        text_font
    }

    pub fn role_contract(self) -> TextRoleContract {
        const INTERFACE: &[&str] = &[];
        const MONO: &[&str] = &[AppFonts::MONO_FALLBACK];

        match self {
            Self::Title => TextRoleContract {
                family: AppFonts::INTERFACE,
                fallbacks: INTERFACE,
                size: FontSizes::TITLE.into(),
                weight: FontWeight::BOLD,
                color: TextDefaultColor::Strong,
            },
            Self::Heading => TextRoleContract {
                family: AppFonts::INTERFACE,
                fallbacks: INTERFACE,
                size: FontSizes::XL.into(),
                weight: FontWeight::BOLD,
                color: TextDefaultColor::Strong,
            },
            Self::Body => TextRoleContract {
                family: AppFonts::INTERFACE,
                fallbacks: INTERFACE,
                size: FontSizes::BASE.into(),
                weight: FontWeight::MEDIUM,
                color: TextDefaultColor::Foreground,
            },
            Self::BodySm => TextRoleContract {
                family: AppFonts::INTERFACE,
                fallbacks: INTERFACE,
                size: FontSizes::XS.into(),
                weight: FontWeight::MEDIUM,
                color: TextDefaultColor::Foreground,
            },
            Self::Label => TextRoleContract {
                family: AppFonts::DISPLAY,
                fallbacks: INTERFACE,
                size: FontSizes::LABEL.into(),
                weight: FontWeight::EXTRA_BOLD,
                color: TextDefaultColor::MutedForeground,
            },
            Self::Caption => TextRoleContract {
                family: AppFonts::INTERFACE,
                fallbacks: INTERFACE,
                size: FontSizes::XS.into(),
                weight: FontWeight::MEDIUM,
                color: TextDefaultColor::MutedForeground,
            },
            Self::Code => TextRoleContract {
                family: AppFonts::MONO,
                fallbacks: MONO,
                size: FontSizes::SM.into(),
                weight: FontWeight::MEDIUM,
                color: TextDefaultColor::Foreground,
            },
            Self::KeyHint => TextRoleContract {
                family: AppFonts::MONO,
                fallbacks: MONO,
                size: FontSizes::XS.into(),
                weight: FontWeight::MEDIUM,
                color: TextDefaultColor::MutedForeground,
            },
        }
    }
}

impl RenderOnce for Text {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let contract = self.variant.role_contract();

        let size = self
            .size_override
            .unwrap_or_else(|| self.variant.density_size(cx).into());
        let weight = self.weight_override.unwrap_or(contract.weight);
        let color = self
            .color_override
            .unwrap_or(TextColorSelection::RoleDefault(contract.color))
            .resolve(theme);

        let text_font = self.variant.resolved_font(cx);
        let letter_spacing_em = self.variant.letter_spacing_em();

        div()
            .font(text_font)
            .text_size(size)
            .font_weight(weight)
            .text_color(color)
            .when(letter_spacing_em > 0.0, |el| {
                el.letter_spacing(size.to_pixels(window.rem_size()) * letter_spacing_em)
            })
            .child(self.content)
    }
}

#[cfg(test)]
mod tests {
    use super::{Text, TextColorSelection, TextDefaultColor, TextVariant};
    use crate::tokens::FontSizes;
    use crate::typography::AppFonts;
    use gpui::FontWeight;

    const ALL_ROLES: [TextVariant; 8] = [
        TextVariant::Title,
        TextVariant::Heading,
        TextVariant::Body,
        TextVariant::BodySm,
        TextVariant::Label,
        TextVariant::Caption,
        TextVariant::Code,
        TextVariant::KeyHint,
    ];

    #[test]
    fn headings_use_the_interface_face_at_bold_weight() {
        for role in [TextVariant::Title, TextVariant::Heading] {
            let contract = role.role_contract();
            assert_eq!(contract.family, AppFonts::INTERFACE, "{role:?}");
            assert_eq!(contract.weight, FontWeight::BOLD, "{role:?}");
            assert_eq!(contract.color, TextDefaultColor::Strong, "{role:?}");
        }

        assert_eq!(
            TextVariant::Title.role_contract().size,
            FontSizes::TITLE.into()
        );
        assert_eq!(
            TextVariant::Heading.role_contract().size,
            FontSizes::XL.into()
        );
    }

    #[test]
    fn display_face_is_reserved_for_the_label_role() {
        for role in ALL_ROLES {
            let is_display = role.role_contract().family == AppFonts::DISPLAY;
            assert_eq!(is_display, role == TextVariant::Label, "{role:?}");
        }

        let label = TextVariant::Label.role_contract();
        assert_eq!(label.size, FontSizes::LABEL.into());
        assert_eq!(label.weight, FontWeight::EXTRA_BOLD);
    }

    #[test]
    fn mono_face_is_reserved_for_code_and_key_hints() {
        for role in ALL_ROLES {
            let contract = role.role_contract();
            let is_mono = contract.family == AppFonts::MONO;
            assert_eq!(
                is_mono,
                matches!(role, TextVariant::Code | TextVariant::KeyHint),
                "{role:?}"
            );

            if is_mono {
                assert_eq!(contract.fallbacks, &[AppFonts::MONO_FALLBACK]);
            } else {
                assert!(contract.fallbacks.is_empty(), "{role:?}");
            }
        }
    }

    #[test]
    fn label_role_uppercases_and_tracks_its_text() {
        assert!(TextVariant::Label.is_uppercase());
        assert_eq!(TextVariant::Label.letter_spacing_em(), 0.14);

        for role in ALL_ROLES
            .into_iter()
            .filter(|role| *role != TextVariant::Label)
        {
            assert!(!role.is_uppercase(), "{role:?}");
            assert_eq!(role.letter_spacing_em(), 0.0, "{role:?}");
        }

        assert_eq!(
            Text::label("Connection details").content.as_ref(),
            "CONNECTION DETAILS"
        );
        assert_eq!(
            Text::body("Connection details").content.as_ref(),
            "Connection details"
        );
    }

    #[gpui::test]
    fn roles_render_with_the_families_chosen_in_settings(cx: &mut gpui::TestAppContext) {
        cx.update(|cx| {
            assert_eq!(
                TextVariant::Body.resolved_font(cx).family.as_ref(),
                AppFonts::INTERFACE
            );
            assert_eq!(
                TextVariant::Label.resolved_font(cx).family.as_ref(),
                AppFonts::DISPLAY
            );

            crate::fonts::init(
                cx,
                crate::fonts::FontSettings {
                    ui_family: "Inter".into(),
                    editor_family: "Fira Code".into(),
                    ..crate::fonts::FontSettings::default()
                },
            );

            assert_eq!(TextVariant::Body.resolved_font(cx).family.as_ref(), "Inter");
            assert_eq!(
                TextVariant::Label.resolved_font(cx).family.as_ref(),
                "Inter"
            );

            let code = TextVariant::Code.resolved_font(cx);
            assert_eq!(code.family.as_ref(), "Fira Code");
            assert!(code.fallbacks.is_some(), "mono roles keep their fallback");
        });
    }

    #[test]
    fn inspection_reports_color_overrides() {
        let plain = Text::caption("Meta").inspect();
        assert!(plain.uses_role_default_color);
        assert_eq!(
            plain.color_selection,
            TextColorSelection::RoleDefault(TextDefaultColor::MutedForeground)
        );

        let muted = Text::body("Meta").muted_foreground().inspect();
        assert!(muted.uses_muted_foreground_override);

        let custom = Text::code("id").color(gpui::red()).inspect();
        assert!(custom.has_custom_color_override);
        assert_eq!(custom.family, AppFonts::MONO);
    }
}
