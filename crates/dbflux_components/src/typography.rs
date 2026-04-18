use gpui::prelude::*;
use gpui::{div, AnyElement, App, FontWeight, Hsla, SharedString, Window};
use gpui_component::ActiveTheme;
use std::borrow::Cow;

use crate::primitives::Text;

pub struct BundledFontAsset {
    pub family: &'static str,
    pub file_name: &'static str,
    pub data: &'static [u8],
}

pub struct AppFonts;

impl AppFonts {
    pub const HEADLINE: &'static str = "Space Grotesk";
    pub const BODY: &'static str = "Inter";
    pub const MONO: &'static str = "JetBrains Mono";
    pub const MONO_FALLBACK: &'static str = "monospace";
    pub const CODE: &'static str = Self::MONO;
    pub const SHORTCUT: &'static str = Self::MONO;
}

pub const BUNDLED_FONT_ASSETS: [BundledFontAsset; 8] = [
    BundledFontAsset {
        family: AppFonts::BODY,
        file_name: "Inter-Variable.ttf",
        data: include_bytes!("../assets/fonts/Inter-Variable.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::BODY,
        file_name: "Inter-Variable-Italic.ttf",
        data: include_bytes!("../assets/fonts/Inter-Variable-Italic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::HEADLINE,
        file_name: "SpaceGrotesk-Regular.ttf",
        data: include_bytes!("../assets/fonts/SpaceGrotesk-Regular.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::HEADLINE,
        file_name: "SpaceGrotesk-Bold.ttf",
        data: include_bytes!("../assets/fonts/SpaceGrotesk-Bold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-Regular.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-Regular.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-Bold.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-Bold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-Italic.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-Italic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-BoldItalic.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-BoldItalic.ttf"),
    },
];

pub fn bundled_font_data() -> Vec<Cow<'static, [u8]>> {
    BUNDLED_FONT_ASSETS
        .iter()
        .map(|font| Cow::Borrowed(font.data))
        .collect()
}

/// Load all bundled fonts into GPUI's text system.
///
/// Called once during UI theme initialization. If registration fails, keep
/// the app running so GPUI can fall back to system fonts instead of aborting
/// startup.
pub fn load_bundled_fonts(cx: &mut App) {
    if let Err(error) = cx.text_system().add_fonts(bundled_font_data()) {
        eprintln!("failed to register bundled UI fonts, falling back to system fonts: {error}");
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HeadlineSize {
    #[default]
    Xl3,
    Xl2,
    Xl,
}

#[derive(IntoElement)]
pub struct Headline {
    text: SharedString,
    color: Option<Hsla>,
    size: HeadlineSize,
}

impl Headline {
    pub fn new(text: impl Into<SharedString>) -> Self {
        Self {
            text: text.into(),
            color: None,
            size: HeadlineSize::Xl3,
        }
    }

    pub fn xl2(mut self) -> Self {
        self.size = HeadlineSize::Xl2;
        self
    }

    pub fn xl(mut self) -> Self {
        self.size = HeadlineSize::Xl;
        self
    }

    pub fn color(mut self, color: Hsla) -> Self {
        self.color = Some(color);
        self
    }
}

impl RenderOnce for Headline {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let color = self.color.unwrap_or(cx.theme().foreground);

        match self.size {
            HeadlineSize::Xl3 => Text::headline_3(self.text).color(color),
            HeadlineSize::Xl2 => Text::headline_2(self.text).color(color),
            HeadlineSize::Xl => Text::headline_1(self.text).color(color),
        }
    }
}

#[derive(IntoElement)]
pub struct SubSectionLabel {
    text: SharedString,
}

impl SubSectionLabel {
    pub fn new(text: impl Into<SharedString>) -> Self {
        Self { text: text.into() }
    }
}

impl RenderOnce for SubSectionLabel {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        Text::subsection_label(SharedString::from(self.text.to_uppercase()))
    }
}

#[derive(IntoElement)]
pub struct SidebarGroupLabel {
    text: SharedString,
}

impl SidebarGroupLabel {
    pub fn new(text: impl Into<SharedString>) -> Self {
        Self { text: text.into() }
    }
}

impl RenderOnce for SidebarGroupLabel {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        div()
            .overflow_hidden()
            .whitespace_nowrap()
            .text_ellipsis()
            .child(Text::sidebar_group_label(SharedString::from(
                self.text.to_uppercase(),
            )))
    }
}

#[derive(IntoElement)]
pub struct Body {
    text: SharedString,
    color: Option<Hsla>,
}

impl Body {
    pub fn new(text: impl Into<SharedString>) -> Self {
        Self {
            text: text.into(),
            color: None,
        }
    }

    pub fn muted(mut self, cx: &App) -> Self {
        self.color = Some(cx.theme().muted_foreground);
        self
    }

    pub fn color(mut self, color: Hsla) -> Self {
        self.color = Some(color);
        self
    }
}

impl RenderOnce for Body {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let color = self.color.unwrap_or(cx.theme().foreground);
        Text::body_sm(self.text).color(color)
    }
}

#[derive(IntoElement)]
pub struct Caption {
    text: SharedString,
    color: Option<Hsla>,
}

impl Caption {
    pub fn new(text: impl Into<SharedString>) -> Self {
        Self {
            text: text.into(),
            color: None,
        }
    }

    pub fn color(mut self, color: Hsla) -> Self {
        self.color = Some(color);
        self
    }
}

impl RenderOnce for Caption {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let color = self.color.unwrap_or(cx.theme().muted_foreground);
        Text::caption_xs(self.text).color(color)
    }
}

#[derive(IntoElement)]
pub struct Code {
    text: SharedString,
    color: Option<Hsla>,
}

impl Code {
    pub fn new(text: impl Into<SharedString>) -> Self {
        Self {
            text: text.into(),
            color: None,
        }
    }

    pub fn color(mut self, color: Hsla) -> Self {
        self.color = Some(color);
        self
    }
}

impl RenderOnce for Code {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let color = self.color.unwrap_or(cx.theme().foreground);
        Text::code(self.text).color(color)
    }
}

#[derive(IntoElement)]
pub struct KeyHint {
    text: SharedString,
}

impl KeyHint {
    pub fn new(text: impl Into<SharedString>) -> Self {
        Self { text: text.into() }
    }
}

impl RenderOnce for KeyHint {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        Text::key_hint(self.text)
    }
}

#[derive(IntoElement)]
pub struct FieldLabel {
    text: SharedString,
    color: Option<Hsla>,
}

impl FieldLabel {
    pub fn new(text: impl Into<SharedString>) -> Self {
        Self {
            text: text.into(),
            color: None,
        }
    }

    pub fn color(mut self, color: Hsla) -> Self {
        self.color = Some(color);
        self
    }

    fn build_text(text: SharedString, color: Option<Hsla>) -> Text {
        match color {
            Some(color) => Text::field_label(text).color(color),
            None => Text::field_label(text),
        }
    }

    #[cfg(test)]
    fn text(&self) -> Text {
        Self::build_text(self.text.clone(), self.color)
    }
}

impl RenderOnce for FieldLabel {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        Self::build_text(self.text, self.color)
    }
}

#[derive(IntoElement, Default)]
pub struct RequiredMarker;

impl RequiredMarker {
    pub fn new() -> Self {
        Self
    }

    fn build_text() -> Text {
        Text::field_label("*").danger()
    }

    #[cfg(test)]
    fn text(&self) -> Text {
        Self::build_text()
    }
}

impl RenderOnce for RequiredMarker {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        Self::build_text()
    }
}

#[derive(IntoElement, Default)]
pub struct SectionDivider;

impl SectionDivider {
    pub fn new() -> Self {
        Self
    }
}

impl RenderOnce for SectionDivider {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        div().h_px().border_1().border_color(cx.theme().border)
    }
}

#[derive(Default, IntoElement)]
pub struct AppButton {
    children: Vec<AnyElement>,
}

impl AppButton {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.children.push(child.into_any_element());
        self
    }
}

impl RenderOnce for AppButton {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        div()
            .font_family(AppFonts::BODY)
            .font_weight(FontWeight::MEDIUM)
            .children(self.children)
    }
}

#[derive(Default, IntoElement)]
pub struct AppInput {
    children: Vec<AnyElement>,
}

impl AppInput {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.children.push(child.into_any_element());
        self
    }
}

impl RenderOnce for AppInput {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        div().font_family(AppFonts::BODY).children(self.children)
    }
}

#[derive(IntoElement)]
pub struct AppTab {
    active: bool,
    children: Vec<AnyElement>,
}

impl AppTab {
    pub fn new(active: bool) -> Self {
        Self {
            active,
            children: Vec::new(),
        }
    }

    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.children.push(child.into_any_element());
        self
    }
}

impl RenderOnce for AppTab {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        let weight = if self.active {
            FontWeight::MEDIUM
        } else {
            FontWeight::NORMAL
        };

        div()
            .font_family(AppFonts::BODY)
            .font_weight(weight)
            .children(self.children)
    }
}

#[derive(Default, IntoElement)]
pub struct AppSection {
    children: Vec<AnyElement>,
}

impl AppSection {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.children.push(child.into_any_element());
        self
    }
}

impl RenderOnce for AppSection {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        div().font_family(AppFonts::BODY).children(self.children)
    }
}

#[derive(Default, IntoElement)]
pub struct AppPanel {
    children: Vec<AnyElement>,
}

impl AppPanel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.children.push(child.into_any_element());
        self
    }
}

impl RenderOnce for AppPanel {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        div().font_family(AppFonts::BODY).children(self.children)
    }
}

#[cfg(test)]
mod tests {
    use super::{AppFonts, FieldLabel, RequiredMarker, BUNDLED_FONT_ASSETS};
    use crate::primitives::TextVariant;

    #[test]
    fn field_label_wrappers_share_the_central_text_contract() {
        let field_label = FieldLabel::new("Host").text();
        assert_eq!(
            field_label.role_contract(),
            TextVariant::FieldLabel.role_contract()
        );
        assert!(field_label.uses_role_default_color());

        let required_marker = RequiredMarker::new().text();
        assert_eq!(
            required_marker.role_contract(),
            TextVariant::FieldLabel.role_contract()
        );
        assert!(required_marker.uses_danger_override());
    }

    #[test]
    fn mono_font_contract_stays_on_jetbrains_mono() {
        assert_eq!(AppFonts::MONO, "JetBrains Mono");
        assert_eq!(AppFonts::MONO_FALLBACK, "monospace");
        assert_eq!(AppFonts::CODE, AppFonts::MONO);
        assert_eq!(AppFonts::SHORTCUT, AppFonts::MONO);
    }

    #[test]
    fn mono_bundled_assets_remain_jetbrains_mono_files() {
        let mono_assets: Vec<_> = BUNDLED_FONT_ASSETS
            .iter()
            .filter(|asset| asset.family == AppFonts::MONO)
            .map(|asset| asset.file_name)
            .collect();

        assert_eq!(
            mono_assets,
            vec![
                "JetBrainsMono-Regular.ttf",
                "JetBrainsMono-Bold.ttf",
                "JetBrainsMono-Italic.ttf",
                "JetBrainsMono-BoldItalic.ttf",
            ]
        );
    }
}
