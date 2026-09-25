use gpui::prelude::*;
use gpui::{AnyElement, App, FontWeight, Hsla, Pixels, SharedString, Window, div};
use gpui_component::ActiveTheme;
use std::borrow::Cow;

use crate::density;
use crate::primitives::{Text, TextColorSelection, TextDefaultColor};
use crate::tokens::FontSizes;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MonoTextInspection {
    pub family: Option<&'static str>,
    pub fallbacks: &'static [&'static str],
    pub size_override: Option<gpui::Pixels>,
    pub weight_override: Option<FontWeight>,
    pub color_selection: MonoColorSelection,
    pub uses_role_default_color: bool,
    pub uses_muted_foreground_override: bool,
    pub has_custom_color_override: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MonoDefaultColor {
    Foreground,
    MutedForeground,
    MutedForegroundDim,
    MutedForegroundSecondary,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MonoColorSelection {
    RoleDefault(MonoDefaultColor),
    Custom(Hsla),
    Danger,
    Warning,
    Success,
    Primary,
    Link,
    MutedForeground,
}

fn inspect_default_color(color: TextDefaultColor) -> MonoDefaultColor {
    match color {
        TextDefaultColor::Foreground => MonoDefaultColor::Foreground,
        TextDefaultColor::MutedForeground => MonoDefaultColor::MutedForeground,
        TextDefaultColor::MutedForegroundDim => MonoDefaultColor::MutedForegroundDim,
        TextDefaultColor::MutedForegroundSecondary => MonoDefaultColor::MutedForegroundSecondary,
    }
}

fn inspect_color_selection(selection: TextColorSelection) -> MonoColorSelection {
    match selection {
        TextColorSelection::RoleDefault(color) => {
            MonoColorSelection::RoleDefault(inspect_default_color(color))
        }
        TextColorSelection::Custom(color) => MonoColorSelection::Custom(color),
        TextColorSelection::Danger => MonoColorSelection::Danger,
        TextColorSelection::Warning => MonoColorSelection::Warning,
        TextColorSelection::Success => MonoColorSelection::Success,
        TextColorSelection::Primary => MonoColorSelection::Primary,
        TextColorSelection::Link => MonoColorSelection::Link,
        TextColorSelection::MutedForeground => MonoColorSelection::MutedForeground,
    }
}

fn inspect_mono_text(text: &Text) -> MonoTextInspection {
    let contract = text.role_contract();

    MonoTextInspection {
        family: contract.family,
        fallbacks: contract.fallbacks,
        size_override: text.font_size_override(),
        weight_override: text.font_weight_override(),
        color_selection: inspect_color_selection(text.color_selection()),
        uses_role_default_color: text.uses_role_default_color(),
        uses_muted_foreground_override: text.uses_muted_foreground_override(),
        has_custom_color_override: text.has_custom_color_override(),
    }
}

pub struct BundledFontAsset {
    pub family: &'static str,
    pub file_name: &'static str,
    pub data: &'static [u8],
}

// Metrics sweep checklist (design-bundle-application S9b.5):
// - Button label vertical centering at Heights::BUTTON (28px)
// - Input baseline at Heights::INPUT (32px)
// - Sidebar group label uppercase tracking
// - Doc tab label truncation at Heights::TAB (36px)
// - Status-bar items at Heights::TOOLBAR (32px)
// - Modal body min-height 96px
// - Table row text at Heights::ROW (28px) / ROW_COMPACT (24px)
pub struct AppFonts;

impl AppFonts {
    /// Interface face: labels, buttons, menus, settings, body copy, tabs and tree rows.
    pub const INTERFACE: &'static str = "Archivo";
    /// Display face: uppercase section labels and large titles. Never used for body text.
    pub const DISPLAY: &'static str = "Archivo Expanded";
    /// Data face: grid cells, keys, queries, identifiers, key hints and numeric readouts.
    pub const MONO: &'static str = "JetBrains Mono";
    pub const MONO_FALLBACK: &'static str = "monospace";
}

pub const BUNDLED_FONT_ASSETS: [BundledFontAsset; 17] = [
    BundledFontAsset {
        family: AppFonts::INTERFACE,
        file_name: "Archivo-Regular.ttf",
        data: include_bytes!("../assets/fonts/Archivo-Regular.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::INTERFACE,
        file_name: "Archivo-Italic.ttf",
        data: include_bytes!("../assets/fonts/Archivo-Italic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::INTERFACE,
        file_name: "Archivo-Medium.ttf",
        data: include_bytes!("../assets/fonts/Archivo-Medium.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::INTERFACE,
        file_name: "Archivo-SemiBold.ttf",
        data: include_bytes!("../assets/fonts/Archivo-SemiBold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::INTERFACE,
        file_name: "Archivo-Bold.ttf",
        data: include_bytes!("../assets/fonts/Archivo-Bold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::INTERFACE,
        file_name: "Archivo-ExtraBold.ttf",
        data: include_bytes!("../assets/fonts/Archivo-ExtraBold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::DISPLAY,
        file_name: "ArchivoExpanded-Bold.ttf",
        data: include_bytes!("../assets/fonts/ArchivoExpanded-Bold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::DISPLAY,
        file_name: "ArchivoExpanded-ExtraBold.ttf",
        data: include_bytes!("../assets/fonts/ArchivoExpanded-ExtraBold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::DISPLAY,
        file_name: "ArchivoExpanded-Black.ttf",
        data: include_bytes!("../assets/fonts/ArchivoExpanded-Black.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-Regular.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-Regular.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-Italic.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-Italic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-Medium.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-Medium.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-MediumItalic.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-MediumItalic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-SemiBold.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-SemiBold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-SemiBoldItalic.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-SemiBoldItalic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-Bold.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-Bold.ttf"),
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

    #[cfg(test)]
    fn text(&self) -> Text {
        match self.size {
            HeadlineSize::Xl3 => Text::headline_3(self.text.clone()),
            HeadlineSize::Xl2 => Text::headline_2(self.text.clone()),
            HeadlineSize::Xl => Text::headline_1(self.text.clone()),
        }
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
        Text::subsection_label(self.text)
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
            .child(Text::sidebar_group_label(self.text))
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

    #[cfg(test)]
    fn text(&self) -> Text {
        Text::body_sm(self.text.clone())
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

    #[cfg(test)]
    fn text(&self) -> Text {
        Text::caption_xs(self.text.clone())
    }
}

impl RenderOnce for Caption {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let color = self.color.unwrap_or(cx.theme().muted_foreground);
        Text::caption_xs(self.text).color(color)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DefaultTextTone {
    Role,
    Muted,
}

fn apply_scaled_text_overrides(
    text: Text,
    size: Pixels,
    color: Option<Hsla>,
    weight_override: Option<FontWeight>,
    default_tone: DefaultTextTone,
) -> Text {
    let text = text.font_size(size);

    let text = match weight_override {
        Some(weight) => text.font_weight(weight),
        None => text,
    };

    match (color, default_tone) {
        (Some(color), _) => text.color(color),
        (None, DefaultTextTone::Role) => text,
        (None, DefaultTextTone::Muted) => text.muted_foreground(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InterfaceTextScale {
    Label,
    Caption,
    Meta,
}

impl InterfaceTextScale {
    fn default_size(self) -> Pixels {
        match self {
            Self::Label => FontSizes::BASE,
            Self::Caption => FontSizes::XS,
            Self::Meta => FontSizes::SM,
        }
    }

    fn density_size(self, cx: &App) -> Pixels {
        match self {
            Self::Label => density::font_base(cx),
            Self::Caption => density::font_xs(cx),
            Self::Meta => density::font_sm(cx),
        }
    }

    fn default_tone(self) -> DefaultTextTone {
        match self {
            Self::Label => DefaultTextTone::Role,
            Self::Caption | Self::Meta => DefaultTextTone::Muted,
        }
    }
}

/// Interface-face counterpart of `MonoLabel`, `MonoCaption` and `MonoMeta`
/// for chrome copy such as tab titles, tree rows, panel headers and status
/// items. Sizes and default colors match the mono helpers of the same scale.
#[derive(IntoElement)]
pub struct InterfaceText {
    text: SharedString,
    scale: InterfaceTextScale,
    color: Option<Hsla>,
    size_override: Option<gpui::Pixels>,
    weight_override: Option<FontWeight>,
}

impl InterfaceText {
    fn with_scale(text: impl Into<SharedString>, scale: InterfaceTextScale) -> Self {
        Self {
            text: text.into(),
            scale,
            color: None,
            size_override: None,
            weight_override: None,
        }
    }

    /// Base-size label in the foreground color.
    pub fn label(text: impl Into<SharedString>) -> Self {
        Self::with_scale(text, InterfaceTextScale::Label)
    }

    /// Extra-small caption in the muted foreground color.
    pub fn caption(text: impl Into<SharedString>) -> Self {
        Self::with_scale(text, InterfaceTextScale::Caption)
    }

    /// Small metadata text in the muted foreground color.
    pub fn meta(text: impl Into<SharedString>) -> Self {
        Self::with_scale(text, InterfaceTextScale::Meta)
    }

    pub fn color(mut self, color: Hsla) -> Self {
        self.color = Some(color);
        self
    }

    pub fn font_size(mut self, size: gpui::Pixels) -> Self {
        self.size_override = Some(size);
        self
    }

    pub fn font_weight(mut self, weight: FontWeight) -> Self {
        self.weight_override = Some(weight);
        self
    }

    /// Inspect the text contract without a render context.
    ///
    /// Uses Default-tier font sizes when no explicit size override is set.
    #[doc(hidden)]
    pub fn inspect(&self) -> MonoTextInspection {
        inspect_mono_text(&self.build_text(
            self.text.clone(),
            self.size_override.unwrap_or(self.scale.default_size()),
        ))
    }

    fn build_text(&self, text: SharedString, size: Pixels) -> Text {
        apply_scaled_text_overrides(
            Text::body_sm(text),
            size,
            self.color,
            self.weight_override,
            self.scale.default_tone(),
        )
    }
}

impl RenderOnce for InterfaceText {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let size = self
            .size_override
            .unwrap_or_else(|| self.scale.density_size(cx));
        let text = self.text.clone();

        self.build_text(text, size)
    }
}

#[derive(IntoElement)]
pub struct MonoLabel {
    text: SharedString,
    color: Option<Hsla>,
    size_override: Option<gpui::Pixels>,
    weight_override: Option<FontWeight>,
}

impl MonoLabel {
    pub fn new(text: impl Into<SharedString>) -> Self {
        Self {
            text: text.into(),
            color: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn color(mut self, color: Hsla) -> Self {
        self.color = Some(color);
        self
    }

    pub fn font_size(mut self, size: gpui::Pixels) -> Self {
        self.size_override = Some(size);
        self
    }

    pub fn font_weight(mut self, weight: FontWeight) -> Self {
        self.weight_override = Some(weight);
        self
    }

    /// Inspect the text contract without a render context.
    ///
    /// Uses Default-tier font sizes as the fallback when no explicit size
    /// override is set, because `inspect()` is called outside render where
    /// the density global is not available.
    #[doc(hidden)]
    pub fn inspect(&self) -> MonoTextInspection {
        inspect_mono_text(&Self::build_text(
            self.text.clone(),
            self.color,
            self.size_override.unwrap_or(FontSizes::BASE),
            self.weight_override,
        ))
    }

    fn build_text(
        text: SharedString,
        color: Option<Hsla>,
        size: Pixels,
        weight_override: Option<FontWeight>,
    ) -> Text {
        apply_scaled_text_overrides(
            Text::code(text),
            size,
            color,
            weight_override,
            DefaultTextTone::Role,
        )
    }

    #[cfg(test)]
    fn text(&self) -> Text {
        Self::build_text(
            self.text.clone(),
            self.color,
            self.size_override.unwrap_or(FontSizes::BASE),
            self.weight_override,
        )
    }
}

impl RenderOnce for MonoLabel {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let size = self.size_override.unwrap_or_else(|| density::font_base(cx));
        Self::build_text(self.text, self.color, size, self.weight_override)
    }
}

#[derive(IntoElement)]
pub struct MonoCaption {
    text: SharedString,
    color: Option<Hsla>,
    size_override: Option<gpui::Pixels>,
    weight_override: Option<FontWeight>,
}

impl MonoCaption {
    pub fn new(text: impl Into<SharedString>) -> Self {
        Self {
            text: text.into(),
            color: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn color(mut self, color: Hsla) -> Self {
        self.color = Some(color);
        self
    }

    pub fn font_size(mut self, size: gpui::Pixels) -> Self {
        self.size_override = Some(size);
        self
    }

    pub fn font_weight(mut self, weight: FontWeight) -> Self {
        self.weight_override = Some(weight);
        self
    }

    /// Inspect the text contract without a render context.
    ///
    /// Uses Default-tier font sizes as the fallback when no explicit size
    /// override is set.
    #[doc(hidden)]
    pub fn inspect(&self) -> MonoTextInspection {
        inspect_mono_text(&Self::build_text(
            self.text.clone(),
            self.color,
            self.size_override.unwrap_or(FontSizes::XS),
            self.weight_override,
        ))
    }

    fn build_text(
        text: SharedString,
        color: Option<Hsla>,
        size: Pixels,
        weight_override: Option<FontWeight>,
    ) -> Text {
        apply_scaled_text_overrides(
            Text::code(text),
            size,
            color,
            weight_override,
            DefaultTextTone::Muted,
        )
    }

    #[cfg(test)]
    fn text(&self) -> Text {
        Self::build_text(
            self.text.clone(),
            self.color,
            self.size_override.unwrap_or(FontSizes::XS),
            self.weight_override,
        )
    }
}

impl RenderOnce for MonoCaption {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let size = self.size_override.unwrap_or_else(|| density::font_xs(cx));
        Self::build_text(self.text, self.color, size, self.weight_override)
    }
}

#[derive(IntoElement)]
pub struct MonoMeta {
    text: SharedString,
    color: Option<Hsla>,
    size_override: Option<gpui::Pixels>,
    weight_override: Option<FontWeight>,
}

impl MonoMeta {
    pub fn new(text: impl Into<SharedString>) -> Self {
        Self {
            text: text.into(),
            color: None,
            size_override: None,
            weight_override: None,
        }
    }

    pub fn color(mut self, color: Hsla) -> Self {
        self.color = Some(color);
        self
    }

    pub fn font_size(mut self, size: gpui::Pixels) -> Self {
        self.size_override = Some(size);
        self
    }

    pub fn font_weight(mut self, weight: FontWeight) -> Self {
        self.weight_override = Some(weight);
        self
    }

    /// Inspect the text contract without a render context.
    ///
    /// Uses Default-tier font sizes as the fallback when no explicit size
    /// override is set.
    #[doc(hidden)]
    pub fn inspect(&self) -> MonoTextInspection {
        inspect_mono_text(&Self::build_text(
            self.text.clone(),
            self.color,
            self.size_override.unwrap_or(FontSizes::SM),
            self.weight_override,
        ))
    }

    fn build_text(
        text: SharedString,
        color: Option<Hsla>,
        size: Pixels,
        weight_override: Option<FontWeight>,
    ) -> Text {
        apply_scaled_text_overrides(
            Text::code(text),
            size,
            color,
            weight_override,
            DefaultTextTone::Muted,
        )
    }

    #[cfg(test)]
    fn text(&self) -> Text {
        Self::build_text(
            self.text.clone(),
            self.color,
            self.size_override.unwrap_or(FontSizes::SM),
            self.weight_override,
        )
    }
}

impl RenderOnce for MonoMeta {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let size = self.size_override.unwrap_or_else(|| density::font_sm(cx));
        Self::build_text(self.text, self.color, size, self.weight_override)
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

#[derive(IntoElement)]
pub struct PanelTitle {
    text: SharedString,
    color: Option<Hsla>,
}

impl PanelTitle {
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

    fn build_text(text: SharedString, color: Option<Hsla>, size: Pixels) -> Text {
        let text = Text::field_label(text)
            .font_size(size)
            .font_weight(FontWeight::SEMIBOLD);

        match color {
            Some(color) => text.color(color),
            None => text,
        }
    }
}

impl RenderOnce for PanelTitle {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        Self::build_text(self.text, self.color, density::font_lg(cx))
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

#[cfg(test)]
mod typography_role_tests {
    use super::{
        InterfaceText, MonoCaption, MonoColorSelection, MonoDefaultColor, MonoLabel, MonoMeta,
    };
    use crate::tokens::FontSizes;
    use crate::typography::AppFonts;
    #[test]
    fn mono_label_uses_shared_mono_family_with_label_metrics() {
        let text = MonoLabel::new("driver-key").text();

        let contract = text.role_contract();
        assert_eq!(contract.family, Some(AppFonts::MONO));
        assert_eq!(contract.fallbacks, &[AppFonts::MONO_FALLBACK]);
        assert_eq!(text.font_size_override(), Some(FontSizes::BASE));
        assert_eq!(text.font_weight_override(), None);
        assert!(text.uses_role_default_color());

        let inspection = MonoLabel::new("driver-key").inspect();
        assert_eq!(
            inspection.color_selection,
            MonoColorSelection::RoleDefault(MonoDefaultColor::Foreground)
        );
    }

    #[test]
    fn mono_caption_uses_shared_mono_family_with_caption_metrics() {
        let text = MonoCaption::new("v0.1.0").text();

        let contract = text.role_contract();
        assert_eq!(contract.family, Some(AppFonts::MONO));
        assert_eq!(contract.fallbacks, &[AppFonts::MONO_FALLBACK]);
        assert_eq!(text.font_size_override(), Some(FontSizes::XS));
        assert_eq!(text.font_weight_override(), None);
        assert!(text.uses_muted_foreground_override());

        let inspection = MonoCaption::new("v0.1.0").inspect();
        assert_eq!(
            inspection.color_selection,
            MonoColorSelection::MutedForeground
        );
    }

    #[test]
    fn mono_meta_uses_shared_mono_family_with_small_metadata_metrics() {
        let text = MonoMeta::new("dbflux-postgres").text();

        let contract = text.role_contract();
        assert_eq!(contract.family, Some(AppFonts::MONO));
        assert_eq!(contract.fallbacks, &[AppFonts::MONO_FALLBACK]);
        assert_eq!(text.font_size_override(), Some(FontSizes::SM));
        assert_eq!(text.font_weight_override(), None);
        assert!(text.uses_muted_foreground_override());

        let inspection = MonoMeta::new("dbflux-postgres").inspect();
        assert_eq!(
            inspection.color_selection,
            MonoColorSelection::MutedForeground
        );
    }

    #[test]
    fn mono_helpers_allow_explicit_color_overrides_without_losing_mono_contract() {
        let label_color = gpui::red();
        let caption_color = gpui::blue();
        let meta_color = gpui::green();

        let label = MonoLabel::new("driver-key").color(label_color).text();
        let caption = MonoCaption::new("12ms").color(caption_color).text();
        let meta = MonoMeta::new("actor: agent-a").color(meta_color).text();

        let label_inspection = MonoLabel::new("driver-key").color(label_color).inspect();
        let caption_inspection = MonoCaption::new("12ms").color(caption_color).inspect();
        let meta_inspection = MonoMeta::new("actor: agent-a").color(meta_color).inspect();

        assert_eq!(label.role_contract().family, Some(AppFonts::MONO));
        assert_eq!(caption.role_contract().family, Some(AppFonts::MONO));
        assert_eq!(meta.role_contract().family, Some(AppFonts::MONO));
        assert!(label.has_custom_color_override());
        assert!(caption.has_custom_color_override());
        assert!(meta.has_custom_color_override());
        assert!(!caption.uses_muted_foreground_override());
        assert!(!meta.uses_muted_foreground_override());
        assert_eq!(
            label_inspection.color_selection,
            MonoColorSelection::Custom(label_color)
        );
        assert_eq!(
            caption_inspection.color_selection,
            MonoColorSelection::Custom(caption_color)
        );
        assert_eq!(
            meta_inspection.color_selection,
            MonoColorSelection::Custom(meta_color)
        );
    }

    #[test]
    fn interface_text_uses_interface_family_with_mono_helper_metrics() {
        let label = InterfaceText::label("users").inspect();
        let caption = InterfaceText::caption("Tables").inspect();
        let meta = InterfaceText::meta("Connected").inspect();

        for inspection in [label, caption, meta] {
            assert_eq!(inspection.family, Some(AppFonts::INTERFACE));
            assert!(inspection.fallbacks.is_empty());
            assert_eq!(inspection.weight_override, None);
        }

        assert_eq!(label.size_override, Some(FontSizes::BASE));
        assert_eq!(
            label.color_selection,
            MonoColorSelection::RoleDefault(MonoDefaultColor::Foreground)
        );
        assert_eq!(caption.size_override, Some(FontSizes::XS));
        assert!(caption.uses_muted_foreground_override);
        assert_eq!(meta.size_override, Some(FontSizes::SM));
        assert!(meta.uses_muted_foreground_override);
    }

    #[test]
    fn interface_text_keeps_explicit_overrides() {
        let color = gpui::red();
        let inspection = InterfaceText::caption("Panel")
            .color(color)
            .font_size(FontSizes::SM)
            .font_weight(gpui::FontWeight::BOLD)
            .inspect();

        assert_eq!(inspection.family, Some(AppFonts::INTERFACE));
        assert_eq!(
            inspection.color_selection,
            MonoColorSelection::Custom(color)
        );
        assert_eq!(inspection.size_override, Some(FontSizes::SM));
        assert_eq!(inspection.weight_override, Some(gpui::FontWeight::BOLD));
    }

    #[test]
    fn mono_helpers_allow_explicit_size_and_weight_overrides() {
        let label = MonoLabel::new("table_name")
            .font_size(FontSizes::SM)
            .font_weight(gpui::FontWeight::MEDIUM)
            .inspect();

        let caption = MonoCaption::new("Background Tasks")
            .font_size(FontSizes::SM)
            .font_weight(gpui::FontWeight::BOLD)
            .inspect();

        let meta = MonoMeta::new("dbflux-postgres")
            .font_weight(gpui::FontWeight::SEMIBOLD)
            .inspect();

        assert_eq!(label.size_override, Some(FontSizes::SM));
        assert_eq!(label.weight_override, Some(gpui::FontWeight::MEDIUM));
        assert_eq!(caption.size_override, Some(FontSizes::SM));
        assert_eq!(caption.weight_override, Some(gpui::FontWeight::BOLD));
        assert_eq!(meta.size_override, Some(FontSizes::SM));
        assert_eq!(meta.weight_override, Some(gpui::FontWeight::SEMIBOLD));
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
            .font_family(AppFonts::INTERFACE)
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
        div()
            .font_family(AppFonts::INTERFACE)
            .font_weight(FontWeight::MEDIUM)
            .children(self.children)
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
            FontWeight::SEMIBOLD
        } else {
            FontWeight::MEDIUM
        };

        div()
            .font_family(AppFonts::INTERFACE)
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
        div()
            .font_family(AppFonts::INTERFACE)
            .font_weight(FontWeight::MEDIUM)
            .children(self.children)
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
        div()
            .font_family(AppFonts::INTERFACE)
            .font_weight(FontWeight::MEDIUM)
            .children(self.children)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppFonts, BUNDLED_FONT_ASSETS, Body, Caption, FieldLabel, Headline, RequiredMarker,
    };
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
    fn font_contract_splits_interface_display_and_data_families() {
        assert_eq!(AppFonts::INTERFACE, "Archivo");
        assert_eq!(AppFonts::DISPLAY, "Archivo Expanded");
        assert_eq!(AppFonts::MONO, "JetBrains Mono");
        assert_eq!(AppFonts::MONO_FALLBACK, "monospace");
    }

    #[test]
    fn bundled_assets_cover_every_family() {
        let assets: Vec<_> = BUNDLED_FONT_ASSETS
            .iter()
            .map(|asset| (asset.family, asset.file_name))
            .collect();

        assert_eq!(
            assets,
            vec![
                (AppFonts::INTERFACE, "Archivo-Regular.ttf"),
                (AppFonts::INTERFACE, "Archivo-Italic.ttf"),
                (AppFonts::INTERFACE, "Archivo-Medium.ttf"),
                (AppFonts::INTERFACE, "Archivo-SemiBold.ttf"),
                (AppFonts::INTERFACE, "Archivo-Bold.ttf"),
                (AppFonts::INTERFACE, "Archivo-ExtraBold.ttf"),
                (AppFonts::DISPLAY, "ArchivoExpanded-Bold.ttf"),
                (AppFonts::DISPLAY, "ArchivoExpanded-ExtraBold.ttf"),
                (AppFonts::DISPLAY, "ArchivoExpanded-Black.ttf"),
                (AppFonts::MONO, "JetBrainsMono-Regular.ttf"),
                (AppFonts::MONO, "JetBrainsMono-Italic.ttf"),
                (AppFonts::MONO, "JetBrainsMono-Medium.ttf"),
                (AppFonts::MONO, "JetBrainsMono-MediumItalic.ttf"),
                (AppFonts::MONO, "JetBrainsMono-SemiBold.ttf"),
                (AppFonts::MONO, "JetBrainsMono-SemiBoldItalic.ttf"),
                (AppFonts::MONO, "JetBrainsMono-Bold.ttf"),
                (AppFonts::MONO, "JetBrainsMono-BoldItalic.ttf"),
            ]
        );
    }

    #[test]
    fn headline_wrapper_keeps_shared_headline_contract_for_window_titles() {
        let headline = Headline::new("Connection Manager").xl().text();

        assert_eq!(
            headline.role_contract(),
            TextVariant::Headline1.role_contract()
        );
        assert!(headline.uses_role_default_color());
    }

    #[test]
    fn body_and_caption_wrappers_keep_shared_interface_contracts() {
        let body = Body::new("Sidebar").text();
        let caption = Caption::new("Settings").text();

        assert_eq!(body.role_contract(), TextVariant::BodySm.role_contract());
        assert_eq!(
            caption.role_contract(),
            TextVariant::CaptionXs.role_contract()
        );
        assert!(body.uses_role_default_color());
        assert!(caption.uses_role_default_color());
    }
}
