use gpui::prelude::*;
use gpui::{div, AnyElement, App, FontWeight, Hsla, SharedString, Window};
use gpui_component::ActiveTheme;
use std::borrow::Cow;

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

pub struct AppFonts;

impl AppFonts {
    pub const HEADLINE: &'static str = "IBM Plex Mono";
    pub const BODY: &'static str = "IBM Plex Mono";
    pub const MONO: &'static str = "IBM Plex Mono";
    pub const MONO_FALLBACK: &'static str = "monospace";
    pub const CODE: &'static str = Self::MONO;
    pub const SHORTCUT: &'static str = Self::MONO;
}

pub const BUNDLED_FONT_ASSETS: [BundledFontAsset; 8] = [
    BundledFontAsset {
        family: AppFonts::BODY,
        file_name: "IBMPlexMono-Regular.ttf",
        data: include_bytes!("../assets/fonts/IBMPlexMono-Regular.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::BODY,
        file_name: "IBMPlexMono-Italic.ttf",
        data: include_bytes!("../assets/fonts/IBMPlexMono-Italic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::BODY,
        file_name: "IBMPlexMono-Medium.ttf",
        data: include_bytes!("../assets/fonts/IBMPlexMono-Medium.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::BODY,
        file_name: "IBMPlexMono-MediumItalic.ttf",
        data: include_bytes!("../assets/fonts/IBMPlexMono-MediumItalic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::BODY,
        file_name: "IBMPlexMono-SemiBold.ttf",
        data: include_bytes!("../assets/fonts/IBMPlexMono-SemiBold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::BODY,
        file_name: "IBMPlexMono-SemiBoldItalic.ttf",
        data: include_bytes!("../assets/fonts/IBMPlexMono-SemiBoldItalic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::BODY,
        file_name: "IBMPlexMono-Bold.ttf",
        data: include_bytes!("../assets/fonts/IBMPlexMono-Bold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::BODY,
        file_name: "IBMPlexMono-BoldItalic.ttf",
        data: include_bytes!("../assets/fonts/IBMPlexMono-BoldItalic.ttf"),
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

    #[doc(hidden)]
    pub fn inspect(&self) -> MonoTextInspection {
        inspect_mono_text(&Self::build_text(
            self.text.clone(),
            self.color,
            self.size_override,
            self.weight_override,
        ))
    }

    fn build_text(
        text: SharedString,
        color: Option<Hsla>,
        size_override: Option<gpui::Pixels>,
        weight_override: Option<FontWeight>,
    ) -> Text {
        let text = Text::code(text).font_size(size_override.unwrap_or(FontSizes::BASE));

        let text = match weight_override {
            Some(weight) => text.font_weight(weight),
            None => text,
        };

        match color {
            Some(color) => text.color(color),
            None => text,
        }
    }

    #[cfg(test)]
    fn text(&self) -> Text {
        Self::build_text(
            self.text.clone(),
            self.color,
            self.size_override,
            self.weight_override,
        )
    }
}

impl RenderOnce for MonoLabel {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        Self::build_text(
            self.text,
            self.color,
            self.size_override,
            self.weight_override,
        )
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

    #[doc(hidden)]
    pub fn inspect(&self) -> MonoTextInspection {
        inspect_mono_text(&Self::build_text(
            self.text.clone(),
            self.color,
            self.size_override,
            self.weight_override,
        ))
    }

    fn build_text(
        text: SharedString,
        color: Option<Hsla>,
        size_override: Option<gpui::Pixels>,
        weight_override: Option<FontWeight>,
    ) -> Text {
        let text = Text::code(text).font_size(size_override.unwrap_or(FontSizes::XS));

        let text = match weight_override {
            Some(weight) => text.font_weight(weight),
            None => text,
        };

        match color {
            Some(color) => text.color(color),
            None => text.muted_foreground(),
        }
    }

    #[cfg(test)]
    fn text(&self) -> Text {
        Self::build_text(
            self.text.clone(),
            self.color,
            self.size_override,
            self.weight_override,
        )
    }
}

impl RenderOnce for MonoCaption {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        Self::build_text(
            self.text,
            self.color,
            self.size_override,
            self.weight_override,
        )
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

    #[doc(hidden)]
    pub fn inspect(&self) -> MonoTextInspection {
        inspect_mono_text(&Self::build_text(
            self.text.clone(),
            self.color,
            self.size_override,
            self.weight_override,
        ))
    }

    fn build_text(
        text: SharedString,
        color: Option<Hsla>,
        size_override: Option<gpui::Pixels>,
        weight_override: Option<FontWeight>,
    ) -> Text {
        let text = Text::code(text).font_size(size_override.unwrap_or(FontSizes::SM));

        let text = match weight_override {
            Some(weight) => text.font_weight(weight),
            None => text,
        };

        match color {
            Some(color) => text.color(color),
            None => text.muted_foreground(),
        }
    }

    #[cfg(test)]
    fn text(&self) -> Text {
        Self::build_text(
            self.text.clone(),
            self.color,
            self.size_override,
            self.weight_override,
        )
    }
}

impl RenderOnce for MonoMeta {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        Self::build_text(
            self.text,
            self.color,
            self.size_override,
            self.weight_override,
        )
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

    fn build_text(text: SharedString, color: Option<Hsla>) -> Text {
        let text = Text::field_label(text)
            .font_size(FontSizes::LG)
            .font_weight(FontWeight::SEMIBOLD);

        match color {
            Some(color) => text.color(color),
            None => text,
        }
    }
}

impl RenderOnce for PanelTitle {
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

#[cfg(test)]
mod typography_role_tests {
    use super::{MonoCaption, MonoColorSelection, MonoDefaultColor, MonoLabel, MonoMeta};
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
        div()
            .font_family(AppFonts::BODY)
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
            FontWeight::BOLD
        } else {
            FontWeight::MEDIUM
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
        div()
            .font_family(AppFonts::BODY)
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
            .font_family(AppFonts::BODY)
            .font_weight(FontWeight::MEDIUM)
            .children(self.children)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppFonts, Body, Caption, FieldLabel, Headline, RequiredMarker, BUNDLED_FONT_ASSETS,
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
    fn mono_font_contract_stays_on_ibm_plex_mono() {
        assert_eq!(AppFonts::BODY, "IBM Plex Mono");
        assert_eq!(AppFonts::HEADLINE, "IBM Plex Mono");
        assert_eq!(AppFonts::MONO, "IBM Plex Mono");
        assert_eq!(AppFonts::MONO_FALLBACK, "monospace");
        assert_eq!(AppFonts::CODE, AppFonts::MONO);
        assert_eq!(AppFonts::SHORTCUT, AppFonts::MONO);
    }

    #[test]
    fn mono_bundled_assets_use_ibm_plex_mono_files() {
        let mono_assets: Vec<_> = BUNDLED_FONT_ASSETS
            .iter()
            .map(|asset| asset.file_name)
            .collect();

        assert_eq!(
            mono_assets,
            vec![
                "IBMPlexMono-Regular.ttf",
                "IBMPlexMono-Italic.ttf",
                "IBMPlexMono-Medium.ttf",
                "IBMPlexMono-MediumItalic.ttf",
                "IBMPlexMono-SemiBold.ttf",
                "IBMPlexMono-SemiBoldItalic.ttf",
                "IBMPlexMono-Bold.ttf",
                "IBMPlexMono-BoldItalic.ttf",
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
    fn body_and_caption_wrappers_keep_shared_mono_first_contracts() {
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
