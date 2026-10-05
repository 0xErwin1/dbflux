//! `BannerBlock` — an inline status banner: a kind-colored edge stripe and
//! icon on a lightly tinted field, with optional body, pre-block and actions.
//!
//! Colors are sourced entirely from `BannerColors` — no hardcoded hex values.

use gpui::prelude::*;
use gpui::{AnyElement, App, FontWeight, Hsla, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon};
use crate::semantic::BannerColors as SemBannerColors;
use crate::tokens::{ChamferCut, ChromeColors, Feedback, FontSizes, Spacing};

/// Semantic variant controlling banner colors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BannerVariant {
    Info,
    Success,
    Warning,
    Danger,
}

impl BannerVariant {
    /// Icon shown when the caller does not supply one.
    fn default_icon(self) -> AppIcon {
        match self {
            Self::Info => AppIcon::Info,
            Self::Success => AppIcon::CircleCheck,
            Self::Warning | Self::Danger => AppIcon::TriangleAlert,
        }
    }

    /// Stripe and icon color.
    fn accent(self, cx: &App) -> Hsla {
        let banners = SemBannerColors::for_current(cx);
        match self {
            Self::Info => banners.info_fg,
            Self::Success => banners.success_fg,
            Self::Warning => banners.warning_fg,
            Self::Danger => banners.error_fg,
        }
    }
}

/// A stateless notification banner with leading icon, title, optional body,
/// optional monospace pre-block, and optional trailing action slot.
///
/// The field is the variant color at `Feedback::BANNER_FILL_ALPHA` with a
/// `Feedback::BANNER_STRIPE` stripe on the left edge, on an 8 px chamfer. Text
/// stays in the body color; only the stripe and icon carry the variant.
#[derive(IntoElement)]
pub struct BannerBlock {
    variant: BannerVariant,
    icon: Option<AnyElement>,
    title: SharedString,
    body: Option<SharedString>,
    pre_block: Option<SharedString>,
    actions: Option<AnyElement>,
}

impl BannerBlock {
    pub fn new(variant: BannerVariant, title: impl Into<SharedString>) -> Self {
        Self {
            variant,
            icon: None,
            title: title.into(),
            body: None,
            pre_block: None,
            actions: None,
        }
    }

    /// Replace the variant's default leading icon.
    pub fn with_icon(mut self, icon: impl IntoElement) -> Self {
        self.icon = Some(icon.into_any_element());
        self
    }

    /// Add a secondary descriptive line below the title.
    pub fn with_body(mut self, text: impl Into<SharedString>) -> Self {
        self.body = Some(text.into());
        self
    }

    /// Add a monospace pre-formatted block (e.g. error details, stack trace).
    pub fn with_pre(mut self, text: impl Into<SharedString>) -> Self {
        self.pre_block = Some(text.into());
        self
    }

    /// Attach a trailing slot for action buttons.
    pub fn with_actions(mut self, actions: impl IntoElement) -> Self {
        self.actions = Some(actions.into_any_element());
        self
    }
}

impl RenderOnce for BannerBlock {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let accent = self.variant.accent(cx);
        let theme = cx.theme();
        let text_color = theme.foreground;
        let title_color = ChromeColors::strong(theme);
        let field = accent.opacity(Feedback::BANNER_FILL_ALPHA);
        let pre_field = theme.background;

        let is_multiline = self.body.is_some() || self.pre_block.is_some();

        let icon = self.icon.unwrap_or_else(|| {
            Icon::new(self.variant.default_icon())
                .size(Feedback::BANNER_ICON)
                .color(accent)
                .into_any_element()
        });

        // `flex_1 + min_w_0` on every flex item in the chain lets the title
        // and body wrap to the banner's width instead of overflowing on long
        // error messages.
        let content = div()
            .flex_1()
            .min_w_0()
            .flex()
            .flex_col()
            .gap(Spacing::XXS)
            .child(
                div()
                    .text_size(FontSizes::BASE)
                    .text_color(if is_multiline {
                        title_color
                    } else {
                        text_color
                    })
                    .when(is_multiline, |el| el.font_weight(FontWeight::SEMIBOLD))
                    .child(self.title),
            )
            .when_some(self.body, |el, body| {
                el.child(
                    div()
                        .text_size(FontSizes::BASE)
                        .text_color(text_color)
                        .child(body),
                )
            })
            .when_some(self.pre_block, |el, pre| {
                el.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .bg(pre_field)
                        .font_family(crate::fonts::editor_family(cx))
                        .text_size(FontSizes::XS)
                        .text_color(text_color)
                        .child(pre),
                )
            });

        div()
            .relative()
            .flex()
            .w_full()
            .min_w_0()
            .when(is_multiline, |el| el.items_start())
            .when(!is_multiline, |el| el.items_center())
            .gap(Feedback::BANNER_GAP)
            .py(Feedback::BANNER_PADDING_Y)
            .px(Feedback::BANNER_PADDING_X)
            .child(
                Chamfer::new(ChamferCut::INPUT)
                    .fill(field)
                    .left_edge(accent, Feedback::BANNER_STRIPE),
            )
            .child(div().flex_shrink_0().flex().items_center().child(icon))
            .child(content)
            .when_some(self.actions, |el, actions| {
                el.child(div().flex_shrink_0().child(actions))
            })
    }
}
