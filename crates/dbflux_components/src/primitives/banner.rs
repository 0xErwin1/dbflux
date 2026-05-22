//! `BannerBlock` — a status/notification banner with optional icon, body, and actions.
//!
//! Colors are sourced entirely from `BannerColors` — no hardcoded hex values.

use gpui::prelude::*;
use gpui::{AnyElement, App, FontWeight, SharedString, Window, div};

use crate::semantic::BannerColors as SemBannerColors;
use crate::tokens::{FontSizes, Radii, Spacing};
use crate::typography::AppFonts;

/// Semantic variant controlling banner colors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BannerVariant {
    Info,
    Success,
    Warning,
    Danger,
}

/// A stateless notification banner with leading icon, title, optional body,
/// optional monospace pre-block, and optional trailing action slot.
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

    /// Attach a leading icon element (e.g. an `Icon` or `StatusDot`).
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

    fn resolve_colors(variant: BannerVariant, cx: &App) -> (gpui::Hsla, gpui::Hsla) {
        let b = SemBannerColors::for_current(cx);
        match variant {
            BannerVariant::Info => (b.info_bg, b.info_fg),
            BannerVariant::Success => (b.success_bg, b.success_fg),
            BannerVariant::Warning => (b.warning_bg, b.warning_fg),
            BannerVariant::Danger => (b.error_bg, b.error_fg),
        }
    }
}

impl RenderOnce for BannerBlock {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let (bg, fg) = Self::resolve_colors(self.variant, cx);

        let mut pre_tint = fg;
        pre_tint.a = 0.08;

        // `flex_1 + min_w_0` lets the title/body text wrap to the
        // banner's width instead of overflowing on long error messages.
        let mut content_col = div()
            .flex_1()
            .min_w_0()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(fg)
                    .child(self.title),
            );

        if let Some(body) = self.body {
            content_col =
                content_col.child(div().text_size(FontSizes::SM).text_color(fg).child(body));
        }

        if let Some(pre) = self.pre_block {
            content_col = content_col.child(
                div()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .rounded(Radii::SM)
                    .bg(pre_tint)
                    .font_family(AppFonts::MONO)
                    .text_size(FontSizes::XS)
                    .text_color(fg)
                    .child(pre),
            );
        }

        // `min_w_0` on every flex item in the chain (row, icon-wrapper,
        // outer) is required for `content_col` to actually have a
        // constrained width — without it the chain reports content-size
        // upward and `min_w_0` on content_col alone has no effect.
        let mut row = div()
            .flex()
            .items_start()
            .gap(Spacing::SM)
            .flex_1()
            .min_w_0()
            .child(content_col);

        if let Some(icon) = self.icon {
            row = div()
                .flex()
                .items_start()
                .gap(Spacing::SM)
                .flex_1()
                .min_w_0()
                .child(icon)
                .child(row);
        }

        let mut outer = div()
            .flex()
            .w_full()
            .items_start()
            .justify_between()
            .gap(Spacing::SM)
            .p(Spacing::MD)
            .rounded(Radii::SM)
            .bg(bg)
            .child(row);

        if let Some(actions) = self.actions {
            outer = outer.child(actions);
        }

        outer
    }
}
