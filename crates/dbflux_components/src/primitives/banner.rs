//! Product-side default icons over bolt's generic [`BannerBlock`].
//!
//! The generic banner primitive lives in `bolt`; the icon catalog is a
//! product asset (see `bolt::icon`), so the variant-to-glyph mapping and the
//! default-icon behavior stay here.

use gpui::prelude::*;
use gpui::{App, SharedString, Window};

use crate::icons::AppIcon;

// The generic banner moved to `bolt`; re-exported at this path so call sites
// keep compiling.
pub use bolt::primitives::BannerVariant;

/// Icon shown when the caller does not supply one.
fn default_icon(variant: BannerVariant) -> AppIcon {
    match variant {
        BannerVariant::Info => AppIcon::Info,
        BannerVariant::Success => AppIcon::CircleCheck,
        BannerVariant::Warning | BannerVariant::Danger => AppIcon::TriangleAlert,
    }
}

/// A stateless notification banner, with the product's default leading icon
/// per variant. See `bolt::primitives::BannerBlock` for the generic primitive.
#[derive(IntoElement)]
pub struct BannerBlock(bolt::primitives::BannerBlock);

impl BannerBlock {
    pub fn new(variant: BannerVariant, title: impl Into<SharedString>) -> Self {
        Self(bolt::primitives::BannerBlock::new(variant, title).default_icon(default_icon(variant)))
    }

    /// Replace the default leading icon.
    pub fn with_icon(mut self, icon: impl IntoElement) -> Self {
        self.0 = self.0.with_icon(icon);
        self
    }

    /// Add a secondary descriptive line below the title.
    pub fn with_body(mut self, text: impl Into<SharedString>) -> Self {
        self.0 = self.0.with_body(text);
        self
    }

    /// Add a monospace pre-formatted block (e.g. error details, stack trace).
    pub fn with_pre(mut self, text: impl Into<SharedString>) -> Self {
        self.0 = self.0.with_pre(text);
        self
    }

    /// Attach a trailing slot for action buttons.
    pub fn with_actions(mut self, actions: impl IntoElement) -> Self {
        self.0 = self.0.with_actions(actions);
        self
    }
}

impl RenderOnce for BannerBlock {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        self.0.render(window, cx)
    }
}
