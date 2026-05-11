//! `StatusDot` — a fixed-color 8 px circle indicating status.
//!
//! Animation (e.g. pulse for Busy) is the consumer's responsibility.
//! `StatusDot` only renders a static colored circle.

use gpui::prelude::*;
use gpui::{App, Window, div, px};
use gpui_component::ActiveTheme;

use crate::tokens::{Radii, StatusDotPalette};

/// Semantic variant controlling the dot color.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusDotVariant {
    Idle,
    Busy,
    Success,
    Warning,
    Danger,
    Neutral,
}

/// A stateless 8 px colored circle drawn at full-radius (pill / circle shape).
///
/// Color is sourced from [`StatusDotPalette`]. The `Busy` variant renders
/// amber — if pulsing is required the caller must wrap this element in an
/// animated container.
#[derive(IntoElement)]
pub struct StatusDot {
    variant: StatusDotVariant,
}

impl StatusDot {
    pub fn new(variant: StatusDotVariant) -> Self {
        Self { variant }
    }
}

impl RenderOnce for StatusDot {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();

        let color = match self.variant {
            StatusDotVariant::Idle => StatusDotPalette::idle(theme),
            StatusDotVariant::Busy => StatusDotPalette::busy(theme),
            StatusDotVariant::Success => StatusDotPalette::success(theme),
            StatusDotVariant::Warning => StatusDotPalette::warning(theme),
            StatusDotVariant::Danger => StatusDotPalette::danger(theme),
            StatusDotVariant::Neutral => StatusDotPalette::neutral(theme),
        };

        div().size(px(8.0)).rounded(Radii::FULL).bg(color)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_dot_variant_clone_copy() {
        let v = StatusDotVariant::Busy;
        let v2 = v;
        assert_eq!(v, v2);
    }

    #[test]
    fn status_dot_new_stores_variant() {
        let dot = StatusDot::new(StatusDotVariant::Success);
        assert_eq!(dot.variant, StatusDotVariant::Success);
    }
}
