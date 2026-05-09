//! `KbdBadge` — a styled keyboard shortcut badge primitive.
//!
//! Renders a keyboard key label inside a bordered, rounded container that
//! resembles a physical keycap. Distinct from `KeyHint` (which is plain
//! monospace text): `KbdBadge` adds a background tint, a visible border, and
//! horizontal padding so it reads as a pressable element.
//!
//! # Usage
//!
//! ```ignore
//! KbdBadge::new("⌘K")
//! KbdBadge::new("Ctrl+P").muted()
//! ```

use gpui::prelude::*;
use gpui::{App, FontWeight, SharedString, Window, div, px};
use gpui_component::ActiveTheme;

use crate::tokens::{FontSizes, Radii, Spacing};
use crate::typography::AppFonts;

/// A keyboard shortcut badge that looks like a keycap.
///
/// By default uses `theme.secondary` as the background and `theme.foreground`
/// as the text color. Call `.muted()` to switch to `theme.muted_foreground`
/// for de-emphasized shortcuts in help text or status bars.
#[derive(IntoElement)]
pub struct KbdBadge {
    label: SharedString,
    muted: bool,
}

impl KbdBadge {
    /// Create a new `KbdBadge` with the given key label.
    ///
    /// `label` should be a short string such as `"⌘K"`, `"Esc"`, or `"F1"`.
    pub fn new(label: impl Into<SharedString>) -> Self {
        Self {
            label: label.into(),
            muted: false,
        }
    }

    /// De-emphasize the badge — uses `theme.muted_foreground` for the key text
    /// and a slightly more transparent background.
    pub fn muted(mut self) -> Self {
        self.muted = true;
        self
    }

    /// Return the rendered font weight for the key label.
    ///
    /// `SEMIBOLD` (600) matches the "font weight 700→600" chrome fix applied
    /// across active UI elements in Phase 5.
    fn font_weight() -> FontWeight {
        FontWeight::SEMIBOLD
    }
}

impl RenderOnce for KbdBadge {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();

        let text_color = if self.muted {
            theme.muted_foreground
        } else {
            theme.foreground
        };

        let border_color = if self.muted {
            theme.border
        } else {
            theme.border
        };

        div()
            .flex()
            .items_center()
            .px(Spacing::XS)
            .py(px(1.0))
            .rounded(Radii::SM)
            .border_1()
            .border_color(border_color)
            .bg(theme.secondary)
            .font_family(AppFonts::SHORTCUT)
            .text_size(FontSizes::XS)
            .font_weight(Self::font_weight())
            .text_color(text_color)
            .child(self.label)
    }
}

// ---------------------------------------------------------------------------
// Inspectable API for tests
// ---------------------------------------------------------------------------

/// Inspection output for unit-testing `KbdBadge` properties without rendering.
#[derive(Debug, PartialEq)]
pub struct KbdBadgeInspection {
    pub font_family: &'static str,
    pub font_size: gpui::Pixels,
    pub font_weight: FontWeight,
    pub is_muted: bool,
}

impl KbdBadge {
    /// Return inspectable properties for unit testing.
    #[doc(hidden)]
    pub fn inspect(&self) -> KbdBadgeInspection {
        KbdBadgeInspection {
            font_family: AppFonts::SHORTCUT,
            font_size: FontSizes::XS,
            font_weight: Self::font_weight(),
            is_muted: self.muted,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::FontSizes;
    use crate::typography::AppFonts;

    #[test]
    fn kbd_badge_uses_shortcut_font_and_xs_size() {
        let badge = KbdBadge::new("⌘K").inspect();
        assert_eq!(badge.font_family, AppFonts::SHORTCUT);
        assert_eq!(badge.font_size, FontSizes::XS);
    }

    #[test]
    fn kbd_badge_uses_semibold_weight() {
        let badge = KbdBadge::new("Ctrl+P").inspect();
        assert_eq!(badge.font_weight, FontWeight::SEMIBOLD);
    }

    #[test]
    fn kbd_badge_muted_flag_is_reflected_in_inspection() {
        let normal = KbdBadge::new("Esc").inspect();
        let muted = KbdBadge::new("Esc").muted().inspect();
        assert!(!normal.is_muted);
        assert!(muted.is_muted);
    }
}
