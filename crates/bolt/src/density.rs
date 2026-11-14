//! How dense a product renders its chrome.
//!
//! Two tiers: the baseline [`Density::Default`] and the optional
//! [`Density::Compact`]. A product maps its own style setting onto a
//! `Density`, publishes it with [`init`] (startup) or [`set`] (runtime), and
//! hands it to the theme; a product that never asks for compact simply never
//! receives it.
//!
//! The `font_*` and `radius_*` accessors read the published density and return
//! the value for the active tier: font sizes in rems (so they follow the
//! interface scale), radii in pixels.

use gpui::{App, Global, Pixels, Rems, px};

use crate::tokens::ui;

/// The density tier a product renders at.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Density {
    /// The baseline tier: square corners, roomy chrome.
    #[default]
    Default,
    /// The compact tier: subtle radii, tighter chrome.
    Compact,
}

impl Density {
    /// Corner radius of a control.
    pub fn radius(self) -> Pixels {
        match self {
            Density::Default => px(0.0),
            Density::Compact => px(2.0),
        }
    }

    /// Corner radius of a large surface, such as a popover shell.
    pub fn radius_lg(self) -> Pixels {
        match self {
            Density::Default => px(0.0),
            Density::Compact => px(3.0),
        }
    }
}

// ---------------------------------------------------------------------------
// Published density
// ---------------------------------------------------------------------------

/// GPUI global that stores the density the product published.
///
/// Accessors fall back to [`Density::Default`] when the global has not been
/// registered (e.g., in unit tests that skip full app init).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DensityGlobal {
    pub density: Density,
}

impl Global for DensityGlobal {}

/// Register the density global. Call this once during app startup, after the
/// theme is initialized and before any rendering occurs.
pub fn init(cx: &mut App, density: Density) {
    cx.set_global(DensityGlobal { density });
}

/// Update the density global when the user changes the density at runtime.
pub fn set(cx: &mut App, density: Density) {
    cx.set_global(DensityGlobal { density });
}

/// The density the product published, or [`Density::Default`] before it does.
pub fn active(cx: &App) -> Density {
    cx.try_global::<DensityGlobal>()
        .map(|global| global.density)
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Font-size accessors
// ---------------------------------------------------------------------------

/// Section label font: 11 px (Default) / 10 px (Compact).
pub fn font_label(cx: &App) -> Rems {
    match active(cx) {
        Density::Default => ui(11.0),
        Density::Compact => ui(10.0),
    }
}

/// Extra-small font: 12 px (Default) / 11 px (Compact).
pub fn font_xs(cx: &App) -> Rems {
    match active(cx) {
        Density::Default => ui(12.0),
        Density::Compact => ui(11.0),
    }
}

/// Small font: 13 px (Default) / 12 px (Compact).
pub fn font_sm(cx: &App) -> Rems {
    match active(cx) {
        Density::Default => ui(13.0),
        Density::Compact => ui(12.0),
    }
}

/// Base font: 13 px (Default) / 12 px (Compact).
pub fn font_base(cx: &App) -> Rems {
    match active(cx) {
        Density::Default => ui(13.0),
        Density::Compact => ui(12.0),
    }
}

/// Large font: 15 px (Default) / 14 px (Compact).
pub fn font_lg(cx: &App) -> Rems {
    match active(cx) {
        Density::Default => ui(15.0),
        Density::Compact => ui(14.0),
    }
}

/// Extra-large font: 18 px (Default) / 16 px (Compact).
pub fn font_xl(cx: &App) -> Rems {
    match active(cx) {
        Density::Default => ui(18.0),
        Density::Compact => ui(16.0),
    }
}

/// Title font: 20 px (Default) / 18 px (Compact).
pub fn font_title(cx: &App) -> Rems {
    match active(cx) {
        Density::Default => ui(20.0),
        Density::Compact => ui(18.0),
    }
}

// ---------------------------------------------------------------------------
// Border-radius accessors
// ---------------------------------------------------------------------------

/// Small radius: 0 px (Default) / 2 px (Compact).
pub fn radius_sm(cx: &App) -> Pixels {
    active(cx).radius()
}

/// Medium radius: 0 px (Default) / 2 px (Compact).
pub fn radius_md(cx: &App) -> Pixels {
    active(cx).radius()
}

/// Large radius: 0 px (Default) / 3 px (Compact).
pub fn radius_lg(cx: &App) -> Pixels {
    active(cx).radius_lg()
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::{TestAppContext, px};

    #[test]
    fn the_baseline_tier_keeps_square_corners() {
        assert_eq!(Density::default(), Density::Default);
        assert_eq!(Density::Default.radius(), px(0.0));
        assert_eq!(Density::Default.radius_lg(), px(0.0));
    }

    #[test]
    fn the_compact_tier_applies_the_token_radii() {
        assert_eq!(Density::Compact.radius(), px(2.0));
        assert_eq!(Density::Compact.radius_lg(), px(3.0));
    }

    #[gpui::test]
    fn the_default_tier_yields_the_legacy_font_and_square_radius(cx: &mut TestAppContext) {
        cx.update(|cx| {
            init(cx, Density::Default);

            assert_eq!(font_label(cx), ui(11.0));
            assert_eq!(font_xs(cx), ui(12.0));
            assert_eq!(font_sm(cx), ui(13.0));
            assert_eq!(font_base(cx), ui(13.0));
            assert_eq!(font_lg(cx), ui(15.0));
            assert_eq!(font_xl(cx), ui(18.0));
            assert_eq!(font_title(cx), ui(20.0));

            assert_eq!(radius_sm(cx), px(0.0));
            assert_eq!(radius_md(cx), px(0.0));
            assert_eq!(radius_lg(cx), px(0.0));
        });
    }

    #[gpui::test]
    fn the_compact_tier_yields_the_ds_font_and_rounded_radius(cx: &mut TestAppContext) {
        cx.update(|cx| {
            init(cx, Density::Compact);

            assert_eq!(font_label(cx), ui(10.0));
            assert_eq!(font_xs(cx), ui(11.0));
            assert_eq!(font_sm(cx), ui(12.0));
            assert_eq!(font_base(cx), ui(12.0));
            assert_eq!(font_lg(cx), ui(14.0));
            assert_eq!(font_xl(cx), ui(16.0));
            assert_eq!(font_title(cx), ui(18.0));

            assert_eq!(radius_sm(cx), px(2.0));
            assert_eq!(radius_md(cx), px(2.0));
            assert_eq!(radius_lg(cx), px(3.0));
        });
    }

    #[gpui::test]
    fn the_density_falls_back_to_default_when_global_absent(cx: &mut TestAppContext) {
        cx.update(|cx| {
            // Do not call init — global is absent.
            assert_eq!(active(cx), Density::Default);
            // Accessors should still return Default-tier values.
            assert_eq!(font_xs(cx), ui(12.0));
            assert_eq!(radius_sm(cx), px(0.0));
        });
    }

    #[gpui::test]
    fn set_updates_the_density_at_runtime(cx: &mut TestAppContext) {
        cx.update(|cx| {
            init(cx, Density::Default);
            assert_eq!(active(cx), Density::Default);

            set(cx, Density::Compact);
            assert_eq!(active(cx), Density::Compact);
            assert_eq!(font_xs(cx), ui(11.0));
        });
    }

    #[gpui::test]
    fn font_accessors_follow_the_rem_size_and_radii_do_not(cx: &mut TestAppContext) {
        cx.update(|cx| {
            init(cx, Density::Default);

            let doubled_rem = px(crate::tokens::BASE_REM * 2.0);
            assert_eq!(font_label(cx).to_pixels(doubled_rem), px(22.0));
            assert_eq!(font_sm(cx).to_pixels(doubled_rem), px(26.0));
            assert_eq!(font_title(cx).to_pixels(doubled_rem), px(40.0));
            assert_eq!(radius_sm(cx), px(0.0));

            set(cx, Density::Compact);
            assert_eq!(font_xs(cx).to_pixels(doubled_rem), px(22.0));
            assert_eq!(radius_lg(cx), px(3.0));
        });
    }
}
