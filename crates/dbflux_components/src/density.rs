//! Density system — layout style and cx-based token accessors.
//!
//! Call `density::init(cx, style)` once during app startup (after theme init)
//! to register the current `AppStyle`. The accessor functions (`font_xs`,
//! `font_sm`, …, `radius_sm`, …) read the registered style and return the
//! appropriate `Pixels` value for the active density tier.
//!
//! Two tiers are supported:
//!
//! | Accessor           | `Default`  | `Compact`  |
//! |--------------------|------------|------------|
//! | `font_xs`          | 12 px      | 11 px      |
//! | `font_sm`          | 13 px      | 12 px      |
//! | `font_base`        | 14 px      | 13 px      |
//! | `font_lg`          | 15 px      | 14 px      |
//! | `font_xl`          | 18 px      | 16 px      |
//! | `font_title`       | 20 px      | 18 px      |
//! | `radius_sm`        |  0 px      |  2 px      |
//! | `radius_md`        |  0 px      |  2 px      |
//! | `radius_lg`        |  0 px      |  3 px      |

use dbflux_core::AppStyle;
use gpui::{App, Global, Pixels, px};

/// GPUI global that stores the active `AppStyle`.
///
/// Registered by `density::init`. Accessors fall back to `AppStyle::Default`
/// when the global is absent (e.g., in unit tests that skip full app init).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DensityGlobal {
    pub style: AppStyle,
}

impl Global for DensityGlobal {}

/// Register the density global. Call this once during app startup,
/// after `theme::init(cx)` and before any rendering occurs.
pub fn init(cx: &mut App, style: AppStyle) {
    cx.set_global(DensityGlobal { style });
}

/// Update the density global when the user changes the style setting at runtime.
pub fn set_style(cx: &mut App, style: AppStyle) {
    cx.set_global(DensityGlobal { style });
}

/// Read the active `AppStyle` from the context.
///
/// Falls back to `AppStyle::Default` when the global has not been initialized.
pub fn active_style(cx: &App) -> AppStyle {
    cx.try_global::<DensityGlobal>()
        .map(|g| g.style)
        .unwrap_or(AppStyle::Default)
}

// ---------------------------------------------------------------------------
// Font-size accessors
// ---------------------------------------------------------------------------

/// Extra-small font: 12 px (Default) / 11 px (Compact).
pub fn font_xs(cx: &App) -> Pixels {
    match active_style(cx) {
        AppStyle::Default => px(12.0),
        AppStyle::Compact => px(11.0),
    }
}

/// Small font: 13 px (Default) / 12 px (Compact).
pub fn font_sm(cx: &App) -> Pixels {
    match active_style(cx) {
        AppStyle::Default => px(13.0),
        AppStyle::Compact => px(12.0),
    }
}

/// Base font: 14 px (Default) / 13 px (Compact).
pub fn font_base(cx: &App) -> Pixels {
    match active_style(cx) {
        AppStyle::Default => px(14.0),
        AppStyle::Compact => px(13.0),
    }
}

/// Large font: 15 px (Default) / 14 px (Compact).
pub fn font_lg(cx: &App) -> Pixels {
    match active_style(cx) {
        AppStyle::Default => px(15.0),
        AppStyle::Compact => px(14.0),
    }
}

/// Extra-large font: 18 px (Default) / 16 px (Compact).
pub fn font_xl(cx: &App) -> Pixels {
    match active_style(cx) {
        AppStyle::Default => px(18.0),
        AppStyle::Compact => px(16.0),
    }
}

/// Title font: 20 px (Default) / 18 px (Compact).
pub fn font_title(cx: &App) -> Pixels {
    match active_style(cx) {
        AppStyle::Default => px(20.0),
        AppStyle::Compact => px(18.0),
    }
}

// ---------------------------------------------------------------------------
// Border-radius accessors
// ---------------------------------------------------------------------------

/// Small radius: 0 px (Default) / 2 px (Compact).
pub fn radius_sm(cx: &App) -> Pixels {
    match active_style(cx) {
        AppStyle::Default => px(0.0),
        AppStyle::Compact => px(2.0),
    }
}

/// Medium radius: 0 px (Default) / 2 px (Compact).
pub fn radius_md(cx: &App) -> Pixels {
    match active_style(cx) {
        AppStyle::Default => px(0.0),
        AppStyle::Compact => px(2.0),
    }
}

/// Large radius: 0 px (Default) / 3 px (Compact).
pub fn radius_lg(cx: &App) -> Pixels {
    match active_style(cx) {
        AppStyle::Default => px(0.0),
        AppStyle::Compact => px(3.0),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::AppStyle;
    use gpui::{TestAppContext, px};

    #[gpui::test]
    fn default_style_yields_legacy_font_and_square_radius(cx: &mut TestAppContext) {
        cx.update(|cx| {
            init(cx, AppStyle::Default);

            assert_eq!(font_xs(cx), px(12.0));
            assert_eq!(font_sm(cx), px(13.0));
            assert_eq!(font_base(cx), px(14.0));
            assert_eq!(font_lg(cx), px(15.0));
            assert_eq!(font_xl(cx), px(18.0));
            assert_eq!(font_title(cx), px(20.0));

            assert_eq!(radius_sm(cx), px(0.0));
            assert_eq!(radius_md(cx), px(0.0));
            assert_eq!(radius_lg(cx), px(0.0));
        });
    }

    #[gpui::test]
    fn compact_style_yields_ds_font_and_rounded_radius(cx: &mut TestAppContext) {
        cx.update(|cx| {
            init(cx, AppStyle::Compact);

            assert_eq!(font_xs(cx), px(11.0));
            assert_eq!(font_sm(cx), px(12.0));
            assert_eq!(font_base(cx), px(13.0));
            assert_eq!(font_lg(cx), px(14.0));
            assert_eq!(font_xl(cx), px(16.0));
            assert_eq!(font_title(cx), px(18.0));

            assert_eq!(radius_sm(cx), px(2.0));
            assert_eq!(radius_md(cx), px(2.0));
            assert_eq!(radius_lg(cx), px(3.0));
        });
    }

    #[gpui::test]
    fn active_style_falls_back_to_default_when_global_absent(cx: &mut TestAppContext) {
        cx.update(|cx| {
            // Do not call init — global is absent.
            assert_eq!(active_style(cx), AppStyle::Default);
            // Accessors should still return Default-tier values.
            assert_eq!(font_xs(cx), px(12.0));
            assert_eq!(radius_sm(cx), px(0.0));
        });
    }

    #[gpui::test]
    fn set_style_updates_active_style_at_runtime(cx: &mut TestAppContext) {
        cx.update(|cx| {
            init(cx, AppStyle::Default);
            assert_eq!(active_style(cx), AppStyle::Default);

            set_style(cx, AppStyle::Compact);
            assert_eq!(active_style(cx), AppStyle::Compact);
            assert_eq!(font_xs(cx), px(11.0));
        });
    }
}
