//! Semantic color tokens for banners and data-grid row states.
//!
//! These tokens are hand-picked per-theme (Ayu Dark, Mirage, Light) to ensure
//! legibility across all three palettes. They are NOT derived at runtime from
//! `theme.*` opacity calculations — the hex values are embedded here.
//!
//! # Usage
//!
//! ```
//! use dbflux_components::semantic::{BannerColors, RowStateColors, ThemeSettingGlobal};
//! ```
//!
//! Register the current theme once during startup via `ThemeSettingGlobal::set`.
//! Then call `BannerColors::for_current(cx)` or `RowStateColors::for_current(cx)`
//! in any rendering context.

use dbflux_core::ThemeSetting;
use gpui::{App, Global, Hsla, hsla};
use gpui_component::ActiveTheme;

// ---------------------------------------------------------------------------
// Hex helpers
// ---------------------------------------------------------------------------

fn hex(r: u8, g: u8, b: u8, a: f32) -> Hsla {
    let rf = r as f32 / 255.0;
    let gf = g as f32 / 255.0;
    let bf = b as f32 / 255.0;

    let max = rf.max(gf).max(bf);
    let min = rf.min(gf).min(bf);
    let l = (max + min) / 2.0;

    if (max - min).abs() < f32::EPSILON {
        return hsla(0.0, 0.0, l, a);
    }

    let d = max - min;
    let s = if l > 0.5 {
        d / (2.0 - max - min)
    } else {
        d / (max + min)
    };

    let h = if (max - rf).abs() < f32::EPSILON {
        let mut h = (gf - bf) / d;
        if gf < bf {
            h += 6.0;
        }
        h
    } else if (max - gf).abs() < f32::EPSILON {
        (bf - rf) / d + 2.0
    } else {
        (rf - gf) / d + 4.0
    };

    hsla(h / 6.0, s, l, a)
}

fn from_hex(hex_value: u32, alpha: f32) -> Hsla {
    let r = ((hex_value >> 16) & 0xFF) as u8;
    let g = ((hex_value >> 8) & 0xFF) as u8;
    let b = (hex_value & 0xFF) as u8;
    hex(r, g, b, alpha)
}

// ---------------------------------------------------------------------------
// GPUI global — tracks the active ThemeSetting
// ---------------------------------------------------------------------------

/// GPUI global tracking the active `ThemeSetting`.
///
/// Register once during startup (after `theme::apply_theme`) by calling
/// `ThemeSettingGlobal::set(cx, setting)`. Semantic color accessors use it
/// to select the correct token values for the active palette.
#[derive(Debug, Clone, Copy)]
pub struct ThemeSettingGlobal {
    pub setting: ThemeSetting,
}

impl Global for ThemeSettingGlobal {}

impl ThemeSettingGlobal {
    /// Register (or update) the active `ThemeSetting` in the GPUI context.
    pub fn set(cx: &mut App, setting: ThemeSetting) {
        cx.set_global(ThemeSettingGlobal { setting });
    }

    /// Read the active `ThemeSetting`. Falls back to `ThemeSetting::Dark` when
    /// the global has not been registered.
    pub fn get(cx: &App) -> ThemeSetting {
        cx.try_global::<Self>()
            .map(|g| g.setting)
            .unwrap_or(ThemeSetting::Dark)
    }
}

// ---------------------------------------------------------------------------
// BannerColors
// ---------------------------------------------------------------------------

/// Semantic colors for informational banners (info, success, warning, error).
///
/// Each variant exposes a `background` (low-chroma tinted surface) and
/// `foreground` (high-contrast text/icon color) that are legible on top of it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BannerColors {
    /// Background and foreground for an informational banner.
    pub info_bg: Hsla,
    pub info_fg: Hsla,
    /// Background and foreground for a success banner.
    pub success_bg: Hsla,
    pub success_fg: Hsla,
    /// Background and foreground for a warning banner.
    pub warning_bg: Hsla,
    pub warning_fg: Hsla,
    /// Background and foreground for an error/danger banner.
    pub error_bg: Hsla,
    pub error_fg: Hsla,
}

impl BannerColors {
    /// Select tokens for the Ayu Dark palette.
    pub fn dark() -> Self {
        Self {
            // #59C2FF at 12% over dark background
            info_bg: from_hex(0x59C2FF, 0.12),
            info_fg: from_hex(0x59C2FF, 1.0),
            // #AAD94C at 12% over dark background
            success_bg: from_hex(0xAAD94C, 0.12),
            success_fg: from_hex(0xAAD94C, 1.0),
            // #FFB454 at 12% over dark background
            warning_bg: from_hex(0xFFB454, 0.12),
            warning_fg: from_hex(0xFFB454, 1.0),
            // #F07178 at 12% over dark background
            error_bg: from_hex(0xF07178, 0.12),
            error_fg: from_hex(0xF07178, 1.0),
        }
    }

    /// Select tokens for the Ayu Mirage palette.
    pub fn mirage() -> Self {
        Self {
            // #73D0FF at 14% over mirage background — slightly more opaque for contrast
            info_bg: from_hex(0x73D0FF, 0.14),
            info_fg: from_hex(0x73D0FF, 1.0),
            // #AAD94C at 14%
            success_bg: from_hex(0xAAD94C, 0.14),
            success_fg: from_hex(0xAAD94C, 1.0),
            // #FFCC66 at 14%
            warning_bg: from_hex(0xFFCC66, 0.14),
            warning_fg: from_hex(0xFFCC66, 1.0),
            // #F28779 at 14%
            error_bg: from_hex(0xF28779, 0.14),
            error_fg: from_hex(0xF28779, 1.0),
        }
    }

    /// Select tokens for the Ayu Light palette.
    pub fn light() -> Self {
        Self {
            // #399EE6 at 10% over light background — low saturation tint
            info_bg: from_hex(0x399EE6, 0.10),
            info_fg: from_hex(0x2A7BBF, 1.0),
            // #86B300 at 10%
            success_bg: from_hex(0x86B300, 0.10),
            success_fg: from_hex(0x6A8F00, 1.0),
            // #F2AE49 at 10%
            warning_bg: from_hex(0xF2AE49, 0.10),
            warning_fg: from_hex(0xC07800, 1.0),
            // #E65050 at 10%
            error_bg: from_hex(0xE65050, 0.10),
            error_fg: from_hex(0xBF3030, 1.0),
        }
    }

    /// Return the `BannerColors` that reproduce exactly what the former
    /// `tokens::BannerColors` produced for all 9 call-sites.
    ///
    /// - `info`, `success`, `error`: theme-agnostic fixed hex values taken
    ///   verbatim from the former `tokens::BannerColors` implementation.
    /// - `warning`: derived from `theme.primary` at runtime exactly as the
    ///   former implementation did (bg = primary @ 0.20 alpha,
    ///   fg = primary @ 1.0 alpha).
    ///
    /// The named constructors `dark()`, `mirage()`, and `light()` carry
    /// per-palette semantic values intended for future use. Call sites that
    /// need pixel-exact backwards compatibility MUST call this method instead.
    pub fn for_current(cx: &App) -> Self {
        let theme = cx.theme();
        let mut warning_bg = theme.primary;
        warning_bg.a = 0.20;
        let mut warning_fg = theme.primary;
        warning_fg.a = 1.0;

        Self {
            // #1E3A5F / #93C5FD — former tokens::BannerColors::info_*
            info_bg: from_hex(0x1E3A5F, 1.0),
            info_fg: from_hex(0x93C5FD, 1.0),
            // #14532D / #86EFAC — former tokens::BannerColors::success_*
            success_bg: from_hex(0x14532D, 1.0),
            success_fg: from_hex(0x86EFAC, 1.0),
            // theme.primary @ 0.20 / 1.0 — former tokens::BannerColors::warning_*
            warning_bg,
            warning_fg,
            // #7F1D1D / #FCA5A5 — former tokens::BannerColors::danger_*
            error_bg: from_hex(0x7F1D1D, 1.0),
            error_fg: from_hex(0xFCA5A5, 1.0),
        }
    }
}

// ---------------------------------------------------------------------------
// RowStateColors
// ---------------------------------------------------------------------------

/// Semantic background tints for data-grid row states.
///
/// All values are semi-transparent so they blend with alternating row stripes.
/// `dirty` is `None` — dirty state is indicated at the cell level only.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RowStateColors {
    /// Dirty rows: `None` — use cell-level indicators instead of row background.
    pub dirty: Option<Hsla>,
    /// Row currently being saved (optimistic, transient).
    pub saving: Hsla,
    /// Row whose last save attempt failed.
    pub error: Hsla,
    /// New row pending INSERT.
    pub pending_insert: Hsla,
    /// Row marked for DELETE.
    pub pending_delete: Hsla,
}

impl RowStateColors {
    /// Row state tokens for the Ayu Dark palette.
    pub fn dark() -> Self {
        Self {
            dirty: None,
            saving: from_hex(0xFFB454, 0.10),
            error: from_hex(0xF07178, 0.15),
            pending_insert: from_hex(0xAAD94C, 0.15),
            pending_delete: from_hex(0xF07178, 0.10),
        }
    }

    /// Row state tokens for the Ayu Mirage palette.
    pub fn mirage() -> Self {
        Self {
            dirty: None,
            saving: from_hex(0xFFCC66, 0.12),
            error: from_hex(0xF28779, 0.16),
            pending_insert: from_hex(0xAAD94C, 0.16),
            pending_delete: from_hex(0xF28779, 0.12),
        }
    }

    /// Row state tokens for the Ayu Light palette.
    pub fn light() -> Self {
        Self {
            dirty: None,
            saving: from_hex(0xF2AE49, 0.14),
            error: from_hex(0xE65050, 0.14),
            pending_insert: from_hex(0x86B300, 0.14),
            pending_delete: from_hex(0xE65050, 0.12),
        }
    }

    /// Return the `RowStateColors` for the currently active theme.
    ///
    /// Reads `ThemeSettingGlobal` from `cx`; falls back to Dark when absent.
    pub fn for_current(cx: &App) -> Self {
        match ThemeSettingGlobal::get(cx) {
            ThemeSetting::Dark => Self::dark(),
            ThemeSetting::Mirage => Self::mirage(),
            ThemeSetting::Light => Self::light(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::ThemeSetting;
    use gpui::TestAppContext;

    #[gpui::test]
    fn theme_setting_global_falls_back_to_dark_when_absent(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Dark);
        });
    }

    #[gpui::test]
    fn theme_setting_global_roundtrips_all_variants(cx: &mut TestAppContext) {
        cx.update(|cx| {
            ThemeSettingGlobal::set(cx, ThemeSetting::Mirage);
            assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Mirage);

            ThemeSettingGlobal::set(cx, ThemeSetting::Light);
            assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Light);

            ThemeSettingGlobal::set(cx, ThemeSetting::Dark);
            assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Dark);
        });
    }

    /// `for_current` returns the former `tokens::BannerColors` fixed values
    /// for info/success/error across all themes, and derives warning from
    /// `theme.primary`. The per-palette constructors (`dark`, `mirage`, `light`)
    /// are distinct and carry per-theme semantic values for future use.
    #[gpui::test]
    fn banner_colors_for_current_returns_legacy_pixel_exact_values(cx: &mut TestAppContext) {
        // gpui_component::init registers the Theme global required by cx.theme().
        cx.update(gpui_component::init);
        cx.update(|cx| {
            // info/success/error are theme-agnostic (same across all themes).
            ThemeSettingGlobal::set(cx, ThemeSetting::Dark);
            let colors_dark = BannerColors::for_current(cx);
            ThemeSettingGlobal::set(cx, ThemeSetting::Mirage);
            let colors_mirage = BannerColors::for_current(cx);
            ThemeSettingGlobal::set(cx, ThemeSetting::Light);
            let colors_light = BannerColors::for_current(cx);

            // Fixed hex values taken from former tokens::BannerColors.
            // info_bg = #1E3A5F at full opacity.
            assert_eq!(colors_dark.info_bg, colors_mirage.info_bg);
            assert_eq!(colors_dark.info_bg, colors_light.info_bg);
            assert_eq!(colors_dark.info_fg, colors_mirage.info_fg);

            // success_bg = #14532D at full opacity.
            assert_eq!(colors_dark.success_bg, colors_mirage.success_bg);
            assert_eq!(colors_dark.success_fg, colors_mirage.success_fg);

            // error_bg = #7F1D1D at full opacity.
            assert_eq!(colors_dark.error_bg, colors_mirage.error_bg);
            assert_eq!(colors_dark.error_fg, colors_mirage.error_fg);

            // All fg colors must be fully opaque.
            assert_eq!(colors_dark.info_fg.a, 1.0);
            assert_eq!(colors_dark.success_fg.a, 1.0);
            assert_eq!(colors_dark.error_fg.a, 1.0);
        });
    }

    #[gpui::test]
    fn banner_colors_for_current_warning_derives_from_theme_primary(cx: &mut TestAppContext) {
        // gpui_component::init registers the Theme global required by cx.theme().
        cx.update(gpui_component::init);
        cx.update(|cx| {
            // warning_bg = theme.primary @ 0.20, warning_fg = theme.primary @ 1.0.
            let colors = BannerColors::for_current(cx);
            assert!((colors.warning_bg.a - 0.20).abs() < 0.001);
            assert!((colors.warning_fg.a - 1.0).abs() < 0.001);
        });
    }

    #[gpui::test]
    fn row_state_colors_dirty_is_none_in_all_themes(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert!(RowStateColors::dark().dirty.is_none());
            assert!(RowStateColors::mirage().dirty.is_none());
            assert!(RowStateColors::light().dirty.is_none());

            // for_current also respects fallback
            assert!(RowStateColors::for_current(cx).dirty.is_none());
        });
    }

    #[gpui::test]
    fn row_state_colors_for_current_dispatches_to_correct_theme(cx: &mut TestAppContext) {
        cx.update(|cx| {
            ThemeSettingGlobal::set(cx, ThemeSetting::Mirage);
            let mirage = RowStateColors::for_current(cx);
            assert_eq!(mirage.saving.a, 0.12);

            ThemeSettingGlobal::set(cx, ThemeSetting::Light);
            let light = RowStateColors::for_current(cx);
            assert!(light.pending_insert.a > 0.0);
        });
    }

    #[test]
    fn banner_colors_info_fg_is_fully_opaque_in_dark_theme() {
        assert_eq!(BannerColors::dark().info_fg.a, 1.0);
        assert_eq!(BannerColors::mirage().info_fg.a, 1.0);
        assert_eq!(BannerColors::light().info_fg.a, 1.0);
    }
}
