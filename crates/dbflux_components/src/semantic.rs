//! Semantic color tokens for banners, data-grid row states, and chart chrome.
//!
//! These tokens are hand-picked per Bolt Byzantium variant (Dark, Light) from
//! the design board. They are NOT derived at runtime from `theme.*` opacity
//! calculations — the hex values are embedded here.
//!
//! # Usage
//!
//! ```
//! use dbflux_components::semantic::{BannerColors, RowStateColors, ThemeSettingGlobal};
//! ```
//!
//! `theme::apply_theme` registers the resolved variant through
//! `ThemeSettingGlobal::set`. Call `BannerColors::for_current(cx)`,
//! `RowStateColors::for_current(cx)`, or `ChartColors::for_current(cx)` in any
//! rendering context.

use dbflux_core::ThemeSetting;
use gpui::{App, Global, Hsla, hsla};

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

/// GPUI global tracking the palette variant currently on screen.
///
/// `theme::apply_theme` stores the resolved variant (`Dark` or `Light`, never
/// `System`) on every call. Semantic color accessors use it to select the
/// token values that match the active palette.
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
    /// Banner tokens for the Bolt Byzantium dark palette: each semantic color
    /// over a 12% wash of itself.
    pub fn dark() -> Self {
        Self {
            info_bg: from_hex(0x6EA8FF, 0.12),
            info_fg: from_hex(0x6EA8FF, 1.0),
            success_bg: from_hex(0x7BE0A0, 0.12),
            success_fg: from_hex(0x7BE0A0, 1.0),
            warning_bg: from_hex(0xFFC23D, 0.12),
            warning_fg: from_hex(0xFFC23D, 1.0),
            error_bg: from_hex(0xFF6B5E, 0.12),
            error_fg: from_hex(0xFF6B5E, 1.0),
        }
    }

    /// Banner tokens for the Bolt Byzantium light palette: each semantic color
    /// over a 14% wash of itself.
    pub fn light() -> Self {
        Self {
            info_bg: from_hex(0x1F5FD1, 0.14),
            info_fg: from_hex(0x1F5FD1, 1.0),
            success_bg: from_hex(0x1C7F45, 0.14),
            success_fg: from_hex(0x1C7F45, 1.0),
            warning_bg: from_hex(0xB7791F, 0.14),
            warning_fg: from_hex(0xB7791F, 1.0),
            error_bg: from_hex(0xC7362B, 0.14),
            error_fg: from_hex(0xC7362B, 1.0),
        }
    }

    /// Return the `BannerColors` for the currently active theme.
    ///
    /// Reads `ThemeSettingGlobal` from `cx`; falls back to Dark when absent.
    pub fn for_current(cx: &App) -> Self {
        match ThemeSettingGlobal::get(cx) {
            ThemeSetting::Light => Self::light(),
            ThemeSetting::Dark | ThemeSetting::System => Self::dark(),
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
    /// Row state tokens for the Bolt Byzantium dark palette.
    pub fn dark() -> Self {
        Self {
            dirty: None,
            saving: from_hex(0xFFC23D, 0.10),
            error: from_hex(0xFF6B5E, 0.15),
            pending_insert: from_hex(0x7BE0A0, 0.15),
            pending_delete: from_hex(0xFF6B5E, 0.10),
        }
    }

    /// Row state tokens for the Bolt Byzantium light palette.
    pub fn light() -> Self {
        Self {
            dirty: None,
            saving: from_hex(0xB7791F, 0.14),
            error: from_hex(0xC7362B, 0.14),
            pending_insert: from_hex(0x1C7F45, 0.14),
            pending_delete: from_hex(0xC7362B, 0.12),
        }
    }

    /// Return the `RowStateColors` for the currently active theme.
    ///
    /// Reads `ThemeSettingGlobal` from `cx`; falls back to Dark when absent.
    pub fn for_current(cx: &App) -> Self {
        match ThemeSettingGlobal::get(cx) {
            ThemeSetting::Light => Self::light(),
            ThemeSetting::Dark | ThemeSetting::System => Self::dark(),
        }
    }
}

// ---------------------------------------------------------------------------
// ChartColors
// ---------------------------------------------------------------------------

/// Semantic colors for chart chrome: inspector overlays, axis-bar pills,
/// legend, and stats dock.
///
/// All values are self-contained per-variant hex literals — they do NOT
/// derive from `cx.theme()` at runtime so the struct can be constructed without
/// a live render context (e.g., in unit tests and `for_current` dispatch).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChartColors {
    /// Panel / overlay background (inspector, readout overlay).
    pub panel_bg: Hsla,
    /// Panel / overlay border.
    pub panel_border: Hsla,
    /// Label text (muted descriptors, axis tick labels).
    pub label_fg: Hsla,
    /// Primary value text (high-contrast numbers and identifiers).
    pub value_fg: Hsla,
    /// Secondary / muted text (counters, de-emphasised stats).
    pub muted_fg: Hsla,
    /// Row / item hover background.
    pub hover_bg: Hsla,
    /// Pill / chip background (axis-bar column pills).
    pub pill_bg: Hsla,
    /// Pill / chip border.
    pub pill_border: Hsla,
    /// Checkbox checked fill (axis-bar toggle).
    pub checkbox_checked: Hsla,
    /// Stats accent — cyan-family highlight for the stats dock value.
    pub stats_accent: Hsla,
}

impl ChartColors {
    /// Chart chrome tokens for the Bolt Byzantium dark palette.
    pub fn dark() -> Self {
        Self {
            panel_bg: from_hex(0x100F13, 1.0),
            panel_border: from_hex(0x232128, 1.0),
            label_fg: from_hex(0x8E8996, 1.0),
            value_fg: from_hex(0xF7F4F7, 1.0),
            muted_fg: from_hex(0x8E8996, 1.0),
            hover_bg: from_hex(0xFFFFFF, 0.04),
            pill_bg: from_hex(0x1A181E, 1.0),
            pill_border: from_hex(0x37333D, 1.0),
            checkbox_checked: from_hex(0xD48CC8, 1.0),
            stats_accent: from_hex(0x6FD3D8, 1.0),
        }
    }

    /// Chart chrome tokens for the Bolt Byzantium light palette.
    pub fn light() -> Self {
        Self {
            panel_bg: from_hex(0xFFFFFF, 1.0),
            panel_border: from_hex(0xE3DEE6, 1.0),
            label_fg: from_hex(0x6B6572, 1.0),
            value_fg: from_hex(0x141118, 1.0),
            muted_fg: from_hex(0x6B6572, 1.0),
            hover_bg: from_hex(0x141118, 0.04),
            pill_bg: from_hex(0xEEEAF0, 1.0),
            pill_border: from_hex(0xCBC4D1, 1.0),
            checkbox_checked: from_hex(0x702963, 1.0),
            stats_accent: from_hex(0x0F7C82, 1.0),
        }
    }

    /// Return the `ChartColors` for the currently active theme.
    ///
    /// Reads `ThemeSettingGlobal` from `cx`; falls back to Dark when absent.
    pub fn for_current(cx: &App) -> Self {
        match ThemeSettingGlobal::get(cx) {
            ThemeSetting::Light => Self::light(),
            ThemeSetting::Dark | ThemeSetting::System => Self::dark(),
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

    fn assert_all_chart_fields_populated(colors: ChartColors) {
        assert!(colors.panel_bg.a > 0.0);
        assert!(colors.panel_border.a > 0.0);
        assert!(colors.label_fg.a > 0.0);
        assert!(colors.value_fg.a > 0.0);
        assert!(colors.muted_fg.a > 0.0);
        assert!(colors.hover_bg.a > 0.0);
        assert!(colors.pill_bg.a > 0.0);
        assert!(colors.pill_border.a > 0.0);
        assert!(colors.checkbox_checked.a > 0.0);
        assert!(colors.stats_accent.a > 0.0);
    }

    #[gpui::test]
    fn theme_setting_global_falls_back_to_dark_when_absent(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Dark);
        });
    }

    #[gpui::test]
    fn theme_setting_global_roundtrips_all_variants(cx: &mut TestAppContext) {
        cx.update(|cx| {
            ThemeSettingGlobal::set(cx, ThemeSetting::Light);
            assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Light);

            ThemeSettingGlobal::set(cx, ThemeSetting::Dark);
            assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Dark);
        });
    }

    #[gpui::test]
    fn banner_colors_for_current_dispatches_to_the_active_variant(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert_eq!(BannerColors::for_current(cx), BannerColors::dark());

            ThemeSettingGlobal::set(cx, ThemeSetting::Light);
            assert_eq!(BannerColors::for_current(cx), BannerColors::light());

            ThemeSettingGlobal::set(cx, ThemeSetting::Dark);
            assert_eq!(BannerColors::for_current(cx), BannerColors::dark());
        });
    }

    /// Banners use the semantic color as text over a soft wash of itself:
    /// 12% on dark surfaces, 14% on light ones.
    #[test]
    fn banner_colors_pair_semantic_foregrounds_with_soft_washes() {
        for (colors, wash) in [(BannerColors::dark(), 0.12), (BannerColors::light(), 0.14)] {
            for (background, foreground) in [
                (colors.info_bg, colors.info_fg),
                (colors.success_bg, colors.success_fg),
                (colors.warning_bg, colors.warning_fg),
                (colors.error_bg, colors.error_fg),
            ] {
                assert_eq!(foreground.a, 1.0);
                assert!((background.a - wash).abs() < 0.001);
                assert_eq!(
                    Hsla {
                        a: 1.0,
                        ..background
                    },
                    foreground
                );
            }
        }

        assert_eq!(BannerColors::dark().error_fg, from_hex(0xFF6B5E, 1.0));
        assert_eq!(BannerColors::light().error_fg, from_hex(0xC7362B, 1.0));
    }

    #[gpui::test]
    fn row_state_colors_dirty_is_none_in_all_themes(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert!(RowStateColors::dark().dirty.is_none());
            assert!(RowStateColors::light().dirty.is_none());

            // for_current also respects fallback
            assert!(RowStateColors::for_current(cx).dirty.is_none());
        });
    }

    #[gpui::test]
    fn row_state_colors_for_current_dispatches_to_correct_theme(cx: &mut TestAppContext) {
        cx.update(|cx| {
            ThemeSettingGlobal::set(cx, ThemeSetting::Light);
            assert_eq!(RowStateColors::for_current(cx), RowStateColors::light());

            ThemeSettingGlobal::set(cx, ThemeSetting::Dark);
            let dark = RowStateColors::for_current(cx);
            assert_eq!(dark, RowStateColors::dark());
            assert_eq!(dark.saving, from_hex(0xFFC23D, 0.10));
        });
    }

    #[test]
    fn chart_colors_dark_all_fields_populated() {
        assert_all_chart_fields_populated(ChartColors::dark());
    }

    #[test]
    fn chart_colors_light_all_fields_populated() {
        assert_all_chart_fields_populated(ChartColors::light());
    }

    /// `for_current` must dispatch to the matching constructor for each theme.
    #[gpui::test]
    fn chart_colors_for_current_dispatches_to_correct_variant(cx: &mut TestAppContext) {
        cx.update(|cx| {
            ThemeSettingGlobal::set(cx, ThemeSetting::Dark);
            assert_eq!(ChartColors::for_current(cx), ChartColors::dark());

            ThemeSettingGlobal::set(cx, ThemeSetting::Light);
            assert_eq!(ChartColors::for_current(cx), ChartColors::light());
        });
    }
}
