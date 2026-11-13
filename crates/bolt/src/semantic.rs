//! Semantic color tokens for banners, data-grid row states, and chart chrome.
//!
//! These tokens are hand-picked per palette variant (Dark, Light). They are
//! NOT derived at runtime from theme opacity calculations — the hex values are
//! embedded here.
//!
//! # Usage
//!
//! ```
//! use bolt::semantic::{BannerColors, RowStateColors};
//! ```
//!
//! A product publishes the palette it installed through
//! [`crate::palette::set_mode`]; `theme::apply_theme` does this on every call.
//! Call [`BannerColors::for_current`], [`RowStateColors::for_current`], or
//! [`ChartColors::for_current`] in any rendering context.

use gpui::{App, Hsla, hsla};

use crate::palette::{self, PaletteMode};

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
    /// Banner tokens for the dark palette: each semantic color over a 12%
    /// wash of itself.
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

    /// Banner tokens for the light palette: each semantic color over a 14%
    /// wash of itself.
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

    /// Return the `BannerColors` for the currently active palette.
    ///
    /// Reads the published palette from `cx`; falls back to Dark when absent.
    pub fn for_current(cx: &App) -> Self {
        match palette::resolved_mode(cx) {
            PaletteMode::Light => Self::light(),
            PaletteMode::Dark | PaletteMode::System => Self::dark(),
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
    /// Row state tokens for the dark palette.
    pub fn dark() -> Self {
        Self {
            dirty: None,
            saving: from_hex(0xFFC23D, 0.10),
            error: from_hex(0xFF6B5E, 0.15),
            pending_insert: from_hex(0x7BE0A0, 0.15),
            pending_delete: from_hex(0xFF6B5E, 0.10),
        }
    }

    /// Row state tokens for the light palette.
    pub fn light() -> Self {
        Self {
            dirty: None,
            saving: from_hex(0xB7791F, 0.14),
            error: from_hex(0xC7362B, 0.14),
            pending_insert: from_hex(0x1C7F45, 0.14),
            pending_delete: from_hex(0xC7362B, 0.12),
        }
    }

    /// Return the `RowStateColors` for the currently active palette.
    ///
    /// Reads the published palette from `cx`; falls back to Dark when absent.
    pub fn for_current(cx: &App) -> Self {
        match palette::resolved_mode(cx) {
            PaletteMode::Light => Self::light(),
            PaletteMode::Dark | PaletteMode::System => Self::dark(),
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
/// derive from the active theme at runtime so the struct can be constructed
/// without a live render context (e.g., in unit tests and `for_current`
/// dispatch).
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
    /// Chart chrome tokens for the dark palette.
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
            checkbox_checked: from_hex(0x8E8996, 1.0),
            stats_accent: from_hex(0x6FD3D8, 1.0),
        }
    }

    /// Chart chrome tokens for the light palette.
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
            checkbox_checked: from_hex(0x6B6572, 1.0),
            stats_accent: from_hex(0x0F7C82, 1.0),
        }
    }

    /// Return the `ChartColors` for the currently active palette and brand.
    ///
    /// Reads the published palette and brand from `cx`; falls back to the dark
    /// variant with a neutral accent when absent.
    pub fn for_current(cx: &App) -> Self {
        let colors = match palette::resolved_mode(cx) {
            PaletteMode::Light => Self::light(),
            PaletteMode::Dark | PaletteMode::System => Self::dark(),
        };
        let brand = crate::brand::brand(cx);
        let accent = match palette::resolved_mode(cx) {
            PaletteMode::Light => brand.accent,
            PaletteMode::Dark | PaletteMode::System => brand.accent_tint,
        };

        colors.with_accent(accent)
    }

    /// These roles with the accent-shaped ones replaced by the product accent.
    pub fn with_accent(mut self, accent: Hsla) -> Self {
        self.checkbox_checked = accent;
        self
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
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
    fn banner_colors_for_current_dispatches_to_the_published_palette(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert_eq!(BannerColors::for_current(cx), BannerColors::dark());

            palette::set_mode(cx, PaletteMode::Light);
            assert_eq!(BannerColors::for_current(cx), BannerColors::light());

            palette::set_mode(cx, PaletteMode::Dark);
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
    fn row_state_colors_dirty_is_none_in_all_palettes(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert!(RowStateColors::dark().dirty.is_none());
            assert!(RowStateColors::light().dirty.is_none());

            // for_current also respects fallback
            assert!(RowStateColors::for_current(cx).dirty.is_none());
        });
    }

    #[gpui::test]
    fn row_state_colors_for_current_dispatches_to_the_published_palette(cx: &mut TestAppContext) {
        cx.update(|cx| {
            palette::set_mode(cx, PaletteMode::Light);
            assert_eq!(RowStateColors::for_current(cx), RowStateColors::light());

            palette::set_mode(cx, PaletteMode::Dark);
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

    /// `for_current` must dispatch to the matching constructor for each palette
    /// variant and take the accent of the installed brand.
    #[gpui::test]
    fn chart_colors_for_current_follow_the_published_palette_and_brand(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let brand = crate::brand::Brand {
                name: "Test".into(),
                accent: from_hex(0x102030, 1.0),
                accent_hover: from_hex(0x203040, 1.0),
                accent_pressed: from_hex(0x304050, 1.0),
                accent_tint: from_hex(0x405060, 1.0),
                on_accent: from_hex(0xFFFFFF, 1.0),
            };
            crate::brand::install(brand.clone(), cx);

            palette::set_mode(cx, PaletteMode::Dark);
            assert_eq!(
                ChartColors::for_current(cx),
                ChartColors::dark().with_accent(brand.accent_tint)
            );

            palette::set_mode(cx, PaletteMode::Light);
            assert_eq!(
                ChartColors::for_current(cx),
                ChartColors::light().with_accent(brand.accent)
            );
        });
    }
}
