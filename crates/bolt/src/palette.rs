//! The palette a product selects, and the one the roles resolve to.
//!
//! A product maps its own settings enum onto [`PaletteMode`] and publishes the result with
//! [`set_mode`]. Screens never read this module: they read roles, and every role that has a
//! light variant resolves through [`resolved_mode`].
//!
//! `System` is a request, not a palette: the product resolves it against the platform
//! appearance and publishes `Dark` or `Light` once it knows which one it installed.

use gpui::{App, Global};

/// The palette a product selected.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PaletteMode {
    /// The dark palette.
    #[default]
    Dark,
    /// The light palette.
    Light,
    /// Whatever the platform appearance is, until the product resolves it.
    System,
}

/// The palette the product published.
#[derive(Debug, Clone, Copy)]
struct ActivePalette(PaletteMode);

impl Global for ActivePalette {}

/// Publishes the palette the product installed.
pub fn set_mode(cx: &mut App, mode: PaletteMode) {
    cx.set_global(ActivePalette(mode));
}

/// The palette the product published, or [`PaletteMode::Dark`] before it publishes one.
pub fn mode(cx: &App) -> PaletteMode {
    cx.try_global::<ActivePalette>()
        .map(|active| active.0)
        .unwrap_or_default()
}

/// The palette a role with a light variant resolves to.
///
/// An unresolved [`PaletteMode::System`] counts as dark, so a role never has to fail.
pub fn resolved_mode(cx: &App) -> PaletteMode {
    match mode(cx) {
        PaletteMode::Light => PaletteMode::Light,
        PaletteMode::Dark | PaletteMode::System => PaletteMode::Dark,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::TestAppContext;

    #[gpui::test]
    fn the_palette_is_dark_before_a_product_installs_one(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert_eq!(mode(cx), PaletteMode::Dark);
            assert_eq!(resolved_mode(cx), PaletteMode::Dark);
        });
    }

    #[gpui::test]
    fn a_selection_survives_until_the_product_replaces_it(cx: &mut TestAppContext) {
        cx.update(|cx| {
            set_mode(cx, PaletteMode::Light);
            assert_eq!(mode(cx), PaletteMode::Light);
            assert_eq!(resolved_mode(cx), PaletteMode::Light);

            set_mode(cx, PaletteMode::Dark);
            assert_eq!(mode(cx), PaletteMode::Dark);
        });
    }

    #[gpui::test]
    fn follows_system_resolves_to_dark_until_the_product_maps_it(cx: &mut TestAppContext) {
        cx.update(|cx| {
            set_mode(cx, PaletteMode::System);
            assert_eq!(mode(cx), PaletteMode::System);
            assert_eq!(resolved_mode(cx), PaletteMode::Dark);
        });
    }
}
