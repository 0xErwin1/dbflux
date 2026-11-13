//! The brand a product installs, and the washes derived from it.
//!
//! A product's accent is data, never a constant: the product builds a [`Brand`]
//! with its own colors and installs it at startup. Surfaces read the roles
//! through [`brand`] and [`colors`] — never a hardcoded hue — so the same crate
//! serves every product in the family. Before a product installs one, the
//! accessors return a neutral stand-in so nothing has to fail.

use gpui::{App, Global, Hsla, SharedString, hsla};

use crate::palette::{PaletteMode, resolved_mode};

/// The washes a surface paints for selection and drag-and-drop feedback.
///
/// Derived from the installed [`Brand`] and the resolved palette mode: the
/// wash color is the accent tint on dark surfaces and the accent itself on
/// light ones.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AccentWashes {
    /// Background of a selected row in a table or a flat list.
    pub selected_row: Hsla,
    /// Background of a selected item in a sidebar or a tree.
    pub selected_item: Hsla,
    /// Highlight behind selected text.
    pub text_selection: Hsla,
    /// Highlight of the surface under an active drag.
    pub drop_target: Hsla,
}

/// Share of the wash color behind a selected row.
const SELECTED_ROW_ALPHA: f32 = 0.07;
/// Share behind a selected item on dark surfaces.
const SELECTED_ITEM_ALPHA_DARK: f32 = 0.12;
/// Share behind a selected item on light surfaces, where the accent reads
/// stronger than its tint.
const SELECTED_ITEM_ALPHA_LIGHT: f32 = 0.14;
/// Share behind selected text.
const TEXT_SELECTION_ALPHA: f32 = 0.25;
/// Share behind the drop target.
const DROP_TARGET_ALPHA: f32 = 0.10;

impl AccentWashes {
    /// Derives the washes `brand` paints on `mode`.
    ///
    /// An unresolved [`PaletteMode::System`] counts as dark, like every role
    /// with a light variant.
    pub fn for_brand(brand: &Brand, mode: PaletteMode) -> Self {
        match mode {
            PaletteMode::Light => {
                let accent = brand.accent;
                Self {
                    selected_row: with_alpha(accent, SELECTED_ROW_ALPHA),
                    selected_item: with_alpha(accent, SELECTED_ITEM_ALPHA_LIGHT),
                    text_selection: with_alpha(accent, TEXT_SELECTION_ALPHA),
                    drop_target: with_alpha(accent, DROP_TARGET_ALPHA),
                }
            }
            PaletteMode::Dark | PaletteMode::System => {
                let tint = brand.accent_tint;
                Self {
                    selected_row: with_alpha(tint, SELECTED_ROW_ALPHA),
                    selected_item: with_alpha(tint, SELECTED_ITEM_ALPHA_DARK),
                    text_selection: with_alpha(tint, TEXT_SELECTION_ALPHA),
                    drop_target: with_alpha(tint, DROP_TARGET_ALPHA),
                }
            }
        }
    }
}

/// The name and accent roles a product installs.
///
/// The accent is the product's identity: every theme surface derives its
/// primary, focus-ring and selection colors from these fields, so two products
/// share one theme engine and differ only in the `Brand` they install.
#[derive(Clone, Debug, PartialEq)]
pub struct Brand {
    /// The product's display name, for surfaces that label themselves.
    pub name: SharedString,
    /// The solid fill of a primary control.
    pub accent: Hsla,
    /// The accent fill while the pointer holds it down.
    pub accent_hover: Hsla,
    /// The accent fill in its active (pressed) state.
    pub accent_pressed: Hsla,
    /// The lighter text-accent tone used on dark surfaces; on light surfaces
    /// the accent itself takes this role.
    pub accent_tint: Hsla,
    /// Text drawn on top of a solid accent fill.
    pub on_accent: Hsla,
}

impl Brand {
    /// The neutral stand-in returned before a product installs its brand.
    ///
    /// A grayscale accent at mid lightness: unusable as an identity, good
    /// enough that a surface rendering before startup finishes still paints.
    pub fn neutral() -> Self {
        Self {
            name: SharedString::from("Bolt"),
            accent: hsla(0.0, 0.0, 0.45, 1.0),
            accent_hover: hsla(0.0, 0.0, 0.52, 1.0),
            accent_pressed: hsla(0.0, 0.0, 0.38, 1.0),
            accent_tint: hsla(0.0, 0.0, 0.62, 1.0),
            on_accent: hsla(0.0, 0.0, 1.0, 1.0),
        }
    }
}

/// GPUI global holding the installed [`Brand`].
#[derive(Debug)]
struct InstalledBrand(Brand);

impl Global for InstalledBrand {}

/// Installs `brand` as the product's identity. Replaces any earlier brand.
pub fn install(brand: Brand, cx: &mut App) {
    cx.set_global(InstalledBrand(brand));
}

/// The installed brand, or [`Brand::neutral`] before a product installs one.
pub fn brand(cx: &App) -> Brand {
    cx.try_global::<InstalledBrand>()
        .map(|installed| installed.0.clone())
        .unwrap_or_else(Brand::neutral)
}

/// The washes the installed brand paints for the resolved palette mode.
pub fn washes(cx: &App) -> AccentWashes {
    AccentWashes::for_brand(&brand(cx), resolved_mode(cx))
}

fn with_alpha(color: Hsla, alpha: f32) -> Hsla {
    Hsla { a: alpha, ..color }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A synthetic accent, deliberately far from any product's hue.
    fn test_brand() -> Brand {
        Brand {
            name: SharedString::from("Synthetic"),
            accent: hsla(0.58, 0.65, 0.4, 1.0),
            accent_hover: hsla(0.58, 0.65, 0.46, 1.0),
            accent_pressed: hsla(0.58, 0.65, 0.34, 1.0),
            accent_tint: hsla(0.55, 0.7, 0.75, 1.0),
            on_accent: hsla(0.0, 0.0, 0.0, 1.0),
        }
    }

    #[gpui::test]
    fn the_brand_is_neutral_before_a_product_installs_one(cx: &mut gpui::TestAppContext) {
        cx.update(|cx| {
            assert_eq!(brand(cx), Brand::neutral());

            let washes = super::washes(cx);
            assert_eq!(
                washes,
                AccentWashes::for_brand(&Brand::neutral(), resolved_mode(cx))
            );
            assert_eq!(resolved_mode(cx), PaletteMode::Dark);
        });
    }

    #[gpui::test]
    fn the_installed_brand_replaces_the_fallback(cx: &mut gpui::TestAppContext) {
        cx.update(|cx| {
            install(test_brand(), cx);
            assert_eq!(brand(cx), test_brand());
        });
    }

    #[test]
    fn dark_washes_derive_from_the_accent_tint() {
        let brand = test_brand();
        let washes = AccentWashes::for_brand(&brand, PaletteMode::Dark);

        assert_eq!(washes.selected_row, with_alpha(brand.accent_tint, 0.07));
        assert_eq!(washes.selected_item, with_alpha(brand.accent_tint, 0.12));
        assert_eq!(washes.text_selection, with_alpha(brand.accent_tint, 0.25));
        assert_eq!(washes.drop_target, with_alpha(brand.accent_tint, 0.10));
    }

    #[test]
    fn light_washes_derive_from_the_accent() {
        let brand = test_brand();
        let washes = AccentWashes::for_brand(&brand, PaletteMode::Light);

        assert_eq!(washes.selected_row, with_alpha(brand.accent, 0.07));
        assert_eq!(washes.selected_item, with_alpha(brand.accent, 0.14));
        assert_eq!(washes.text_selection, with_alpha(brand.accent, 0.25));
        assert_eq!(washes.drop_target, with_alpha(brand.accent, 0.10));
    }

    #[test]
    fn an_unresolved_system_mode_counts_as_dark() {
        let brand = test_brand();
        assert_eq!(
            AccentWashes::for_brand(&brand, PaletteMode::System),
            AccentWashes::for_brand(&brand, PaletteMode::Dark)
        );
    }
}
