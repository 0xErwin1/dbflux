//! The accent colors DBFlux derives from a color the user picked.
//!
//! The palette's own accent never passes through here: it is installed as it
//! is. A user color is only a starting point, so it is moved until it reads on
//! the pane of the palette it applies to:
//!
//! - the fill (primary buttons, checked boxes, the progress bar, the selection)
//!   is lightened on Dark and darkened on Light until it reaches 3:1 against
//!   the pane;
//! - the text tint (text accents, the text cursor and the focus ring) is moved
//!   the same way until it reaches 4.5:1;
//! - the text on a primary button is white, or near-black when white would fall
//!   under 4.5:1 on the fill.
//!
//! Hover and pressed are a lightness step from the fill.

use bolt::theme::{BACKGROUND_DARK, BACKGROUND_LIGHT, rgb_to_hsla};
use dbflux_core::{AccentColor, ThemeSetting};
use gpui::{App, Global, Hsla};

/// Contrast the fills reach against the pane.
pub const FILL_CONTRAST: f32 = 3.0;
/// Contrast the text tint and the button text reach.
pub const TEXT_CONTRAST: f32 = 4.5;

/// Margin over a contrast target, so a ratio measured with another
/// luminance formula (the WCAG 0.03928 threshold against sRGB's 0.04045)
/// still clears the target.
const CONTRAST_MARGIN: f32 = 0.02;
/// Lightness between a fill and its hover state.
const HOVER_STEP: f32 = 0.05;
/// Lightness between a fill and its pressed state.
const PRESSED_STEP: f32 = -0.06;

const WHITE: u32 = 0xFFFFFF;
/// Near-black text on a bright fill; the Light palette's strong text.
const NEAR_BLACK: u32 = 0x141118;
const BLACK: u32 = 0x000000;

/// The roles DBFlux derives from one user accent.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DerivedAccent {
    /// The solid fill of a primary control.
    pub fill: Hsla,
    /// The fill under the pointer.
    pub hover: Hsla,
    /// The fill while pressed.
    pub pressed: Hsla,
    /// The text-accent tint.
    pub text: Hsla,
    /// Text drawn on the fill.
    pub on_fill: Hsla,
}

/// The accents the user picked, one per palette. `None` keeps the palette's
/// own accent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AccentOverrides {
    pub dark: Option<AccentColor>,
    pub light: Option<AccentColor>,
}

impl AccentOverrides {
    pub fn new(dark: Option<AccentColor>, light: Option<AccentColor>) -> Self {
        Self { dark, light }
    }

    /// The override for the resolved palette. `System` counts as Dark, like
    /// every role with a light variant.
    pub fn for_setting(&self, setting: ThemeSetting) -> Option<AccentColor> {
        match setting {
            ThemeSetting::Light => self.light,
            ThemeSetting::Dark | ThemeSetting::System => self.dark,
        }
    }
}

impl Global for AccentOverrides {}

/// The overrides applied by the next `apply_theme`.
pub fn overrides(cx: &App) -> AccentOverrides {
    cx.try_global::<AccentOverrides>()
        .copied()
        .unwrap_or_default()
}

/// Publishes the overrides the next `apply_theme` applies. Returns whether
/// they differ from the ones already published.
pub fn set_overrides(cx: &mut App, new: AccentOverrides) -> bool {
    let changed = overrides(cx) != new;

    if changed {
        cx.set_global(new);
    }

    changed
}

/// Derives the roles of `accent` on the resolved palette `setting`.
pub fn derive(accent: AccentColor, setting: ThemeSetting) -> DerivedAccent {
    let light = setting == ThemeSetting::Light;
    let pane = rgb_to_hsla(if light {
        BACKGROUND_LIGHT
    } else {
        BACKGROUND_DARK
    });
    let user = rgb_to_hsla(accent.rgb());

    let fill = reach_contrast(user, pane, FILL_CONTRAST, light);
    let text = reach_contrast(user, pane, TEXT_CONTRAST, light);

    DerivedAccent {
        fill,
        hover: shift_lightness(fill, HOVER_STEP),
        pressed: shift_lightness(fill, PRESSED_STEP),
        text,
        on_fill: text_on(fill),
    }
}

/// White text on `fill` when it reads at 4.5:1, otherwise near-black, otherwise
/// black. The middle tones are the reason for the last step: a fill whose
/// luminance sits where white falls under 4.5:1 and near-black still does too
/// is only readable with black.
fn text_on(fill: Hsla) -> Hsla {
    [WHITE, NEAR_BLACK, BLACK]
        .into_iter()
        .map(rgb_to_hsla)
        .find(|candidate| contrast_ratio(*candidate, fill) >= TEXT_CONTRAST + CONTRAST_MARGIN)
        .unwrap_or_else(|| rgb_to_hsla(BLACK))
}

/// Moves `color` toward white (`darken` false) or black until it reaches
/// `target` contrast against `pane`, keeping its hue and saturation. A color
/// that already reaches it is returned unchanged.
fn reach_contrast(color: Hsla, pane: Hsla, target: f32, darken: bool) -> Hsla {
    let target = target + CONTRAST_MARGIN;

    if contrast_ratio(color, pane) >= target {
        return color;
    }

    let limit = if darken { 0.0 } else { 1.0 };
    let at = |lightness: f32| Hsla {
        l: lightness,
        ..color
    };

    // Contrast against the pane grows monotonically with lightness moving away
    // from the pane, so bisect between the color and the extreme.
    let (mut failing, mut passing) = (color.l, limit);

    if contrast_ratio(at(passing), pane) < target {
        return at(passing);
    }

    for _ in 0..24 {
        let middle = (failing + passing) / 2.0;

        if contrast_ratio(at(middle), pane) >= target {
            passing = middle;
        } else {
            failing = middle;
        }
    }

    at(passing)
}

fn shift_lightness(color: Hsla, amount: f32) -> Hsla {
    Hsla {
        l: (color.l + amount).clamp(0.0, 1.0),
        ..color
    }
}

fn relative_luminance(color: Hsla) -> f32 {
    let rgba = color.to_rgb();

    fn linearize(channel: f32) -> f32 {
        if channel <= 0.04045 {
            channel / 12.92
        } else {
            ((channel + 0.055) / 1.055).powf(2.4)
        }
    }

    0.2126 * linearize(rgba.r) + 0.7152 * linearize(rgba.g) + 0.0722 * linearize(rgba.b)
}

/// WCAG 2.x contrast ratio between two opaque colors.
pub fn contrast_ratio(foreground: Hsla, background: Hsla) -> f32 {
    let fg = relative_luminance(foreground);
    let bg = relative_luminance(background);

    (fg.max(bg) + 0.05) / (fg.min(bg) + 0.05)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accent(rgb: u32) -> AccentColor {
        AccentColor::from_rgb(rgb)
    }

    fn pane(setting: ThemeSetting) -> Hsla {
        rgb_to_hsla(match setting {
            ThemeSetting::Light => BACKGROUND_LIGHT,
            _ => BACKGROUND_DARK,
        })
    }

    #[test]
    fn a_readable_accent_keeps_its_fill_and_only_the_text_tint_moves() {
        // The approved artboard's custom accent: 4.1:1 on the Dark pane.
        let derived = derive(accent(0x2F7D6D), ThemeSetting::Dark);

        assert_eq!(derived.fill, rgb_to_hsla(0x2F7D6D));
        assert!(contrast_ratio(derived.text, pane(ThemeSetting::Dark)) >= TEXT_CONTRAST);
        assert!(
            derived.text.l > derived.fill.l,
            "Dark lightens the text tint"
        );
        assert_eq!(derived.on_fill, rgb_to_hsla(WHITE));
    }

    #[test]
    fn dark_lightens_and_light_darkens_until_the_fill_reaches_3_to_1() {
        // The palette accent itself measures 2.1:1 on the Dark pane.
        let dark = derive(accent(0x702963), ThemeSetting::Dark);
        assert!(dark.fill.l > rgb_to_hsla(0x702963).l);
        assert!(contrast_ratio(dark.fill, pane(ThemeSetting::Dark)) >= FILL_CONTRAST);

        let light = derive(accent(0xE9D8F0), ThemeSetting::Light);
        assert!(light.fill.l < rgb_to_hsla(0xE9D8F0).l);
        assert!(contrast_ratio(light.fill, pane(ThemeSetting::Light)) >= FILL_CONTRAST);
    }

    #[test]
    fn hover_is_lighter_and_pressed_darker_than_the_fill() {
        let derived = derive(accent(0x2F7D6D), ThemeSetting::Dark);

        assert!(derived.hover.l > derived.fill.l);
        assert!(derived.pressed.l < derived.fill.l);
    }

    #[test]
    fn button_text_turns_dark_when_white_falls_under_4_5_to_1() {
        let bright = derive(accent(0xF4F1EC), ThemeSetting::Dark);

        assert_eq!(bright.on_fill, rgb_to_hsla(NEAR_BLACK));
        assert!(contrast_ratio(bright.on_fill, bright.fill) >= TEXT_CONTRAST);
    }

    #[test]
    fn mid_tones_where_near_black_is_not_enough_use_black() {
        // Luminance about 0.195: white and near-black both measure about 4.3:1.
        let fill = rgb_to_hsla(0x7A7A7A);

        assert!(contrast_ratio(rgb_to_hsla(WHITE), fill) < TEXT_CONTRAST);
        assert!(contrast_ratio(rgb_to_hsla(NEAR_BLACK), fill) < TEXT_CONTRAST);
        assert_eq!(text_on(fill), rgb_to_hsla(BLACK));
        assert!(contrast_ratio(text_on(fill), fill) >= TEXT_CONTRAST);
    }

    #[test]
    fn overrides_select_by_palette_and_system_counts_as_dark() {
        let overrides = AccentOverrides::new(Some(accent(0x111111)), Some(accent(0x222222)));

        assert_eq!(
            overrides.for_setting(ThemeSetting::Dark),
            Some(accent(0x111111))
        );
        assert_eq!(
            overrides.for_setting(ThemeSetting::System),
            Some(accent(0x111111))
        );
        assert_eq!(
            overrides.for_setting(ThemeSetting::Light),
            Some(accent(0x222222))
        );
        assert_eq!(
            AccentOverrides::default().for_setting(ThemeSetting::Dark),
            None
        );
    }
}
