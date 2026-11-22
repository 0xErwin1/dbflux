//! A user accent bypasses the palette, so `theme_contrast_regression.rs`
//! cannot guard it. These tests apply accents through the production
//! `apply_theme` path, the extremes the maintainer named (near white, near
//! black and the pane color itself) and a sweep of the color cube, and check
//! what the accent drives: the fills at 3:1 against the pane, the text tint at
//! 4.5:1, and the primary button text at 4.5:1 on the fill.

use dbflux_core::{AccentColor, AppStyle, ThemeSetting};
use dbflux_ui::theme::{self, AccentOverrides};
use gpui::{Hsla, TestAppContext, Window};
use gpui_component::theme::Theme;

const FILL_MIN: f32 = 3.0;
const TEXT_MIN: f32 = 4.5;

fn relative_luminance(color: Hsla) -> f32 {
    let rgba = color.to_rgb();

    fn linearize(channel: f32) -> f32 {
        if channel <= 0.03928 {
            channel / 12.92
        } else {
            ((channel + 0.055) / 1.055).powf(2.4)
        }
    }

    0.2126 * linearize(rgba.r) + 0.7152 * linearize(rgba.g) + 0.0722 * linearize(rgba.b)
}

fn contrast_ratio(foreground: Hsla, background: Hsla) -> f32 {
    let fg = relative_luminance(foreground);
    let bg = relative_luminance(background);

    (fg.max(bg) + 0.05) / (fg.min(bg) + 0.05)
}

fn hex(rgb: u32) -> AccentColor {
    AccentColor::from_rgb(rgb)
}

fn apply(setting: ThemeSetting, accent: AccentColor, cx: &mut gpui::App) {
    let overrides = match setting {
        ThemeSetting::Light => AccentOverrides::new(None, Some(accent)),
        _ => AccentOverrides::new(Some(accent), None),
    };

    theme::set_accent_overrides(cx, overrides);
    theme::apply_theme(setting, AppStyle::Default, Option::<&mut Window>::None, cx);
}

fn assert_accent_is_readable(theme: &Theme, setting: ThemeSetting, accent: AccentColor) {
    let label = format!("{setting:?} {}", accent.to_hex());
    let pane = theme.background;

    for (name, fill) in [
        ("button fill", theme.tokens.button_primary.color),
        ("checkbox fill", theme.tokens.primary.color),
    ] {
        let ratio = contrast_ratio(fill, pane);
        assert!(
            ratio >= FILL_MIN,
            "{label}: {name} is {ratio:.2}:1 on the pane"
        );
    }

    let tint = contrast_ratio(theme.ring, pane);
    assert!(
        tint >= TEXT_MIN,
        "{label}: text tint is {tint:.2}:1 on the pane"
    );

    let caret = contrast_ratio(theme.caret, pane);
    assert!(
        caret >= FILL_MIN,
        "{label}: text cursor is {caret:.2}:1 on the pane"
    );

    let button_text = contrast_ratio(
        theme.tokens.button_primary_foreground.color,
        theme.tokens.button_primary.color,
    );
    assert!(
        button_text >= TEXT_MIN,
        "{label}: primary button text is {button_text:.2}:1 on the fill"
    );
}

#[gpui::test]
fn the_named_extremes_stay_readable_on_both_palettes(cx: &mut TestAppContext) {
    cx.update(theme::init);

    for setting in [ThemeSetting::Dark, ThemeSetting::Light] {
        let pane = match setting {
            ThemeSetting::Light => 0xF6F4F7,
            _ => 0x09090B,
        };

        for rgb in [
            // The palette's own #702963 is no override: it installs the palette
            // brand unchanged, which `the_palette_accent_is_untouched_without_an_override`
            // covers.
            0xF4F1EC, 0xFFFFFF, 0x0E0C10, 0x000000, pane, 0x2F7D6D,
        ] {
            cx.update(|cx| apply(setting, hex(rgb), cx));
            cx.update(|cx| assert_accent_is_readable(Theme::global(cx), setting, hex(rgb)));
        }
    }
}

#[gpui::test]
fn a_sweep_of_the_color_cube_stays_readable_on_both_palettes(cx: &mut TestAppContext) {
    cx.update(theme::init);

    let steps = [0x00, 0x40, 0x80, 0xC0, 0xFF];

    for setting in [ThemeSetting::Dark, ThemeSetting::Light] {
        for red in steps {
            for green in steps {
                for blue in steps {
                    let accent = hex((red << 16) | (green << 8) | blue);

                    cx.update(|cx| apply(setting, accent, cx));
                    cx.update(|cx| assert_accent_is_readable(Theme::global(cx), setting, accent));
                }
            }
        }
    }
}

#[gpui::test]
fn the_palette_accent_is_untouched_without_an_override(cx: &mut TestAppContext) {
    cx.update(theme::init);

    for setting in [ThemeSetting::Dark, ThemeSetting::Light] {
        cx.update(|cx| {
            theme::set_accent_overrides(cx, AccentOverrides::default());
            theme::apply_theme(setting, AppStyle::Default, Option::<&mut Window>::None, cx);
        });
        cx.update(|cx| {
            let theme = Theme::global(cx);

            assert_eq!(
                theme.tokens.button_primary.color,
                Hsla::from(gpui::rgb(0x702963)),
                "{setting:?}"
            );
            assert_eq!(
                theme.primary_foreground,
                Hsla::from(gpui::rgb(0xFFFFFF)),
                "{setting:?}"
            );
        });
    }
}
