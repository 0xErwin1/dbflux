//! DBFlux's product adapter over `bolt`.
//!
//! Bolt owns the theme engine and the shared palettes; this module supplies
//! DBFlux's brand (the byzantine accent), maps the persisted `ThemeSetting`
//! and `AppStyle` settings onto bolt's `PaletteMode` and `Density`, and keeps
//! the public API the rest of the workspace compiles against.

use crate::fonts::FontSettings;
use crate::semantic::ThemeSettingGlobal;
pub use crate::typography::AppFonts;
use crate::typography::load_bundled_fonts;
use bolt::brand::Brand;
use bolt::palette::PaletteMode;
use bolt::theme::rgb_to_hsla;
use bolt::theme::{self as bolt_theme};
use dbflux_core::{AppStyle, ThemeSetting};
use gpui::{App, Hsla, Window, WindowAppearance};
use gpui_component::theme::Theme;

/// Byzantine `#702963`, the solid fill of a primary control in both variants.
const ACCENT: u32 = 0x702963;
/// Byzantine hover `#7F3171`.
const ACCENT_HOVER: u32 = 0x7F3171;
/// Byzantine deep `#4A1B41`, the pressed state.
const ACCENT_PRESSED: u32 = 0x4A1B41;
/// Text-accent tint `#D48CC8` on dark surfaces; on light surfaces the accent
/// itself takes this role.
const ACCENT_TINT: u32 = 0xD48CC8;
const ON_ACCENT: u32 = 0xFFFFFF;

/// The DBFlux brand: the byzantine accent of the Bolt Byzantium palette.
pub fn brand() -> Brand {
    Brand {
        name: "DBFlux".into(),
        accent: rgb_to_hsla(ACCENT),
        accent_hover: rgb_to_hsla(ACCENT_HOVER),
        accent_pressed: rgb_to_hsla(ACCENT_PRESSED),
        accent_tint: rgb_to_hsla(ACCENT_TINT),
        on_accent: rgb_to_hsla(ON_ACCENT),
    }
}

/// Structural separator between major UI regions; resolves to the palette line.
pub fn ghost_border_color(theme: &Theme) -> Hsla {
    bolt_theme::ghost_border_color(theme)
}

pub fn init(cx: &mut App) {
    gpui_component::init(cx);
    crate::highlighting::register_languages();
    load_bundled_fonts(cx);
    crate::controls::install_dropdown_seams(cx);
    crate::modals::modal::install_modal_seams(cx);
    crate::composites::install_shell_icons(cx);
    apply_theme(ThemeSetting::Dark, AppStyle::Default, None, cx);
}

/// Initialize the theme, density and font globals from persisted settings.
///
/// Call this after `init` and after the config has been loaded, before
/// the first window opens. This sets up the correct radius tokens, density
/// global and fonts for the first frame.
pub fn init_with_settings(
    setting: ThemeSetting,
    style: AppStyle,
    fonts: FontSettings,
    cx: &mut App,
) {
    crate::density::init(cx, style);
    crate::fonts::init(cx, fonts);
    apply_theme(setting, style, None, cx);
}

/// Write the active font settings into the global theme without changing
/// its palette, so open windows pick them up on their next render.
///
/// Does nothing before the theme has been initialized.
pub fn apply_fonts(cx: &mut App) {
    bolt_theme::apply_fonts(cx);
}

/// Apply the Bolt Byzantium palette for `setting`.
///
/// `ThemeSetting::System` resolves through the OS appearance (the window's when
/// one is given, which is more reliable on Linux). The resolved Dark or Light
/// variant is published to `bolt` so semantic color accessors follow the
/// palette that is actually on screen, and recorded in `ThemeSettingGlobal`
/// as the product-side setting.
pub fn apply_theme(
    setting: ThemeSetting,
    style: AppStyle,
    window: Option<&mut Window>,
    cx: &mut App,
) {
    let resolved = match setting {
        ThemeSetting::System => {
            let appearance = window
                .as_ref()
                .map(|window| window.appearance())
                .unwrap_or_else(|| cx.window_appearance());
            variant_for_appearance(appearance)
        }
        ThemeSetting::Dark | ThemeSetting::Light => setting,
    };

    bolt::brand::install(brand(), cx);
    bolt_theme::apply_theme(
        palette_mode(resolved),
        crate::density::bolt_density(style),
        window,
        cx,
    );

    ThemeSettingGlobal::set(cx, resolved);
}

/// Map an OS appearance to the palette variant that matches it.
pub fn variant_for_appearance(appearance: WindowAppearance) -> ThemeSetting {
    match appearance {
        WindowAppearance::Dark | WindowAppearance::VibrantDark => ThemeSetting::Dark,
        WindowAppearance::Light | WindowAppearance::VibrantLight => ThemeSetting::Light,
    }
}

fn palette_mode(setting: ThemeSetting) -> PaletteMode {
    match setting {
        ThemeSetting::System => PaletteMode::System,
        ThemeSetting::Dark => PaletteMode::Dark,
        ThemeSetting::Light => PaletteMode::Light,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bolt::theme::rgb_to_hsla_alpha;
    use gpui::TestAppContext;
    use gpui_component::theme::Theme;

    /// Byzantine is the primary fill of both variants.
    fn expected_primary() -> Hsla {
        rgb_to_hsla(ACCENT)
    }

    fn expected_tint() -> Hsla {
        rgb_to_hsla(ACCENT_TINT)
    }

    /// Primary buttons fill from `tokens.button_primary` and checked
    /// checkboxes from `tokens.primary`; both must carry the byzantine fill
    /// of the applied setting, not the dependency's default (white in dark
    /// mode). Exercises the production `apply_theme` path.
    #[gpui::test]
    fn component_tokens_follow_byzantine_primary_across_all_settings(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        for setting in [
            ThemeSetting::System,
            ThemeSetting::Dark,
            ThemeSetting::Light,
        ] {
            cx.update(|cx| apply_theme(setting, AppStyle::Default, None, cx));
            cx.update(|cx| {
                let theme = Theme::global(cx);
                let expected = expected_primary();

                assert_eq!(
                    theme.tokens.primary.color, expected,
                    "{setting:?}: checkbox fill"
                );
                assert_eq!(
                    theme.tokens.button_primary.color, expected,
                    "{setting:?}: primary button fill"
                );
                assert_eq!(
                    theme.tokens.button_primary_hover.color, theme.colors.primary_hover,
                    "{setting:?}: primary button hover"
                );
                assert_eq!(
                    theme.tokens.button_primary_active.color, theme.colors.primary_active,
                    "{setting:?}: primary button active"
                );
                assert_eq!(
                    theme.tokens.button_primary_foreground.color, theme.colors.primary_foreground,
                    "{setting:?}: primary button foreground"
                );
                assert_eq!(
                    theme.colors.button_primary, expected,
                    "{setting:?}: legacy button field"
                );
            });
        }
    }

    /// Switching settings re-resolves the tokens every time; no palette may
    /// leak into another. Pins the byzantine accent, its tint and the neutral
    /// backgrounds of both variants.
    #[gpui::test]
    fn theme_switches_keep_component_tokens_in_sync(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        cx.update(|cx| apply_theme(ThemeSetting::Dark, AppStyle::Default, None, cx));
        cx.update(|cx| apply_theme(ThemeSetting::Light, AppStyle::Default, None, cx));
        cx.update(|cx| {
            let theme = Theme::global(cx);
            assert_eq!(theme.colors.background, rgb_to_hsla(0xF6F4F7));
            assert_eq!(theme.tokens.button_primary.color, expected_primary());
            assert_eq!(theme.tokens.primary.color, expected_primary());
            // On light surfaces the accent doubles as the text accent.
            assert_eq!(theme.colors.ring, expected_primary());
        });

        cx.update(|cx| apply_theme(ThemeSetting::Dark, AppStyle::Default, None, cx));
        cx.update(|cx| {
            let theme = Theme::global(cx);
            assert_eq!(theme.colors.background, rgb_to_hsla(0x09090B));
            assert_eq!(theme.tokens.button_primary.color, expected_primary());
            assert_eq!(theme.tokens.primary.color, expected_primary());
            assert_eq!(theme.colors.ring, expected_tint());
        });
    }

    /// The selection and drop-target washes derive from the byzantine tint on
    /// dark and the accent on light, at the design-board shares.
    #[gpui::test]
    fn selection_washes_follow_the_byzantine_accent(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        cx.update(|cx| apply_theme(ThemeSetting::Dark, AppStyle::Default, None, cx));
        cx.update(|cx| {
            let theme = Theme::global(cx);
            assert_eq!(theme.colors.selection, rgb_to_hsla_alpha(ACCENT_TINT, 0.25));
            assert_eq!(
                theme.colors.drop_target,
                rgb_to_hsla_alpha(ACCENT_TINT, 0.10)
            );
            assert_eq!(
                theme.colors.table_active,
                rgb_to_hsla_alpha(ACCENT_TINT, 0.07)
            );
            assert_eq!(
                theme.colors.list_active,
                rgb_to_hsla_alpha(ACCENT_TINT, 0.12)
            );
        });

        cx.update(|cx| apply_theme(ThemeSetting::Light, AppStyle::Default, None, cx));
        cx.update(|cx| {
            let theme = Theme::global(cx);
            assert_eq!(theme.colors.selection, rgb_to_hsla_alpha(ACCENT, 0.25));
            assert_eq!(theme.colors.drop_target, rgb_to_hsla_alpha(ACCENT, 0.10));
            assert_eq!(theme.colors.table_active, rgb_to_hsla_alpha(ACCENT, 0.07));
            assert_eq!(theme.colors.list_active, rgb_to_hsla_alpha(ACCENT, 0.14));
        });
    }

    #[test]
    fn os_appearance_maps_to_the_matching_variant() {
        assert_eq!(
            variant_for_appearance(WindowAppearance::Dark),
            ThemeSetting::Dark
        );
        assert_eq!(
            variant_for_appearance(WindowAppearance::VibrantDark),
            ThemeSetting::Dark
        );
        assert_eq!(
            variant_for_appearance(WindowAppearance::Light),
            ThemeSetting::Light
        );
        assert_eq!(
            variant_for_appearance(WindowAppearance::VibrantLight),
            ThemeSetting::Light
        );
    }

    /// The semantic color accessors read `ThemeSettingGlobal`, so every
    /// `apply_theme` call must publish the variant it put on screen. The test
    /// platform reports a light appearance, which `System` must follow.
    #[gpui::test]
    fn apply_theme_publishes_the_resolved_variant(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        cx.update(|cx| apply_theme(ThemeSetting::Dark, AppStyle::Default, None, cx));
        cx.update(|cx| assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Dark));

        cx.update(|cx| apply_theme(ThemeSetting::Light, AppStyle::Default, None, cx));
        cx.update(|cx| assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Light));

        cx.update(|cx| apply_theme(ThemeSetting::Dark, AppStyle::Default, None, cx));
        cx.update(|cx| apply_theme(ThemeSetting::System, AppStyle::Default, None, cx));
        cx.update(|cx| {
            let expected = variant_for_appearance(cx.window_appearance());
            assert_eq!(ThemeSettingGlobal::get(cx), expected);
            assert_eq!(
                Theme::global(cx).colors.background == rgb_to_hsla(0xF6F4F7),
                expected == ThemeSetting::Light
            );
        });
    }

    fn hex_of(color: Hsla) -> u32 {
        let rgba = gpui::Rgba::from(color);
        let channel = |value: f32| (value * 255.0).round() as u32;

        (channel(rgba.r) << 16) | (channel(rgba.g) << 8) | channel(rgba.b)
    }

    fn syntax_hex(theme: &Theme, capture: &str) -> Option<u32> {
        theme
            .highlight_theme
            .style
            .syntax
            .style(capture)
            .and_then(|style| style.color)
            .map(hex_of)
    }

    /// The SQL editor's highlight theme and the tint accessor follow the
    /// palette's syntax roles in both variants, and the syntax keyword takes
    /// the byzantine accent role: `#D48CC8` on dark, `#702963` on light. The
    /// coupling between the accent and the editor is pinned here, on the
    /// product side, because it is DBFlux's rule, not Bolt's.
    #[gpui::test]
    fn editor_syntax_and_tint_follow_the_palette(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);

        for (setting, expected, keyword_hex) in [
            (
                ThemeSetting::Dark,
                bolt::tokens::SyntaxColors::dark().with_keyword(brand().accent_tint),
                ACCENT_TINT,
            ),
            (
                ThemeSetting::Light,
                bolt::tokens::SyntaxColors::light().with_keyword(brand().accent),
                ACCENT,
            ),
        ] {
            assert_eq!(
                hex_of(expected.keyword),
                keyword_hex,
                "{setting:?}: keyword role"
            );

            cx.update(|cx| apply_theme(setting, AppStyle::Default, None, cx));
            cx.update(|cx| {
                let theme = Theme::global(cx);

                for (capture, color) in [
                    ("keyword", expected.keyword),
                    ("string", expected.string),
                    ("number", expected.number),
                    ("comment", expected.comment),
                    ("type", expected.type_name),
                    ("function", expected.function),
                    ("operator", expected.operator),
                    ("punctuation.delimiter", expected.operator),
                    ("variable", expected.plain),
                ] {
                    assert_eq!(
                        syntax_hex(theme, capture),
                        Some(hex_of(color)),
                        "{setting:?}: {capture}"
                    );
                }

                assert_eq!(
                    hex_of(crate::tokens::ChromeColors::tint(theme)),
                    hex_of(expected.keyword),
                    "{setting:?}: tint"
                );
            });

            cx.update(|cx| assert_eq!(bolt::tokens::SyntaxColors::for_current(cx), expected));
        }
    }
}
