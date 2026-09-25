use dbflux_components::typography::AppFonts;
use dbflux_core::{AppStyle, ThemeSetting};
use dbflux_ui::theme;
use gpui::{SharedString, TestAppContext, Window, hsla};
use gpui_component::theme::Theme;
use std::fs;

const THEME_SOURCE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../dbflux_components/src/theme.rs"
);

fn rgb_to_hsla(hex: u32) -> gpui::Hsla {
    let r = ((hex >> 16) & 0xFF) as f32 / 255.0;
    let g = ((hex >> 8) & 0xFF) as f32 / 255.0;
    let b = (hex & 0xFF) as f32 / 255.0;

    let max = r.max(g).max(b);
    let min = r.min(g).min(b);
    let l = (max + min) / 2.0;

    if (max - min).abs() < f32::EPSILON {
        return hsla(0.0, 0.0, l, 1.0);
    }

    let d = max - min;
    let s = if l > 0.5 {
        d / (2.0 - max - min)
    } else {
        d / (max + min)
    };

    let h = if (max - r).abs() < f32::EPSILON {
        let mut h = (g - b) / d;
        if g < b {
            h += 6.0;
        }
        h
    } else if (max - g).abs() < f32::EPSILON {
        (b - r) / d + 2.0
    } else {
        (r - g) / d + 4.0
    };

    hsla(h / 6.0, s, l, 1.0)
}

fn assert_centralized_fonts(theme: &Theme) {
    assert_eq!(theme.font_family, SharedString::from(AppFonts::INTERFACE));
    assert_eq!(theme.mono_font_family, SharedString::from(AppFonts::MONO));
    assert_eq!(
        theme.dark_theme.font_family,
        Some(SharedString::from(AppFonts::INTERFACE))
    );
    assert_eq!(
        theme.dark_theme.mono_font_family,
        Some(SharedString::from(AppFonts::MONO))
    );
    assert_eq!(
        theme.light_theme.font_family,
        Some(SharedString::from(AppFonts::INTERFACE))
    );
    assert_eq!(
        theme.light_theme.mono_font_family,
        Some(SharedString::from(AppFonts::MONO))
    );
}

fn read_theme_source() -> String {
    fs::read_to_string(THEME_SOURCE).expect("theme source should be readable")
}

fn rgb_to_hsla_alpha(hex: u32, alpha: f32) -> gpui::Hsla {
    let mut color = rgb_to_hsla(hex);
    color.a = alpha;
    color
}

#[gpui::test]
fn theme_init_and_apply_theme_keep_centralized_fonts_without_changing_base_tokens(
    cx: &mut TestAppContext,
) {
    cx.update(theme::init);

    cx.update(|cx| {
        let theme = Theme::global_mut(cx);

        assert_centralized_fonts(theme);
        assert_eq!(theme.border, rgb_to_hsla(0x232128));
        assert_eq!(theme.popover, rgb_to_hsla(0x100F13));
    });

    cx.update(|cx| {
        theme::apply_theme(
            ThemeSetting::Light,
            AppStyle::Default,
            Option::<&mut Window>::None,
            cx,
        )
    });

    cx.update(|cx| {
        let theme = Theme::global_mut(cx);

        assert_centralized_fonts(theme);
        assert_eq!(theme.background, rgb_to_hsla(0xF6F4F7));
        assert_eq!(theme.foreground, rgb_to_hsla(0x3B3740));
        assert_eq!(theme.border, rgb_to_hsla(0xE3DEE6));
        assert_eq!(theme.primary, rgb_to_hsla(0x702963));
        assert_eq!(theme.primary_foreground, rgb_to_hsla(0xFFFFFF));
        assert_eq!(theme.danger_foreground, rgb_to_hsla(0xFFFFFF));
        assert_eq!(theme.success_foreground, rgb_to_hsla(0xFFFFFF));
        assert_eq!(theme.warning_foreground, rgb_to_hsla(0xFFFFFF));
        assert_eq!(theme.info_foreground, rgb_to_hsla(0xFFFFFF));
        assert_eq!(theme.sidebar_primary_foreground, rgb_to_hsla(0xFFFFFF));
        assert_eq!(theme.popover, rgb_to_hsla(0xFFFFFF));
        assert_eq!(theme.secondary, rgb_to_hsla(0xEEEAF0));
        assert_eq!(theme.secondary_hover, rgb_to_hsla(0xE3DEE6));
        assert_eq!(theme.secondary_active, rgb_to_hsla(0xCBC4D1));
        assert_eq!(theme.primary_hover, rgb_to_hsla(0x7F3171));
        assert_eq!(theme.primary_active, rgb_to_hsla(0x4A1B41));
        assert_eq!(theme.input, rgb_to_hsla(0xCBC4D1));
        assert_eq!(theme.ring, rgb_to_hsla(0x702963));
    });
}

#[gpui::test]
fn dark_theme_maps_palette_roles_and_editor_chrome(cx: &mut TestAppContext) {
    cx.update(theme::init);
    cx.update(|cx| {
        theme::apply_theme(
            ThemeSetting::Dark,
            AppStyle::Default,
            Option::<&mut Window>::None,
            cx,
        )
    });

    cx.update(|cx| {
        let theme = Theme::global_mut(cx);

        assert_centralized_fonts(theme);
        assert_eq!(theme.background, rgb_to_hsla(0x09090B));
        assert_eq!(theme.foreground, rgb_to_hsla(0xC6C3CC));
        assert_eq!(theme.muted_foreground, rgb_to_hsla(0x8E8996));
        assert_eq!(theme.primary, rgb_to_hsla(0x702963));
        assert_eq!(theme.primary_hover, rgb_to_hsla(0x7F3171));
        assert_eq!(theme.primary_active, rgb_to_hsla(0x4A1B41));
        assert_eq!(theme.primary_foreground, rgb_to_hsla(0xFFFFFF));
        assert_eq!(theme.ring, rgb_to_hsla(0xD48CC8));
        assert_eq!(theme.caret, rgb_to_hsla(0xD48CC8));
        assert_eq!(theme.selection, rgb_to_hsla_alpha(0xD48CC8, 0.25));
        assert_eq!(theme.popover, rgb_to_hsla(0x100F13));
        assert_eq!(theme.secondary, rgb_to_hsla(0x1A181E));
        assert_eq!(theme.secondary_hover, rgb_to_hsla(0x232128));
        assert_eq!(theme.secondary_active, rgb_to_hsla(0x37333D));
        assert_eq!(theme.input, rgb_to_hsla(0x37333D));
        assert_eq!(theme.danger, rgb_to_hsla(0xFF6B5E));
        assert_eq!(theme.danger_foreground, rgb_to_hsla(0x09090B));
        assert_eq!(theme.table_row_border, rgb_to_hsla(0x18161B));
        assert_eq!(theme.list_active, rgb_to_hsla_alpha(0xD48CC8, 0.12));
        assert_eq!(theme.table_active, rgb_to_hsla_alpha(0xD48CC8, 0.07));
        assert_eq!(theme.title_bar_border, rgb_to_hsla(0x232128));
        assert_eq!(theme.window_border, rgb_to_hsla(0x232128));
        assert_eq!(
            theme.highlight_theme.style.editor_background,
            Some(rgb_to_hsla(0x09090B))
        );
        assert_eq!(
            theme.highlight_theme.style.editor_active_line,
            Some(rgb_to_hsla_alpha(0xFFFFFF, 0.04))
        );
        assert_eq!(
            theme.highlight_theme.style.editor_line_number,
            Some(rgb_to_hsla(0x8E8996))
        );
        assert_eq!(
            theme.highlight_theme.style.editor_active_line_number,
            Some(rgb_to_hsla(0xF7F4F7))
        );
    });
}

#[gpui::test]
fn ghost_border_resolves_to_the_palette_line_in_both_variants(cx: &mut TestAppContext) {
    cx.update(theme::init);

    for (setting, line) in [
        (ThemeSetting::Dark, 0x232128),
        (ThemeSetting::Light, 0xE3DEE6),
    ] {
        cx.update(|cx| {
            theme::apply_theme(setting, AppStyle::Default, Option::<&mut Window>::None, cx)
        });

        cx.update(|cx| {
            let theme = Theme::global(cx);

            assert_eq!(
                dbflux_components::tokens::ChromeColors::ghost_border(theme),
                rgb_to_hsla(line)
            );
            assert_eq!(
                theme::ghost_border_color(theme),
                dbflux_components::tokens::ChromeColors::ghost_border(theme)
            );
        });
    }
}

#[test]
fn theme_module_keeps_palette_and_font_mapping_but_not_shared_chrome_helpers() {
    let source = read_theme_source();

    assert!(source.contains("pub use crate::typography::AppFonts;"));
    assert!(source.contains("load_bundled_fonts(cx);"));
    assert!(source.contains("ThemeSetting::System"));
    assert!(source.contains("apply_palette(&palette, style, cx);"));
    assert!(source.contains("theme.popover = palette.panel;"));
    assert!(!source.contains("apply_ayu_"));
    assert!(!source.contains("pub fn surface_highest_color()"));
}
