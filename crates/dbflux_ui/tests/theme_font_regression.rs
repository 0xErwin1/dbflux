use dbflux_components::typography::AppFonts;
use dbflux_core::ThemeSetting;
use dbflux_ui::ui::theme;
use gpui::{hsla, SharedString, TestAppContext, Window};
use gpui_component::theme::Theme;
use std::fs;

const THEME_SOURCE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/ui/theme.rs");

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
    assert_eq!(theme.font_family, SharedString::from(AppFonts::BODY));
    assert_eq!(theme.mono_font_family, SharedString::from(AppFonts::MONO));
    assert_eq!(
        theme.dark_theme.font_family,
        Some(SharedString::from(AppFonts::BODY))
    );
    assert_eq!(
        theme.dark_theme.mono_font_family,
        Some(SharedString::from(AppFonts::MONO))
    );
    assert_eq!(
        theme.light_theme.font_family,
        Some(SharedString::from(AppFonts::BODY))
    );
    assert_eq!(
        theme.light_theme.mono_font_family,
        Some(SharedString::from(AppFonts::MONO))
    );
}

fn read_theme_source() -> String {
    fs::read_to_string(THEME_SOURCE).expect("theme source should be readable")
}

#[gpui::test]
fn theme_init_and_apply_theme_keep_centralized_fonts_without_changing_base_tokens(
    cx: &mut TestAppContext,
) {
    cx.update(theme::init);

    cx.update(|cx| {
        let theme = Theme::global_mut(cx);

        assert_centralized_fonts(theme);
        assert_eq!(theme.border, rgb_to_hsla(0x1F2430));
        assert_eq!(theme.popover, rgb_to_hsla(0x141B24));
    });

    cx.update(|cx| theme::apply_theme(ThemeSetting::Light, Option::<&mut Window>::None, cx));

    cx.update(|cx| {
        let theme = Theme::global_mut(cx);

        assert_centralized_fonts(theme);
        assert_eq!(theme.foreground, rgb_to_hsla(0x5C6166));
        assert_eq!(theme.border, rgb_to_hsla(0xD9DEE8));
        assert_eq!(theme.primary_foreground, rgb_to_hsla(0x0A0E14));
        assert_eq!(theme.danger_foreground, rgb_to_hsla(0x0A0E14));
        assert_eq!(theme.success_foreground, rgb_to_hsla(0x0A0E14));
        assert_eq!(theme.warning_foreground, rgb_to_hsla(0x0A0E14));
        assert_eq!(theme.info_foreground, rgb_to_hsla(0x0A0E14));
        assert_eq!(theme.sidebar_primary_foreground, rgb_to_hsla(0x0A0E14));
        assert_eq!(theme.popover, rgb_to_hsla(0xF7F8FA));
    });
}

#[gpui::test]
fn mirage_theme_uses_dark_mode_palette_while_preserving_centralized_fonts(cx: &mut TestAppContext) {
    cx.update(theme::init);
    cx.update(|cx| theme::apply_theme(ThemeSetting::Mirage, Option::<&mut Window>::None, cx));

    cx.update(|cx| {
        let theme = Theme::global_mut(cx);

        assert_centralized_fonts(theme);
        assert_eq!(theme.background, rgb_to_hsla(0x1F2430));
        assert_eq!(theme.foreground, rgb_to_hsla(0xCBCCC6));
        assert_eq!(theme.primary, rgb_to_hsla(0xFFCC66));
        assert_eq!(theme.primary_foreground, rgb_to_hsla(0x1F2430));
        assert_eq!(theme.popover, rgb_to_hsla(0x242936));
        assert_eq!(theme.title_bar_border, rgb_to_hsla(0x3A4052));
        assert_eq!(theme.window_border, rgb_to_hsla(0x3A4052));
        assert_eq!(
            theme.highlight_theme.style.editor_background,
            Some(rgb_to_hsla(0x1F2430))
        );
        assert_eq!(
            theme.highlight_theme.style.editor_active_line,
            Some(rgb_to_hsla(0x242936))
        );
        assert_eq!(
            theme.highlight_theme.style.editor_line_number,
            Some(rgb_to_hsla(0x707A8C))
        );
        assert_eq!(
            theme.highlight_theme.style.editor_active_line_number,
            Some(rgb_to_hsla(0xCBCCC6))
        );
    });
}

#[test]
fn ghost_border_contract_moves_out_of_theme_module() {
    let mut expected = rgb_to_hsla(0x524436);
    expected.a = 0.15;

    assert_eq!(
        dbflux_components::tokens::ChromeColors::ghost_border(),
        expected
    );
}

#[test]
fn theme_ghost_border_forward_matches_component_token() {
    assert_eq!(
        theme::ghost_border_color(),
        dbflux_components::tokens::ChromeColors::ghost_border()
    );
}

#[test]
fn theme_module_keeps_palette_and_font_mapping_but_not_shared_chrome_helpers() {
    let source = read_theme_source();

    assert!(source.contains("pub use dbflux_components::typography::AppFonts;"));
    assert!(source.contains("load_bundled_fonts(cx);"));
    assert!(source.contains("ThemeSetting::Mirage"));
    assert!(source.contains("apply_ayu_mirage(cx);"));
    assert!(source.contains("theme.popover = raised;"));
    assert!(!source.contains("pub fn surface_highest_color()"));
}
