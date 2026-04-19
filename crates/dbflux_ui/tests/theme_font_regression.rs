use dbflux_components::typography::AppFonts;
use dbflux_core::ThemeSetting;
use dbflux_ui::ui::theme;
use gpui::{SharedString, TestAppContext, Window, hsla};
use gpui_component::theme::Theme;

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

#[gpui::test]
fn theme_init_and_apply_theme_keep_centralized_fonts_without_changing_base_tokens(
    cx: &mut TestAppContext,
) {
    cx.update(theme::init);

    cx.update(|cx| {
        let theme = Theme::global_mut(cx);

        assert_centralized_fonts(theme);
        assert_eq!(theme.border, rgb_to_hsla(0x1F2430));
        assert_eq!(theme.popover, rgb_to_hsla(0x31353C));
    });

    cx.update(|cx| theme::apply_theme(ThemeSetting::Light, Option::<&mut Window>::None, cx));

    cx.update(|cx| {
        let theme = Theme::global_mut(cx);

        assert_centralized_fonts(theme);
        assert_eq!(theme.foreground, rgb_to_hsla(0x5C6166));
        assert_eq!(theme.border, rgb_to_hsla(0xDCDCDC));
        assert_eq!(theme.primary_foreground, rgb_to_hsla(0x0A0E14));
        assert_eq!(theme.success_foreground, rgb_to_hsla(0x0A0E14));
        assert_eq!(theme.warning_foreground, rgb_to_hsla(0x0A0E14));
        assert_eq!(theme.info_foreground, rgb_to_hsla(0x0A0E14));
        assert_eq!(theme.sidebar_primary_foreground, rgb_to_hsla(0x0A0E14));
        assert_eq!(theme.popover, rgb_to_hsla(0xFFFFFF));
    });
}
