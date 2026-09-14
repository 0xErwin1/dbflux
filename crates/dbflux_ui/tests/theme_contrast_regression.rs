use dbflux_core::{AppStyle, ThemeSetting};
use dbflux_ui::theme;
use gpui::{Hsla, TestAppContext, Window};
use gpui_component::theme::Theme;

/// WCAG 2.x minimum contrast for normal text.
const AA_TEXT: f32 = 4.5;

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

fn assert_readable_secondary_text(theme: &Theme, setting: ThemeSetting) {
    let surfaces = [
        ("background", theme.background),
        ("panel", theme.tab),
        ("raised", theme.popover),
    ];

    let foregrounds = [
        ("muted_foreground", theme.muted_foreground),
        ("tab_foreground", theme.tab_foreground),
        ("table_head_foreground", theme.table_head_foreground),
        (
            "description_list_label_foreground",
            theme.description_list_label_foreground,
        ),
    ];

    for (surface_name, surface) in surfaces {
        for (foreground_name, foreground) in foregrounds {
            let ratio = contrast_ratio(foreground, surface);

            assert!(
                ratio >= AA_TEXT,
                "{setting:?}: {foreground_name} on {surface_name} measures {ratio:.2}:1, below {AA_TEXT}:1"
            );
        }
    }
}

#[gpui::test]
fn secondary_text_meets_aa_contrast_on_every_palette(cx: &mut TestAppContext) {
    cx.update(theme::init);

    for setting in [
        ThemeSetting::Dark,
        ThemeSetting::Mirage,
        ThemeSetting::Light,
    ] {
        cx.update(|cx| {
            theme::apply_theme(setting, AppStyle::Default, Option::<&mut Window>::None, cx)
        });

        cx.update(|cx| {
            let theme = Theme::global_mut(cx);

            assert_readable_secondary_text(theme, setting);
        });
    }
}
