use gpui::TestAppContext;
use gpui_component::Theme;

use super::*;
use crate::{
    brand::Brand, density::Density, palette::PaletteMode, semantic::BannerColors, theme::install,
    tokens::ChromeColors,
};

/// Every role paired with the color the installed theme draws for it.
fn drawn_roles(theme: &Theme) -> [(&'static str, Hsla); 19] {
    [
        ("desk", ChromeColors::desk(theme)),
        ("background", theme.background),
        ("panel", theme.popover),
        ("raised", theme.secondary),
        ("line", theme.border),
        ("line_2", theme.input),
        ("row_divider", theme.table_row_border),
        ("strong", ChromeColors::strong(theme)),
        ("foreground", theme.foreground),
        ("muted", theme.muted_foreground),
        ("hover", theme.list_hover),
        ("alternating_row", theme.table_even),
        ("island_edge", ChromeColors::island_edge(theme)),
        ("scrim", theme.overlay),
        ("success", theme.success),
        ("info", theme.info),
        ("warning", theme.warning),
        ("danger", theme.danger),
        ("on_semantic", theme.success_foreground),
    ]
}

fn role_list(colors: &Colors) -> [(&'static str, Hsla); 19] {
    [
        ("desk", colors.desk),
        ("background", colors.background),
        ("panel", colors.panel),
        ("raised", colors.raised),
        ("line", colors.line),
        ("line_2", colors.line_2),
        ("row_divider", colors.row_divider),
        ("strong", colors.strong),
        ("foreground", colors.foreground),
        ("muted", colors.muted),
        ("hover", colors.hover),
        ("alternating_row", colors.alternating_row),
        ("island_edge", colors.island_edge),
        ("scrim", colors.scrim),
        ("success", colors.success),
        ("info", colors.info),
        ("warning", colors.warning),
        ("danger", colors.danger),
        ("on_semantic", colors.on_semantic),
    ]
}

#[gpui::test]
fn every_role_matches_what_the_installed_theme_draws(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);

    for (mode, expected) in [
        (PaletteMode::Dark, Colors::dark()),
        (PaletteMode::Light, Colors::light()),
    ] {
        cx.update(|cx| {
            // The neutral and semantic roles do not depend on the brand.
            install(Brand::neutral(), mode, Density::Default, cx);
            let theme = Theme::global(cx);

            for ((role, drawn), (_, read)) in
                drawn_roles(theme).into_iter().zip(role_list(&expected))
            {
                assert_eq!(read, drawn, "{mode:?} {role}");
            }
        });
    }
}

#[test]
fn semantic_roles_match_the_banner_foregrounds() {
    for (colors, banner) in [
        (Colors::dark(), BannerColors::dark()),
        (Colors::light(), BannerColors::light()),
    ] {
        assert_eq!(colors.success, banner.success_fg);
        assert_eq!(colors.info, banner.info_fg);
        assert_eq!(colors.warning, banner.warning_fg);
        assert_eq!(colors.danger, banner.error_fg);
    }
}

#[gpui::test]
fn the_accessor_follows_the_published_palette(cx: &mut TestAppContext) {
    cx.update(|cx| {
        assert_eq!(crate::colors(cx), Colors::dark());

        crate::palette::set_mode(cx, PaletteMode::Light);
        assert_eq!(crate::colors(cx), Colors::light());
        assert_eq!(Colors::for_current(cx), Colors::light());

        crate::palette::set_mode(cx, PaletteMode::System);
        assert_eq!(crate::colors(cx), Colors::dark());
    });
}
