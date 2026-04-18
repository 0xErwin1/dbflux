use dbflux_components::typography::{bundled_font_data, AppFonts, BUNDLED_FONT_ASSETS};

#[test]
fn app_fonts_define_shared_family_contract() {
    assert_eq!(AppFonts::BODY, "IBM Plex Mono");
    assert_eq!(AppFonts::HEADLINE, "IBM Plex Mono");
    assert_eq!(AppFonts::MONO, "IBM Plex Mono");
    assert_eq!(AppFonts::MONO_FALLBACK, "monospace");
    assert_eq!(AppFonts::CODE, AppFonts::MONO);
    assert_eq!(AppFonts::SHORTCUT, AppFonts::MONO);
}

#[test]
fn bundled_font_data_registers_all_shared_font_assets() {
    let bundled_fonts = bundled_font_data();

    let expected_assets = [
        (AppFonts::BODY, "IBMPlexMono-Regular.ttf"),
        (AppFonts::BODY, "IBMPlexMono-Italic.ttf"),
        (AppFonts::BODY, "IBMPlexMono-Medium.ttf"),
        (AppFonts::BODY, "IBMPlexMono-MediumItalic.ttf"),
        (AppFonts::BODY, "IBMPlexMono-SemiBold.ttf"),
        (AppFonts::BODY, "IBMPlexMono-SemiBoldItalic.ttf"),
        (AppFonts::BODY, "IBMPlexMono-Bold.ttf"),
        (AppFonts::BODY, "IBMPlexMono-BoldItalic.ttf"),
    ];

    let actual_assets: Vec<_> = BUNDLED_FONT_ASSETS
        .iter()
        .map(|asset| (asset.family, asset.file_name))
        .collect();

    assert_eq!(actual_assets, expected_assets);
    assert_eq!(bundled_fonts.len(), expected_assets.len());

    for (asset, bundled_font) in BUNDLED_FONT_ASSETS.iter().zip(bundled_fonts.iter()) {
        assert_eq!(
            bundled_font.as_ref(),
            asset.data,
            "{} bytes changed",
            asset.file_name
        );
        assert!(
            asset.data.len() > 1_024,
            "{} looks truncated",
            asset.file_name
        );
    }

    assert_eq!(
        BUNDLED_FONT_ASSETS
            .iter()
            .filter(|asset| asset.family == AppFonts::BODY)
            .count(),
        expected_assets.len()
    );
    assert!(BUNDLED_FONT_ASSETS
        .iter()
        .all(|asset| asset.family == AppFonts::BODY));
}
