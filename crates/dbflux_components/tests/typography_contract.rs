use dbflux_components::typography::{AppFonts, BUNDLED_FONT_ASSETS, bundled_font_data};

#[test]
fn app_fonts_define_shared_family_contract() {
    assert_eq!(AppFonts::BODY, "JetBrains Mono");
    assert_eq!(AppFonts::HEADLINE, "JetBrains Mono");
    assert_eq!(AppFonts::MONO, "JetBrains Mono");
    assert_eq!(AppFonts::MONO_FALLBACK, "monospace");
    assert_eq!(AppFonts::CODE, AppFonts::MONO);
    assert_eq!(AppFonts::SHORTCUT, AppFonts::MONO);
}

#[test]
fn bundled_font_data_registers_all_shared_font_assets() {
    let bundled_fonts = bundled_font_data();

    let expected_assets = [
        (AppFonts::BODY, "JetBrainsMono-Regular.ttf"),
        (AppFonts::BODY, "JetBrainsMono-Italic.ttf"),
        (AppFonts::BODY, "JetBrainsMono-Medium.ttf"),
        (AppFonts::BODY, "JetBrainsMono-MediumItalic.ttf"),
        (AppFonts::BODY, "JetBrainsMono-SemiBold.ttf"),
        (AppFonts::BODY, "JetBrainsMono-SemiBoldItalic.ttf"),
        (AppFonts::BODY, "JetBrainsMono-Bold.ttf"),
        (AppFonts::BODY, "JetBrainsMono-BoldItalic.ttf"),
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
    assert!(
        BUNDLED_FONT_ASSETS
            .iter()
            .all(|asset| asset.family == AppFonts::BODY)
    );
}
