use dbflux_components::typography::{AppFonts, BUNDLED_FONT_ASSETS, bundled_font_data};

#[test]
fn app_fonts_define_shared_family_contract() {
    assert_eq!(AppFonts::BODY, "Inter");
    assert_eq!(AppFonts::HEADLINE, "Space Grotesk");
    assert_eq!(AppFonts::MONO, "JetBrains Mono");
    assert_eq!(AppFonts::MONO_FALLBACK, "monospace");
    assert_eq!(AppFonts::CODE, AppFonts::MONO);
    assert_eq!(AppFonts::SHORTCUT, AppFonts::MONO);
}

#[test]
fn bundled_font_data_registers_all_shared_font_assets() {
    let bundled_fonts = bundled_font_data();

    let expected_assets = [
        (AppFonts::BODY, "Inter-Variable.ttf"),
        (AppFonts::BODY, "Inter-Variable-Italic.ttf"),
        (AppFonts::HEADLINE, "SpaceGrotesk-Regular.ttf"),
        (AppFonts::HEADLINE, "SpaceGrotesk-Bold.ttf"),
        (AppFonts::MONO, "JetBrainsMono-Regular.ttf"),
        (AppFonts::MONO, "JetBrainsMono-Bold.ttf"),
        (AppFonts::MONO, "JetBrainsMono-Italic.ttf"),
        (AppFonts::MONO, "JetBrainsMono-BoldItalic.ttf"),
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
        2
    );
    assert_eq!(
        BUNDLED_FONT_ASSETS
            .iter()
            .filter(|asset| asset.family == AppFonts::HEADLINE)
            .count(),
        2
    );
    assert_eq!(
        BUNDLED_FONT_ASSETS
            .iter()
            .filter(|asset| asset.family == AppFonts::MONO)
            .count(),
        4
    );
}
