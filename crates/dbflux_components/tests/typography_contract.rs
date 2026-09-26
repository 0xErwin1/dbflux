use dbflux_components::typography::{AppFonts, BUNDLED_FONT_ASSETS, bundled_font_data};
use std::path::Path;

const FONT_DIRECTORY: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/assets/fonts");

#[test]
fn app_fonts_define_shared_family_contract() {
    assert_eq!(AppFonts::INTERFACE, "Archivo");
    assert_eq!(AppFonts::DISPLAY, "Archivo Expanded");
    assert_eq!(AppFonts::MONO, "JetBrains Mono");
    assert_eq!(AppFonts::MONO_FALLBACK, "monospace");
}

#[test]
fn bundled_font_data_registers_all_shared_font_assets() {
    let bundled_fonts = bundled_font_data();

    let expected_assets = [
        (AppFonts::INTERFACE, "Archivo-Regular.ttf"),
        (AppFonts::INTERFACE, "Archivo-Italic.ttf"),
        (AppFonts::INTERFACE, "Archivo-Medium.ttf"),
        (AppFonts::INTERFACE, "Archivo-SemiBold.ttf"),
        (AppFonts::INTERFACE, "Archivo-Bold.ttf"),
        (AppFonts::INTERFACE, "Archivo-ExtraBold.ttf"),
        (AppFonts::DISPLAY, "ArchivoExpanded-Bold.ttf"),
        (AppFonts::DISPLAY, "ArchivoExpanded-ExtraBold.ttf"),
        (AppFonts::DISPLAY, "ArchivoExpanded-Black.ttf"),
        (AppFonts::MONO, "JetBrainsMono-Regular.ttf"),
        (AppFonts::MONO, "JetBrainsMono-Italic.ttf"),
        (AppFonts::MONO, "JetBrainsMono-Medium.ttf"),
        (AppFonts::MONO, "JetBrainsMono-MediumItalic.ttf"),
        (AppFonts::MONO, "JetBrainsMono-SemiBold.ttf"),
        (AppFonts::MONO, "JetBrainsMono-SemiBoldItalic.ttf"),
        (AppFonts::MONO, "JetBrainsMono-Bold.ttf"),
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

    for family in [AppFonts::INTERFACE, AppFonts::DISPLAY, AppFonts::MONO] {
        assert!(
            BUNDLED_FONT_ASSETS
                .iter()
                .any(|asset| asset.family == family),
            "{family} has no bundled font file"
        );
    }
}

#[test]
fn every_bundled_font_file_exists_on_disk() {
    for asset in BUNDLED_FONT_ASSETS.iter() {
        let path = Path::new(FONT_DIRECTORY).join(asset.file_name);
        assert!(path.is_file(), "{} is missing", path.display());
    }
}

#[test]
fn bundled_font_families_ship_their_licenses() {
    let licenses = [
        ("Archivo-OFL.txt", "The Archivo Project Authors"),
        (
            "JetBrainsMono-OFL.txt",
            "The JetBrains Mono Project Authors",
        ),
    ];

    for (file_name, copyright_holder) in licenses {
        let path = Path::new(FONT_DIRECTORY).join(file_name);
        let license = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} is unreadable: {error}", path.display()));

        assert!(license.contains(copyright_holder), "{file_name}");
        assert!(
            license.contains("SIL OPEN FONT LICENSE Version 1.1"),
            "{file_name}"
        );
    }
}

#[test]
fn letter_spacing_widens_shaped_text_by_one_step_per_character() {
    use gpui::{NoopTextSystem, TextSystem, WindowTextSystem, font, px};
    use std::sync::Arc;

    let text_system =
        WindowTextSystem::new(Arc::new(TextSystem::new(Arc::new(NoopTextSystem::new()))));
    let text: gpui::SharedString = "SECTION".into();
    let runs = [gpui::TextRun {
        len: text.len(),
        font: font(AppFonts::DISPLAY),
        color: gpui::black(),
        background_color: None,
        underline: None,
        strikethrough: None,
    }];

    let shape = |letter_spacing| {
        text_system
            .shape_text_with_letter_spacing(
                text.clone(),
                px(11.),
                &runs,
                None,
                None,
                letter_spacing,
            )
            .expect("text should shape")
    };

    let plain = shape(px(0.));
    let spaced = shape(px(1.5));
    let default_shape = text_system
        .shape_text(text.clone(), px(11.), &runs, None, None)
        .expect("text should shape");

    assert_eq!(default_shape[0].width(), plain[0].width());
    assert_eq!(
        spaced[0].width(),
        plain[0].width() + px(1.5) * text.chars().count() as f32
    );

    let plain_positions: Vec<_> = plain[0].unwrapped_layout.runs[0]
        .glyphs
        .iter()
        .map(|glyph| glyph.position.x)
        .collect();
    let spaced_positions: Vec<_> = spaced[0].unwrapped_layout.runs[0]
        .glyphs
        .iter()
        .map(|glyph| glyph.position.x)
        .collect();

    assert_eq!(plain_positions.len(), text.chars().count());

    for (index, (plain_x, spaced_x)) in plain_positions.iter().zip(&spaced_positions).enumerate() {
        assert_eq!(*spaced_x, *plain_x + px(1.5) * index as f32);
    }
}
