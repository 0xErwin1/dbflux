use gpui::App;
use std::borrow::Cow;

pub struct BundledFontAsset {
    pub family: &'static str,
    pub file_name: &'static str,
    pub data: &'static [u8],
}

// Metrics sweep checklist (design-bundle-application S9b.5):
// - Button label vertical centering at Heights::BUTTON (30px)
// - Input baseline at Heights::INPUT (30px)
// - Sidebar group label uppercase tracking
// - Doc tab label truncation at Heights::TAB (36px)
// - Status-bar items at Heights::TOOLBAR (32px)
// - Modal body min-height 96px
// - Table row text at Heights::ROW (28px) / ROW_COMPACT (24px)
pub struct AppFonts;

impl AppFonts {
    /// Interface face: labels, buttons, menus, settings, body copy, tabs and tree rows.
    pub const INTERFACE: &'static str = "Archivo";
    /// Display face: uppercase section labels and large titles. Never used for body text.
    pub const DISPLAY: &'static str = "Archivo Expanded";
    /// Data face: grid cells, keys, queries, identifiers, key hints and numeric readouts.
    pub const MONO: &'static str = "JetBrains Mono";
    pub const MONO_FALLBACK: &'static str = "monospace";
}

pub const BUNDLED_FONT_ASSETS: [BundledFontAsset; 17] = [
    BundledFontAsset {
        family: AppFonts::INTERFACE,
        file_name: "Archivo-Regular.ttf",
        data: include_bytes!("../assets/fonts/Archivo-Regular.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::INTERFACE,
        file_name: "Archivo-Italic.ttf",
        data: include_bytes!("../assets/fonts/Archivo-Italic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::INTERFACE,
        file_name: "Archivo-Medium.ttf",
        data: include_bytes!("../assets/fonts/Archivo-Medium.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::INTERFACE,
        file_name: "Archivo-SemiBold.ttf",
        data: include_bytes!("../assets/fonts/Archivo-SemiBold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::INTERFACE,
        file_name: "Archivo-Bold.ttf",
        data: include_bytes!("../assets/fonts/Archivo-Bold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::INTERFACE,
        file_name: "Archivo-ExtraBold.ttf",
        data: include_bytes!("../assets/fonts/Archivo-ExtraBold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::DISPLAY,
        file_name: "ArchivoExpanded-Bold.ttf",
        data: include_bytes!("../assets/fonts/ArchivoExpanded-Bold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::DISPLAY,
        file_name: "ArchivoExpanded-ExtraBold.ttf",
        data: include_bytes!("../assets/fonts/ArchivoExpanded-ExtraBold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::DISPLAY,
        file_name: "ArchivoExpanded-Black.ttf",
        data: include_bytes!("../assets/fonts/ArchivoExpanded-Black.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-Regular.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-Regular.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-Italic.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-Italic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-Medium.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-Medium.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-MediumItalic.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-MediumItalic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-SemiBold.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-SemiBold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-SemiBoldItalic.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-SemiBoldItalic.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-Bold.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-Bold.ttf"),
    },
    BundledFontAsset {
        family: AppFonts::MONO,
        file_name: "JetBrainsMono-BoldItalic.ttf",
        data: include_bytes!("../assets/fonts/JetBrainsMono-BoldItalic.ttf"),
    },
];

pub fn bundled_font_data() -> Vec<Cow<'static, [u8]>> {
    BUNDLED_FONT_ASSETS
        .iter()
        .map(|font| Cow::Borrowed(font.data))
        .collect()
}

/// Load all bundled fonts into GPUI's text system.
///
/// Called once during UI theme initialization. If registration fails, keep
/// the app running so GPUI can fall back to system fonts instead of aborting
/// startup.
pub fn load_bundled_fonts(cx: &mut App) {
    if let Err(error) = cx.text_system().add_fonts(bundled_font_data()) {
        eprintln!("failed to register bundled UI fonts, falling back to system fonts: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::{AppFonts, BUNDLED_FONT_ASSETS, bundled_font_data};
    use gpui::{
        Bounds, DevicePixels, Font, FontId, FontRun, GlyphId, LineLayout, Pixels,
        PlatformTextSystem, Result, Size, TextRenderingMode, TextSystem, font,
    };
    use std::borrow::Cow;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

    /// A family the platform text system below does not have.
    #[derive(Debug)]
    struct MissingFont(String);

    impl std::fmt::Display for MissingFont {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "no font family named {:?}", self.0)
        }
    }

    impl std::error::Error for MissingFont {}

    /// A platform text system whose families arrive in the batches the test queues.
    ///
    /// The Linux platform starts its text system without the system fonts, the app
    /// registers its own faces, and the system fonts are installed later through
    /// `TextSystem::add_fonts`. A queued batch stands in for the bytes a real `add_fonts`
    /// parses, so the install path runs as it does in the app, cache clearing included.
    #[derive(Default)]
    struct QueuedFontSystem {
        installed: Mutex<Vec<&'static str>>,
        queued: Mutex<VecDeque<Vec<&'static str>>>,
    }

    impl QueuedFontSystem {
        /// Makes `families` arrive with the next `add_fonts` call.
        fn queue(&self, families: &[&'static str]) {
            self.batches().push_back(families.to_vec());
        }

        /// The batches the next `add_fonts` calls install.
        fn batches(&self) -> MutexGuard<'_, VecDeque<Vec<&'static str>>> {
            self.queued.lock().unwrap_or_else(PoisonError::into_inner)
        }

        /// The families installed so far.
        fn families(&self) -> MutexGuard<'_, Vec<&'static str>> {
            self.installed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        }
    }

    impl PlatformTextSystem for QueuedFontSystem {
        fn add_fonts(&self, _fonts: Vec<Cow<'static, [u8]>>) -> Result<()> {
            let arrived = self.batches().pop_front().unwrap_or_default();
            self.families().extend(arrived);
            Ok(())
        }

        fn all_font_names(&self) -> Vec<String> {
            self.families()
                .iter()
                .map(|family| (*family).to_string())
                .collect()
        }

        fn font_id(&self, descriptor: &Font) -> Result<FontId> {
            self.families()
                .iter()
                .position(|family| *family == descriptor.family.as_ref())
                .map(FontId)
                .ok_or_else(|| MissingFont(descriptor.family.to_string()).into())
        }

        fn font_metrics(&self, _font_id: FontId) -> gpui::FontMetrics {
            unreachable!("these tests resolve fonts, they never measure, shape or rasterize text")
        }

        fn typographic_bounds(&self, _font_id: FontId, _glyph_id: GlyphId) -> Result<Bounds<f32>> {
            unreachable!("these tests resolve fonts, they never measure, shape or rasterize text")
        }

        fn advance(&self, _font_id: FontId, _glyph_id: GlyphId) -> Result<Size<f32>> {
            unreachable!("these tests resolve fonts, they never measure, shape or rasterize text")
        }

        fn glyph_for_char(&self, _font_id: FontId, _ch: char) -> Option<GlyphId> {
            unreachable!("these tests resolve fonts, they never measure, shape or rasterize text")
        }

        fn glyph_raster_bounds(
            &self,
            _params: &gpui::RenderGlyphParams,
        ) -> Result<Bounds<DevicePixels>> {
            unreachable!("these tests resolve fonts, they never measure, shape or rasterize text")
        }

        fn rasterize_glyph(
            &self,
            _params: &gpui::RenderGlyphParams,
            _raster_bounds: Bounds<DevicePixels>,
        ) -> Result<(Size<DevicePixels>, Vec<u8>)> {
            unreachable!("these tests resolve fonts, they never measure, shape or rasterize text")
        }

        fn layout_line(&self, _text: &str, _font_size: Pixels, _runs: &[FontRun]) -> LineLayout {
            unreachable!("these tests resolve fonts, they never measure, shape or rasterize text")
        }

        fn recommended_rendering_mode(
            &self,
            _font_id: FontId,
            _font_size: Pixels,
        ) -> TextRenderingMode {
            unreachable!("these tests resolve fonts, they never measure, shape or rasterize text")
        }
    }

    /// A text system whose platform installs no font the test does not queue.
    fn deferred_font_system() -> (TextSystem, Arc<QueuedFontSystem>) {
        let platform = Arc::new(QueuedFontSystem::default());
        (TextSystem::new(platform.clone()), platform)
    }

    /// The family the text system resolved `font_id` from.
    fn resolved_family(text_system: &TextSystem, font_id: FontId) -> String {
        text_system
            .get_font_for_id(font_id)
            .expect("a family for a resolved font id")
            .family
            .to_string()
    }

    /// S3b: a family that is not installed yet, as the UI font is on the first frame of a
    /// platform that loads its system fonts in the background, resolves to an installed
    /// face instead of aborting the process.
    #[test]
    fn an_unresolved_family_uses_an_installed_face() {
        let (text_system, platform) = deferred_font_system();
        platform.queue(&[AppFonts::INTERFACE, AppFonts::MONO]);
        text_system
            .add_fonts(bundled_font_data())
            .expect("the bundled fonts");

        let resolved = text_system.resolve_font(&font("Inter"));

        assert_eq!(resolved_family(&text_system, resolved), AppFonts::INTERFACE);
    }

    /// S3b: the fallback is not cached past the arrival of the system fonts, so the next
    /// resolution is the family that was asked for.
    #[test]
    fn the_requested_family_wins_once_the_system_fonts_arrive() {
        let (text_system, platform) = deferred_font_system();
        platform.queue(&[AppFonts::INTERFACE, AppFonts::MONO]);
        text_system
            .add_fonts(bundled_font_data())
            .expect("the bundled fonts");
        let before = text_system.resolve_font(&font("Inter"));
        assert_eq!(resolved_family(&text_system, before), AppFonts::INTERFACE);

        platform.queue(&["Inter"]);
        text_system
            .add_fonts(vec![Cow::Borrowed(b"the system fonts")])
            .expect("the system fonts");

        let after = text_system.resolve_font(&font("Inter"));

        assert_eq!(resolved_family(&text_system, after), "Inter");
    }

    /// The default fallback stack still decides before any other installed family does.
    #[test]
    fn the_default_fallback_stack_is_tried_before_an_installed_family() {
        // "Noto Sans" is one of gpui's default fallbacks, "Archivo" is not.
        let (text_system, platform) = deferred_font_system();
        platform.queue(&[AppFonts::INTERFACE, "Noto Sans"]);
        text_system
            .add_fonts(bundled_font_data())
            .expect("the bundled fonts");

        let resolved = text_system.resolve_font(&font("Inter"));

        assert_eq!(resolved_family(&text_system, resolved), "Noto Sans");
    }

    /// The non-panicking probe gpui-component's theme asks with keeps reporting a stack the
    /// installed families do not cover, so the theme can ask again once they arrive.
    #[test]
    fn try_resolve_font_reports_a_stack_no_installed_family_covers() {
        let (text_system, platform) = deferred_font_system();
        platform.queue(&[AppFonts::INTERFACE, AppFonts::MONO]);
        text_system
            .add_fonts(bundled_font_data())
            .expect("the bundled fonts");

        assert_eq!(text_system.try_resolve_font(&font("Inter")), None);
    }

    /// With no font installed at all there is nothing to draw text with, so the failure
    /// stays fatal and says what is wrong.
    #[test]
    #[should_panic(expected = "no font is installed")]
    fn resolving_with_no_installed_font_still_fails_loudly() {
        let (text_system, _platform) = deferred_font_system();

        text_system.resolve_font(&font("Inter"));
    }

    #[test]
    fn font_contract_splits_interface_display_and_data_families() {
        assert_eq!(AppFonts::INTERFACE, "Archivo");
        assert_eq!(AppFonts::DISPLAY, "Archivo Expanded");
        assert_eq!(AppFonts::MONO, "JetBrains Mono");
        assert_eq!(AppFonts::MONO_FALLBACK, "monospace");
    }

    #[test]
    fn bundled_assets_cover_every_family() {
        let assets: Vec<_> = BUNDLED_FONT_ASSETS
            .iter()
            .map(|asset| (asset.family, asset.file_name))
            .collect();

        assert_eq!(
            assets,
            vec![
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
            ]
        );
    }
}
