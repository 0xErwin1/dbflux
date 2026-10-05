//! Runtime font settings: the interface, editor and data-grid families and
//! sizes the user chose in Settings > General.
//!
//! `FontSettings` is the resolved form of the persisted
//! `dbflux_core::GeneralSettings` font fields: sizes are clamped and every
//! family is one the text system can load. It is stored in the
//! `FontSettingsGlobal` cx-global, and the accessors below read it with a
//! fallback to `FontSettings::default()`, which reproduces the bundled fonts
//! and sizes DBFlux rendered before the settings existed.

use std::sync::{Arc, LazyLock, OnceLock};

use dbflux_core::GeneralSettings;
use gpui::{App, Global, Pixels, Rems, SharedString, px};

use crate::tokens::{BASE_REM, EditorMetrics, GridMetrics};
use crate::typography::{AppFonts, BUNDLED_FONT_ASSETS};

/// Height of a record-mode field row at the default grid size.
const RECORD_ROW_HEIGHT: f32 = 30.0;

/// Advance of a JetBrains Mono glyph as a fraction of the font size.
const MONO_ADVANCE_EM: f32 = 0.6;

static DEFAULT_FONT_SETTINGS: LazyLock<FontSettings> = LazyLock::new(FontSettings::default);

/// Resolved font families and sizes for the running app.
#[derive(Debug, Clone, PartialEq)]
pub struct FontSettings {
    pub ui_family: SharedString,
    pub ui_size: f32,
    pub editor_family: SharedString,
    pub editor_size: f32,
    pub grid_family: SharedString,
    pub grid_size: f32,
}

impl Default for FontSettings {
    fn default() -> Self {
        Self {
            ui_family: SharedString::from(AppFonts::INTERFACE),
            ui_size: GeneralSettings::DEFAULT_UI_FONT_SIZE,
            editor_family: SharedString::from(AppFonts::MONO),
            editor_size: GeneralSettings::DEFAULT_EDITOR_FONT_SIZE,
            grid_family: SharedString::from(AppFonts::MONO),
            grid_size: GeneralSettings::DEFAULT_GRID_FONT_SIZE,
        }
    }
}

impl FontSettings {
    /// Resolves the persisted settings against the installed font families.
    ///
    /// Sizes are clamped to the supported range. A family that is neither
    /// bundled nor in `installed` falls back to the bundled default, because
    /// laying out text in a family the text system cannot find panics. The
    /// grid uses the resolved editor family unless its own family is set and
    /// loadable.
    pub fn from_general(settings: &GeneralSettings, installed: &[SharedString]) -> Self {
        let editor_family = resolve_family(
            &settings.editor_font_family,
            SharedString::from(AppFonts::MONO),
            installed,
        );
        let grid_family =
            resolve_family(&settings.grid_font_family, editor_family.clone(), installed);
        let ui_family = resolve_family(
            &settings.ui_font_family,
            SharedString::from(AppFonts::INTERFACE),
            installed,
        );

        Self {
            ui_family,
            ui_size: GeneralSettings::clamp_font_size(
                settings.ui_font_size,
                GeneralSettings::DEFAULT_UI_FONT_SIZE,
            ),
            editor_family,
            editor_size: GeneralSettings::clamp_font_size(
                settings.editor_font_size,
                GeneralSettings::DEFAULT_EDITOR_FONT_SIZE,
            ),
            grid_family,
            grid_size: GeneralSettings::clamp_font_size(
                settings.grid_font_size,
                GeneralSettings::DEFAULT_GRID_FONT_SIZE,
            ),
        }
    }
}

/// GPUI global holding the active `FontSettings`.
#[derive(Debug, Clone, PartialEq)]
pub struct FontSettingsGlobal {
    pub settings: FontSettings,
}

impl Global for FontSettingsGlobal {}

/// Registers the font global. Call once during startup, before the theme is
/// applied and before the first window opens.
pub fn init(cx: &mut App, settings: FontSettings) {
    cx.set_global(FontSettingsGlobal { settings });
}

/// Replaces the active font settings. Returns `true` when they changed.
pub fn set(cx: &mut App, settings: FontSettings) -> bool {
    let changed = *active(cx) != settings;
    cx.set_global(FontSettingsGlobal { settings });
    changed
}

/// The active font settings, or the defaults when the global is absent.
pub fn current(cx: &App) -> FontSettings {
    active(cx).clone()
}

/// Every font family the platform reports, listed once per process.
///
/// Enumerating fonts is slow (around 100 ms on some platforms) and the
/// system font set does not change while the process runs, so the first
/// call's answer is reused.
pub fn installed_font_names(cx: &App) -> Arc<[SharedString]> {
    static INSTALLED: OnceLock<Arc<[SharedString]>> = OnceLock::new();

    INSTALLED
        .get_or_init(|| {
            cx.text_system()
                .all_font_names()
                .into_iter()
                .map(SharedString::from)
                .collect()
        })
        .clone()
}

/// Interface scale factor: the chosen interface size over the 13 px default.
pub fn ui_scale(cx: &App) -> f32 {
    active(cx).ui_size / GeneralSettings::DEFAULT_UI_FONT_SIZE
}

/// Scales an interface measurement by `ui_scale`.
pub fn scaled(cx: &App, value: Pixels) -> Pixels {
    value * ui_scale(cx)
}

/// Resolves an interface length in rems to pixels at the current interface
/// size, for code that does pixel arithmetic outside layout. It matches the
/// rem size `Root` applies to every window.
pub fn ui_px(cx: &App, value: Rems) -> Pixels {
    value.to_pixels(px(BASE_REM) * ui_scale(cx))
}

pub fn ui_family(cx: &App) -> SharedString {
    active(cx).ui_family.clone()
}

pub fn editor_family(cx: &App) -> SharedString {
    active(cx).editor_family.clone()
}

pub fn editor_font_size(cx: &App) -> Pixels {
    px(active(cx).editor_size)
}

/// Code line height, keeping the 22 px on 13 px proportion of the default
/// editor, rounded to whole pixels.
pub fn editor_line_height(cx: &App) -> Pixels {
    let size = active(cx).editor_size;
    let ratio_numerator = f32::from(EditorMetrics::CODE_LINE_HEIGHT);
    let ratio_denominator = f32::from(EditorMetrics::CODE_FONT);

    px((size * ratio_numerator / ratio_denominator).round())
}

pub fn grid_family(cx: &App) -> SharedString {
    active(cx).grid_family.clone()
}

/// Grid cell text and column name size.
pub fn grid_font_size(cx: &App) -> Pixels {
    px(active(cx).grid_size)
}

/// Grid column type size.
pub fn grid_type_font_size(cx: &App) -> Pixels {
    GridMetrics::TYPE_FONT * grid_scale(cx)
}

/// Grid data row height, its 1 px divider included.
pub fn grid_row_height(cx: &App) -> Pixels {
    scaled_grid_height(cx, f32::from(GridMetrics::ROW_HEIGHT))
}

/// Grid header row height, its bottom edge included.
pub fn grid_header_height(cx: &App) -> Pixels {
    scaled_grid_height(cx, f32::from(GridMetrics::HEADER_HEIGHT))
}

/// Minimum height of a record-mode field row.
pub fn grid_record_row_height(cx: &App) -> Pixels {
    scaled_grid_height(cx, RECORD_ROW_HEIGHT)
}

/// Scales a grid measurement drawn for the default grid size, such as a
/// header strip height, rounded to whole pixels.
pub fn grid_scaled(cx: &App, value: Pixels) -> Pixels {
    scaled_grid_height(cx, f32::from(value))
}

/// Advance of one grid cell character, in pixels.
pub fn grid_char_advance(cx: &App) -> f32 {
    active(cx).grid_size * MONO_ADVANCE_EM
}

/// Advance of one column type character, in pixels.
pub fn grid_type_char_advance(cx: &App) -> f32 {
    f32::from(grid_type_font_size(cx)) * MONO_ADVANCE_EM
}

fn active(cx: &App) -> &FontSettings {
    cx.try_global::<FontSettingsGlobal>()
        .map(|global| &global.settings)
        .unwrap_or(&DEFAULT_FONT_SETTINGS)
}

fn grid_scale(cx: &App) -> f32 {
    active(cx).grid_size / GeneralSettings::DEFAULT_GRID_FONT_SIZE
}

fn scaled_grid_height(cx: &App, default_height: f32) -> Pixels {
    px((default_height * grid_scale(cx)).round())
}

/// The normalized `family` when the text system can load it, else `fallback`.
fn resolve_family(
    family: &Option<String>,
    fallback: SharedString,
    installed: &[SharedString],
) -> SharedString {
    let Some(family) = GeneralSettings::normalize_font_family(family.clone()) else {
        return fallback;
    };

    let bundled = BUNDLED_FONT_ASSETS
        .iter()
        .any(|asset| asset.family == family);
    let is_installed = installed.iter().any(|name| name.as_ref() == family);

    if bundled || is_installed {
        SharedString::from(family)
    } else {
        fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::ui;
    use gpui::TestAppContext;

    fn installed(names: &[&str]) -> Vec<SharedString> {
        names.iter().map(|name| SharedString::from(*name)).collect()
    }

    fn general() -> GeneralSettings {
        GeneralSettings::default()
    }

    #[test]
    fn defaults_match_the_bundled_fonts_and_sizes() {
        let settings = FontSettings::default();

        assert_eq!(settings.ui_family, SharedString::from(AppFonts::INTERFACE));
        assert_eq!(settings.ui_size, 13.0);
        assert_eq!(settings.editor_family, SharedString::from(AppFonts::MONO));
        assert_eq!(settings.editor_size, 13.0);
        assert_eq!(settings.grid_family, SharedString::from(AppFonts::MONO));
        assert_eq!(settings.grid_size, 12.5);
    }

    #[test]
    fn default_general_settings_resolve_to_the_defaults() {
        let resolved = FontSettings::from_general(&general(), &[]);

        assert_eq!(resolved, FontSettings::default());
    }

    #[test]
    fn installed_and_bundled_families_are_kept() {
        let mut settings = general();
        settings.ui_font_family = Some("  Inter ".to_string());
        settings.editor_font_family = Some("Fira Code".to_string());
        settings.grid_font_family = Some(AppFonts::INTERFACE.to_string());

        let resolved = FontSettings::from_general(&settings, &installed(&["Fira Code", "Inter"]));

        assert_eq!(resolved.ui_family, SharedString::from("Inter"));
        assert_eq!(resolved.editor_family, SharedString::from("Fira Code"));
        assert_eq!(
            resolved.grid_family,
            SharedString::from(AppFonts::INTERFACE)
        );
    }

    #[test]
    fn unknown_families_fall_back_to_the_bundled_defaults() {
        let mut settings = general();
        settings.ui_font_family = Some("Missing Sans".to_string());
        settings.editor_font_family = Some("Missing Mono".to_string());
        settings.grid_font_family = Some("Missing Grid".to_string());

        let resolved = FontSettings::from_general(&settings, &installed(&["Inter"]));

        assert_eq!(resolved.ui_family, SharedString::from(AppFonts::INTERFACE));
        assert_eq!(resolved.editor_family, SharedString::from(AppFonts::MONO));
        assert_eq!(resolved.grid_family, SharedString::from(AppFonts::MONO));
    }

    #[test]
    fn unknown_grid_family_falls_back_to_the_editor_family() {
        let mut settings = general();
        settings.editor_font_family = Some("Fira Code".to_string());
        settings.grid_font_family = Some("Missing Grid".to_string());

        let resolved = FontSettings::from_general(&settings, &installed(&["Fira Code"]));

        assert_eq!(resolved.grid_family, SharedString::from("Fira Code"));
    }

    #[test]
    fn grid_without_a_family_uses_the_editor_family() {
        let mut settings = general();
        settings.editor_font_family = Some("Fira Code".to_string());
        settings.grid_font_family = Some("   ".to_string());

        let resolved = FontSettings::from_general(&settings, &installed(&["Fira Code"]));

        assert_eq!(resolved.grid_family, SharedString::from("Fira Code"));
    }

    #[test]
    fn sizes_are_clamped_and_non_finite_sizes_use_the_defaults() {
        let mut settings = general();
        settings.ui_font_size = 2.0;
        settings.editor_font_size = 99.0;
        settings.grid_font_size = f32::NAN;

        let resolved = FontSettings::from_general(&settings, &[]);

        assert_eq!(resolved.ui_size, GeneralSettings::MIN_FONT_SIZE);
        assert_eq!(resolved.editor_size, GeneralSettings::MAX_FONT_SIZE);
        assert_eq!(resolved.grid_size, GeneralSettings::DEFAULT_GRID_FONT_SIZE);
    }

    #[gpui::test]
    fn accessors_without_the_global_return_todays_constants(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert_eq!(current(cx), FontSettings::default());
            assert_eq!(ui_scale(cx), 1.0);
            assert_eq!(scaled(cx, px(11.0)), px(11.0));
            assert_eq!(ui_family(cx), SharedString::from(AppFonts::INTERFACE));
            assert_eq!(editor_family(cx), SharedString::from(AppFonts::MONO));
            assert_eq!(editor_font_size(cx), EditorMetrics::CODE_FONT);
            assert_eq!(editor_line_height(cx), EditorMetrics::CODE_LINE_HEIGHT);
            assert_eq!(grid_family(cx), SharedString::from(AppFonts::MONO));
            assert_eq!(grid_font_size(cx), GridMetrics::FONT);
            assert_eq!(grid_type_font_size(cx), GridMetrics::TYPE_FONT);
            assert_eq!(grid_row_height(cx), GridMetrics::ROW_HEIGHT);
            assert_eq!(grid_header_height(cx), GridMetrics::HEADER_HEIGHT);
            assert_eq!(grid_record_row_height(cx), px(30.0));
            assert_eq!(grid_char_advance(cx), f32::from(GridMetrics::FONT) * 0.6);
            assert_eq!(grid_type_char_advance(cx), 10.5 * 0.6);
        });
    }

    #[gpui::test]
    fn initialized_defaults_return_todays_constants(cx: &mut TestAppContext) {
        cx.update(|cx| {
            init(cx, FontSettings::default());

            assert_eq!(ui_scale(cx), 1.0);
            assert_eq!(editor_line_height(cx), px(22.0));
            assert_eq!(grid_row_height(cx), px(31.0));
            assert_eq!(grid_header_height(cx), px(40.0));
        });
    }

    #[gpui::test]
    fn sizes_scale_metrics_and_round_heights(cx: &mut TestAppContext) {
        cx.update(|cx| {
            init(
                cx,
                FontSettings {
                    ui_size: 19.5,
                    editor_size: 20.0,
                    grid_size: 25.0,
                    ..FontSettings::default()
                },
            );

            assert_eq!(ui_scale(cx), 1.5);
            assert_eq!(scaled(cx, px(10.0)), px(15.0));
            assert_eq!(editor_font_size(cx), px(20.0));
            assert_eq!(editor_line_height(cx), px(34.0));
            assert_eq!(grid_font_size(cx), px(25.0));
            assert_eq!(grid_type_font_size(cx), px(21.0));
            assert_eq!(grid_row_height(cx), px(62.0));
            assert_eq!(grid_header_height(cx), px(80.0));
            assert_eq!(grid_record_row_height(cx), px(60.0));
            assert_eq!(grid_char_advance(cx), 25.0 * 0.6);
            assert_eq!(grid_type_char_advance(cx), 21.0 * 0.6);
        });
    }

    #[gpui::test]
    fn ui_px_resolves_rems_at_the_default_interface_size(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert_eq!(ui_px(cx, ui(30.0)), px(30.0));
            assert_eq!(ui_px(cx, ui(13.0)), px(13.0));
        });
    }

    #[gpui::test]
    fn ui_px_follows_the_interface_scale(cx: &mut TestAppContext) {
        cx.update(|cx| {
            init(
                cx,
                FontSettings {
                    ui_size: 26.0,
                    ..FontSettings::default()
                },
            );

            assert_eq!(ui_px(cx, ui(30.0)), px(60.0));
            assert_eq!(ui_px(cx, ui(13.0)), px(26.0));
        });
    }

    #[gpui::test]
    fn set_reports_whether_the_settings_changed(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert!(!set(cx, FontSettings::default()));

            let larger = FontSettings {
                ui_size: 16.0,
                ..FontSettings::default()
            };
            assert!(set(cx, larger.clone()));
            assert!(!set(cx, larger.clone()));
            assert_eq!(current(cx), larger);
        });
    }

    #[gpui::test]
    fn installed_font_names_are_listed(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let first = installed_font_names(cx);
            let second = installed_font_names(cx);

            assert!(Arc::ptr_eq(&first, &second));
        });
    }
}
