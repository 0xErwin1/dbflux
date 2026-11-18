//! Bridges the persisted font settings onto Bolt's typography values.
//!
//! The settings type belongs to the product; the clamping and the font-family
//! resolution belong to Bolt. This module is the only place both sides meet.

use bolt::fonts::{self, FontChoices, FontSettings};
use dbflux_core::GeneralSettings;
use gpui::App;

/// The Bolt choices a persisted settings value asks for.
pub fn choices(settings: &GeneralSettings) -> FontChoices {
    FontChoices {
        ui_family: settings.ui_font_family.clone(),
        ui_size: settings.ui_font_size,
        editor_family: settings.editor_font_family.clone(),
        editor_size: settings.editor_font_size,
        grid_family: settings.grid_font_family.clone(),
        grid_size: settings.grid_font_size,
    }
}

/// The font settings for `settings`, resolved against the fonts this machine has.
pub fn resolve(settings: &GeneralSettings, cx: &App) -> FontSettings {
    FontSettings::resolve(&choices(settings), &fonts::installed_font_names(cx))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The persisted bounds and Bolt's bounds are declared in two crates, so a change
    /// to either has to change both. A settings value the product accepts must be one
    /// Bolt draws.
    #[test]
    fn bolt_and_the_persisted_settings_agree_on_the_supported_range() {
        assert_eq!(fonts::MIN_FONT_SIZE, GeneralSettings::MIN_FONT_SIZE);
        assert_eq!(fonts::MAX_FONT_SIZE, GeneralSettings::MAX_FONT_SIZE);
        assert_eq!(
            fonts::DEFAULT_UI_FONT_SIZE,
            GeneralSettings::DEFAULT_UI_FONT_SIZE
        );
        assert_eq!(
            fonts::DEFAULT_EDITOR_FONT_SIZE,
            GeneralSettings::DEFAULT_EDITOR_FONT_SIZE
        );
        assert_eq!(
            fonts::DEFAULT_GRID_FONT_SIZE,
            GeneralSettings::DEFAULT_GRID_FONT_SIZE
        );
    }

    /// Every field of the persisted settings reaches Bolt, so a new field cannot be
    /// silently dropped on the way to the typography.
    #[test]
    fn every_persisted_font_field_reaches_bolt() {
        let settings = GeneralSettings {
            ui_font_family: Some("Inter".to_string()),
            ui_font_size: 15.0,
            editor_font_family: Some("Fira Code".to_string()),
            editor_font_size: 14.0,
            grid_font_family: Some("Iosevka".to_string()),
            grid_font_size: 11.0,
            ..GeneralSettings::default()
        };

        let choices = choices(&settings);

        assert_eq!(choices.ui_family.as_deref(), Some("Inter"));
        assert_eq!(choices.ui_size, 15.0);
        assert_eq!(choices.editor_family.as_deref(), Some("Fira Code"));
        assert_eq!(choices.editor_size, 14.0);
        assert_eq!(choices.grid_family.as_deref(), Some("Iosevka"));
        assert_eq!(choices.grid_size, 11.0);
    }

    /// The persisted defaults resolve to the bundled typography, so a fresh install
    /// renders the design system's own fonts.
    #[test]
    fn the_persisted_defaults_resolve_to_the_bundled_typography() {
        let resolved = FontSettings::resolve(&choices(&GeneralSettings::default()), &[]);

        assert_eq!(resolved, FontSettings::default());
    }
}
