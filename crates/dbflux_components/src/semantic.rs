//! DBFlux's product shim over `bolt::semantic`.
//!
//! The semantic token sets (banners, data-grid row states, chart chrome) live
//! in `bolt`; they are re-exported here so existing call sites keep compiling.
//! This module adds only the product global that tracks the persisted
//! `ThemeSetting`, which the theme adapter publishes on every apply.

pub use bolt::semantic::{BannerColors, ChartColors, RowStateColors};

use dbflux_core::ThemeSetting;
use gpui::{App, Global};

/// GPUI global tracking the persisted `ThemeSetting` the adapter applied.
///
/// The palette itself is published to `bolt` by `bolt::theme::apply_theme`;
/// semantic color accessors read that. This global records the product-side
/// setting so the adapter and its tests can assert what is on screen.
#[derive(Debug, Clone, Copy)]
pub struct ThemeSettingGlobal {
    pub setting: ThemeSetting,
}

impl Global for ThemeSettingGlobal {}

impl ThemeSettingGlobal {
    /// Register (or update) the active `ThemeSetting` in the GPUI context.
    pub fn set(cx: &mut App, setting: ThemeSetting) {
        cx.set_global(ThemeSettingGlobal { setting });
    }

    /// Read the active `ThemeSetting`. Falls back to `ThemeSetting::Dark` when
    /// the global has not been registered.
    pub fn get(cx: &App) -> ThemeSetting {
        cx.try_global::<Self>()
            .map(|global| global.setting)
            .unwrap_or(ThemeSetting::Dark)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::TestAppContext;

    #[gpui::test]
    fn theme_setting_global_falls_back_to_dark_when_absent(cx: &mut TestAppContext) {
        cx.update(|cx| {
            assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Dark);
        });
    }

    #[gpui::test]
    fn theme_setting_global_roundtrips_all_variants(cx: &mut TestAppContext) {
        cx.update(|cx| {
            ThemeSettingGlobal::set(cx, ThemeSetting::Light);
            assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Light);

            ThemeSettingGlobal::set(cx, ThemeSetting::Dark);
            assert_eq!(ThemeSettingGlobal::get(cx), ThemeSetting::Dark);
        });
    }
}
