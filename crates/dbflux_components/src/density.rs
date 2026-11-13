//! DBFlux's product shim over `bolt::density`.
//!
//! The density global and the `font_*` / `radius_*` accessors live in `bolt`;
//! they are re-exported here so existing call sites keep compiling. This
//! module maps the persisted `AppStyle` setting onto `bolt::density::Density`
//! and publishes the result.

use dbflux_core::AppStyle;
use gpui::App;

pub use bolt::density::{
    font_base, font_label, font_lg, font_sm, font_title, font_xl, font_xs, radius_lg, radius_md,
    radius_sm,
};

/// Register the density global. Call this once during app startup,
/// after `theme::init(cx)` and before any rendering occurs.
pub fn init(cx: &mut App, style: AppStyle) {
    bolt::density::init(cx, bolt_density(style));
}

/// Update the density global when the user changes the style setting at runtime.
pub fn set_style(cx: &mut App, style: AppStyle) {
    bolt::density::set(cx, bolt_density(style));
}

/// Maps the persisted style setting onto the shared `bolt` density tier.
pub fn bolt_density(style: AppStyle) -> bolt::density::Density {
    match style {
        AppStyle::Default => bolt::density::Density::Default,
        AppStyle::Compact => bolt::density::Density::Compact,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bolt::density;
    use gpui::TestAppContext;

    #[test]
    fn app_style_maps_onto_the_bolt_density() {
        assert_eq!(
            bolt_density(AppStyle::Default),
            bolt::density::Density::Default
        );
        assert_eq!(
            bolt_density(AppStyle::Compact),
            bolt::density::Density::Compact
        );
    }

    /// `init` and `set_style` publish the mapped tier into `bolt`'s global, so
    /// every `bolt::density` accessor follows the product's style setting.
    #[gpui::test]
    fn init_and_set_style_publish_the_mapped_density_into_bolt(cx: &mut TestAppContext) {
        cx.update(|cx| {
            init(cx, AppStyle::Default);
            assert_eq!(density::active(cx), bolt::density::Density::Default);
            assert_eq!(density::font_xs(cx), bolt::tokens::ui(12.0));

            set_style(cx, AppStyle::Compact);
            assert_eq!(density::active(cx), bolt::density::Density::Compact);
            assert_eq!(density::font_xs(cx), bolt::tokens::ui(11.0));
        });
    }
}
