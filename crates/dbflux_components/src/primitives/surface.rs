use gpui::prelude::*;
use gpui::{div, App, Hsla, Pixels};
use gpui_component::ActiveTheme;

use crate::tokens::Radii;

/// Background variant controlling the surface color.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SurfaceVariant {
    /// Main panel background — `theme.background`.
    Panel,
    /// Slightly elevated card — `theme.secondary`.
    Card,
    /// Popover / dropdown surface — `theme.popover`.
    Raised,
    /// Modal backdrop — semi-transparent black.
    Overlay,
}

fn variant_bg(variant: SurfaceVariant, theme: &gpui_component::Theme) -> Hsla {
    match variant {
        SurfaceVariant::Panel => theme.background,
        SurfaceVariant::Card => theme.secondary,
        SurfaceVariant::Raised => theme.popover,
        SurfaceVariant::Overlay => gpui::black().opacity(0.5),
    }
}

fn variant_radius(variant: SurfaceVariant) -> Pixels {
    match variant {
        SurfaceVariant::Panel => Radii::LG,
        SurfaceVariant::Card => Radii::LG,
        SurfaceVariant::Raised => Radii::MD,
        SurfaceVariant::Overlay => Radii::LG,
    }
}

/// Create a panel surface (`theme.background`, border, large radius).
///
/// Returns a `Div` so callers can chain `.shadow_lg()`, `.overflow_hidden()`,
/// `.child(...)`, and any other GPUI attributes. Chain `.rounded()` to
/// override the default radius.
pub fn surface_panel(cx: &App) -> gpui::Div {
    let theme = cx.theme();
    div()
        .bg(theme.background)
        .border_1()
        .border_color(theme.border)
        .rounded(Radii::LG)
}

/// Create a card surface (`theme.secondary`, border, large radius).
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
/// Chain `.rounded()` to override the default radius.
pub fn surface_card(cx: &App) -> gpui::Div {
    let theme = cx.theme();
    div()
        .bg(theme.secondary)
        .border_1()
        .border_color(theme.border)
        .rounded(Radii::LG)
}

/// Create a raised surface (`theme.popover`, border, medium radius).
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
/// Chain `.rounded()` to override the default radius.
pub fn surface_raised(cx: &App) -> gpui::Div {
    let theme = cx.theme();
    div()
        .bg(theme.popover)
        .border_1()
        .border_color(theme.border)
        .rounded(Radii::MD)
}

/// Create an overlay surface (semi-transparent black, no border, large radius).
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
/// Chain `.rounded()` to override the default radius.
pub fn surface_overlay(_cx: &App) -> gpui::Div {
    div().bg(gpui::black().opacity(0.5)).rounded(Radii::LG)
}

/// Create a surface with a specific variant.
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
pub fn surface(variant: SurfaceVariant, cx: &App) -> gpui::Div {
    let theme = cx.theme();
    let bg = variant_bg(variant, theme);
    let radius = variant_radius(variant);

    let mut el = div().bg(bg).rounded(radius);

    if variant != SurfaceVariant::Overlay {
        el = el.border_1().border_color(theme.border);
    }

    el
}

/// Returns the default overlay background color (semi-transparent black at 0.5 opacity).
///
/// Use this when building overlay containers that need their own element ID,
/// click handlers, or key contexts — situations where `surface_overlay` cannot
/// be used directly because it returns an un-styled `Div` without an ID.
pub fn overlay_bg() -> Hsla {
    gpui::black().opacity(0.5)
}
