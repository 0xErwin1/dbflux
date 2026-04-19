use gpui::prelude::*;
use gpui::{App, Hsla, Pixels, div};
use gpui_component::ActiveTheme;

use crate::tokens::Radii;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SurfaceRole {
    Panel,
    Card,
    Raised,
    Scrim,
    ModalContainer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SurfaceThemeColorSlot {
    Background,
    Secondary,
    Popover,
    Overlay,
}

impl SurfaceThemeColorSlot {
    pub fn resolve(self, theme: &gpui_component::Theme) -> Hsla {
        match self {
            Self::Background => theme.background,
            Self::Secondary => theme.secondary,
            Self::Popover => theme.popover,
            Self::Overlay => theme.overlay.opacity(0.5),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SurfaceInspection {
    pub background: SurfaceThemeColorSlot,
    pub has_border: bool,
    pub radius: Pixels,
}

impl SurfaceInspection {
    pub fn resolve_background_color(self, theme: &gpui_component::Theme) -> Hsla {
        self.background.resolve(theme)
    }
}

pub fn inspect_surface_role(role: SurfaceRole) -> SurfaceInspection {
    match role {
        SurfaceRole::Panel => SurfaceInspection {
            background: SurfaceThemeColorSlot::Background,
            has_border: true,
            radius: Radii::LG,
        },
        SurfaceRole::Card => SurfaceInspection {
            background: SurfaceThemeColorSlot::Secondary,
            has_border: true,
            radius: Radii::LG,
        },
        SurfaceRole::Raised => SurfaceInspection {
            background: SurfaceThemeColorSlot::Popover,
            has_border: true,
            radius: Radii::MD,
        },
        SurfaceRole::Scrim => SurfaceInspection {
            background: SurfaceThemeColorSlot::Overlay,
            has_border: false,
            radius: Radii::LG,
        },
        SurfaceRole::ModalContainer => SurfaceInspection {
            background: SurfaceThemeColorSlot::Popover,
            has_border: true,
            radius: Radii::LG,
        },
    }
}

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

impl From<SurfaceVariant> for SurfaceRole {
    fn from(value: SurfaceVariant) -> Self {
        match value {
            SurfaceVariant::Panel => SurfaceRole::Panel,
            SurfaceVariant::Card => SurfaceRole::Card,
            SurfaceVariant::Raised => SurfaceRole::Raised,
            SurfaceVariant::Overlay => SurfaceRole::Scrim,
        }
    }
}

fn variant_bg(variant: SurfaceVariant, theme: &gpui_component::Theme) -> Hsla {
    inspect_surface_role(variant.into())
        .background
        .resolve(theme)
}

fn variant_radius(variant: SurfaceVariant) -> Pixels {
    inspect_surface_role(variant.into()).radius
}

fn role_bg(role: SurfaceRole, theme: &gpui_component::Theme) -> Hsla {
    inspect_surface_role(role).background.resolve(theme)
}

pub fn surface_role(role: SurfaceRole, cx: &App) -> gpui::Div {
    let theme = cx.theme();
    let inspection = inspect_surface_role(role);

    let mut el = div().bg(role_bg(role, theme)).rounded(inspection.radius);

    if inspection.has_border {
        el = el.border_1().border_color(theme.border);
    }

    el
}

pub fn surface_modal_container(cx: &App) -> gpui::Div {
    surface_role(SurfaceRole::ModalContainer, cx)
}

/// Create a panel surface (`theme.background`, border, large radius).
///
/// Returns a `Div` so callers can chain `.shadow_lg()`, `.overflow_hidden()`,
/// `.child(...)`, and any other GPUI attributes. Chain `.rounded()` to
/// override the default radius.
pub fn surface_panel(cx: &App) -> gpui::Div {
    surface_role(SurfaceRole::Panel, cx)
}

/// Create a card surface (`theme.secondary`, border, large radius).
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
/// Chain `.rounded()` to override the default radius.
pub fn surface_card(cx: &App) -> gpui::Div {
    surface_role(SurfaceRole::Card, cx)
}

/// Create a raised surface (`theme.popover`, border, medium radius).
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
/// Chain `.rounded()` to override the default radius.
pub fn surface_raised(cx: &App) -> gpui::Div {
    surface_role(SurfaceRole::Raised, cx)
}

/// Create an overlay surface (semi-transparent black, no border, large radius).
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
/// Chain `.rounded()` to override the default radius.
pub fn surface_overlay(_cx: &App) -> gpui::Div {
    surface_role(SurfaceRole::Scrim, _cx)
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

/// Returns the default overlay background color backed by the active theme overlay slot.
///
/// Use this when building overlay containers that need their own element ID,
/// click handlers, or key contexts - situations where `surface_overlay` cannot
/// be used directly because it returns an un-styled `Div` without an ID.
pub fn overlay_bg(theme: &gpui_component::Theme) -> Hsla {
    inspect_surface_role(SurfaceRole::Scrim).resolve_background_color(theme)
}

#[cfg(test)]
mod tests {
    use super::{
        SurfaceRole, SurfaceThemeColorSlot, inspect_surface_role, overlay_bg, surface_overlay,
    };
    use crate::tokens::Radii;

    #[test]
    fn semantic_surface_roles_stay_on_canonical_theme_slots() {
        let panel = inspect_surface_role(SurfaceRole::Panel);
        assert_eq!(panel.background, SurfaceThemeColorSlot::Background);
        assert!(panel.has_border);
        assert_eq!(panel.radius, Radii::LG);

        let card = inspect_surface_role(SurfaceRole::Card);
        assert_eq!(card.background, SurfaceThemeColorSlot::Secondary);
        assert!(card.has_border);
        assert_eq!(card.radius, Radii::LG);

        let raised = inspect_surface_role(SurfaceRole::Raised);
        assert_eq!(raised.background, SurfaceThemeColorSlot::Popover);
        assert!(raised.has_border);
        assert_eq!(raised.radius, Radii::MD);
    }

    #[test]
    fn scrim_and_modal_container_keep_distinct_shared_roles() {
        let scrim = inspect_surface_role(SurfaceRole::Scrim);
        assert_eq!(scrim.background, SurfaceThemeColorSlot::Overlay);
        assert!(!scrim.has_border);
        assert_eq!(scrim.radius, Radii::LG);

        let modal = inspect_surface_role(SurfaceRole::ModalContainer);
        assert_eq!(modal.background, SurfaceThemeColorSlot::Popover);
        assert!(modal.has_border);
        assert_eq!(modal.radius, Radii::LG);

        let _ = surface_overlay;
        let _ = overlay_bg;
    }
}
