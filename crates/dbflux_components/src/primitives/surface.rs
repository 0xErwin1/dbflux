use gpui::prelude::*;
use gpui::{App, Hsla, Pixels, div};
use gpui_component::ActiveTheme;

use crate::primitives::Chamfer;
use crate::tokens::{ChamferCut, ChromeColorSlot};

/// The five surfaces of the design system (DSAppPlan "Surface").
///
/// - `Panel`: panes and document areas, square, panel fill and line border.
/// - `Card`: palettes and framed content, cut 14, panel fill and line-2 border.
/// - `Raised`: inset blocks such as SQL previews and chips, square, raised fill.
/// - `Overlay`: popovers and floating menus, cut 12, panel fill and line-2 border.
/// - `Modal`: dialog cards, cut 18, panel fill and line-2 border.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SurfaceRole {
    Panel,
    Card,
    Raised,
    Overlay,
    Modal,
}

/// Render-free description of a surface role.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SurfaceInspection {
    pub fill: ChromeColorSlot,
    pub border: ChromeColorSlot,
    /// Chamfer cut of the top-left and bottom-right corners; `None` for a
    /// square surface.
    pub cut: Option<Pixels>,
}

pub fn inspect_surface_role(role: SurfaceRole) -> SurfaceInspection {
    match role {
        SurfaceRole::Panel => SurfaceInspection {
            fill: ChromeColorSlot::Popover,
            border: ChromeColorSlot::Border,
            cut: None,
        },
        SurfaceRole::Card => SurfaceInspection {
            fill: ChromeColorSlot::Popover,
            border: ChromeColorSlot::Input,
            cut: Some(ChamferCut::CARD),
        },
        SurfaceRole::Raised => SurfaceInspection {
            fill: ChromeColorSlot::Secondary,
            border: ChromeColorSlot::Border,
            cut: None,
        },
        SurfaceRole::Overlay => SurfaceInspection {
            fill: ChromeColorSlot::Popover,
            border: ChromeColorSlot::Input,
            cut: Some(ChamferCut::OVERLAY),
        },
        SurfaceRole::Modal => SurfaceInspection {
            fill: ChromeColorSlot::Popover,
            border: ChromeColorSlot::Input,
            cut: Some(ChamferCut::MODAL),
        },
    }
}

/// A surface of the given role.
///
/// Square roles paint their fill and a 1 px border on the returned `Div`.
/// Chamfered roles return a `.relative()` `Div` whose first child is the
/// `Chamfer` that paints fill and border, so the caller must not set `.bg()`
/// on it; content added with `.child()` sits above the shape. Returns a `Div`
/// so callers can chain layout, sizing and children.
pub fn surface(role: SurfaceRole, cx: &App) -> gpui::Div {
    let theme = cx.theme();
    let inspection = inspect_surface_role(role);
    let fill = inspection.fill.resolve(theme);
    let border = inspection.border.resolve(theme);

    match inspection.cut {
        None => div().bg(fill).border_1().border_color(border),
        Some(cut) => div()
            .relative()
            .child(Chamfer::new(cut).fill(fill).border(border)),
    }
}

/// The scrim behind a modal or command palette, per theme.
pub fn overlay_bg(theme: &gpui_component::Theme) -> Hsla {
    theme.overlay
}

#[cfg(test)]
mod tests {
    use super::{SurfaceRole, inspect_surface_role};
    use crate::tokens::{ChamferCut, ChromeColorSlot};

    #[test]
    fn surfaces_follow_the_cut_depth_scale() {
        assert_eq!(inspect_surface_role(SurfaceRole::Panel).cut, None);
        assert_eq!(inspect_surface_role(SurfaceRole::Raised).cut, None);
        assert_eq!(
            inspect_surface_role(SurfaceRole::Card).cut,
            Some(ChamferCut::CARD)
        );
        assert_eq!(
            inspect_surface_role(SurfaceRole::Overlay).cut,
            Some(ChamferCut::OVERLAY)
        );
        assert_eq!(
            inspect_surface_role(SurfaceRole::Modal).cut,
            Some(ChamferCut::MODAL)
        );
    }

    #[test]
    fn floating_surfaces_use_the_panel_fill_and_the_strong_line() {
        for role in [SurfaceRole::Card, SurfaceRole::Overlay, SurfaceRole::Modal] {
            let inspection = inspect_surface_role(role);
            assert_eq!(inspection.fill, ChromeColorSlot::Popover, "{role:?}");
            assert_eq!(inspection.border, ChromeColorSlot::Input, "{role:?}");
        }

        let raised = inspect_surface_role(SurfaceRole::Raised);
        assert_eq!(raised.fill, ChromeColorSlot::Secondary);
    }
}
