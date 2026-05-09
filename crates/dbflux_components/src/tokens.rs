use gpui::{BoxShadow, Hsla, Pixels, Point, px, rgb};

pub struct Spacing;

impl Spacing {
    pub const XS: Pixels = px(4.0);
    pub const SM: Pixels = px(8.0);
    pub const MD: Pixels = px(12.0);
    pub const LG: Pixels = px(16.0);
    pub const XL: Pixels = px(24.0);
}

pub struct Heights;

impl Heights {
    pub const ROW: Pixels = px(28.0);
    pub const ROW_COMPACT: Pixels = px(24.0);
    pub const HEADER: Pixels = px(40.0);
    pub const TOOLBAR: Pixels = px(32.0);
    pub const TAB: Pixels = px(36.0);
    pub const INPUT: Pixels = px(32.0);
    pub const BUTTON: Pixels = px(28.0);
    /// Standard inline control height (input, dropdown, button) when packed
    /// into a toolbar/filter bar. Use this to keep heterogeneous controls aligned.
    pub const CONTROL: Pixels = px(28.0);
    pub const ICON_SM: Pixels = px(16.0);
    pub const ICON_MD: Pixels = px(20.0);
    pub const ICON_LG: Pixels = px(24.0);
    /// Height of the active-tab indicator stripe — a 1 px absolutely-positioned
    /// child div rendered at the bottom edge of the active tab item.
    pub const TAB_STRIPE: Pixels = px(1.0);
}

pub struct FontSizes;

/// Static font-size constants matching `AppStyle::Default` (the project's
/// baseline density). For style-aware sizing at render sites, prefer the
/// `density::font_*(cx)` accessors so the active `AppStyle` is honoured.
impl FontSizes {
    /// Extra-small — used for badges, captions, tooltips (Default: 12 px).
    pub const XS: Pixels = px(12.0);
    /// Small — used for labels and secondary metadata (Default: 13 px).
    pub const SM: Pixels = px(13.0);
    /// Base — primary body and input text (Default: 14 px).
    pub const BASE: Pixels = px(14.0);
    /// Large — emphasized labels and nav items (Default: 15 px).
    pub const LG: Pixels = px(15.0);
    /// Extra-large — sub-headings and panel titles (Default: 18 px).
    pub const XL: Pixels = px(18.0);
    /// Title — window-level headings (Default: 20 px).
    pub const TITLE: Pixels = px(20.0);
}

pub struct Radii;

/// Static border-radius constants matching `AppStyle::Default` (square
/// corners). For style-aware radii at render sites, prefer the
/// `density::radius_*(cx)` accessors so the active `AppStyle` is honoured.
impl Radii {
    /// Small radius — controls, inputs, badges (Default: 0 px).
    pub const SM: Pixels = px(0.0);
    /// Medium radius — dropdowns, popovers (Default: 0 px).
    pub const MD: Pixels = px(0.0);
    /// Large radius — modals, cards (Default: 0 px).
    pub const LG: Pixels = px(0.0);
    /// Full radius — pill shapes, avatars, status dots.
    pub const FULL: Pixels = px(9999.0);
}

/// Centralized box-shadow definitions.
///
/// Use these instead of constructing `BoxShadow` at call sites so the shadow
/// treatment stays consistent across the app.
pub struct Shadows;

impl Shadows {
    /// Medium shadow — used for elevated panels, dropdowns, and tooltips.
    ///
    /// Equivalent to a subtle single-layer downward shadow with moderate blur.
    pub fn md() -> BoxShadow {
        BoxShadow {
            color: gpui::hsla(0.0, 0.0, 0.0, 0.24),
            offset: Point {
                x: px(0.0),
                y: px(4.0),
            },
            blur_radius: px(8.0),
            spread_radius: px(0.0),
        }
    }

    /// Large shadow — used for modals, overlays, and floating windows.
    ///
    /// Two-layer shadow: a large diffuse spread plus a tight close shadow for
    /// depth perception.
    pub fn lg() -> BoxShadow {
        BoxShadow {
            color: gpui::hsla(0.0, 0.0, 0.0, 0.32),
            offset: Point {
                x: px(0.0),
                y: px(8.0),
            },
            blur_radius: px(24.0),
            spread_radius: px(0.0),
        }
    }
}

pub struct SyntaxColors;

pub struct ChromeColors;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChromeColorSlot {
    Background,
    Secondary,
    Border,
    Input,
    Popover,
}

impl ChromeColorSlot {
    pub fn resolve(self, theme: &gpui_component::Theme) -> Hsla {
        match self {
            Self::Background => theme.background,
            Self::Secondary => theme.secondary,
            Self::Border => theme.border,
            Self::Input => theme.input,
            Self::Popover => theme.popover,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChromeEdgeRole {
    Surface,
    Separator,
    Control,
    Popover,
    ModalSeparator,
}

impl ChromeEdgeRole {
    pub fn color_slot(self) -> ChromeColorSlot {
        match self {
            Self::Surface | Self::Separator | Self::Control | Self::ModalSeparator => {
                ChromeColorSlot::Input
            }
            Self::Popover => ChromeColorSlot::Border,
        }
    }

    pub fn resolve(self, theme: &gpui_component::Theme) -> Hsla {
        self.color_slot().resolve(theme)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChromeSurfaceRole {
    ControlShell,
    PopoverShell,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChromeSurfaceInspection {
    pub background: ChromeColorSlot,
    pub edge: ChromeEdgeRole,
    pub radius: Pixels,
}

impl ChromeSurfaceRole {
    pub fn inspect(self) -> ChromeSurfaceInspection {
        match self {
            Self::ControlShell => ChromeSurfaceInspection {
                background: ChromeColorSlot::Secondary,
                edge: ChromeEdgeRole::Control,
                radius: Radii::SM,
            },
            Self::PopoverShell => ChromeSurfaceInspection {
                background: ChromeColorSlot::Popover,
                edge: ChromeEdgeRole::Popover,
                radius: Radii::MD,
            },
        }
    }
}

impl ChromeColors {
    pub fn ghost_border() -> Hsla {
        let mut color: Hsla = rgb(0x524436).into();
        color.a = 0.15;
        color
    }
}

impl SyntaxColors {
    pub fn table() -> Hsla {
        rgb(0x4EC9B0).into()
    }

    pub fn view() -> Hsla {
        rgb(0xDCDCAA).into()
    }

    pub fn column() -> Hsla {
        rgb(0x9CDCFE).into()
    }

    pub fn type_item() -> Hsla {
        rgb(0xC586C0).into()
    }

    pub fn folder_dim() -> Hsla {
        rgb(0x808080).into()
    }

    pub fn database() -> Hsla {
        rgb(0xCE9178).into()
    }

    pub fn schema() -> Hsla {
        rgb(0x569CD6).into()
    }
}

#[cfg(test)]
mod tests {
    use super::{ChromeColorSlot, ChromeEdgeRole, ChromeSurfaceRole, FontSizes, Radii, Shadows};
    use gpui::px;

    // Static-constant baseline: matches AppStyle::Default (project's flat,
    // larger-text default). Style-aware sites use density::font_*/radius_*.
    #[test]
    fn font_sizes_match_default_style_scale() {
        assert_eq!(FontSizes::XS, px(12.0));
        assert_eq!(FontSizes::SM, px(13.0));
        assert_eq!(FontSizes::BASE, px(14.0));
        assert_eq!(FontSizes::LG, px(15.0));
        assert_eq!(FontSizes::XL, px(18.0));
        assert_eq!(FontSizes::TITLE, px(20.0));
    }

    #[test]
    fn radii_match_default_style_scale() {
        assert_eq!(Radii::SM, px(0.0));
        assert_eq!(Radii::MD, px(0.0));
        assert_eq!(Radii::LG, px(0.0));
        assert_eq!(Radii::FULL, px(9999.0));
    }

    #[test]
    fn shadows_md_has_expected_geometry() {
        let shadow = Shadows::md();
        assert_eq!(shadow.offset.y, px(4.0));
        assert_eq!(shadow.blur_radius, px(8.0));
        assert_eq!(shadow.spread_radius, px(0.0));
        assert!((shadow.color.a - 0.24).abs() < 0.001);
    }

    #[test]
    fn shadows_lg_has_expected_geometry() {
        let shadow = Shadows::lg();
        assert_eq!(shadow.offset.y, px(8.0));
        assert_eq!(shadow.blur_radius, px(24.0));
        assert_eq!(shadow.spread_radius, px(0.0));
        assert!((shadow.color.a - 0.32).abs() < 0.001);
    }

    #[test]
    fn chrome_edge_roles_map_to_low_emphasis_theme_slots() {
        assert_eq!(ChromeEdgeRole::Surface.color_slot(), ChromeColorSlot::Input);
        assert_eq!(
            ChromeEdgeRole::Separator.color_slot(),
            ChromeColorSlot::Input
        );
        assert_eq!(ChromeEdgeRole::Control.color_slot(), ChromeColorSlot::Input);
        assert_eq!(
            ChromeEdgeRole::Popover.color_slot(),
            ChromeColorSlot::Border
        );
        assert_eq!(
            ChromeEdgeRole::ModalSeparator.color_slot(),
            ChromeColorSlot::Input
        );
    }

    #[test]
    fn chrome_surface_roles_capture_tight_controls_and_popover_shells() {
        let control = ChromeSurfaceRole::ControlShell.inspect();
        assert_eq!(control.background, ChromeColorSlot::Secondary);
        assert_eq!(control.edge, ChromeEdgeRole::Control);
        assert_eq!(control.radius, Radii::SM);

        let popover = ChromeSurfaceRole::PopoverShell.inspect();
        assert_eq!(popover.background, ChromeColorSlot::Popover);
        assert_eq!(popover.edge, ChromeEdgeRole::Popover);
        assert_eq!(popover.radius, Radii::MD);
    }
}
