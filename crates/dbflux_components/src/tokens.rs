use gpui::{Hsla, Pixels, px, rgb};

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
}

pub struct FontSizes;

impl FontSizes {
    pub const XS: Pixels = px(12.0);
    pub const SM: Pixels = px(13.0);
    pub const BASE: Pixels = px(14.0);
    pub const LG: Pixels = px(15.0);
    pub const XL: Pixels = px(18.0);
    pub const TITLE: Pixels = px(20.0);
}

pub struct Radii;

impl Radii {
    // DBFlux uses a flat, square chrome — every named radius collapses to 0.
    // `Radii::FULL` is reserved for explicitly pill-shaped surfaces (avatars,
    // status dots) and stays as-is.
    pub const SM: Pixels = px(0.0);
    pub const MD: Pixels = px(0.0);
    pub const LG: Pixels = px(0.0);
    pub const FULL: Pixels = px(9999.0);
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
    use super::{ChromeColorSlot, ChromeEdgeRole, ChromeSurfaceRole, Radii};

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
