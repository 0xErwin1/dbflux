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
    pub const TAB: Pixels = px(28.0);
    pub const INPUT: Pixels = px(32.0);
    pub const BUTTON: Pixels = px(28.0);
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
    pub const SM: Pixels = px(3.0);
    pub const MD: Pixels = px(4.0);
    pub const LG: Pixels = px(6.0);
    pub const FULL: Pixels = px(9999.0);
}

pub struct SyntaxColors;

pub struct ChromeColors;

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
