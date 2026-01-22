use gpui::{px, Pixels};

#[allow(dead_code)]
pub struct Spacing;

#[allow(dead_code)]
impl Spacing {
    pub const XS: Pixels = px(4.0);
    pub const SM: Pixels = px(8.0);
    pub const MD: Pixels = px(12.0);
    pub const LG: Pixels = px(16.0);
    pub const XL: Pixels = px(24.0);
}

#[allow(dead_code)]
pub struct Heights;

#[allow(dead_code)]
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

#[allow(dead_code)]
pub struct FontSizes;

#[allow(dead_code)]
impl FontSizes {
    pub const XS: Pixels = px(11.0);
    pub const SM: Pixels = px(12.0);
    pub const BASE: Pixels = px(13.0);
    pub const LG: Pixels = px(14.0);
    pub const XL: Pixels = px(16.0);
    pub const TITLE: Pixels = px(18.0);
}

#[allow(dead_code)]
pub struct Radii;

#[allow(dead_code)]
impl Radii {
    pub const SM: Pixels = px(3.0);
    pub const MD: Pixels = px(4.0);
    pub const LG: Pixels = px(6.0);
    pub const FULL: Pixels = px(9999.0);
}
