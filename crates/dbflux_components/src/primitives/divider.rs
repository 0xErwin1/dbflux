//! `divider` — the 1 px rule between regions and groups of controls.

use gpui::prelude::*;
use gpui::{App, Axis, Div, div};
use gpui_component::ActiveTheme;

use crate::tokens::Borders;

/// Which palette line a divider is drawn in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DividerTone {
    /// The palette line (`#232128` dark, `#E3DEE6` light): separators between
    /// regions and inside panels.
    #[default]
    Line,
    /// The stronger line-2 (`#37333D` dark, `#CBC4D1` light): edges that sit
    /// on raised surfaces or need to read against a fill.
    Strong,
}

/// A 1 px divider along `axis`.
///
/// A horizontal divider spans the width of its parent and a vertical one its
/// height; callers size it further (`.h(...)` on a vertical divider inside a
/// toolbar, `.mx(...)` for inset rules) and never shrink it away in a flex
/// row or column.
pub fn divider(axis: Axis, tone: DividerTone, cx: &App) -> Div {
    let theme = cx.theme();
    let color = match tone {
        DividerTone::Line => theme.border,
        DividerTone::Strong => theme.input,
    };

    let rule = div().flex_shrink_0().bg(color);

    match axis {
        Axis::Horizontal => rule.w_full().h(Borders::THIN),
        Axis::Vertical => rule.h_full().w(Borders::THIN),
    }
}

/// A horizontal divider in the palette line.
pub fn hdivider(cx: &App) -> Div {
    divider(Axis::Horizontal, DividerTone::Line, cx)
}

/// A vertical divider in the palette line.
pub fn vdivider(cx: &App) -> Div {
    divider(Axis::Vertical, DividerTone::Line, cx)
}
