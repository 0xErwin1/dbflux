use gpui::prelude::*;
use gpui::{App, Hsla, Pixels, div, px};
use gpui_component::ActiveTheme;

use crate::tokens::Radii;

pub(crate) const FOCUS_FRAME_BORDER_WIDTH: Pixels = px(2.0);
const FOCUS_FRAME_RADIUS: Pixels = Radii::MD;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FocusFrameBorderKind {
    Transparent,
    ThemeRing,
    CustomRing,
}

pub(crate) fn focus_frame_border_kind(
    show_ring: bool,
    has_custom_ring_color: bool,
) -> FocusFrameBorderKind {
    if !show_ring {
        FocusFrameBorderKind::Transparent
    } else if has_custom_ring_color {
        FocusFrameBorderKind::CustomRing
    } else {
        FocusFrameBorderKind::ThemeRing
    }
}

pub fn focus_frame(
    show_ring: bool,
    ring_color: Option<Hsla>,
    child: impl IntoElement,
    cx: &App,
) -> gpui::Div {
    let theme = cx.theme();
    let border_color = match focus_frame_border_kind(show_ring, ring_color.is_some()) {
        FocusFrameBorderKind::Transparent => gpui::transparent_black(),
        FocusFrameBorderKind::ThemeRing => theme.ring,
        FocusFrameBorderKind::CustomRing => ring_color.expect("custom ring color should exist"),
    };

    div()
        .relative()
        .rounded(FOCUS_FRAME_RADIUS)
        .child(
            div()
                .absolute()
                .inset_0()
                .rounded(FOCUS_FRAME_RADIUS)
                .border(FOCUS_FRAME_BORDER_WIDTH)
                .border_color(border_color),
        )
        .child(child)
}

#[cfg(test)]
mod tests {
    use super::{FOCUS_FRAME_BORDER_WIDTH, FocusFrameBorderKind, focus_frame_border_kind};
    use gpui::px;

    #[test]
    fn unfocused_frame_uses_transparent_border() {
        assert_eq!(
            focus_frame_border_kind(false, true),
            FocusFrameBorderKind::Transparent
        );
    }

    #[test]
    fn focused_frame_uses_theme_ring_without_custom_color() {
        assert_eq!(
            focus_frame_border_kind(true, false),
            FocusFrameBorderKind::ThemeRing
        );
    }

    #[test]
    fn focused_frame_prefers_custom_ring_color() {
        assert_eq!(
            focus_frame_border_kind(true, true),
            FocusFrameBorderKind::CustomRing
        );
    }

    #[test]
    fn frame_layout_matches_shared_border_contract() {
        assert_eq!(FOCUS_FRAME_BORDER_WIDTH, px(2.0));
    }
}
