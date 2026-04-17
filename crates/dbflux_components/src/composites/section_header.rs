use gpui::prelude::*;
use gpui::{div, App, SharedString};
use gpui_component::ActiveTheme;

use crate::primitives::Text;
use crate::tokens::Spacing;

/// Render a settings-style section header with title, subtitle, and bottom border.
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
pub fn section_header(
    title: impl Into<SharedString>,
    subtitle: impl Into<SharedString>,
    cx: &App,
) -> gpui::Div {
    section_header_inner(title, subtitle, None, cx)
}

/// Render a section header with a right-aligned action element.
pub fn section_header_with_action(
    title: impl Into<SharedString>,
    subtitle: impl Into<SharedString>,
    action: impl IntoElement,
    cx: &App,
) -> gpui::Div {
    section_header_inner(title, subtitle, Some(action.into_any_element()), cx)
}

fn section_header_inner(
    title: impl Into<SharedString>,
    subtitle: impl Into<SharedString>,
    action: Option<gpui::AnyElement>,
    cx: &App,
) -> gpui::Div {
    let theme = cx.theme();

    let mut header = div()
        .px(Spacing::XL)
        .py(Spacing::LG)
        .border_b_1()
        .border_color(theme.border)
        .child(Text::heading(title))
        .child(div().mt_1().child(Text::muted(subtitle)));

    if let Some(action_el) = action {
        header = header.child(action_el);
    }

    header
}
