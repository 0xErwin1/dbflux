use gpui::prelude::*;
use gpui::{App, ClickEvent, SharedString, Stateful, Window, div};
use gpui_component::ActiveTheme;
use gpui_component::IconName;

use crate::icon::IconSource;
use crate::primitives::Icon;
use crate::tokens::{FontSizes, Heights, Spacing};

/// Render a collapsible section with a clickable header that toggles content visibility.
///
/// Returns a `Stateful<Div>` so callers can chain additional GPUI attributes.
/// The parent component is responsible for managing the `collapsed` boolean and
/// providing the `on_toggle` callback.
///
/// # Example
///
/// ```ignore
/// let section = collapsible_section(
///     "Details",
///     true,
///     |_, _, _| self.collapsed = !self.collapsed,
///     cx,
/// )
/// .child(some_content)
/// .overflow_hidden();
/// ```
pub fn collapsible_section(
    title: impl Into<SharedString>,
    collapsed: bool,
    on_toggle: impl Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync + 'static,
    cx: &App,
) -> Stateful<gpui::Div> {
    let theme = cx.theme();
    let title = title.into();

    let chevron = if collapsed {
        IconName::ChevronRight
    } else {
        IconName::ChevronDown
    };

    let header = div()
        .id("collapsible-header")
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .h(Heights::HEADER)
        .px(Spacing::SM)
        .bg(theme.secondary)
        .border_b_1()
        .border_color(theme.border)
        .cursor_pointer()
        .hover(|s| s.bg(theme.accent))
        .on_click(on_toggle)
        .child(
            Icon::new(IconSource::Named(chevron))
                .size(Heights::ICON_SM)
                .color(theme.muted_foreground),
        )
        .child(
            div()
                .text_size(FontSizes::SM)
                .font_weight(gpui::FontWeight::MEDIUM)
                .text_color(theme.foreground)
                .child(title),
        );

    let mut section = div()
        .id("collapsible-section")
        .flex()
        .flex_col()
        .child(header);

    if !collapsed {
        section = section.overflow_hidden();
    }

    section
}
