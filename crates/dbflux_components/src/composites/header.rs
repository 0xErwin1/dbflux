use gpui::prelude::*;
use gpui::{App, ClickEvent, SharedString, Stateful, Window, div};
use gpui_component::ActiveTheme;
use gpui_component::IconName;

use crate::icon::IconSource;
use crate::primitives::{Icon, Text};
use crate::tokens::{ChromeColors, HeaderMetrics, Heights, Spacing};

/// Panel header: 40 px, the title as an uppercase `Label`, actions on the
/// right, and a line along the bottom.
pub fn panel_header(title: impl Into<SharedString>, cx: &App) -> gpui::Div {
    panel_header_with_actions(title, Vec::<gpui::AnyElement>::new(), cx)
}

/// Panel header with right-aligned action elements.
pub fn panel_header_with_actions(
    title: impl Into<SharedString>,
    actions: Vec<impl IntoElement>,
    cx: &App,
) -> gpui::Div {
    let mut header = panel_header_row(cx).child(Text::label(title));

    if !actions.is_empty() {
        header = header.child(div().flex_1()).child(
            div()
                .flex()
                .items_center()
                .gap(Spacing::XS)
                .children(actions),
        );
    }

    header
}

/// Collapsible panel header: a chevron, an optional leading icon and the
/// title, toggled by a click anywhere on the row. `focused` switches the
/// title and icons to the tint, the way a focused panel reads.
#[allow(clippy::too_many_arguments)]
pub fn panel_header_collapsible(
    id: impl Into<gpui::ElementId>,
    title: impl Into<SharedString>,
    collapsed: bool,
    focused: bool,
    leading_icon: Option<IconName>,
    on_toggle: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    cx: &App,
) -> Stateful<gpui::Div> {
    let theme = cx.theme();
    let tone = if focused {
        ChromeColors::tint(theme)
    } else {
        theme.muted_foreground
    };

    let chevron = if collapsed {
        IconName::ChevronRight
    } else {
        IconName::ChevronDown
    };

    let hover = theme.secondary;

    panel_header_row(cx)
        .id(id)
        .cursor_pointer()
        .hover(move |style| style.bg(hover))
        .child(
            Icon::new(IconSource::Named(chevron))
                .size(Heights::ICON_SM)
                .color(tone),
        )
        .when_some(leading_icon, |row, icon| {
            row.child(
                Icon::new(IconSource::Named(icon))
                    .size(Heights::ICON_SM)
                    .color(tone),
            )
        })
        .child(Text::label(title).color(tone))
        .on_click(on_toggle)
}

fn panel_header_row(cx: &App) -> gpui::Div {
    div()
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(HeaderMetrics::PANEL_GAP)
        .h(HeaderMetrics::PANEL_HEIGHT)
        .pl(HeaderMetrics::PANEL_PADDING_LEFT)
        .pr(HeaderMetrics::PANEL_PADDING_RIGHT)
        .border_b_1()
        .border_color(cx.theme().border)
}

/// Section header, as used between groups of settings: an optional icon and
/// the title as an uppercase `Label`, over a line.
pub fn section_header(
    title: impl Into<SharedString>,
    icon: Option<IconSource>,
    cx: &App,
) -> gpui::Div {
    let theme = cx.theme();
    let muted = theme.muted_foreground;

    div()
        .flex()
        .items_center()
        .gap(HeaderMetrics::PANEL_GAP)
        .pt(HeaderMetrics::LABEL_PADDING_TOP)
        .pb(HeaderMetrics::LABEL_PADDING_BOTTOM)
        .mb(HeaderMetrics::LABEL_PADDING_BOTTOM)
        .border_b_1()
        .border_color(theme.border)
        .when_some(icon, |row, icon| {
            row.child(Icon::new(icon).size(Heights::ICON_SM).color(muted))
        })
        .child(Text::label(title))
}

/// Page head of a settings page: the page title and a one-line description.
pub fn page_header(
    title: impl Into<SharedString>,
    description: impl Into<SharedString>,
    _cx: &App,
) -> gpui::Div {
    page_header_layout(title.into(), description.into(), None)
}

/// Page head with a right-aligned action element.
pub fn page_header_with_action(
    title: impl Into<SharedString>,
    description: impl Into<SharedString>,
    action: impl IntoElement,
    _cx: &App,
) -> gpui::Div {
    page_header_layout(
        title.into(),
        description.into(),
        Some(action.into_any_element()),
    )
}

fn page_header_layout(
    title: SharedString,
    description: SharedString,
    action: Option<gpui::AnyElement>,
) -> gpui::Div {
    let text_block = div()
        .flex()
        .flex_col()
        .flex_1()
        .min_w_0()
        .gap(HeaderMetrics::SECTION_GAP)
        .child(Text::title(title))
        .child(Text::body(description).muted_foreground());

    div()
        .flex()
        .items_center()
        .justify_between()
        .gap(Spacing::MD)
        .px(Spacing::LG)
        .pt(HeaderMetrics::SECTION_PADDING_TOP)
        .pb(HeaderMetrics::SECTION_PADDING_BOTTOM)
        .child(text_block)
        .when_some(action, |row, action| row.child(action))
}
