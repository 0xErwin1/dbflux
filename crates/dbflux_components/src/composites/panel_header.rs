use gpui::prelude::*;
use gpui::{App, ClickEvent, SharedString, Stateful, Window, div};
use gpui_component::ActiveTheme;
use gpui_component::IconName;

use crate::icon::IconSource;
use crate::primitives::Icon;
use crate::tokens::{FontSizes, Heights, Spacing};

/// Render a toolbar-height panel header with a title.
///
/// Returns a `Div` so callers can chain additional GPUI attributes.
pub fn panel_header(title: impl Into<SharedString>, cx: &App) -> gpui::Div {
    panel_header_inner(title.into(), None, None, cx)
}

/// Render a panel header with right-aligned action elements.
pub fn panel_header_with_actions(
    title: impl Into<SharedString>,
    actions: Vec<impl IntoElement>,
    cx: &App,
) -> gpui::Div {
    let action_els: Vec<gpui::AnyElement> =
        actions.into_iter().map(|a| a.into_any_element()).collect();
    panel_header_inner(title.into(), None, Some(action_els), cx)
}

/// Render a collapsible panel header with a chevron toggle and click handler.
///
/// Returns a `Stateful<Div>` (has an element ID) so it supports click events.
pub fn panel_header_collapsible(
    id: impl Into<gpui::ElementId>,
    title: impl Into<SharedString>,
    collapsed: bool,
    on_toggle: impl Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync + 'static,
    cx: &App,
) -> Stateful<gpui::Div> {
    let chevron = if collapsed {
        IconName::ChevronRight
    } else {
        IconName::ChevronDown
    };

    panel_header_inner_stateful(
        id.into(),
        title.into(),
        Some(chevron),
        actions_from_toggle(on_toggle),
        cx,
    )
}

fn actions_from_toggle(
    on_toggle: impl Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync + 'static,
) -> Option<Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync>> {
    Some(Box::new(on_toggle))
}

fn panel_header_inner(
    title: SharedString,
    chevron: Option<IconName>,
    actions: Option<Vec<gpui::AnyElement>>,
    cx: &App,
) -> gpui::Div {
    let theme = cx.theme();

    let mut left = div().flex().items_center().gap(Spacing::SM);

    if let Some(icon) = chevron {
        left = left.child(
            Icon::new(IconSource::Named(icon))
                .size(Heights::ICON_SM)
                .color(theme.muted_foreground),
        );
    }

    left = left.child(
        div()
            .text_size(FontSizes::SM)
            .font_weight(gpui::FontWeight::MEDIUM)
            .text_color(theme.foreground)
            .child(title),
    );

    let mut header = div()
        .flex()
        .items_center()
        .justify_between()
        .h(Heights::TOOLBAR)
        .px(Spacing::SM)
        .bg(theme.secondary)
        .border_b_1()
        .border_color(theme.border)
        .child(left);

    if let Some(actions) = actions {
        header = header.child(
            div()
                .flex()
                .items_center()
                .gap(Spacing::XS)
                .children(actions),
        );
    }

    header
}

fn panel_header_inner_stateful(
    id: gpui::ElementId,
    title: SharedString,
    chevron: Option<IconName>,
    on_click: Option<Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + Send + Sync>>,
    cx: &App,
) -> Stateful<gpui::Div> {
    let theme = cx.theme();

    let mut left = div().flex().items_center().gap(Spacing::SM);

    if let Some(icon) = chevron {
        left = left.child(
            Icon::new(IconSource::Named(icon))
                .size(Heights::ICON_SM)
                .color(theme.muted_foreground),
        );
    }

    left = left.child(
        div()
            .text_size(FontSizes::SM)
            .font_weight(gpui::FontWeight::MEDIUM)
            .text_color(theme.foreground)
            .child(title),
    );

    let mut header = div()
        .id(id)
        .flex()
        .items_center()
        .justify_between()
        .h(Heights::TOOLBAR)
        .px(Spacing::SM)
        .bg(theme.secondary)
        .border_b_1()
        .border_color(theme.border)
        .cursor_pointer()
        .child(left);

    if let Some(handler) = on_click {
        header = header.on_click(handler);
    }

    header
}
