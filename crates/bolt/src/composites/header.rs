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

/// Bottom-docked collapsible bar, as used for Background Tasks: 30 px, a
/// line above, the title in muted interface text and an optional status in
/// mono (for example "idle").
#[allow(clippy::too_many_arguments)]
pub fn collapsible_bar(
    id: impl Into<gpui::ElementId>,
    title: impl Into<SharedString>,
    status: Option<SharedString>,
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
    let status_color = theme.muted;

    let chevron = if collapsed {
        IconName::ChevronRight
    } else {
        IconName::ChevronDown
    };

    let hover = theme.secondary;

    div()
        .id(id)
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(HeaderMetrics::BAR_GAP)
        .h(HeaderMetrics::BAR_HEIGHT)
        .px(HeaderMetrics::BAR_PADDING_X)
        .border_t_1()
        .border_color(theme.border)
        .text_size(HeaderMetrics::BAR_FONT)
        .text_color(tone)
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
        .child(title.into())
        .when_some(status, |row, status| {
            row.child(
                div()
                    .font_family(crate::fonts::editor_family(cx))
                    .text_color(status_color)
                    .child(status),
            )
        })
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

/// Section header, as used between groups of settings: an optional tint
/// icon and the title as a 10 px uppercase `Label`, over a line.
pub fn section_header(
    title: impl Into<SharedString>,
    icon: Option<IconSource>,
    cx: &App,
) -> gpui::Div {
    let theme = cx.theme();
    let tint = ChromeColors::tint(theme);

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
            row.child(Icon::new(icon).size(HeaderMetrics::LABEL_ICON).color(tint))
        })
        .child(Text::label(title).font_size(HeaderMetrics::LABEL_FONT))
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
        .px(HeaderMetrics::SECTION_PADDING_X)
        .pt(HeaderMetrics::SECTION_PADDING_TOP)
        .pb(HeaderMetrics::SECTION_PADDING_BOTTOM)
        .child(text_block)
        .when_some(action, |row, action| row.child(action))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controls::Button;
    use crate::tokens::{BASE_REM, Spacing};
    use gpui::px;

    fn install_test_theme(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(|cx| {
            crate::theme::install(
                crate::brand::Brand::neutral(),
                crate::palette::PaletteMode::Dark,
                crate::density::Density::Default,
                cx,
            );
        });
    }

    struct HeaderHarness;

    impl Render for HeaderHarness {
        fn render(
            &mut self,
            _window: &mut Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            div()
                .size_full()
                .flex()
                .flex_col()
                .child(
                    div()
                        .debug_selector(|| "panel-header-under-test".to_string())
                        .child(panel_header_with_actions(
                            "Connections",
                            vec![
                                div()
                                    .debug_selector(|| "panel-header-action-a".to_string())
                                    .child(Button::new("panel-header-action-a", "One")),
                                div()
                                    .debug_selector(|| "panel-header-action-b".to_string())
                                    .child(Button::new("panel-header-action-b", "Two")),
                            ],
                            cx,
                        )),
                )
                .child(div().flex_1())
        }
    }

    /// Pins the panel header's token-derived layout: one
    /// `HeaderMetrics::PANEL_HEIGHT` tall row, the actions right-aligned
    /// inside `HeaderMetrics::PANEL_PADDING_RIGHT`, and the actions
    /// separated by `Spacing::XS`. These are the values the surface
    /// commits to; any replacement rendering must reproduce them exactly.
    #[gpui::test]
    fn panel_header_renders_at_the_header_metrics(cx: &mut gpui::TestAppContext) {
        install_test_theme(cx);

        let (_, window) = cx.add_window_view(|window, cx| {
            let harness = cx.new(|_| HeaderHarness);
            gpui_component::Root::new(harness, window, cx)
        });
        window.run_until_parked();

        let header = window
            .debug_bounds("panel-header-under-test")
            .expect("the panel header is laid out");
        assert_eq!(
            header.size.height,
            HeaderMetrics::PANEL_HEIGHT.to_pixels(px(BASE_REM)),
            "the header is one panel header tall"
        );

        let action_a = window
            .debug_bounds("panel-header-action-a")
            .expect("the first action is laid out");
        let action_b = window
            .debug_bounds("panel-header-action-b")
            .expect("the second action is laid out");

        assert_eq!(
            action_b.left() - action_a.right(),
            Spacing::XS,
            "two actions are separated by the actions-row gap"
        );
        assert_eq!(
            header.right() - action_b.right(),
            HeaderMetrics::PANEL_PADDING_RIGHT,
            "the actions end at the header's right padding"
        );
    }
}
