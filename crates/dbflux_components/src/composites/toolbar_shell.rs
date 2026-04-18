use gpui::prelude::*;
use gpui::{App, Pixels, div};
use gpui_component::ActiveTheme;

use crate::tokens::{Heights, Radii, Spacing};

pub(crate) const TOOLBAR_SHELL_HEIGHT: Pixels = Heights::TOOLBAR;
const TOOLBAR_SHELL_RADIUS: Pixels = Radii::MD;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ToolbarShellMetrics {
    pub height: Pixels,
    pub gap: Pixels,
}

pub(crate) fn toolbar_shell_metrics() -> ToolbarShellMetrics {
    ToolbarShellMetrics {
        height: TOOLBAR_SHELL_HEIGHT,
        gap: Spacing::SM,
    }
}

pub fn toolbar_shell(children: Vec<gpui::AnyElement>, cx: &App) -> gpui::Div {
    let theme = cx.theme();
    let metrics = toolbar_shell_metrics();

    div()
        .w_full()
        .h(metrics.height)
        .flex()
        .items_center()
        .gap(metrics.gap)
        .px(Spacing::SM)
        .rounded(TOOLBAR_SHELL_RADIUS)
        .border_1()
        .border_color(theme.border)
        .bg(theme.background)
        .children(children)
}

pub fn split_toolbar_action(
    main: impl IntoElement,
    trailing: impl IntoElement,
    cx: &App,
) -> gpui::Div {
    let theme = cx.theme();

    div()
        .flex()
        .items_center()
        .h(Heights::BUTTON)
        .rounded(Radii::SM)
        .border_1()
        .border_color(theme.border)
        .bg(theme.background)
        .child(div().flex_1().h_full().child(main))
        .child(
            div()
                .h_full()
                .border_l_1()
                .border_color(theme.border)
                .child(trailing),
        )
}

#[cfg(test)]
mod tests {
    use super::{TOOLBAR_SHELL_HEIGHT, toolbar_shell_metrics};
    use crate::tokens::{Heights, Spacing};

    #[test]
    fn toolbar_shell_matches_compact_toolbar_metrics() {
        let metrics = toolbar_shell_metrics();

        assert_eq!(metrics.height, Heights::TOOLBAR);
        assert_eq!(metrics.gap, Spacing::SM);
        assert_eq!(TOOLBAR_SHELL_HEIGHT, Heights::TOOLBAR);
    }
}
