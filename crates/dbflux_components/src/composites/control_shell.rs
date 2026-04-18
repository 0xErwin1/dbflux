use gpui::prelude::*;
use gpui::{App, Pixels, div};
use gpui_component::ActiveTheme;

use crate::tokens::{Heights, Radii, Spacing};

pub(crate) const CONTROL_SHELL_HEIGHT: Pixels = Heights::INPUT;
pub(crate) const CONTROL_SHELL_HORIZONTAL_PADDING: Pixels = Spacing::SM;
const CONTROL_SHELL_RADIUS: Pixels = Radii::MD;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ControlShellMetrics {
    pub height: Pixels,
    pub horizontal_padding: Pixels,
}

pub(crate) fn control_shell_metrics() -> ControlShellMetrics {
    ControlShellMetrics {
        height: CONTROL_SHELL_HEIGHT,
        horizontal_padding: CONTROL_SHELL_HORIZONTAL_PADDING,
    }
}

pub fn control_shell(child: impl IntoElement, cx: &App) -> gpui::Div {
    let metrics = control_shell_metrics();

    control_shell_with_padding(child, metrics.horizontal_padding, cx)
}

pub(crate) fn control_shell_with_padding(
    child: impl IntoElement,
    horizontal_padding: Pixels,
    cx: &App,
) -> gpui::Div {
    let theme = cx.theme();
    let metrics = control_shell_metrics();

    div()
        .w_full()
        .h(metrics.height)
        .flex()
        .items_center()
        .px(horizontal_padding)
        .rounded(CONTROL_SHELL_RADIUS)
        .bg(theme.background)
        .border_1()
        .border_color(theme.input)
        .child(child)
}

#[cfg(test)]
mod tests {
    use super::{CONTROL_SHELL_HEIGHT, CONTROL_SHELL_HORIZONTAL_PADDING, control_shell_metrics};
    use crate::tokens::{Heights, Spacing};

    #[test]
    fn control_shell_matches_shared_input_chrome_metrics() {
        let metrics = control_shell_metrics();

        assert_eq!(metrics.height, Heights::INPUT);
        assert_eq!(metrics.horizontal_padding, Spacing::SM);
        assert_eq!(CONTROL_SHELL_HEIGHT, Heights::INPUT);
        assert_eq!(CONTROL_SHELL_HORIZONTAL_PADDING, Spacing::SM);
    }
}
