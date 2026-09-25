use gpui::prelude::*;
use gpui::{App, Pixels, div};
use gpui_component::ActiveTheme;

use crate::primitives::Chamfer;
use crate::tokens::{ChamferCut, Fields};

pub(crate) const CONTROL_SHELL_HEIGHT: Pixels = Fields::HEIGHT;
pub(crate) const CONTROL_SHELL_HORIZONTAL_PADDING: Pixels = Fields::PADDING_X;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ControlShellMetrics {
    pub height: Pixels,
    pub horizontal_padding: Pixels,
    pub cut: Pixels,
}

pub(crate) fn control_shell_metrics() -> ControlShellMetrics {
    ControlShellMetrics {
        height: CONTROL_SHELL_HEIGHT,
        horizontal_padding: CONTROL_SHELL_HORIZONTAL_PADDING,
        cut: ChamferCut::CONTROL,
    }
}

/// Wraps a frameless control (a toolbar-style `Dropdown`, an `Input` with
/// `appearance(false)`) in the select field shape: a raised chamfered field
/// with a 1 px line on its straight edges, `Fields::HEIGHT` tall.
pub fn control_shell(child: impl IntoElement, cx: &App) -> gpui::Div {
    let theme = cx.theme();
    let metrics = control_shell_metrics();

    div()
        .relative()
        .w_full()
        .h(metrics.height)
        .flex()
        .items_center()
        .px(metrics.horizontal_padding)
        .child(
            Chamfer::new(metrics.cut)
                .fill(theme.secondary)
                .border(theme.border),
        )
        .child(child)
}

#[cfg(test)]
mod tests {
    use super::{CONTROL_SHELL_HEIGHT, CONTROL_SHELL_HORIZONTAL_PADDING, control_shell_metrics};
    use crate::tokens::{ChamferCut, Fields};

    #[test]
    fn control_shell_matches_the_select_field_metrics() {
        let metrics = control_shell_metrics();

        assert_eq!(metrics.height, Fields::HEIGHT);
        assert_eq!(metrics.horizontal_padding, Fields::PADDING_X);
        assert_eq!(metrics.cut, ChamferCut::CONTROL);
        assert_eq!(CONTROL_SHELL_HEIGHT, Fields::HEIGHT);
        assert_eq!(CONTROL_SHELL_HORIZONTAL_PADDING, Fields::PADDING_X);
    }
}
