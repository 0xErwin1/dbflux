use gpui::prelude::*;
use gpui::{App, div};
use gpui_component::ActiveTheme;

use crate::tokens::{Heights, Radii};

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
