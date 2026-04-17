use dbflux_components::primitives::Text;
use gpui::prelude::*;
use gpui::*;
use gpui_component::scroll::ScrollableElement;

use crate::ui::theme::ghost_border_color;

pub(super) fn section_header(
    title: impl Into<SharedString>,
    subtitle: impl Into<SharedString>,
    _theme: &gpui_component::Theme,
) -> Div {
    div()
        .px_6()
        .py_5()
        .border_b_1()
        .border_color(ghost_border_color())
        .child(Text::heading(title))
        .child(div().mt_1().child(Text::muted(subtitle)))
}

pub(super) fn section_container(content: impl IntoElement) -> Div {
    div()
        .flex_1()
        .min_h_0()
        .flex()
        .flex_col()
        .overflow_hidden()
        .child(content)
}

pub(super) fn sticky_form_shell(
    header: impl IntoElement,
    body: impl IntoElement,
    footer: impl IntoElement,
    theme: &gpui_component::Theme,
) -> Div {
    div()
        .flex_1()
        .h_full()
        .min_h_0()
        .flex()
        .flex_col()
        .overflow_hidden()
        .child(
            div()
                .p_4()
                .border_b_1()
                .border_color(theme.border)
                .child(header),
        )
        .child(
            div()
                .flex_1()
                .min_h_0()
                .overflow_y_scrollbar()
                .p_4()
                .flex()
                .flex_col()
                .gap_5()
                .child(body),
        )
        .child(
            div()
                .p_4()
                .border_t_1()
                .border_color(theme.border)
                .flex()
                .justify_end()
                .child(footer),
        )
}
