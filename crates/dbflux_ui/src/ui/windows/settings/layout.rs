use gpui::prelude::*;
use gpui::*;
use gpui_component::scroll::ScrollableElement;

pub(super) fn section_header(
    title: impl Into<SharedString>,
    subtitle: impl Into<SharedString>,
    theme: &gpui_component::Theme,
) -> Div {
    let title: SharedString = title.into();
    let subtitle: SharedString = subtitle.into();

    div()
        .p_4()
        .border_b_1()
        .border_color(theme.border)
        .child(
            div()
                .text_lg()
                .font_weight(FontWeight::SEMIBOLD)
                .child(title),
        )
        .child(
            div()
                .text_sm()
                .text_color(theme.muted_foreground)
                .child(subtitle),
        )
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
