use dbflux_components::primitives::Text;
use gpui::prelude::*;
use gpui::*;
use gpui_component::scroll::ScrollableElement;

use crate::ui::theme::ghost_border_color;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StickyFooterLayout {
    FullWidth,
}

fn sticky_footer_layout() -> StickyFooterLayout {
    StickyFooterLayout::FullWidth
}

pub(super) fn compact_input_shell(child: impl IntoElement) -> Div {
    div().w_full().child(child)
}

pub(super) fn editor_panel_title(noun: &str, is_editing: bool) -> String {
    let prefix = if is_editing { "Edit" } else { "New" };

    format!("{} {}", prefix, noun)
}

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
        .child(div().p_4().border_t_1().border_color(theme.border).child(
            match sticky_footer_layout() {
                StickyFooterLayout::FullWidth => div().w_full().child(footer),
            },
        ))
}

#[cfg(test)]
mod tests {
    use super::{
        StickyFooterLayout, compact_input_shell, editor_panel_title, sticky_footer_layout,
    };
    use gpui::div;

    #[test]
    fn editor_panel_title_uses_new_prefix_when_creating() {
        assert_eq!(editor_panel_title("Proxy", false), "New Proxy");
        assert_eq!(
            editor_panel_title("Auth Profile", false),
            "New Auth Profile"
        );
    }

    #[test]
    fn editor_panel_title_uses_edit_prefix_when_updating() {
        assert_eq!(editor_panel_title("Proxy", true), "Edit Proxy");
        assert_eq!(editor_panel_title("SSH Tunnel", true), "Edit SSH Tunnel");
    }

    #[test]
    fn sticky_form_footer_preserves_full_width_layout() {
        assert_eq!(sticky_footer_layout(), StickyFooterLayout::FullWidth);
    }

    #[test]
    fn compact_settings_inputs_skip_standard_control_shell() {
        let _ = compact_input_shell(div());
    }
}
