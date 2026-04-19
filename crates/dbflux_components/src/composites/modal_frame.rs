use gpui::prelude::*;
use gpui::{App, SharedString, div};
use gpui_component::ActiveTheme;

use crate::primitives::{SurfaceRole, Text, surface_modal_container};
use crate::tokens::Spacing;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModalFrameVariant {
    Dialog,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ModalFrameTitleInspection {
    pub variant: crate::primitives::TextVariant,
    pub uses_role_default_color: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ModalFrameInspection {
    pub scrim_role: SurfaceRole,
    pub container_role: SurfaceRole,
    pub header_padding_x: gpui::Pixels,
    pub header_padding_y: gpui::Pixels,
    pub has_close_button: bool,
    pub has_header_extra: bool,
    pub title: ModalFrameTitleInspection,
}

pub fn inspect_modal_frame(
    variant: ModalFrameVariant,
    has_header_extra: bool,
) -> ModalFrameInspection {
    let title = match variant {
        ModalFrameVariant::Dialog => Text::label_sm("Modal"),
    };

    ModalFrameInspection {
        scrim_role: SurfaceRole::Scrim,
        container_role: SurfaceRole::ModalContainer,
        header_padding_x: Spacing::MD,
        header_padding_y: Spacing::SM,
        has_close_button: true,
        has_header_extra,
        title: ModalFrameTitleInspection {
            variant: crate::primitives::TextVariant::LabelSm,
            uses_role_default_color: title.uses_role_default_color(),
        },
    }
}

pub fn modal_frame(title: impl Into<SharedString>, body: impl IntoElement, cx: &App) -> gpui::Div {
    modal_frame_with_header_extra(title, None, body, cx)
}

pub fn modal_frame_with_header_extra(
    title: impl Into<SharedString>,
    header_extra: Option<gpui::AnyElement>,
    body: impl IntoElement,
    cx: &App,
) -> gpui::Div {
    let theme = cx.theme();
    let inspection = inspect_modal_frame(ModalFrameVariant::Dialog, header_extra.is_some());

    let mut header_left = div()
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .child(Text::label_sm(title));

    if let Some(extra) = header_extra {
        header_left = header_left.child(extra);
    }

    let header = div()
        .flex()
        .items_center()
        .justify_between()
        .px(inspection.header_padding_x)
        .py(inspection.header_padding_y)
        .border_b_1()
        .border_color(theme.border)
        .child(header_left)
        .child(
            div()
                .px(Spacing::SM)
                .py(Spacing::XS)
                .rounded_sm()
                .text_size(crate::tokens::FontSizes::SM)
                .text_color(theme.muted_foreground)
                .child("Close"),
        );

    div()
        .absolute()
        .inset_0()
        .bg(crate::primitives::overlay_bg())
        .flex()
        .items_center()
        .justify_center()
        .child(
            surface_modal_container(cx)
                .min_w(gpui::px(320.0))
                .max_w(gpui::px(900.0))
                .shadow_lg()
                .overflow_hidden()
                .flex()
                .flex_col()
                .child(header)
                .child(body),
        )
}

#[cfg(test)]
mod tests {
    use super::{ModalFrameVariant, inspect_modal_frame};
    use crate::primitives::{SurfaceRole, TextVariant};
    use crate::tokens::Spacing;

    #[test]
    fn modal_frame_keeps_scrim_container_and_title_contracts_centralized() {
        let inspection = inspect_modal_frame(ModalFrameVariant::Dialog, false);

        assert_eq!(inspection.scrim_role, SurfaceRole::Scrim);
        assert_eq!(inspection.container_role, SurfaceRole::ModalContainer);
        assert_eq!(inspection.title.variant, TextVariant::LabelSm);
        assert!(inspection.title.uses_role_default_color);
    }

    #[test]
    fn modal_frame_header_contract_tracks_close_button_and_extra_content_slots() {
        let without_extra = inspect_modal_frame(ModalFrameVariant::Dialog, false);
        assert_eq!(without_extra.header_padding_x, Spacing::MD);
        assert_eq!(without_extra.header_padding_y, Spacing::SM);
        assert!(without_extra.has_close_button);
        assert!(!without_extra.has_header_extra);

        let with_extra = inspect_modal_frame(ModalFrameVariant::Dialog, true);
        assert!(with_extra.has_header_extra);
    }
}
