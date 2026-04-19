use std::sync::Arc;

use gpui::prelude::*;
use gpui::{App, ElementId, FocusHandle, MouseButton, Pixels, SharedString, Window, div, px};
use gpui_component::ActiveTheme;

use crate::icon::IconSource;
use crate::primitives::{IconButton, SurfaceRole, Text, surface_modal_container};
use crate::tokens::Heights;
use crate::tokens::Spacing;

type CloseHandler = Arc<dyn Fn(&mut Window, &mut App) + Send + Sync>;

enum ModalHeight {
    Fixed(Pixels),
    Max(Pixels),
}

enum ModalFrameCloseAffordance {
    Label,
    Icon(IconSource),
}

pub struct ModalFrame {
    id: ElementId,
    focus_handle: FocusHandle,
    key_context: Option<String>,
    title: SharedString,
    width: Pixels,
    height: ModalHeight,
    top_offset: Pixels,
    on_close: CloseHandler,
    header_leading: Option<gpui::AnyElement>,
    header_extra: Option<gpui::AnyElement>,
    close_affordance: ModalFrameCloseAffordance,
    block_scroll: bool,
    children: Vec<gpui::AnyElement>,
}

impl ModalFrame {
    pub fn new(
        id: impl Into<ElementId>,
        focus_handle: &FocusHandle,
        on_close: impl Fn(&mut Window, &mut App) + Send + Sync + 'static,
    ) -> Self {
        Self {
            id: id.into(),
            focus_handle: focus_handle.clone(),
            key_context: None,
            title: SharedString::default(),
            width: px(900.0),
            height: ModalHeight::Fixed(px(600.0)),
            top_offset: px(80.0),
            on_close: Arc::new(on_close),
            header_leading: None,
            header_extra: None,
            close_affordance: ModalFrameCloseAffordance::Label,
            block_scroll: false,
            children: Vec::new(),
        }
    }

    pub fn key_context(mut self, key_context: impl Into<String>) -> Self {
        self.key_context = Some(key_context.into());
        self
    }

    pub fn title(mut self, title: impl Into<SharedString>) -> Self {
        self.title = title.into();
        self
    }

    pub fn width(mut self, width: Pixels) -> Self {
        self.width = width;
        self
    }

    pub fn height(mut self, height: Pixels) -> Self {
        self.height = ModalHeight::Fixed(height);
        self
    }

    pub fn max_height(mut self, height: Pixels) -> Self {
        self.height = ModalHeight::Max(height);
        self
    }

    pub fn top_offset(mut self, offset: Pixels) -> Self {
        self.top_offset = offset;
        self
    }

    pub fn header_leading(mut self, element: impl IntoElement) -> Self {
        self.header_leading = Some(element.into_any_element());
        self
    }

    pub fn header_extra(mut self, element: impl IntoElement) -> Self {
        self.header_extra = Some(element.into_any_element());
        self
    }

    pub fn close_icon(mut self, icon: IconSource) -> Self {
        self.close_affordance = ModalFrameCloseAffordance::Icon(icon);
        self
    }

    pub fn block_scroll(mut self) -> Self {
        self.block_scroll = true;
        self
    }

    pub fn child(mut self, child: impl IntoElement) -> Self {
        self.children.push(child.into_any_element());
        self
    }

    pub fn render(self, cx: &App) -> gpui::AnyElement {
        let theme = cx.theme();

        let close_for_overlay = self.on_close.clone();
        let close_for_button = self.on_close.clone();
        let close_for_action = self.on_close.clone();

        let inspection =
            inspect_modal_frame(ModalFrameVariant::Dialog, self.header_extra.is_some());

        let mut container = surface_modal_container(cx)
            .w(self.width)
            .shadow_lg()
            .overflow_hidden()
            .flex()
            .flex_col()
            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            });

        match self.height {
            ModalHeight::Fixed(height) => container = container.h(height),
            ModalHeight::Max(height) => container = container.max_h(height),
        };

        let mut header_left = div().flex().items_center().gap(Spacing::SM);

        if let Some(leading) = self.header_leading {
            header_left = header_left.child(leading);
        }

        header_left = header_left.child(Text::label_sm(self.title));

        if let Some(extra) = self.header_extra {
            header_left = header_left.child(extra);
        }

        let close_control = match self.close_affordance {
            ModalFrameCloseAffordance::Label => div()
                .px(Spacing::SM)
                .py(Spacing::XS)
                .rounded_sm()
                .cursor_pointer()
                .text_size(crate::tokens::FontSizes::SM)
                .text_color(theme.muted_foreground)
                .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                    (close_for_button)(window, cx);
                })
                .child("Close")
                .into_any_element(),
            ModalFrameCloseAffordance::Icon(icon) => IconButton::new("close-btn", icon)
                .icon_size(Heights::ICON_SM)
                .on_click(move |_, window, cx| {
                    (close_for_button)(window, cx);
                })
                .into_any_element(),
        };

        let header = div()
            .flex()
            .items_center()
            .justify_between()
            .px(inspection.header_padding_x)
            .py(inspection.header_padding_y)
            .border_b_1()
            .border_color(theme.border)
            .child(header_left)
            .child(close_control);

        container = container.child(header);

        for child in self.children {
            container = container.child(child);
        }

        let mut overlay = div()
            .id(self.id)
            .track_focus(&self.focus_handle)
            .absolute()
            .inset_0()
            .bg(crate::primitives::overlay_bg(theme))
            .flex()
            .justify_center()
            .items_start()
            .pt(self.top_offset)
            .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                (close_for_overlay)(window, cx);
            })
            .on_action(move |_: &crate::actions::Cancel, window, cx| {
                (close_for_action)(window, cx);
            });

        if let Some(key_context) = self.key_context {
            overlay = overlay.key_context(key_context.as_str());
        }

        if self.block_scroll {
            overlay = overlay.on_scroll_wheel(|_, _, cx| {
                cx.stop_propagation();
            });
        }

        overlay.child(container).into_any_element()
    }
}

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
        .bg(crate::primitives::overlay_bg(theme))
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
