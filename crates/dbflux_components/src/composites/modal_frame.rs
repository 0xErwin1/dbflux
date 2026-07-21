use std::sync::Arc;

use gpui::prelude::*;
use gpui::{App, ElementId, FocusHandle, MouseButton, Pixels, SharedString, Window, div, px};
use gpui_component::ActiveTheme;

use crate::icon::IconSource;
use crate::primitives::{IconButton, SurfaceRole, Text, surface_modal_container};
use crate::tokens::{ChromeEdgeRole, Heights, Spacing};

type CloseHandler = Arc<dyn Fn(&mut Window, &mut App) + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq)]
enum ModalHeight {
    Fixed(Pixels),
    Max(Pixels),
    Fraction(f32),
}

/// Whether the modal overlay anchors its container near the top (the default for
/// every existing modal) or centers it vertically (opt-in via `center_vertically()`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OverlayVerticalPlacement {
    TopAnchored,
    Centered,
}

fn overlay_vertical_placement(center_vertically: bool) -> OverlayVerticalPlacement {
    if center_vertically {
        OverlayVerticalPlacement::Centered
    } else {
        OverlayVerticalPlacement::TopAnchored
    }
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
    center_vertically: bool,
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
            center_vertically: false,
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

    /// Opt-in: vertically centers the overlay instead of anchoring it near the top.
    /// Every other modal keeps the default top-anchored behavior unless it calls this.
    pub fn center_vertically(mut self) -> Self {
        self.center_vertically = true;
        self
    }

    /// Opt-in: sizes the container as a fraction of the viewport height (e.g. `0.8` = 80%).
    /// Every other modal keeps its default fixed/max height unless it calls this.
    pub fn height_fraction(mut self, fraction: f32) -> Self {
        self.height = ModalHeight::Fraction(fraction);
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
            ModalHeight::Fraction(fraction) => container = container.h(gpui::relative(fraction)),
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
            .border_color(inspection.header_separator_edge.resolve(theme))
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
            .justify_center();

        overlay = match overlay_vertical_placement(self.center_vertically) {
            OverlayVerticalPlacement::Centered => overlay.items_center(),
            OverlayVerticalPlacement::TopAnchored => overlay.items_start().pt(self.top_offset),
        };

        let mut overlay = overlay
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
    pub header_separator_edge: ChromeEdgeRole,
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
        header_separator_edge: ChromeEdgeRole::ModalSeparator,
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
        .border_color(inspection.header_separator_edge.resolve(theme))
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
    use super::{
        ModalFrame, ModalFrameVariant, ModalHeight, OverlayVerticalPlacement, inspect_modal_frame,
        overlay_vertical_placement,
    };
    use crate::primitives::{SurfaceRole, TextVariant};
    use crate::tokens::ChromeEdgeRole;
    use crate::tokens::Spacing;
    use gpui::{TestAppContext, px};

    #[test]
    fn modal_frame_keeps_scrim_container_and_title_contracts_centralized() {
        let inspection = inspect_modal_frame(ModalFrameVariant::Dialog, false);

        assert_eq!(inspection.scrim_role, SurfaceRole::Scrim);
        assert_eq!(inspection.container_role, SurfaceRole::ModalContainer);
        assert_eq!(inspection.title.variant, TextVariant::LabelSm);
        assert!(inspection.title.uses_role_default_color);
        assert_eq!(
            inspection.header_separator_edge,
            ChromeEdgeRole::ModalSeparator
        );
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

    // ── overlay_vertical_placement (pure) ──────────────────────

    #[test]
    fn overlay_vertical_placement_defaults_to_top_anchored() {
        assert_eq!(
            overlay_vertical_placement(false),
            OverlayVerticalPlacement::TopAnchored
        );
    }

    #[test]
    fn overlay_vertical_placement_centers_when_opted_in() {
        assert_eq!(
            overlay_vertical_placement(true),
            OverlayVerticalPlacement::Centered
        );
    }

    // ── ModalFrame builder state (regression guard for R6) ─────

    #[gpui::test]
    fn default_modal_frame_stays_top_anchored_with_fixed_height(cx: &mut TestAppContext) {
        let focus_handle = cx.update(|cx| cx.focus_handle());
        let frame = ModalFrame::new("regression-modal", &focus_handle, |_, _| {});

        assert!(!frame.center_vertically);
        assert_eq!(frame.top_offset, px(80.0));
        assert_eq!(frame.height, ModalHeight::Fixed(px(600.0)));
    }

    #[gpui::test]
    fn center_vertically_and_height_fraction_opt_into_centered_layout(cx: &mut TestAppContext) {
        let focus_handle = cx.update(|cx| cx.focus_handle());
        let frame = ModalFrame::new("wizard-modal", &focus_handle, |_, _| {})
            .center_vertically()
            .height_fraction(0.8);

        assert!(frame.center_vertically);
        assert_eq!(frame.height, ModalHeight::Fraction(0.8));
    }
}
