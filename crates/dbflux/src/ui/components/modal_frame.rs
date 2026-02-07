use std::rc::Rc;

use crate::keymap::{Cancel, ContextId};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::*;
use gpui_component::ActiveTheme;

type CloseHandler = Rc<dyn Fn(&mut Window, &mut App)>;

/// Reusable modal shell: dark overlay, centered container, header with icon/title/close.
///
/// Use the builder API to configure, then call `.child()` to add body content.
/// The `on_close` callback is shared across the overlay click, the X button,
/// and the `Cancel` action (Escape key).
pub struct ModalFrame {
    id: ElementId,
    focus_handle: FocusHandle,
    context_id: ContextId,
    title: SharedString,
    icon: AppIcon,
    width: Pixels,
    height: ModalHeight,
    top_offset: Pixels,
    on_close: CloseHandler,
    header_extra: Option<AnyElement>,
    block_scroll: bool,
    children: Vec<AnyElement>,
}

enum ModalHeight {
    Fixed(Pixels),
    Max(Pixels),
}

impl ModalFrame {
    pub fn new(
        id: impl Into<ElementId>,
        focus_handle: &FocusHandle,
        on_close: impl Fn(&mut Window, &mut App) + 'static,
    ) -> Self {
        Self {
            id: id.into(),
            focus_handle: focus_handle.clone(),
            context_id: ContextId::SqlPreviewModal,
            title: SharedString::default(),
            icon: AppIcon::X,
            width: px(900.0),
            height: ModalHeight::Fixed(px(600.0)),
            top_offset: px(80.0),
            on_close: Rc::new(on_close),
            header_extra: None,
            block_scroll: false,
            children: Vec::new(),
        }
    }

    pub fn title(mut self, title: impl Into<SharedString>) -> Self {
        self.title = title.into();
        self
    }

    pub fn icon(mut self, icon: AppIcon) -> Self {
        self.icon = icon;
        self
    }

    #[allow(dead_code)]
    pub fn context_id(mut self, context_id: ContextId) -> Self {
        self.context_id = context_id;
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

    pub fn header_extra(mut self, element: impl IntoElement) -> Self {
        self.header_extra = Some(element.into_any_element());
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

    pub fn render(self, cx: &App) -> AnyElement {
        let theme = cx.theme();

        let close_for_overlay = self.on_close.clone();
        let close_for_button = self.on_close.clone();
        let close_for_action = self.on_close.clone();

        // Build the container with width and height
        let mut container = div()
            .w(self.width)
            .bg(theme.background)
            .border_1()
            .border_color(theme.border)
            .rounded(Radii::LG)
            .shadow_lg()
            .overflow_hidden()
            .flex()
            .flex_col()
            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            });

        match self.height {
            ModalHeight::Fixed(h) => container = container.h(h),
            ModalHeight::Max(h) => container = container.max_h(h),
        };

        // Header
        let mut header_left = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                svg()
                    .path(self.icon.path())
                    .size_4()
                    .text_color(theme.primary),
            )
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(theme.foreground)
                    .child(self.title),
            );

        if let Some(extra) = self.header_extra {
            header_left = header_left.child(extra);
        }

        let header = div()
            .flex()
            .items_center()
            .justify_between()
            .px(Spacing::MD)
            .py(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .child(header_left)
            .child(
                div()
                    .id("close-btn")
                    .flex()
                    .items_center()
                    .justify_center()
                    .size(Heights::ICON_SM)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(move |_, window, cx| {
                        (close_for_button)(window, cx);
                    })
                    .child(
                        svg()
                            .path(AppIcon::X.path())
                            .size_4()
                            .text_color(theme.muted_foreground),
                    ),
            );

        container = container.child(header);

        // Body children
        for child in self.children {
            container = container.child(child);
        }

        // Overlay
        let mut overlay = div()
            .id(self.id)
            .key_context(self.context_id.as_gpui_context())
            .track_focus(&self.focus_handle)
            .absolute()
            .inset_0()
            .bg(gpui::black().opacity(0.5))
            .flex()
            .justify_center()
            .items_start()
            .pt(self.top_offset)
            .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                (close_for_overlay)(window, cx);
            })
            .on_action(move |_: &Cancel, window, cx| {
                (close_for_action)(window, cx);
            });

        if self.block_scroll {
            overlay = overlay.on_scroll_wheel(|_, _, cx| {
                cx.stop_propagation();
            });
        }

        overlay = overlay.child(container);

        overlay.into_any_element()
    }
}
