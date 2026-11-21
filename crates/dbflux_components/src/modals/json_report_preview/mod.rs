//! `ModalJsonReportPreview` — a read-only viewer for one stored report.
//!
//! The body shows the payload prettified for reading; "Copy raw" puts the
//! stored bytes on the clipboard unchanged. There is no editor: the payload
//! is an audit record and cannot be modified here.

use crate::controls::Button;
use crate::icons::AppIcon;
use crate::modals::modal::{Modal, ModalFocus};
use crate::primitives::{Chamfer, Text};
use crate::tokens::{ChamferCut, ChromeColors, FontSizes, ModalMetrics};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::Scrollbar;

/// What to show: the stored bytes plus where they came from. The viewer is
/// domain-free, so the caller carries any catalog text in `meta`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonReportPreviewRequest {
    pub title: SharedString,
    /// One-line metadata under the title, such as time, level and size.
    pub meta: Option<SharedString>,
    /// Stored payload bytes, exactly as recorded.
    pub raw: String,
}

/// The payload as displayed: prettified when it is JSON, otherwise the stored
/// bytes unchanged. Copy always keeps `JsonReportPreviewRequest::raw`.
pub fn report_display_text(raw: &str) -> String {
    crate::components::json_editor_view::format_json(raw).unwrap_or_else(|| raw.to_owned())
}

pub struct ModalJsonReportPreview {
    visible: bool,
    request: Option<JsonReportPreviewRequest>,
    scroll: ScrollHandle,
    focus: ModalFocus,
}

impl ModalJsonReportPreview {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            visible: false,
            request: None,
            scroll: ScrollHandle::new(),
            focus: ModalFocus::new(cx),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, request: JsonReportPreviewRequest, cx: &mut Context<Self>) {
        self.request = Some(request);
        self.visible = true;
        self.scroll.set_offset(Point::default());
        // Captures what held focus before the viewer and moves focus into it
        // on the next render, following the modal convention.
        self.focus.focus_on_next_render();
        cx.notify();
    }

    pub fn hide(&mut self, cx: &mut Context<Self>) {
        if !self.visible {
            return;
        }
        self.visible = false;
        self.focus.restore(cx);
        cx.notify();
    }

    /// Whether focus currently sits inside the viewer.
    pub fn contains_focus(&self, window: &Window, cx: &App) -> bool {
        self.focus.handle().contains_focused(window, cx)
    }

    /// Current scroll offset of the read-only body.
    pub fn scroll_offset(&self) -> Point<Pixels> {
        self.scroll.offset()
    }

    fn copy_raw(&self, cx: &mut App) {
        if let Some(request) = &self.request {
            cx.write_to_clipboard(ClipboardItem::new_string(request.raw.clone()));
        }
    }
}

impl ModalJsonReportPreview {
    /// The scrolling, read-only payload text with its scrollbar.
    fn render_body(&self, display: String, cx: &Context<Self>) -> Div {
        let theme = cx.theme();
        div()
            .relative()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .child(
                div()
                    .id("json-report-preview-body")
                    .min_h_0()
                    .flex()
                    .flex_col()
                    .overflow_y_scroll()
                    .track_scroll(&self.scroll)
                    .p(ModalMetrics::PADDING)
                    .child(
                        div().flex_none().child(
                            div()
                                .relative()
                                .p(ModalMetrics::PADDING)
                                .child(
                                    Chamfer::new(ChamferCut::INPUT)
                                        .fill(theme.background)
                                        .border(theme.border),
                                )
                                .child(
                                    div()
                                        .font_family(crate::fonts::editor_family(cx))
                                        .text_size(FontSizes::SM)
                                        .text_color(ChromeColors::strong(theme))
                                        .child(display),
                                ),
                        ),
                    ),
            )
            .child(
                div()
                    .absolute()
                    .inset_0()
                    .child(Scrollbar::vertical(&self.scroll)),
            )
    }

    /// The footer with the copy and close actions.
    fn render_footer(cx: &Context<Self>) -> Div {
        let theme = cx.theme();
        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .justify_end()
            .gap(ModalMetrics::FOOTER_GAP)
            .px(ModalMetrics::PADDING)
            .py(ModalMetrics::FOOTER_PADDING_Y)
            .border_t_1()
            .border_color(theme.border)
            .child(
                Button::new(
                    "json-report-preview-copy",
                    dbflux_i18n::t!("components.json_report_preview.copy_raw"),
                )
                .icon(AppIcon::Copy)
                .on_click(cx.listener(|this, _, _, cx| this.copy_raw(cx))),
            )
            .child(
                Button::new(
                    "json-report-preview-close",
                    dbflux_i18n::t!("components.json_report_preview.close"),
                )
                .primary()
                .on_click(cx.listener(|this, _, _, cx| this.hide(cx))),
            )
    }
}

impl Render for ModalJsonReportPreview {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        self.focus.apply_pending(window, cx);

        let theme = cx.theme();
        let Some(request) = self.request.clone() else {
            return div().into_any_element();
        };
        let display = report_display_text(&request.raw);

        let entity = cx.entity().downgrade();
        let entity_for_confirm = entity.clone();
        let close = move |_window: &mut Window, cx: &mut App| {
            entity.update(cx, |this, cx| this.hide(cx)).ok();
        };
        let confirm_close = move |_window: &mut Window, cx: &mut App| {
            entity_for_confirm.update(cx, |this, cx| this.hide(cx)).ok();
        };

        let body = self.render_body(display, cx);
        let footer = Self::render_footer(cx);

        let column = div()
            .flex()
            .flex_col()
            .min_h_0()
            .flex_1()
            .when_some(request.meta.clone(), |column, meta| {
                column.child(
                    div()
                        .flex_shrink_0()
                        .px(ModalMetrics::PADDING)
                        .pt(ModalMetrics::FOOTER_PADDING_Y)
                        .child(Text::caption(meta).text_color(theme.muted_foreground)),
                )
            })
            .child(body)
            .child(footer);

        Modal::new(request.title)
            .id("json-report-preview-modal")
            .focus_handle(self.focus.handle())
            .width(px(760.0))
            .height(px(560.0))
            .icon(AppIcon::Braces)
            .scroll_with_keys(&self.scroll)
            .on_close(close)
            .on_confirm(confirm_close)
            .child(column)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests;
