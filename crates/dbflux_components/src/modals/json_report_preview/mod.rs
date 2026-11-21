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

        let body = div()
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
            );

        let footer = div()
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
            );

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
mod tests {
    use super::{JsonReportPreviewRequest, ModalJsonReportPreview, report_display_text};
    use gpui::{
        AppContext as _, Context, Entity, FocusHandle, InteractiveElement as _, IntoElement,
        ParentElement as _, Render, Styled as _, TestAppContext, VisualTestContext, Window, div,
        px, size,
    };

    fn request() -> JsonReportPreviewRequest {
        JsonReportPreviewRequest {
            title: "Telemetry report".into(),
            meta: Some("2026-01-01 00:00 · basic · 128 B".into()),
            raw: r#"{"schema":"dbflux.telemetry-event/v1","event":"basic","features":{"query_run":3}}"#
                .to_owned(),
        }
    }

    fn large_request() -> JsonReportPreviewRequest {
        let mut raw = String::from("{\"lines\":[");
        for index in 0..400 {
            if index > 0 {
                raw.push(',');
            }
            raw.push_str(&format!(
                "\"line {index:04} with enough text to wrap a width-limited body row\""
            ));
        }
        raw.push_str("]}");
        JsonReportPreviewRequest {
            title: "Telemetry report".into(),
            meta: None,
            raw,
        }
    }

    #[test]
    fn display_prettifies_json_and_keeps_other_bytes() {
        let pretty = report_display_text(&request().raw);
        assert!(pretty.contains('\n'), "JSON is prettified for reading");
        assert_eq!(
            report_display_text("not json at all"),
            "not json at all",
            "non-JSON bytes are shown unchanged"
        );
        // Reading is a presentation only: compaction would restore the bytes.
        let compact: serde_json::Value =
            serde_json::from_str(&pretty).expect("pretty text still parses");
        assert_eq!(compact["event"], "basic");
    }

    struct Host {
        modal: Entity<ModalJsonReportPreview>,
        focus: FocusHandle,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .size_full()
                .track_focus(&self.focus)
                .child(self.modal.clone())
        }
    }

    fn open_viewer(
        cx: &mut TestAppContext,
        request: JsonReportPreviewRequest,
    ) -> (
        Entity<Host>,
        Entity<ModalJsonReportPreview>,
        &mut VisualTestContext,
    ) {
        super::super::modal::bind_modal_keys_for_tests(cx);
        cx.update(|cx| {
            gpui_component::init(cx);
            crate::theme::init(cx);
        });
        let slot = std::rc::Rc::new(std::cell::RefCell::new(None));
        let slot_for_view = slot.clone();
        let (host, window) = cx.add_window_view({
            let slot_for_view = slot_for_view.clone();
            move |_window, cx| {
                let modal = cx.new(ModalJsonReportPreview::new);
                slot_for_view.borrow_mut().replace(modal.clone());
                Host {
                    modal,
                    focus: cx.focus_handle(),
                }
            }
        });
        window.run_until_parked();
        window.update(|window, cx| {
            let focus = host.read(cx).focus.clone();
            focus.focus(window, cx);
        });
        window.run_until_parked();

        let modal = slot.borrow().clone().expect("modal built");
        window.update(|_, cx| {
            modal.update(cx, |viewer, cx| viewer.open(request, cx));
        });
        window.run_until_parked();
        (host, modal, window)
    }

    #[gpui::test]
    fn escape_closes_the_viewer_and_restores_the_previous_focus(cx: &mut TestAppContext) {
        let (host, modal, window) = open_viewer(cx, request());

        let viewer_focused = window.update(|window, cx| modal.read(cx).contains_focus(window, cx));
        assert!(viewer_focused, "opening the viewer moves focus into it");

        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));

        // What held focus before the viewer (the host) has it back.
        let restored = window.update(|window, cx| host.read(cx).focus.contains_focused(window, cx));
        assert!(restored, "the previous focus is restored after close");
    }

    #[gpui::test]
    fn a_large_body_scrolls_with_the_keyboard(cx: &mut TestAppContext) {
        let (_host, modal, window) = open_viewer(cx, large_request());
        window.simulate_resize(size(px(900.0), px(400.0)));
        window.run_until_parked();
        window.update(|window, _| window.refresh());

        window.simulate_keystrokes("pagedown");
        window.run_until_parked();

        let offset = window.update(|_, cx| modal.read(cx).scroll_offset().y);
        assert!(
            offset < px(0.0),
            "pagedown scrolls the read-only body, offset was {offset:?}"
        );
    }

    #[gpui::test]
    fn the_viewer_is_read_only_and_copy_keeps_the_raw_bytes(cx: &mut TestAppContext) {
        let (_host, modal, window) = open_viewer(cx, request());

        // Typing anywhere cannot edit the payload: the body is plain text.
        window.simulate_keystrokes("hello world");
        window.run_until_parked();
        assert!(window.update(|_, cx| modal.read(cx).is_visible()));

        // The copy control runs on the entity (the click path is covered by
        // the settings window's keyboard tests); it must put the stored bytes
        // on the clipboard unchanged.
        window.update(|_, cx| {
            modal.update(cx, |viewer, cx| viewer.copy_raw(cx));
        });

        let clipboard =
            window.update(|_, cx| cx.read_from_clipboard().and_then(|item| item.text()));
        assert_eq!(
            clipboard.as_deref(),
            Some(request().raw.as_str()),
            "copy preserves the stored bytes exactly"
        );

        window.update(|_, cx| {
            modal.update(cx, |viewer, cx| viewer.hide(cx));
        });
        window.run_until_parked();
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
    }

    #[test]
    fn viewer_copy_resolves_in_every_locale() {
        for key in [
            "components.json_report_preview.copy_raw",
            "components.json_report_preview.close",
        ] {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing in {locale}"
                );
                if locale != "en" {
                    assert_ne!(value, dbflux_i18n::t!(key, locale = "en"));
                }
            }
        }
    }
}
