use super::{JsonReportPreviewRequest, ModalJsonReportPreview, report_display_text};
use gpui::{
    AppContext as _, Context, Entity, FocusHandle, InteractiveElement as _, IntoElement,
    ParentElement as _, Render, Styled as _, TestAppContext, VisualTestContext, Window, div, px,
    size,
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

    let clipboard = window.update(|_, cx| cx.read_from_clipboard().and_then(|item| item.text()));
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
