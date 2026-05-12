//! Integration tests for `WorkspaceInspector`.

use dbflux_ui::ui::views::workspace::inspector::{
    INSPECTOR_DEFAULT_WIDTH, INSPECTOR_MAX_WIDTH, INSPECTOR_MIN_WIDTH, WorkspaceInspector,
};
use gpui::prelude::*;
use gpui::{AnyView, Context, Render, SharedString, TestAppContext, Window, div, px};

struct DummyContent;

impl Render for DummyContent {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
    }
}

#[gpui::test]
fn inspector_starts_closed(cx: &mut TestAppContext) {
    let inspector = cx.new(|cx| WorkspaceInspector::new(INSPECTOR_DEFAULT_WIDTH, cx));
    cx.read(|cx| {
        let insp = inspector.read(cx);
        assert!(!insp.is_open(), "inspector must start closed");
        assert!(!insp.is_resizing());
        assert_eq!(insp.width(), INSPECTOR_DEFAULT_WIDTH);
    });
}

#[gpui::test]
fn inspector_clamps_initial_width_to_min(cx: &mut TestAppContext) {
    let inspector = cx.new(|cx| WorkspaceInspector::new(px(10.0), cx));
    cx.read(|cx| {
        assert_eq!(inspector.read(cx).width(), INSPECTOR_MIN_WIDTH);
    });
}

#[gpui::test]
fn inspector_clamps_initial_width_to_max(cx: &mut TestAppContext) {
    let inspector = cx.new(|cx| WorkspaceInspector::new(px(9999.0), cx));
    cx.read(|cx| {
        assert_eq!(inspector.read(cx).width(), INSPECTOR_MAX_WIDTH);
    });
}

#[gpui::test]
fn inspector_opens_with_title(cx: &mut TestAppContext) {
    let inspector = cx.new(|cx| WorkspaceInspector::new(INSPECTOR_DEFAULT_WIDTH, cx));

    cx.update(|cx| {
        inspector.update(cx, |insp, cx| {
            let view = cx.new(|_| DummyContent);
            insp.open_with(AnyView::from(view), SharedString::from("Row 1"), cx);
        });
    });

    cx.read(|cx| {
        let insp = inspector.read(cx);
        assert!(insp.is_open());
        assert_eq!(insp.title().as_ref(), "Row 1");
    });
}

#[gpui::test]
fn inspector_close_sets_is_open_false(cx: &mut TestAppContext) {
    let inspector = cx.new(|cx| WorkspaceInspector::new(INSPECTOR_DEFAULT_WIDTH, cx));

    cx.update(|cx| {
        inspector.update(cx, |insp, cx| {
            let view = cx.new(|_| DummyContent);
            insp.open_with(AnyView::from(view), SharedString::from("Row 1"), cx);
        });
        inspector.update(cx, |insp, cx| {
            insp.close(cx);
        });
    });

    cx.read(|cx| {
        assert!(!inspector.read(cx).is_open());
    });
}

#[gpui::test]
fn inspector_resize_clamps_to_min_max(cx: &mut TestAppContext) {
    let inspector = cx.new(|cx| WorkspaceInspector::new(INSPECTOR_DEFAULT_WIDTH, cx));

    cx.update(|cx| {
        inspector.update(cx, |insp, cx| {
            // Simulate drag start at x=500, then move far right (clamps to min)
            insp.fake_begin_resize_at(px(500.0), cx);
            insp.update_resize(px(900.0), cx);
        });
    });

    cx.read(|cx| {
        assert_eq!(inspector.read(cx).width(), INSPECTOR_MIN_WIDTH);
    });
}

#[gpui::test]
fn inspector_begin_update_finish_resize(cx: &mut TestAppContext) {
    let inspector = cx.new(|cx| WorkspaceInspector::new(INSPECTOR_DEFAULT_WIDTH, cx));

    cx.update(|cx| {
        inspector.update(cx, |insp, cx| {
            // Simulate drag start at x=500, move 20px left (grows width by 20)
            let fake_event_x = px(500.0);
            insp.fake_begin_resize_at(fake_event_x, cx);
            insp.update_resize(fake_event_x - px(20.0), cx);
        });
    });

    cx.read(|cx| {
        let insp = inspector.read(cx);
        assert!(insp.is_resizing());
        assert_eq!(insp.width(), INSPECTOR_DEFAULT_WIDTH + px(20.0));
    });

    cx.update(|cx| {
        inspector.update(cx, |insp, cx| {
            insp.finish_resize(cx);
        });
    });

    cx.read(|cx| {
        let insp = inspector.read(cx);
        assert!(!insp.is_resizing());
    });
}

#[gpui::test]
fn inspector_update_resize_noop_when_not_resizing(cx: &mut TestAppContext) {
    let inspector = cx.new(|cx| WorkspaceInspector::new(INSPECTOR_DEFAULT_WIDTH, cx));

    cx.update(|cx| {
        inspector.update(cx, |insp, cx| {
            insp.update_resize(px(0.0), cx);
        });
    });

    cx.read(|cx| {
        assert_eq!(inspector.read(cx).width(), INSPECTOR_DEFAULT_WIDTH);
    });
}
