// Explicit imports: a parent glob would make `#[test]` resolve to
// `gpui::test`, whose expansion recurses without bound.
use super::{PendingProxyAction, ProxiesSection};
use dbflux_components::modals::confirm::{CONFIRM_CANCEL_ID, CONFIRM_CONFIRM_ID};
use dbflux_core::{ProxyKind, ProxyProfile};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{
    AccessibilityFrame, AppContext as _, Bounds, Entity, FrameObserver, ParentElement as _, Pixels,
    TestAppContext, VisualTestContext,
};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

/// Keeps the latest rendered frame of the window it observes.
#[derive(Default)]
struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

impl FrameObserver for FrameCapture {
    fn accessibility_updated(&self, frame: &AccessibilityFrame) {
        *self.0.lock().expect("frame capture lock") = Some(frame.clone());
    }
}

impl FrameCapture {
    fn bounds_of(&self, id: &str) -> Option<Bounds<Pixels>> {
        let frame = self.0.lock().expect("frame capture lock").clone()?;
        frame
            .nodes()
            .find(|(_, node)| node.id() == id)
            .map(|(_, node)| node.bounds())
    }
}

struct Harness {
    app_state: Entity<AppStateEntity>,
    section: Entity<ProxiesSection>,
    capture: Arc<FrameCapture>,
}

struct SectionView {
    section: Entity<ProxiesSection>,
}

impl gpui::Render for SectionView {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        _cx: &mut gpui::Context<Self>,
    ) -> impl gpui::IntoElement {
        gpui::div().child(self.section.clone())
    }
}

fn open(cx: &mut TestAppContext) -> (Harness, &mut VisualTestContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });

    let app_state: Entity<AppStateEntity> = cx.update(|cx| {
        cx.new(|_| {
            let runtime = StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });

    let capture = Arc::new(FrameCapture::default());
    let slot = Rc::new(RefCell::new(None));
    let capture_for_view = capture.clone();
    let app_for_view = app_state.clone();
    let slot_for_view = slot.clone();
    let (_view, window) = cx.add_window_view(move |window, cx| {
        window.observe_frames(&capture_for_view);
        let section = cx.new(|cx| ProxiesSection::new(app_for_view.clone(), window, cx));
        slot_for_view.borrow_mut().replace(section.clone());
        SectionView { section }
    });
    window.run_until_parked();

    let section = slot.borrow().clone().expect("section built");
    (
        Harness {
            app_state,
            section,
            capture,
        },
        window,
    )
}

/// Adds one proxy to the saved state and returns its id.
fn add_proxy(harness: &Harness, window: &mut VisualTestContext) -> ProxyProfile {
    let proxy = ProxyProfile::new("corp", ProxyKind::Http);
    window.update(|_, cx| {
        harness
            .app_state
            .update(cx, |state, _| state.add_proxy(proxy.clone()));
    });
    proxy
}

#[gpui::test]
fn the_proxy_delete_confirmation_cancels_and_confirms(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    let proxy = add_proxy(&harness, window);

    window.update(|_, cx| {
        harness
            .section
            .update(cx, |section, cx| section.request_delete_proxy(proxy.id, cx));
    });
    window.run_until_parked();
    assert!(
        window.update(|_, cx| {
            harness
                .section
                .read(cx)
                .delete_confirm
                .read(cx)
                .is_visible()
        }),
        "a pending delete opens the confirmation"
    );
    let cancel = harness
        .capture
        .bounds_of(CONFIRM_CANCEL_ID)
        .expect("the confirmation renders a cancel control");
    let confirm = harness
        .capture
        .bounds_of(CONFIRM_CONFIRM_ID)
        .expect("the confirmation renders a confirm control");
    assert_ne!(cancel, confirm, "the two controls are distinct elements");

    // Cancelling keeps the proxy and clears the pending delete.
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section
                .delete_confirm
                .update(cx, |modal, cx| modal.cancel(cx));
        });
    });
    window.run_until_parked();
    window.update(|_, cx| {
        let section = harness.section.read(cx);
        assert!(
            !section.delete_confirm.read(cx).is_visible(),
            "cancelling closes the confirmation"
        );
        assert_eq!(
            section.pending_delete_proxy_id, None,
            "cancelling clears the pending delete"
        );
        assert_eq!(
            harness.app_state.read(cx).proxies().len(),
            1,
            "cancelling keeps the proxy"
        );
    });

    // Re-opening and confirming deletes the proxy.
    window.update(|_, cx| {
        harness
            .section
            .update(cx, |section, cx| section.request_delete_proxy(proxy.id, cx));
    });
    window.run_until_parked();
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section
                .delete_confirm
                .update(cx, |modal, cx| modal.confirm(cx));
        });
    });
    window.run_until_parked();
    window.update(|_, cx| {
        let section = harness.section.read(cx);
        assert!(
            !section.delete_confirm.read(cx).is_visible(),
            "confirming closes the confirmation"
        );
        assert!(
            harness.app_state.read(cx).proxies().is_empty(),
            "confirming deletes the proxy"
        );
    });
}

#[gpui::test]
fn the_proxy_discard_confirmation_cancels_and_confirms(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    add_proxy(&harness, window);

    // Stage a pending switch to the saved proxy, as the dirty form does.
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.pending_discard_action = Some(PendingProxyAction::EditIndex(0));
            cx.notify();
        });
    });
    window.run_until_parked();
    assert!(
        window.update(|_, cx| {
            harness
                .section
                .read(cx)
                .discard_confirm
                .read(cx)
                .is_visible()
        }),
        "a pending discard opens the confirmation"
    );
    let cancel = harness
        .capture
        .bounds_of(CONFIRM_CANCEL_ID)
        .expect("the confirmation renders a cancel control");
    let confirm = harness
        .capture
        .bounds_of(CONFIRM_CONFIRM_ID)
        .expect("the confirmation renders a confirm control");
    assert_ne!(cancel, confirm, "the two controls are distinct elements");

    // Cancelling keeps the unsaved state and clears the pending action.
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section
                .discard_confirm
                .update(cx, |modal, cx| modal.cancel(cx));
        });
    });
    window.run_until_parked();
    window.update(|_, cx| {
        let section = harness.section.read(cx);
        assert!(
            !section.discard_confirm.read(cx).is_visible(),
            "cancelling closes the confirmation"
        );
        assert_eq!(
            section.pending_discard_action, None,
            "cancelling clears the pending discard"
        );
        assert_eq!(
            section.editing_proxy_id, None,
            "cancelling keeps the form untouched"
        );
    });

    // Re-opening and confirming applies the staged action.
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.pending_discard_action = Some(PendingProxyAction::EditIndex(0));
            cx.notify();
        });
    });
    window.run_until_parked();
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section
                .discard_confirm
                .update(cx, |modal, cx| modal.confirm(cx));
        });
    });
    window.run_until_parked();
    window.update(|_, cx| {
        let section = harness.section.read(cx);
        assert!(
            !section.discard_confirm.read(cx).is_visible(),
            "confirming closes the confirmation"
        );
        assert_eq!(
            section.pending_discard_action, None,
            "confirming clears the pending discard"
        );
        assert!(
            section.editing_proxy_id.is_some(),
            "confirming applies the staged action"
        );
    });
}
