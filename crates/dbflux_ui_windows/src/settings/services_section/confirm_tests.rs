// Explicit imports: a parent glob would make `#[test]` resolve to
// `gpui::test`, whose expansion recurses without bound.
use super::ServicesSection;
use dbflux_components::modals::confirm::{CONFIRM_CANCEL_ID, CONFIRM_CONFIRM_ID};
use dbflux_core::{RpcServiceKind, ServiceConfig};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{
    AccessibilityFrame, AppContext as _, Bounds, Entity, FrameObserver, ParentElement as _, Pixels,
    TestAppContext, VisualTestContext,
};
use std::cell::RefCell;
use std::collections::HashMap;
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
    section: Entity<ServicesSection>,
    capture: Arc<FrameCapture>,
}

struct SectionView {
    section: Entity<ServicesSection>,
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
        let section = cx.new(|cx| ServicesSection::new(app_for_view.clone(), window, cx));
        slot_for_view.borrow_mut().replace(section.clone());
        SectionView { section }
    });
    window.run_until_parked();

    let section = slot.borrow().clone().expect("section built");
    (Harness { section, capture }, window)
}

fn service_config(socket_id: &str) -> ServiceConfig {
    ServiceConfig {
        socket_id: socket_id.to_string(),
        enabled: true,
        command: None,
        args: Vec::new(),
        env: HashMap::new(),
        startup_timeout_ms: None,
        kind: RpcServiceKind::Driver,
        api_contract: None,
    }
}

/// Stages one service and a pending delete of it.
fn stage_delete(harness: &Harness, window: &mut VisualTestContext) {
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.svc_services = vec![service_config("my-driver.sock")];
            section.request_delete_service(0, cx);
        });
    });
    window.run_until_parked();
}

#[gpui::test]
fn the_service_delete_confirmation_cancels_and_confirms(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    stage_delete(&harness, window);
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

    // Cancelling keeps the service and clears the pending delete.
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
            section.pending_delete_svc_idx, None,
            "cancelling clears the pending delete"
        );
        assert_eq!(
            section.svc_services.len(),
            1,
            "cancelling keeps the service"
        );
    });

    // Re-opening and confirming deletes the service.
    stage_delete(&harness, window);
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
            section.svc_services.is_empty(),
            "confirming deletes the service"
        );
    });
}
