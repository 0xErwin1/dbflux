// Explicit imports: a parent glob would make `#[test]` resolve to
// `gpui::test`, whose expansion recurses without bound.
use super::HooksSection;
use dbflux_app::config_loader::{EditableGlobalHook, HookDefinitionSave};
use dbflux_components::modals::confirm::{CONFIRM_CANCEL_ID, CONFIRM_CONFIRM_ID};
use dbflux_core::{ConnectionHook, HookExecutionMode, HookFailureMode, HookKind};
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
    app_state: Entity<AppStateEntity>,
    section: Entity<HooksSection>,
    capture: Arc<FrameCapture>,
}

struct SectionView {
    section: Entity<HooksSection>,
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

fn command_hook(command: &str) -> ConnectionHook {
    ConnectionHook {
        enabled: true,
        kind: HookKind::Command {
            command: command.to_string(),
            args: Vec::new(),
        },
        cwd: None,
        env: HashMap::new(),
        inherit_env: true,
        env_denylist: Vec::new(),
        timeout_ms: None,
        execution_mode: HookExecutionMode::Blocking,
        ready_signal: None,
        on_failure: HookFailureMode::Disconnect,
    }
}

fn app_state_with_runtime(cx: &mut gpui::App, runtime: StorageRuntime) -> Entity<AppStateEntity> {
    cx.new(|_| AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup"))
}

fn open(cx: &mut TestAppContext) -> (Harness, &mut VisualTestContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });

    let app_state = cx.update(|cx| {
        app_state_with_runtime(cx, StorageRuntime::in_memory().expect("in-memory storage"))
    });

    let capture = Arc::new(FrameCapture::default());
    let slot = Rc::new(RefCell::new(None));
    let capture_for_view = capture.clone();
    let app_for_view = app_state.clone();
    let slot_for_view = slot.clone();
    let (_view, window) = cx.add_window_view(move |window, cx| {
        window.observe_frames(&capture_for_view);
        let section = cx.new(|cx| HooksSection::new(app_for_view.clone(), window, cx));
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

#[gpui::test]
fn the_hook_delete_confirmation_cancels_and_confirms(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.hook_definitions.insert(
                "notify".to_string(),
                EditableGlobalHook {
                    id: Some("hook-1".to_string()),
                    hook: command_hook("notify-send"),
                },
            );
            section.request_delete_hook("notify".to_string(), cx);
        });
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

    // Cancelling keeps the hook and clears the pending delete.
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
            section.pending_delete_hook_id, None,
            "cancelling clears the pending delete"
        );
        assert!(
            section.hook_definitions.contains_key("notify"),
            "cancelling keeps the hook"
        );
    });

    // Re-opening and confirming deletes the hook.
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.request_delete_hook("notify".to_string(), cx)
        });
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
            !section.hook_definitions.contains_key("notify"),
            "confirming deletes the hook"
        );
    });
}

/// A protected (unreadable) hook row, seeded the way the loader finds
/// them: a saved canonical row whose payload is corrupted on disk.
fn open_with_protected_row(cx: &mut TestAppContext) -> (Harness, &mut VisualTestContext, String) {
    let runtime = StorageRuntime::in_memory().expect("in-memory storage");
    let saved = dbflux_app::config_loader::save_hook_definitions(
        &runtime,
        &[HookDefinitionSave {
            id: None,
            name: "broken".to_string(),
            hook: command_hook("echo broken"),
        }],
        &[],
    )
    .expect("save hook");
    let row_id = saved["broken"].id.clone().expect("generated id");

    let repo = runtime.hook_definitions();
    let mut dto = repo.get(&row_id).expect("load row").expect("saved row");
    dto.kind_json = Some("{not valid json".to_string());
    repo.update(&dto).expect("corrupt canonical payload");

    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });
    let app_state = cx.update(|cx| app_state_with_runtime(cx, runtime));

    let capture = Arc::new(FrameCapture::default());
    let slot = Rc::new(RefCell::new(None));
    let capture_for_view = capture.clone();
    let app_for_view = app_state.clone();
    let slot_for_view = slot.clone();
    let (_view, window) = cx.add_window_view(move |window, cx| {
        window.observe_frames(&capture_for_view);
        let section = cx.new(|cx| HooksSection::new(app_for_view.clone(), window, cx));
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
        row_id,
    )
}

#[gpui::test]
fn the_protected_row_delete_confirmation_cancels_and_confirms(cx: &mut TestAppContext) {
    let (harness, window, row_id) = open_with_protected_row(cx);

    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.request_delete_protected_row(row_id.clone(), cx);
        });
    });
    window.run_until_parked();
    assert!(
        window.update(|_, cx| {
            harness
                .section
                .read(cx)
                .protected_delete_confirm
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

    // Cancelling keeps the unreadable row.
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section
                .protected_delete_confirm
                .update(cx, |modal, cx| modal.cancel(cx));
        });
    });
    window.run_until_parked();
    window.update(|_, cx| {
        let section = harness.section.read(cx);
        assert!(
            !section.protected_delete_confirm.read(cx).is_visible(),
            "cancelling closes the confirmation"
        );
        assert_eq!(
            section.pending_delete_protected_row_id, None,
            "cancelling clears the pending delete"
        );
        assert_eq!(
            harness.app_state.read(cx).protected_hook_rows().len(),
            1,
            "cancelling keeps the unreadable row"
        );
    });

    // Re-opening and confirming removes the unreadable row.
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.request_delete_protected_row(row_id.clone(), cx);
        });
    });
    window.run_until_parked();
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section
                .protected_delete_confirm
                .update(cx, |modal, cx| modal.confirm(cx));
        });
    });
    window.run_until_parked();
    window.update(|_, cx| {
        let section = harness.section.read(cx);
        assert!(
            !section.protected_delete_confirm.read(cx).is_visible(),
            "confirming closes the confirmation"
        );
        assert!(
            harness.app_state.read(cx).protected_hook_rows().is_empty(),
            "confirming deletes the unreadable row"
        );
    });
}
