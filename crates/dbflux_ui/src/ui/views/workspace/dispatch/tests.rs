// No `use super::*` here: combining the parent glob with `#[gpui::test]`
// sends the gpui_macros expansion into unbounded recursion. Explicit imports
// sidestep it.
use crate::keymap::Command;
use crate::ui::views::workspace::Workspace;
use dbflux_ui_base::AppStateEntity;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::cell::RefCell;
use std::rc::Rc;

/// Commands `Workspace::dispatch` answers itself before any dispatch domain.
const ANSWERED_BEFORE_DOMAINS: &[Command] = &[Command::Quit, Command::Cancel];

fn open_workspace(cx: &mut TestAppContext) -> (Entity<Workspace>, &mut VisualTestContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(dbflux_ui_base::keymap::init_keymap);

    let app_state: Entity<AppStateEntity> = cx.update(|cx| {
        cx.new(|_| {
            let runtime =
                dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });

    let holder: Rc<RefCell<Option<Entity<Workspace>>>> = Rc::default();
    let (_, window) = cx.add_window_view({
        let holder = holder.clone();
        move |window, cx| {
            let workspace = cx.new(|cx| Workspace::new(app_state, window, cx));
            holder.replace(Some(workspace.clone()));
            gpui_component::Root::new(workspace, window, cx)
        }
    });
    window.run_until_parked();

    let workspace = holder.borrow().clone().expect("workspace created");
    (workspace, window)
}

/// Every domain ends in `_ => None`, so the compiler does not check that a
/// command reaches one; this walks every variant through the notifications
/// center and the domains instead, in the order `Workspace::dispatch` asks them.
#[gpui::test]
fn every_command_is_routed_by_a_dispatch_domain(cx: &mut TestAppContext) {
    let (workspace, window) = open_workspace(cx);

    let mut unrouted = Vec::new();
    for command in Command::all_variants() {
        if ANSWERED_BEFORE_DOMAINS.contains(&command) {
            continue;
        }

        let routed = window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace
                    .dispatch_notifications(command, window, cx)
                    .or_else(|| workspace.dispatch_to_domain(command, window, cx))
            })
        });
        window.run_until_parked();

        if routed.is_none() {
            unrouted.push(command);
        }
    }

    assert!(
        unrouted.is_empty(),
        "commands no dispatch domain routes: {unrouted:?}"
    );
}
