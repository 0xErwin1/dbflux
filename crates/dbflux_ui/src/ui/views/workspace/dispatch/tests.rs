// No `use super::*` here: combining the parent glob with `#[gpui::test]`
// sends the gpui_macros expansion into unbounded recursion. Explicit imports
// sidestep it.
use crate::keymap::Command;
use crate::ui::views::workspace::Workspace;
use dbflux_ui_base::AppStateEntity;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::cell::RefCell;
use std::rc::Rc;

fn init(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(dbflux_ui_base::keymap::init_keymap);

    cx.update(|cx| {
        cx.new(|_| {
            let runtime =
                dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    })
}

fn open_workspace<'a>(
    app_state: &Entity<AppStateEntity>,
    cx: &'a mut TestAppContext,
) -> (Entity<Workspace>, &'a mut VisualTestContext) {
    let holder: Rc<RefCell<Option<Entity<Workspace>>>> = Rc::default();
    let (_, window) = cx.add_window_view({
        let app_state = app_state.clone();
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
/// command reaches one; this walks every variant through the path
/// `Workspace::dispatch` takes instead. Each command gets its own workspace,
/// closed afterwards: one a previous command left with a context menu or a
/// modal open would claim every command and hide an unrouted one.
#[gpui::test]
fn every_command_is_routed_by_a_dispatch_domain(cx: &mut TestAppContext) {
    let app_state = init(cx);

    let mut unrouted = Vec::new();
    for command in Command::all_variants() {
        let (workspace, window) = open_workspace(&app_state, cx);

        let routed = window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.route_command(command, window, cx)
            })
        });
        window.run_until_parked();
        window.update(|window, _| window.remove_window());
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
