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

fn route_create_folder_on_sidebar_tab(
    workspace: &Entity<Workspace>,
    tab: dbflux_ui_sidebar::SidebarTab,
    window: &mut VisualTestContext,
) -> Option<bool> {
    let routed = window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.focus_target = crate::ui::views::workspace::FocusTarget::Sidebar;
            workspace
                .sidebar
                .update(cx, |sidebar, cx| sidebar.set_active_tab(tab, cx));
            workspace.route_command(Command::CreateFolder, window, cx)
        })
    });
    window.run_until_parked();
    routed
}

/// Dashboards have no folders, so Create Folder must leave the key for
/// another handler instead of consuming it and doing nothing.
#[gpui::test]
fn create_folder_on_dashboards_tab_is_not_consumed(cx: &mut TestAppContext) {
    let app_state = init(cx);
    let (workspace, window) = open_workspace(&app_state, cx);

    let routed = route_create_folder_on_sidebar_tab(
        &workspace,
        dbflux_ui_sidebar::SidebarTab::Dashboards,
        window,
    );

    assert_eq!(routed, Some(false));
}

#[gpui::test]
fn create_folder_on_connections_tab_creates_a_folder(cx: &mut TestAppContext) {
    let app_state = init(cx);
    let (workspace, window) = open_workspace(&app_state, cx);
    let nodes_before = window.update(|_, cx| app_state.read(cx).connection_tree().nodes.len());

    let routed = route_create_folder_on_sidebar_tab(
        &workspace,
        dbflux_ui_sidebar::SidebarTab::Connections,
        window,
    );

    let nodes_after = window.update(|_, cx| app_state.read(cx).connection_tree().nodes.len());
    assert_eq!(routed, Some(true));
    assert_eq!(nodes_after, nodes_before + 1);
}

#[gpui::test]
fn toggle_full_screen_enters_and_leaves_full_screen(cx: &mut TestAppContext) {
    let app_state = init(cx);
    let (workspace, window) = open_workspace(&app_state, cx);

    let toggle = |window: &mut VisualTestContext| {
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.route_command(Command::ToggleFullScreen, window, cx)
            })
        })
    };

    assert!(!window.update(|window, _| window.is_fullscreen()));

    assert_eq!(toggle(window), Some(true));
    assert!(window.update(|window, _| window.is_fullscreen()));

    assert_eq!(toggle(window), Some(true));
    assert!(!window.update(|window, _| window.is_fullscreen()));
}

/// The interface size commands move the saved font sizes from the
/// workspace, whatever has focus.
#[gpui::test]
fn interface_size_commands_change_the_saved_font_sizes(cx: &mut TestAppContext) {
    let app_state = init(cx);
    let (workspace, window) = open_workspace(&app_state, cx);

    let route = |window: &mut VisualTestContext, command: Command| {
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.route_command(command, window, cx)
            })
        })
    };
    let ui_size = |window: &mut VisualTestContext| {
        window.update(|_, cx| app_state.read(cx).general_settings().ui_font_size)
    };

    assert_eq!(route(window, Command::IncreaseInterfaceSize), Some(true));
    assert_eq!(ui_size(window), 14.0);

    assert_eq!(route(window, Command::DecreaseInterfaceSize), Some(true));
    assert_eq!(route(window, Command::DecreaseInterfaceSize), Some(true));
    assert_eq!(ui_size(window), 12.0);

    assert_eq!(route(window, Command::ResetInterfaceSize), Some(true));
    assert_eq!(ui_size(window), 13.0);
    window.update(|_, cx| {
        let settings = app_state.read(cx).general_settings();
        assert_eq!(settings.editor_font_size, 13.0);
        assert_eq!(settings.grid_font_size, 12.5);
    });
}
