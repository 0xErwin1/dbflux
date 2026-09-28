//! Keyboard coverage of the workspace shell: the activity rail, the title
//! bar, the status bar, the tab bar, the sidebar header, the empty state, the
//! tasks panel, the notifications popover, the toasts and the pane actions
//! menu (see `dbflux_ui_base::keyboard_coverage`).
//!
//! Reaching each part from the keyboard is proven by the tests beside it:
//! `pane_actions::tests::the_tasks_panel_keys_select_and_act_on_a_task`
//! (Ctrl+Shift+4, then the tasks keys), `notifications::tests::
//! the_popover_rows_are_driven_by_the_keyboard` (Ctrl+Shift+B),
//! `pane_actions::tests::m_in_the_editor_chrome_lists_the_toolbar_and_runs_the_chosen_entry`
//! (the pane actions menu) and the toast actions test in `pane_actions`
//! (Ctrl+Shift+Y).

use crate::keymap::{Command, ContextId};
use crate::ui::views::workspace::Workspace;
use dbflux_core::TaskKind;
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture, KeyboardPath, SurfaceRegistry};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

const SHELL: SurfaceRegistry = SurfaceRegistry {
    name: "workspace shell (rail, title bar, status bar, tab bar, empty state)",
    contexts: &[ContextId::Global],
    entries: &[
        (
            "title-bar-drag-area*",
            KeyboardPath::MouseOnly("window chrome: drags the window"),
        ),
        (
            "title-bar-command-search",
            KeyboardPath::Command(Command::ToggleCommandPalette),
        ),
        (
            "notifications-bell",
            KeyboardPath::Command(Command::ToggleNotifications),
        ),
        (
            "rail-connections",
            KeyboardPath::Command(Command::ShowConnectionsView),
        ),
        (
            "rail-scripts",
            KeyboardPath::Command(Command::ShowScriptsView),
        ),
        (
            "rail-dashboards",
            KeyboardPath::Command(Command::ShowDashboardsView),
        ),
        (
            "rail-approvals",
            KeyboardPath::Command(Command::OpenMcpApprovals),
        ),
        (
            "rail-audit",
            KeyboardPath::Command(Command::OpenAuditViewer),
        ),
        (
            "rail-settings",
            KeyboardPath::Command(Command::OpenSettings),
        ),
        ("tasks-toggle", KeyboardPath::Command(Command::ToggleTasks)),
        ("tasks-failed", KeyboardPath::Command(Command::ToggleTasks)),
        (
            "error-badge",
            KeyboardPath::Command(Command::OpenLastErrorInAudit),
        ),
        (
            "status-bar-approvals",
            KeyboardPath::Command(Command::OpenMcpApprovals),
        ),
        ("new-tab-btn", KeyboardPath::Command(Command::NewQueryTab)),
        (
            "tab-close-*",
            KeyboardPath::Command(Command::CloseCurrentTab),
        ),
        ("tab-*", KeyboardPath::Command(Command::NextTab)),
        (
            "empty-start-new_query_tab",
            KeyboardPath::Command(Command::NewQueryTab),
        ),
        (
            "empty-start-toggle_command_palette",
            KeyboardPath::Command(Command::ToggleCommandPalette),
        ),
        (
            "empty-start-open_script_file",
            KeyboardPath::Command(Command::OpenScriptFile),
        ),
        (
            "empty-start-open_connection_manager",
            KeyboardPath::Command(Command::OpenConnectionManager),
        ),
        (
            "empty-recent-*",
            KeyboardPath::Command(Command::OpenScriptFile),
        ),
    ],
};

const SIDEBAR_HEADER: SurfaceRegistry = SurfaceRegistry {
    name: "sidebar header",
    contexts: &[ContextId::Sidebar],
    entries: &[
        (
            "sidebar-filter",
            KeyboardPath::Command(Command::FocusSearch),
        ),
        // The add menu offers new folders, connections and scripts; the item
        // menu (`m`) lists the same entries for the selected folder.
        ("sidebar-add", KeyboardPath::Command(Command::OpenItemMenu)),
    ],
};

const TASKS_PANEL: SurfaceRegistry = SurfaceRegistry {
    name: "tasks panel",
    contexts: &[ContextId::BackgroundTasks],
    entries: &[
        (
            "tasks-panel-header",
            KeyboardPath::Command(Command::TogglePanel),
        ),
        (
            "tasks-clear-finished",
            KeyboardPath::Command(Command::ClearFinishedTasks),
        ),
        ("task-row-*", KeyboardPath::Command(Command::ExpandCollapse)),
        (
            "cancel-task-button-*",
            KeyboardPath::Command(Command::CancelTask),
        ),
        (
            "dismiss-task-button-*",
            KeyboardPath::Command(Command::Delete),
        ),
    ],
};

const NOTIFICATIONS: SurfaceRegistry = SurfaceRegistry {
    name: "notifications popover",
    contexts: &[ContextId::Notifications],
    entries: &[
        (
            "notifications-close",
            KeyboardPath::Command(Command::ToggleNotifications),
        ),
        (
            "notifications-mark-all-read",
            KeyboardPath::Command(Command::MarkAllNotificationsRead),
        ),
        (
            "notifications-clear-read",
            KeyboardPath::Command(Command::ClearReadNotifications),
        ),
        (
            "notifications-filter-*",
            KeyboardPath::Command(Command::NextPanelTab),
        ),
        (
            "notification-install-*",
            KeyboardPath::Command(Command::InstallUpdate),
        ),
        (
            "notification-later-*",
            KeyboardPath::Command(Command::Delete),
        ),
        ("notification-*", KeyboardPath::Command(Command::Execute)),
    ],
};

const TOASTS: SurfaceRegistry = SurfaceRegistry {
    name: "toasts",
    contexts: &[ContextId::Global],
    entries: &[
        ("toast-action-*", KeyboardPath::Menu("toast-action-*")),
        ("toast-toggle-*", KeyboardPath::Menu("toast-details")),
        ("toast-close-*", KeyboardPath::Menu("toast-dismiss")),
    ],
};

const PANE_ACTIONS_MENU: SurfaceRegistry = SurfaceRegistry {
    name: "pane actions menu",
    contexts: &[ContextId::ContextMenu],
    entries: &[("pane-action-*", KeyboardPath::Command(Command::MenuSelect))],
};

struct Harness<'a> {
    workspace: Entity<Workspace>,
    capture: Arc<FrameCapture>,
    window: &'a mut VisualTestContext,
}

fn open_workspace(cx: &mut TestAppContext) -> Harness<'_> {
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
    window.update(|window, _| window.activate_window());

    let workspace = holder.borrow().clone().expect("workspace created");
    let capture = FrameCapture::observe(window);

    Harness {
        workspace,
        capture,
        window,
    }
}

impl Harness<'_> {
    fn keys(&mut self, keystrokes: &str) {
        for keystroke in keystrokes.split(' ') {
            self.window.simulate_keystrokes(keystroke);
            self.window.update(|window, _| window.refresh());
            self.window.run_until_parked();
        }
    }

    fn open_query_tab(&mut self) {
        let workspace = self.workspace.clone();
        self.window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| workspace.new_query_tab(window, cx));
        });
        self.window.run_until_parked();
    }

    /// Starts a running, a finished and a failed task, and reports an error,
    /// which also shows its toast.
    fn start_tasks_and_report_an_error(&mut self) {
        let app_state = self
            .window
            .update(|_, cx| self.workspace.read(cx).app_state.clone());

        app_state.update(self.window, |state, cx| {
            let (_running, _token) = state.start_task(TaskKind::Query, "running query");
            let (export, _token) = state.start_task(TaskKind::Export, "export");
            state.complete_task_with_details(export, "wrote 10 rows\nwrote 20 rows");
            let (import, _token) = state.start_task(TaskKind::Import, "import");
            state.fail_task(import, "disk full");
            cx.emit(AppStateChanged);
        });
        self.window.run_until_parked();

        self.window.update(|_, cx| {
            report_error(
                UserFacingError::new(ErrorKind::Storage, "Export failed"),
                cx,
            )
        });
        self.window.run_until_parked();
    }

    fn pane_actions_menu_entries(&mut self) -> Vec<String> {
        let workspace = self.workspace.clone();
        self.window.update(|_, cx| {
            workspace
                .read(cx)
                .pane_actions_menu
                .as_ref()
                .map(|menu| menu.entry_ids())
                .unwrap_or_default()
        })
    }

    fn assert_covered(&mut self, menu_entries: &[String]) {
        let frame = self.capture.frame(self.window);

        shell_coverage()
            .with_menu_entries(menu_entries.iter().cloned())
            .assert_covered(&frame);
    }
}

fn shell_coverage() -> Coverage {
    let palette = Workspace::palette_commands_for_test()
        .into_iter()
        .filter_map(|command| Command::from_palette_id(command.id));

    Coverage::new(SHELL)
        .with_surface(SIDEBAR_HEADER)
        .with_surface(TASKS_PANEL)
        .with_surface(NOTIFICATIONS)
        .with_surface(TOASTS)
        .with_surface(PANE_ACTIONS_MENU)
        .delegate(
            "sql-doc-*",
            "the code editor chrome coverage in dbflux_ui_document",
        )
        .with_palette(palette)
}

#[gpui::test]
fn the_empty_workspace_shell_is_covered(cx: &mut TestAppContext) {
    let mut harness = open_workspace(cx);

    harness.assert_covered(&[]);
}

#[gpui::test]
fn the_tab_bar_is_covered(cx: &mut TestAppContext) {
    let mut harness = open_workspace(cx);
    harness.open_query_tab();
    harness.open_query_tab();

    harness.assert_covered(&[]);
}

#[gpui::test]
fn the_tasks_panel_status_bar_and_toasts_are_covered(cx: &mut TestAppContext) {
    let mut harness = open_workspace(cx);
    harness.open_query_tab();
    harness.start_tasks_and_report_an_error();
    harness.keys("ctrl-shift-4 space");

    harness.keys("ctrl-shift-y");
    let toast_menu = harness.pane_actions_menu_entries();
    assert!(
        toast_menu.iter().any(|entry| entry == "toast-dismiss"),
        "{toast_menu:?}"
    );

    harness.assert_covered(&toast_menu);
}

#[gpui::test]
fn the_notifications_popover_is_covered(cx: &mut TestAppContext) {
    let mut harness = open_workspace(cx);
    harness.start_tasks_and_report_an_error();

    // The error's toast stays on screen under the popover.
    harness.keys("ctrl-shift-y");
    let toast_menu = harness.pane_actions_menu_entries();
    harness.keys("escape");

    harness.keys("ctrl-shift-b");

    let workspace = harness.workspace.clone();
    assert!(
        harness
            .window
            .update(|_, cx| workspace.read(cx).notifications.is_open())
    );

    harness.assert_covered(&toast_menu);
}
