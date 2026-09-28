//! The pane-actions menu: the actions the active document offers besides its
//! key bindings (toolbar buttons and other pointer-only controls), listed by
//! `OpenPaneActions` and driven with the context-menu keys.
//!
//! Documents fill `PaneHandle::pane_actions`; the workspace only lists and
//! runs the entries, so it never knows which document it is serving. The
//! background tasks panel, which the workspace draws itself, offers its
//! actions through the same menu.

use super::Workspace;
use crate::keymap::{Command, CommandDispatcher, ContextId, FocusTarget};
use dbflux_components::composites::{MenuItem, menu_row, render_menu_container};
use dbflux_components::icons::AppIcon;
use dbflux_ui_document::DocumentId;
use dbflux_ui_document::pane::{PaneAction, PaneActionRun};
use gpui::prelude::*;
use gpui::{AnyElement, App, Context, Window};

/// The pane that offered the entries of an open menu.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PaneActionsOwner {
    Document(DocumentId),
    BackgroundTasks,
}

/// The open pane-actions menu: the entries the pane offered when it
/// opened, and the row the keyboard points at.
pub(super) struct PaneActionsMenu {
    owner: PaneActionsOwner,
    actions: Vec<PaneAction>,
    selected_index: usize,
}

impl PaneActionsMenu {
    /// A menu over `actions`, pointing at the first enabled one, or `None`
    /// when no entry can be chosen.
    fn new(owner: PaneActionsOwner, actions: Vec<PaneAction>) -> Option<Self> {
        let selected_index = actions.iter().position(|action| action.enabled)?;

        Some(Self {
            owner,
            actions,
            selected_index,
        })
    }

    fn select_next(&mut self) {
        if let Some(index) =
            (self.selected_index + 1..self.actions.len()).find(|index| self.actions[*index].enabled)
        {
            self.selected_index = index;
        }
    }

    fn select_prev(&mut self) {
        if let Some(index) = (0..self.selected_index)
            .rev()
            .find(|index| self.actions[*index].enabled)
        {
            self.selected_index = index;
        }
    }

    fn menu_item(action: &PaneAction) -> MenuItem {
        let mut item = MenuItem::new(action.label.clone());

        if let Some(icon) = action.icon {
            item = item.icon(icon);
        }
        if let Some(shortcut) = action.shortcut.clone() {
            item = item.shortcut(shortcut);
        }
        if !action.enabled {
            item = item.disabled();
        }

        item
    }
}

impl Workspace {
    /// Whether the pane-actions menu is open.
    pub(super) fn has_pane_actions_menu(&self) -> bool {
        self.pane_actions_menu.is_some()
    }

    /// Whether the open menu belongs to the background tasks panel, which
    /// draws it over itself instead of over the document area.
    pub(super) fn pane_actions_menu_is_for_tasks(&self) -> bool {
        self.pane_actions_menu
            .as_ref()
            .is_some_and(|menu| menu.owner == PaneActionsOwner::BackgroundTasks)
    }

    /// Opens the pane-actions menu of the focused pane: the background tasks
    /// panel, or else the active document. Returns `false` when that pane
    /// offers no action that can be chosen now.
    pub(super) fn open_pane_actions(&mut self, cx: &mut Context<Self>) -> bool {
        let menu = if self.focus_target == FocusTarget::BackgroundTasks {
            PaneActionsMenu::new(
                PaneActionsOwner::BackgroundTasks,
                self.tasks_pane_actions(cx),
            )
        } else {
            let tab_manager = self.tab_manager.read(cx);
            let Some(tab) = tab_manager.active_tab() else {
                return false;
            };

            PaneActionsMenu::new(
                PaneActionsOwner::Document(tab.id()),
                tab.as_pane().pane_actions(cx),
            )
        };

        let Some(menu) = menu else {
            return false;
        };

        self.pane_actions_menu = Some(menu);
        cx.notify();
        true
    }

    pub(super) fn close_pane_actions(&mut self, cx: &mut Context<Self>) {
        if self.pane_actions_menu.take().is_some() {
            cx.notify();
        }
    }

    /// Answers the context-menu commands while the pane-actions menu is open.
    pub(super) fn dispatch_pane_actions_menu(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match command {
            Command::MenuDown => {
                if let Some(menu) = self.pane_actions_menu.as_mut() {
                    menu.select_next();
                    cx.notify();
                }
            }
            Command::MenuUp => {
                if let Some(menu) = self.pane_actions_menu.as_mut() {
                    menu.select_prev();
                    cx.notify();
                }
            }
            Command::MenuSelect => {
                if let Some(index) = self.pane_actions_menu.as_ref().map(|m| m.selected_index) {
                    self.run_pane_action_at(index, window, cx);
                }
            }
            Command::MenuBack | Command::OpenPaneActions | Command::OpenContextMenu => {
                self.close_pane_actions(cx);
            }
            _ => {}
        }
    }

    /// The actions of the background tasks panel: the selected task's
    /// output, cancel and dismiss buttons, then Clear finished and hiding
    /// the panel.
    fn tasks_pane_actions(&self, cx: &App) -> Vec<PaneAction> {
        let panel = self.tasks_panel.read(cx);
        let selected = panel.selected_actions(cx);
        let context = ContextId::BackgroundTasks;

        let output_label = if selected.is_some_and(|task| task.output_shown) {
            dbflux_i18n::t!("tasks_panel.hide_output")
        } else {
            dbflux_i18n::t!("tasks_panel.show_output")
        };

        vec![
            PaneAction::command(
                "task-output",
                output_label,
                Command::ExpandCollapse,
                context,
            )
            .icon(AppIcon::ChevronDown)
            .enabled(selected.is_some_and(|task| task.has_output)),
            PaneAction::command(
                "task-cancel",
                dbflux_i18n::t!("tasks_panel.cancel"),
                Command::CancelTask,
                context,
            )
            .icon(AppIcon::X)
            .enabled(selected.is_some_and(|task| task.cancellable)),
            PaneAction::command(
                "task-dismiss",
                dbflux_i18n::t!("tasks_panel.dismiss"),
                Command::Delete,
                context,
            )
            .icon(AppIcon::X)
            .enabled(selected.is_some_and(|task| task.finished)),
            PaneAction::command(
                "tasks-clear-finished",
                dbflux_i18n::t!("tasks_panel.clear_finished"),
                Command::ClearFinishedTasks,
                context,
            )
            .icon(AppIcon::CircleX)
            .enabled(panel.has_finished_tasks(cx)),
            PaneAction::command(
                "tasks-collapse",
                dbflux_i18n::t!("tasks_panel.collapse"),
                Command::TogglePanel,
                context,
            )
            .icon(AppIcon::ChevronDown),
        ]
    }

    /// Whether the pane that offered `owner`'s entries still owns the
    /// keyboard, so running one acts on what the menu listed.
    fn pane_actions_owner_is_current(&self, owner: PaneActionsOwner, cx: &App) -> bool {
        match owner {
            PaneActionsOwner::Document(document_id) => {
                self.tab_manager.read(cx).active_id() == Some(document_id)
            }
            PaneActionsOwner::BackgroundTasks => self.focus_target == FocusTarget::BackgroundTasks,
        }
    }

    /// Closes the menu and runs its entry at `index`, provided the entry is
    /// enabled and the pane that offered it still owns the keyboard.
    fn run_pane_action_at(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        let Some(menu) = self.pane_actions_menu.take() else {
            return;
        };
        cx.notify();

        let Some(action) = menu.actions.get(index).filter(|action| action.enabled) else {
            return;
        };

        if !self.pane_actions_owner_is_current(menu.owner, cx) {
            return;
        }

        match action.run.clone() {
            PaneActionRun::Command(command) => {
                self.dispatch(command, window, cx);
            }
            PaneActionRun::Callback(callback) => callback(window, cx),
        }
    }

    fn hover_pane_action(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(menu) = self.pane_actions_menu.as_mut()
            && menu.selected_index != index
            && menu.actions.get(index).is_some_and(|action| action.enabled)
        {
            menu.selected_index = index;
            cx.notify();
        }
    }

    /// The open menu, drawn at the top left of the document area over the
    /// pane's own toolbar.
    pub(super) fn render_pane_actions_menu(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let menu = self.pane_actions_menu.as_ref()?;

        let rows: Vec<AnyElement> = menu
            .actions
            .iter()
            .enumerate()
            .map(|(index, action)| {
                let item = PaneActionsMenu::menu_item(action);
                let row = menu_row(
                    format!("pane-action-{}", action.id),
                    &item,
                    index == menu.selected_index,
                    cx,
                );

                if !action.enabled {
                    return row.into_any_element();
                }

                row.on_mouse_move(cx.listener(move |this, _, _, cx| {
                    this.hover_pane_action(index, cx);
                }))
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.run_pane_action_at(index, window, cx);
                }))
                .into_any_element()
            })
            .collect();

        Some(
            render_menu_container(rows, cx)
                .id("pane-actions-menu")
                .into_any_element(),
        )
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{PaneActionsMenu, PaneActionsOwner};
    use crate::keymap::{Command, CommandDispatcher as _, ContextId, FocusTarget};
    use crate::ui::document::{CodeDocument, Tab};
    use crate::ui::views::tasks_panel::TasksPanel;
    use crate::ui::views::workspace::Workspace;
    use dbflux_core::{TaskId, TaskKind, TaskStatus};
    use dbflux_ui_base::{AppStateChanged, AppStateEntity};
    use dbflux_ui_document::DocumentId;
    use dbflux_ui_document::pane::{PaneAction, PaneActionRun};
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::cell::RefCell;
    use std::rc::Rc;

    fn open_workspace(cx: &mut TestAppContext) -> (Entity<Workspace>, &mut VisualTestContext) {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);
        cx.update(dbflux_ui_base::keymap::init_keymap);

        let app_state: Entity<AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
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
        let workspace = holder.borrow().clone().expect("workspace created");
        window.run_until_parked();

        (workspace, window)
    }

    fn keys(window: &mut VisualTestContext, keystrokes: &str) {
        for keystroke in keystrokes.split(' ') {
            window.simulate_keystrokes(keystroke);
            window.update(|window, _| window.refresh());
            window.run_until_parked();
        }
    }

    fn menu_ids(workspace: &Entity<Workspace>, window: &mut VisualTestContext) -> Vec<String> {
        window.update(|_, cx| {
            workspace
                .read(cx)
                .pane_actions_menu
                .as_ref()
                .map(|menu| {
                    menu.actions
                        .iter()
                        .map(|action| action.id.to_string())
                        .collect()
                })
                .unwrap_or_default()
        })
    }

    fn side_panel_ids(
        workspace: &Entity<Workspace>,
        window: &mut VisualTestContext,
    ) -> Vec<String> {
        window.update(|window, cx| {
            let tab_manager = workspace.read(cx).tab_manager.clone();
            tab_manager.update(cx, |manager, cx| {
                manager
                    .active_side_panels(window, cx)
                    .into_iter()
                    .map(|panel| panel.id.to_string())
                    .collect()
            })
        })
    }

    /// `m` in the code editor's context bar lists the whole SQL toolbar, and
    /// choosing an entry runs the command its toolbar button runs.
    #[gpui::test]
    fn m_in_the_editor_chrome_lists_the_toolbar_and_runs_the_chosen_entry(cx: &mut TestAppContext) {
        let (workspace, window) = open_workspace(cx);

        window.update(|window, cx| {
            window.activate_window();
            workspace.update(cx, |workspace, cx| workspace.new_query_tab(window, cx));
        });
        window.run_until_parked();

        keys(window, "ctrl-k m");

        assert_eq!(
            menu_ids(&workspace, window),
            [
                "run",
                "run-in-new-tab",
                "save",
                "format",
                "history",
                "explain",
                "chart",
                "refresh",
                "auto-refresh",
            ]
        );
        assert!(
            !side_panel_ids(&workspace, window).contains(&"query-history".to_string()),
            "the history starts closed"
        );

        // The disabled formatter entry is skipped on the way to History.
        keys(window, "j j j enter");

        assert!(
            menu_ids(&workspace, window).is_empty(),
            "choosing closes the menu"
        );
        assert!(
            side_panel_ids(&workspace, window).contains(&"query-history".to_string()),
            "the History entry opens the query history, as its button does"
        );

        keys(window, "escape");
        keys(window, "ctrl-k m");
        assert!(!menu_ids(&workspace, window).is_empty());

        keys(window, "escape");
        assert!(
            menu_ids(&workspace, window).is_empty(),
            "Escape closes the menu"
        );
    }

    /// A script editor has no connection controls, yet Ctrl+K still lands on
    /// its context bar, where `m` and Shift+F10 open the pane actions.
    #[gpui::test]
    fn ctrl_k_then_m_opens_the_pane_actions_in_a_script_editor(cx: &mut TestAppContext) {
        let (workspace, window) = open_workspace(cx);

        window.update(|window, cx| {
            window.activate_window();
            workspace.update(cx, |workspace, cx| {
                let app_state = workspace.app_state.clone();
                let document = cx.new(|cx| {
                    CodeDocument::new_with_language(
                        app_state,
                        None,
                        dbflux_core::QueryLanguage::Lua,
                        window,
                        cx,
                    )
                });
                let pane = CodeDocument::into_pane(document, cx);
                workspace.tab_manager.update(cx, |manager, cx| {
                    manager.open(Tab::Pane(Box::new(pane)), cx)
                });
                workspace.set_focus(FocusTarget::Document, window, cx);
            });
        });
        window.run_until_parked();

        keys(window, "ctrl-k m");
        assert_eq!(
            menu_ids(&workspace, window).first().map(String::as_str),
            Some("run")
        );

        keys(window, "escape");
        assert!(menu_ids(&workspace, window).is_empty());

        keys(window, "shift-f10");
        assert_eq!(
            menu_ids(&workspace, window).first().map(String::as_str),
            Some("run"),
            "Shift+F10 opens it from the context bar too"
        );

        keys(window, "escape");
        assert!(menu_ids(&workspace, window).is_empty());

        keys(window, "enter");
        assert_eq!(
            menu_ids(&workspace, window).first().map(String::as_str),
            Some("run"),
            "Enter presses the focused pane-actions button"
        );
    }

    /// The result tab entries run their commands through the workspace, like
    /// their keys: the commands reach the active document, which reports
    /// that a query without results has no tab to switch or close. The
    /// panel tab commands reach it the same way; with the history closed
    /// there is no panel to switch.
    #[gpui::test]
    fn result_tab_commands_reach_the_active_document(cx: &mut TestAppContext) {
        let (workspace, window) = open_workspace(cx);

        let handled = window.update(|window, cx| {
            window.activate_window();
            workspace.update(cx, |workspace, cx| {
                workspace.new_query_tab(window, cx);

                [
                    Command::NextResultTab,
                    Command::PrevResultTab,
                    Command::CloseResultTab,
                    Command::NextPanelTab,
                    Command::PrevPanelTab,
                ]
                .map(|command| workspace.dispatch(command, window, cx))
            })
        });

        assert_eq!(handled, [false, false, false, false, false]);
    }

    /// A running query, an export that finished with output and a failed
    /// import, in the order the tasks panel lists them.
    fn start_three_tasks(
        workspace: &Entity<Workspace>,
        window: &mut VisualTestContext,
    ) -> [TaskId; 3] {
        let app_state = window.update(|_, cx| workspace.read(cx).app_state.clone());

        let ids = app_state.update(window, |state, cx| {
            let (running, _token) = state.start_task(TaskKind::Query, "running query");
            let (export, _token) = state.start_task(TaskKind::Export, "export");
            state.complete_task_with_details(export, "wrote 10 rows\nwrote 20 rows");
            let (import, _token) = state.start_task(TaskKind::Import, "import");
            state.fail_task(import, "disk full");
            cx.emit(AppStateChanged);
            [running, export, import]
        });
        window.run_until_parked();

        ids
    }

    fn tasks_panel(
        workspace: &Entity<Workspace>,
        window: &mut VisualTestContext,
    ) -> Entity<TasksPanel> {
        window.update(|_, cx| workspace.read(cx).tasks_panel.clone())
    }

    fn selected_task(workspace: &Entity<Workspace>, window: &mut VisualTestContext) -> TaskId {
        let panel = tasks_panel(workspace, window);
        window
            .update(|_, cx| panel.read(cx).selected_task(cx).map(|task| task.id))
            .expect("a task is selected")
    }

    fn task_status(
        workspace: &Entity<Workspace>,
        window: &mut VisualTestContext,
        task_id: TaskId,
    ) -> Option<TaskStatus> {
        window.update(|_, cx| {
            workspace
                .read(cx)
                .app_state
                .read(cx)
                .tasks()
                .get(task_id)
                .map(|task| task.status)
        })
    }

    /// With the tasks panel focused, J, K, G and Shift+G move over its rows,
    /// Space shows a task's output, C cancels, X dismisses a finished task
    /// and Shift+X clears the finished ones.
    #[gpui::test]
    fn the_tasks_panel_keys_select_and_act_on_a_task(cx: &mut TestAppContext) {
        let (workspace, window) = open_workspace(cx);
        window.update(|window, _| window.activate_window());
        let [running, export, import] = start_three_tasks(&workspace, window);

        keys(window, "ctrl-shift-4");
        assert_eq!(selected_task(&workspace, window), running);

        keys(window, "shift-g");
        let last = selected_task(&workspace, window);
        assert_ne!(last, running);
        keys(window, "g");
        assert_eq!(selected_task(&workspace, window), running);

        keys(window, "c");
        assert_eq!(
            task_status(&workspace, window, running),
            Some(TaskStatus::Cancelled)
        );

        // Walk down to the export, which has output. The cancelled query
        // moved to the finished tasks, so start again from the top.
        let panel = tasks_panel(&workspace, window);
        keys(window, "g");
        for _ in 0..3 {
            if selected_task(&workspace, window) == export {
                break;
            }
            keys(window, "j");
        }
        assert_eq!(selected_task(&workspace, window), export);

        keys(window, "space");
        let actions = window.update(|_, cx| panel.read(cx).selected_actions(cx));
        assert_eq!(actions.map(|actions| actions.output_shown), Some(true));

        keys(window, "x");
        assert_eq!(task_status(&workspace, window, export), None, "X dismisses");
        assert!(task_status(&workspace, window, import).is_some());

        keys(window, "shift-x");
        assert_eq!(task_status(&workspace, window, import), None);
        assert_eq!(task_status(&workspace, window, running), None);
    }

    /// Focusing the tasks panel from a code editor takes the keyboard from
    /// the editor, so the panel keys move its selection instead of typing.
    #[gpui::test]
    fn the_tasks_panel_keys_reach_it_from_a_code_editor(cx: &mut TestAppContext) {
        let (workspace, window) = open_workspace(cx);
        window.update(|window, cx| {
            window.activate_window();
            workspace.update(cx, |workspace, cx| workspace.new_query_tab(window, cx));
        });
        window.run_until_parked();
        let [running, _export, _import] = start_three_tasks(&workspace, window);

        keys(window, "ctrl-shift-4 j");

        assert_ne!(selected_task(&workspace, window), running);
        assert_eq!(
            window
                .update(|_, cx| workspace.update(cx, |workspace, cx| workspace.active_context(cx))),
            ContextId::BackgroundTasks
        );
    }

    /// `m` in the tasks panel lists what can be done with the selected task
    /// and the panel, and Enter runs the entry.
    #[gpui::test]
    fn m_in_the_tasks_panel_lists_the_task_actions(cx: &mut TestAppContext) {
        let (workspace, window) = open_workspace(cx);
        window.update(|window, _| window.activate_window());
        let [running, _export, _import] = start_three_tasks(&workspace, window);

        keys(window, "ctrl-shift-4 m");
        assert_eq!(
            menu_ids(&workspace, window),
            [
                "task-output",
                "task-cancel",
                "task-dismiss",
                "tasks-clear-finished",
                "tasks-collapse"
            ]
        );

        // The running query has no output, so the menu starts on Cancel.
        keys(window, "enter");
        assert!(menu_ids(&workspace, window).is_empty());
        assert_eq!(
            task_status(&workspace, window, running),
            Some(TaskStatus::Cancelled)
        );
    }

    fn action(id: &'static str, enabled: bool) -> PaneAction {
        PaneAction {
            id: id.into(),
            label: id.into(),
            icon: None,
            shortcut: None,
            enabled,
            run: PaneActionRun::Command(Command::RunQuery),
        }
    }

    /// The palette entry opens the same menu.
    #[gpui::test]
    fn the_palette_entry_opens_the_pane_actions(cx: &mut TestAppContext) {
        let (workspace, window) = open_workspace(cx);

        window.update(|window, cx| {
            window.activate_window();
            workspace.update(cx, |workspace, cx| {
                workspace.new_query_tab(window, cx);
                workspace.handle_command("open_pane_actions", window, cx);
            });
        });
        window.run_until_parked();

        assert_eq!(
            menu_ids(&workspace, window).first().map(String::as_str),
            Some("run")
        );
    }

    #[test]
    fn the_menu_starts_on_the_first_enabled_entry_and_skips_disabled_ones() {
        let mut menu = PaneActionsMenu::new(
            PaneActionsOwner::Document(DocumentId::new()),
            vec![
                action("off", false),
                action("first", true),
                action("also-off", false),
                action("last", true),
            ],
        )
        .expect("an enabled entry");

        assert_eq!(menu.selected_index, 1);

        menu.select_next();
        assert_eq!(menu.selected_index, 3);

        menu.select_next();
        assert_eq!(menu.selected_index, 3, "the last entry stays selected");

        menu.select_prev();
        assert_eq!(menu.selected_index, 1);

        menu.select_prev();
        assert_eq!(
            menu.selected_index, 1,
            "the disabled first entry is skipped"
        );
    }

    #[test]
    fn no_menu_opens_without_an_enabled_entry() {
        assert!(
            PaneActionsMenu::new(PaneActionsOwner::Document(DocumentId::new()), Vec::new())
                .is_none()
        );
        assert!(
            PaneActionsMenu::new(
                PaneActionsOwner::Document(DocumentId::new()),
                vec![action("off", false)]
            )
            .is_none()
        );
    }
}
