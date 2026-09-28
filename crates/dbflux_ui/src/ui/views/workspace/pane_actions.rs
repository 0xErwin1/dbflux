//! The pane-actions menu: the actions the active document offers besides its
//! key bindings (toolbar buttons and other pointer-only controls), listed by
//! `OpenPaneActions` and driven with the context-menu keys.
//!
//! Documents fill `PaneHandle::pane_actions`; the workspace only lists and
//! runs the entries, so it never knows which document it is serving.

use super::Workspace;
use crate::keymap::{Command, CommandDispatcher};
use dbflux_components::composites::{MenuItem, menu_row, render_menu_container};
use dbflux_ui_document::DocumentId;
use dbflux_ui_document::pane::{PaneAction, PaneActionRun};
use gpui::prelude::*;
use gpui::{AnyElement, Context, Window};

/// The open pane-actions menu: the entries the document offered when it
/// opened, and the row the keyboard points at.
pub(super) struct PaneActionsMenu {
    document_id: DocumentId,
    actions: Vec<PaneAction>,
    selected_index: usize,
}

impl PaneActionsMenu {
    /// A menu over `actions`, pointing at the first enabled one, or `None`
    /// when no entry can be chosen.
    fn new(document_id: DocumentId, actions: Vec<PaneAction>) -> Option<Self> {
        let selected_index = actions.iter().position(|action| action.enabled)?;

        Some(Self {
            document_id,
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

    /// Opens the pane-actions menu of the active document. Returns `false`
    /// when there is no active document or it offers no action that can be
    /// chosen now.
    pub(super) fn open_pane_actions(&mut self, cx: &mut Context<Self>) -> bool {
        let menu = {
            let tab_manager = self.tab_manager.read(cx);
            let Some(tab) = tab_manager.active_tab() else {
                return false;
            };

            PaneActionsMenu::new(tab.id(), tab.as_pane().pane_actions(cx))
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

    /// Closes the menu and runs its entry at `index`, provided the entry is
    /// enabled and the document that offered it is still the active one.
    fn run_pane_action_at(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        let Some(menu) = self.pane_actions_menu.take() else {
            return;
        };
        cx.notify();

        let Some(action) = menu.actions.get(index).filter(|action| action.enabled) else {
            return;
        };

        if self.tab_manager.read(cx).active_id() != Some(menu.document_id) {
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
    use super::PaneActionsMenu;
    use crate::keymap::{Command, CommandDispatcher as _, FocusTarget};
    use crate::ui::document::{CodeDocument, Tab};
    use crate::ui::views::workspace::Workspace;
    use dbflux_ui_base::AppStateEntity;
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
    /// that a query without results has no tab to switch or close.
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
                ]
                .map(|command| workspace.dispatch(command, window, cx))
            })
        });

        assert_eq!(handled, [false, false, false]);
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
            DocumentId::new(),
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
        assert!(PaneActionsMenu::new(DocumentId::new(), Vec::new()).is_none());
        assert!(PaneActionsMenu::new(DocumentId::new(), vec![action("off", false)]).is_none());
    }
}
