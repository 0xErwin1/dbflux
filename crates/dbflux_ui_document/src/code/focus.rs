use super::*;
use dbflux_ui_base::keymap::{LANGUAGE_KEY, VIM_MODE_KEY};

impl CodeDocument {
    pub(super) fn enter_editor_mode(&mut self, cx: &mut Context<Self>) {
        self.clear_vim_count_and_notify(cx);
        if self.focus_mode != SqlQueryFocus::Editor {
            self.focus_mode = SqlQueryFocus::Editor;
            cx.notify();
        }
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.clear_vim_count_and_notify(cx);
        self.focus_handle.focus(window, cx);

        if self.focus_mode == SqlQueryFocus::Editor {
            self.editor
                .input_state
                .update(cx, |state, cx| state.focus(window, cx));
        }
    }

    /// Handles a pane move (`FocusLeft` / `FocusRight` / `FocusUp` /
    /// `FocusDown`) while one of the editor's own overlays has the keyboard,
    /// before the workspace moves focus.
    ///
    /// With focus in the find panel, the panel closes and focus returns to
    /// the editor first. Moving down ends there, because the panel sits above
    /// the editor text; the other directions go on to the workspace. With a
    /// completion or code-action menu open in the editor, moving down or up
    /// steps through the menu instead of leaving the editor.
    ///
    /// Returns true when the command was consumed.
    pub(super) fn handle_editor_overlay_pane_move(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if !matches!(
            command,
            Command::FocusLeft | Command::FocusRight | Command::FocusUp | Command::FocusDown
        ) {
            return false;
        }

        let input = self.editor.input_state.clone();
        let editor_focused = input.read(cx).focus_handle(cx).is_focused(window);

        if !editor_focused && input.read(cx).search_session().open {
            input.update(cx, |state, cx| {
                state.close_search(cx);
                state.focus(window, cx);
            });
            return command == Command::FocusDown;
        }

        if editor_focused
            && matches!(command, Command::FocusDown | Command::FocusUp)
            && self.editor_menu_open(cx)
        {
            let step: Box<dyn gpui::Action> = if command == Command::FocusDown {
                Box::new(gpui_component::input::MoveDown)
            } else {
                Box::new(gpui_component::input::MoveUp)
            };
            input.update(cx, |state, cx| state.route_overlay_action(step, window, cx));
            return true;
        }

        false
    }

    /// Key context entries the keymap sees while this editor owns the
    /// keyboard: the query language and, with Vim editing on, the Vim mode.
    /// The mode is left out while the find panel is open, because its fields
    /// take every key as typed text.
    pub fn key_context_entries(&self, cx: &App) -> Vec<(SharedString, SharedString)> {
        let mut entries: Vec<(SharedString, SharedString)> = vec![(
            LANGUAGE_KEY.into(),
            self.effective_language().context_id().into(),
        )];

        if let Some(mode) = self.vim_mode()
            && !self.editor.input_state.read(cx).search_session().open
        {
            entries.push((VIM_MODE_KEY.into(), mode.context_id().into()));
        }

        entries
    }

    /// The panels this editor hands to the workspace as islands: the active
    /// result grid's chart stats rail while results show, then the query
    /// history while it is open.
    pub(super) fn side_panels(
        &self,
        cx: &mut Context<Self>,
    ) -> Vec<crate::pane::DocumentSidePanel> {
        let mut panels = Vec::new();

        if self.layout != SqlQueryLayout::EditorOnly
            && let Some(grid) = self.active_result_grid()
        {
            panels.extend(grid.update(cx, |grid, cx| grid.side_panels(cx)));
        }

        if self.history.history_panel.read(cx).is_visible() {
            panels.push(crate::pane::DocumentSidePanel {
                id: "query-history".into(),
                width: dbflux_components::tokens::HistoryPanelMetrics::WIDTH,
                content: self.history.history_panel.clone().into_any_element(),
            });
        }

        panels
    }

    /// Returns the active context for keyboard handling based on internal focus.
    pub fn active_context(&self, cx: &App) -> ContextId {
        if self.pending.dangerous_query.is_some() || self.pending.script_confirm.is_some() {
            return ContextId::ConfirmModal;
        }

        if self.history.history_panel.read(cx).owns_keyboard() {
            return ContextId::HistoryModal;
        }

        // Check if the active result tab's grid has a modal, context menu, or inline edit open
        if self.focus_mode == SqlQueryFocus::Results
            && let Some(index) = self.result_tabs.active_result_index
            && let Some(tab) = self.result_tabs.result_tabs.get(index)
        {
            let grid_context = tab.grid.read(cx).active_context(cx);

            if grid_context != ContextId::Results {
                return grid_context;
            }
        }

        match self.focus_mode {
            SqlQueryFocus::Editor => ContextId::Editor,
            SqlQueryFocus::Results => ContextId::Results,
            SqlQueryFocus::ContextBar => ContextId::ContextBar,
        }
    }
}

#[cfg(test)]
mod history_keyboard_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use crate::code::CodeDocument;
    use crate::history_panel::{HistoryPanel, HistoryTab};
    use dbflux_components::theme;
    use dbflux_core::QueryLanguage;
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};

    /// The editor next to its query history, under a workspace-like root
    /// that reports the document's context and hands it every command.
    struct HistoryHost {
        document: Entity<CodeDocument>,
        history: Entity<HistoryPanel>,
    }

    impl gpui::Render for HistoryHost {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            use dbflux_ui_base::keymap::{
                RunCommand, WORKSPACE_KEY_CONTEXT, root_key_context, run_command,
            };
            use gpui::{InteractiveElement as _, ParentElement as _, Styled as _};

            let context = self.document.read(cx).active_context(cx);

            gpui::div()
                .size_full()
                .flex()
                .key_context(root_key_context(WORKSPACE_KEY_CONTEXT, context, &[]))
                .on_action(cx.listener(|this, action: &RunCommand, window, cx| {
                    if let Some(command) = run_command(action) {
                        this.document.update(cx, |document, cx| {
                            document.dispatch_command(command, window, cx)
                        });
                    }
                }))
                .child(self.document.clone())
                .child(self.history.clone())
        }
    }

    fn keys(window: &mut VisualTestContext, keystrokes: &str) {
        for keystroke in keystrokes.split(' ') {
            window.simulate_keystrokes(keystroke);
            window.update(|window, _| window.refresh());
            window.run_until_parked();
        }
    }

    /// With the query history holding the keyboard, Alt+L and Alt+H switch
    /// between its Recent and Saved lists, from the list and from its search
    /// field alike.
    #[gpui::test]
    fn alt_l_and_alt_h_switch_the_history_lists(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(theme::init);
        cx.update(dbflux_ui_base::keymap::init_keymap);
        cx.update(|cx| {
            let host = cx.new(|_| ToastHost::new());
            cx.set_global(ToastGlobal { host });
        });

        let app_state = cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    StorageRuntime::in_memory().expect("isolated storage runtime"),
                )
                .expect("test storage setup")
            })
        });

        let (host, window) = cx.add_window_view(|window, cx| {
            let document = cx.new(|cx| {
                CodeDocument::new_with_language(app_state, None, QueryLanguage::Sql, window, cx)
            });
            let history = document.read(cx).history.history_panel.clone();
            HistoryHost { document, history }
        });
        let history = window.update(|_, cx| host.read(cx).history.clone());

        window.update(|window, cx| {
            window.activate_window();
            history.update(cx, |panel, cx| panel.open(window, cx));
        });
        window.run_until_parked();
        window.update(|window, _| window.refresh());
        window.run_until_parked();

        keys(window, "alt-l");

        let tab =
            |window: &mut VisualTestContext| window.update(|_, cx| history.read(cx).active_tab());

        assert!(window.update(|_, cx| history.read(cx).owns_keyboard()));
        assert_eq!(tab(window), HistoryTab::Saved, "Alt+L shows Saved");

        keys(window, "alt-l");
        assert_eq!(tab(window), HistoryTab::Recent, "the tabs wrap");

        keys(window, "/");
        keys(window, "alt-h");
        if cfg!(target_os = "macos") {
            assert_eq!(tab(window), HistoryTab::Recent, "Option+H types there");
        } else {
            assert_eq!(tab(window), HistoryTab::Saved, "the search field too");
        }
    }
}
