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
