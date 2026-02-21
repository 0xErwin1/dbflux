use super::*;

impl SqlQueryDocument {
    pub(super) fn enter_editor_mode(&mut self, cx: &mut Context<Self>) {
        if self.focus_mode != SqlQueryFocus::Editor {
            self.focus_mode = SqlQueryFocus::Editor;
            cx.notify();
        }
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_handle.focus(window);

        if self.focus_mode == SqlQueryFocus::Editor {
            self.input_state
                .update(cx, |state, cx| state.focus(window, cx));
        }
    }

    /// Returns the active context for keyboard handling based on internal focus.
    pub fn active_context(&self, cx: &App) -> ContextId {
        if self.pending_dangerous_query.is_some() {
            return ContextId::ConfirmModal;
        }

        if self.history_modal.read(cx).is_visible() {
            return ContextId::HistoryModal;
        }

        // Check if the active result tab's grid has a modal, context menu, or inline edit open
        if self.focus_mode == SqlQueryFocus::Results
            && let Some(index) = self.active_result_index
            && let Some(tab) = self.result_tabs.get(index)
        {
            let grid_context = tab.grid.read(cx).active_context(cx);

            if grid_context != ContextId::Results {
                return grid_context;
            }
        }

        match self.focus_mode {
            SqlQueryFocus::Editor => ContextId::Editor,
            SqlQueryFocus::Results => ContextId::Results,
        }
    }
}
