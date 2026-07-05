use super::*;

impl Workspace {
    pub(super) fn dispatch_query(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<bool> {
        match cmd {
            Command::NewQueryTab => {
                self.new_query_tab(window, cx);
                Some(true)
            }
            Command::RunQuery => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::RunQuery, window, cx);
                });
                Some(true)
            }
            Command::RunQueryInNewTab => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::RunQueryInNewTab, window, cx);
                });
                Some(true)
            }
            Command::ExportResults => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::ExportResults, window, cx);
                });
                Some(true)
            }
            Command::ToggleEditor => {
                // Route to active document for layout toggle
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::ToggleEditor, window, cx);
                });
                Some(true)
            }
            Command::ToggleResults => {
                // Route to active document for layout toggle
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::ToggleResults, window, cx);
                });
                Some(true)
            }

            Command::CancelQuery => {
                // Route to active document
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::CancelQuery, window, cx);
                });
                Some(true)
            }

            Command::ToggleHistoryDropdown => {
                // Route to active document
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::ToggleHistoryDropdown, window, cx);
                });
                Some(true)
            }

            Command::OpenSavedQueries => {
                // Route to active document
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::OpenSavedQueries, window, cx);
                });
                Some(true)
            }

            Command::SaveQuery => {
                // Route to active document
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::SaveQuery, window, cx);
                });
                Some(true)
            }

            Command::SaveFileAs => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::SaveFileAs, window, cx);
                });
                Some(true)
            }

            Command::ResultsNextPage => {
                // Route to active document
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::ResultsNextPage, window, cx);
                });
                Some(true)
            }

            Command::ResultsPrevPage => {
                // Route to active document
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::ResultsPrevPage, window, cx);
                });
                Some(true)
            }

            Command::ResultsAddRow | Command::ResultsCopyRow | Command::ResultsCopyCell => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(cmd, window, cx);
                });
                Some(true)
            }

            // Row operations - handled via GPUI actions in DataTable
            Command::ResultsDeleteRow | Command::ResultsDuplicateRow | Command::ResultsSetNull => {
                log::debug!(
                    "Row operation {:?} handled via GPUI actions in Results context",
                    cmd
                );
                Some(false)
            }

            _ => None,
        }
    }
}
