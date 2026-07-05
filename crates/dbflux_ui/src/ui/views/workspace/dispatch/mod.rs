use super::*;

mod audit;
mod charts_dashboards;
mod connections;
mod documents;
mod navigation;
mod preflight;
mod query;
mod scripts;
mod settings;

use preflight::sidebar_tree_command_is_blocked_by_search_focus;

impl CommandDispatcher for Workspace {
    fn dispatch(&mut self, cmd: Command, window: &mut Window, cx: &mut Context<Self>) -> bool {
        if self.sidebar.read(cx).has_child_picker_open() {
            match cmd {
                Command::SelectNext => {
                    self.sidebar.update(cx, |s, cx| s.picker_select_next(cx));
                    return true;
                }
                Command::SelectPrev => {
                    self.sidebar.update(cx, |s, cx| s.picker_select_prev(cx));
                    return true;
                }
                Command::SelectFirst => {
                    self.sidebar.update(cx, |s, cx| s.picker_select_first(cx));
                    return true;
                }
                Command::SelectLast => {
                    self.sidebar.update(cx, |s, cx| s.picker_select_last(cx));
                    return true;
                }
                Command::Execute => {
                    self.sidebar.update(cx, |s, cx| s.picker_execute(cx));
                    return true;
                }
                Command::FocusSearch => {
                    self.sidebar
                        .update(cx, |s, cx| s.picker_focus_search(window, cx));
                    return true;
                }
                Command::Cancel => {
                    if self.sidebar.read(cx).child_picker_filter_is_focused() {
                        // Pop focus back to the list so subsequent Cancel closes the modal.
                        self.sidebar
                            .update(cx, |s, cx| s.picker_focus_list(window, cx));
                    } else {
                        self.sidebar.update(cx, |s, cx| s.close_child_picker(cx));
                    }
                    return true;
                }
                _ => return false,
            }
        }

        if self.focus_target == FocusTarget::Sidebar
            && self.sidebar.read(cx).search_input_is_focused(window, cx)
            && sidebar_tree_command_is_blocked_by_search_focus(cmd)
        {
            return false;
        }

        // When context menu is open, only allow menu-related commands
        if self.focus_target == FocusTarget::Sidebar
            && self.sidebar.read(cx).has_context_menu_open()
        {
            match cmd {
                Command::SelectNext
                | Command::SelectPrev
                | Command::SelectFirst
                | Command::SelectLast
                | Command::Execute
                | Command::ColumnLeft
                | Command::ColumnRight
                | Command::Cancel
                | Command::NewQueryTab => {}
                _ => return true,
            }
        }

        if cmd == Command::Cancel {
            return self.handle_cancel(window, cx);
        }

        if let Some(result) = self.dispatch_connections(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_settings(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_audit(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_scripts(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_charts_dashboards(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_query(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_documents(cmd, window, cx) {
            return result;
        }
        if let Some(result) = self.dispatch_navigation(cmd, window, cx) {
            return result;
        }

        unreachable!("Command::{:?} not handled by any dispatch domain", cmd)
    }
}

impl Workspace {
    fn handle_cancel(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        if self.command_palette.read(cx).is_visible() {
            self.command_palette.update(cx, |p, cx| p.hide(cx));
            self.set_focus(self.focus_target, window, cx);
            return true;
        }

        // Cancel delete confirmation modal
        if self.sidebar.read(cx).has_delete_modal() {
            self.sidebar.update(cx, |s, cx| s.cancel_modal_delete(cx));
            return true;
        }

        // Cancel pending delete (keyboard x)
        if self.sidebar.read(cx).has_pending_delete() {
            self.sidebar.update(cx, |s, cx| s.cancel_pending_delete(cx));
            return true;
        }

        if self.sidebar.read(cx).has_context_menu_open() {
            self.sidebar.update(cx, |s, cx| s.close_context_menu(cx));
            return true;
        }

        // Clear multi-selection in sidebar
        if self.sidebar.read(cx).has_multi_selection() {
            self.sidebar.update(cx, |s, cx| s.clear_selection(cx));
            return true;
        }

        // Route Cancel to active document (handles modals, edit modes, etc.).
        if self.tab_manager.update(cx, |mgr, cx| {
            mgr.dispatch_active(Command::Cancel, window, cx)
        }) {
            return true;
        }

        // Workspace-level inspector close fallback.
        // Only fires after the document has declined Cancel.
        if self.workspace_inspector.read(cx).is_open() {
            self.workspace_inspector.update(cx, |insp, cx| {
                insp.close(cx);
            });
            return true;
        }

        // Always focus workspace to blur any input and enable keyboard navigation
        self.focus_handle.focus(window);
        true
    }
}
