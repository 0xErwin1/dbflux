use super::*;

impl Workspace {
    pub(super) fn dispatch_documents(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<bool> {
        match cmd {
            Command::NextTab => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.next_visual_tab(cx);
                });
                // Focus the newly active document
                self.tab_manager
                    .update(cx, |mgr, cx| mgr.focus_active(window, cx));
                Some(true)
            }
            Command::PrevTab => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.prev_visual_tab(cx);
                });
                // Focus the newly active document
                self.tab_manager
                    .update(cx, |mgr, cx| mgr.focus_active(window, cx));
                Some(true)
            }
            Command::SwitchToTab(n) => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.switch_to_tab(n, cx);
                });
                // Focus the newly active document
                self.tab_manager
                    .update(cx, |mgr, cx| mgr.focus_active(window, cx));
                Some(true)
            }
            Command::CloseCurrentTab => {
                self.close_active_tab(window, cx);
                // Focus the newly active document if any
                self.tab_manager
                    .update(cx, |mgr, cx| mgr.focus_active(window, cx));
                Some(true)
            }

            Command::OpenTabMenu => {
                self.tab_bar
                    .update(cx, |tb, cx| tb.open_context_menu_for_active(cx));
                Some(true)
            }

            // Context menu commands — route to tab bar if its menu is open,
            // otherwise to the active document (DataGridPanel).
            Command::OpenContextMenu
            | Command::MenuUp
            | Command::MenuDown
            | Command::MenuSelect
            | Command::MenuBack => {
                if self.tab_bar.read(cx).has_context_menu_open() {
                    self.tab_bar.update(cx, |tb, cx| match cmd {
                        Command::MenuDown => tb.context_menu_select_next(cx),
                        Command::MenuUp => tb.context_menu_select_prev(cx),
                        Command::MenuSelect => tb.context_menu_execute(cx),
                        Command::MenuBack => tb.close_context_menu(cx),
                        _ => {}
                    });
                } else {
                    self.tab_manager.update(cx, |mgr, cx| {
                        mgr.dispatch_active(cmd, window, cx);
                    });
                }
                Some(true)
            }

            _ => None,
        }
    }
}
