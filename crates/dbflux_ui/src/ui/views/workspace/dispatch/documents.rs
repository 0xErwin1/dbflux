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
                let closed = self.close_active_tab(window, cx);
                // Focus the newly active document if any. When the dirty
                // check opened the confirmation instead, it owns the keyboard
                // and the document focus must stay off the editor input.
                if closed {
                    self.tab_manager
                        .update(cx, |mgr, cx| mgr.focus_active(window, cx));
                }
                Some(true)
            }

            Command::MoveTabLeft | Command::MoveTabRight => {
                let forward = cmd == Command::MoveTabRight;
                Some(
                    self.tab_manager
                        .update(cx, |mgr, cx| mgr.move_active_tab(forward, cx)),
                )
            }

            Command::OpenTabMenu => {
                self.tab_bar
                    .update(cx, |tb, cx| tb.open_context_menu_for_active(cx));
                Some(true)
            }

            // The active document may answer this itself (with a menu of its
            // own); otherwise the workspace lists the actions the focused
            // pane offers.
            Command::OpenPaneActions => {
                if self.has_pane_actions_menu() {
                    self.close_pane_actions_from_keyboard(window, cx);
                    return Some(true);
                }

                // The tasks panel lists its own actions, so a document menu
                // does not open under it.
                let handled_by_document = self.focus_target != FocusTarget::BackgroundTasks
                    && self.tab_manager.update(cx, |mgr, cx| {
                        mgr.dispatch_active(Command::OpenPaneActions, window, cx)
                    });

                Some(handled_by_document || self.open_pane_actions(window, cx))
            }

            // Context menu commands — route to the pane-actions menu or the
            // tab bar when one is open, otherwise to the active document
            // (DataGridPanel).
            Command::OpenContextMenu
            | Command::MenuUp
            | Command::MenuDown
            | Command::MenuSelect
            | Command::MenuBack => {
                if self.has_pane_actions_menu() {
                    self.dispatch_pane_actions_menu(cmd, window, cx);
                } else if self.tab_bar.read(cx).has_context_menu_open() {
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

            // Document tree and schema diagram commands only mean something
            // to the active document, which reports whether it handled them.
            Command::PreviewDocument
            | Command::ToggleRawView
            | Command::NextMatch
            | Command::PrevMatch
            | Command::ZoomIn
            | Command::ZoomOut
            | Command::PanLeft
            | Command::PanRight
            | Command::PanUp
            | Command::PanDown
            | Command::SelectTableLeft
            | Command::SelectTableRight
            | Command::SelectTableUp
            | Command::SelectTableDown
            | Command::MoveTableLeft
            | Command::MoveTableRight
            | Command::MoveTableUp
            | Command::MoveTableDown
            | Command::LayoutLeftRight
            | Command::LayoutSnowflake
            | Command::LayoutCompact
            // Element commands (grids, inputs) normally run as the element's
            // own action; a user binding that sends one here reaches the
            // active document, which decides whether it applies.
            | Command::ExtendSelectLeft
            | Command::ExtendSelectRight
            | Command::MoveToRowStart
            | Command::MoveToRowEnd
            | Command::ExtendSelectRowStart
            | Command::ExtendSelectRowEnd
            | Command::ExtendSelectFirst
            | Command::ExtendSelectLast
            | Command::SelectAll
            | Command::SaveRow
            | Command::Undo
            | Command::Redo
            | Command::ToggleColumnGroup
            | Command::StepOut
            | Command::TriggerCompletion
            // The filter a table shows belongs to its document.
            | Command::ClearFilter
            // So do the result tabs of a query.
            | Command::NextResultTab
            | Command::PrevResultTab
            | Command::CloseResultTab
            // So do the tabs of a panel a document draws (the query
            // history's Recent and Saved).
            | Command::NextPanelTab
            | Command::PrevPanelTab
            // So do the rows of a side rail the document draws (the query
            // builders).
            | Command::AddItem
            | Command::AddGroup => Some(
                self.tab_manager
                    .update(cx, |mgr, cx| mgr.dispatch_active(cmd, window, cx)),
            ),

            // Closing a window belongs to the settings window.
            Command::CloseWindow => Some(false),

            _ => None,
        }
    }
}
