use super::*;

impl CommandDispatcher for Workspace {
    fn dispatch(&mut self, cmd: Command, window: &mut Window, cx: &mut Context<Self>) -> bool {
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

        match cmd {
            Command::ToggleCommandPalette => {
                self.toggle_command_palette(window, cx);
                true
            }
            Command::NewQueryTab => {
                self.new_query_tab(window, cx);
                true
            }
            Command::OpenScriptFile => {
                self.open_script_file(window, cx);
                true
            }
            Command::RunQuery => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::RunQuery, window, cx);
                }
                true
            }
            Command::RunQueryInNewTab => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::RunQueryInNewTab, window, cx);
                }
                true
            }
            Command::ExportResults => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ExportResults, window, cx);
                }
                true
            }
            Command::OpenConnectionManager => {
                self.open_connection_manager(cx);
                true
            }
            Command::Disconnect => {
                self.disconnect_active(window, cx);
                true
            }
            Command::RefreshSchema => {
                self.refresh_schema(window, cx);
                true
            }
            Command::ToggleEditor => {
                // Route to active document for layout toggle
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ToggleEditor, window, cx);
                }
                true
            }
            Command::ToggleResults => {
                // Route to active document for layout toggle
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ToggleResults, window, cx);
                }
                true
            }
            Command::ToggleTasks => {
                self.toggle_tasks_panel(cx);
                true
            }
            Command::ToggleSidebar => {
                self.toggle_sidebar(cx);
                true
            }
            Command::FocusSidebar => {
                if self.is_sidebar_collapsed(cx) {
                    self.toggle_sidebar(cx);
                }
                self.set_focus(FocusTarget::Sidebar, window, cx);
                true
            }
            Command::FocusEditor => {
                self.set_focus(FocusTarget::Document, window, cx);
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::FocusUp, window, cx);
                }
                true
            }
            Command::FocusResults => {
                self.set_focus(FocusTarget::Document, window, cx);
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::FocusDown, window, cx);
                }
                true
            }

            Command::CycleFocusForward => {
                let next = self.next_focus_target(cx);
                self.set_focus(next, window, cx);
                true
            }
            Command::CycleFocusBackward => {
                let prev = self.prev_focus_target(cx);
                self.set_focus(prev, window, cx);
                true
            }
            Command::NextTab => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.next_visual_tab(cx);
                });
                // Focus the newly active document
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.focus(window, cx);
                }
                true
            }
            Command::PrevTab => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.prev_visual_tab(cx);
                });
                // Focus the newly active document
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.focus(window, cx);
                }
                true
            }
            Command::SwitchToTab(n) => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.switch_to_tab(n, cx);
                });
                // Focus the newly active document
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.focus(window, cx);
                }
                true
            }
            Command::CloseCurrentTab => {
                self.close_active_tab(window, cx);
                // Focus the newly active document if any
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.focus(window, cx);
                }
                true
            }
            Command::Cancel => {
                if self.command_palette.read(cx).is_visible() {
                    self.command_palette.update(cx, |p, cx| p.hide(cx));
                    self.focus_handle.focus(window);
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

                // Route Cancel to active document (handles modals, edit modes, etc.)
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned()
                    && doc.dispatch_command(Command::Cancel, window, cx)
                {
                    return true;
                }

                // Always focus workspace to blur any input and enable keyboard navigation
                self.focus_handle.focus(window);
                true
            }

            Command::CancelQuery => {
                // Route to active document
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::CancelQuery, window, cx);
                }
                true
            }

            Command::SelectNext => match self.focus_target {
                FocusTarget::Sidebar => {
                    if self.sidebar.read(cx).has_context_menu_open() {
                        self.sidebar
                            .update(cx, |s, cx| s.context_menu_select_next(cx));
                    } else {
                        self.sidebar.update(cx, |s, cx| s.select_next(cx));
                    }
                    true
                }
                FocusTarget::Document => {
                    if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                        doc.dispatch_command(Command::SelectNext, window, cx);
                    }
                    true
                }
                _ => false,
            },

            Command::SelectPrev => match self.focus_target {
                FocusTarget::Sidebar => {
                    if self.sidebar.read(cx).has_context_menu_open() {
                        self.sidebar
                            .update(cx, |s, cx| s.context_menu_select_prev(cx));
                    } else {
                        self.sidebar.update(cx, |s, cx| s.select_prev(cx));
                    }
                    true
                }
                FocusTarget::Document => {
                    if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                        doc.dispatch_command(Command::SelectPrev, window, cx);
                    }
                    true
                }
                _ => false,
            },

            Command::SelectFirst => match self.focus_target {
                FocusTarget::Sidebar => {
                    if self.sidebar.read(cx).has_context_menu_open() {
                        self.sidebar
                            .update(cx, |s, cx| s.context_menu_select_first(cx));
                    } else {
                        self.sidebar.update(cx, |s, cx| s.select_first(cx));
                    }
                    true
                }
                FocusTarget::Document => {
                    if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                        doc.dispatch_command(Command::SelectFirst, window, cx);
                    }
                    true
                }
                _ => false,
            },

            Command::SelectLast => match self.focus_target {
                FocusTarget::Sidebar => {
                    if self.sidebar.read(cx).has_context_menu_open() {
                        self.sidebar
                            .update(cx, |s, cx| s.context_menu_select_last(cx));
                    } else {
                        self.sidebar.update(cx, |s, cx| s.select_last(cx));
                    }
                    true
                }
                FocusTarget::Document => {
                    if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                        doc.dispatch_command(Command::SelectLast, window, cx);
                    }
                    true
                }
                _ => false,
            },

            Command::Execute => match self.focus_target {
                FocusTarget::Sidebar => {
                    if self.sidebar.read(cx).has_context_menu_open() {
                        self.sidebar.update(cx, |s, cx| s.context_menu_execute(cx));
                    } else {
                        self.sidebar.update(cx, |s, cx| s.execute(cx));
                    }
                    true
                }
                FocusTarget::Document => {
                    if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                        doc.dispatch_command(Command::Execute, window, cx);
                    }
                    true
                }
                _ => false,
            },

            Command::ExpandCollapse => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar.update(cx, |s, cx| s.expand_collapse(cx));
                    true
                } else {
                    false
                }
            }

            Command::ColumnLeft => match self.focus_target {
                FocusTarget::Sidebar => {
                    if self.sidebar.read(cx).has_context_menu_open() {
                        let went_back = self.sidebar.update(cx, |s, cx| s.context_menu_go_back(cx));
                        if !went_back {
                            self.sidebar.update(cx, |s, cx| s.close_context_menu(cx));
                        }
                    } else {
                        self.sidebar.update(cx, |s, cx| s.collapse(cx));
                    }
                    true
                }
                FocusTarget::Document => {
                    if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                        doc.dispatch_command(Command::ColumnLeft, window, cx);
                    }
                    true
                }
                _ => false,
            },

            Command::ColumnRight => match self.focus_target {
                FocusTarget::Sidebar => {
                    if self.sidebar.read(cx).has_context_menu_open() {
                        self.sidebar.update(cx, |s, cx| s.context_menu_execute(cx));
                    } else {
                        self.sidebar.update(cx, |s, cx| s.expand(cx));
                    }
                    true
                }
                FocusTarget::Document => {
                    if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                        doc.dispatch_command(Command::ColumnRight, window, cx);
                    }
                    true
                }
                _ => false,
            },

            Command::TogglePanel => match self.focus_target {
                FocusTarget::Document => {
                    if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                        doc.dispatch_command(Command::TogglePanel, window, cx);
                    }
                    true
                }
                FocusTarget::BackgroundTasks => {
                    self.tasks_state.toggle();
                    cx.notify();
                    true
                }
                _ => false,
            },

            Command::FocusToolbar => {
                // Route to active document
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::FocusToolbar, window, cx);
                }
                true
            }

            Command::ToggleFavorite => false,

            // Directional focus navigation
            // Layout:  Sidebar | Document
            //                  | BackgroundTasks
            Command::FocusLeft => {
                // Only try document dispatch when in context bar mode,
                // otherwise FocusLeft would be swallowed by DataGridPanel column navigation.
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned()
                    && doc.active_context(cx) == ContextId::ContextBar
                    && doc.dispatch_command(Command::FocusLeft, window, cx)
                {
                    return true;
                }

                if self.is_sidebar_collapsed(cx) {
                    return false;
                }
                match self.focus_target {
                    FocusTarget::Document | FocusTarget::BackgroundTasks => {
                        self.set_focus(FocusTarget::Sidebar, window, cx);
                        true
                    }
                    _ => false,
                }
            }

            Command::FocusRight => {
                // Only try document dispatch when in context bar mode,
                // otherwise FocusRight would be swallowed by DataGridPanel column navigation.
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned()
                    && doc.active_context(cx) == ContextId::ContextBar
                    && doc.dispatch_command(Command::FocusRight, window, cx)
                {
                    return true;
                }

                match self.focus_target {
                    FocusTarget::Sidebar => {
                        self.set_focus(FocusTarget::Document, window, cx);
                        true
                    }
                    _ => false,
                }
            }

            Command::FocusDown => {
                // First try the active document (for internal editor->results navigation)
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned()
                    && doc.dispatch_command(Command::FocusDown, window, cx)
                {
                    return true;
                }
                // Workspace-level: Document -> BackgroundTasks
                let next = match self.focus_target {
                    FocusTarget::Document => FocusTarget::BackgroundTasks,
                    FocusTarget::BackgroundTasks => FocusTarget::Document,
                    _ => return false,
                };
                self.set_focus(next, window, cx);
                true
            }

            Command::FocusUp => {
                // First try the active document (for internal results->editor navigation)
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned()
                    && doc.dispatch_command(Command::FocusUp, window, cx)
                {
                    return true;
                }
                // Workspace-level: BackgroundTasks -> Document
                let prev = match self.focus_target {
                    FocusTarget::BackgroundTasks => FocusTarget::Document,
                    FocusTarget::Document => FocusTarget::BackgroundTasks,
                    _ => return false,
                };
                self.set_focus(prev, window, cx);
                true
            }

            Command::ToggleHistoryDropdown => {
                // Route to active document
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ToggleHistoryDropdown, window, cx);
                }
                true
            }

            Command::OpenSavedQueries => {
                // Route to active document
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::OpenSavedQueries, window, cx);
                }
                true
            }

            Command::SaveQuery => {
                // Route to active document
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::SaveQuery, window, cx);
                }
                true
            }

            Command::SaveFileAs => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::SaveFileAs, window, cx);
                }
                true
            }

            Command::FocusBackgroundTasks => {
                self.set_focus(FocusTarget::BackgroundTasks, window, cx);
                true
            }

            Command::OpenSettings => {
                self.open_settings(cx);
                true
            }

            Command::Rename => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar
                        .update(cx, |s, cx| s.start_rename_selected(window, cx));
                    true
                } else if self.focus_target == FocusTarget::Document {
                    if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                        doc.dispatch_command(Command::Rename, window, cx);
                    }
                    true
                } else {
                    false
                }
            }

            Command::Delete => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar
                        .update(cx, |s, cx| s.request_delete_selected(cx));
                    true
                } else if self.focus_target == FocusTarget::Document {
                    if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                        doc.dispatch_command(Command::Delete, window, cx);
                    }
                    true
                } else {
                    false
                }
            }

            Command::CreateFolder => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar.update(cx, |s, cx| match s.active_tab() {
                        crate::ui::sidebar::SidebarTab::Connections => {
                            s.create_root_folder(cx);
                        }
                        crate::ui::sidebar::SidebarTab::Scripts => {
                            s.create_script_folder(cx);
                        }
                    });
                    true
                } else {
                    false
                }
            }

            Command::SidebarNextTab => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar.update(cx, |s, cx| s.cycle_tab(cx));
                    true
                } else {
                    false
                }
            }

            Command::FocusSearch => {
                if self.focus_target == FocusTarget::Document {
                    if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                        doc.dispatch_command(Command::FocusSearch, window, cx);
                    }
                    true
                } else {
                    false
                }
            }

            Command::OpenItemMenu => {
                if self.focus_target == FocusTarget::Sidebar {
                    let position = self.sidebar.read(cx).selected_item_menu_position(cx);
                    self.sidebar
                        .update(cx, |s, cx| s.open_item_menu(position, cx));
                    true
                } else {
                    false
                }
            }

            Command::ResultsNextPage => {
                // Route to active document
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ResultsNextPage, window, cx);
                }
                true
            }

            Command::ResultsPrevPage => {
                // Route to active document
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ResultsPrevPage, window, cx);
                }
                true
            }

            Command::ExtendSelectNext => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar.update(cx, |s, cx| s.extend_select_next(cx));
                    true
                } else {
                    false
                }
            }

            Command::ExtendSelectPrev => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar.update(cx, |s, cx| s.extend_select_prev(cx));
                    true
                } else {
                    false
                }
            }

            Command::ToggleSelection => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar
                        .update(cx, |s, cx| s.toggle_current_selection(cx));
                    true
                } else {
                    false
                }
            }

            Command::MoveSelectedUp => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar
                        .update(cx, |s, cx| s.move_selected_items(-1, cx));
                    true
                } else {
                    false
                }
            }

            Command::MoveSelectedDown => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar
                        .update(cx, |s, cx| s.move_selected_items(1, cx));
                    true
                } else {
                    false
                }
            }

            Command::PageDown | Command::PageUp => {
                log::debug!("Context-specific command {:?} not yet implemented", cmd);
                false
            }

            Command::ResultsAddRow | Command::ResultsCopyRow => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(cmd, window, cx);
                }
                true
            }

            // Row operations - handled via GPUI actions in DataTable
            Command::ResultsDeleteRow | Command::ResultsDuplicateRow | Command::ResultsSetNull => {
                log::debug!(
                    "Row operation {:?} handled via GPUI actions in Results context",
                    cmd
                );
                false
            }

            // Context menu commands - handled by DataGridPanel
            Command::OpenContextMenu
            | Command::MenuUp
            | Command::MenuDown
            | Command::MenuSelect
            | Command::MenuBack => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(cmd, window, cx);
                }
                true
            }
        }
    }
}
