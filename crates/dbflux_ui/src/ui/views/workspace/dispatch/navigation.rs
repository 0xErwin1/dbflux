use super::*;

impl Workspace {
    pub(super) fn dispatch_navigation(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<bool> {
        match cmd {
            Command::ToggleCommandPalette => {
                self.toggle_command_palette(window, cx);
                Some(true)
            }

            Command::ToggleTasks => {
                self.toggle_tasks_panel(cx);
                Some(true)
            }
            Command::ToggleSidebar => {
                self.toggle_sidebar(cx);
                Some(true)
            }
            Command::FocusSidebar => {
                if self.is_sidebar_collapsed(cx) {
                    self.toggle_sidebar(cx);
                }
                self.set_focus(FocusTarget::Sidebar, window, cx);
                Some(true)
            }
            Command::FocusEditor => {
                self.set_focus(FocusTarget::Document, window, cx);
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::FocusUp, window, cx);
                });
                Some(true)
            }
            Command::FocusResults => {
                self.set_focus(FocusTarget::Document, window, cx);
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::FocusDown, window, cx);
                });
                Some(true)
            }

            Command::CycleFocusForward => {
                let next = self.next_focus_target(cx);
                self.set_focus(next, window, cx);
                Some(true)
            }
            Command::CycleFocusBackward => {
                let prev = self.prev_focus_target(cx);
                self.set_focus(prev, window, cx);
                Some(true)
            }

            Command::SelectNext => Some(match self.focus_target {
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
                    self.tab_manager.update(cx, |mgr, cx| {
                        mgr.dispatch_active(Command::SelectNext, window, cx);
                    });
                    true
                }
                _ => false,
            }),

            Command::SelectPrev => Some(match self.focus_target {
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
                    self.tab_manager.update(cx, |mgr, cx| {
                        mgr.dispatch_active(Command::SelectPrev, window, cx);
                    });
                    true
                }
                _ => false,
            }),

            Command::SelectFirst => Some(match self.focus_target {
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
                    self.tab_manager.update(cx, |mgr, cx| {
                        mgr.dispatch_active(Command::SelectFirst, window, cx);
                    });
                    true
                }
                _ => false,
            }),

            Command::SelectLast => Some(match self.focus_target {
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
                    self.tab_manager.update(cx, |mgr, cx| {
                        mgr.dispatch_active(Command::SelectLast, window, cx);
                    });
                    true
                }
                _ => false,
            }),

            Command::Execute => Some(match self.focus_target {
                FocusTarget::Sidebar => {
                    if self.sidebar.read(cx).has_context_menu_open() {
                        self.sidebar.update(cx, |s, cx| s.context_menu_execute(cx));
                    } else {
                        self.sidebar.update(cx, |s, cx| s.execute(cx));
                    }
                    true
                }
                FocusTarget::Document => {
                    self.tab_manager.update(cx, |mgr, cx| {
                        mgr.dispatch_active(Command::Execute, window, cx);
                    });
                    true
                }
                _ => false,
            }),

            Command::ExpandCollapse => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar.update(cx, |s, cx| s.expand_collapse(cx));
                    Some(true)
                } else {
                    Some(false)
                }
            }

            Command::ColumnLeft => Some(match self.focus_target {
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
                    self.tab_manager.update(cx, |mgr, cx| {
                        mgr.dispatch_active(Command::ColumnLeft, window, cx);
                    });
                    true
                }
                _ => false,
            }),

            Command::ColumnRight => Some(match self.focus_target {
                FocusTarget::Sidebar => {
                    if self.sidebar.read(cx).has_context_menu_open() {
                        self.sidebar.update(cx, |s, cx| s.context_menu_execute(cx));
                    } else {
                        self.sidebar.update(cx, |s, cx| s.expand(cx));
                    }
                    true
                }
                FocusTarget::Document => {
                    self.tab_manager.update(cx, |mgr, cx| {
                        mgr.dispatch_active(Command::ColumnRight, window, cx);
                    });
                    true
                }
                _ => false,
            }),

            Command::TogglePanel => Some(match self.focus_target {
                FocusTarget::Document => {
                    self.tab_manager.update(cx, |mgr, cx| {
                        mgr.dispatch_active(Command::TogglePanel, window, cx);
                    });
                    true
                }
                FocusTarget::BackgroundTasks => {
                    self.tasks_state.toggle();
                    cx.notify();
                    true
                }
                _ => false,
            }),

            Command::FocusToolbar => {
                // Route to active document
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::FocusToolbar, window, cx);
                });
                Some(true)
            }

            Command::ToggleFavorite => Some(false),

            // Directional focus navigation
            // Layout:  Sidebar | Document
            //                  | BackgroundTasks
            Command::FocusLeft => Some(self.handle_focus_left(window, cx)),

            Command::FocusRight => Some(self.handle_focus_right(window, cx)),

            Command::FocusDown => Some(self.handle_focus_down(window, cx)),

            Command::FocusUp => Some(self.handle_focus_up(window, cx)),

            Command::FocusBackgroundTasks => {
                self.set_focus(FocusTarget::BackgroundTasks, window, cx);
                Some(true)
            }

            Command::Rename => Some(if self.focus_target == FocusTarget::Sidebar {
                self.sidebar
                    .update(cx, |s, cx| s.start_rename_selected(window, cx));
                true
            } else if self.focus_target == FocusTarget::Document {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::Rename, window, cx);
                });
                true
            } else {
                false
            }),

            Command::Delete => Some(if self.focus_target == FocusTarget::Sidebar {
                self.sidebar
                    .update(cx, |s, cx| s.request_delete_selected(cx));
                true
            } else if self.focus_target == FocusTarget::Document {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::Delete, window, cx);
                });
                true
            } else {
                false
            }),

            Command::CreateFolder => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar.update(cx, |s, cx| match s.active_tab() {
                        dbflux_ui_sidebar::SidebarTab::Connections => {
                            s.create_root_folder(cx);
                        }
                        dbflux_ui_sidebar::SidebarTab::Scripts => {
                            s.create_script_folder(cx);
                        }
                    });
                    Some(true)
                } else {
                    Some(false)
                }
            }

            Command::SidebarNextTab => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar.update(cx, |s, cx| s.cycle_tab(cx));
                    Some(true)
                } else {
                    Some(false)
                }
            }

            Command::FocusSearch => Some(if self.focus_target == FocusTarget::Sidebar {
                self.sidebar.update(cx, |sidebar, cx| {
                    sidebar.focus_active_search(window, cx);
                });
                true
            } else if self.focus_target == FocusTarget::Document {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::FocusSearch, window, cx);
                });
                true
            } else {
                false
            }),

            Command::OpenItemMenu => {
                if self.focus_target == FocusTarget::Sidebar {
                    let position = self.sidebar.read(cx).selected_item_menu_position(cx);
                    self.sidebar
                        .update(cx, |s, cx| s.open_item_menu(position, cx));
                    Some(true)
                } else {
                    Some(false)
                }
            }

            Command::ExtendSelectNext => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar.update(cx, |s, cx| s.extend_select_next(cx));
                    Some(true)
                } else {
                    Some(false)
                }
            }

            Command::ExtendSelectPrev => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar.update(cx, |s, cx| s.extend_select_prev(cx));
                    Some(true)
                } else {
                    Some(false)
                }
            }

            Command::ToggleSelection => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar
                        .update(cx, |s, cx| s.toggle_current_selection(cx));
                    Some(true)
                } else {
                    Some(false)
                }
            }

            Command::MoveSelectedUp => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar
                        .update(cx, |s, cx| s.move_selected_items(-1, cx));
                    Some(true)
                } else {
                    Some(false)
                }
            }

            Command::MoveSelectedDown => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar
                        .update(cx, |s, cx| s.move_selected_items(1, cx));
                    Some(true)
                } else {
                    Some(false)
                }
            }

            Command::PageDown | Command::PageUp => {
                log::debug!("Context-specific command {:?} not yet implemented", cmd);
                Some(false)
            }

            _ => None,
        }
    }

    fn handle_focus_left(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        // Only try document dispatch when in context bar mode,
        // otherwise FocusLeft would be swallowed by DataGridPanel column navigation.
        let active_ctx = self
            .tab_manager
            .read(cx)
            .active_tab()
            .map(|tab| tab.active_context(cx));
        if active_ctx == Some(ContextId::ContextBar)
            && self.tab_manager.update(cx, |mgr, cx| {
                mgr.dispatch_active(Command::FocusLeft, window, cx)
            })
        {
            return true;
        }

        if self.is_sidebar_collapsed(cx) {
            return false;
        }

        // If a document is active (its context is visible), treat
        // focus_target as Document even if the internal field is stale —
        // this covers the case where the audit or results view received
        // keyboard focus before any mouse click updated focus_target.
        let effective_target = match active_ctx {
            Some(ctx)
                if ctx == ContextId::Audit
                    || ctx == ContextId::Results
                    || ctx == ContextId::Editor =>
            {
                FocusTarget::Document
            }
            _ => self.focus_target,
        };

        match effective_target {
            FocusTarget::Document | FocusTarget::BackgroundTasks => {
                self.set_focus(FocusTarget::Sidebar, window, cx);
                true
            }
            _ => false,
        }
    }

    fn handle_focus_right(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        // Only try document dispatch when in context bar mode,
        // otherwise FocusRight would be swallowed by DataGridPanel column navigation.
        let active_ctx = self
            .tab_manager
            .read(cx)
            .active_tab()
            .map(|tab| tab.active_context(cx));
        if active_ctx == Some(ContextId::ContextBar)
            && self.tab_manager.update(cx, |mgr, cx| {
                mgr.dispatch_active(Command::FocusRight, window, cx)
            })
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

    fn handle_focus_down(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        // First try the active document (for internal editor->results navigation)
        if self.tab_manager.update(cx, |mgr, cx| {
            mgr.dispatch_active(Command::FocusDown, window, cx)
        }) {
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

    fn handle_focus_up(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        // First try the active document (for internal results->editor navigation)
        if self.tab_manager.update(cx, |mgr, cx| {
            mgr.dispatch_active(Command::FocusUp, window, cx)
        }) {
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
}
