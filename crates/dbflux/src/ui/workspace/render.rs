use super::*;

impl Workspace {
    fn render_panel_header(
        &self,
        title: &'static str,
        icon: AppIcon,
        is_expanded: bool,
        is_focused: bool,
        on_toggle: impl Fn(&mut Self, &mut Context<Self>) + 'static,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let theme = cx.theme();
        let chevron = if is_expanded {
            AppIcon::ChevronDown
        } else {
            AppIcon::ChevronRight
        };

        let title_color = if is_focused {
            theme.primary
        } else {
            theme.foreground
        };

        let title_weight = if is_focused {
            FontWeight::BOLD
        } else {
            FontWeight::MEDIUM
        };

        div()
            .id(SharedString::from(format!("panel-header-{}", title)))
            .flex()
            .items_center()
            .justify_between()
            .h(px(24.0))
            .px_2()
            .bg(theme.tab_bar)
            .border_b_1()
            .border_color(theme.border)
            .cursor_pointer()
            .hover(|s| s.bg(theme.secondary))
            .on_click(cx.listener(move |this, _, _, cx| {
                on_toggle(this, cx);
            }))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .text_xs()
                    .font_weight(title_weight)
                    .text_color(title_color)
                    .child(svg().path(chevron.path()).size_3().text_color(title_color))
                    .child(svg().path(icon.path()).size_3().text_color(title_color))
                    .child(title),
            )
    }

    /// Renders the active document from TabManager (v0.3).
    fn render_active_document(&self, cx: &App) -> Option<AnyElement> {
        self.tab_manager
            .read(cx)
            .active_document()
            .map(|doc| doc.render())
    }
}

impl Render for Workspace {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(command_id) = self.pending_command.take() {
            self.handle_command(command_id, window, cx);
            self.focus_handle.focus(window);
        }

        // Handle SQL generated from sidebar (e.g., SELECT * FROM table)
        if let Some(sql) = self.pending_sql.take() {
            self.new_query_tab_with_content(sql, window, cx);
        }

        if let Some(target) = self.pending_focus.take() {
            self.set_focus(target, window, cx);
        }

        if self.needs_focus_restore {
            self.needs_focus_restore = false;
            self.focus_handle.focus(window);
        }

        let sidebar_dock = self.sidebar_dock.clone();
        let status_bar = self.status_bar.clone();
        let tasks_panel = self.tasks_panel.clone();
        let toast_host = self.toast_host.clone();
        let command_palette = self.command_palette.clone();

        let tab_bar = self.tab_bar.clone();
        let has_tabs = !self.tab_manager.read(cx).is_empty();
        let active_doc_element = self.render_active_document(cx);

        let tasks_expanded = self.tasks_state.is_expanded();
        let tasks_focused = self.focus_target == FocusTarget::BackgroundTasks;

        let theme = cx.theme();
        let bg_color = theme.background;
        let muted_fg = theme.muted_foreground;
        let header_size = px(25.0);

        let right_pane = if has_tabs {
            let tasks_header = self.render_panel_header(
                "Background Tasks",
                AppIcon::Loader,
                tasks_expanded,
                tasks_focused,
                Self::toggle_tasks_panel,
                cx,
            );

            v_resizable("main-panels")
                .child(
                    resizable_panel()
                        .size(px(500.0))
                        .size_range(px(200.0)..px(2000.0))
                        .child(
                            div()
                                .id("document-area")
                                .flex()
                                .flex_col()
                                .size_full()
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(|this, _, window, cx| {
                                        if this.focus_target != FocusTarget::Document {
                                            this.set_focus(FocusTarget::Document, window, cx);
                                        }
                                    }),
                                )
                                .child(tab_bar)
                                .when_some(active_doc_element, |el, doc| {
                                    el.child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .flex_1()
                                            .overflow_hidden()
                                            .child(doc),
                                    )
                                }),
                        ),
                )
                .child(
                    resizable_panel()
                        .size(if tasks_expanded {
                            px(150.0)
                        } else {
                            header_size
                        })
                        .size_range(if tasks_expanded {
                            px(80.0)..px(2000.0)
                        } else {
                            header_size..header_size
                        })
                        .child(
                            div()
                                .id("tasks-panel")
                                .flex()
                                .flex_col()
                                .size_full()
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(|this, _, window, cx| {
                                        if this.focus_target != FocusTarget::BackgroundTasks {
                                            this.set_focus(
                                                FocusTarget::BackgroundTasks,
                                                window,
                                                cx,
                                            );
                                        }
                                    }),
                                )
                                .child(tasks_header)
                                .when(tasks_expanded, |el| {
                                    el.child(div().flex_1().overflow_hidden().child(tasks_panel))
                                }),
                        ),
                )
        } else {
            // Empty state: welcome message + tasks panel
            let tasks_header_empty = self.render_panel_header(
                "Background Tasks",
                AppIcon::Loader,
                tasks_expanded,
                tasks_focused,
                Self::toggle_tasks_panel,
                cx,
            );

            v_resizable("main-panels")
                .child(
                    resizable_panel()
                        .size(px(500.0))
                        .size_range(px(200.0)..px(2000.0))
                        .child(
                            div()
                                .id("empty-state")
                                .flex()
                                .flex_col()
                                .size_full()
                                .items_center()
                                .justify_center()
                                .gap_4()
                                .child(
                                    svg()
                                        .path(AppIcon::Database.path())
                                        .size_16()
                                        .text_color(muted_fg.opacity(0.5)),
                                )
                                .child(
                                    div()
                                        .text_color(muted_fg)
                                        .text_sm()
                                        .child("No documents open"),
                                )
                                .child(
                                    div()
                                        .text_color(muted_fg.opacity(0.7))
                                        .text_xs()
                                        .child("Press Ctrl+N to create a new query"),
                                ),
                        ),
                )
                .child(
                    resizable_panel()
                        .size(if tasks_expanded {
                            px(150.0)
                        } else {
                            header_size
                        })
                        .size_range(if tasks_expanded {
                            px(80.0)..px(2000.0)
                        } else {
                            header_size..header_size
                        })
                        .child(
                            div()
                                .id("tasks-panel")
                                .flex()
                                .flex_col()
                                .size_full()
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(|this, _, window, cx| {
                                        if this.focus_target != FocusTarget::BackgroundTasks {
                                            this.set_focus(
                                                FocusTarget::BackgroundTasks,
                                                window,
                                                cx,
                                            );
                                        }
                                    }),
                                )
                                .child(tasks_header_empty)
                                .when(tasks_expanded, |el| {
                                    el.child(
                                        div().flex_1().overflow_hidden().child(tasks_panel.clone()),
                                    )
                                }),
                        ),
                )
        };

        let focus_handle = self.focus_handle.clone();

        div()
            .id("workspace-root")
            .relative()
            .size_full()
            .bg(bg_color)
            .on_mouse_move(cx.listener(|this, event: &MouseMoveEvent, _, cx| {
                if this.sidebar_dock.read(cx).is_resizing() {
                    this.sidebar_dock.update(cx, |dock, cx| {
                        dock.handle_resize_move(event.position.x, cx);
                    });
                }
            }))
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    if this.sidebar_dock.read(cx).is_resizing() {
                        this.sidebar_dock.update(cx, |dock, cx| {
                            dock.finish_resize(cx);
                        });
                    }
                }),
            )
            .track_focus(&focus_handle)
            .on_action(
                cx.listener(|this, _: &keymap::ToggleCommandPalette, window, cx| {
                    this.toggle_command_palette(window, cx);
                }),
            )
            .on_action(cx.listener(|this, _: &keymap::NewQueryTab, window, cx| {
                this.new_query_tab(window, cx);
            }))
            .on_action(
                cx.listener(|this, _: &keymap::CloseCurrentTab, _window, cx| {
                    this.tab_manager.update(cx, |mgr, cx| {
                        mgr.close_active(cx);
                    });
                }),
            )
            .on_action(cx.listener(|this, _: &keymap::NextTab, _window, cx| {
                this.tab_manager.update(cx, |mgr, cx| {
                    mgr.next_visual_tab(cx);
                });
            }))
            .on_action(cx.listener(|this, _: &keymap::PrevTab, _window, cx| {
                this.tab_manager.update(cx, |mgr, cx| {
                    mgr.prev_visual_tab(cx);
                });
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab1, _window, cx| {
                this.tab_manager
                    .update(cx, |mgr, cx| mgr.switch_to_tab(1, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab2, _window, cx| {
                this.tab_manager
                    .update(cx, |mgr, cx| mgr.switch_to_tab(2, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab3, _window, cx| {
                this.tab_manager
                    .update(cx, |mgr, cx| mgr.switch_to_tab(3, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab4, _window, cx| {
                this.tab_manager
                    .update(cx, |mgr, cx| mgr.switch_to_tab(4, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab5, _window, cx| {
                this.tab_manager
                    .update(cx, |mgr, cx| mgr.switch_to_tab(5, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab6, _window, cx| {
                this.tab_manager
                    .update(cx, |mgr, cx| mgr.switch_to_tab(6, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab7, _window, cx| {
                this.tab_manager
                    .update(cx, |mgr, cx| mgr.switch_to_tab(7, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab8, _window, cx| {
                this.tab_manager
                    .update(cx, |mgr, cx| mgr.switch_to_tab(8, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab9, _window, cx| {
                this.tab_manager
                    .update(cx, |mgr, cx| mgr.switch_to_tab(9, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::FocusSidebar, window, cx| {
                this.set_focus(FocusTarget::Sidebar, window, cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::FocusEditor, window, cx| {
                this.set_focus(FocusTarget::Document, window, cx);
                if let Some(doc) = this.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::FocusUp, window, cx);
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::FocusResults, window, cx| {
                this.set_focus(FocusTarget::Document, window, cx);
                if let Some(doc) = this.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::FocusDown, window, cx);
                }
            }))
            .on_action(
                cx.listener(|this, _: &keymap::FocusBackgroundTasks, window, cx| {
                    this.set_focus(FocusTarget::BackgroundTasks, window, cx);
                }),
            )
            .on_action(
                cx.listener(|this, _: &keymap::CycleFocusForward, window, cx| {
                    let next = this.next_focus_target(cx);
                    this.set_focus(next, window, cx);
                }),
            )
            .on_action(
                cx.listener(|this, _: &keymap::CycleFocusBackward, window, cx| {
                    let prev = this.prev_focus_target(cx);
                    this.set_focus(prev, window, cx);
                }),
            )
            .on_action(cx.listener(|this, _: &keymap::FocusLeft, window, cx| {
                this.dispatch(Command::FocusLeft, window, cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::FocusRight, window, cx| {
                this.dispatch(Command::FocusRight, window, cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::FocusUp, window, cx| {
                this.dispatch(Command::FocusUp, window, cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::FocusDown, window, cx| {
                this.dispatch(Command::FocusDown, window, cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::RunQuery, window, cx| {
                if let Some(doc) = this.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::RunQuery, window, cx);
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::Cancel, window, cx| {
                if this.command_palette.read(cx).is_visible() {
                    this.command_palette.update(cx, |p, cx| p.hide(cx));
                }
                // Always focus the workspace to exit any input and enable navigation
                this.focus_handle.focus(window);
            }))
            .on_action(cx.listener(|this, _: &keymap::ExportResults, window, cx| {
                if let Some(doc) = this.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ExportResults, window, cx);
                }
            }))
            .on_action(
                cx.listener(|this, _: &keymap::OpenConnectionManager, _window, cx| {
                    this.open_connection_manager(cx);
                }),
            )
            .on_action(cx.listener(|this, _: &keymap::Disconnect, window, cx| {
                this.disconnect_active(window, cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::RefreshSchema, window, cx| {
                this.refresh_schema(window, cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::ToggleEditor, window, cx| {
                if let Some(doc) = this.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ToggleEditor, window, cx);
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::ToggleResults, window, cx| {
                if let Some(doc) = this.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ToggleResults, window, cx);
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::ToggleTasks, _window, cx| {
                this.toggle_tasks_panel(cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::ToggleSidebar, _window, cx| {
                this.toggle_sidebar(cx);
            }))
            // List navigation actions - propagate if not handled so editor can receive keys
            .on_action(cx.listener(|this, _: &keymap::SelectNext, window, cx| {
                if !this.dispatch(Command::SelectNext, window, cx) {
                    cx.propagate();
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::SelectPrev, window, cx| {
                if !this.dispatch(Command::SelectPrev, window, cx) {
                    cx.propagate();
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::SelectFirst, window, cx| {
                if !this.dispatch(Command::SelectFirst, window, cx) {
                    cx.propagate();
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::SelectLast, window, cx| {
                if !this.dispatch(Command::SelectLast, window, cx) {
                    cx.propagate();
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::Execute, window, cx| {
                if !this.dispatch(Command::Execute, window, cx) {
                    cx.propagate();
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::ExpandCollapse, window, cx| {
                if !this.dispatch(Command::ExpandCollapse, window, cx) {
                    cx.propagate();
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::ColumnLeft, window, cx| {
                if !this.dispatch(Command::ColumnLeft, window, cx) {
                    cx.propagate();
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::ColumnRight, window, cx| {
                if !this.dispatch(Command::ColumnRight, window, cx) {
                    cx.propagate();
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::FocusToolbar, window, cx| {
                if !this.dispatch(Command::FocusToolbar, window, cx) {
                    cx.propagate();
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::TogglePanel, window, cx| {
                if !this.dispatch(Command::TogglePanel, window, cx) {
                    cx.propagate();
                }
            }))
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                let chord = KeyChord::from_gpui(&event.keystroke);
                let context = this.active_context(cx);

                if let Some(cmd) = this.keymap.resolve(context, &chord)
                    && this.dispatch(cmd, window, cx)
                {
                    cx.stop_propagation();
                }
            }))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .size_full()
                    .child(
                        div()
                            .flex()
                            .flex_row()
                            .flex_1()
                            .overflow_hidden()
                            .child(
                                div()
                                    .id("sidebar-panel")
                                    .h_full()
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, window, cx| {
                                            if !this.is_sidebar_collapsed(cx)
                                                && this.focus_target != FocusTarget::Sidebar
                                            {
                                                this.set_focus(FocusTarget::Sidebar, window, cx);
                                            }
                                        }),
                                    )
                                    .child(sidebar_dock),
                            )
                            .child(div().flex_1().overflow_hidden().child(right_pane)),
                    )
                    .child(status_bar),
            )
            .child(command_palette)
            .child(self.sql_preview_modal.clone())
            .child(
                div()
                    .absolute()
                    .top_0()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .child(toast_host),
            )
            // Shutdown overlay (rendered above everything during shutdown)
            .child(self.shutdown_overlay.clone())
            // Context menu rendered at workspace level for proper positioning
            .when_some(self.sidebar.read(cx).context_menu_state(), |this, menu| {
                let theme = cx.theme();
                let sidebar_entity = self.sidebar.clone();

                let menu_x = menu.position.x;
                let menu_y = menu.position.y;
                let menu_width = px(160.0);
                let menu_gap = Spacing::XS;
                let menu_item_height = px(32.0);
                let menu_container_padding = px(4.0);

                let parent_entry = menu.parent_stack.last();

                let submenu_y_offset = if let Some((_, parent_selected)) = parent_entry {
                    menu_container_padding + (menu_item_height * (*parent_selected as f32))
                } else {
                    px(0.0)
                };

                let in_submenu = parent_entry.is_some();

                this
                    // Full-screen overlay to capture clicks outside
                    .child(
                        div()
                            .id("context-menu-overlay")
                            .absolute()
                            .top_0()
                            .left_0()
                            .size_full()
                            .on_mouse_down(MouseButton::Left, {
                                let sidebar = sidebar_entity.clone();
                                move |_, _, cx| {
                                    sidebar.update(cx, |s, cx| s.close_context_menu(cx));
                                }
                            }),
                    )
                    // Parent menu (shown when in submenu, at original position)
                    .when_some(parent_entry, |d, (parent_items, parent_selected)| {
                        d.child(
                            div()
                                .absolute()
                                .top(menu_y)
                                .left(menu_x)
                                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                    cx.stop_propagation();
                                })
                                .child(Sidebar::render_menu_panel(
                                    theme,
                                    parent_items,
                                    Some(*parent_selected),
                                    Some(sidebar_entity.clone()),
                                    "parent-menu",
                                    true, // is_parent_menu
                                )),
                        )
                    })
                    // Current menu (submenu to the right of parent, or main menu at click position)
                    .child(
                        div()
                            .absolute()
                            .top(menu_y + submenu_y_offset)
                            .left(if in_submenu {
                                menu_x + menu_width + menu_gap
                            } else {
                                menu_x
                            })
                            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                cx.stop_propagation();
                            })
                            .child(Sidebar::render_menu_panel(
                                theme,
                                &menu.items,
                                Some(menu.selected_index),
                                Some(sidebar_entity.clone()),
                                "context-menu",
                                false, // is_parent_menu
                            )),
                    )
            })
            // Delete confirmation modal rendered at workspace level for proper centering
            .when_some(
                self.sidebar.read(cx).delete_modal_info(),
                |el, (item_name, is_folder)| {
                    let theme = cx.theme();
                    let sidebar_confirm = self.sidebar.clone();
                    let sidebar_cancel = self.sidebar.clone();

                    let message = if is_folder {
                        format!("Delete folder \"{}\"?", item_name)
                    } else {
                        format!("Delete connection \"{}\"?", item_name)
                    };

                    let btn_hover = theme.muted;

                    el.child(
                        div()
                            .id("delete-modal-overlay")
                            .absolute()
                            .inset_0()
                            .bg(gpui::hsla(0.0, 0.0, 0.0, 0.5))
                            .flex()
                            .items_center()
                            .justify_center()
                            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                cx.stop_propagation();
                            })
                            .child(
                                div()
                                    .bg(theme.sidebar)
                                    .border_1()
                                    .border_color(theme.border)
                                    .rounded(Radii::MD)
                                    .p(Spacing::MD)
                                    .min_w(px(250.0))
                                    .flex()
                                    .flex_col()
                                    .gap(Spacing::MD)
                                    .child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap_2()
                                            .child(
                                                svg()
                                                    .path(AppIcon::TriangleAlert.path())
                                                    .size_5()
                                                    .text_color(theme.warning),
                                            )
                                            .child(
                                                div()
                                                    .text_size(FontSizes::SM)
                                                    .text_color(theme.foreground)
                                                    .child(message),
                                            ),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .justify_end()
                                            .gap(Spacing::SM)
                                            .child(
                                                div()
                                                    .id("delete-cancel")
                                                    .flex()
                                                    .items_center()
                                                    .gap_1()
                                                    .px(Spacing::SM)
                                                    .py(Spacing::XS)
                                                    .rounded(Radii::SM)
                                                    .cursor_pointer()
                                                    .text_size(FontSizes::SM)
                                                    .text_color(theme.muted_foreground)
                                                    .bg(theme.secondary)
                                                    .hover(move |d| d.bg(btn_hover))
                                                    .on_click(move |_, _, cx| {
                                                        sidebar_cancel.update(cx, |this, cx| {
                                                            this.cancel_modal_delete(cx);
                                                        });
                                                    })
                                                    .child(
                                                        svg()
                                                            .path(AppIcon::X.path())
                                                            .size_4()
                                                            .text_color(theme.muted_foreground),
                                                    )
                                                    .child("Cancel"),
                                            )
                                            .child(
                                                div()
                                                    .id("delete-confirm")
                                                    .flex()
                                                    .items_center()
                                                    .gap_1()
                                                    .px(Spacing::SM)
                                                    .py(Spacing::XS)
                                                    .rounded(Radii::SM)
                                                    .cursor_pointer()
                                                    .text_size(FontSizes::SM)
                                                    .text_color(theme.background)
                                                    .bg(theme.danger)
                                                    .hover(|d| d.opacity(0.9))
                                                    .on_click(move |_, _, cx| {
                                                        sidebar_confirm.update(cx, |this, cx| {
                                                            this.confirm_modal_delete(cx);
                                                        });
                                                    })
                                                    .child(
                                                        svg()
                                                            .path(AppIcon::Delete.path())
                                                            .size_4()
                                                            .text_color(theme.background),
                                                    )
                                                    .child("Delete"),
                                            ),
                                    ),
                            ),
                    )
                },
            )
    }
}
