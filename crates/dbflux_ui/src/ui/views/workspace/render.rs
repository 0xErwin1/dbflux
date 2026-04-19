use super::*;
use crate::platform;
use crate::ui::tokens::FontSizes;
use dbflux_components::composites::{PanelHeaderVariant, panel_header_collapsible_variant};
use dbflux_components::primitives::{Icon, Text, overlay_bg};
use dbflux_components::typography::Body;
use gpui_component::IconName;

impl Workspace {
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

        if let Some(pending) = self.pending_open_script.take() {
            self.finalize_open_script(pending, window, cx);
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
        let login_modal = self.login_modal.clone();
        let sso_wizard = self.sso_wizard.clone();

        let tab_bar = self.tab_bar.clone();
        let has_tabs = !self.tab_manager.read(cx).is_empty();
        let active_doc_element = self.render_active_document(cx);

        let tasks_expanded = self.tasks_state.is_expanded();
        let tasks_focused = self.focus_target == FocusTarget::BackgroundTasks;

        let theme = cx.theme().clone();
        let bg_color = theme.background;
        let muted_fg = theme.muted_foreground;
        let header_size = px(25.0);
        let sidebar_context_menu = self.sidebar.read(cx).context_menu_state().cloned();
        let tab_context_menu = self.tab_bar.read(cx).context_menu_state().cloned();

        // Linux CSD title bar: render only when the compositor has negotiated CSD mode.
        let linux_title_bar = platform::render_csd_title_bar(window, cx, "DBFlux");

        let right_pane = if has_tabs {
            let workspace = cx.entity().clone();
            let tasks_header = panel_header_collapsible_variant(
                "panel-header-Background Tasks",
                "Background Tasks",
                PanelHeaderVariant::WorkspaceTasks,
                !tasks_expanded,
                tasks_focused,
                Some(IconName::Loader),
                move |_, _, app| {
                    let _ = workspace.update(app, |workspace, cx| {
                        workspace.toggle_tasks_panel(cx);
                    });
                },
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
                                .on_mouse_down(
                                    MouseButton::Right,
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
                                            .min_h_0()
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
            let workspace = cx.entity().clone();
            let tasks_header_empty = panel_header_collapsible_variant(
                "panel-header-Background Tasks",
                "Background Tasks",
                PanelHeaderVariant::WorkspaceTasks,
                !tasks_expanded,
                tasks_focused,
                Some(IconName::Loader),
                move |_, _, app| {
                    let _ = workspace.update(app, |workspace, cx| {
                        workspace.toggle_tasks_panel(cx);
                    });
                },
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
                                    Icon::new(AppIcon::Database)
                                        .size(px(64.0))
                                        .color(muted_fg.opacity(0.5)),
                                )
                                .child(Body::new("No documents open").muted(cx))
                                .child(Text::dim_secondary("Press Ctrl+N to create a new query")),
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
                cx.listener(|this, _: &keymap::CloseCurrentTab, window, cx| {
                    this.close_active_tab(window, cx);
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
                if !this.dispatch(Command::Cancel, window, cx) {
                    cx.propagate();
                }
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
            .on_action(cx.listener(|this, _: &keymap::OpenScriptFile, window, cx| {
                this.open_script_file(window, cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::SaveFileAs, window, cx| {
                if let Some(doc) = this.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::SaveFileAs, window, cx);
                }
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
                let chord = key_chord_from_gpui(&event.keystroke);
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
                    .when_some(linux_title_bar, |el, title_bar| el.child(title_bar))
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
            .child(login_modal)
            .child(sso_wizard)
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
            .when(cfg!(feature = "mcp"), |root| {
                #[cfg(feature = "mcp")]
                {
                    root.when_some(self.active_governance_panel, |root, panel| {
                        let _close_entity = cx.entity().clone();
                        let title = match panel {
                            super::GovernancePanel::Approvals => "MCP Approvals",
                        };

                        let content = match panel {
                            super::GovernancePanel::Approvals => {
                                self.mcp_approvals_view.clone().into_any_element()
                            }
                        };

                        root.child(
                            div()
                                .id("governance-overlay")
                                .absolute()
                                .inset_0()
                                .bg(gpui::hsla(0.0, 0.0, 0.0, 0.45))
                                .flex()
                                .items_center()
                                .justify_center()
                                .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                    cx.stop_propagation();
                                })
                                .child(
                                    div()
                                        .w(px(1080.0))
                                        .h(px(680.0))
                                        .bg(theme.sidebar)
                                        .border_1()
                                        .border_color(theme.border)
                                        .rounded(Radii::MD)
                                        .overflow_hidden()
                                        .flex()
                                        .flex_col()
                                        .child(
                                            div()
                                                .h(px(40.0))
                                                .px(Spacing::MD)
                                                .flex()
                                                .items_center()
                                                .justify_between()
                                                .border_b_1()
                                                .border_color(theme.border)
                                                .child(Text::heading(title)),
                                        )
                                        .child(div().flex_1().min_h_0().child(content)),
                                ),
                        )
                    })
                }
                #[cfg(not(feature = "mcp"))]
                {
                    root
                }
            })
            // Context menu rendered at workspace level for proper positioning
            .when_some(sidebar_context_menu, |this, menu| {
                use crate::ui::components::context_menu as ctx;
                use crate::ui::views::sidebar::ContextMenuItem;

                let sidebar_entity = self.sidebar.clone();

                let menu_x = menu.position.x;
                let menu_y = menu.position.y;
                let menu_width = px(160.0);
                let menu_gap = Spacing::XS;
                let menu_item_height = Heights::ROW_COMPACT;
                let menu_container_padding = px(4.0);

                let parent_entry = menu.parent_stack.last();

                let submenu_y_offset = if let Some((_, parent_selected)) = parent_entry {
                    menu_container_padding + (menu_item_height * (*parent_selected as f32))
                } else {
                    px(0.0)
                };

                let in_submenu = parent_entry.is_some();

                // Overlay to dismiss on outside click
                let sidebar_dismiss = sidebar_entity.clone();
                let overlay = ctx::render_menu_overlay("context-menu-overlay", move |_, cx| {
                    sidebar_dismiss.update(cx, |s, cx| s.close_context_menu(cx));
                });

                this.child(overlay)
                    // Parent menu (shown when in submenu, at original position)
                    .when_some(parent_entry, |d, (parent_items, parent_selected)| {
                        let shared_items = ContextMenuItem::to_menu_items(parent_items);
                        let sidebar_click = sidebar_entity.clone();
                        let sidebar_hover = sidebar_entity.clone();

                        d.child(div().absolute().top(menu_y).left(menu_x).child(
                            ctx::render_menu_container(
                                "parent-menu",
                                &shared_items,
                                Some(*parent_selected),
                                move |idx, cx| {
                                    sidebar_click.update(cx, |s, cx| {
                                        s.context_menu_parent_execute_at(idx, cx);
                                    });
                                },
                                move |idx, cx| {
                                    sidebar_hover.update(cx, |s, cx| {
                                        s.context_menu_parent_hover_at(idx, cx);
                                    });
                                },
                                cx,
                            ),
                        ))
                    })
                    // Current menu (submenu to the right of parent, or main menu at click position)
                    .child({
                        let shared_items = ContextMenuItem::to_menu_items(&menu.items);
                        let sidebar_click = sidebar_entity.clone();
                        let sidebar_hover = sidebar_entity.clone();

                        div()
                            .absolute()
                            .top(menu_y + submenu_y_offset)
                            .left(if in_submenu {
                                menu_x + menu_width + menu_gap
                            } else {
                                menu_x
                            })
                            .child(ctx::render_menu_container(
                                "context-menu",
                                &shared_items,
                                Some(menu.selected_index),
                                move |idx, cx| {
                                    sidebar_click.update(cx, |s, cx| {
                                        s.context_menu_execute_at(idx, cx);
                                    });
                                },
                                move |idx, cx| {
                                    sidebar_hover.update(cx, |s, cx| {
                                        s.context_menu_hover_at(idx, cx);
                                    });
                                },
                                cx,
                            ))
                    })
            })
            // Tab context menu rendered at workspace level for proper positioning
            .when_some(tab_context_menu, |this, menu| {
                use crate::ui::components::context_menu as ctx;
                use crate::ui::document::tab_bar::TabBar;

                let tab_bar_entity = self.tab_bar.clone();

                let menu_x = menu.position_x;
                let menu_y = px(36.0);
                let items = TabBar::build_tab_menu_items();
                let selected = menu.selected_index;

                let tab_bar_dismiss = tab_bar_entity.clone();
                let overlay = ctx::render_menu_overlay("tab-context-menu-overlay", move |_, cx| {
                    tab_bar_dismiss.update(cx, |tb, cx| tb.close_context_menu(cx));
                });

                let tab_bar_click = tab_bar_entity.clone();
                let tab_bar_hover = tab_bar_entity.clone();

                this.child(overlay)
                    .child(div().absolute().top(menu_y).left(menu_x).child(
                        ctx::render_menu_container(
                            "tab-context-menu",
                            &items,
                            Some(selected),
                            move |idx, cx| {
                                tab_bar_click.update(cx, |tb, cx| {
                                    tb.context_menu_execute_at(idx, cx);
                                });
                            },
                            move |idx, cx| {
                                tab_bar_hover.update(cx, |tb, cx| {
                                    tb.context_menu_hover_at(idx, cx);
                                });
                            },
                            cx,
                        ),
                    ))
            })
            // Delete confirmation modal rendered at workspace level for proper centering
            .when_some(
                self.sidebar.read(cx).delete_modal_state(),
                |el, modal_state| {
                    let theme = cx.theme();
                    let sidebar_confirm = self.sidebar.clone();
                    let sidebar_cancel = self.sidebar.clone();

                    let message = if modal_state.is_ddl {
                        let object_type = modal_state.object_type.unwrap_or("Object");
                        format!("Drop {} \"{}\"?", object_type, modal_state.item_name)
                    } else if modal_state.is_folder {
                        format!("Delete folder \"{}\"?", modal_state.item_name)
                    } else {
                        format!("Delete connection \"{}\"?", modal_state.item_name)
                    };

                    let confirm_label = if modal_state.is_ddl { "Drop" } else { "Delete" };
                    let btn_hover = theme.muted;

                    el.child(
                        div()
                            .id("delete-modal-overlay")
                            .absolute()
                            .inset_0()
                            .bg(overlay_bg())
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
                                    .border_color(if modal_state.is_ddl {
                                        theme.danger
                                    } else {
                                        theme.border
                                    })
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
                                                    .path(if modal_state.is_ddl {
                                                        AppIcon::Delete.path()
                                                    } else {
                                                        AppIcon::TriangleAlert.path()
                                                    })
                                                    .size_5()
                                                    .text_color(if modal_state.is_ddl {
                                                        theme.danger
                                                    } else {
                                                        theme.warning
                                                    }),
                                            )
                                            .child(Text::body(message)),
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
                                                    .bg(theme.secondary)
                                                    .hover(move |d| d.bg(btn_hover))
                                                    .on_click(move |_, _, cx| {
                                                        sidebar_cancel.update(cx, |this, cx| {
                                                            this.cancel_modal_delete(cx);
                                                        });
                                                    })
                                                    .child(
                                                        Icon::new(AppIcon::X)
                                                            .size(px(16.0))
                                                            .muted(),
                                                    )
                                                    .child(
                                                        Text::caption("Cancel").muted_foreground(),
                                                    ),
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
                                                    .bg(theme.danger)
                                                    .hover(|d| d.opacity(0.9))
                                                    .on_click(move |_, _, cx| {
                                                        sidebar_confirm.update(cx, |this, cx| {
                                                            this.confirm_modal_delete(cx);
                                                        });
                                                    })
                                                    .child(
                                                        Icon::new(AppIcon::Delete)
                                                            .size(px(16.0))
                                                            .color(theme.background),
                                                    )
                                                    .child(
                                                        Text::caption(confirm_label)
                                                            .color(theme.background),
                                                    ),
                                            ),
                                    ),
                            ),
                    )
                },
            )
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use dbflux_components::composites::{
        PanelHeaderBackground, PanelHeaderTitleColor, PanelHeaderVariant, inspect_panel_header,
    };
    use dbflux_components::primitives::SurfaceRole;
    use dbflux_components::tokens::FontSizes;
    use dbflux_components::typography::AppFonts;
    use gpui::FontWeight;

    #[test]
    fn panel_headers_keep_mono_family_and_focus_weight_difference() {
        let focused = inspect_panel_header(PanelHeaderVariant::WorkspaceTasks, true, true, false);
        let unfocused =
            inspect_panel_header(PanelHeaderVariant::WorkspaceTasks, true, false, false);

        for inspection in [&focused.title, &unfocused.title] {
            assert_eq!(inspection.family, Some(AppFonts::MONO));
            assert_eq!(inspection.fallbacks, &[AppFonts::MONO_FALLBACK]);
            assert_eq!(inspection.size_override, Some(FontSizes::SM));
        }

        assert_eq!(focused.title.weight_override, Some(FontWeight::BOLD));
        assert_eq!(unfocused.title.weight_override, Some(FontWeight::MEDIUM));
    }

    #[test]
    fn workspace_render_uses_canonical_panel_header_contract() {
        let source = workspace_render_source();

        assert!(source.contains("panel_header_collapsible_variant("));
        assert!(source.contains("PanelHeaderVariant::WorkspaceTasks"));
        assert!(!source.contains("fn background_tasks_panel_header("));
        assert!(!source.contains("fn render_panel_header("));
        assert!(!source.contains("fn panel_header_title("));
    }

    #[test]
    fn workspace_render_drops_local_background_tasks_header_styling() {
        let source = workspace_render_source();

        assert!(!source.contains(".bg(theme.tab_bar)"));
        assert!(!source.contains(".hover(|s| s.bg(theme.secondary))"));
        assert!(!source.contains("theme.primary"));
    }

    #[test]
    fn workspace_render_keeps_loader_icon_in_the_tasks_header_contract() {
        let source = workspace_render_source();

        assert!(source.contains("Some(IconName::Loader)"));
    }

    #[test]
    fn workspace_tasks_panel_variant_matches_expected_shared_chrome() {
        let collapsed =
            inspect_panel_header(PanelHeaderVariant::WorkspaceTasks, true, false, false);

        assert_eq!(collapsed.background, PanelHeaderBackground::ThemeTabBar);
        assert_eq!(
            collapsed.hover_background,
            Some(PanelHeaderBackground::Surface(SurfaceRole::Card))
        );
        assert_eq!(
            collapsed.base_title_color,
            PanelHeaderTitleColor::Foreground
        );

        let focused = inspect_panel_header(PanelHeaderVariant::WorkspaceTasks, true, true, false);

        assert_eq!(
            focused.focus_title_color,
            Some(PanelHeaderTitleColor::Primary)
        );
        assert_eq!(focused.title.family, Some(AppFonts::MONO));
        assert_eq!(focused.title.size_override, Some(FontSizes::SM));
        assert_eq!(focused.title.weight_override, Some(FontWeight::BOLD));
    }

    #[test]
    fn tabbed_and_empty_workspace_paths_both_use_the_workspace_tasks_contract() {
        let invocations = background_tasks_header_invocations();

        assert_eq!(invocations.len(), 2);

        for invocation in invocations {
            assert!(invocation.contains("panel_header_collapsible_variant("));
            assert!(invocation.contains("PanelHeaderVariant::WorkspaceTasks"));
            assert!(invocation.contains("tasks_focused"));
            assert!(invocation.contains("Some(IconName::Loader)"));
        }
    }

    #[test]
    fn workspace_background_tasks_contract_stays_out_of_local_helper_code_paths() {
        let invocations = background_tasks_header_invocations();

        for invocation in invocations {
            assert!(!invocation.contains("theme.tab_bar"));
            assert!(!invocation.contains("theme.primary"));
        }
    }

    fn workspace_render_source() -> String {
        let source = fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/ui/views/workspace/render.rs"
        ))
        .expect("render.rs should be readable for source-inspection tests");

        source
            .split("#[cfg(test)]")
            .next()
            .expect("render.rs should contain production code before tests")
            .to_string()
    }

    fn background_tasks_header_invocations() -> Vec<String> {
        let source = workspace_render_source();
        let mut invocations = Vec::new();
        let mut remaining = source.as_str();

        while let Some(start) = remaining.find("panel_header_collapsible_variant(") {
            let tail = &remaining[start..];
            let end = tail
                .find(",\n                cx,\n            );")
                .map(|index| index + ",\n                cx,\n            );".len())
                .expect("workspace render should close the panel_header_collapsible_variant call");

            invocations.push(tail[..end].to_string());
            remaining = &tail[end..];
        }

        invocations
    }
}
