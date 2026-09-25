use super::*;
use crate::keymap::ContextId;
use dbflux_components::composites::collapsible_bar;
use dbflux_components::controls::Button;
use dbflux_components::modals::Modal;
use dbflux_components::modals::ModalVariant;
use dbflux_components::primitives::Text;
#[cfg(feature = "mcp")]
use dbflux_components::primitives::{SurfaceRole, surface};
#[cfg(feature = "mcp")]
use dbflux_components::tokens::Borders;
use dbflux_components::tokens::{HeaderMetrics, ShellMetrics};
use gpui_component::IconName;
use gpui_component::resizable::ResizablePanel;

/// Schedules `run` at the end of the current effect cycle instead of running
/// it inline (`Context::defer_in`). Commands that open native windows
/// (Settings, Connection Manager) must never be dispatched from inside
/// `Render::render`: opening a window while the workspace render pass is on
/// the stack nests a full window draw into it. The deferred callback runs at
/// most once; if the window closes before the cycle drains, the callback is
/// dropped rather than run.
fn defer_to_end_of_effect_cycle<T: 'static>(
    window: &Window,
    cx: &mut Context<T>,
    run: impl FnOnce(&mut T, &mut Window, &mut Context<T>) + 'static,
) {
    cx.defer_in(window, run);
}

/// Palette commands that open a separate native window. After these run, the
/// parent workspace must not steal focus back from the newly opened window;
/// the window activation inside the command owns the final focus.
fn palette_command_opens_native_window(command_id: &str) -> bool {
    matches!(command_id, "open_settings" | "open_connection_manager")
}

impl Workspace {
    /// Renders the active document from TabManager (v0.3).
    ///
    /// Routes through `TabManager::render_active` so that both `Legacy` and
    /// `Pane` tabs produce output. The old `active_document().map(render)` path
    /// returned `None` for `Pane` tabs, leaving `ChartDocument` with an empty
    /// canvas.
    fn render_active_document(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        self.tab_manager
            .update(cx, |mgr, cx| mgr.render_active(window, cx))
    }
}

impl Workspace {
    /// The background tasks dock under the document area: the collapsed bar,
    /// or, expanded, the tasks panel with its own header.
    fn render_tasks_dock(&self, cx: &mut Context<Self>) -> ResizablePanel {
        let tasks_expanded = self.tasks_state.is_expanded();
        let tasks_focused = self.focus_target == FocusTarget::BackgroundTasks;
        let collapsed_height = HeaderMetrics::BAR_HEIGHT;

        let content = if tasks_expanded {
            self.tasks_panel.clone().into_any_element()
        } else {
            let running_task_count = self.app_state.read(cx).tasks().running_tasks().len();
            let tasks_status: SharedString = if running_task_count == 0 {
                dbflux_i18n::t!("workspace.background_tasks_idle").into()
            } else {
                crate::ui::labels::tasks_running_label(running_task_count).into()
            };
            let workspace = cx.entity().clone();

            collapsible_bar(
                "panel-header-Background Tasks",
                dbflux_i18n::t!("workspace.background_tasks"),
                Some(tasks_status),
                true,
                tasks_focused,
                Some(IconName::Loader),
                move |_, _, app| {
                    workspace.update(app, |workspace, cx| {
                        workspace.toggle_tasks_panel(cx);
                    });
                },
                cx,
            )
            .into_any_element()
        };

        resizable_panel()
            .size(if tasks_expanded {
                ShellMetrics::TASKS_PANEL_HEIGHT
            } else {
                collapsed_height
            })
            .size_range(if tasks_expanded {
                px(80.0)..px(2000.0)
            } else {
                collapsed_height..collapsed_height
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
                                this.set_focus(FocusTarget::BackgroundTasks, window, cx);
                            }
                        }),
                    )
                    .child(content),
            )
    }
}

impl Render for Workspace {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(command_id) = self.pending_command.take() {
            // `take` before scheduling keeps dispatch at most once across
            // re-renders; the deferred callback runs after the render pass
            // returns, or not at all if the window closes first.
            let refocus_parent = !palette_command_opens_native_window(command_id);
            defer_to_end_of_effect_cycle(window, cx, move |this, window, cx| {
                this.handle_command(command_id, window, cx);
                if refocus_parent {
                    this.focus_handle.focus(window, cx);
                }
            });
        }

        // Handle SQL generated from sidebar (e.g., SELECT * FROM table)
        if let Some(sql) = self.pending_sql.take() {
            self.new_query_tab_with_content(sql, window, cx);
        }

        if let Some(target) = self.pending_focus.take() {
            self.set_focus(target, window, cx);
        }

        // The sidebar's inline delete confirmation is drawn below, so its
        // request to take focus is applied here, in the render that shows it.
        let inline_delete_focus = self
            .sidebar
            .read(cx)
            .delete_modal_state()
            .is_some()
            .then(|| {
                self.sidebar.update(cx, |sidebar, cx| {
                    sidebar.apply_delete_modal_focus(window, cx);
                    sidebar.delete_modal_focus_handle().clone()
                })
            });

        if let Some(pending) = self.pending_open_script.take() {
            self.finalize_open_script(pending, window, cx);
        }

        if let Some(pending) = self.pending_open_routine.take() {
            self.finalize_open_routine(pending, window, cx);
        }

        // Drains `BucketsTableDocument::pending_open_bucket` (Enter-on-row)
        // via the generic `Tab::take_pending_open_bucket` optional helper —
        // the workspace never branches on document type, it just polls the
        // active tab for the intent and opens the object browser on `Some`.
        let pending_bucket_open = {
            let active_id = self.tab_manager.read(cx).active_id();
            active_id.and_then(|id| {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.document(id).and_then(|tab| {
                        tab.take_pending_open_bucket(cx)
                            .map(|bucket| (tab.connection_id(cx), bucket))
                    })
                })
            })
        };
        if let Some((Some(profile_id), bucket)) = pending_bucket_open {
            self.open_object_browser(profile_id, bucket, window, cx);
        }

        // Same generic drain for "open this text object in its own editor
        // tab", raised by the object browser's preview header and its row
        // context menu.
        let pending_object_editor_open = {
            let active_id = self.tab_manager.read(cx).active_id();
            active_id.and_then(|id| {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.document(id).and_then(|tab| {
                        tab.take_pending_open_object_editor(cx)
                            .map(|request| (tab.connection_id(cx), request))
                    })
                })
            })
        };
        if let Some((Some(profile_id), request)) = pending_object_editor_open {
            self.open_object_editor(profile_id, request, window, cx);
        }

        if self.needs_focus_restore {
            self.needs_focus_restore = false;
            self.set_focus(self.focus_target, window, cx);
        }

        // Open the login modal on behalf of a settings-window auth-profile login.
        if let Some((provider_name, profile_name, url)) = self.pending_login_modal_open.take() {
            self.login_modal.update(cx, |modal, cx| {
                modal.open_manual(provider_name, profile_name, url, window, cx);
            });
        }

        let sidebar_dock = self.sidebar_dock.clone();
        let status_bar = self.status_bar.clone();
        // Toasts stack at the top right of the document area, never over the
        // sidebar or the status bar. They are deferred so they still paint
        // above modals, and hidden while the shutdown overlay is up.
        let toast_layer = (!self.app_state.read(cx).shutdown_phase().is_active())
            .then(|| deferred(self.toast_host.clone()));
        let command_palette = self.command_palette.clone();
        let login_modal = self.login_modal.clone();
        let sso_wizard = self.sso_wizard.clone();

        let has_tabs = !self.tab_manager.read(cx).is_empty();
        let active_doc_element = self.render_active_document(window, cx);
        let inspector_open = self.workspace_inspector.read(cx).is_open();
        let inspector_resizing = self.workspace_inspector.read(cx).is_resizing();
        let inspector_entity = self.workspace_inspector.clone();

        let theme = cx.theme().clone();
        let bg_color = theme.background;
        let sidebar_context_menu = self.sidebar.read(cx).context_menu_state().cloned();
        let tab_context_menu = self.tab_bar.read(cx).context_menu_state().cloned();
        let child_picker_open = self.sidebar.read(cx).has_child_picker_open();

        let title_bar = self.render_title_bar(window, cx).into_any_element();
        let rail = self.render_rail(cx).into_any_element();

        let document_area = if has_tabs {
            div()
                .id("document-content-row")
                .relative()
                .flex()
                .flex_row()
                .size_full()
                .overflow_hidden()
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
                })
                .when(inspector_open, |el| el.child(inspector_entity.clone()))
                .children(toast_layer)
                .into_any_element()
        } else {
            self.render_empty_workspace(cx)
                .children(toast_layer)
                .into_any_element()
        };

        // One resizable state per tasks-dock mode: the panel group keeps the
        // sizes it laid out, so reusing one state would reopen the expanded
        // dock at the collapsed bar's height instead of its default.
        let panels_id = if self.tasks_state.is_expanded() {
            "main-panels-tasks-expanded"
        } else {
            "main-panels"
        };

        let right_pane = v_resizable(panels_id)
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
                            .bg(theme.popover)
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
                            .child(document_area),
                    ),
            )
            .child(self.render_tasks_dock(cx));

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
                this.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::FocusUp, window, cx);
                });
            }))
            .on_action(cx.listener(|this, _: &keymap::FocusResults, window, cx| {
                this.set_focus(FocusTarget::Document, window, cx);
                this.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::FocusDown, window, cx);
                });
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
                this.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::RunQuery, window, cx);
                });
            }))
            .on_action(cx.listener(|this, _: &keymap::Cancel, window, cx| {
                if !this.dispatch(Command::Cancel, window, cx) {
                    cx.propagate();
                }
            }))
            .on_action(cx.listener(|this, _: &keymap::ExportResults, window, cx| {
                this.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::ExportResults, window, cx);
                });
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
                this.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::ToggleEditor, window, cx);
                });
            }))
            .on_action(cx.listener(|this, _: &keymap::ToggleResults, window, cx| {
                this.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::ToggleResults, window, cx);
                });
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
                this.tab_manager.update(cx, |mgr, cx| {
                    mgr.dispatch_active(Command::SaveFileAs, window, cx);
                });
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
                    .child(title_bar)
                    .child(
                        div()
                            .flex()
                            .flex_row()
                            .flex_1()
                            .overflow_hidden()
                            .child(rail)
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
            // S8 modals — rendered as full-screen overlays using the shared `Modal` chrome.
            .when(self.modal_delete_connection.read(cx).is_visible(), |root| {
                root.child(self.modal_delete_connection.clone())
            })
            .when(self.modal_unsaved_changes.read(cx).is_visible(), |root| {
                root.child(self.modal_unsaved_changes.clone())
            })
            .when(self.modal_drop_table.read(cx).is_visible(), |root| {
                root.child(self.modal_drop_table.clone())
            })
            .when(self.modal_tunnel_auth.read(cx).is_visible(), |root| {
                root.child(self.modal_tunnel_auth.clone())
            })
            .when(self.modal_import_dashboard.read(cx).is_visible(), |root| {
                root.child(self.modal_import_dashboard.clone())
            })
            .when(self.import_wizard.read(cx).is_visible(), |root| {
                root.child(self.import_wizard.clone())
            })
            .when(self.migrate_wizard.read(cx).is_visible(), |root| {
                root.child(self.migrate_wizard.clone())
            })
            .when(self.export_wizard.read(cx).is_visible(), |root| {
                root.child(self.export_wizard.clone())
            })
            .when(self.modal_create_dashboard.read(cx).is_visible(), |root| {
                root.child(self.modal_create_dashboard.clone())
            })
            .when(self.modal_rename_item.read(cx).is_visible(), |root| {
                root.child(self.modal_rename_item.clone())
            })
            .when(self.modal_delete_dashboard.read(cx).is_visible(), |root| {
                root.child(self.modal_delete_dashboard.clone())
            })
            .when(
                self.modal_delete_saved_chart.read(cx).is_visible(),
                |root| root.child(self.modal_delete_saved_chart.clone()),
            )
            .when(self.modal_add_panel.read(cx).is_visible(), |root| {
                root.child(self.modal_add_panel.clone())
            })
            .when(self.export_modal.read(cx).is_visible(), |root| {
                root.child(self.export_modal.clone())
            })
            .when(self.welcome_dialog.read(cx).is_visible(), |root| {
                root.child(self.welcome_dialog.clone())
            })
            .when(self.whats_new_dialog.read(cx).is_visible(), |root| {
                root.child(self.whats_new_dialog.clone())
            })
            // Last of the modals so a quit prompt sits above any dialog that
            // was already open.
            .when(self.modal_active_query.read(cx).is_visible(), |root| {
                root.child(self.modal_active_query.clone())
            })
            // Drag mask — rendered only while inspector grip is being dragged.
            // Sits above document/inspector content so cursor tracking works
            // anywhere on screen, but below toast host and shutdown overlay.
            .when(inspector_resizing, |root| {
                root.child(
                    div()
                        .id("workspace-inspector-drag-mask")
                        .absolute()
                        .inset_0()
                        .cursor_col_resize()
                        .on_mouse_move(cx.listener(move |this, event: &MouseMoveEvent, _, cx| {
                            this.workspace_inspector.update(cx, |insp, cx| {
                                insp.update_resize(event.position.x, cx);
                            });
                        }))
                        .on_mouse_up(
                            MouseButton::Left,
                            cx.listener(move |this, _, _, cx| {
                                this.workspace_inspector.update(cx, |insp, cx| {
                                    insp.finish_resize(cx);
                                });
                            }),
                        ),
                )
            })
            // Shutdown overlay (rendered above everything during shutdown)
            .child(self.shutdown_overlay.clone())
            .when(cfg!(feature = "mcp"), |root| {
                #[cfg(feature = "mcp")]
                {
                    root.when_some(self.active_governance_panel, |root, panel| {
                        let workspace_for_backdrop = cx.entity().clone();

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
                                .bg(theme.overlay.opacity(0.45))
                                .flex()
                                .items_center()
                                .justify_center()
                                .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                                    cx.stop_propagation();
                                    workspace_for_backdrop.update(cx, |workspace, cx| {
                                        workspace.close_governance_panel(window, cx);
                                    });
                                })
                                .child(
                                    surface(SurfaceRole::Modal, cx)
                                        .w(px(1080.0))
                                        .h(px(680.0))
                                        .flex()
                                        .flex_col()
                                        .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                            cx.stop_propagation();
                                        })
                                        .child(
                                            div()
                                                .flex_1()
                                                .min_h_0()
                                                .p(Borders::THIN)
                                                .child(content),
                                        ),
                                ),
                        )
                    })
                }
                #[cfg(not(feature = "mcp"))]
                {
                    root
                }
            })
            .when(child_picker_open, |root| {
                let sidebar_entity = self.sidebar.clone();
                let focus_handle = self
                    .sidebar
                    .read(cx)
                    .child_picker_focus_handle()
                    .unwrap_or_else(|| self.focus_handle.clone());
                let content = self.sidebar.update(cx, |sidebar, cx| {
                    sidebar.render_child_picker_content(cx).into_any_element()
                });

                root.child(
                    Modal::new(dbflux_i18n::t!("workspace.event_streams"))
                        .id("event-stream-child-picker")
                        .focus_handle(&focus_handle)
                        .on_close(move |_window, cx| {
                            sidebar_entity.update(cx, |sidebar, cx| {
                                sidebar.close_child_picker(cx);
                            });
                        })
                        .key_context(ContextId::EventStreamsPicker.as_gpui_context())
                        .icon(AppIcon::ScrollText)
                        .width(px(1000.0))
                        .height(px(720.0))
                        .top_offset(px(60.0))
                        .block_scroll()
                        .child(content)
                        .into_any_element(),
                )
            })
            // Context menu rendered at workspace level for proper positioning
            .when_some(sidebar_context_menu, |this, menu| {
                use crate::ui::components::context_menu as ctx;
                use dbflux_ui_sidebar::ContextMenuItem;

                let sidebar_entity = self.sidebar.clone();

                let menu_x = menu.position.x;
                let menu_y = menu.position.y;
                let menu_width = px(160.0);
                let menu_gap = Spacing::XS;
                let menu_item_height = Heights::ROW_COMPACT;
                let separator_height = px(1.0) + Spacing::XS * 2.0;
                let menu_container_padding = px(4.0);

                let parent_entry = menu.parent_stack.last();

                // Anchor the submenu to its owning row by summing the actual
                // rendered height of every preceding row. Separators are ~9px,
                // not a full ROW_COMPACT, so a flat `height * index` over-counts
                // and pushes the submenu too far down.
                let submenu_y_offset = if let Some((parent_items, parent_selected)) = parent_entry {
                    let rows_above =
                        parent_items
                            .iter()
                            .take(*parent_selected)
                            .fold(px(0.0), |acc, item| {
                                acc + if item.is_separator {
                                    separator_height
                                } else {
                                    menu_item_height
                                }
                            });

                    menu_container_padding + rows_above
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

                        d.child(
                            deferred(
                                anchored()
                                    .position(point(menu_x, menu_y))
                                    .snap_to_window()
                                    .child(ctx::render_menu_container(
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
                                    )),
                            )
                            .with_priority(1),
                        )
                    })
                    // Current menu (submenu to the right of parent, or main menu at click position)
                    .child({
                        let shared_items = ContextMenuItem::to_menu_items(&menu.items);
                        let sidebar_click = sidebar_entity.clone();
                        let sidebar_hover = sidebar_entity.clone();

                        let menu_left = if in_submenu {
                            menu_x + menu_width + menu_gap
                        } else {
                            menu_x
                        };
                        let menu_top = menu_y + submenu_y_offset;

                        deferred(
                            anchored()
                                .position(point(menu_left, menu_top))
                                .snap_to_window()
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
                                )),
                        )
                        .with_priority(1)
                    })
            })
            // Tab context menu rendered at workspace level for proper positioning
            .when_some(tab_context_menu, |this, menu| {
                use crate::ui::components::context_menu as ctx;
                use crate::ui::document::tab_bar::TabBar;

                let tab_bar_entity = self.tab_bar.clone();

                let menu_x = menu.position_x;
                let menu_y = ShellMetrics::TITLE_BAR_HEIGHT;
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
                self.sidebar
                    .read(cx)
                    .delete_modal_state()
                    .zip(inline_delete_focus),
                |el, (modal_state, focus_handle)| {
                    // Capture sidebar clones for each callback before building the footer.
                    let sidebar_confirm = self.sidebar.clone();
                    let sidebar_cancel = self.sidebar.clone();
                    let sidebar_close = self.sidebar.clone();
                    let sidebar_enter = self.sidebar.clone();

                    let title = if modal_state.multi_count.is_some() {
                        dbflux_i18n::t!("workspace.action.delete")
                    } else if modal_state.is_ddl {
                        dbflux_i18n::t!("workspace.action.drop")
                    } else if modal_state.is_folder {
                        dbflux_i18n::t!("workspace.action.delete_folder")
                    } else {
                        dbflux_i18n::t!("workspace.action.delete_connection")
                    };

                    let message = if let Some(count) = modal_state.multi_count {
                        crate::ui::labels::workspace_delete_selected_message(count)
                    } else if modal_state.is_ddl {
                        crate::ui::labels::workspace_drop_object_message(
                            modal_state.object_type,
                            modal_state.item_name,
                        )
                    } else if modal_state.is_folder {
                        crate::ui::labels::workspace_delete_folder_message(modal_state.item_name)
                    } else {
                        crate::ui::labels::workspace_delete_connection_message(
                            modal_state.item_name,
                        )
                    };

                    let confirm_label = if modal_state.is_ddl {
                        dbflux_i18n::t!("workspace.action.drop")
                    } else {
                        dbflux_i18n::t!("workspace.action.delete")
                    };
                    let variant = if modal_state.is_ddl {
                        ModalVariant::Danger
                    } else {
                        ModalVariant::Default
                    };

                    let body = Text::body(message).into_any_element();

                    let footer = div()
                        .flex()
                        .gap(Spacing::SM)
                        .child(
                            Button::new(
                                "delete-cancel",
                                dbflux_i18n::t!("workspace.action.cancel"),
                            )
                            .on_click(move |_, _, cx| {
                                sidebar_cancel.update(cx, |this, cx| {
                                    this.cancel_modal_delete(cx);
                                });
                            }),
                        )
                        .child(
                            Button::new("delete-confirm", confirm_label)
                                .when(modal_state.is_ddl, |b| b.danger())
                                .on_click(move |_, _, cx| {
                                    sidebar_confirm.update(cx, |this, cx| {
                                        this.confirm_modal_delete(cx);
                                    });
                                }),
                        )
                        .into_any_element();

                    el.child(
                        Modal::new(title)
                            .body(body)
                            .footer(footer)
                            .icon(AppIcon::Delete)
                            .width(px(360.0))
                            .variant(variant)
                            .focus_handle(&focus_handle)
                            .on_close(move |_, cx| {
                                sidebar_close.update(cx, |this, cx| {
                                    this.cancel_modal_delete(cx);
                                });
                            })
                            .on_confirm(move |_, cx| {
                                sidebar_enter.update(cx, |this, cx| {
                                    this.confirm_modal_delete(cx);
                                });
                            }),
                    )
                },
            )
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::fs;
    use std::rc::Rc;

    use gpui::{Context, IntoElement, Render, TestAppContext, VisualTestContext, Window, div};

    use super::{defer_to_end_of_effect_cycle, palette_command_opens_native_window};

    #[test]
    fn workspace_render_uses_canonical_panel_header_contract() {
        let source = workspace_render_source();

        assert!(source.contains("collapsible_bar("));
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
    fn tabbed_and_empty_workspace_paths_share_one_tasks_dock() {
        let source = workspace_render_source();
        let invocations = background_tasks_header_invocations();

        assert_eq!(invocations.len(), 1);
        assert_eq!(source.matches("self.render_tasks_dock(cx)").count(), 1);

        for invocation in invocations {
            assert!(invocation.contains("collapsible_bar("));
            assert!(invocation.contains("tasks_focused"));
            assert!(invocation.contains("Some(IconName::Loader)"));
        }
    }

    #[test]
    fn document_area_draws_no_focus_ring_of_its_own() {
        let source = workspace_render_source();
        let start = source
            .find(".id(\"document-area\")")
            .expect("workspace render must draw the document area");
        let area = &source[start..start + 1200];

        assert!(!area.contains("focus_ring"));
        assert!(!area.contains("ChamferRing"));
        assert!(!area.contains("border_color"));
    }

    #[test]
    fn workspace_background_tasks_contract_stays_out_of_local_helper_code_paths() {
        let invocations = background_tasks_header_invocations();

        for invocation in invocations {
            assert!(!invocation.contains("theme.tab_bar"));
            assert!(!invocation.contains("theme.primary"));
        }
    }

    #[test]
    fn native_window_palette_commands_skip_the_parent_refocus() {
        assert!(palette_command_opens_native_window("open_settings"));
        assert!(palette_command_opens_native_window(
            "open_connection_manager"
        ));
    }

    #[test]
    fn in_window_palette_commands_keep_the_parent_refocus() {
        for command_id in [
            "new_query_tab",
            "open_audit_viewer",
            "open_login_modal",
            "focus_sidebar",
        ] {
            assert!(
                !palette_command_opens_native_window(command_id),
                "`{command_id}` runs inside the workspace window and must keep \
                 the parent refocus"
            );
        }
    }

    #[test]
    fn workspace_render_schedules_pending_palette_commands_outside_the_render_pass() {
        let source = workspace_render_source();

        let start = source
            .find("self.pending_command.take()")
            .expect("workspace render must consume pending_command");
        let branch_end = source[start..]
            .find("self.pending_sql.take()")
            .expect("workspace render should continue after pending_command");
        let branch = &source[start..start + branch_end];

        assert!(
            branch.contains("defer_to_end_of_effect_cycle("),
            "pending commands must be scheduled for the end of the effect \
             cycle, never dispatched while the render pass is on the stack"
        );
        assert!(
            !branch.contains("self.handle_command("),
            "workspace render must not dispatch commands inline"
        );
        assert!(
            branch.contains("palette_command_opens_native_window"),
            "native-window commands must not steal focus back from the newly \
             opened window"
        );
    }

    struct DeferredDispatchProbe {
        runs: Rc<Cell<usize>>,
    }

    impl Render for DeferredDispatchProbe {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
        }
    }

    /// Exercises the production deferral helper: work scheduled through
    /// `defer_to_end_of_effect_cycle` must not run while the current effect
    /// cycle is on the stack, and must run exactly once when the cycle
    /// flushes. This is the scheduling behavior the palette-command fix
    /// relies on to keep `open_window` (which draws synchronously) out of
    /// the workspace render pass.
    #[gpui::test]
    fn deferred_palette_dispatch_runs_once_after_the_effect_cycle_flushes(cx: &mut TestAppContext) {
        let runs = Rc::new(Cell::new(0usize));
        let handle = cx.add_window(|_, _| DeferredDispatchProbe { runs: runs.clone() });
        let cx = &mut VisualTestContext::from_window(handle.into(), cx);

        handle
            .update(cx, |probe, window, cx| {
                defer_to_end_of_effect_cycle(
                    window,
                    cx,
                    |probe: &mut DeferredDispatchProbe, _, _| {
                        probe.runs.set(probe.runs.get() + 1);
                    },
                );
                assert_eq!(
                    probe.runs.get(),
                    0,
                    "deferred work must not run while the effect cycle is on the stack"
                );
            })
            .expect("probe window update");

        cx.run_until_parked();

        handle
            .update(cx, |probe, _window, _cx| {
                assert_eq!(
                    probe.runs.get(),
                    1,
                    "deferred work must run exactly once after flush"
                );
            })
            .expect("probe window update");

        cx.run_until_parked();

        handle
            .update(cx, |probe, _window, _cx| {
                assert_eq!(
                    probe.runs.get(),
                    1,
                    "a second flush must not re-run deferred work"
                );
            })
            .expect("probe window update");
    }

    /// Closing the parent window before the effect cycle drains drops the
    /// deferred callback instead of running it: the dispatch is at most
    /// once, not unconditionally exactly once.
    #[gpui::test]
    fn closing_the_window_before_the_deferred_dispatch_drains_drops_it(cx: &mut TestAppContext) {
        let runs = Rc::new(Cell::new(0usize));
        let handle = cx.add_window(|_, _| DeferredDispatchProbe { runs: runs.clone() });
        let cx = &mut VisualTestContext::from_window(handle.into(), cx);

        handle
            .update(cx, |_, window, cx| {
                defer_to_end_of_effect_cycle(
                    window,
                    cx,
                    |probe: &mut DeferredDispatchProbe, _, _| {
                        probe.runs.set(probe.runs.get() + 1);
                    },
                );
                window.remove_window();
            })
            .expect("probe window update");

        cx.run_until_parked();

        assert_eq!(
            runs.get(),
            0,
            "a deferred dispatch must be dropped when the window closes \
             before the effect cycle drains"
        );
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

        while let Some(start) = remaining.find("collapsible_bar(") {
            let tail = &remaining[start..];
            let end = tail
                .find("\n                cx,\n")
                .map(|index| index + "\n                cx,\n".len())
                .expect("workspace render should close the collapsible_bar call");

            invocations.push(tail[..end].to_string());
            remaining = &tail[end..];
        }

        invocations
    }
}

#[cfg(test)]
mod inline_delete_keyboard_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use crate::ui::views::workspace::Workspace;
    use dbflux_core::SchemaNodeId;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::modals::test_host::{click_backdrop, has_focus};
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::cell::RefCell;
    use std::rc::Rc;
    use uuid::Uuid;

    struct Harness<'a> {
        workspace: Entity<Workspace>,
        app_state: Entity<AppStateEntity>,
        folder_id: Uuid,
        window: &'a mut VisualTestContext,
    }

    /// Asks to delete a connection folder from a focused workspace, which
    /// opens the sidebar's inline delete confirmation.
    fn open_confirmation(cx: &mut TestAppContext) -> Harness<'_> {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);

        let app_state: Entity<AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
            })
        });
        let folder_id =
            cx.update(|cx| app_state.update(cx, |state, _| state.create_folder("Staging", None)));

        let holder: Rc<RefCell<Option<Entity<Workspace>>>> = Rc::default();
        let (_, window) = cx.add_window_view({
            let holder = holder.clone();
            let app_state = app_state.clone();
            move |window, cx| {
                let workspace = cx.new(|cx| Workspace::new(app_state, window, cx));
                holder.replace(Some(workspace.clone()));
                gpui_component::Root::new(workspace, window, cx)
            }
        });
        let workspace = holder.borrow().clone().expect("workspace created");

        window.update(|window, cx| {
            let (focus_handle, sidebar) = {
                let workspace = workspace.read(cx);
                (workspace.focus_handle.clone(), workspace.sidebar.clone())
            };
            focus_handle.focus(window, cx);

            let item_id = SchemaNodeId::ConnectionFolder { node_id: folder_id }.to_string();
            sidebar.update(cx, |sidebar, cx| {
                sidebar.show_delete_confirm_modal(&item_id, cx);
            });
        });
        window.run_until_parked();

        Harness {
            workspace,
            app_state,
            folder_id,
            window,
        }
    }

    impl Harness<'_> {
        fn is_open(&mut self) -> bool {
            let workspace = self.workspace.clone();
            self.window
                .update(|_, cx| workspace.read(cx).sidebar.read(cx).has_delete_modal())
        }

        fn folder_exists(&mut self) -> bool {
            let (app_state, folder_id) = (self.app_state.clone(), self.folder_id);
            self.window.update(|_, cx| {
                app_state
                    .read(cx)
                    .connection_tree()
                    .find_by_id(folder_id)
                    .is_some()
            })
        }

        fn workspace_has_focus(&mut self) -> bool {
            let handle = self
                .window
                .update(|_, cx| self.workspace.read(cx).focus_handle.clone());
            has_focus(self.window, &handle)
        }
    }

    #[gpui::test]
    fn enter_deletes_the_folder(cx: &mut TestAppContext) {
        let mut harness = open_confirmation(cx);
        assert!(harness.is_open());

        harness.window.simulate_keystrokes("enter");

        assert!(!harness.is_open());
        assert!(!harness.folder_exists());
    }

    #[gpui::test]
    fn escape_keeps_the_folder_and_gives_focus_back(cx: &mut TestAppContext) {
        let mut harness = open_confirmation(cx);

        harness.window.simulate_keystrokes("escape");

        assert!(!harness.is_open());
        assert!(harness.folder_exists());
        assert!(harness.workspace_has_focus());
    }

    impl Harness<'_> {
        /// Moves focus back to the workspace while the confirmation stays
        /// open, as when something else takes focus behind it.
        fn focus_workspace(&mut self) {
            let workspace = self.workspace.clone();
            self.window.update(|window, cx| {
                let handle = workspace.read(cx).focus_handle.clone();
                handle.focus(window, cx);
            });
            self.window.run_until_parked();
            assert!(self.is_open());
        }
    }

    #[gpui::test]
    fn enter_deletes_the_folder_when_focus_is_outside_the_confirmation(cx: &mut TestAppContext) {
        let mut harness = open_confirmation(cx);
        harness.focus_workspace();

        harness.window.simulate_keystrokes("enter");

        assert!(!harness.is_open());
        assert!(!harness.folder_exists());
    }

    #[gpui::test]
    fn escape_keeps_the_folder_when_focus_is_outside_the_confirmation(cx: &mut TestAppContext) {
        let mut harness = open_confirmation(cx);
        harness.focus_workspace();

        harness.window.simulate_keystrokes("escape");

        assert!(!harness.is_open());
        assert!(harness.folder_exists());
    }

    #[gpui::test]
    fn a_backdrop_click_keeps_the_folder(cx: &mut TestAppContext) {
        let mut harness = open_confirmation(cx);

        click_backdrop(harness.window);

        assert!(!harness.is_open());
        assert!(harness.folder_exists());
    }
}
