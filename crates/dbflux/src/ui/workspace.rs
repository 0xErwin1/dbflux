use crate::app::{AppState, AppStateChanged};
use crate::keymap::{
    self, Command, CommandDispatcher, ContextId, FocusTarget, KeyChord, KeymapStack, default_keymap,
};
use crate::ui::command_palette::{
    CommandExecuted, CommandPalette, CommandPaletteClosed, PaletteCommand,
};
use crate::ui::editor::EditorPane;
use crate::ui::results::{ResultsPane, ResultsReceived};
use crate::ui::sidebar::Sidebar;
use crate::ui::status_bar::{StatusBar, ToggleTasksPanel};
use crate::ui::tasks_panel::TasksPanel;
use crate::ui::toast::ToastManager;
use crate::ui::windows::connection_manager::ConnectionManagerWindow;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Root;
use gpui_component::notification::NotificationList;
use gpui_component::resizable::{h_resizable, resizable_panel, v_resizable};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PanelState {
    Expanded,
    Collapsed,
}

impl PanelState {
    fn is_expanded(self) -> bool {
        self == PanelState::Expanded
    }

    fn toggle(&mut self) {
        *self = match self {
            PanelState::Expanded => PanelState::Collapsed,
            PanelState::Collapsed => PanelState::Expanded,
        };
    }
}

pub struct Workspace {
    app_state: Entity<AppState>,
    sidebar: Entity<Sidebar>,
    editor: Entity<EditorPane>,
    results: Entity<ResultsPane>,
    status_bar: Entity<StatusBar>,
    tasks_panel: Entity<TasksPanel>,
    notification_list: Entity<NotificationList>,
    command_palette: Entity<CommandPalette>,

    editor_state: PanelState,
    results_state: PanelState,
    tasks_state: PanelState,
    pending_command: Option<&'static str>,
    needs_focus_restore: bool,

    focus_target: FocusTarget,
    keymap: KeymapStack,
    focus_handle: FocusHandle,
}

impl Workspace {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        ToastManager::init(window, cx);

        let results = cx.new(|cx| ResultsPane::new(app_state.clone(), window, cx));
        let editor = cx.new(|cx| EditorPane::new(app_state.clone(), results.clone(), window, cx));
        let sidebar = cx.new(|cx| {
            Sidebar::new(
                app_state.clone(),
                editor.clone(),
                results.clone(),
                window,
                cx,
            )
        });
        let status_bar = cx.new(|cx| StatusBar::new(app_state.clone(), window, cx));
        let tasks_panel = cx.new(|cx| TasksPanel::new(app_state.clone(), window, cx));
        let notification_list = ToastManager::notification_list(cx);

        let command_palette = cx.new(|cx| {
            let mut palette = CommandPalette::new(window, cx);
            palette.register_commands(Self::default_commands());
            palette
        });

        cx.subscribe(&status_bar, |this, _, _: &ToggleTasksPanel, cx| {
            this.toggle_tasks_panel(cx);
        })
        .detach();

        cx.subscribe(&results, |this, _, _: &ResultsReceived, cx| {
            this.on_results_received(cx);
        })
        .detach();

        cx.subscribe(&command_palette, |this, _, event: &CommandExecuted, cx| {
            this.pending_command = Some(event.command_id);
            cx.notify();
        })
        .detach();

        cx.subscribe(&command_palette, |this, _, _: &CommandPaletteClosed, cx| {
            this.needs_focus_restore = true;
            cx.notify();
        })
        .detach();

        let focus_handle = cx.focus_handle();
        focus_handle.focus(window);

        Self {
            app_state,
            sidebar,
            editor,
            results,
            status_bar,
            tasks_panel,
            notification_list,
            command_palette,
            editor_state: PanelState::Expanded,
            results_state: PanelState::Expanded,
            tasks_state: PanelState::Collapsed,
            pending_command: None,
            needs_focus_restore: false,
            focus_target: FocusTarget::default(),
            keymap: default_keymap(),
            focus_handle,
        }
    }

    fn default_commands() -> Vec<PaletteCommand> {
        vec![
            PaletteCommand::new("new_query_tab", "New Query Tab", "Editor"),
            PaletteCommand::new("run_query", "Run Query", "Editor").with_shortcut("Ctrl+Enter"),
            PaletteCommand::new("export_results", "Export Results to CSV", "Results"),
            PaletteCommand::new(
                "open_connection_manager",
                "Open Connection Manager",
                "Connections",
            ),
            PaletteCommand::new("disconnect", "Disconnect Current", "Connections"),
            PaletteCommand::new("refresh_schema", "Refresh Schema", "Connections"),
            PaletteCommand::new("toggle_editor", "Toggle Editor Panel", "View"),
            PaletteCommand::new("toggle_results", "Toggle Results Panel", "View"),
            PaletteCommand::new("toggle_tasks", "Toggle Tasks Panel", "View"),
        ]
    }

    fn active_context(&self, cx: &Context<Self>) -> ContextId {
        if self.command_palette.read(cx).is_visible() {
            return ContextId::CommandPalette;
        }
        self.focus_target.to_context()
    }

    pub fn set_focus(&mut self, target: FocusTarget, _window: &mut Window, cx: &mut Context<Self>) {
        log::debug!("Focus changed to: {:?}", target);
        self.focus_target = target;

        self.sidebar.update(cx, |sidebar, cx| {
            sidebar.set_connections_focused(target == FocusTarget::Sidebar, cx);
            sidebar.set_history_focused(target == FocusTarget::History, cx);
        });

        cx.notify();
    }

    fn handle_command(&mut self, command_id: &str, window: &mut Window, cx: &mut Context<Self>) {
        match command_id {
            "new_query_tab" => {
                self.editor.update(cx, |editor, cx| {
                    editor.add_new_tab(window, cx);
                });
            }
            "run_query" => {
                self.editor.update(cx, |editor, cx| {
                    editor.run_query(window, cx);
                });
            }
            "export_results" => {
                self.results.update(cx, |results, cx| {
                    results.export_results(window, cx);
                });
            }
            "open_connection_manager" => {
                let app_state = self.app_state.clone();
                cx.spawn(async move |_this, cx| {
                    cx.update(|cx| {
                        let bounds = Bounds::centered(None, size(px(700.0), px(650.0)), cx);
                        cx.open_window(
                            WindowOptions {
                                titlebar: Some(TitlebarOptions {
                                    title: Some("Connection Manager".into()),
                                    ..Default::default()
                                }),
                                window_bounds: Some(WindowBounds::Windowed(bounds)),
                                kind: WindowKind::Floating,
                                ..Default::default()
                            },
                            |window, cx| {
                                let manager =
                                    cx.new(|cx| ConnectionManagerWindow::new(app_state, window, cx));
                                cx.new(|cx| Root::new(manager, window, cx))
                            },
                        )
                        .ok();
                    })
                    .ok();
                })
                .detach();
            }
            "disconnect" => {
                self.disconnect_active(window, cx);
            }
            "refresh_schema" => {
                self.refresh_schema(window, cx);
            }
            "toggle_editor" => {
                self.toggle_editor(cx);
            }
            "toggle_results" => {
                self.toggle_results(cx);
            }
            "toggle_tasks" => {
                self.toggle_tasks_panel(cx);
            }
            _ => {
                log::warn!("Unknown command: {}", command_id);
            }
        }
    }

    fn open_connection_manager(&self, cx: &mut Context<Self>) {
        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(700.0), px(650.0)), cx);

        cx.open_window(
            WindowOptions {
                titlebar: Some(TitlebarOptions {
                    title: Some("Connection Manager".into()),
                    ..Default::default()
                }),
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                kind: WindowKind::Floating,
                ..Default::default()
            },
            |window, cx| {
                let manager = cx.new(|cx| ConnectionManagerWindow::new(app_state, window, cx));
                cx.new(|cx| Root::new(manager, window, cx))
            },
        )
        .ok();
    }

    fn disconnect_active(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::toast::ToastExt;

        let profile_id = self.app_state.read(cx).active_connection_id;

        if let Some(id) = profile_id {
            let name = self
                .app_state
                .read(cx)
                .connections
                .get(&id)
                .map(|c| c.profile.name.clone());

            self.app_state.update(cx, |state, cx| {
                state.disconnect(id);
                cx.emit(AppStateChanged);
            });

            if let Some(name) = name {
                cx.toast_info(format!("Disconnected from {}", name), window);
            }
        }
    }

    fn refresh_schema(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::toast::ToastExt;

        let active = self.app_state.read(cx).active_connection();

        let Some(active) = active else {
            cx.toast_warning("No active connection", window);
            return;
        };

        let conn = active.connection.clone();
        let profile_id = active.profile.id;
        let app_state = self.app_state.clone();

        let task = cx.background_executor().spawn(async move { conn.schema() });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| match result {
                Ok(schema) => {
                    app_state.update(cx, |state, cx| {
                        if let Some(connected) = state.connections.get_mut(&profile_id) {
                            connected.schema = Some(schema);
                        }
                        cx.emit(AppStateChanged);
                    });
                }
                Err(e) => {
                    log::error!("Failed to refresh schema: {:?}", e);
                }
            })
            .ok();
        })
        .detach();

        cx.toast_info("Refreshing schema...", window);
    }

    pub fn toggle_command_palette(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let was_visible = self.command_palette.read(cx).is_visible();
        self.command_palette.update(cx, |palette, cx| {
            palette.toggle(window, cx);
        });

        if was_visible {
            self.focus_handle.focus(window);
        }
    }

    pub fn toggle_tasks_panel(&mut self, cx: &mut Context<Self>) {
        self.tasks_state.toggle();
        cx.notify();
    }

    pub fn toggle_editor(&mut self, cx: &mut Context<Self>) {
        self.editor_state.toggle();
        cx.notify();
    }

    pub fn toggle_results(&mut self, cx: &mut Context<Self>) {
        self.results_state.toggle();
        cx.notify();
    }

    fn on_results_received(&mut self, cx: &mut Context<Self>) {
        if !self.results_state.is_expanded() {
            self.results_state = PanelState::Expanded;
            cx.notify();
        }
    }

    fn render_panel_header(
        &self,
        title: &'static str,
        is_expanded: bool,
        is_focused: bool,
        on_toggle: impl Fn(&mut Self, &mut Context<Self>) + 'static,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let theme = cx.theme();
        let chevron = if is_expanded { "▼" } else { "▶" };

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
                    .child(chevron)
                    .child(title),
            )
    }
}

impl Render for Workspace {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(command_id) = self.pending_command.take() {
            self.handle_command(command_id, window, cx);
            self.focus_handle.focus(window);
        }

        if self.needs_focus_restore {
            self.needs_focus_restore = false;
            self.focus_handle.focus(window);
        }

        let sidebar = self.sidebar.clone();
        let editor = self.editor.clone();
        let results = self.results.clone();
        let status_bar = self.status_bar.clone();
        let tasks_panel = self.tasks_panel.clone();
        let notification_list = self.notification_list.clone();
        let command_palette = self.command_palette.clone();

        let editor_expanded = self.editor_state.is_expanded();
        let results_expanded = self.results_state.is_expanded();
        let tasks_expanded = self.tasks_state.is_expanded();

        let editor_focused = self.focus_target == FocusTarget::Editor;
        let results_focused = self.focus_target == FocusTarget::Results;
        let tasks_focused = self.focus_target == FocusTarget::BackgroundTasks;

        let theme = cx.theme();
        let bg_color = theme.background;

        let editor_header = self.render_panel_header(
            "Editor",
            editor_expanded,
            editor_focused,
            Self::toggle_editor,
            cx,
        );
        let results_header = self.render_panel_header(
            "Results",
            results_expanded,
            results_focused,
            Self::toggle_results,
            cx,
        );
        let tasks_header = self.render_panel_header(
            "Background Tasks",
            tasks_expanded,
            tasks_focused,
            Self::toggle_tasks_panel,
            cx,
        );

        let header_size = px(25.0);

        let right_pane = v_resizable("main-panels")
            .child(
                resizable_panel()
                    .size(if editor_expanded {
                        px(300.0)
                    } else {
                        header_size
                    })
                    .size_range(if editor_expanded {
                        px(100.0)..px(2000.0)
                    } else {
                        header_size..header_size
                    })
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .size_full()
                            .child(editor_header)
                            .when(editor_expanded, |el| {
                                el.child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .flex_1()
                                        .overflow_hidden()
                                        .child(editor),
                                )
                            }),
                    ),
            )
            .child(
                resizable_panel()
                    .size(if results_expanded {
                        px(300.0)
                    } else {
                        header_size
                    })
                    .size_range(if results_expanded {
                        px(100.0)..px(2000.0)
                    } else {
                        header_size..header_size
                    })
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .size_full()
                            .child(results_header)
                            .when(results_expanded, |el| {
                                el.child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .flex_1()
                                        .overflow_hidden()
                                        .child(results),
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
                            .flex()
                            .flex_col()
                            .size_full()
                            .child(tasks_header)
                            .when(tasks_expanded, |el| {
                                el.child(div().flex_1().overflow_hidden().child(tasks_panel))
                            }),
                    ),
            );

        let focus_handle = self.focus_handle.clone();

        div()
            .id("workspace-root")
            .relative()
            .size_full()
            .bg(bg_color)
            .track_focus(&focus_handle)
            .on_action(
                cx.listener(|this, _: &keymap::ToggleCommandPalette, window, cx| {
                    this.toggle_command_palette(window, cx);
                }),
            )
            .on_action(cx.listener(|this, _: &keymap::NewQueryTab, window, cx| {
                this.editor.update(cx, |editor, cx| {
                    editor.add_new_tab(window, cx);
                });
            }))
            .on_action(
                cx.listener(|this, _: &keymap::CloseCurrentTab, _window, cx| {
                    this.editor.update(cx, |editor, cx| {
                        editor.close_current_tab(cx);
                    });
                }),
            )
            .on_action(cx.listener(|this, _: &keymap::NextTab, _window, cx| {
                this.editor.update(cx, |editor, cx| {
                    editor.next_tab(cx);
                });
            }))
            .on_action(cx.listener(|this, _: &keymap::PrevTab, _window, cx| {
                this.editor.update(cx, |editor, cx| {
                    editor.prev_tab(cx);
                });
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab1, _window, cx| {
                this.editor
                    .update(cx, |editor, cx| editor.switch_to_tab(1, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab2, _window, cx| {
                this.editor
                    .update(cx, |editor, cx| editor.switch_to_tab(2, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab3, _window, cx| {
                this.editor
                    .update(cx, |editor, cx| editor.switch_to_tab(3, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab4, _window, cx| {
                this.editor
                    .update(cx, |editor, cx| editor.switch_to_tab(4, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab5, _window, cx| {
                this.editor
                    .update(cx, |editor, cx| editor.switch_to_tab(5, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab6, _window, cx| {
                this.editor
                    .update(cx, |editor, cx| editor.switch_to_tab(6, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab7, _window, cx| {
                this.editor
                    .update(cx, |editor, cx| editor.switch_to_tab(7, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab8, _window, cx| {
                this.editor
                    .update(cx, |editor, cx| editor.switch_to_tab(8, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::SwitchToTab9, _window, cx| {
                this.editor
                    .update(cx, |editor, cx| editor.switch_to_tab(9, cx));
            }))
            .on_action(cx.listener(|this, _: &keymap::FocusSidebar, window, cx| {
                this.set_focus(FocusTarget::Sidebar, window, cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::FocusEditor, window, cx| {
                this.set_focus(FocusTarget::Editor, window, cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::FocusResults, window, cx| {
                this.set_focus(FocusTarget::Results, window, cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::FocusHistory, window, cx| {
                this.set_focus(FocusTarget::History, window, cx);
            }))
            .on_action(
                cx.listener(|this, _: &keymap::FocusBackgroundTasks, window, cx| {
                    this.set_focus(FocusTarget::BackgroundTasks, window, cx);
                }),
            )
            .on_action(
                cx.listener(|this, _: &keymap::CycleFocusForward, window, cx| {
                    let next = this.focus_target.next();
                    this.set_focus(next, window, cx);
                }),
            )
            .on_action(
                cx.listener(|this, _: &keymap::CycleFocusBackward, window, cx| {
                    let prev = this.focus_target.prev();
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
                this.editor.update(cx, |editor, cx| {
                    editor.run_query(window, cx);
                });
            }))
            .on_action(cx.listener(|this, _: &keymap::Cancel, window, cx| {
                if this.command_palette.read(cx).is_visible() {
                    this.command_palette.update(cx, |p, cx| p.hide(cx));
                }
                // Always focus the workspace to exit any input and enable navigation
                this.focus_handle.focus(window);
            }))
            .on_action(cx.listener(|this, _: &keymap::ExportResults, window, cx| {
                this.results.update(cx, |results, cx| {
                    results.export_results(window, cx);
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
            .on_action(cx.listener(|this, _: &keymap::ToggleEditor, _window, cx| {
                this.toggle_editor(cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::ToggleResults, _window, cx| {
                this.toggle_results(cx);
            }))
            .on_action(cx.listener(|this, _: &keymap::ToggleTasks, _window, cx| {
                this.toggle_tasks_panel(cx);
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
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                let chord = KeyChord::from_gpui(&event.keystroke);
                let context = this.active_context(cx);

                log::debug!(
                    "Key event: {:?}, context: {:?}, chord: {:?}",
                    event.keystroke.key,
                    context,
                    chord
                );

                if let Some(cmd) = this.keymap.resolve(context, &chord) {
                    log::debug!("Resolved command: {:?}", cmd);
                    if this.dispatch(cmd, window, cx) {
                        cx.stop_propagation();
                    }
                }
            }))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .size_full()
                    .child(
                        div().flex().flex_1().overflow_hidden().child(
                            h_resizable("workspace")
                                .child(
                                    resizable_panel()
                                        .size(px(240.0))
                                        .size_range(px(150.0)..px(500.0))
                                        .child(sidebar),
                                )
                                .child(resizable_panel().child(right_pane)),
                        ),
                    )
                    .child(status_bar),
            )
            .child(command_palette)
            .child(notification_list)
    }
}

impl CommandDispatcher for Workspace {
    fn dispatch(&mut self, cmd: Command, window: &mut Window, cx: &mut Context<Self>) -> bool {
        match cmd {
            Command::ToggleCommandPalette => {
                self.toggle_command_palette(window, cx);
                true
            }
            Command::NewQueryTab => {
                self.editor.update(cx, |editor, cx| {
                    editor.add_new_tab(window, cx);
                });
                self.set_focus(FocusTarget::Editor, window, cx);
                self.editor.update(cx, |editor, cx| {
                    editor.focus_input(window, cx);
                });
                true
            }
            Command::RunQuery => {
                self.editor.update(cx, |editor, cx| {
                    editor.run_query(window, cx);
                });
                true
            }
            Command::ExportResults => {
                self.results.update(cx, |results, cx| {
                    results.export_results(window, cx);
                });
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
                self.toggle_editor(cx);
                true
            }
            Command::ToggleResults => {
                self.toggle_results(cx);
                true
            }
            Command::ToggleTasks => {
                self.toggle_tasks_panel(cx);
                true
            }
            Command::FocusSidebar => {
                self.set_focus(FocusTarget::Sidebar, window, cx);
                true
            }
            Command::FocusEditor => {
                self.set_focus(FocusTarget::Editor, window, cx);
                true
            }
            Command::FocusResults => {
                self.set_focus(FocusTarget::Results, window, cx);
                true
            }
            Command::FocusHistory => {
                self.set_focus(FocusTarget::History, window, cx);
                true
            }
            Command::CycleFocusForward => {
                let next = self.focus_target.next();
                self.set_focus(next, window, cx);
                true
            }
            Command::CycleFocusBackward => {
                let prev = self.focus_target.prev();
                self.set_focus(prev, window, cx);
                true
            }
            Command::NextTab => {
                self.editor.update(cx, |editor, cx| {
                    editor.next_tab(cx);
                });
                if self.focus_target == FocusTarget::Editor {
                    self.editor.update(cx, |editor, cx| {
                        editor.focus_input(window, cx);
                    });
                }
                true
            }
            Command::PrevTab => {
                self.editor.update(cx, |editor, cx| {
                    editor.prev_tab(cx);
                });
                if self.focus_target == FocusTarget::Editor {
                    self.editor.update(cx, |editor, cx| {
                        editor.focus_input(window, cx);
                    });
                }
                true
            }
            Command::SwitchToTab(n) => {
                self.editor.update(cx, |editor, cx| {
                    editor.switch_to_tab(n, cx);
                });
                if self.focus_target == FocusTarget::Editor {
                    self.editor.update(cx, |editor, cx| {
                        editor.focus_input(window, cx);
                    });
                }
                true
            }
            Command::CloseCurrentTab => {
                self.editor.update(cx, |editor, cx| {
                    editor.close_current_tab(cx);
                });
                if self.focus_target == FocusTarget::Editor {
                    self.editor.update(cx, |editor, cx| {
                        editor.focus_input(window, cx);
                    });
                }
                true
            }
            Command::Cancel => {
                if self.command_palette.read(cx).is_visible() {
                    self.command_palette.update(cx, |p, cx| p.hide(cx));
                }
                // Always focus workspace to blur any input and enable keyboard navigation
                self.focus_handle.focus(window);
                true
            }

            Command::CancelQuery => {
                log::debug!("Command {:?} not yet implemented", cmd);
                false
            }

            Command::SelectNext => match self.focus_target {
                FocusTarget::Sidebar => {
                    self.sidebar.update(cx, |s, cx| s.select_next(cx));
                    true
                }
                FocusTarget::Results => {
                    self.results.update(cx, |r, cx| r.select_next(cx));
                    true
                }
                FocusTarget::History => {
                    self.sidebar.update(cx, |s, cx| {
                        s.history_panel.update(cx, |h, cx| h.select_next(cx));
                    });
                    true
                }
                _ => false,
            },

            Command::SelectPrev => match self.focus_target {
                FocusTarget::Sidebar => {
                    self.sidebar.update(cx, |s, cx| s.select_prev(cx));
                    true
                }
                FocusTarget::Results => {
                    self.results.update(cx, |r, cx| r.select_prev(cx));
                    true
                }
                FocusTarget::History => {
                    self.sidebar.update(cx, |s, cx| {
                        s.history_panel.update(cx, |h, cx| h.select_prev(cx));
                    });
                    true
                }
                _ => false,
            },

            Command::SelectFirst => match self.focus_target {
                FocusTarget::Sidebar => {
                    self.sidebar.update(cx, |s, cx| s.select_first(cx));
                    true
                }
                FocusTarget::Results => {
                    self.results.update(cx, |r, cx| r.select_first(cx));
                    true
                }
                FocusTarget::History => {
                    self.sidebar.update(cx, |s, cx| {
                        s.history_panel.update(cx, |h, cx| h.select_first(cx));
                    });
                    true
                }
                _ => false,
            },

            Command::SelectLast => match self.focus_target {
                FocusTarget::Sidebar => {
                    self.sidebar.update(cx, |s, cx| s.select_last(cx));
                    true
                }
                FocusTarget::Results => {
                    self.results.update(cx, |r, cx| r.select_last(cx));
                    true
                }
                FocusTarget::History => {
                    self.sidebar.update(cx, |s, cx| {
                        s.history_panel.update(cx, |h, cx| h.select_last(cx));
                    });
                    true
                }
                _ => false,
            },

            Command::Execute => match self.focus_target {
                FocusTarget::Sidebar => {
                    self.sidebar.update(cx, |s, cx| s.execute(cx));
                    true
                }
                FocusTarget::History => {
                    self.sidebar.update(cx, |s, cx| {
                        s.history_panel.update(cx, |h, cx| h.execute(cx));
                    });
                    true
                }
                FocusTarget::Editor => {
                    self.editor.update(cx, |e, cx| e.focus_input(window, cx));
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
                    self.sidebar.update(cx, |s, cx| s.collapse(cx));
                    true
                }
                FocusTarget::Results => {
                    if self.results_state.is_expanded() {
                        self.results_state = PanelState::Collapsed;
                        cx.notify();
                    }
                    true
                }
                FocusTarget::Editor => {
                    if self.editor_state.is_expanded() {
                        self.editor_state = PanelState::Collapsed;
                        cx.notify();
                    }
                    true
                }
                FocusTarget::BackgroundTasks => {
                    if self.tasks_state.is_expanded() {
                        self.tasks_state = PanelState::Collapsed;
                        cx.notify();
                    }
                    true
                }
                _ => false,
            },

            Command::ColumnRight => match self.focus_target {
                FocusTarget::Sidebar => {
                    self.sidebar.update(cx, |s, cx| s.expand(cx));
                    true
                }
                FocusTarget::Results => {
                    if !self.results_state.is_expanded() {
                        self.results_state = PanelState::Expanded;
                        cx.notify();
                    }
                    true
                }
                FocusTarget::Editor => {
                    if !self.editor_state.is_expanded() {
                        self.editor_state = PanelState::Expanded;
                        cx.notify();
                    }
                    true
                }
                FocusTarget::BackgroundTasks => {
                    if !self.tasks_state.is_expanded() {
                        self.tasks_state = PanelState::Expanded;
                        cx.notify();
                    }
                    true
                }
                _ => false,
            },

            Command::ToggleFavorite => {
                if self.focus_target == FocusTarget::History {
                    self.sidebar.update(cx, |s, cx| {
                        s.history_panel
                            .update(cx, |h, cx| h.toggle_favorite_selected(cx));
                    });
                    true
                } else {
                    false
                }
            }

            Command::DeleteHistoryEntry => {
                if self.focus_target == FocusTarget::History {
                    self.sidebar.update(cx, |s, cx| {
                        s.history_panel.update(cx, |h, cx| h.delete_selected(cx));
                    });
                    true
                } else {
                    false
                }
            }

            // Directional focus navigation
            // Layout:  Sidebar | Editor
            //                  | Results
            //                  | BackgroundTasks
            Command::FocusLeft => {
                // From main area → Sidebar (or History if it was focused)
                match self.focus_target {
                    FocusTarget::Editor | FocusTarget::Results | FocusTarget::BackgroundTasks => {
                        self.set_focus(FocusTarget::Sidebar, window, cx);
                        true
                    }
                    _ => false,
                }
            }

            Command::FocusRight => {
                // From Sidebar/History → Editor
                match self.focus_target {
                    FocusTarget::Sidebar | FocusTarget::History => {
                        self.set_focus(FocusTarget::Editor, window, cx);
                        true
                    }
                    _ => false,
                }
            }

            Command::FocusDown => {
                // Editor → Results → BackgroundTasks (wrap to Editor)
                let next = match self.focus_target {
                    FocusTarget::Editor => FocusTarget::Results,
                    FocusTarget::Results => FocusTarget::BackgroundTasks,
                    FocusTarget::BackgroundTasks => FocusTarget::Editor,
                    _ => return false,
                };
                self.set_focus(next, window, cx);
                true
            }

            Command::FocusUp => {
                // BackgroundTasks → Results → Editor (wrap to Tasks)
                let prev = match self.focus_target {
                    FocusTarget::BackgroundTasks => FocusTarget::Results,
                    FocusTarget::Results => FocusTarget::Editor,
                    FocusTarget::Editor => FocusTarget::BackgroundTasks,
                    _ => return false,
                };
                self.set_focus(prev, window, cx);
                true
            }

            Command::PageDown
            | Command::PageUp
            | Command::Delete
            | Command::ResultsNextPage
            | Command::ResultsPrevPage
            | Command::LoadQuery => {
                log::debug!("Context-specific command {:?} not yet implemented", cmd);
                false
            }
        }
    }
}
