use crate::app::{AppState, AppStateChanged};
use crate::keymap::{
    self, Command, CommandDispatcher, ContextId, FocusTarget, KeyChord, KeymapStack, default_keymap,
};
use crate::ui::command_palette::{
    CommandExecuted, CommandPalette, CommandPaletteClosed, PaletteCommand,
};
use crate::ui::dock::{SidebarDock, SidebarDockEvent};
use crate::ui::document::{
    DataDocument, DocumentHandle, SqlQueryDocument, TabBar, TabBarEvent, TabManager,
};
use crate::ui::icons::AppIcon;
use crate::ui::shutdown_overlay::ShutdownOverlay;
use crate::ui::sidebar::{Sidebar, SidebarEvent};
use crate::ui::sql_preview_modal::SqlPreviewModal;
use crate::ui::status_bar::{StatusBar, ToggleTasksPanel};
use crate::ui::tasks_panel::TasksPanel;
use crate::ui::toast::{ToastGlobal, ToastHost};
use crate::ui::tokens::{FontSizes, Radii, Spacing};
use crate::ui::windows::connection_manager::ConnectionManagerWindow;
use crate::ui::windows::settings::SettingsWindow;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Root;
use gpui_component::resizable::{resizable_panel, v_resizable};

/// State for collapsible panels (tasks panel).
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
    sidebar_dock: Entity<SidebarDock>,
    status_bar: Entity<StatusBar>,
    tasks_panel: Entity<TasksPanel>,
    toast_host: Entity<ToastHost>,
    command_palette: Entity<CommandPalette>,
    sql_preview_modal: Entity<SqlPreviewModal>,
    shutdown_overlay: Entity<ShutdownOverlay>,

    tab_manager: Entity<TabManager>,
    tab_bar: Entity<TabBar>,

    tasks_state: PanelState,
    pending_command: Option<&'static str>,
    pending_sql: Option<String>,
    pending_focus: Option<FocusTarget>,
    needs_focus_restore: bool,

    focus_target: FocusTarget,
    keymap: &'static KeymapStack,
    focus_handle: FocusHandle,
}

impl Workspace {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let toast_host = cx.new(|_cx| ToastHost::new());
        cx.set_global(ToastGlobal {
            host: toast_host.clone(),
        });

        let sidebar = cx.new(|cx| Sidebar::new(app_state.clone(), window, cx));
        let sidebar_dock = cx.new(|cx| SidebarDock::new(sidebar.clone(), cx));
        let status_bar = cx.new(|cx| StatusBar::new(app_state.clone(), window, cx));
        let tasks_panel = cx.new(|cx| TasksPanel::new(app_state.clone(), window, cx));

        let tab_manager = cx.new(|_cx| TabManager::new());
        let tab_bar = cx.new(|cx| TabBar::new(tab_manager.clone(), cx));

        let command_palette = cx.new(|cx| {
            let mut palette = CommandPalette::new(window, cx);
            palette.register_commands(Self::default_commands());
            palette
        });

        let sql_preview_modal = cx.new(|cx| SqlPreviewModal::new(app_state.clone(), window, cx));
        let shutdown_overlay = cx.new(|cx| ShutdownOverlay::new(app_state.clone(), window, cx));

        cx.subscribe(&status_bar, |this, _, _: &ToggleTasksPanel, cx| {
            this.toggle_tasks_panel(cx);
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

        cx.subscribe_in(
            &sidebar,
            window,
            |this, _, event: &SidebarEvent, window, cx| match event {
                SidebarEvent::GenerateSql(sql) => {
                    this.pending_sql = Some(sql.clone());
                    cx.notify();
                }
                SidebarEvent::RequestFocus => {
                    this.pending_focus = Some(FocusTarget::Sidebar);
                    cx.notify();
                }
                SidebarEvent::OpenTable { profile_id, table } => {
                    this.open_table_document(*profile_id, table.clone(), window, cx);
                }
                SidebarEvent::OpenCollection {
                    profile_id,
                    collection,
                } => {
                    this.open_collection_document(*profile_id, collection.clone(), window, cx);
                }
                SidebarEvent::RequestSqlPreview {
                    profile_id,
                    table_info,
                    generation_type,
                } => {
                    use crate::ui::sql_preview_modal::SqlPreviewContext;
                    let context = SqlPreviewContext::SidebarTable {
                        profile_id: *profile_id,
                        table_info: table_info.clone(),
                    };
                    this.sql_preview_modal.update(cx, |modal, cx| {
                        modal.open(context, *generation_type, window, cx);
                    });
                }
            },
        )
        .detach();

        cx.subscribe(
            &sidebar_dock,
            |this, _, event: &SidebarDockEvent, cx| match event {
                SidebarDockEvent::OpenSettings => {
                    this.open_settings(cx);
                }
                SidebarDockEvent::Collapsed => {
                    this.pending_focus = Some(FocusTarget::Document);
                    cx.notify();
                }
                SidebarDockEvent::Expanded => {
                    // When expanded, focus the sidebar
                    this.pending_focus = Some(FocusTarget::Sidebar);
                    cx.notify();
                }
            },
        )
        .detach();

        cx.subscribe_in(
            &tab_bar,
            window,
            |this, _, event: &TabBarEvent, window, cx| match event {
                TabBarEvent::NewTabRequested => {
                    this.new_query_tab(window, cx);
                }
            },
        )
        .detach();

        cx.subscribe_in(
            &tab_manager,
            window,
            |this, _, event: &crate::ui::document::TabManagerEvent, window, cx| {
                use crate::ui::document::TabManagerEvent;
                if let TabManagerEvent::RequestSqlPreview {
                    profile_id,
                    schema_name,
                    table_name,
                    column_names,
                    row_values,
                    pk_indices,
                    generation_type,
                } = event
                {
                    use crate::ui::sql_preview_modal::SqlPreviewContext;
                    let context = SqlPreviewContext::DataTableRow {
                        profile_id: *profile_id,
                        schema_name: schema_name.clone(),
                        table_name: table_name.clone(),
                        column_names: column_names.clone(),
                        row_values: row_values.clone(),
                        pk_indices: pk_indices.clone(),
                    };
                    this.sql_preview_modal.update(cx, |modal, cx| {
                        modal.open(context, *generation_type, window, cx);
                    });
                }
            },
        )
        .detach();

        let focus_handle = cx.focus_handle();
        focus_handle.focus(window);

        Self {
            app_state,
            sidebar,
            sidebar_dock,
            status_bar,
            tasks_panel,
            toast_host,
            command_palette,
            sql_preview_modal,
            shutdown_overlay,
            tab_manager,
            tab_bar,
            tasks_state: PanelState::Collapsed,
            pending_command: None,
            pending_sql: None,
            pending_focus: None,
            needs_focus_restore: false,
            focus_target: FocusTarget::default(),
            keymap: default_keymap(),
            focus_handle,
        }
    }

    fn default_commands() -> Vec<PaletteCommand> {
        vec![
            // Editor
            PaletteCommand::new("new_query_tab", "New Query Tab", "Editor").with_shortcut("Ctrl+N"),
            PaletteCommand::new("run_query", "Run Query", "Editor").with_shortcut("Ctrl+Enter"),
            PaletteCommand::new("run_query_in_new_tab", "Run Query in New Tab", "Editor")
                .with_shortcut("Ctrl+Shift+Enter"),
            PaletteCommand::new("save_query", "Save Query", "Editor").with_shortcut("Ctrl+S"),
            PaletteCommand::new("open_history", "Open Query History", "Editor")
                .with_shortcut("Ctrl+P"),
            PaletteCommand::new("cancel_query", "Cancel Running Query", "Editor")
                .with_shortcut("Esc"),
            // Tabs
            PaletteCommand::new("close_tab", "Close Current Tab", "Tabs").with_shortcut("Ctrl+W"),
            PaletteCommand::new("next_tab", "Next Tab", "Tabs").with_shortcut("Ctrl+Tab"),
            PaletteCommand::new("prev_tab", "Previous Tab", "Tabs").with_shortcut("Ctrl+Shift+Tab"),
            // Results
            PaletteCommand::new("export_results", "Export Results to CSV", "Results")
                .with_shortcut("Ctrl+E"),
            // Connections
            PaletteCommand::new(
                "open_connection_manager",
                "Open Connection Manager",
                "Connections",
            ),
            PaletteCommand::new("disconnect", "Disconnect Current", "Connections"),
            PaletteCommand::new("refresh_schema", "Refresh Schema", "Connections"),
            // Focus
            PaletteCommand::new("focus_sidebar", "Focus Sidebar", "Focus")
                .with_shortcut("Ctrl+Shift+1"),
            PaletteCommand::new("focus_editor", "Focus Editor", "Focus")
                .with_shortcut("Ctrl+Shift+2"),
            PaletteCommand::new("focus_results", "Focus Results", "Focus")
                .with_shortcut("Ctrl+Shift+3"),
            PaletteCommand::new("focus_tasks", "Focus Tasks Panel", "Focus")
                .with_shortcut("Ctrl+Shift+4"),
            // View
            PaletteCommand::new("toggle_sidebar", "Toggle Sidebar", "View").with_shortcut("Ctrl+B"),
            PaletteCommand::new("toggle_editor", "Toggle Editor Panel", "View"),
            PaletteCommand::new("toggle_results", "Toggle Results Panel", "View"),
            PaletteCommand::new("toggle_tasks", "Toggle Tasks Panel", "View"),
            PaletteCommand::new("open_settings", "Open Settings", "View"),
        ]
    }

    fn active_context(&self, cx: &Context<Self>) -> ContextId {
        if self.command_palette.read(cx).is_visible() {
            return ContextId::CommandPalette;
        }

        if self.sql_preview_modal.read(cx).is_visible() {
            return ContextId::SqlPreviewModal;
        }

        if self.focus_target == FocusTarget::Sidebar && self.sidebar.read(cx).is_renaming() {
            return ContextId::TextInput;
        }

        // When focused on document area, delegate context to the active document
        if self.focus_target == FocusTarget::Document
            && let Some(doc) = self.tab_manager.read(cx).active_document()
        {
            return doc.active_context(cx);
        }

        self.focus_target.to_context()
    }

    pub fn set_focus(&mut self, target: FocusTarget, _window: &mut Window, cx: &mut Context<Self>) {
        // Don't allow focus on sidebar when it's collapsed
        let target = if target == FocusTarget::Sidebar && self.is_sidebar_collapsed(cx) {
            FocusTarget::Document
        } else {
            target
        };

        log::debug!("Focus changed to: {:?}", target);
        self.focus_target = target;

        self.sidebar.update(cx, |sidebar, cx| {
            sidebar.set_connections_focused(target == FocusTarget::Sidebar, cx);
        });

        cx.notify();
    }

    fn handle_command(&mut self, command_id: &str, window: &mut Window, cx: &mut Context<Self>) {
        match command_id {
            // Editor/Document commands - route to active document
            "new_query_tab" => {
                self.new_query_tab(window, cx);
            }
            "run_query" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::RunQuery, window, cx);
                }
            }
            "run_query_in_new_tab" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::RunQueryInNewTab, window, cx);
                }
            }
            "save_query" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::SaveQuery, window, cx);
                }
            }
            "open_history" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ToggleHistoryDropdown, window, cx);
                }
            }
            "cancel_query" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::CancelQuery, window, cx);
                }
            }

            // Tabs
            "close_tab" => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.close_active(cx);
                });
            }
            "next_tab" => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.next_visual_tab(cx);
                });
            }
            "prev_tab" => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.prev_visual_tab(cx);
                });
            }

            // Results - route to active document
            "export_results" => {
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ExportResults, window, cx);
                }
            }

            // Connections
            "open_connection_manager" => {
                self.open_connection_manager(cx);
            }
            "disconnect" => {
                self.disconnect_active(window, cx);
            }
            "refresh_schema" => {
                self.refresh_schema(window, cx);
            }

            // Focus
            "focus_sidebar" => {
                self.set_focus(FocusTarget::Sidebar, window, cx);
            }
            "focus_editor" => {
                self.set_focus(FocusTarget::Document, window, cx);
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::FocusUp, window, cx);
                }
            }
            "focus_results" => {
                self.set_focus(FocusTarget::Document, window, cx);
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::FocusDown, window, cx);
                }
            }
            "focus_tasks" => {
                self.set_focus(FocusTarget::BackgroundTasks, window, cx);
            }

            // View
            "toggle_sidebar" => {
                self.toggle_sidebar(cx);
            }
            "toggle_editor" => {
                // Route to active document if it supports layout toggling
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ToggleEditor, window, cx);
                }
            }
            "toggle_results" => {
                // Route to active document if it supports layout toggling
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned() {
                    doc.dispatch_command(Command::ToggleResults, window, cx);
                }
            }
            "toggle_tasks" => {
                self.toggle_tasks_panel(cx);
            }
            "open_settings" => {
                self.open_settings(cx);
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
                app_id: Some("dbflux".into()),
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

    fn open_settings(&self, cx: &mut Context<Self>) {
        if let Some(handle) = self.app_state.read(cx).settings_window {
            if handle
                .update(cx, |_root, window, _cx| window.activate_window())
                .is_ok()
            {
                return;
            }
            self.app_state.update(cx, |state, _| {
                state.settings_window = None;
            });
        }

        let app_state = self.app_state.clone();
        let bounds = Bounds::centered(None, size(px(950.0), px(700.0)), cx);

        if let Ok(handle) = cx.open_window(
            WindowOptions {
                app_id: Some("dbflux".into()),
                titlebar: Some(TitlebarOptions {
                    title: Some("Settings".into()),
                    ..Default::default()
                }),
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                kind: WindowKind::Floating,
                focus: true,
                ..Default::default()
            },
            |window, cx| {
                let settings = cx.new(|cx| SettingsWindow::new(app_state.clone(), window, cx));
                cx.new(|cx| Root::new(settings, window, cx))
            },
        ) {
            self.app_state.update(cx, |state, _| {
                state.settings_window = Some(handle);
            });
        }
    }

    fn disconnect_active(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::toast::ToastExt;

        let profile_id = self.app_state.read(cx).active_connection_id();

        if let Some(id) = profile_id {
            let name = self
                .app_state
                .read(cx)
                .connections()
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
                        if let Some(connected) = state.connections_mut().get_mut(&profile_id) {
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

    /// Opens a table in a new DataDocument tab (v0.3).
    /// If the table is already open, focuses the existing tab instead.
    fn open_table_document(
        &mut self,
        profile_id: uuid::Uuid,
        table: dbflux_core::TableRef,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::toast::ToastExt;

        // Check if connection exists
        if !self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id)
        {
            cx.toast_error("No active connection for this table", window);
            return;
        }

        // Check if table is already open - if so, focus that tab
        let existing_id = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .find(|doc| doc.is_table(&table, cx))
            .map(|doc| doc.id());

        if let Some(id) = existing_id {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            log::info!(
                "Focused existing table document: {:?}.{:?}",
                table.schema,
                table.name
            );
            return;
        }

        // Create a DataDocument for the table
        let doc = cx.new(|cx| {
            DataDocument::new_for_table(
                profile_id,
                table.clone(),
                self.app_state.clone(),
                window,
                cx,
            )
        });
        let handle = DocumentHandle::data(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        log::info!("Opened table document: {:?}.{:?}", table.schema, table.name);
    }

    fn open_collection_document(
        &mut self,
        profile_id: uuid::Uuid,
        collection: dbflux_core::CollectionRef,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::toast::ToastExt;

        // Check if connection exists
        if !self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id)
        {
            cx.toast_error("No active connection for this collection", window);
            return;
        }

        // Check if collection is already open - if so, focus that tab
        let existing_id = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .find(|doc| doc.is_collection(&collection, cx))
            .map(|doc| doc.id());

        if let Some(id) = existing_id {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            log::info!(
                "Focused existing collection document: {}.{}",
                collection.database,
                collection.name
            );
            return;
        }

        // Create a DataDocument for the collection
        let doc = cx.new(|cx| {
            DataDocument::new_for_collection(
                profile_id,
                collection.clone(),
                self.app_state.clone(),
                window,
                cx,
            )
        });
        let handle = DocumentHandle::data(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        log::info!(
            "Opened collection document: {}.{}",
            collection.database,
            collection.name
        );
    }

    /// Creates a new SQL query tab (v0.3).
    fn new_query_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // Count existing query tabs for naming
        let query_count = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .filter(|d| matches!(d.kind(), crate::ui::document::DocumentKind::Script))
            .count();

        let title = format!("Query {}", query_count + 1);

        let doc = cx
            .new(|cx| SqlQueryDocument::new(self.app_state.clone(), window, cx).with_title(title));
        let handle = DocumentHandle::sql_query(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    fn new_query_tab_with_content(
        &mut self,
        sql: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Count existing query tabs for naming
        let query_count = self
            .tab_manager
            .read(cx)
            .documents()
            .iter()
            .filter(|d| matches!(d.kind(), crate::ui::document::DocumentKind::Script))
            .count();

        let title = format!("Query {}", query_count + 1);

        let doc = cx.new(|cx| {
            let mut doc =
                SqlQueryDocument::new(self.app_state.clone(), window, cx).with_title(title);
            doc.set_content(&sql, window, cx);
            doc
        });
        let handle = DocumentHandle::sql_query(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(handle, cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
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

    pub fn toggle_sidebar(&mut self, cx: &mut Context<Self>) {
        self.sidebar_dock.update(cx, |dock, cx| {
            dock.toggle(cx);
        });
    }

    fn is_sidebar_collapsed(&self, cx: &Context<Self>) -> bool {
        self.sidebar_dock.read(cx).is_collapsed()
    }

    /// Get next focus target, skipping sidebar if collapsed
    fn next_focus_target(&self, cx: &Context<Self>) -> FocusTarget {
        let mut target = self.focus_target.next();
        if target == FocusTarget::Sidebar && self.is_sidebar_collapsed(cx) {
            target = target.next();
        }
        target
    }

    /// Get previous focus target, skipping sidebar if collapsed
    fn prev_focus_target(&self, cx: &Context<Self>) -> FocusTarget {
        let mut target = self.focus_target.prev();
        if target == FocusTarget::Sidebar && self.is_sidebar_collapsed(cx) {
            target = target.prev();
        }
        target
    }

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
                | Command::Cancel => {}
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
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.close_active(cx);
                });
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

            Command::FocusRight => match self.focus_target {
                FocusTarget::Sidebar => {
                    self.set_focus(FocusTarget::Document, window, cx);
                    true
                }
                _ => false,
            },

            Command::FocusDown => {
                // First try the active document (for internal editor→results navigation)
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned()
                    && doc.dispatch_command(Command::FocusDown, window, cx)
                {
                    return true;
                }
                // Workspace-level: Document → BackgroundTasks
                let next = match self.focus_target {
                    FocusTarget::Document => FocusTarget::BackgroundTasks,
                    FocusTarget::BackgroundTasks => FocusTarget::Document,
                    _ => return false,
                };
                self.set_focus(next, window, cx);
                true
            }

            Command::FocusUp => {
                // First try the active document (for internal results→editor navigation)
                if let Some(doc) = self.tab_manager.read(cx).active_document().cloned()
                    && doc.dispatch_command(Command::FocusUp, window, cx)
                {
                    return true;
                }
                // Workspace-level: BackgroundTasks → Document
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
                } else {
                    false
                }
            }

            Command::Delete => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar
                        .update(cx, |s, cx| s.request_delete_selected(cx));
                    true
                } else {
                    false
                }
            }

            Command::CreateFolder => {
                if self.focus_target == FocusTarget::Sidebar {
                    self.sidebar.update(cx, |s, cx| s.create_root_folder(cx));
                    true
                } else {
                    false
                }
            }

            Command::FocusSearch => {
                // Context-specific (saved queries modal)
                false
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

            // Row operations - handled via GPUI actions in DataTable
            Command::ResultsDeleteRow
            | Command::ResultsAddRow
            | Command::ResultsDuplicateRow
            | Command::ResultsCopyRow
            | Command::ResultsSetNull => {
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
