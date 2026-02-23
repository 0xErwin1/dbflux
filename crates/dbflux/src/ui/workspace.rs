mod actions;
mod dispatch;
mod render;

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
                SidebarEvent::OpenKeyValueDatabase {
                    profile_id,
                    database,
                } => {
                    this.open_key_value_document(*profile_id, database.clone(), window, cx);
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
                SidebarEvent::RequestQueryPreview {
                    language,
                    badge,
                    query,
                } => {
                    this.sql_preview_modal.update(cx, |modal, cx| {
                        modal.open_query_preview(*language, badge, query.clone(), window, cx);
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
                match event {
                    TabManagerEvent::DocumentRequestedFocus => {
                        this.set_focus(FocusTarget::Document, window, cx);
                    }
                    TabManagerEvent::RequestSqlPreview {
                        profile_id,
                        schema_name,
                        table_name,
                        column_names,
                        row_values,
                        pk_indices,
                        generation_type,
                    } => {
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
                    _ => {}
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
            PaletteCommand::new("export_results", "Export Results", "Results")
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

    pub fn set_focus(&mut self, target: FocusTarget, window: &mut Window, cx: &mut Context<Self>) {
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

        if target == FocusTarget::Document
            && let Some(doc) = self.tab_manager.read(cx).active_document().cloned()
        {
            doc.focus(window, cx);
        }

        cx.notify();
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
}
