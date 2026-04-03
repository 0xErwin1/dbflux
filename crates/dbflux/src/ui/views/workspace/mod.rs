mod actions;
mod dispatch;
pub mod pipeline;
mod render;

use crate::app::{AppState, AppStateChanged};
use dbflux_core::observability::actions::CONFIG_CHANGE;

#[cfg(feature = "mcp")]
use crate::app::McpRuntimeEventRaised;

use crate::keymap::{
    self, key_chord_from_gpui, Command, CommandDispatcher, ContextId, FocusTarget, KeymapStack, default_keymap,
};
use crate::ui::components::toast::{ToastGlobal, ToastHost};
use crate::ui::dock::{SidebarDock, SidebarDockEvent};
use crate::ui::document::{
    CodeDocument, DataDocument, DocumentHandle, TabBar, TabBarEvent, TabManager,
};

#[cfg(feature = "mcp")]
use crate::ui::document::{McpApprovalsView, McpAuditView};
use crate::ui::icons::AppIcon;
use crate::ui::overlays::command_palette::{
    CommandExecuted, CommandPalette, CommandPaletteClosed, PaletteCommand,
};
use crate::ui::overlays::login_modal::{LoginModal, LoginModalEvent};
use crate::ui::overlays::shutdown_overlay::ShutdownOverlay;
use crate::ui::overlays::sql_preview_modal::SqlPreviewModal;
use crate::ui::overlays::sso_wizard::{SsoWizard, SsoWizardEvent};
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use crate::ui::views::sidebar::{Sidebar, SidebarEvent, SidebarTab};
use crate::ui::views::status_bar::{StatusBar, ToggleTasksPanel};
use crate::ui::views::tasks_panel::TasksPanel;
use crate::ui::windows::connection_manager::ConnectionManagerWindow;
use crate::ui::windows::settings::SettingsWindow;
use dbflux_core::{ExecutionContext, QueryLanguage};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Root;
use gpui_component::resizable::{resizable_panel, v_resizable};
use std::path::PathBuf;

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

/// Deferred until render (needs `Window` access).
pub(super) struct PendingOpenScript {
    pub path: Option<PathBuf>,
    pub title: String,
    pub body: String,
    pub language: QueryLanguage,
    pub connection_id: Option<uuid::Uuid>,
    pub exec_ctx: ExecutionContext,
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
    login_modal: Entity<LoginModal>,
    sso_wizard: Entity<SsoWizard>,
    shutdown_overlay: Entity<ShutdownOverlay>,

    tab_manager: Entity<TabManager>,
    tab_bar: Entity<TabBar>,

    #[cfg(feature = "mcp")]
    mcp_approvals_view: Entity<McpApprovalsView>,
    #[cfg(feature = "mcp")]
    mcp_audit_view: Entity<McpAuditView>,

    tasks_state: PanelState,
    pending_command: Option<&'static str>,
    pending_sql: Option<String>,
    pending_focus: Option<FocusTarget>,
    pending_open_script: Option<PendingOpenScript>,
    needs_focus_restore: bool,

    /// Active pipeline progress watcher for pipeline-enabled connects.
    pipeline_progress: Option<Entity<pipeline::PipelineProgress>>,
    _pipeline_subscription: Option<Subscription>,

    focus_target: FocusTarget,
    keymap: &'static KeymapStack,
    focus_handle: FocusHandle,

    #[cfg(feature = "mcp")]
    active_governance_panel: Option<GovernancePanel>,

    /// Background task handle for periodic audit purge.
    /// Kept to ensure the task stays alive for the workspace lifetime.
    _background_purge_task: Option<Task<()>>,
}

#[cfg(feature = "mcp")]
#[derive(Clone, Copy, PartialEq, Eq)]
enum GovernancePanel {
    Approvals,
    Audit,
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

        #[cfg(feature = "mcp")]
        let mcp_approvals_view = cx.new(|_cx| McpApprovalsView::new(app_state.clone()));
        #[cfg(feature = "mcp")]
        let mcp_audit_view = cx.new(|cx| McpAuditView::new(app_state.clone(), window, cx));

        let command_palette = cx.new(|cx| {
            let mut palette = CommandPalette::new(window, cx);
            palette.register_commands(Self::default_commands());
            palette
        });

        let sql_preview_modal = cx.new(|cx| SqlPreviewModal::new(app_state.clone(), window, cx));
        let login_modal = cx.new(|cx| LoginModal::new(window, cx));
        let sso_wizard = cx.new(|cx| SsoWizard::new(app_state.clone(), window, cx));
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
            &login_modal,
            window,
            |this, _, event: &LoginModalEvent, window, cx| match event {
                LoginModalEvent::OpenSsoWizard => {
                    this.open_sso_wizard(window, cx);
                }
            },
        )
        .detach();

        cx.subscribe_in(
            &sso_wizard,
            window,
            |this, _, event: &SsoWizardEvent, _window, cx| match event {
                SsoWizardEvent::ProfileCreated { profile_id } => {
                    this.app_state.update(cx, |_state, cx| {
                        cx.emit(AppStateChanged);
                    });

                    if this.pipeline_progress.is_some() {
                        this.login_modal.update(cx, |modal, cx| {
                            modal.close(cx);
                        });

                        this.pipeline_progress = None;
                        this._pipeline_subscription = None;

                        this.sidebar.update(cx, |sidebar, cx| {
                            sidebar.connect_to_profile(*profile_id, cx);
                        });
                    }
                }
            },
        )
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
                SidebarEvent::OpenTable {
                    profile_id,
                    table,
                    database,
                } => {
                    this.open_table_document(
                        *profile_id,
                        table.clone(),
                        database.clone(),
                        window,
                        cx,
                    );
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
                    use crate::ui::overlays::sql_preview_modal::SqlPreviewContext;
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
                        modal.open_query_preview(
                            language.clone(),
                            badge,
                            query.clone(),
                            window,
                            cx,
                        );
                    });
                }
                SidebarEvent::OpenScript { path } => {
                    if dbflux_core::is_openable_script(path) {
                        this.open_script_from_path(path.clone(), cx);
                    } else {
                        use crate::ui::components::toast::ToastExt;
                        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
                        cx.toast_warning(format!("Unsupported file type: {}", name), window);
                    }
                }
                SidebarEvent::PipelineStarted {
                    profile_name,
                    watcher,
                } => {
                    this.start_pipeline_progress(profile_name.clone(), watcher.clone(), window, cx);
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
                SidebarDockEvent::OpenConnections => {
                    this.sidebar.update(cx, |s, cx| {
                        s.set_active_tab(SidebarTab::Connections, cx);
                    });
                    this.sidebar_dock.update(cx, |d, cx| d.expand(cx));
                    this.pending_focus = Some(FocusTarget::Sidebar);
                    cx.notify();
                }
                SidebarDockEvent::OpenScripts => {
                    this.sidebar.update(cx, |s, cx| {
                        s.set_active_tab(SidebarTab::Scripts, cx);
                    });
                    this.sidebar_dock.update(cx, |d, cx| d.expand(cx));
                    this.pending_focus = Some(FocusTarget::Sidebar);
                    cx.notify();
                }
                SidebarDockEvent::Collapsed => {
                    this.pending_focus = Some(FocusTarget::Document);
                    cx.notify();
                }
                SidebarDockEvent::Expanded => {
                    this.pending_focus = Some(FocusTarget::Sidebar);
                    cx.notify();
                }
            },
        )
        .detach();

        #[cfg(feature = "mcp")]
        cx.subscribe(&app_state, |this, _, _event: &McpRuntimeEventRaised, cx| {
            this.app_state.update(cx, |_state, cx| {
                cx.emit(AppStateChanged);
            });
            cx.notify();
        })
        .detach();

        cx.subscribe_in(
            &tab_bar,
            window,
            |this, _, event: &TabBarEvent, window, cx| match event {
                TabBarEvent::NewTabRequested => {
                    this.new_query_tab(window, cx);
                }
                TabBarEvent::CloseTab(id) => {
                    this.close_tab(*id, window, cx);
                }
                TabBarEvent::CloseOtherTabs(id) => {
                    this.close_tabs_batch(
                        window,
                        cx,
                        |docs, keep| {
                            docs.iter()
                                .map(|d| d.id())
                                .filter(|&did| did != keep)
                                .collect()
                        },
                        *id,
                    );
                }
                TabBarEvent::CloseAllTabs => {
                    let ids: Vec<_> = this
                        .tab_manager
                        .read(cx)
                        .documents()
                        .iter()
                        .map(|d| d.id())
                        .collect();
                    for doc_id in ids {
                        this.close_tab(doc_id, window, cx);
                    }
                }
                TabBarEvent::CloseTabsToLeft(id) => {
                    this.close_tabs_batch(
                        window,
                        cx,
                        |docs, target| {
                            let idx = docs.iter().position(|d| d.id() == target).unwrap_or(0);
                            docs[..idx].iter().map(|d| d.id()).collect()
                        },
                        *id,
                    );
                }
                TabBarEvent::CloseTabsToRight(id) => {
                    this.close_tabs_batch(
                        window,
                        cx,
                        |docs, target| {
                            let idx = docs
                                .iter()
                                .position(|d| d.id() == target)
                                .unwrap_or(docs.len().saturating_sub(1));
                            docs[(idx + 1)..].iter().map(|d| d.id()).collect()
                        },
                        *id,
                    );
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
                        context,
                        generation_type,
                    } => {
                        this.sql_preview_modal.update(cx, |modal, cx| {
                            modal.open(context.as_ref().clone(), *generation_type, window, cx);
                        });
                    }
                    TabManagerEvent::Activated(new_id) => {
                        let docs: Vec<_> = this
                            .tab_manager
                            .read(cx)
                            .documents()
                            .iter()
                            .map(|d| (d.clone(), d.id() == *new_id))
                            .collect();

                        for (doc, is_active) in docs {
                            doc.set_active_tab(is_active, cx);
                        }

                        this.write_session_manifest(cx);
                    }
                    TabManagerEvent::Opened(_)
                    | TabManagerEvent::Closed(_)
                    | TabManagerEvent::Reordered => {
                        this.write_session_manifest(cx);
                    }
                }
            },
        )
        .detach();

        let focus_handle = cx.focus_handle();
        focus_handle.focus(window);

        let mut workspace = Self {
            app_state,
            sidebar,
            sidebar_dock,
            status_bar,
            tasks_panel,
            toast_host,
            command_palette,
            sql_preview_modal,
            login_modal,
            sso_wizard,
            shutdown_overlay,
            tab_manager,
            tab_bar,
            #[cfg(feature = "mcp")]
            mcp_approvals_view,
            #[cfg(feature = "mcp")]
            mcp_audit_view,
            tasks_state: PanelState::Collapsed,
            pending_command: None,
            pending_sql: None,
            pending_focus: None,
            pending_open_script: None,
            needs_focus_restore: false,
            pipeline_progress: None,
            _pipeline_subscription: None,
            focus_target: FocusTarget::default(),
            keymap: default_keymap(),
            focus_handle,
            #[cfg(feature = "mcp")]
            active_governance_panel: None,
            _background_purge_task: None,
        };

        {
            let settings = workspace.app_state.read(cx).general_settings().clone();

            if settings.restore_session_on_startup {
                workspace.restore_session(window, cx);

                if settings.reopen_last_connections {
                    workspace.reopen_last_connections(cx);
                }
            }

            let has_tabs = !workspace.tab_manager.read(cx).is_empty();
            match settings.default_focus_on_startup {
                dbflux_core::StartupFocus::Sidebar => {
                    workspace.pending_focus = Some(FocusTarget::Sidebar);
                }
                dbflux_core::StartupFocus::LastTab => {
                    if !has_tabs {
                        workspace.pending_focus = Some(FocusTarget::Sidebar);
                    }
                }
            }
        }

        // Spawn periodic audit purge task if configured.
        {
            let app_state = workspace.app_state.clone();
            let interval_minutes = {
                let runtime = app_state.read(cx).storage_runtime();
                let repo = runtime.audit_settings();
                repo.get()
                    .ok()
                    .flatten()
                    .map(|s| s.background_purge_interval_minutes)
                    .unwrap_or(0)
            };

            if interval_minutes > 0 {
                let task = cx.spawn(async move |_workspace, cx| {
                    let interval_duration =
                        std::time::Duration::from_secs((interval_minutes as u64) * 60);

                    loop {
                        // Use GPUI's background timer instead of tokio sleep for compatibility.
                        cx.background_executor()
                            .timer(interval_duration)
                            .await;

                        // Get retention_days from settings.
                        let retention_days = cx
                            .update(|cx| {
                                let runtime = app_state.read(cx).storage_runtime();
                                let repo = runtime.audit_settings();
                                repo.get()
                                    .ok()
                                    .flatten()
                                    .map(|s| s.retention_days)
                                    .unwrap_or(30)
                            })
                            .unwrap_or(30);

                        // Get audit_service for purge and emit from foreground update.
                        let purge_result = cx
                            .update(|cx| {
                                let audit_service = app_state.read(cx).audit_service().clone();
                                audit_service.purge_old_events(retention_days, 500)
                            })
                            .ok();

                        match purge_result {
                            Some(Ok(stats)) => {
                                log::info!(
                                    "Periodic audit purge completed: deleted {} events in {} batches ({}ms)",
                                    stats.deleted_count,
                                    stats.batches,
                                    stats.duration_ms
                                );
                                // Emit purge success audit event.
                                let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
                                let event = dbflux_core::observability::EventRecord::new(
                                    now_ms,
                                    dbflux_core::observability::EventSeverity::Info,
                                    dbflux_core::observability::EventCategory::System,
                                    dbflux_core::observability::EventOutcome::Success,
                                )
                                .with_typed_action(CONFIG_CHANGE)
                                .with_summary(format!(
                                    "Periodic audit purge completed: deleted {} events",
                                    stats.deleted_count
                                ))
                                .with_duration_ms(stats.duration_ms as i64);
                                let _ = cx.update(|cx| {
                                    let audit_service = app_state.read(cx).audit_service().clone();
                                    if let Err(rec_err) = audit_service.record(event) {
                                        log::warn!("Failed to record purge success audit event: {}", rec_err);
                                    }
                                });
                            }
                            Some(Err(e)) => {
                                log::warn!("Periodic audit purge failed: {}", e);
                                // Emit a system failure event for the purge failure.
                                let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
                                let event = dbflux_core::observability::EventRecord::new(
                                    now_ms,
                                    dbflux_core::observability::EventSeverity::Error,
                                    dbflux_core::observability::EventCategory::System,
                                    dbflux_core::observability::EventOutcome::Failure,
                                )
                                .with_typed_action(CONFIG_CHANGE)
                                .with_summary(format!(
                                    "Periodic audit purge failed: {}",
                                    e
                                ));
                                // Emit through a foreground update so we have proper context.
                                let _ = cx.update(|cx| {
                                    let audit_service = app_state.read(cx).audit_service().clone();
                                    if let Err(rec_err) = audit_service.record(event) {
                                        log::warn!("Failed to record purge failure audit event: {}", rec_err);
                                    }
                                });
                            }
                            None => {
                                // cx.update failed - skip this cycle.
                            }
                        }
                    }
                });
                workspace._background_purge_task = Some(task);
            }
        }

        workspace
    }

    fn default_commands() -> Vec<PaletteCommand> {
        vec![
            // Editor
            PaletteCommand::new("new_query_tab", "New Query Tab", "Editor").with_shortcut("Ctrl+N"),
            PaletteCommand::new("run_query", "Run Query", "Editor").with_shortcut("Ctrl+Enter"),
            PaletteCommand::new("run_query_in_new_tab", "Run Query in New Tab", "Editor")
                .with_shortcut("Ctrl+Shift+Enter"),
            PaletteCommand::new("save_query", "Save Query", "Editor").with_shortcut("Ctrl+S"),
            PaletteCommand::new("save_file_as", "Save File As", "Editor")
                .with_shortcut("Ctrl+Shift+S"),
            PaletteCommand::new("open_script_file", "Open Script File", "Editor")
                .with_shortcut("Ctrl+O"),
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
            PaletteCommand::new("open_login_modal", "Open Login Modal", "View"),
            PaletteCommand::new("open_sso_wizard", "Open AWS SSO Wizard", "View"),
            #[cfg(feature = "mcp")]
            PaletteCommand::new("open_mcp_approvals", "Open MCP Approvals", "View"),
            #[cfg(feature = "mcp")]
            PaletteCommand::new("open_mcp_audit", "Open MCP Audit Viewer", "View"),
            #[cfg(feature = "mcp")]
            PaletteCommand::new("refresh_mcp_governance", "Refresh MCP Governance", "View"),
            PaletteCommand::new("open_audit_viewer", "Open Audit Viewer", "View")
                .with_shortcut("Ctrl+Shift+A"),
        ]
    }

    fn active_context(&self, cx: &Context<Self>) -> ContextId {
        if self.command_palette.read(cx).is_visible() {
            return ContextId::CommandPalette;
        }

        if self.sql_preview_modal.read(cx).is_visible() {
            return ContextId::SqlPreviewModal;
        }

        if self.tab_bar.read(cx).has_context_menu_open() {
            return ContextId::ContextMenu;
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

    fn start_pipeline_progress(
        &mut self,
        profile_name: String,
        watcher: dbflux_core::StateWatcher,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let progress = cx.new(|cx| pipeline::PipelineProgress::new(profile_name, watcher, cx));

        let pipeline_profile_name = progress.read(cx).profile_name().to_string();

        let subscription = cx.subscribe_in(
            &progress,
            window,
            move |this, _, event: &pipeline::PipelineProgressEvent, window, cx| {
                match event {
                    pipeline::PipelineProgressEvent::StateChanged(state) => {
                        this.login_modal.update(cx, |modal, cx| {
                            modal.apply_pipeline_state(&pipeline_profile_name, state, window, cx);
                        });
                    }
                    pipeline::PipelineProgressEvent::Completed => {
                        this.pipeline_progress = None;
                        this._pipeline_subscription = None;
                        this.login_modal.update(cx, |modal, cx| {
                            modal.close(cx);
                        });
                        this.app_state.update(cx, |_state, cx| {
                            cx.emit(AppStateChanged);
                        });
                        // Toast is handled by the sidebar connect flow
                    }
                    pipeline::PipelineProgressEvent::Failed { stage, error } => {
                        this.pipeline_progress = None;
                        this._pipeline_subscription = None;
                        log::warn!("Pipeline failed at {}: {}", stage, error);
                    }
                    pipeline::PipelineProgressEvent::Cancelled => {
                        this.pipeline_progress = None;
                        this._pipeline_subscription = None;
                        this.login_modal.update(cx, |modal, cx| {
                            modal.close(cx);
                        });
                    }
                    pipeline::PipelineProgressEvent::WatchClosed { last_state } => {
                        if !matches!(last_state, dbflux_core::PipelineState::Connected) {
                            this.login_modal.update(cx, |modal, cx| {
                                modal.apply_pipeline_state(
                                    &pipeline_profile_name,
                                    last_state,
                                    window,
                                    cx,
                                );
                            });
                        }
                        this.pipeline_progress = None;
                        this._pipeline_subscription = None;
                    }
                }
                cx.notify();
            },
        );

        self.pipeline_progress = Some(progress);
        self._pipeline_subscription = Some(subscription);
        cx.notify();
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
