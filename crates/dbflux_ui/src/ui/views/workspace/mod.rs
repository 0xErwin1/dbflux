mod actions;
mod dispatch;
pub mod inspector;
pub mod pipeline;
mod render;

pub use inspector::{WorkspaceInspector, WorkspaceInspectorEvent};

use crate::app::{AppStateChanged, AppStateEntity};
use dbflux_components;
use dbflux_core::observability::actions::CONFIG_CHANGE;
use dbflux_ui_base::modals::{
    AddPanelOutcome, AddPanelRequest, CreateDashboardOutcome, CreateDashboardRequest,
    DeleteDashboardOutcome, DeleteDashboardRequest, DeleteSavedChartOutcome,
    DeleteSavedChartRequest, ModalAddPanelPicker, ModalCreateDashboard,
    ModalDeleteDashboardConfirm, ModalDeleteSavedChartConfirm, ModalRenameItem, RenameItemOutcome,
    RenameItemRequest, RenameTarget,
};

#[cfg(feature = "mcp")]
use crate::app::McpRuntimeEventRaised;

use crate::keymap::{
    self, Command, CommandDispatcher, ContextId, FocusTarget, KeymapStack, default_keymap,
    key_chord_from_gpui,
};
use crate::ui::components::toast::{Toast, copy_action, now_hms};
use crate::ui::components::toast::{ToastGlobal, ToastHost};
use crate::ui::dock::{SidebarDock, SidebarDockEvent};
use crate::ui::document::{CodeDocument, DataDocument, Tab, TabBar, TabBarEvent, TabManager};

#[cfg(feature = "mcp")]
use crate::ui::document::McpApprovalsView;
use crate::ui::icons::AppIcon;
use crate::ui::overlays::command_palette::{
    CommandPalette, CommandPaletteClosed, PaletteCommand, PaletteItem, PaletteSelection,
    ResourceItem,
};
use crate::ui::overlays::login_modal::{LoginModal, LoginModalEvent};
use crate::ui::overlays::shutdown_overlay::ShutdownOverlay;
use crate::ui::overlays::sql_preview_modal::SqlPreviewModal;
use crate::ui::overlays::sso_wizard::{SsoWizard, SsoWizardEvent};
use crate::ui::tokens::{Heights, Radii, Spacing};
use crate::ui::views::sidebar::{Sidebar, SidebarEvent, SidebarTab};
use crate::ui::views::status_bar::{StatusBar, ToggleTasksPanel};
use crate::ui::views::tasks_panel::TasksPanel;
use crate::ui::windows::connection_manager::ConnectionManagerWindow;
use crate::ui::windows::settings::SettingsWindow;
#[cfg(test)]
use dbflux_core::{CollectionRef, TableRef};
use dbflux_core::{ExecutionContext, QueryLanguage};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Root;
use gpui_component::resizable::{resizable_panel, v_resizable};
use std::path::PathBuf;

/// Extract resource items from a schema snapshot into a palette item list.
///
/// Separated from `Workspace` for testability — this is pure data transformation
/// with no GPUI dependency.
pub(super) fn build_resource_items_from_schema(
    profile_id: uuid::Uuid,
    profile_name: &str,
    structure: &dbflux_core::DataStructure,
    items: &mut Vec<PaletteItem>,
) {
    match structure {
        dbflux_core::DataStructure::Relational(rel) => {
            let database = rel.current_database.clone();
            for table in &rel.tables {
                items.push(PaletteItem::Resource(ResourceItem::Table {
                    profile_id,
                    profile_name: profile_name.to_string(),
                    database: database.clone(),
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                }));
            }
            for view in &rel.views {
                items.push(PaletteItem::Resource(ResourceItem::View {
                    profile_id,
                    profile_name: profile_name.to_string(),
                    database: database.clone(),
                    schema: view.schema.clone(),
                    name: view.name.clone(),
                }));
            }
            for db_schema in &rel.schemas {
                let schema_name = db_schema.name.clone();
                for table in &db_schema.tables {
                    items.push(PaletteItem::Resource(ResourceItem::Table {
                        profile_id,
                        profile_name: profile_name.to_string(),
                        database: database.clone(),
                        schema: Some(schema_name.clone()),
                        name: table.name.clone(),
                    }));
                }
                for view in &db_schema.views {
                    items.push(PaletteItem::Resource(ResourceItem::View {
                        profile_id,
                        profile_name: profile_name.to_string(),
                        database: database.clone(),
                        schema: Some(schema_name.clone()),
                        name: view.name.clone(),
                    }));
                }
            }
        }
        dbflux_core::DataStructure::Document(doc) => {
            let default_db = doc
                .current_database
                .clone()
                .unwrap_or_else(|| "default".to_string());
            for collection in &doc.collections {
                items.push(PaletteItem::Resource(ResourceItem::Collection {
                    profile_id,
                    profile_name: profile_name.to_string(),
                    database: collection
                        .database
                        .clone()
                        .unwrap_or_else(|| default_db.clone()),
                    name: collection.name.clone(),
                }));
            }
        }
        dbflux_core::DataStructure::KeyValue(kv) => {
            for ks in &kv.keyspaces {
                items.push(PaletteItem::Resource(ResourceItem::KeyValueDb {
                    profile_id,
                    profile_name: profile_name.to_string(),
                    database: format!("db{}", ks.db_index),
                }));
            }
        }
        _ => {}
    }
}

/// Map a `PaletteItem` to its corresponding `PaletteSelection`.
///
/// Separated from `CommandPalette` for testability — pure data transformation.
#[cfg(test)]
pub(super) fn map_item_to_selection(item: &PaletteItem) -> Option<PaletteSelection> {
    match item {
        PaletteItem::Action { id, .. } => Some(PaletteSelection::Command { id }),
        PaletteItem::Connection {
            profile_id,
            is_connected,
            ..
        } => {
            if *is_connected {
                Some(PaletteSelection::FocusConnection {
                    profile_id: *profile_id,
                })
            } else {
                Some(PaletteSelection::Connect {
                    profile_id: *profile_id,
                })
            }
        }
        PaletteItem::Resource(r) => match r {
            ResourceItem::Table {
                profile_id,
                schema,
                name,
                database,
                ..
            }
            | ResourceItem::View {
                profile_id,
                schema,
                name,
                database,
                ..
            } => Some(PaletteSelection::OpenTable {
                profile_id: *profile_id,
                table: TableRef {
                    schema: schema.clone(),
                    name: name.clone(),
                },
                database: database.clone(),
            }),
            ResourceItem::Collection {
                profile_id,
                database,
                name,
                ..
            } => Some(PaletteSelection::OpenCollection {
                profile_id: *profile_id,
                collection: CollectionRef {
                    database: database.clone(),
                    name: name.clone(),
                },
            }),
            ResourceItem::KeyValueDb {
                profile_id,
                database,
                ..
            } => Some(PaletteSelection::OpenKeyValue {
                profile_id: *profile_id,
                database: database.clone(),
            }),
        },
        PaletteItem::Script { path, .. } => {
            Some(PaletteSelection::OpenScript { path: path.clone() })
        }
        PaletteItem::SavedChart { id, .. } => {
            Some(PaletteSelection::OpenSavedChart { chart_id: *id })
        }
        PaletteItem::ImportDashboard => Some(PaletteSelection::ImportDashboard),
    }
}

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

/// Deferred routine-definition open (needs `Window` access for CodeDocument creation).
pub(super) struct PendingOpenRoutine {
    pub profile_id: uuid::Uuid,
    pub schema: String,
    pub specific_name: String,
    pub title: String,
    pub body: String,
}

pub struct Workspace {
    app_state: Entity<AppStateEntity>,
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

    workspace_inspector: Entity<inspector::WorkspaceInspector>,
    _workspace_inspector_subscription: Subscription,

    #[cfg(feature = "mcp")]
    mcp_approvals_view: Entity<McpApprovalsView>,

    /// S8 modals — rendered as full-screen overlays via `ModalShell`.
    modal_delete_connection: Entity<crate::ui::overlays::modals::ModalDeleteConnection>,
    modal_unsaved_changes: Entity<crate::ui::overlays::modals::ModalUnsavedChanges>,
    modal_drop_table: Entity<crate::ui::overlays::modals::ModalDropTable>,
    /// Item ID of the drop-table pending delete, consumed when modal confirms.
    pending_drop_table_item_id: Option<String>,
    /// SSH tunnel passphrase modal.
    modal_tunnel_auth: Entity<crate::ui::overlays::modals::ModalTunnelAuth>,
    /// Import Dashboard from JSON modal.
    modal_import_dashboard: Entity<crate::ui::overlays::modals::ModalImportDashboard>,

    /// Dashboard / saved-chart management modals.
    modal_create_dashboard: Entity<ModalCreateDashboard>,
    modal_rename_item: Entity<ModalRenameItem>,
    modal_delete_dashboard: Entity<ModalDeleteDashboardConfirm>,
    modal_delete_saved_chart: Entity<ModalDeleteSavedChartConfirm>,
    modal_add_panel: Entity<ModalAddPanelPicker>,

    tasks_state: PanelState,
    pending_command: Option<&'static str>,
    pending_sql: Option<String>,
    pending_focus: Option<FocusTarget>,
    pending_open_script: Option<PendingOpenScript>,
    pending_open_routine: Option<PendingOpenRoutine>,
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

    /// Pending login modal open request from a settings window auth-profile
    /// login flow. Consumed in render() to call `login_modal.open_manual`.
    ///
    /// Fields: `(provider_name, profile_name, url)`.
    pending_login_modal_open: Option<(String, String, Option<String>)>,
}

#[cfg(feature = "mcp")]
#[derive(Clone, Copy, PartialEq, Eq)]
enum GovernancePanel {
    Approvals,
}

impl Workspace {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
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

        let command_palette = cx.new(|cx| CommandPalette::new(window, cx));

        let sql_preview_modal = cx.new(|cx| SqlPreviewModal::new(app_state.clone(), window, cx));
        let login_modal = cx.new(|cx| LoginModal::new(window, cx));
        let sso_wizard = cx.new(|cx| SsoWizard::new(app_state.clone(), window, cx));
        let shutdown_overlay = cx.new(|cx| ShutdownOverlay::new(app_state.clone(), window, cx));

        let modal_delete_connection =
            cx.new(crate::ui::overlays::modals::ModalDeleteConnection::new);
        let modal_unsaved_changes = cx.new(crate::ui::overlays::modals::ModalUnsavedChanges::new);
        let modal_drop_table =
            cx.new(|cx| crate::ui::overlays::modals::ModalDropTable::new(window, cx));
        let modal_tunnel_auth =
            cx.new(|cx| crate::ui::overlays::modals::ModalTunnelAuth::new(window, cx));
        let modal_import_dashboard =
            cx.new(|cx| crate::ui::overlays::modals::ModalImportDashboard::new(window, cx));

        let modal_create_dashboard = cx.new(|cx| ModalCreateDashboard::new(window, cx));
        let modal_rename_item = cx.new(|cx| ModalRenameItem::new(window, cx));
        let modal_delete_dashboard = cx.new(ModalDeleteDashboardConfirm::new);
        let modal_delete_saved_chart = cx.new(ModalDeleteSavedChartConfirm::new);
        let modal_add_panel = cx.new(|cx| ModalAddPanelPicker::new(window, cx));

        // Subscribe: ModalDeleteConnection — on Confirmed, execute the pending delete.
        cx.subscribe(
            &modal_delete_connection,
            |this, _, outcome: &crate::ui::overlays::modals::DeleteConnectionOutcome, cx| {
                use crate::ui::overlays::modals::DeleteConnectionOutcome;
                log::debug!("ModalDeleteConnection outcome received: {:?}", outcome);
                if matches!(outcome, DeleteConnectionOutcome::Confirmed) {
                    this.sidebar.update(cx, |sidebar, cx| {
                        sidebar.confirm_modal_delete(cx);
                    });
                } else {
                    this.sidebar.update(cx, |sidebar, cx| {
                        sidebar.cancel_modal_delete(cx);
                    });
                }
            },
        )
        .detach();

        // Subscribe: ModalDropTable — on Confirmed, execute the pending DDL drop.
        cx.subscribe(
            &modal_drop_table,
            |this, _, outcome: &crate::ui::overlays::modals::DropTableOutcome, cx| {
                use crate::ui::overlays::modals::DropTableOutcome;
                if matches!(outcome, DropTableOutcome::Confirmed) {
                    this.sidebar.update(cx, |sidebar, cx| {
                        sidebar.confirm_modal_delete(cx);
                    });
                } else {
                    this.sidebar.update(cx, |sidebar, cx| {
                        sidebar.cancel_modal_delete(cx);
                    });
                }
                this.pending_drop_table_item_id = None;
            },
        )
        .detach();

        // Subscribe: ModalTunnelAuth — handle passphrase provided or cancelled.
        cx.subscribe_in(
            &modal_tunnel_auth,
            window,
            |this, _, outcome: &crate::ui::overlays::modals::TunnelAuthOutcome, _window, cx| {
                use crate::ui::overlays::modals::TunnelAuthOutcome;

                match outcome.clone() {
                    TunnelAuthOutcome::Provided {
                        passphrase,
                        remember,
                    } => {
                        // Find the profile waiting for auth.
                        let profile_id = this.sidebar.read(cx).pending_tunnel_auth_profile_id;

                        if let Some(profile_id) = profile_id {
                            if remember {
                                // Cache optimistically: evicted if the connect fails again with
                                // passphrase error (modal reopens with last_attempt_failed=true).
                                this.app_state.update(cx, |state, _cx| {
                                    if let Some(tunnel_id) =
                                        state.ssh_tunnel_id_for_profile(profile_id)
                                    {
                                        state.cache_passphrase(tunnel_id, passphrase.clone());
                                    }
                                });
                            }

                            this.sidebar.update(cx, |sidebar, cx| {
                                sidebar
                                    .connect_to_profile_with_passphrase(profile_id, passphrase, cx);
                            });
                        }
                    }
                    TunnelAuthOutcome::Cancelled => {
                        this.sidebar.update(cx, |sidebar, cx| {
                            sidebar.pending_tunnel_auth_profile_id = None;
                            cx.notify();
                        });
                        this.app_state.update(cx, |_state, cx| {
                            cx.emit(AppStateChanged);
                        });
                    }
                }
            },
        )
        .detach();

        // Subscribe: ModalUnsavedChanges — handle save / discard / cancel outcomes.
        cx.subscribe_in(
            &modal_unsaved_changes,
            window,
            |this, _, outcome: &crate::ui::overlays::modals::UnsavedChangesOutcome, window, cx| {
                use crate::ui::overlays::modals::UnsavedChangesOutcome;
                match outcome {
                    UnsavedChangesOutcome::DiscardAll => {
                        // Close all tabs without saving.
                        let ids: Vec<_> = this
                            .tab_manager
                            .read(cx)
                            .documents()
                            .iter()
                            .map(|d| d.id())
                            .collect();
                        for id in ids {
                            this.close_tab(id, window, cx);
                        }
                    }
                    UnsavedChangesOutcome::SaveSelected(ids) => {
                        let ids = ids.clone();
                        for id in &ids {
                            this.tab_manager.update(cx, |mgr, cx| {
                                if let Some(tab) = mgr.document(*id) {
                                    tab.dispatch_command(
                                        crate::keymap::Command::SaveFileAs,
                                        window,
                                        cx,
                                    );
                                }
                            });
                        }
                    }
                    UnsavedChangesOutcome::Cancelled => {}
                }
            },
        )
        .detach();

        cx.subscribe(&status_bar, |this, _, _: &ToggleTasksPanel, cx| {
            this.toggle_tasks_panel(cx);
        })
        .detach();

        cx.subscribe_in(
            &command_palette,
            window,
            |this, _, event: &PaletteSelection, window, cx| match event {
                PaletteSelection::Command { id } => {
                    this.pending_command = Some(id);
                    cx.notify();
                }
                PaletteSelection::Connect { profile_id } => {
                    this.sidebar.update(cx, |sidebar, cx| {
                        sidebar.connect_to_profile(*profile_id, cx);
                    });
                }
                PaletteSelection::FocusConnection { profile_id } => {
                    // Mirror sidebar's execute_item for connected profiles:
                    // set the connection as active in AppState, then focus the sidebar.
                    this.app_state.update(cx, |state, cx| {
                        state.set_active_connection(*profile_id);
                        cx.emit(AppStateChanged);
                    });
                    if this.is_sidebar_collapsed(cx) {
                        this.toggle_sidebar(cx);
                    }
                    this.pending_focus = Some(FocusTarget::Sidebar);
                    cx.notify();
                }
                PaletteSelection::OpenTable {
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
                PaletteSelection::OpenCollection {
                    profile_id,
                    collection,
                } => {
                    this.open_collection_document(*profile_id, collection.clone(), window, cx);
                }
                PaletteSelection::OpenKeyValue {
                    profile_id,
                    database,
                } => {
                    this.open_key_value_document(*profile_id, database.clone(), window, cx);
                }
                PaletteSelection::OpenScript { path } => {
                    this.open_script_from_path(path.clone(), cx);
                }
                PaletteSelection::OpenSavedChart { chart_id } => {
                    this.open_saved_chart(*chart_id, window, cx);
                }
                PaletteSelection::ImportDashboard => {
                    this.modal_import_dashboard.update(cx, |modal, cx| {
                        modal.open(window, cx);
                    });
                }
            },
        )
        .detach();

        cx.subscribe(&command_palette, |this, _, _: &CommandPaletteClosed, cx| {
            this.needs_focus_restore = true;
            cx.notify();
        })
        .detach();

        // Subscribe: ModalImportDashboard — on Confirmed, run the dashboard import flow.
        cx.subscribe_in(
            &modal_import_dashboard,
            window,
            |this, _, event: &crate::ui::overlays::modals::ImportDashboardConfirmed, window, cx| {
                this.run_dashboard_import(event.json.clone(), event.name.clone(), window, cx);
            },
        )
        .detach();

        // Subscribe: ModalCreateDashboard — on Confirmed, create the dashboard and open it.
        cx.subscribe_in(
            &modal_create_dashboard,
            window,
            |this, _, outcome: &CreateDashboardOutcome, window, cx| {
                if let CreateDashboardOutcome::Confirmed { profile_id, name } = outcome.clone() {
                    this.on_create_dashboard_confirmed(profile_id, name, window, cx);
                }
            },
        )
        .detach();

        // Subscribe: ModalRenameItem — on Confirmed, apply the rename.
        cx.subscribe_in(
            &modal_rename_item,
            window,
            |this, _, outcome: &RenameItemOutcome, window, cx| {
                if let RenameItemOutcome::Confirmed { target, new_name } = outcome.clone() {
                    this.on_rename_item_confirmed(target, new_name, window, cx);
                }
            },
        )
        .detach();

        // Subscribe: ModalDeleteDashboardConfirm — on Confirmed, delete the dashboard.
        cx.subscribe_in(
            &modal_delete_dashboard,
            window,
            |this, _, outcome: &DeleteDashboardOutcome, window, cx| {
                if let DeleteDashboardOutcome::Confirmed { dashboard_id } = *outcome {
                    this.on_delete_dashboard_confirmed(dashboard_id, window, cx);
                }
            },
        )
        .detach();

        // Subscribe: ModalDeleteSavedChartConfirm — on Confirmed, delete the saved chart.
        cx.subscribe_in(
            &modal_delete_saved_chart,
            window,
            |this, _, outcome: &DeleteSavedChartOutcome, window, cx| {
                if let DeleteSavedChartOutcome::Confirmed { chart_id } = *outcome {
                    this.on_delete_saved_chart_confirmed(chart_id, window, cx);
                }
            },
        )
        .detach();

        // Subscribe: ModalAddPanelPicker — on Confirmed, append panels to the dashboard.
        cx.subscribe_in(
            &modal_add_panel,
            window,
            |this, _, outcome: &AddPanelOutcome, window, cx| {
                if let AddPanelOutcome::Confirmed {
                    dashboard_id,
                    chart_ids,
                } = outcome.clone()
                {
                    this.on_add_panels_confirmed(dashboard_id, chart_ids, window, cx);
                }
            },
        )
        .detach();

        cx.subscribe_in(
            &login_modal,
            window,
            |this, _, event: &LoginModalEvent, window, cx| match event {
                LoginModalEvent::OpenAuthProfilesSettings => {
                    let _ = window;
                    this.open_auth_profiles_settings(cx);
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
                SidebarEvent::OpenCollectionChild {
                    profile_id,
                    target,
                    title,
                } => {
                    this.open_event_stream_document(
                        *profile_id,
                        target.clone(),
                        title.clone(),
                        window,
                        cx,
                    );
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
                SidebarEvent::OpenNewQueryWithContent {
                    profile_id,
                    language: _,
                    query,
                } => {
                    // Activate the correct connection first so the new tab is
                    // associated with the right profile.
                    this.app_state.update(cx, |state, _cx| {
                        state.set_active_connection(*profile_id);
                    });

                    this.new_query_tab_with_content(query.clone(), window, cx);
                }
                SidebarEvent::OpenScript { path } => {
                    if dbflux_core::is_openable_script(path) {
                        this.open_script_from_path(path.clone(), cx);
                    } else {
                        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
                        Toast::warning(format!("Unsupported file type: {}", name))
                            .meta_right(now_hms())
                            .push(cx);
                    }
                }
                SidebarEvent::PipelineStarted {
                    profile_name,
                    watcher,
                } => {
                    this.start_pipeline_progress(profile_name.clone(), watcher.clone(), window, cx);
                }
                SidebarEvent::RequestDeleteConnection {
                    connection_name,
                    has_open_documents,
                    ..
                } => {
                    use crate::ui::overlays::modals::DeleteConnectionRequest;
                    let req = DeleteConnectionRequest {
                        connection_name: connection_name.clone(),
                        has_open_documents: *has_open_documents,
                    };
                    this.modal_delete_connection.update(cx, |modal, cx| {
                        modal.open(req, cx);
                    });
                }
                SidebarEvent::RequestDropTable {
                    item_id,
                    table_name,
                    schema_name,
                    dependents,
                } => {
                    use crate::ui::overlays::modals::DropTableRequest;
                    let req = DropTableRequest {
                        table_name: table_name.clone(),
                        schema_name: schema_name.clone(),
                        dependents: dependents.clone(),
                    };
                    this.pending_drop_table_item_id = Some(item_id.clone());
                    this.modal_drop_table.update(cx, |modal, cx| {
                        modal.open(req, window, cx);
                    });
                }
                SidebarEvent::OpenRoutineDefinition {
                    profile_id,
                    schema,
                    specific_name,
                    title,
                } => {
                    this.open_routine_definition(
                        *profile_id,
                        schema.clone(),
                        specific_name.clone(),
                        title.clone(),
                        cx,
                    );
                }
                SidebarEvent::OpenMetricChart {
                    profile_id,
                    namespace,
                    metric_name,
                } => {
                    this.open_metric_chart_from_sidebar(
                        *profile_id,
                        namespace.clone(),
                        metric_name.clone(),
                        window,
                        cx,
                    );
                }
                SidebarEvent::OpenDashboard { dashboard_id } => {
                    this.open_dashboard(*dashboard_id, window, cx);
                }
                SidebarEvent::OpenSavedChart { chart_id } => {
                    this.open_saved_chart(*chart_id, window, cx);
                }
                SidebarEvent::RequestCreateDashboard { profile_id } => {
                    this.create_dashboard_from_sidebar(*profile_id, window, cx);
                }
                SidebarEvent::RequestImportDashboard { profile_id } => {
                    this.import_dashboard_for_profile(*profile_id, window, cx);
                }
                SidebarEvent::RequestRenameDashboard { dashboard_id } => {
                    this.rename_dashboard(*dashboard_id, window, cx);
                }
                SidebarEvent::RequestDeleteDashboard { dashboard_id } => {
                    this.delete_dashboard(*dashboard_id, window, cx);
                }
                SidebarEvent::RequestDuplicateDashboard { dashboard_id } => {
                    this.duplicate_dashboard(*dashboard_id, cx);
                }
                SidebarEvent::RequestRenameSavedChart { chart_id } => {
                    this.rename_saved_chart(*chart_id, window, cx);
                }
                SidebarEvent::RequestDeleteSavedChart { chart_id } => {
                    this.delete_saved_chart(*chart_id, window, cx);
                }
                SidebarEvent::RequestDuplicateSavedChart { chart_id } => {
                    this.duplicate_saved_chart(*chart_id, cx);
                }
                SidebarEvent::RequestTunnelAuth {
                    tunnel_id,
                    tunnel_name,
                    host,
                    port,
                    user,
                    last_attempt_failed,
                    ..
                } => {
                    use crate::ui::overlays::modals::TunnelAuthRequest;
                    let req = TunnelAuthRequest {
                        tunnel_id: *tunnel_id,
                        tunnel_name: tunnel_name.clone(),
                        host: host.clone(),
                        port: *port,
                        user: user.clone(),
                        last_attempt_failed: *last_attempt_failed,
                    };
                    this.modal_tunnel_auth.update(cx, |modal, cx| {
                        modal.open(req, window, cx);
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

        // Create the workspace inspector with the persisted width (or default).
        let initial_inspector_width = {
            let settings = app_state.read(cx).general_settings();
            settings
                .workspace_inspector_width_px
                .map(px)
                .unwrap_or(inspector::INSPECTOR_DEFAULT_WIDTH)
        };
        let workspace_inspector =
            cx.new(|cx| inspector::WorkspaceInspector::new(initial_inspector_width, cx));

        let workspace_inspector_subscription = cx.subscribe(
            &workspace_inspector,
            |this, _, event: &inspector::WorkspaceInspectorEvent, cx| match event {
                inspector::WorkspaceInspectorEvent::ResizeCommitted(px_width) => {
                    this.persist_inspector_width(*px_width, cx);
                }
                inspector::WorkspaceInspectorEvent::Closed => {
                    cx.notify();
                }
            },
        );

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
                    TabManagerEvent::OpenInspector { title, content } => {
                        this.workspace_inspector.update(cx, |insp, cx| {
                            insp.open_with(content.clone(), title.clone(), cx);
                        });
                    }
                    TabManagerEvent::Activated(new_id) => {
                        let doc_ids: Vec<_> = this
                            .tab_manager
                            .read(cx)
                            .documents()
                            .iter()
                            .map(|d| (d.id(), d.id() == *new_id))
                            .collect();

                        for (id, is_active) in doc_ids {
                            this.tab_manager.update(cx, |mgr, cx| {
                                if let Some(tab) = mgr.document(id) {
                                    tab.set_active_tab(is_active, cx);
                                }
                            });
                        }

                        this.write_session_manifest(cx);
                    }
                    TabManagerEvent::ChartThisQuery {
                        query,
                        connection_id,
                    } => {
                        this.open_chart_from_query(query.clone(), *connection_id, window, cx);
                    }
                    TabManagerEvent::RequestAddPanel { dashboard_id } => {
                        this.open_add_panel_picker(*dashboard_id, window, cx);
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
            workspace_inspector,
            _workspace_inspector_subscription: workspace_inspector_subscription,
            #[cfg(feature = "mcp")]
            mcp_approvals_view,
            modal_delete_connection,
            modal_unsaved_changes,
            modal_drop_table,
            pending_drop_table_item_id: None,
            modal_tunnel_auth,
            modal_import_dashboard,
            modal_create_dashboard,
            modal_rename_item,
            modal_delete_dashboard,
            modal_delete_saved_chart,
            modal_add_panel,
            tasks_state: PanelState::Collapsed,
            pending_command: None,
            pending_sql: None,
            pending_focus: None,
            pending_open_script: None,
            pending_open_routine: None,
            needs_focus_restore: false,
            pipeline_progress: None,
            _pipeline_subscription: None,
            focus_target: FocusTarget::default(),
            keymap: default_keymap(),
            focus_handle,
            #[cfg(feature = "mcp")]
            active_governance_panel: None,
            _background_purge_task: None,
            pending_login_modal_open: None,
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
        // Shortcut labels for the command palette. The strings here are in
        // the kebab-case form expected by `palette_shortcut_parts` so they
        // render as a multi-badge `Chord` (e.g. `[Ctrl] + [N]`) rather than a
        // single collapsed token.
        //
        // The primary-modifier bindings in `keymap::defaults` use Cmd on
        // macOS and Ctrl elsewhere, so the labels below mirror that. Bindings
        // kept literal on every platform (Ctrl+Tab, Ctrl+Shift+1..4) keep
        // `ctrl-` here as well.
        struct ShortcutLabels {
            new_query_tab: &'static str,
            run_query: &'static str,
            run_query_in_new_tab: &'static str,
            save_query: &'static str,
            save_file_as: &'static str,
            open_script_file: &'static str,
            open_history: &'static str,
            close_tab: &'static str,
            export_results: &'static str,
            toggle_sidebar: &'static str,
            open_audit_viewer: &'static str,
        }

        #[cfg(target_os = "macos")]
        const SC: ShortcutLabels = ShortcutLabels {
            new_query_tab: "cmd-n",
            run_query: "cmd-enter",
            run_query_in_new_tab: "cmd-shift-enter",
            save_query: "cmd-s",
            save_file_as: "cmd-shift-s",
            open_script_file: "cmd-o",
            open_history: "cmd-p",
            close_tab: "cmd-w",
            export_results: "cmd-e",
            toggle_sidebar: "cmd-b",
            open_audit_viewer: "cmd-shift-a",
        };
        #[cfg(not(target_os = "macos"))]
        const SC: ShortcutLabels = ShortcutLabels {
            new_query_tab: "ctrl-n",
            run_query: "ctrl-enter",
            run_query_in_new_tab: "ctrl-shift-enter",
            save_query: "ctrl-s",
            save_file_as: "ctrl-shift-s",
            open_script_file: "ctrl-o",
            open_history: "ctrl-p",
            close_tab: "ctrl-w",
            export_results: "ctrl-e",
            toggle_sidebar: "ctrl-b",
            open_audit_viewer: "ctrl-shift-a",
        };

        vec![
            // Editor
            PaletteCommand::new("new_query_tab", "New Query Tab", "Editor")
                .with_shortcut(SC.new_query_tab),
            PaletteCommand::new("run_query", "Run Query", "Editor").with_shortcut(SC.run_query),
            PaletteCommand::new("run_query_in_new_tab", "Run Query in New Tab", "Editor")
                .with_shortcut(SC.run_query_in_new_tab),
            PaletteCommand::new("save_query", "Save Query", "Editor").with_shortcut(SC.save_query),
            PaletteCommand::new("save_file_as", "Save File As", "Editor")
                .with_shortcut(SC.save_file_as),
            PaletteCommand::new("open_script_file", "Open Script File", "Editor")
                .with_shortcut(SC.open_script_file),
            PaletteCommand::new("open_history", "Open Query History", "Editor")
                .with_shortcut(SC.open_history),
            PaletteCommand::new("cancel_query", "Cancel Running Query", "Editor")
                .with_shortcut("esc"),
            // Tabs — Ctrl+Tab / Ctrl+Shift+Tab stay literal Ctrl on every
            // platform (Cmd+Tab is the macOS app switcher).
            PaletteCommand::new("close_tab", "Close Current Tab", "Tabs")
                .with_shortcut(SC.close_tab),
            PaletteCommand::new("next_tab", "Next Tab", "Tabs").with_shortcut("ctrl-tab"),
            PaletteCommand::new("prev_tab", "Previous Tab", "Tabs").with_shortcut("ctrl-shift-tab"),
            // Results
            PaletteCommand::new("export_results", "Export Results", "Results")
                .with_shortcut(SC.export_results),
            // Connections
            PaletteCommand::new(
                "open_connection_manager",
                "Open Connection Manager",
                "Connections",
            ),
            PaletteCommand::new("disconnect", "Disconnect Current", "Connections"),
            PaletteCommand::new("refresh_schema", "Refresh Schema", "Connections"),
            // Focus — Ctrl+Shift+1..4 stay literal Ctrl on every platform
            // (Cmd+Shift+3/4 are macOS screenshot shortcuts).
            PaletteCommand::new("focus_sidebar", "Focus Sidebar", "Focus")
                .with_shortcut("ctrl-shift-1"),
            PaletteCommand::new("focus_editor", "Focus Editor", "Focus")
                .with_shortcut("ctrl-shift-2"),
            PaletteCommand::new("focus_results", "Focus Results", "Focus")
                .with_shortcut("ctrl-shift-3"),
            PaletteCommand::new("focus_tasks", "Focus Tasks Panel", "Focus")
                .with_shortcut("ctrl-shift-4"),
            // View
            PaletteCommand::new("toggle_sidebar", "Toggle Sidebar", "View")
                .with_shortcut(SC.toggle_sidebar),
            PaletteCommand::new("toggle_editor", "Toggle Editor Panel", "View"),
            PaletteCommand::new("toggle_results", "Toggle Results Panel", "View"),
            PaletteCommand::new("toggle_tasks", "Toggle Tasks Panel", "View"),
            PaletteCommand::new("open_settings", "Open Settings", "View"),
            PaletteCommand::new("open_login_modal", "Open Auth Profile Login", "View"),
            PaletteCommand::new("open_sso_wizard", "Open AWS SSO Wizard", "View"),
            #[cfg(feature = "mcp")]
            PaletteCommand::new("open_mcp_approvals", "Open MCP Approvals", "View"),
            #[cfg(feature = "mcp")]
            PaletteCommand::new("refresh_mcp_governance", "Refresh MCP Governance", "View"),
            PaletteCommand::new("open_audit_viewer", "Open Audit Viewer", "View")
                .with_shortcut(SC.open_audit_viewer),
            // Charts / Dashboards
            PaletteCommand::new("open_saved_chart", "Open Chart...", "Charts"),
            PaletteCommand::new("new_dashboard", "New Dashboard...", "Dashboards"),
        ]
    }

    /// Test-only accessor to the list of default palette commands.
    ///
    /// Used by command_palette tests to verify command labels without
    /// constructing a full `Workspace` entity.
    #[cfg(test)]
    pub fn palette_commands_for_test() -> Vec<PaletteCommand> {
        Self::default_commands()
    }

    fn active_context(&self, cx: &Context<Self>) -> ContextId {
        if self.command_palette.read(cx).is_visible() {
            return ContextId::CommandPalette;
        }

        if self.sidebar.read(cx).has_child_picker_open() {
            // When the filter input inside the picker is focused, defer to the
            // text-input keymap so typing does not trigger list navigation.
            if self.sidebar.read(cx).child_picker_filter_is_focused() {
                return ContextId::TextInput;
            }
            return ContextId::EventStreamsPicker;
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

        if self.focus_target == FocusTarget::Sidebar
            && self.sidebar.read(cx).search_input_has_focus_state()
        {
            return ContextId::TextInput;
        }

        // When focused on document area, delegate context to the active document
        if self.focus_target == FocusTarget::Document
            && let Some(tab) = self.tab_manager.read(cx).active_tab()
        {
            return tab.active_context(cx);
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

        if target == FocusTarget::Sidebar {
            self.focus_handle.focus(window);
        }

        if target == FocusTarget::Document {
            self.tab_manager
                .update(cx, |mgr, cx| mgr.focus_active(window, cx));
        }

        cx.notify();
    }

    pub fn toggle_command_palette(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let was_visible = self.command_palette.read(cx).is_visible();

        if !was_visible {
            let items = self.build_palette_items(cx);
            self.command_palette.update(cx, |palette, cx| {
                palette.open_with_items(items, window, cx);
            });
        } else {
            self.command_palette.update(cx, |palette, cx| {
                palette.hide(cx);
            });
            self.set_focus(self.focus_target, window, cx);
        }
    }

    /// Build the palette item list from current app state.
    fn build_palette_items(&self, cx: &Context<Self>) -> Vec<PaletteItem> {
        let mut items: Vec<PaletteItem> = Self::default_commands()
            .into_iter()
            .map(|cmd| cmd.into())
            .collect();

        let app_state = self.app_state.read(cx);
        let connections = app_state.connections();

        for profile in app_state.profiles() {
            let is_connected = connections.contains_key(&profile.id);
            items.push(PaletteItem::Connection {
                profile_id: profile.id,
                name: profile.name.clone(),
                is_connected,
            });
        }

        for (&profile_id, connected) in connections.iter() {
            let profile_name = connected.profile.name.clone();

            if let Some(schema) = &connected.schema {
                build_resource_items_from_schema(
                    profile_id,
                    &profile_name,
                    &schema.structure,
                    &mut items,
                );
            }
        }

        if let Some(dir) = app_state.scripts_directory() {
            let root = dir.root_path().to_path_buf();
            Self::flatten_script_entries(dir.entries(), &root, &mut items);
        }

        // Add the "Import Dashboard from JSON" entry only when the active
        // connection advertises the DASHBOARD_IMPORT capability.
        if app_state.active_connection().is_some_and(|a| {
            a.connection
                .metadata()
                .capabilities
                .contains(dbflux_core::DriverCapabilities::DASHBOARD_IMPORT)
        }) {
            items.push(PaletteItem::ImportDashboard);
        }

        items
    }

    /// Recursively flatten script directory entries into palette items.
    fn flatten_script_entries(
        entries: &[dbflux_core::ScriptEntry],
        scripts_root: &std::path::Path,
        items: &mut Vec<PaletteItem>,
    ) {
        use dbflux_core::ScriptEntry;

        for entry in entries {
            match entry {
                ScriptEntry::File { path, name, .. } => {
                    if !dbflux_core::is_openable_script(path) {
                        continue;
                    }
                    let relative_path = path
                        .strip_prefix(scripts_root)
                        .unwrap_or(path)
                        .to_string_lossy()
                        .to_string();
                    items.push(PaletteItem::Script {
                        path: path.clone(),
                        name: name.clone(),
                        relative_path,
                    });
                }
                ScriptEntry::Folder { children, .. } => {
                    Self::flatten_script_entries(children, scripts_root, items);
                }
            }
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
                        if matches!(last_state, dbflux_core::PipelineState::Connected) {
                            // The pipeline completed successfully but the watch channel sender
                            // was dropped before the poll task could observe it via changed().
                            // Treat this as Completed: the connection succeeded.
                            this.login_modal.update(cx, |modal, cx| {
                                modal.close(cx);
                            });
                        } else {
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

    /// Persist the inspector width to `GeneralSettings` and save to disk.
    fn persist_inspector_width(&mut self, width: Pixels, cx: &mut Context<Self>) {
        let runtime = self.app_state.read(cx).storage_runtime();
        let mut settings = self.app_state.read(cx).general_settings().clone();
        settings.workspace_inspector_width_px = Some(f32::from(width));

        if let Err(e) = dbflux_app::config_loader::save_general_settings(runtime, &settings) {
            log::warn!("Failed to persist inspector width: {}", e);
        }

        self.app_state.update(cx, |state, _cx| {
            state.update_general_settings(settings);
        });
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
