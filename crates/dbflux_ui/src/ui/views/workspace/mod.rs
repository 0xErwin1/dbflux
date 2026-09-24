mod actions;
mod dispatch;
pub mod inspector;
pub mod pipeline;
mod render;

pub use inspector::{WorkspaceInspector, WorkspaceInspectorEvent};

use crate::app::{AppStateChanged, AppStateEntity};
use dbflux_components;
use dbflux_core::observability::actions::CONFIG_CHANGE;
use dbflux_ui_base::app_state_entity::drain_scripts_directory_diagnostics;
use dbflux_ui_base::modals::{
    AddPanelOutcome, AddPanelRequest, CreateDashboardOutcome, CreateDashboardRequest,
    DeleteDashboardOutcome, DeleteDashboardRequest, DeleteSavedChartOutcome,
    DeleteSavedChartRequest, ModalAddPanelPicker, ModalCreateDashboard,
    ModalDeleteDashboardConfirm, ModalDeleteSavedChartConfirm, ModalRenameItem, RenameItemOutcome,
    RenameItemRequest, RenameTarget, RequestMetricsForNamespace,
};
use dbflux_ui_base::{
    AppStateGlobal, OpenAuditRequested, drain_hook_load_diagnostics, report_error,
};

#[cfg(feature = "mcp")]
use crate::app::McpRuntimeEventRaised;

use crate::keymap::{
    self, Command, CommandDispatcher, ContextId, FocusTarget, KeymapStack, default_keymap,
    key_chord_from_gpui,
};
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
use crate::ui::views::status_bar::{StatusBar, ToggleTasksPanel};
use crate::ui::views::tasks_panel::TasksPanel;
use dbflux_components::tokens::{Heights, Radii, Spacing};
#[cfg(test)]
use dbflux_core::{CollectionRef, TableRef};
use dbflux_core::{ExecutionContext, QueryLanguage};
use dbflux_ui_base::toast::{Toast, ToastGlobal, ToastHost, copy_action, now_hms};
use dbflux_ui_sidebar::{Sidebar, SidebarEvent, SidebarTab};
use dbflux_ui_windows::connection_manager::ConnectionManagerWindow;
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

/// How a bounded document flush ended.
///
/// A bool would read as "did the flush finish", which hides the difference the
/// exit report needs: writes that drained, and a deadline that expired with
/// writes still in flight. A drained flush also says nothing about the edits
/// having reached their files, so neither outcome is a successful save.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentFlushOutcome {
    /// The poll reported nothing outstanding before the deadline.
    Drained,
    /// Writes were still outstanding when the deadline expired.
    TimedOut,
}

/// Polls a shutdown flush until it reports idle or `timeout` elapses.
///
/// Returns [`DocumentFlushOutcome::Drained`] when nothing was outstanding before
/// the deadline and [`DocumentFlushOutcome::TimedOut`] when the deadline expired
/// first; the caller continues either way, so a write that never lands can only
/// delay shutdown by `timeout`. Each iteration waits `poll_interval` on the
/// executor, which is what lets queued writes run between polls. The executor's
/// clock is used instead of `Instant::now` so the loop is deterministic under a
/// test executor.
pub async fn await_document_flush<F>(
    cx: &mut AsyncApp,
    timeout: std::time::Duration,
    poll_interval: std::time::Duration,
    mut is_outstanding: F,
) -> DocumentFlushOutcome
where
    F: FnMut(&mut AsyncApp) -> bool,
{
    let deadline = cx.background_executor().now() + timeout;

    loop {
        if !is_outstanding(cx) {
            return DocumentFlushOutcome::Drained;
        }

        if cx.background_executor().now() > deadline {
            return DocumentFlushOutcome::TimedOut;
        }

        cx.background_executor().timer(poll_interval).await;
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
    /// Editor body with any leading annotation header stripped.
    pub body: String,
    /// Raw bytes exactly as read from disk, annotation header included. The
    /// document records these as its physical baseline so autosave compares
    /// against what is actually on disk.
    pub raw: String,
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
    /// "Active query running" prompt shown before a disconnect or quit that
    /// would abandon a running query.
    modal_active_query: Entity<crate::ui::overlays::modals::ModalActiveQuery>,
    /// What the open active-query prompt is guarding, consumed when the user
    /// chooses an outcome.
    pending_active_query: Option<ActiveQueryScope>,
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

    /// In-app single-connection export modal (overlay, not an OS window).
    export_modal: Entity<dbflux_ui_windows::connection_manager::ExportBundleModal>,
    /// Import wizard (folder bundle -> tables), targeting the connection it
    /// was opened from.
    import_wizard: Entity<dbflux_ui_document::import_wizard::ImportWizard>,
    /// Migrate wizard (table -> table, cross-connection), pre-populated from
    /// the sidebar's multi-select Migrate action.
    migrate_wizard: Entity<dbflux_ui_document::migrate_wizard::MigrateWizard>,
    /// Export wizard (table -> file bundle), pre-populated from the
    /// sidebar's multi-select Export action.
    export_wizard: Entity<dbflux_ui_document::export_wizard::ExportWizard>,

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

/// The operation the active-query prompt interrupted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActiveQueryScope {
    /// Disconnecting this connection.
    Disconnect(uuid::Uuid),
    /// Quitting the application.
    Quit,
}

/// Emitted when the user chose to quit even though queries are still
/// running. The application shell owns shutdown and starts it on this event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuitConfirmed;

impl EventEmitter<QuitConfirmed> for Workspace {}

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
        cx.set_global(AppStateGlobal {
            entity: app_state.clone(),
        });

        let hook_load_errors = app_state.update(cx, |state, _| {
            drain_hook_load_diagnostics(&mut state.hook_load_diagnostics)
        });
        for error in hook_load_errors {
            report_error(error, cx);
        }

        let scripts_directory_errors = app_state.update(cx, |state, _| {
            drain_scripts_directory_diagnostics(&mut state.scripts_directory_diagnostics)
        });
        for error in scripts_directory_errors {
            report_error(error, cx);
        }

        let sidebar = cx.new(|cx| Sidebar::new(app_state.clone(), window, cx));
        let sidebar_dock = cx.new(|cx| SidebarDock::new(sidebar.clone(), cx));
        let tasks_panel = cx.new(|cx| TasksPanel::new(app_state.clone(), window, cx));

        let tab_manager = cx.new(|_cx| TabManager::new());
        let tab_bar = cx.new(|cx| TabBar::new(tab_manager.clone(), cx));
        let status_bar =
            cx.new(|cx| StatusBar::new(app_state.clone(), tab_manager.clone(), window, cx));

        #[cfg(feature = "mcp")]
        let mcp_approvals_view = cx.new(|_cx| McpApprovalsView::new(app_state.clone()));

        let command_palette = cx.new(|cx| CommandPalette::new(window, cx));

        let sql_preview_modal = cx.new(|cx| SqlPreviewModal::new(app_state.clone(), window, cx));
        let login_modal = cx.new(|cx| LoginModal::new(window, cx));
        let sso_wizard = cx.new(|cx| SsoWizard::new(app_state.clone(), window, cx));
        let shutdown_overlay = cx.new(|cx| ShutdownOverlay::new(app_state.clone(), window, cx));

        let modal_delete_connection =
            cx.new(crate::ui::overlays::modals::ModalDeleteConnection::new);
        let modal_active_query = cx.new(crate::ui::overlays::modals::ModalActiveQuery::new);
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

        let export_modal = cx.new(|cx| {
            dbflux_ui_windows::connection_manager::ExportBundleModal::new(
                app_state.clone(),
                window,
                cx,
            )
        });
        let import_wizard = cx
            .new(|cx| dbflux_ui_document::import_wizard::ImportWizard::new(app_state.clone(), cx));
        let migrate_wizard = cx.new(|cx| {
            dbflux_ui_document::migrate_wizard::MigrateWizard::new(app_state.clone(), cx)
        });
        let export_wizard = cx.new(|cx| {
            dbflux_ui_document::export_wizard::ExportWizard::new(app_state.clone(), window, cx)
        });

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

        // Subscribe: ModalActiveQuery — apply the choice to the disconnect or
        // quit it interrupted.
        cx.subscribe_in(
            &modal_active_query,
            window,
            |this, _, outcome: &crate::ui::overlays::modals::ActiveQueryOutcome, window, cx| {
                this.resolve_active_query(outcome, window, cx);
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
                    UnsavedChangesOutcome::DiscardAll(ids) => {
                        // Close the documents the dialog listed, nothing else:
                        // every other tab keeps its changes. Discarding must not
                        // go back through `close_tab`, because it would re-enter
                        // this dialog's own gate for exactly the documents it
                        // listed, and the funnel would save the edits the user
                        // just chose to drop. Removing the tab is the discard.
                        for id in ids {
                            this.close_tab_now(*id, window, cx);
                        }
                        this.tab_manager
                            .update(cx, |mgr, cx| mgr.focus_active(window, cx));
                    }
                    UnsavedChangesOutcome::SaveSelected(ids) => {
                        use crate::ui::overlays::modals::CloseAction;

                        let ids = ids.clone();
                        let mut unsaveable = 0;
                        for id in &ids {
                            // Each document says which action its pending edits
                            // need: a code document writes its own file in place
                            // (and only opens Save As for an untitled buffer),
                            // while a grid applies its staged edits to the
                            // database. Both report back through `RequestClose`
                            // once the work lands, so a dismissed dialog or a
                            // failed write leaves the tab open with its changes.
                            let started = this.tab_manager.update(cx, |mgr, cx| {
                                let Some(tab) = mgr.document(*id) else {
                                    return false;
                                };

                                match tab.as_pane().close_action() {
                                    CloseAction::Apply => tab.as_pane().apply_for_close(window, cx),
                                    CloseAction::Save => tab.as_pane().save_for_close(window, cx),
                                }
                            });

                            if !started {
                                unsaveable += 1;
                            }
                        }

                        if unsaveable > 0 {
                            Toast::warning(crate::ui::labels::unsaved_changes_cannot_save_message(
                                unsaveable,
                            ))
                            .meta_right(now_hms())
                            .push(cx);
                        }

                        // The dialog took the keyboard, and the tabs it asked
                        // to save stay open until their writes land. Give the
                        // document its keyboard back instead of leaving the user
                        // without one for the whole write.
                        this.set_focus(this.focus_target, window, cx);
                    }
                    UnsavedChangesOutcome::Cancelled => {
                        // The modal stole focus from the editor input when it
                        // opened; give it back so typing continues seamlessly.
                        this.set_focus(this.focus_target, window, cx);
                    }
                }
            },
        )
        .detach();

        cx.subscribe(&status_bar, |this, _, _: &ToggleTasksPanel, cx| {
            this.toggle_tasks_panel(cx);
        })
        .detach();

        cx.subscribe_in(
            &app_state,
            window,
            |this, _, event: &OpenAuditRequested, window, cx| {
                this.open_audit_viewer_with_correlation(event.0, window, cx);
            },
        )
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

        // Subscribe: ModalAddPanelPicker — handle all three submission paths.
        cx.subscribe_in(
            &modal_add_panel,
            window,
            |this, _, outcome: &AddPanelOutcome, window, cx| match outcome.clone() {
                AddPanelOutcome::Confirmed {
                    dashboard_id,
                    chart_ids,
                } => {
                    this.on_add_panels_confirmed(dashboard_id, chart_ids, window, cx);
                }
                AddPanelOutcome::CreateFromQuery {
                    dashboard_id,
                    profile_id,
                    name,
                    query,
                    chart_kind,
                } => {
                    this.on_create_panel_from_query(
                        dashboard_id,
                        profile_id,
                        name,
                        query,
                        chart_kind,
                        window,
                        cx,
                    );
                }
                AddPanelOutcome::CreateFromMetric {
                    dashboard_id,
                    profile_id,
                    name,
                    namespace,
                    metric_name,
                    dimensions,
                    period_seconds,
                    statistic,
                } => {
                    this.on_create_panel_from_metric(
                        dashboard_id,
                        profile_id,
                        name,
                        namespace,
                        metric_name,
                        dimensions,
                        period_seconds,
                        statistic,
                        window,
                        cx,
                    );
                }
                AddPanelOutcome::Cancelled => {}
            },
        )
        .detach();

        // Subscribe: ModalAddPanelPicker — fetch metrics for a namespace on demand.
        cx.subscribe_in(
            &modal_add_panel,
            window,
            |this, modal, ev: &RequestMetricsForNamespace, _window, cx| {
                this.on_request_metrics_for_namespace(modal.clone(), ev.clone(), cx);
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
                SidebarEvent::OpenSchemaViz {
                    profile_id,
                    database,
                    schema,
                    table,
                } => {
                    this.open_schema_viz_document(
                        *profile_id,
                        database.clone(),
                        schema.clone(),
                        table.clone(),
                        window,
                        cx,
                    );
                }
                SidebarEvent::OpenGlobalSchemaViz {
                    profile_id,
                    database,
                } => {
                    this.open_global_schema_viz_document(*profile_id, database.clone(), window, cx);
                }
                SidebarEvent::OpenObjectStoreBuckets { profile_id } => {
                    this.open_object_store_buckets_document(*profile_id, window, cx);
                }
                SidebarEvent::OpenObjectStoreBucket { profile_id, bucket } => {
                    this.open_object_browser(*profile_id, bucket.clone(), window, cx);
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
                SidebarEvent::RequestActiveQueryDisconnect { profile_id } => {
                    // The query may have finished between the sidebar's check
                    // and this handler; then there is nothing to ask about.
                    if !this.prompt_active_query(
                        ActiveQueryScope::Disconnect(*profile_id),
                        window,
                        cx,
                    ) {
                        this.sidebar.update(cx, |sidebar, cx| {
                            sidebar.disconnect_profile(*profile_id, cx);
                        });
                    }
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
                SidebarEvent::OpenRemoteDashboard { profile_id, name } => {
                    this.open_remote_dashboard(*profile_id, name.clone(), window, cx);
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
                SidebarEvent::OpenInstanceMetric {
                    profile_id,
                    metric_id,
                } => {
                    this.open_instance_metric(*profile_id, metric_id.clone(), window, cx);
                }
                SidebarEvent::OpenInstanceInspector {
                    profile_id,
                    metric_id,
                } => {
                    this.open_instance_inspector(*profile_id, metric_id.clone(), window, cx);
                }
                SidebarEvent::OpenInstanceOverview { profile_id } => {
                    this.open_instance_overview(*profile_id, window, cx);
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
                SidebarEvent::RequestExportConnection { profile_id } => {
                    this.open_export_connection_modal(*profile_id, window, cx);
                }
                SidebarEvent::RequestImportWizard {
                    profile_id,
                    database,
                } => {
                    let profile_id = *profile_id;
                    let database = database.clone();
                    this.import_wizard.update(cx, |wizard, cx| {
                        wizard.open(profile_id, database, window, cx);
                    });
                }
                SidebarEvent::RequestMigrateWizard {
                    profile_id,
                    database,
                    tables,
                } => {
                    let profile_id = *profile_id;
                    let database = database.clone();
                    let tables = tables.clone();
                    this.migrate_wizard.update(cx, |wizard, cx| {
                        wizard.open(profile_id, database, tables, window, cx);
                    });
                }
                SidebarEvent::RequestSchemaDiff {
                    profile_id,
                    database,
                } => {
                    this.open_schema_diff(*profile_id, database.clone(), window, cx);
                }
                SidebarEvent::RequestExportWizard {
                    profile_id,
                    database,
                    tables,
                } => {
                    let profile_id = *profile_id;
                    let database = database.clone();
                    let tables = tables.clone();
                    this.export_wizard.update(cx, |wizard, cx| {
                        wizard.open(profile_id, database, tables, window, cx);
                    });
                }
                SidebarEvent::RequestOpenSettings => {
                    this.open_settings(cx);
                }
                SidebarEvent::RequestOpenConnectionManager => {
                    this.open_connection_manager(cx);
                }
                SidebarEvent::RequestEditConnection { profile_id } => {
                    this.open_connection_manager_for_edit(*profile_id, cx);
                }
                SidebarEvent::RequestOpenConnectionManagerInFolder { folder_id } => {
                    this.open_connection_manager_in_folder(*folder_id, cx);
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
                    let keep = *id;
                    this.close_tabs_by(window, cx, |ids| {
                        TabManager::ids_to_close_others(ids, keep)
                    });
                }
                TabBarEvent::CloseAllTabs => {
                    this.close_tabs_by(window, cx, |ids| ids.to_vec());
                }
                TabBarEvent::CloseTabsToLeft(id) => {
                    let target = *id;
                    this.close_tabs_by(window, cx, |ids| {
                        TabManager::ids_to_close_left(ids, target)
                    });
                }
                TabBarEvent::CloseTabsToRight(id) => {
                    let target = *id;
                    this.close_tabs_by(window, cx, |ids| {
                        TabManager::ids_to_close_right(ids, target)
                    });
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
                    // Propagate explicit user dismissal to the active document
                    // so it forgets its inspector state and does not re-open
                    // the rail on the next tab activation or refresh.
                    let active_id = this.tab_manager.read(cx).active_id();
                    if let Some(id) = active_id {
                        this.tab_manager.update(cx, |mgr, cx| {
                            if let Some(tab) = mgr.document(id) {
                                tab.mark_inspector_closed(cx);
                            }
                        });
                    }
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
                    TabManagerEvent::CloseInspector => {
                        this.workspace_inspector.update(cx, |insp, cx| {
                            insp.hide(cx);
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

                        // The newly active tab is authoritative over the rail.
                        // Hide it first: an `OpenInspector` the tab emits from
                        // `set_active_tab` is queued behind this and re-opens
                        // it, while a tab that owns nothing leaves it hidden.
                        // `hide`, not `close`: `close` emits `Closed`, which
                        // would wipe the new tab's saved rail state.
                        this.workspace_inspector.update(cx, |insp, cx| {
                            insp.hide(cx);
                        });

                        // Hide inactive documents first, then mount the newly
                        // active inspector last. Otherwise document ordering
                        // could let an old tab's CloseInspector event win and
                        // leave stale or hidden rail content after a switch.
                        for (id, is_active) in
                            doc_ids.iter().copied().filter(|(_, is_active)| !*is_active)
                        {
                            this.tab_manager.update(cx, |mgr, cx| {
                                if let Some(tab) = mgr.document(id) {
                                    tab.set_active_tab(is_active, cx);
                                }
                            });
                        }

                        for (id, is_active) in
                            doc_ids.iter().copied().filter(|(_, is_active)| *is_active)
                        {
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
                    TabManagerEvent::RequestSaveAsEditable {
                        source_title,
                        profile_id,
                    } => {
                        this.save_overview_as_editable(
                            source_title.clone(),
                            *profile_id,
                            window,
                            cx,
                        );
                    }
                    TabManagerEvent::OpenEditorWithContent { sql, .. } => {
                        this.new_query_tab_with_content(sql.clone(), window, cx);
                    }
                    TabManagerEvent::SaveFinished { id, succeeded } => {
                        if !succeeded && this.tab_manager.read(cx).active_id() == Some(*id) {
                            // Save As was dismissed or the write failed: the tab
                            // keeps its changes and gets the keyboard back.
                            this.set_focus(this.focus_target, window, cx);
                        }
                    }
                    TabManagerEvent::RequestClose { id } => {
                        // The document finished the save an interrupted close
                        // asked for; the tab is safe to close now.
                        this.close_tab(*id, window, cx);
                        this.tab_manager
                            .update(cx, |mgr, cx| mgr.focus_active(window, cx));
                    }
                    TabManagerEvent::Closed(_) => {
                        // With a tab left, the `Activated` that follows hands
                        // the rail over. With none, nothing owns it any more.
                        if this.tab_manager.read(cx).active_id().is_none() {
                            this.workspace_inspector.update(cx, |insp, cx| {
                                insp.hide(cx);
                            });
                        }

                        this.write_session_manifest(cx);
                    }
                    TabManagerEvent::Opened(_) | TabManagerEvent::Reordered => {
                        this.write_session_manifest(cx);
                    }
                }
            },
        )
        .detach();

        let focus_handle = cx.focus_handle();
        focus_handle.focus(window, cx);

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
            modal_active_query,
            pending_active_query: None,
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
            export_modal,
            import_wizard,
            migrate_wizard,
            export_wizard,
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
                            });

                        // Get audit_service for purge and emit from foreground update.
                        let purge_result = cx.update(|cx| {
                            let audit_service = app_state.read(cx).audit_service().clone();
                            audit_service.purge_old_events(retention_days, 500)
                        });

                        match purge_result {
                            Ok(stats) => {
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
                                cx.update(|cx| {
                                    let audit_service = app_state.read(cx).audit_service().clone();
                                    if let Err(rec_err) = audit_service.record(event) {
                                        log::warn!("Failed to record purge success audit event: {}", rec_err);
                                    }
                                });
                            }
                            Err(e) => {
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
                                cx.update(|cx| {
                                    let audit_service = app_state.read(cx).audit_service().clone();
                                    if let Err(rec_err) = audit_service.record(event) {
                                        log::warn!("Failed to record purge failure audit event: {}", rec_err);
                                    }
                                });
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
            toggle_comment: &'static str,
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
            toggle_comment: "cmd-/",
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
            toggle_comment: "ctrl-/",
            open_history: "ctrl-p",
            close_tab: "ctrl-w",
            export_results: "ctrl-e",
            toggle_sidebar: "ctrl-b",
            open_audit_viewer: "ctrl-shift-a",
        };

        vec![
            // Editor
            PaletteCommand::new(
                "new_query_tab",
                dbflux_i18n::t!("palette.command.new_query_tab.name"),
                dbflux_i18n::t!("palette.category.editor"),
            )
            .with_shortcut(SC.new_query_tab),
            PaletteCommand::new(
                "run_query",
                dbflux_i18n::t!("palette.command.run_query.name"),
                dbflux_i18n::t!("palette.category.editor"),
            )
            .with_shortcut(SC.run_query),
            PaletteCommand::new(
                "run_query_in_new_tab",
                dbflux_i18n::t!("palette.command.run_query_in_new_tab.name"),
                dbflux_i18n::t!("palette.category.editor"),
            )
            .with_shortcut(SC.run_query_in_new_tab),
            PaletteCommand::new(
                "save_query",
                dbflux_i18n::t!("palette.command.save_query.name"),
                dbflux_i18n::t!("palette.category.editor"),
            )
            .with_shortcut(SC.save_query),
            PaletteCommand::new(
                "save_file_as",
                dbflux_i18n::t!("palette.command.save_file_as.name"),
                dbflux_i18n::t!("palette.category.editor"),
            )
            .with_shortcut(SC.save_file_as),
            PaletteCommand::new(
                "open_script_file",
                dbflux_i18n::t!("palette.command.open_script_file.name"),
                dbflux_i18n::t!("palette.category.editor"),
            )
            .with_shortcut(SC.open_script_file),
            PaletteCommand::new(
                "toggle_comment",
                dbflux_i18n::t!("palette.command.toggle_comment.name"),
                dbflux_i18n::t!("palette.category.editor"),
            )
            .with_shortcut(SC.toggle_comment),
            PaletteCommand::new(
                "open_history",
                dbflux_i18n::t!("palette.command.open_history.name"),
                dbflux_i18n::t!("palette.category.editor"),
            )
            .with_shortcut(SC.open_history),
            PaletteCommand::new(
                "cancel_query",
                dbflux_i18n::t!("palette.command.cancel_query.name"),
                dbflux_i18n::t!("palette.category.editor"),
            )
            .with_shortcut("esc"),
            // Tabs — Ctrl+Tab / Ctrl+Shift+Tab stay literal Ctrl on every
            // platform (Cmd+Tab is the macOS app switcher).
            PaletteCommand::new(
                "close_tab",
                dbflux_i18n::t!("palette.command.close_tab.name"),
                dbflux_i18n::t!("palette.category.tabs"),
            )
            .with_shortcut(SC.close_tab),
            PaletteCommand::new(
                "next_tab",
                dbflux_i18n::t!("palette.command.next_tab.name"),
                dbflux_i18n::t!("palette.category.tabs"),
            )
            .with_shortcut("ctrl-tab"),
            PaletteCommand::new(
                "prev_tab",
                dbflux_i18n::t!("palette.command.prev_tab.name"),
                dbflux_i18n::t!("palette.category.tabs"),
            )
            .with_shortcut("ctrl-shift-tab"),
            // Results
            PaletteCommand::new(
                "export_results",
                dbflux_i18n::t!("palette.command.export_results.name"),
                dbflux_i18n::t!("palette.category.results"),
            )
            .with_shortcut(SC.export_results),
            // Connections
            PaletteCommand::new(
                "open_connection_manager",
                dbflux_i18n::t!("palette.command.open_connection_manager.name"),
                dbflux_i18n::t!("palette.category.connections"),
            ),
            PaletteCommand::new(
                "disconnect",
                dbflux_i18n::t!("palette.command.disconnect.name"),
                dbflux_i18n::t!("palette.category.connections"),
            ),
            PaletteCommand::new(
                "refresh_schema",
                dbflux_i18n::t!("palette.command.refresh_schema.name"),
                dbflux_i18n::t!("palette.category.connections"),
            ),
            // Focus — Ctrl+Shift+1..4 stay literal Ctrl on every platform
            // (Cmd+Shift+3/4 are macOS screenshot shortcuts).
            PaletteCommand::new(
                "focus_sidebar",
                dbflux_i18n::t!("palette.command.focus_sidebar.name"),
                dbflux_i18n::t!("palette.category.focus"),
            )
            .with_shortcut("ctrl-shift-1"),
            PaletteCommand::new(
                "focus_editor",
                dbflux_i18n::t!("palette.command.focus_editor.name"),
                dbflux_i18n::t!("palette.category.focus"),
            )
            .with_shortcut("ctrl-shift-2"),
            PaletteCommand::new(
                "focus_results",
                dbflux_i18n::t!("palette.command.focus_results.name"),
                dbflux_i18n::t!("palette.category.focus"),
            )
            .with_shortcut("ctrl-shift-3"),
            PaletteCommand::new(
                "focus_tasks",
                dbflux_i18n::t!("palette.command.focus_tasks.name"),
                dbflux_i18n::t!("palette.category.focus"),
            )
            .with_shortcut("ctrl-shift-4"),
            // View
            PaletteCommand::new(
                "toggle_sidebar",
                dbflux_i18n::t!("palette.command.toggle_sidebar.name"),
                dbflux_i18n::t!("palette.category.view"),
            )
            .with_shortcut(SC.toggle_sidebar),
            PaletteCommand::new(
                "toggle_editor",
                dbflux_i18n::t!("palette.command.toggle_editor.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "toggle_results",
                dbflux_i18n::t!("palette.command.toggle_results.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "toggle_tasks",
                dbflux_i18n::t!("palette.command.toggle_tasks.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "open_settings",
                dbflux_i18n::t!("palette.command.open_settings.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "open_login_modal",
                dbflux_i18n::t!("palette.command.open_login_modal.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "open_sso_wizard",
                dbflux_i18n::t!("palette.command.open_sso_wizard.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            #[cfg(feature = "mcp")]
            PaletteCommand::new(
                "open_mcp_approvals",
                dbflux_i18n::t!("palette.command.open_mcp_approvals.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            #[cfg(feature = "mcp")]
            PaletteCommand::new(
                "refresh_mcp_governance",
                dbflux_i18n::t!("palette.command.refresh_mcp_governance.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "open_audit_viewer",
                dbflux_i18n::t!("palette.command.open_audit_viewer.name"),
                dbflux_i18n::t!("palette.category.view"),
            )
            .with_shortcut(SC.open_audit_viewer),
            // Charts / Dashboards
            PaletteCommand::new(
                "open_saved_chart",
                dbflux_i18n::t!("palette.command.open_saved_chart.name"),
                dbflux_i18n::t!("palette.category.charts"),
            ),
            PaletteCommand::new(
                "new_dashboard",
                dbflux_i18n::t!("palette.command.new_dashboard.name"),
                dbflux_i18n::t!("palette.category.dashboards"),
            ),
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
        // A quit request can open the active-query prompt over any other
        // overlay, and the prompt is drawn above them, so it owns the keyboard
        // first.
        if self.modal_active_query.read(cx).is_visible() {
            return ContextId::ConfirmModal;
        }

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

        // Text-input-bearing modals must own the keymap so the underlying
        // sidebar/document context does not consume typed characters as
        // command shortcuts. Returning `TextInput` (which has no parent in
        // the keymap fallback chain) ensures only input-level bindings fire.
        if self.modal_import_dashboard.read(cx).is_visible()
            || self.modal_create_dashboard.read(cx).is_visible()
            || self.modal_rename_item.read(cx).is_visible()
            || self.modal_add_panel.read(cx).is_visible()
            || self.modal_drop_table.read(cx).is_visible()
            || self.modal_tunnel_auth.read(cx).is_visible()
        {
            return ContextId::TextInput;
        }

        // Confirm-only modals (no text input) still need to swallow keys so
        // global shortcuts do not run while the user is reading a confirmation
        // dialog.
        if self.modal_delete_connection.read(cx).is_visible()
            || self.modal_unsaved_changes.read(cx).is_visible()
            || self.modal_delete_dashboard.read(cx).is_visible()
            || self.modal_delete_saved_chart.read(cx).is_visible()
        {
            return ContextId::ConfirmModal;
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
        self.sidebar_dock.update(cx, |dock, cx| {
            dock.set_sidebar_focused(target == FocusTarget::Sidebar, cx);
        });

        log::debug!("Focus changed to: {:?}", target);
        self.focus_target = target;

        self.sidebar.update(cx, |sidebar, cx| {
            sidebar.set_connections_focused(target == FocusTarget::Sidebar, cx);
        });

        if target == FocusTarget::Sidebar {
            self.focus_handle.focus(window, cx);
        }

        if target == FocusTarget::Document {
            self.tab_manager
                .update(cx, |mgr, cx| mgr.focus_active(window, cx));
        }

        cx.notify();
    }

    /// Flushes every open document that still has pending edits, as part of a
    /// graceful shutdown, and reports whether any flush is still outstanding.
    ///
    /// This never closes a tab: closing rewrites the session manifest, and a
    /// shutdown that emptied it would destroy the restored session. A clean,
    /// idle document is skipped without writing anything. Returns `true` while at
    /// least one document still has a physical write queued or running, so the
    /// caller can poll with [`await_document_flush`] until the writes land or a
    /// deadline elapses.
    pub fn flush_pending_document_edits(&self, cx: &mut Context<Self>) -> bool {
        self.tab_manager.update(cx, |manager, cx| {
            // Every document must be flushed, so this loop cannot short-circuit
            // like `any` would: the first outstanding document must not stop the
            // remaining ones from persisting their edits.
            let mut outstanding = false;
            for tab in manager.documents() {
                outstanding |= tab.as_pane().flush_for_shutdown(cx);
            }
            outstanding
        })
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

        // "Analyze database dump…" only appears when at least one registered
        // driver's `dump_analyzer()` supports offline dump analysis — the
        // workspace never branches on driver id to decide this.
        if app_state
            .drivers()
            .values()
            .any(|driver| driver.dump_analyzer().is_some())
        {
            items.push(PaletteItem::Action {
                id: "analyze_dump_file",
                name: dbflux_i18n::t!("palette.command.analyze_dump_file.name").into(),
                category: dbflux_i18n::t!("palette.category.tools").into(),
                shortcut: None,
            });
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

    fn next_focus_target(&self, _cx: &Context<Self>) -> FocusTarget {
        self.focus_target.next()
    }

    fn prev_focus_target(&self, _cx: &Context<Self>) -> FocusTarget {
        self.focus_target.prev()
    }
}

#[cfg(test)]
mod tab_close_request_tests {
    // Explicit imports, not `use super::*`: the parent glob together with
    // `#[gpui::test]` sends the macro expansion into unbounded recursion.
    use crate::keymap::{Command, CommandDispatcher};
    use crate::ui::document::pane::CloseDisposition;
    use crate::ui::document::{CodeDocument, InspectorPanel, Tab, TabBarEvent, TabManagerEvent};
    use crate::ui::overlays::command_palette::PaletteSelection;
    use crate::ui::overlays::modals::{
        CloseAction, DirtySummaryEntry, UnsavedChangesOutcome, UnsavedChangesRequest,
    };
    use crate::ui::views::workspace::{
        DocumentFlushOutcome, FocusTarget, Workspace, WorkspaceInspectorEvent, await_document_flush,
    };
    use dbflux_core::QueryLanguage;
    use dbflux_core::document_id::DocumentId;
    use dbflux_ui_base::AppStateEntity;
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;

    use dbflux_ui_base::{SaveTargetOutcome, SaveTargetProvider};

    fn new_workspace(
        cx: &mut TestAppContext,
    ) -> (
        Entity<Workspace>,
        Entity<AppStateEntity>,
        &mut VisualTestContext,
    ) {
        let app_state: Entity<AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
            })
        });
        new_workspace_with(cx, app_state)
    }

    /// Like [`new_workspace`], but with a caller-built app state — used by tests
    /// that need a Save As override or other per-entity test seam installed
    /// before any document is opened.
    fn new_workspace_with(
        cx: &mut TestAppContext,
        app_state: Entity<AppStateEntity>,
    ) -> (
        Entity<Workspace>,
        Entity<AppStateEntity>,
        &mut VisualTestContext,
    ) {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);

        let holder: Rc<RefCell<Option<Entity<Workspace>>>> = Rc::new(RefCell::new(None));
        let workspace_ref = holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let workspace = cx.new(|cx| Workspace::new(app_state.clone(), window, cx));
            workspace_ref.replace(Some(workspace.clone()));
            gpui_component::Root::new(workspace, window, cx)
        });

        let workspace = holder
            .borrow()
            .clone()
            .expect("workspace should be created");
        (workspace, app_state, window)
    }

    /// Opens an empty query tab and returns its document id.
    fn open_code_tab(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        app_state: &Entity<AppStateEntity>,
    ) -> DocumentId {
        let document = window.update(|window, cx| {
            cx.new(|cx| {
                CodeDocument::new_with_language(
                    app_state.clone(),
                    None,
                    QueryLanguage::Sql,
                    window,
                    cx,
                )
            })
        });
        let document_id = window.update(|_, cx| document.read(cx).id());

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                let pane = CodeDocument::into_pane(document.clone(), cx);
                workspace.tab_manager.update(cx, |manager, cx| {
                    manager.open(Tab::Pane(Box::new(pane)), cx);
                });
            });
        });

        document_id
    }

    #[gpui::test]
    fn sidebar_dock_hover_waits_cancels_and_preserves_focus(cx: &mut TestAppContext) {
        let (workspace, _, window) = new_workspace(cx);
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.set_focus(FocusTarget::Document, window, cx);
                workspace
                    .sidebar_dock
                    .update(cx, |dock, cx| dock.toggle(cx));
                let start = std::time::Instant::now();
                workspace.sidebar_dock.update(cx, |dock, cx| {
                    dock.pointer_enter(start, cx);
                    dock.process_hover_deadline(start + std::time::Duration::from_millis(249), cx);
                    assert!(dock.is_collapsed());
                    dock.pointer_leave(cx);
                    dock.process_hover_deadline(start + std::time::Duration::from_secs(1), cx);
                    assert!(dock.is_collapsed());
                    dock.pointer_enter(start, cx);
                    dock.process_hover_deadline(start + std::time::Duration::from_millis(250), cx);
                    assert!(!dock.is_collapsed());
                });
                assert_eq!(workspace.focus_target, FocusTarget::Document);
                workspace.set_focus(FocusTarget::Sidebar, window, cx);
                workspace
                    .sidebar_dock
                    .update(cx, |dock, cx| dock.pointer_leave(cx));
                assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
                workspace.set_focus(FocusTarget::Document, window, cx);
                assert!(workspace.sidebar_dock.read(cx).is_collapsed());
            });
        });
    }

    #[gpui::test]
    fn sidebar_dock_menu_closure_dismisses_after_pointer_and_focus_leave(cx: &mut TestAppContext) {
        let (workspace, _, window) = new_workspace(cx);
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.set_focus(FocusTarget::Document, window, cx);
                workspace.sidebar_dock.update(cx, |dock, cx| {
                    dock.toggle(cx);
                    let start = std::time::Instant::now();
                    dock.pointer_enter(start, cx);
                    dock.process_hover_deadline(start + std::time::Duration::from_millis(250), cx);
                });
                let id = dbflux_core::SchemaNodeId::Profile {
                    profile_id: uuid::Uuid::new_v4(),
                }
                .to_string();
                workspace.sidebar.update(cx, |sidebar, cx| {
                    sidebar.open_menu_for_item(&id, gpui::point(gpui::px(5.0), gpui::px(5.0)), cx);
                    assert!(sidebar.has_transient_interaction());
                });
                workspace
                    .sidebar_dock
                    .update(cx, |dock, cx| dock.pointer_leave(cx));
                assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
                workspace
                    .sidebar
                    .update(cx, |sidebar, cx| sidebar.close_context_menu(cx));
            });
        });
        window.run_until_parked();
        window.update(|_, cx| {
            assert!(workspace.read(cx).sidebar_dock.read(cx).is_collapsed());
        });
    }

    #[gpui::test]
    fn sidebar_dock_toggle_on_transient_reveal_collapses_instead_of_expanding(
        cx: &mut TestAppContext,
    ) {
        let (workspace, _, window) = new_workspace(cx);
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.set_focus(FocusTarget::Document, window, cx);
                workspace.sidebar_dock.update(cx, |dock, cx| {
                    dock.toggle(cx);
                    dock.reveal_transiently(cx);
                    assert!(!dock.is_collapsed());
                    dock.toggle(cx);
                    assert!(
                        dock.is_collapsed(),
                        "visible left-chevron must close the transient reveal"
                    );
                });
                workspace.set_focus(FocusTarget::Document, window, cx);
                assert!(workspace.sidebar_dock.read(cx).is_collapsed());
            });
        });
    }

    #[gpui::test]
    fn sidebar_dock_dispatched_focus_preserves_collapsed_preference(cx: &mut TestAppContext) {
        let (workspace, _, window) = new_workspace(cx);
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.set_focus(FocusTarget::Document, window, cx);
                workspace
                    .sidebar_dock
                    .update(cx, |dock, cx| dock.toggle(cx));
                assert!(workspace.sidebar_dock.read(cx).is_collapsed());
                assert!(workspace.dispatch(Command::FocusSidebar, window, cx));
                assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
                assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
                workspace.set_focus(FocusTarget::Document, window, cx);
                assert!(workspace.sidebar_dock.read(cx).is_collapsed());
            });
        });
    }

    #[gpui::test]
    fn sidebar_dock_palette_focus_preserves_collapsed_preference(cx: &mut TestAppContext) {
        let (workspace, _, window) = new_workspace(cx);
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.set_focus(FocusTarget::Document, window, cx);
                workspace
                    .sidebar_dock
                    .update(cx, |dock, cx| dock.toggle(cx));
                assert!(workspace.sidebar_dock.read(cx).is_collapsed());
                workspace.command_palette.update(cx, |_, cx| {
                    cx.emit(PaletteSelection::FocusConnection {
                        profile_id: uuid::Uuid::new_v4(),
                    });
                });
            });
        });
        window.run_until_parked();
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
                assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
                workspace.set_focus(FocusTarget::Document, window, cx);
                assert!(workspace.sidebar_dock.read(cx).is_collapsed());
            });
        });
    }

    #[gpui::test]
    fn sidebar_dock_focus_left_enters_explicitly_collapsed_sidebar(cx: &mut TestAppContext) {
        let (workspace, _, window) = new_workspace(cx);
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.set_focus(FocusTarget::Document, window, cx);
                workspace
                    .sidebar_dock
                    .update(cx, |dock, cx| dock.toggle(cx));
                assert!(workspace.dispatch(Command::FocusLeft, window, cx));
                assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
                assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
                workspace.set_focus(FocusTarget::Document, window, cx);
                assert!(workspace.sidebar_dock.read(cx).is_collapsed());
            });
        });
    }

    #[gpui::test]
    fn sidebar_dock_focus_cycle_enters_explicitly_collapsed_sidebar(cx: &mut TestAppContext) {
        let (workspace, _, window) = new_workspace(cx);
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.set_focus(FocusTarget::Document, window, cx);
                workspace
                    .sidebar_dock
                    .update(cx, |dock, cx| dock.toggle(cx));
                assert!(workspace.dispatch(Command::CycleFocusForward, window, cx));
                assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
                assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
                workspace.set_focus(FocusTarget::BackgroundTasks, window, cx);
                assert!(workspace.sidebar_dock.read(cx).is_collapsed());
                assert!(workspace.dispatch(Command::CycleFocusBackward, window, cx));
                assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
                assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
            });
        });
    }

    #[gpui::test]
    fn sidebar_dock_transient_resize_survives_dismiss_and_reveal(cx: &mut TestAppContext) {
        let (workspace, _, window) = new_workspace(cx);
        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.sidebar_dock.update(cx, |dock, cx| {
                    dock.toggle(cx);
                    dock.reveal_transiently(cx);
                    dock.begin_resize(gpui::px(270.0), cx);
                    dock.handle_resize_move(gpui::px(340.0), cx);
                    assert_eq!(dock.current_width(), gpui::px(350.0));
                    dock.finish_resize(cx);
                    dock.dismiss_transient(cx);
                    assert!(dock.is_collapsed());
                    dock.reveal_transiently(cx);
                    assert_eq!(dock.current_width(), gpui::px(350.0));
                });
            });
        });
    }

    #[gpui::test]
    fn sidebar_dock_window_exit_clears_transient_reveal(cx: &mut TestAppContext) {
        let (workspace, _, window) = new_workspace(cx);
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.set_focus(FocusTarget::Document, window, cx);
                workspace.sidebar_dock.update(cx, |dock, cx| {
                    dock.toggle(cx);
                    let start = std::time::Instant::now();
                    dock.pointer_enter(start, cx);
                    dock.process_hover_deadline(start + std::time::Duration::from_millis(250), cx);
                    assert!(!dock.is_collapsed());
                });
            });
        });
        window.run_until_parked();
        window.update(|window, cx| {
            window.dispatch_event(
                gpui::PlatformInput::MouseExited(gpui::MouseExitEvent::default()),
                cx,
            );
            assert!(workspace.read(cx).sidebar_dock.read(cx).is_collapsed());
        });
    }

    #[gpui::test]
    fn sidebar_dock_keyboard_reveal_preserves_explicit_collapse(cx: &mut TestAppContext) {
        let (workspace, _, window) = new_workspace(cx);
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace
                    .sidebar_dock
                    .update(cx, |dock, cx| dock.toggle(cx));
                assert!(workspace.sidebar_dock.read(cx).is_collapsed());
                workspace.set_focus(FocusTarget::Sidebar, window, cx);
                assert_eq!(workspace.focus_target, FocusTarget::Sidebar);
                assert!(!workspace.sidebar_dock.read(cx).is_collapsed());
                workspace.set_focus(FocusTarget::Document, window, cx);
                assert!(workspace.sidebar_dock.read(cx).is_collapsed());
            });
        });
    }

    /// Regression: the document's close request only ends the tab because the
    /// workspace subscribes to the tab manager. Removing that link would leave
    /// every save-and-close tab open with no test failing.
    #[gpui::test]
    fn a_close_request_closes_the_tab_that_asked(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let document_id = open_code_tab(window, &workspace, &app_state);

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.tab_manager.update(cx, |_manager, cx| {
                    cx.emit(TabManagerEvent::RequestClose { id: document_id });
                });
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            let workspace = workspace.read(cx);
            assert!(
                workspace
                    .tab_manager
                    .read(cx)
                    .document(document_id)
                    .is_none(),
                "the workspace must close the tab whose document asked"
            );
        });
    }

    /// Regression: "Don't save" used to close every open tab, not just the
    /// document the dialog was asking about.
    #[gpui::test]
    fn discarding_closes_only_the_listed_documents(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let discarded = open_code_tab(window, &workspace, &app_state);
        let kept = open_code_tab(window, &workspace, &app_state);

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.modal_unsaved_changes.update(cx, |_modal, cx| {
                    cx.emit(UnsavedChangesOutcome::DiscardAll(vec![discarded]));
                });
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            let manager = workspace.read(cx).tab_manager.read(cx);
            assert!(
                manager.document(discarded).is_none(),
                "the listed document must close"
            );
            assert!(
                manager.document(kept).is_some(),
                "every other tab keeps its changes"
            );
        });
    }

    /// Regression: the workspace confirmation owns the keyboard, so its
    /// routing must run before the sidebar guards in `dispatch`. Resolving the
    /// modal through `dispatch` is what proves the call site is wired, which
    /// calling the router directly would not.
    #[gpui::test]
    fn dispatch_resolves_a_visible_unsaved_changes_modal(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let document_id = open_code_tab(window, &workspace, &app_state);

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.modal_unsaved_changes.update(cx, |modal, cx| {
                    modal.open(
                        UnsavedChangesRequest {
                            entries: vec![DirtySummaryEntry {
                                id: document_id,
                                name: "query.sql".to_string(),
                                summary: "1 pending change".to_string(),
                                action: CloseAction::Save,
                            }],
                        },
                        cx,
                    );
                    assert!(modal.is_visible(), "the modal starts visible");
                });
            });
        });

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.dispatch(Command::Cancel, window, cx);
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            assert!(
                !workspace
                    .read(cx)
                    .modal_unsaved_changes
                    .read(cx)
                    .is_visible(),
                "Escape must resolve the visible confirmation"
            );
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(document_id)
                    .is_some(),
                "cancelling keeps the tab open"
            );
        });
    }

    // === Close-route unification and close flush (T2a) ===

    fn temp_close_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("dbflux-close-{name}-{}.sql", uuid::Uuid::new_v4()))
    }

    /// Opens a real file-backed code tab whose buffer holds `buffer` while the
    /// file on disk holds `on_disk`, with a real loaded baseline.
    ///
    /// When `dirty` is set the tab reports pending edits (through the same
    /// `restore_dirty` session restore uses), so closing it must persist them.
    fn open_file_backed_code_tab(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        app_state: &Entity<AppStateEntity>,
        path: &std::path::Path,
        buffer: &str,
        on_disk: &str,
        dirty: bool,
    ) -> DocumentId {
        std::fs::write(path, on_disk).expect("seed the backing file");
        let path = path.to_path_buf();
        let buffer = buffer.to_string();
        let on_disk = on_disk.to_string();

        let document = window.update(|window, cx| {
            cx.new(|cx| {
                let mut document = CodeDocument::new_with_language(
                    app_state.clone(),
                    None,
                    QueryLanguage::Sql,
                    window,
                    cx,
                )
                .with_path(path.clone());
                document.set_content(&buffer, window, cx);
                document.seed_file_baseline(path.clone(), on_disk);
                if dirty {
                    document.restore_dirty(cx);
                }
                document
            })
        });
        let document_id = window.update(|_, cx| document.read(cx).id());

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                let pane = CodeDocument::into_pane(document.clone(), cx);
                workspace.tab_manager.update(cx, |manager, cx| {
                    manager.open(Tab::Pane(Box::new(pane)), cx);
                });
            });
        });

        document_id
    }

    /// The single close funnel persists a dirty code tab's newest content to the
    /// real file before the tab is removed.
    #[gpui::test]
    fn closing_a_dirty_code_tab_persists_before_removing_it(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let path = temp_close_path("persist");
        let id = open_file_backed_code_tab(
            window, &workspace, &app_state, &path, "NEWEST;", "OLD;", true,
        );

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.close_tab(id, window, cx);
                assert!(
                    workspace.tab_manager.read(cx).document(id).is_some(),
                    "a deferred close must not remove the tab before its flush lands"
                );
            });
        });

        window.run_until_parked();

        assert_eq!(
            std::fs::read_to_string(&path).expect("the flush must land on the real file"),
            "NEWEST;",
            "closing a dirty tab must persist its newest content"
        );
        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(id)
                    .is_none(),
                "the tab closes only once its flush lands"
            );
        });

        std::fs::remove_file(&path).ok();
    }

    /// A close whose flush is refused leaves the tab open with its buffer, and
    /// the foreign bytes on disk stay untouched.
    #[gpui::test]
    fn a_close_flush_refused_over_a_foreign_change_keeps_the_tab_open(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let path = temp_close_path("conflict");
        let id =
            open_file_backed_code_tab(window, &workspace, &app_state, &path, "MINE;", "OLD;", true);

        std::fs::write(&path, "THEIRS;").expect("the external rewrite must succeed");

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.close_tab(id, window, cx);
            });
        });
        window.run_until_parked();

        assert_eq!(
            std::fs::read_to_string(&path).expect("the foreign content must survive"),
            "THEIRS;",
            "a refused flush must never overwrite the foreign change"
        );
        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(id)
                    .is_some(),
                "a refused flush leaves the tab open with its buffer"
            );
        });

        std::fs::remove_file(&path).ok();
    }

    /// A clean, idle tab closes immediately: no flush, no pointless write.
    #[gpui::test]
    fn a_clean_idle_code_tab_closes_without_a_flush(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let path = temp_close_path("clean");
        let id = open_file_backed_code_tab(
            window, &workspace, &app_state, &path, "SEEDED;", "SEEDED;", false,
        );

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                assert!(workspace.close_tab(id, window, cx));
                assert!(
                    workspace.tab_manager.read(cx).document(id).is_none(),
                    "a clean, idle tab closes immediately"
                );
            });
        });
        window.run_until_parked();

        assert_eq!(
            std::fs::read_to_string(&path).expect("the file must be intact"),
            "SEEDED;",
            "a clean close must not rewrite the file"
        );

        std::fs::remove_file(&path).ok();
    }

    /// A dirty code tab closes by persisting, not by asking: the unsaved-changes
    /// dialog is only for documents that need an explicit user save.
    #[gpui::test]
    fn closing_a_dirty_code_tab_persists_instead_of_asking(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let path = temp_close_path("no-dialog");
        let id = open_file_backed_code_tab(
            window, &workspace, &app_state, &path, "NEWEST;", "OLD;", true,
        );

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                assert!(
                    workspace.close_active_tab(window, cx),
                    "a code document's close is accepted and deferred"
                );
                assert!(
                    !workspace.modal_unsaved_changes.read(cx).is_visible(),
                    "a code document persists its edits instead of asking"
                );
                assert!(
                    workspace.tab_manager.read(cx).document(id).is_some(),
                    "the tab stays open until the flush lands"
                );
            });
        });
        window.run_until_parked();

        assert_eq!(
            std::fs::read_to_string(&path).expect("the flush must land"),
            "NEWEST;"
        );
        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(id)
                    .is_none()
            );
        });

        std::fs::remove_file(&path).ok();
    }

    /// Batch close applies the same per-document policy: clean tabs close, a
    /// tab whose flush is refused stays open.
    #[gpui::test]
    fn close_others_closes_the_clean_tabs_and_keeps_a_blocked_one(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let kept_path = temp_close_path("kept");
        let clean_path = temp_close_path("other-clean");
        let blocked_path = temp_close_path("other-blocked");

        let kept = open_file_backed_code_tab(
            window, &workspace, &app_state, &kept_path, "KEEP;", "KEEP;", false,
        );
        let clean = open_file_backed_code_tab(
            window,
            &workspace,
            &app_state,
            &clean_path,
            "CLEAN;",
            "CLEAN;",
            false,
        );
        let blocked = open_file_backed_code_tab(
            window,
            &workspace,
            &app_state,
            &blocked_path,
            "MINE;",
            "OLD;",
            true,
        );

        // A foreign change makes the blocked tab's flush refuse.
        std::fs::write(&blocked_path, "THEIRS;").expect("the external rewrite must succeed");

        // Route through the tab bar exactly as the context menu does.
        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.tab_bar.update(cx, |_bar, cx| {
                    cx.emit(TabBarEvent::CloseOtherTabs(kept));
                });
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            let manager = workspace.read(cx).tab_manager.read(cx);
            assert!(manager.document(kept).is_some(), "the reference tab stays");
            assert!(manager.document(clean).is_none(), "a clean tab closes");
            assert!(
                manager.document(blocked).is_some(),
                "a blocked tab stays open"
            );
        });
        assert_eq!(
            std::fs::read_to_string(&blocked_path).expect("the foreign content must survive"),
            "THEIRS;"
        );

        std::fs::remove_file(&kept_path).ok();
        std::fs::remove_file(&clean_path).ok();
        std::fs::remove_file(&blocked_path).ok();
    }

    /// A Save As picker that records that it was invoked, then reports the
    /// user dismissing the dialog. Used to prove a close route never started
    /// a Save As flow: no picker call, no file created anywhere.
    fn cancelled_recording_picker() -> (SaveTargetProvider, Arc<std::sync::atomic::AtomicBool>) {
        use std::sync::atomic::{AtomicBool, Ordering};

        let invoked = Arc::new(AtomicBool::new(false));
        let flag = invoked.clone();
        let provider: SaveTargetProvider = Arc::new(move |_request| {
            flag.store(true, Ordering::SeqCst);
            gpui::Task::ready(SaveTargetOutcome::Cancelled)
        });
        (provider, invoked)
    }

    /// Opens an untitled code tab holding edits the user typed: no backing file,
    /// a buffer that differs from the content it loaded, and no baseline.
    ///
    /// The text enters through the editor's own input path rather than a
    /// fixture-only setter, so the document is dirty by the same predicate the
    /// close flow consults. The assertion is what keeps this fixture from
    /// looking dirty to a test while looking clean to the code.
    fn open_dirty_untitled_code_tab(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        app_state: &Entity<AppStateEntity>,
    ) -> DocumentId {
        let document = window.update(|window, cx| {
            cx.new(|cx| {
                CodeDocument::new_with_language(
                    app_state.clone(),
                    None,
                    QueryLanguage::Sql,
                    window,
                    cx,
                )
            })
        });
        let document_id = window.update(|_, cx| document.read(cx).id());

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                let pane = CodeDocument::into_pane(document.clone(), cx);
                workspace.tab_manager.update(cx, |manager, cx| {
                    manager.open(Tab::Pane(Box::new(pane)), cx);
                });
                workspace.set_focus(crate::keymap::FocusTarget::Document, window, cx);
            });
        });
        window.run_until_parked();

        window.simulate_input("SELECT 1;");
        window.run_until_parked();

        window.update(|_, cx| {
            assert!(
                document.read(cx).change_summary(cx).is_some(),
                "the fixture must look dirty to the close flow's own predicate"
            );
        });

        document_id
    }

    /// Only code documents decide their own close; every other document keeps
    /// the unsaved-changes dialog and reports `CloseNow` to the funnel. A code
    /// document decides its own close only while it has a file to persist to:
    /// an untitled buffer has no save target short of Save As, so the dialog
    /// guards it.
    #[gpui::test]
    fn only_code_documents_decide_their_own_close(cx: &mut TestAppContext) {
        let (_workspace, app_state, window) = new_workspace(cx);

        let untitled = window.update(|window, cx| {
            cx.new(|cx| {
                CodeDocument::new_with_language(
                    app_state.clone(),
                    None,
                    QueryLanguage::Sql,
                    window,
                    cx,
                )
            })
        });
        let untitled_pane = window.update(|_, cx| CodeDocument::into_pane(untitled, cx));
        let untitled_keeps_dialog = window.update(|_, cx| !untitled_pane.has_close_policy(cx));
        assert!(
            untitled_keeps_dialog,
            "an untitled code document has no file to persist to, so the dialog guards it"
        );

        let backing_path =
            std::env::temp_dir().join(format!("dbflux-policy-{}.sql", uuid::Uuid::new_v4()));
        let file_backed = window.update(|window, cx| {
            cx.new(|cx| {
                CodeDocument::new_with_language(
                    app_state.clone(),
                    None,
                    QueryLanguage::Sql,
                    window,
                    cx,
                )
                .with_path(backing_path.clone())
            })
        });
        let file_backed_pane = window.update(|_, cx| CodeDocument::into_pane(file_backed, cx));
        let file_backed_decides = window.update(|_, cx| file_backed_pane.has_close_policy(cx));
        assert!(
            file_backed_decides,
            "a file-backed code document persists its own edits on close"
        );

        let inspector = window.update(|_, cx| {
            cx.new(|cx| {
                InspectorPanel::new(
                    uuid::Uuid::new_v4(),
                    "metric".to_string(),
                    app_state.clone(),
                    cx,
                )
            })
        });
        let inspector_pane = window.update(|_, cx| InspectorPanel::into_pane(inspector, cx));
        let inspector_keeps_dialog = window.update(|_, cx| !inspector_pane.has_close_policy(cx));
        assert!(
            inspector_keeps_dialog,
            "documents without a close policy keep the unsaved-changes dialog"
        );
        assert_eq!(
            window.update(|window, cx| inspector_pane.resolve_close(window, cx)),
            CloseDisposition::CloseNow,
            "a document without a policy is always closable by the funnel"
        );
    }

    /// Closing a dirty untitled tab through the funnel must ask before
    /// discarding: the confirmation opens, the tab stays open, and no Save As
    /// flow starts, so no file is ever created.
    #[gpui::test]
    fn a_dirty_untitled_tab_closed_through_the_funnel_asks_first(cx: &mut TestAppContext) {
        let (picker, picker_invoked) = cancelled_recording_picker();
        let app_state = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime)
                    .expect("test storage setup")
                    .with_save_target_override(picker)
            })
        });
        let (workspace, app_state, window) = new_workspace_with(cx, app_state);
        let id = open_dirty_untitled_code_tab(window, &workspace, &app_state);

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                let accepted = workspace.close_tab(id, window, cx);
                assert!(!accepted, "the close must wait for the user's decision");
                assert!(
                    workspace.modal_unsaved_changes.read(cx).is_visible(),
                    "a dirty untitled tab must raise the unsaved-changes confirmation"
                );
                assert!(
                    workspace.tab_manager.read(cx).document(id).is_some(),
                    "the tab stays open until the user chooses"
                );
            });
        });

        assert!(
            !picker_invoked.load(std::sync::atomic::Ordering::SeqCst),
            "the funnel must ask, not start a Save As behind the dialog"
        );
    }

    /// "Don't save" must remove the tab directly: routing the discard back
    /// through the funnel would re-open the same confirmation forever, and a
    /// funnel flush would save the very changes the user chose to drop.
    #[gpui::test]
    fn discarding_a_dirty_untitled_tab_removes_it_without_re_asking(cx: &mut TestAppContext) {
        let (picker, picker_invoked) = cancelled_recording_picker();
        let app_state = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime)
                    .expect("test storage setup")
                    .with_save_target_override(picker)
            })
        });
        let (workspace, app_state, window) = new_workspace_with(cx, app_state);
        let id = open_dirty_untitled_code_tab(window, &workspace, &app_state);

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.close_tab(id, window, cx);
            });
        });

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.modal_unsaved_changes.update(cx, |modal, cx| {
                    // The buttons emit and then close, so the confirmation is
                    // already gone by the time the workspace handles the choice.
                    cx.emit(UnsavedChangesOutcome::DiscardAll(vec![id]));
                    modal.close(cx);
                });
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(id)
                    .is_none(),
                "discard removes the listed tab"
            );
            assert!(
                !workspace
                    .read(cx)
                    .modal_unsaved_changes
                    .read(cx)
                    .is_visible(),
                "discard must not re-open the confirmation"
            );
        });
        assert!(
            !picker_invoked.load(std::sync::atomic::Ordering::SeqCst),
            "discard must not save the edits the user chose to drop"
        );
    }

    /// A batch close asks once about every document that needs it: one modal
    /// carrying all their entries, none of them closed by the batch, while the
    /// documents that need no decision still close.
    #[gpui::test]
    fn a_batch_close_asks_once_about_all_documents_that_need_it(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let first = open_dirty_untitled_code_tab(window, &workspace, &app_state);
        let second = open_dirty_untitled_code_tab(window, &workspace, &app_state);
        let clean = open_code_tab(window, &workspace, &app_state);

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                let all = [first, second, clean];
                workspace.close_tabs_by(window, cx, move |ids| {
                    ids.iter().copied().filter(|id| all.contains(id)).collect()
                });
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            let manager = workspace.read(cx).tab_manager.read(cx);
            assert!(
                workspace
                    .read(cx)
                    .modal_unsaved_changes
                    .read(cx)
                    .is_visible(),
                "a batch with documents that must be asked about opens the confirmation"
            );
            assert!(manager.document(first).is_some(), "asked tabs stay open");
            assert!(manager.document(second).is_some(), "asked tabs stay open");
            assert!(
                manager.document(clean).is_none(),
                "a clean tab still closes"
            );
        });
    }

    // === Graceful-shutdown flush (T2b) ===

    fn temp_shutdown_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dbflux-shutdown-{name}-{}.sql",
            uuid::Uuid::new_v4()
        ))
    }

    /// Opens a code tab backed by `path` with its session shadow at `shadow` —
    /// the shape a graceful shutdown flushes. `dirty` marks pending edits the way
    /// the session restore does, and `seed_baseline` controls whether the document
    /// owns a trustworthy on-disk baseline.
    #[allow(clippy::too_many_arguments)]
    fn open_shutdown_code_tab(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        app_state: &Entity<AppStateEntity>,
        path: &std::path::Path,
        shadow: &std::path::Path,
        buffer: &str,
        on_disk: &str,
        dirty: bool,
        seed_baseline: bool,
    ) -> DocumentId {
        std::fs::write(path, on_disk).expect("seed the backing file");
        let path = path.to_path_buf();
        let shadow = shadow.to_path_buf();
        let buffer = buffer.to_string();
        let on_disk = on_disk.to_string();

        let document = window.update(|window, cx| {
            cx.new(|cx| {
                let mut document = CodeDocument::new_with_language(
                    app_state.clone(),
                    None,
                    QueryLanguage::Sql,
                    window,
                    cx,
                )
                .with_path(path.clone());
                document.set_content(&buffer, window, cx);
                document.set_session_paths(None, Some(shadow.clone()));
                if seed_baseline {
                    document.seed_file_baseline(path.clone(), on_disk);
                }
                if dirty {
                    document.restore_dirty(cx);
                }
                document
            })
        });
        let document_id = window.update(|_, cx| document.read(cx).id());

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                let pane = CodeDocument::into_pane(document.clone(), cx);
                workspace.tab_manager.update(cx, |manager, cx| {
                    manager.open(Tab::Pane(Box::new(pane)), cx);
                });
            });
        });

        document_id
    }

    /// Counts the tabs persisted in the workspace session manifest.
    fn manifest_tab_count(
        window: &mut VisualTestContext,
        app_state: &Entity<AppStateEntity>,
    ) -> usize {
        window.update(|_, cx| {
            let runtime = app_state.read(cx).storage_runtime();
            let repo = runtime.sessions();
            let artifacts = runtime.artifacts();
            repo.restore_session(artifacts)
                .ok()
                .flatten()
                .map(|session| session.tabs.len())
                .unwrap_or(0)
        })
    }

    /// A dirty file-backed document flushed at exit lands its newest content in
    /// the real file, without closing its tab or emptying the session manifest.
    #[gpui::test]
    fn a_shutdown_flush_lands_pending_edits_and_keeps_the_tab(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let path = temp_shutdown_path("land");
        let shadow = temp_shutdown_path("land-shadow");
        let id = open_shutdown_code_tab(
            window, &workspace, &app_state, &path, &shadow, "NEWEST;", "OLD;", true, true,
        );

        assert_eq!(
            manifest_tab_count(window, &app_state),
            1,
            "opening a file-backed tab records it in the session manifest"
        );

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                assert!(
                    workspace.flush_pending_document_edits(cx),
                    "a dirty document reports its flush as outstanding"
                );
                assert!(
                    workspace.tab_manager.read(cx).document(id).is_some(),
                    "the shutdown flush must not close the tab"
                );
            });
        });

        window.run_until_parked();

        assert_eq!(
            std::fs::read_to_string(&path).expect("the flush must land on the real file"),
            "NEWEST;",
            "a shutdown flush persists the newest content"
        );
        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(id)
                    .is_some(),
                "the tab survives a shutdown flush"
            );
        });
        assert_eq!(
            manifest_tab_count(window, &app_state),
            1,
            "the session manifest still holds the tab after a shutdown flush"
        );

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&shadow).ok();
    }

    /// The workspace reports a flush as outstanding until its write lands, then
    /// reports nothing outstanding.
    #[gpui::test]
    fn a_shutdown_flush_reports_nothing_outstanding_once_it_lands(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let path = temp_shutdown_path("outstanding");
        let shadow = temp_shutdown_path("outstanding-shadow");
        open_shutdown_code_tab(
            window, &workspace, &app_state, &path, &shadow, "NEWEST;", "OLD;", true, true,
        );

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                assert!(
                    workspace.flush_pending_document_edits(cx),
                    "the write is outstanding before it runs"
                );
                // Polling again before the executor runs must neither block nor
                // drop the queued write.
                assert!(
                    workspace.flush_pending_document_edits(cx),
                    "a queued write keeps reporting outstanding"
                );
            });
        });

        window.run_until_parked();

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                assert!(
                    !workspace.flush_pending_document_edits(cx),
                    "nothing is outstanding once the flush lands"
                );
            });
        });

        assert_eq!(
            std::fs::read_to_string(&path).expect("the flush must land"),
            "NEWEST;"
        );

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&shadow).ok();
    }

    /// A shutdown flush refused over an external change overwrites nothing, still
    /// writes the session shadow, and never closes the tab.
    #[gpui::test]
    fn a_shutdown_flush_refused_over_a_foreign_change_writes_only_the_shadow(
        cx: &mut TestAppContext,
    ) {
        let (workspace, app_state, window) = new_workspace(cx);
        let path = temp_shutdown_path("conflict");
        let shadow = temp_shutdown_path("conflict-shadow");
        let id = open_shutdown_code_tab(
            window, &workspace, &app_state, &path, &shadow, "MINE;", "OLD;", true, true,
        );

        std::fs::write(&path, "THEIRS;").expect("the external rewrite must succeed");

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                assert!(workspace.flush_pending_document_edits(cx));
            });
        });
        window.run_until_parked();

        assert_eq!(
            std::fs::read_to_string(&path).expect("the foreign content must survive"),
            "THEIRS;",
            "a refused shutdown flush must never overwrite the foreign change"
        );
        assert_eq!(
            std::fs::read_to_string(&shadow).expect("the shadow is the safety net"),
            "MINE;",
            "the session shadow still carries the pending edits"
        );

        let entity = workspace.clone();
        window.update(|_, cx| {
            assert!(
                entity.read(cx).tab_manager.read(cx).document(id).is_some(),
                "a refused flush never closes the tab"
            );
            assert!(
                !entity.update(cx, |workspace, cx| workspace
                    .flush_pending_document_edits(cx)),
                "a refusal is reported and not retried forever"
            );
        });

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&shadow).ok();
    }

    /// A shutdown flush refused because the file was deleted keeps it deleted and
    /// writes the shadow instead.
    #[gpui::test]
    fn a_shutdown_flush_refused_over_a_deleted_file_keeps_it_deleted(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let path = temp_shutdown_path("deleted");
        let shadow = temp_shutdown_path("deleted-shadow");
        open_shutdown_code_tab(
            window, &workspace, &app_state, &path, &shadow, "MINE;", "OLD;", true, true,
        );

        std::fs::remove_file(&path).expect("the backing file must be removable");

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                assert!(workspace.flush_pending_document_edits(cx));
            });
        });
        window.run_until_parked();

        assert!(
            !path.exists(),
            "a refused shutdown flush must not recreate the deleted file"
        );
        assert_eq!(
            std::fs::read_to_string(&shadow).expect("the shadow is the safety net"),
            "MINE;"
        );

        std::fs::remove_file(&shadow).ok();
    }

    /// A shutdown flush with no trustworthy baseline refuses rather than creating
    /// or overwriting the file, and still writes the shadow.
    #[gpui::test]
    fn a_shutdown_flush_without_a_baseline_refuses_and_writes_the_shadow(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let path = temp_shutdown_path("no-baseline");
        let shadow = temp_shutdown_path("no-baseline-shadow");
        // No baseline is seeded, so the shutdown flush has nothing to compare the
        // on-disk bytes against and must refuse.
        open_shutdown_code_tab(
            window, &workspace, &app_state, &path, &shadow, "MINE;", "OLD;", true, false,
        );

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                assert!(workspace.flush_pending_document_edits(cx));
            });
        });
        window.run_until_parked();

        assert_eq!(
            std::fs::read_to_string(&path).expect("the file must survive untouched"),
            "OLD;",
            "a shutdown flush with no baseline must not overwrite the file"
        );
        assert_eq!(
            std::fs::read_to_string(&shadow).expect("the shadow is the safety net"),
            "MINE;"
        );

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&shadow).ok();
    }

    /// A clean, idle document is skipped by the shutdown flush: no physical write
    /// and no session shadow.
    #[gpui::test]
    fn a_clean_idle_document_is_skipped_by_the_shutdown_flush(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let path = temp_shutdown_path("clean");
        let shadow = temp_shutdown_path("clean-shadow");
        open_shutdown_code_tab(
            window, &workspace, &app_state, &path, &shadow, "SEEDED;", "SEEDED;", false, true,
        );

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                assert!(
                    !workspace.flush_pending_document_edits(cx),
                    "a clean, idle document has nothing outstanding"
                );
            });
        });
        window.run_until_parked();

        assert_eq!(
            std::fs::read_to_string(&path).expect("the file must be intact"),
            "SEEDED;",
            "a clean, idle document must not be rewritten"
        );
        assert!(
            !shadow.exists(),
            "a clean, idle document must not write a session shadow"
        );

        std::fs::remove_file(&path).ok();
    }

    /// A flush that never finishes is bounded by the deadline rather than hanging
    /// the shutdown sequence.
    #[gpui::test]
    fn the_shutdown_flush_deadline_bounds_a_write_that_never_completes(cx: &mut TestAppContext) {
        let timeout = Duration::from_millis(100);
        let poll_interval = Duration::from_millis(50);

        let result = Rc::new(Cell::new(None));
        let result_out = result.clone();

        let task = cx.spawn(move |mut app_cx| async move {
            let outcome = await_document_flush(
                &mut app_cx,
                timeout,
                poll_interval,
                |_app_cx| true, // the write never finishes
            )
            .await;
            result_out.set(Some(outcome));
        });

        cx.executor().advance_clock(Duration::from_millis(300));
        cx.run_until_parked();

        assert_eq!(
            result.get(),
            Some(DocumentFlushOutcome::TimedOut),
            "the deadline must stop a flush that never finishes"
        );

        drop(task);
    }

    // === Inspector rail follows the active tab ===

    /// Mounts content into the workspace rail exactly as a document does: an
    /// `OpenInspector` relayed by the tab manager.
    fn open_rail(window: &mut VisualTestContext, workspace: &Entity<Workspace>) {
        window.update(|_, cx| {
            let content: gpui::AnyView = cx.new(|_| gpui::EmptyView).into();
            workspace.update(cx, |workspace, cx| {
                workspace.tab_manager.update(cx, |_manager, cx| {
                    cx.emit(TabManagerEvent::OpenInspector {
                        title: "Row".into(),
                        content,
                    });
                });
            });
        });
        window.run_until_parked();

        assert!(
            rail_is_open(window, workspace),
            "the rail starts open with content"
        );
    }

    fn rail_is_open(window: &mut VisualTestContext, workspace: &Entity<Workspace>) -> bool {
        window.update(|_, cx| workspace.read(cx).workspace_inspector.read(cx).is_open())
    }

    fn activate_tab(window: &mut VisualTestContext, workspace: &Entity<Workspace>, id: DocumentId) {
        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace
                    .tab_manager
                    .update(cx, |manager, cx| manager.activate(id, cx));
            });
        });
        window.run_until_parked();
    }

    fn close_tab(window: &mut VisualTestContext, workspace: &Entity<Workspace>, id: DocumentId) {
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                assert!(
                    workspace.close_tab(id, window, cx),
                    "a clean tab closes immediately"
                );
            });
        });
        window.run_until_parked();
    }

    fn record_tab_events(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
    ) -> Rc<RefCell<Vec<TabManagerEvent>>> {
        let events: Rc<RefCell<Vec<TabManagerEvent>>> = Rc::new(RefCell::new(Vec::new()));
        let sink = events.clone();

        window.update(|_, cx| {
            let manager = workspace.read(cx).tab_manager.clone();
            cx.subscribe(&manager, move |_, event: &TabManagerEvent, _| {
                sink.borrow_mut().push(event.clone());
            })
            .detach();
        });

        events
    }

    /// Switching to a tab that owns nothing in the rail hides it, instead of
    /// leaving the previous tab's content on screen. The rail is hidden, not
    /// closed: a `Closed` would make the tab forget its saved rail state.
    #[gpui::test]
    fn switching_to_a_tab_that_owns_nothing_hides_the_rail(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let owns_nothing = open_code_tab(window, &workspace, &app_state);
        let _owner = open_code_tab(window, &workspace, &app_state);
        open_rail(window, &workspace);

        let closed_events = Rc::new(Cell::new(0));
        let closed_sink = closed_events.clone();
        window.update(|_, cx| {
            let inspector = workspace.read(cx).workspace_inspector.clone();
            cx.subscribe(&inspector, move |_, event: &WorkspaceInspectorEvent, _| {
                if matches!(event, WorkspaceInspectorEvent::Closed) {
                    closed_sink.set(closed_sink.get() + 1);
                }
            })
            .detach();
        });

        activate_tab(window, &workspace, owns_nothing);

        assert!(
            !rail_is_open(window, &workspace),
            "a tab that owns nothing must not show the previous tab's rail"
        );
        assert_eq!(
            closed_events.get(),
            0,
            "a tab switch hides the rail and must never close it"
        );
    }

    /// A tab that mounts its content again when it becomes active keeps the
    /// rail open: the hide runs first, so the tab's own `OpenInspector` wins.
    #[gpui::test]
    fn a_tab_that_remounts_its_content_keeps_the_rail_open(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let owner = open_code_tab(window, &workspace, &app_state);
        let _other = open_code_tab(window, &workspace, &app_state);

        window.update(|_, cx| {
            let manager = workspace.read(cx).tab_manager.clone();
            cx.subscribe(&manager, move |manager, event: &TabManagerEvent, cx| {
                if matches!(event, TabManagerEvent::Activated(id) if *id == owner) {
                    let content: gpui::AnyView = cx.new(|_| gpui::EmptyView).into();
                    manager.update(cx, |_manager, cx| {
                        cx.emit(TabManagerEvent::OpenInspector {
                            title: "Row".into(),
                            content,
                        });
                    });
                }
            })
            .detach();
        });

        activate_tab(window, &workspace, owner);

        assert!(
            rail_is_open(window, &workspace),
            "the active tab's own content must re-open the rail after the hide"
        );
    }

    /// Closing the active tab runs the activation pass for the tab that takes
    /// over, so a successor that owns nothing hides the rail.
    #[gpui::test]
    fn closing_the_active_tab_activates_the_tab_that_takes_over(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let successor = open_code_tab(window, &workspace, &app_state);
        let closed = open_code_tab(window, &workspace, &app_state);
        open_rail(window, &workspace);

        let events = record_tab_events(window, &workspace);
        close_tab(window, &workspace, closed);

        let recorded = events.borrow().clone();
        assert!(
            matches!(
                recorded.as_slice(),
                [TabManagerEvent::Closed(first), TabManagerEvent::Activated(second)]
                    if *first == closed && *second == successor
            ),
            "closing the active tab must activate its successor, got {recorded:?}"
        );
        assert!(
            !rail_is_open(window, &workspace),
            "a successor that owns nothing must not show the closed tab's rail"
        );
    }

    /// With no tab left, nothing owns the rail any more.
    #[gpui::test]
    fn closing_the_last_tab_hides_the_rail(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let only = open_code_tab(window, &workspace, &app_state);
        open_rail(window, &workspace);

        close_tab(window, &workspace, only);

        let active_id = window.update(|_, cx| workspace.read(cx).tab_manager.read(cx).active_id());
        assert_eq!(active_id, None, "no tab is left");
        assert!(
            !rail_is_open(window, &workspace),
            "closing the last tab must hide the rail"
        );
    }

    #[cfg(feature = "mcp")]
    fn open_approvals_overlay(window: &mut VisualTestContext, workspace: &Entity<Workspace>) {
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.dispatch(Command::OpenMcpApprovals, window, cx);
            });
        });
        window.run_until_parked();

        assert!(
            approvals_overlay_is_open(window, workspace),
            "opening the approvals must show the overlay"
        );
    }

    #[cfg(feature = "mcp")]
    fn approvals_overlay_is_open(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
    ) -> bool {
        window.update(|_, cx| workspace.read(cx).active_governance_panel.is_some())
    }

    /// Regression: Cancel had no handler for the approvals overlay, so the
    /// only way out was opening the audit viewer.
    #[cfg(feature = "mcp")]
    #[gpui::test]
    fn cancel_closes_the_approvals_overlay(cx: &mut TestAppContext) {
        let (workspace, _app_state, window) = new_workspace(cx);
        open_approvals_overlay(window, &workspace);

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.dispatch(Command::Cancel, window, cx);
            });
        });
        window.run_until_parked();

        assert!(
            !approvals_overlay_is_open(window, &workspace),
            "Cancel must close the approvals overlay"
        );
    }

    /// Escape travels the real key path: the workspace keymap resolves it to
    /// Cancel only if the overlay left keyboard focus inside the workspace.
    #[cfg(feature = "mcp")]
    #[gpui::test]
    fn escape_closes_the_approvals_overlay(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        open_code_tab(window, &workspace, &app_state);
        open_approvals_overlay(window, &workspace);

        window.simulate_keystrokes("escape");

        assert!(
            !approvals_overlay_is_open(window, &workspace),
            "Escape must close the approvals overlay"
        );
    }

    /// A click on the dimmed backdrop closes the overlay, while a click inside
    /// the panel must leave it open.
    #[cfg(feature = "mcp")]
    #[gpui::test]
    fn backdrop_click_closes_the_approvals_overlay(cx: &mut TestAppContext) {
        use gpui::{Modifiers, point, px};

        let (workspace, _app_state, window) = new_workspace(cx);
        open_approvals_overlay(window, &workspace);

        let viewport = window.update(|window, _| window.viewport_size());
        assert!(
            viewport.width > px(1080.0) && viewport.height > px(680.0),
            "the test window must leave backdrop visible around the panel, got {viewport:?}"
        );

        let panel_center = point(viewport.width / 2.0, viewport.height / 2.0);
        window.simulate_click(panel_center, Modifiers::none());
        window.run_until_parked();

        assert!(
            approvals_overlay_is_open(window, &workspace),
            "a click inside the panel must not close the overlay"
        );

        window.simulate_click(point(px(4.0), px(4.0)), Modifiers::none());
        window.run_until_parked();

        assert!(
            !approvals_overlay_is_open(window, &workspace),
            "a click on the backdrop must close the overlay"
        );
    }
}
