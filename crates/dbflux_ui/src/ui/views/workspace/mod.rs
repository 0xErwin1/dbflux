mod actions;
mod dispatch;
pub mod inspector;
#[cfg(test)]
mod keyboard_coverage_tests;
mod notifications;
mod pane_actions;
pub mod pipeline;
mod render;
mod shell;
use actions::TabPlacement;

pub use inspector::{WorkspaceInspector, WorkspaceInspectorEvent};

use crate::app::{AppStateChanged, AppStateEntity};
use dbflux_components;
use dbflux_core::observability::actions::CONFIG_CHANGE;
use dbflux_ui_base::app_state_entity::{
    drain_scripts_directory_diagnostics, rescan_scripts_in_background,
};
use dbflux_ui_base::modals::{
    AddPanelOutcome, AddPanelRequest, CreateDashboardOutcome, CreateDashboardRequest,
    DeleteConfirmOutcome, DeleteConfirmRequest, DeleteTarget, ModalAddPanelPicker,
    ModalCreateDashboard, ModalDeleteConfirm, ModalRenameItem, RenameItemOutcome,
    RenameItemRequest, RenameTarget, RequestMetricsForNamespace,
};
use dbflux_ui_base::{
    AppStateGlobal, OpenAuditRequested, UserErrorReported, drain_hook_load_diagnostics,
    report_error,
};

#[cfg(feature = "mcp")]
use crate::app::McpRuntimeEventRaised;

use crate::keymap::{self, Command, CommandDispatcher, ContextId, FocusTarget};
use crate::ui::dock::{SidebarDock, SidebarDockEvent};
use crate::ui::document::{CodeDocument, DataDocument, Tab, TabBar, TabBarEvent, TabManager};
use dbflux_ui_base::keymap::{RunCommand, run_command};

use crate::ui::icons::AppIcon;
use crate::ui::overlays::command_palette::{
    CommandPalette, CommandPaletteClosed, PaletteCommand, PaletteItem, PaletteSelection,
    ResourceItem,
};
use crate::ui::overlays::login_modal::{LoginModal, LoginModalEvent};
use crate::ui::overlays::shutdown_overlay::ShutdownOverlay;
use crate::ui::overlays::sql_preview_modal::SqlPreviewModal;
use crate::ui::overlays::sso_wizard::{SsoWizard, SsoWizardEvent};
#[cfg(feature = "mcp")]
use crate::ui::views::status_bar::OpenApprovalsRequested;
use crate::ui::views::status_bar::{StatusBar, ToggleTasksPanel};
use crate::ui::views::tasks_panel::{CollapseTasksPanel, TasksPanel};
use dbflux_components::icons::DriverIconTone;
use dbflux_components::tokens::{Heights, Spacing};
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
                new_tab: false,
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
                new_tab: false,
            }),
            ResourceItem::KeyValueDb {
                profile_id,
                database,
                ..
            } => Some(PaletteSelection::OpenKeyValue {
                profile_id: *profile_id,
                database: database.clone(),
                new_tab: false,
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
    whats_new_dialog: Entity<crate::ui::overlays::updates::WhatsNewDialog>,
    welcome_dialog: Entity<crate::ui::overlays::updates::WelcomeDialog>,

    tab_manager: Entity<TabManager>,
    tab_bar: Entity<TabBar>,

    workspace_inspector: Entity<inspector::WorkspaceInspector>,
    _workspace_inspector_subscription: Subscription,

    /// S8 modals — rendered as full-screen overlays via `Modal`.
    modal_delete_connection: Entity<crate::ui::overlays::modals::ModalDeleteConnection>,
    /// "Active query running" prompt shown before a disconnect or quit that
    /// would abandon a running query.
    modal_active_query: Entity<crate::ui::overlays::modals::ModalActiveQuery>,
    /// What the open active-query prompt is guarding, consumed when the user
    /// chooses an outcome.
    pending_active_query: Option<ActiveQueryScope>,
    modal_unsaved_changes: Entity<crate::ui::overlays::modals::ModalUnsavedChanges>,
    /// The quit the unsaved-changes prompt holds, from the moment it asks
    /// until the saves it started have reported. `None` while the prompt is
    /// closed or guards a tab close.
    pending_quit: Option<PendingQuit>,
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
    modal_delete_dashboard: Entity<ModalDeleteConfirm>,
    modal_delete_saved_chart: Entity<ModalDeleteConfirm>,
    modal_add_panel: Entity<ModalAddPanelPicker>,

    /// In-app single-connection export modal (overlay, not an OS window).
    export_modal: Entity<dbflux_ui_windows::connection_manager::ExportBundleModal>,
    /// Import wizard (folder bundle -> tables), targeting the connection it
    /// was opened from.
    import_wizard: Entity<dbflux_ui_document::import_wizard::ImportWizard>,
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
    /// Set when the last tab closed under an open overlay: once every overlay
    /// is gone, the sidebar takes keyboard focus, since an overlay may hand it
    /// back to the closed document or to nothing at all.
    focus_sidebar_after_overlays: bool,

    /// Active pipeline progress watcher for pipeline-enabled connects.
    pipeline_progress: Option<Entity<pipeline::PipelineProgress>>,
    _pipeline_subscription: Option<Subscription>,

    focus_target: FocusTarget,
    focus_handle: FocusHandle,
    /// Tab stop of the title bar's command search, which opens the palette.
    command_search_focus: FocusHandle,
    /// The notifications popover under the title-bar bell.
    notifications: notifications::NotificationsPopoverState,
    /// The active document's actions menu, open after `OpenPaneActions`.
    pane_actions_menu: Option<pane_actions::PaneActionsMenu>,
    /// Correlation id of the most recent user-facing error, the target of
    /// Open Last Error in Audit.
    last_user_error: Option<uuid::Uuid>,

    /// Background task handle for periodic audit purge.
    /// Kept to ensure the task stays alive for the workspace lifetime.
    _background_purge_task: Option<Task<()>>,

    /// Pending login modal open request from a settings window auth-profile
    /// login flow. Consumed in render() to call `login_modal.open_manual`.
    ///
    /// Fields: `(provider_name, profile_name, url)`.
    pending_login_modal_open: Option<(String, String, Option<String>)>,
}

/// The operation the active-query prompt interrupted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActiveQueryScope {
    /// Disconnecting this connection.
    Disconnect(uuid::Uuid),
    /// Quitting the application.
    Quit,
}

/// A quit the unsaved-changes prompt holds. `discard` lists the documents
/// whose changes the user chose to drop in this quit so far; they are dropped
/// when the quit goes on, and not asked about again.
#[derive(Clone, Debug, PartialEq, Eq)]
enum PendingQuit {
    /// The prompt is open.
    Asking {
        discard: Vec<crate::ui::document::DocumentId>,
    },
    /// The user chose to save `waiting`. Once each of them reported, or its
    /// tab closed, the quit checks every document again and goes on when
    /// nothing needs asking. `failed` is set by the first save that did not
    /// succeed, which drops the quit.
    Saving {
        waiting: Vec<crate::ui::document::DocumentId>,
        failed: bool,
        discard: Vec<crate::ui::document::DocumentId>,
    },
}

/// Emitted when a quit started inside the workspace may proceed: nothing
/// needed asking, or the user answered every prompt the quit opened (the
/// active-query prompt, then the unsaved-changes prompt) with a choice that
/// quits. The application shell owns shutdown and starts it on this event.
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
        dbflux_ui_base::app_state_entity::publish_vim_setting(&app_state, cx);
        dbflux_ui_base::app_state_entity::publish_font_settings(&app_state, cx);
        dbflux_ui_base::app_state_entity::publish_scrollbar_setting(&app_state, cx);
        dbflux_ui_base::toast::publish_auto_dismiss_setting(&app_state, &toast_host, cx);

        // Subscribed before the startup diagnostics below are reported, so
        // Open Last Error in Audit also reaches those.
        cx.subscribe(&app_state, |this, _, event: &UserErrorReported, _| {
            this.last_user_error = Some(event.correlation_id);
        })
        .detach();

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

        // External scripts folders are registered unscanned at startup so a
        // slow or unmounted folder never delays the window.
        rescan_scripts_in_background(&app_state, cx);

        let sidebar = cx.new(|cx| Sidebar::new(app_state.clone(), window, cx));
        let sidebar_dock = cx.new(|cx| SidebarDock::new(sidebar.clone(), cx));
        let tasks_panel = cx.new(|cx| TasksPanel::new(app_state.clone(), window, cx));

        let tab_manager = cx.new(|_cx| TabManager::new());
        let tab_bar = cx.new(|cx| TabBar::new(tab_manager.clone(), cx));
        let status_bar =
            cx.new(|cx| StatusBar::new(app_state.clone(), tab_manager.clone(), window, cx));

        let command_palette = cx.new(|cx| CommandPalette::new(window, cx));

        let sql_preview_modal = cx.new(|cx| SqlPreviewModal::new(app_state.clone(), window, cx));
        let login_modal = cx.new(|cx| LoginModal::new(window, cx));
        let sso_wizard = cx.new(|cx| SsoWizard::new(app_state.clone(), window, cx));
        let shutdown_overlay = cx.new(|cx| ShutdownOverlay::new(app_state.clone(), window, cx));
        let whats_new_dialog =
            cx.new(|cx| crate::ui::overlays::updates::WhatsNewDialog::new(app_state.clone(), cx));
        let welcome_dialog =
            cx.new(|cx| crate::ui::overlays::updates::WelcomeDialog::new(app_state.clone(), cx));

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
        let modal_delete_dashboard = cx.new(ModalDeleteConfirm::new);
        let modal_delete_saved_chart = cx.new(ModalDeleteConfirm::new);
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
                if let DropTableOutcome::Confirmed { if_exists, cascade } = *outcome {
                    this.sidebar.update(cx, |sidebar, cx| {
                        sidebar.confirm_modal_drop_table(if_exists, cascade, cx);
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

        // Subscribe: ModalTunnelAuth — handle passphrase or connection password
        // provided, or cancelled.
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
                    TunnelAuthOutcome::PasswordProvided {
                        profile_id,
                        password,
                        remember,
                    } => {
                        this.sidebar.update(cx, |sidebar, cx| {
                            sidebar.connect_to_profile_with_password(
                                profile_id, password, remember, cx,
                            );
                        });
                    }
                    // Cancelling either prompt abandons the connect without an
                    // error: the profile simply stays disconnected.
                    TunnelAuthOutcome::Cancelled => {
                        this.sidebar.update(cx, |sidebar, cx| {
                            sidebar.clear_pending_tunnel_auth();
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

                // A prompt opened for a quit is answered for the quit: its
                // tabs stay open, whatever the answer.
                if matches!(this.pending_quit, Some(PendingQuit::Asking { .. })) {
                    this.resolve_quit_prompt(outcome, window, cx);
                    return;
                }

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

        cx.subscribe(&tasks_panel, |this, _, _: &CollapseTasksPanel, cx| {
            if this.tasks_state.is_expanded() {
                this.toggle_tasks_panel(cx);
            }
        })
        .detach();

        #[cfg(feature = "mcp")]
        cx.subscribe_in(
            &status_bar,
            window,
            |this, _, _: &OpenApprovalsRequested, window, cx| {
                this.open_mcp_approvals(window, cx);
            },
        )
        .detach();

        cx.subscribe_in(
            &app_state,
            window,
            |this, _, event: &OpenAuditRequested, window, cx| {
                this.open_audit_viewer_with_correlation(event.0, window, cx);
            },
        )
        .detach();

        cx.subscribe(
            &app_state,
            |this, _, _: &dbflux_ui_base::updates::UpdateDialogRequested, cx| {
                this.open_requested_update_dialog(cx);
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
                    new_tab,
                } => {
                    this.open_table_document(
                        *profile_id,
                        table.clone(),
                        database.clone(),
                        TabPlacement::from_new_tab(*new_tab),
                        window,
                        cx,
                    );
                }
                PaletteSelection::OpenCollection {
                    profile_id,
                    collection,
                    new_tab,
                } => {
                    this.open_collection_document(
                        *profile_id,
                        collection.clone(),
                        TabPlacement::from_new_tab(*new_tab),
                        window,
                        cx,
                    );
                }
                PaletteSelection::OpenKeyValue {
                    profile_id,
                    database,
                    new_tab,
                } => {
                    this.open_key_value_document(
                        *profile_id,
                        database.clone(),
                        TabPlacement::from_new_tab(*new_tab),
                        window,
                        cx,
                    );
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

        // Subscribe: dashboard delete confirm — on Confirmed, delete the dashboard.
        cx.subscribe_in(
            &modal_delete_dashboard,
            window,
            |this, _, outcome: &DeleteConfirmOutcome, window, cx| {
                if let DeleteConfirmOutcome::Confirmed(DeleteTarget::Dashboard { dashboard_id }) =
                    *outcome
                {
                    this.on_delete_dashboard_confirmed(dashboard_id, window, cx);
                }
            },
        )
        .detach();

        // Subscribe: saved chart delete confirm — on Confirmed, delete the saved chart.
        cx.subscribe_in(
            &modal_delete_saved_chart,
            window,
            |this, _, outcome: &DeleteConfirmOutcome, window, cx| {
                if let DeleteConfirmOutcome::Confirmed(DeleteTarget::SavedChart { chart_id }) =
                    *outcome
                {
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
                        TabPlacement::ReuseExisting,
                        window,
                        cx,
                    );
                }
                SidebarEvent::OpenCollection {
                    profile_id,
                    collection,
                } => {
                    this.open_collection_document(
                        *profile_id,
                        collection.clone(),
                        TabPlacement::ReuseExisting,
                        window,
                        cx,
                    );
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
                    this.open_key_value_document(
                        *profile_id,
                        database.clone(),
                        TabPlacement::ReuseExisting,
                        window,
                        cx,
                    );
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
                SidebarEvent::OpenConsole {
                    profile_id,
                    database,
                } => {
                    this.open_console_document(*profile_id, Some(database.clone()), window, cx);
                }
                SidebarEvent::OpenObjectStoreBuckets { profile_id } => {
                    this.open_object_store_buckets_document(*profile_id, window, cx);
                }
                SidebarEvent::OpenObjectStoreBucket { profile_id, bucket } => {
                    this.open_object_browser(*profile_id, bucket.clone(), window, cx);
                }
                SidebarEvent::OpenSecretStore {
                    profile_id,
                    store_id,
                    path,
                } => {
                    this.open_secret_store_document(
                        *profile_id,
                        store_id.clone(),
                        path.clone(),
                        window,
                        cx,
                    );
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
                    if dbflux_core::is_openable_script(path)
                        || crate::ui::document::file_format::file_document_format(path).is_some()
                    {
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
                    profile_id,
                    table_name,
                    schema_name,
                    dependents,
                } => {
                    use crate::ui::overlays::modals::DropTableRequest;

                    // Without a live connection there is no dialect to ask,
                    // so the preview falls back to ANSI double quotes.
                    let connection = this
                        .app_state
                        .read(cx)
                        .connections()
                        .get(profile_id)
                        .map(|connected| connected.connection.clone());
                    let default_dialect = dbflux_core::DefaultSqlDialect;
                    let dialect: &dyn dbflux_core::SqlDialect = match connection.as_ref() {
                        Some(connection) => connection.dialect(),
                        None => &default_dialect,
                    };

                    let req = DropTableRequest::new(
                        table_name.clone(),
                        schema_name.clone(),
                        dependents.clone(),
                        dialect,
                    );
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
                SidebarEvent::RequestNewDashboard => {
                    this.create_dashboard_from_palette(window, cx);
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
                SidebarEvent::RequestConnectionPassword { prompt } => {
                    use crate::ui::overlays::modals::ConnectionPasswordRequest;
                    let request = ConnectionPasswordRequest {
                        profile_id: prompt.profile_id,
                        profile_name: prompt.profile_name.clone(),
                        field_label: prompt.field_label.clone(),
                    };
                    this.modal_tunnel_auth.update(cx, |modal, cx| {
                        modal.open_connection_password(request, window, cx);
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
                    this.open_migrate_wizard(
                        *profile_id,
                        database.clone(),
                        tables.clone(),
                        window,
                        cx,
                    );
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
                    TabManagerEvent::OpenInspector {
                        title,
                        content,
                        content_has_header,
                    } => {
                        this.workspace_inspector.update(cx, |insp, cx| {
                            insp.open_with(content.clone(), title.clone(), *content_has_header, cx);
                            insp.set_content_min_width(
                                dbflux_ui_document::inspector_min_width(content),
                                cx,
                            );
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
                    TabManagerEvent::OpenReferencedRow { target } => {
                        this.open_table_at_referenced_row(target.clone(), window, cx);
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
                    TabManagerEvent::RequestOpenApprovals => {
                        #[cfg(feature = "mcp")]
                        this.open_mcp_approvals(window, cx);
                    }
                    TabManagerEvent::OpenEditorWithContent { sql, .. } => {
                        this.new_query_tab_with_content(sql.clone(), window, cx);
                    }
                    TabManagerEvent::SaveFinished { id, succeeded } => {
                        this.leave_quit_wait(*id, *succeeded, window, cx);

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
                    TabManagerEvent::Closed(id) => {
                        // A closed tab never reports its save.
                        this.leave_quit_wait(*id, true, window, cx);

                        // With a tab left, the `Activated` that follows hands
                        // the rail over. With none, nothing owns it any more.
                        if this.tab_manager.read(cx).active_id().is_none() {
                            this.workspace_inspector.update(cx, |insp, cx| {
                                insp.hide(cx);
                            });

                            // The focused document is gone and no tab is left
                            // to take it, so keyboard focus would land nowhere.
                            // Focus elsewhere is left alone: a deferred close
                            // can land while the user works in another area.
                            // An open overlay keeps the keyboard until it
                            // closes; render then hands it to the sidebar.
                            if this.focus_target == FocusTarget::Document {
                                if this.overlay_context(cx).is_some() {
                                    this.mark_focus_target(FocusTarget::Sidebar, cx);
                                    this.focus_sidebar_after_overlays = true;
                                } else {
                                    this.set_focus(FocusTarget::Sidebar, window, cx);
                                }
                            }
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

        Self::subscribe_notifications(&app_state, cx);

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
            whats_new_dialog,
            welcome_dialog,
            tab_manager,
            tab_bar,
            workspace_inspector,
            _workspace_inspector_subscription: workspace_inspector_subscription,
            modal_delete_connection,
            modal_active_query,
            pending_active_query: None,
            modal_unsaved_changes,
            pending_quit: None,
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
            export_wizard,
            tasks_state: PanelState::Collapsed,
            pending_command: None,
            pending_sql: None,
            pending_focus: None,
            pending_open_script: None,
            pending_open_routine: None,
            needs_focus_restore: false,
            focus_sidebar_after_overlays: false,
            pipeline_progress: None,
            _pipeline_subscription: None,
            focus_target: FocusTarget::default(),
            focus_handle,
            command_search_focus: cx.focus_handle(),
            notifications: notifications::NotificationsPopoverState::new(cx),
            pane_actions_menu: None,
            last_user_error: None,
            _background_purge_task: None,
            pending_login_modal_open: None,
        };

        {
            let settings = workspace.app_state.read(cx).general_settings().clone();

            if settings.restore_session_on_startup {
                workspace.restore_session(window, cx);
            }

            if settings.reopen_last_connections {
                workspace.reopen_last_connections(cx);
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
        // Keycaps come from the effective keymap for every command it binds
        // (see `palette_command_keycaps`). An explicit shortcut is only
        // needed for a command the keymap does not bind itself.
        vec![
            // Editor
            PaletteCommand::new(
                "new_query_tab",
                dbflux_i18n::t!("palette.command.new_query_tab.name"),
                dbflux_i18n::t!("palette.category.editor"),
            ),
            PaletteCommand::new(
                "run_query",
                dbflux_i18n::t!("palette.command.run_query.name"),
                dbflux_i18n::t!("palette.category.editor"),
            ),
            PaletteCommand::new(
                "run_query_in_new_tab",
                dbflux_i18n::t!("palette.command.run_query_in_new_tab.name"),
                dbflux_i18n::t!("palette.category.editor"),
            ),
            PaletteCommand::new(
                "save_query",
                dbflux_i18n::t!("palette.command.save_query.name"),
                dbflux_i18n::t!("palette.category.editor"),
            ),
            PaletteCommand::new(
                "save_file_as",
                dbflux_i18n::t!("palette.command.save_file_as.name"),
                dbflux_i18n::t!("palette.category.editor"),
            ),
            PaletteCommand::new(
                "open_script_file",
                dbflux_i18n::t!("palette.command.open_script_file.name"),
                dbflux_i18n::t!("palette.category.editor"),
            ),
            PaletteCommand::new(
                "add_external_scripts_folder",
                dbflux_i18n::t!("palette.command.add_external_scripts_folder.name"),
                dbflux_i18n::t!("palette.category.editor"),
            ),
            PaletteCommand::new(
                "toggle_comment",
                dbflux_i18n::t!("palette.command.toggle_comment.name"),
                dbflux_i18n::t!("palette.category.editor"),
            ),
            PaletteCommand::new(
                "open_history",
                dbflux_i18n::t!("palette.command.open_history.name"),
                dbflux_i18n::t!("palette.category.editor"),
            ),
            PaletteCommand::new(
                "cancel_query",
                dbflux_i18n::t!("palette.command.cancel_query.name"),
                dbflux_i18n::t!("palette.category.editor"),
            )
            .with_shortcut("esc"),
            // Tabs
            PaletteCommand::new(
                "close_tab",
                dbflux_i18n::t!("palette.command.close_tab.name"),
                dbflux_i18n::t!("palette.category.tabs"),
            ),
            PaletteCommand::new(
                "next_tab",
                dbflux_i18n::t!("palette.command.next_tab.name"),
                dbflux_i18n::t!("palette.category.tabs"),
            ),
            PaletteCommand::new(
                "prev_tab",
                dbflux_i18n::t!("palette.command.prev_tab.name"),
                dbflux_i18n::t!("palette.category.tabs"),
            ),
            PaletteCommand::new(
                "move_tab_left",
                dbflux_i18n::t!("palette.command.move_tab_left.name"),
                dbflux_i18n::t!("palette.category.tabs"),
            ),
            PaletteCommand::new(
                "move_tab_right",
                dbflux_i18n::t!("palette.command.move_tab_right.name"),
                dbflux_i18n::t!("palette.category.tabs"),
            ),
            // Results
            PaletteCommand::new(
                "export_results",
                dbflux_i18n::t!("palette.command.export_results.name"),
                dbflux_i18n::t!("palette.category.results"),
            ),
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
            PaletteCommand::new(
                "export_connections",
                dbflux_i18n::t!("palette.command.export_connections.name"),
                dbflux_i18n::t!("palette.category.connections"),
            ),
            // Focus
            PaletteCommand::new(
                "focus_sidebar",
                dbflux_i18n::t!("palette.command.focus_sidebar.name"),
                dbflux_i18n::t!("palette.category.focus"),
            ),
            PaletteCommand::new(
                "focus_editor",
                dbflux_i18n::t!("palette.command.focus_editor.name"),
                dbflux_i18n::t!("palette.category.focus"),
            ),
            PaletteCommand::new(
                "focus_results",
                dbflux_i18n::t!("palette.command.focus_results.name"),
                dbflux_i18n::t!("palette.category.focus"),
            ),
            PaletteCommand::new(
                "focus_tasks",
                dbflux_i18n::t!("palette.command.focus_tasks.name"),
                dbflux_i18n::t!("palette.category.focus"),
            ),
            // View
            PaletteCommand::new(
                "toggle_sidebar",
                dbflux_i18n::t!("palette.command.toggle_sidebar.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "toggle_full_screen",
                dbflux_i18n::t!("palette.command.toggle_full_screen.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
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
                "toggle_results_position",
                dbflux_i18n::t!("palette.command.toggle_results_position.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "toggle_tasks",
                dbflux_i18n::t!("palette.command.toggle_tasks.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "clear_finished_tasks",
                dbflux_i18n::t!("palette.command.clear_finished_tasks.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "toggle_notifications",
                dbflux_i18n::t!("palette.command.toggle_notifications.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "open_pane_actions",
                dbflux_i18n::t!("palette.command.open_pane_actions.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "show_connections_view",
                dbflux_i18n::t!("palette.command.show_connections_view.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "show_scripts_view",
                dbflux_i18n::t!("palette.command.show_scripts_view.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "show_dashboards_view",
                dbflux_i18n::t!("palette.command.show_dashboards_view.name"),
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
            ),
            PaletteCommand::new(
                "open_last_error_in_audit",
                dbflux_i18n::t!("palette.command.open_last_error_in_audit.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
            PaletteCommand::new(
                "open_toast_actions",
                dbflux_i18n::t!("palette.command.open_toast_actions.name"),
                dbflux_i18n::t!("palette.category.view"),
            ),
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

    /// The key context of the workspace root: `Workspace`, the context that
    /// owns the keyboard (see [`Workspace::active_context`]) and, while a
    /// document owns it, the entries the document contributes. While a
    /// workspace overlay owns the keyboard the global chords stay out, even
    /// when the overlay reports a text field.
    fn root_key_context(&self, cx: &Context<Self>) -> gpui::KeyContext {
        if let Some(context) = self.overlay_context(cx) {
            return dbflux_ui_base::keymap::overlay_root_key_context(
                dbflux_ui_base::keymap::WORKSPACE_KEY_CONTEXT,
                context,
                &[],
            );
        }

        let context = self.active_context(cx);

        let entries = if self.focus_target == FocusTarget::Document {
            self.tab_manager
                .read(cx)
                .active_tab()
                .map(|tab| tab.key_context_entries(cx))
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        dbflux_ui_base::keymap::root_key_context(
            dbflux_ui_base::keymap::WORKSPACE_KEY_CONTEXT,
            context,
            &entries,
        )
    }

    fn active_context(&self, cx: &Context<Self>) -> ContextId {
        if let Some(context) = self.overlay_context(cx) {
            return context;
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

    /// The context of the workspace overlay that owns the keyboard (a
    /// prompt, the palette, a picker, a dialog or the tab menu), or `None`
    /// when no overlay is open and the focused panel owns it.
    fn overlay_context(&self, cx: &Context<Self>) -> Option<ContextId> {
        // A quit request can open the active-query prompt over any other
        // overlay, and the prompt is drawn above them, so it owns the keyboard
        // first.
        if self.modal_active_query.read(cx).is_visible() {
            return Some(ContextId::ConfirmModal);
        }

        // The popover floats over every other layer and holds focus while
        // it is open.
        if self.notifications.is_open() {
            return Some(ContextId::Notifications);
        }

        if self.command_palette.read(cx).is_visible() {
            return Some(ContextId::CommandPalette);
        }

        if self.sidebar.read(cx).has_child_picker_open() {
            // When the filter input inside the picker is focused, defer to the
            // text-input keymap so typing does not trigger list navigation.
            if self.sidebar.read(cx).child_picker_filter_is_focused() {
                return Some(ContextId::TextInput);
            }
            return Some(ContextId::EventStreamsPicker);
        }

        if self.sql_preview_modal.read(cx).is_visible() {
            return Some(ContextId::SqlPreviewModal);
        }

        // Text-input-bearing modals must own the keymap so the underlying
        // sidebar/document context does not consume typed characters as
        // command shortcuts. `TextInput` has no parent in the keymap fallback
        // chain, and as an overlay it keeps no global chords either, so only
        // input-level bindings fire.
        if self.modal_import_dashboard.read(cx).is_visible()
            || self.modal_create_dashboard.read(cx).is_visible()
            || self.modal_rename_item.read(cx).is_visible()
            || self.modal_add_panel.read(cx).is_visible()
            || self.modal_drop_table.read(cx).is_visible()
            || self.modal_tunnel_auth.read(cx).is_visible()
        {
            return Some(ContextId::TextInput);
        }

        // Confirm-only modals (no text input) still need to swallow keys so
        // global shortcuts do not run while the user is reading a confirmation
        // dialog.
        if self.modal_delete_connection.read(cx).is_visible()
            || self.modal_unsaved_changes.read(cx).is_visible()
            || self.modal_delete_dashboard.read(cx).is_visible()
            || self.modal_delete_saved_chart.read(cx).is_visible()
            || self.sidebar.read(cx).delete_modal_state().is_some()
        {
            return Some(ContextId::ConfirmModal);
        }

        if self.tab_bar.read(cx).has_context_menu_open() || self.has_pane_actions_menu() {
            return Some(ContextId::ContextMenu);
        }

        None
    }

    /// Moves keyboard focus to `target`. Focusing the background tasks
    /// expands their panel first, since a collapsed panel renders nothing
    /// that could hold focus.
    pub fn set_focus(&mut self, target: FocusTarget, window: &mut Window, cx: &mut Context<Self>) {
        self.mark_focus_target(target, cx);

        if target == FocusTarget::Sidebar {
            self.focus_handle.focus(window, cx);
        }

        if target == FocusTarget::Document {
            self.tab_manager
                .update(cx, |mgr, cx| mgr.focus_active(window, cx));
        }

        cx.notify();
    }

    /// Records `target` as the focused area and updates the panes that draw
    /// it, without moving keyboard focus. A click inside a document's side
    /// island uses this, so a field it focuses keeps the keyboard.
    pub(crate) fn mark_focus_target(&mut self, target: FocusTarget, cx: &mut Context<Self>) {
        if target == FocusTarget::BackgroundTasks {
            self.tasks_state = PanelState::Expanded;
        }

        self.sidebar_dock.update(cx, |dock, cx| {
            dock.set_sidebar_focused(target == FocusTarget::Sidebar, cx);
        });

        log::debug!("Focus changed to: {:?}", target);
        self.focus_target = target;

        self.sidebar.update(cx, |sidebar, cx| {
            sidebar.set_connections_focused(target == FocusTarget::Sidebar, cx);
        });

        self.tasks_panel.update(cx, |panel, cx| {
            panel.set_focused(target == FocusTarget::BackgroundTasks, cx);
        });

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
                // A value still in an open editor is committed first, so the
                // flush persists it. Input that cannot be committed does not
                // hold the shutdown up.
                tab.as_pane().commit_pending_input(cx);
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
            let icon = app_state.drivers().get(&profile.driver_id()).map(|driver| {
                let metadata = driver.metadata();
                (
                    AppIcon::for_driver(metadata.icon, metadata.category),
                    DriverIconTone::for_driver(metadata.icon, metadata.category),
                )
            });

            items.push(PaletteItem::Connection {
                profile_id: profile.id,
                name: profile.name.clone(),
                is_connected,
                icon,
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
            Self::flatten_script_entries(dir.entries(), dir.root_path(), None, &mut items);

            for external in dir.external_roots() {
                Self::flatten_script_entries(
                    external.entries(),
                    external.path(),
                    Some(external.label()),
                    &mut items,
                );
            }
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
    /// Lists the openable scripts under `scripts_root` as palette items. An
    /// external folder passes its label as `root_label`, so its scripts read
    /// `label/relative/path` and stay apart from same-named managed scripts.
    fn flatten_script_entries(
        entries: &[dbflux_core::ScriptEntry],
        scripts_root: &std::path::Path,
        root_label: Option<&str>,
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

                    let relative_path = match root_label {
                        Some(label) => format!("{label}/{relative_path}"),
                        None => relative_path,
                    };

                    items.push(PaletteItem::Script {
                        path: path.clone(),
                        name: name.clone(),
                        relative_path,
                    });
                }
                ScriptEntry::Folder { children, .. } => {
                    Self::flatten_script_entries(children, scripts_root, root_label, items);
                }
            }
        }
    }

    /// Expands or collapses the background tasks panel. Collapsing it while
    /// it holds focus hands focus back to the document, on the next render,
    /// because the collapsed panel is not drawn at all.
    pub fn toggle_tasks_panel(&mut self, cx: &mut Context<Self>) {
        self.tasks_state.toggle();

        if !self.tasks_state.is_expanded() && self.focus_target == FocusTarget::BackgroundTasks {
            self.pending_focus = Some(FocusTarget::Document);
        }

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

    /// The next area in the Tab cycle, skipping the background tasks while
    /// their panel is collapsed.
    fn next_focus_target(&self, _cx: &Context<Self>) -> FocusTarget {
        self.skip_collapsed_tasks(self.focus_target.next(), FocusTarget::next)
    }

    /// The previous area in the Tab cycle, skipping the background tasks
    /// while their panel is collapsed.
    fn prev_focus_target(&self, _cx: &Context<Self>) -> FocusTarget {
        self.skip_collapsed_tasks(self.focus_target.prev(), FocusTarget::prev)
    }

    fn skip_collapsed_tasks(
        &self,
        candidate: FocusTarget,
        step: fn(&FocusTarget) -> FocusTarget,
    ) -> FocusTarget {
        if candidate == FocusTarget::BackgroundTasks && !self.tasks_state.is_expanded() {
            step(&candidate)
        } else {
            candidate
        }
    }
}

#[cfg(test)]
mod tests;
