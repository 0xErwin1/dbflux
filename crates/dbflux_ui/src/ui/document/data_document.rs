use super::data_grid_panel::{DataGridEvent, DataGridPanel, DataSource};
use super::types::{DataSourceKind, DocumentId, DocumentState};
use crate::app::AppStateEntity;
use crate::keymap::{Command, ContextId};
use crate::ui::overlays::sql_preview_modal::SqlPreviewContext;
use dbflux_core::{CollectionRef, QueryResult, RefreshPolicy, TableRef};
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;

/// Document for displaying data in a standalone tab.
/// Used for both table browsing (click on sidebar) and promoted query results.
pub struct DataDocument {
    id: DocumentId,
    title: String,
    source_kind: DataSourceKind,
    data_grid: Entity<DataGridPanel>,
    focus_handle: FocusHandle,
    _subscription: Subscription,
}

/// Events emitted by DataDocument.
#[derive(Clone, Debug)]
pub enum DataDocumentEvent {
    #[allow(dead_code)]
    MetaChanged,
    /// The document area was clicked and wants focus.
    RequestFocus,
    /// Request to show SQL preview modal.
    RequestSqlPreview {
        context: Box<SqlPreviewContext>,
        generation_type: crate::ui::overlays::sql_preview_modal::SqlGenerationType,
    },
}

impl DataDocument {
    pub fn new_for_table(
        profile_id: Uuid,
        table: TableRef,
        database: Option<String>,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let title = table.qualified_name();
        let data_grid = cx.new(|cx| {
            DataGridPanel::new_for_table(profile_id, table, database, app_state, window, cx)
        });

        let subscription =
            cx.subscribe(
                &data_grid,
                |_this, _grid, event: &DataGridEvent, cx| match event {
                    DataGridEvent::Focused => {
                        cx.emit(DataDocumentEvent::RequestFocus);
                    }
                    DataGridEvent::RequestSqlPreview {
                        context,
                        generation_type,
                    } => {
                        cx.emit(DataDocumentEvent::RequestSqlPreview {
                            context: context.clone(),
                            generation_type: *generation_type,
                        });
                    }
                    _ => {}
                },
            );

        Self {
            id: DocumentId::new(),
            title,
            source_kind: DataSourceKind::Table,
            data_grid,
            focus_handle: cx.focus_handle(),
            _subscription: subscription,
        }
    }

    pub fn new_for_collection(
        profile_id: Uuid,
        collection: CollectionRef,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let title = collection.qualified_name();
        let data_grid = cx.new(|cx| {
            DataGridPanel::new_for_collection(profile_id, collection, app_state, window, cx)
        });

        let subscription =
            cx.subscribe(
                &data_grid,
                |_this, _grid, event: &DataGridEvent, cx| match event {
                    DataGridEvent::Focused => {
                        cx.emit(DataDocumentEvent::RequestFocus);
                    }
                    DataGridEvent::RequestSqlPreview {
                        context,
                        generation_type,
                    } => {
                        cx.emit(DataDocumentEvent::RequestSqlPreview {
                            context: context.clone(),
                            generation_type: *generation_type,
                        });
                    }
                    _ => {}
                },
            );

        Self {
            id: DocumentId::new(),
            title,
            source_kind: DataSourceKind::Collection,
            data_grid,
            focus_handle: cx.focus_handle(),
            _subscription: subscription,
        }
    }

    #[allow(dead_code)]
    pub fn new_for_result(
        result: Arc<QueryResult>,
        query: String,
        title: String,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let data_grid =
            cx.new(|cx| DataGridPanel::new_for_result(result, query, app_state, window, cx));

        let subscription =
            cx.subscribe(
                &data_grid,
                |_this, _grid, event: &DataGridEvent, cx| match event {
                    DataGridEvent::Focused => {
                        cx.emit(DataDocumentEvent::RequestFocus);
                    }
                    DataGridEvent::RequestSqlPreview {
                        context,
                        generation_type,
                    } => {
                        cx.emit(DataDocumentEvent::RequestSqlPreview {
                            context: context.clone(),
                            generation_type: *generation_type,
                        });
                    }
                    _ => {}
                },
            );

        Self {
            id: DocumentId::new(),
            title,
            source_kind: DataSourceKind::QueryResult,
            data_grid,
            focus_handle: cx.focus_handle(),
            _subscription: subscription,
        }
    }

    // === Accessors ===

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        self.title.clone()
    }

    pub fn state(&self) -> DocumentState {
        DocumentState::Clean
    }

    pub fn source_kind(&self) -> DataSourceKind {
        self.source_kind
    }

    pub fn can_close(&self) -> bool {
        true
    }

    pub fn focus(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
    }

    pub fn connection_id(&self, cx: &App) -> Option<Uuid> {
        match self.data_grid.read(cx).source() {
            DataSource::Table { profile_id, .. } => Some(*profile_id),
            DataSource::Collection { profile_id, .. } => Some(*profile_id),
            DataSource::QueryResult { .. } => None,
        }
    }

    pub fn set_active_tab(&mut self, active: bool, cx: &mut Context<Self>) {
        self.data_grid
            .update(cx, |grid, _cx| grid.set_active_tab(active));
    }

    pub fn refresh_policy(&self, cx: &App) -> RefreshPolicy {
        self.data_grid.read(cx).refresh_policy()
    }

    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        self.data_grid
            .update(cx, |grid, cx| grid.set_refresh_policy(policy, cx));
    }

    /// Returns the table reference if this is a table document.
    pub fn table_ref(&self, cx: &App) -> Option<TableRef> {
        self.data_grid.read(cx).source().table_ref().cloned()
    }

    /// Returns the database name if this is a table document.
    pub fn database(&self, cx: &App) -> Option<String> {
        self.data_grid
            .read(cx)
            .source()
            .database()
            .map(|s| s.to_string())
    }

    pub fn collection_ref(&self, cx: &App) -> Option<CollectionRef> {
        self.data_grid.read(cx).source().collection_ref().cloned()
    }

    /// Returns the active context for keyboard handling.
    pub fn active_context(&self, cx: &App) -> ContextId {
        self.data_grid.read(cx).active_context(cx)
    }

    // === Command Dispatch ===

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        self.data_grid
            .update(cx, |grid, cx| grid.dispatch_command(cmd, window, cx))
    }
}

impl EventEmitter<DataDocumentEvent> for DataDocument {}

impl Render for DataDocument {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .track_focus(&self.focus_handle)
            .child(self.data_grid.clone())
    }
}
