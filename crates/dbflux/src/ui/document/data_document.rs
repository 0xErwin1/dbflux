use super::data_grid_panel::{DataGridEvent, DataGridPanel, DataSource};
use super::types::{DataSourceKind, DocumentId, DocumentState};
use crate::app::AppState;
use crate::keymap::{Command, ContextId};
use dbflux_core::{QueryResult, TableRef};
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
}

impl DataDocument {
    /// Create a document for browsing a table.
    pub fn new_for_table(
        profile_id: Uuid,
        table: TableRef,
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let title = table.qualified_name();
        let data_grid =
            cx.new(|cx| DataGridPanel::new_for_table(profile_id, table, app_state, window, cx));

        let subscription = cx.subscribe(&data_grid, |_this, _grid, _event: &DataGridEvent, _cx| {});

        Self {
            id: DocumentId::new(),
            title,
            source_kind: DataSourceKind::Table,
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
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let data_grid =
            cx.new(|cx| DataGridPanel::new_for_result(result, query, app_state, window, cx));

        let subscription = cx.subscribe(&data_grid, |_this, _grid, _event: &DataGridEvent, _cx| {});

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
            DataSource::QueryResult { .. } => None,
        }
    }

    /// Returns the active context for keyboard handling.
    /// DataDocument is always in Results context since it's a pure data grid.
    pub fn active_context(&self) -> ContextId {
        ContextId::Results
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
