mod pane;

use super::data_grid_panel::{DataGridEvent, DataGridPanel, DataSource};
use super::handle::DocumentEvent;
use super::types::{DataSourceKind, DocumentId, DocumentState};
use crate::app::AppStateEntity;
use crate::keymap::{Command, ContextId};
use dbflux_components::result_panel::{ResultPanel, ResultPanelEvent};
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
    /// Chrome host: owns the mode bar and delegates content rendering to
    /// the inner `data_grid` entity via `ViewHandle`.
    result_panel: Entity<ResultPanel>,
    focus_handle: FocusHandle,
    _subscriptions: Vec<Subscription>,
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

        Self::new_with_grid(title, DataSourceKind::Table, data_grid, window, cx)
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

        Self::new_with_grid(title, DataSourceKind::Collection, data_grid, window, cx)
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
            cx.new(|cx| DataGridPanel::new_for_result(result, query, None, app_state, window, cx));

        Self::new_with_grid(title, DataSourceKind::QueryResult, data_grid, window, cx)
    }

    /// Shared construction logic: builds a `ViewHandle` from the grid, wraps it
    /// in `ResultPanel`, and wires subscriptions.
    fn new_with_grid(
        title: String,
        source_kind: DataSourceKind,
        data_grid: Entity<DataGridPanel>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        // Build a ViewHandle from the DataGridPanel entity. This also sets
        // `toolbar_in_chrome_row = true` on the grid, suppressing its own
        // toolbar row so the filter bar appears in the chrome row instead.
        let view_handle = DataGridPanel::into_view_handle(data_grid.clone(), cx);

        let result_panel = cx.new(|cx| ResultPanel::new(view_handle, cx));

        // Forward DataGridEvent to DocumentEvent and keep ResultPanel in sync
        // for mode changes driven by the grid (e.g. auto chart selection).
        let grid_sub = cx.subscribe(&data_grid, Self::on_grid_event);

        // ResultPanel calls view.set_mode / view.set_refresh_policy directly
        // via ViewHandle closures. We still subscribe to ResultPanelEvent for
        // legacy compatibility (in case any other listener needs these events).
        let panel_sub = cx.subscribe(&result_panel, {
            move |_this: &mut DataDocument, _panel, _event: &ResultPanelEvent, _cx| {
                // ViewHandle closures handle the actual mode/policy changes.
                // No additional forwarding is required here.
            }
        });

        Self {
            id: DocumentId::new(),
            title,
            source_kind,
            data_grid,
            result_panel,
            focus_handle: cx.focus_handle(),
            _subscriptions: vec![grid_sub, panel_sub],
        }
    }

    /// Forwards `DataGridEvent` emissions to `DocumentEvent`.
    ///
    /// Mode sync is no longer needed here — the grid's `ViewHandle::available_modes`
    /// and `ViewHandle::current_mode` closures are called by `ResultPanel` on every
    /// render frame, so the chrome row always reflects the current state.
    fn on_grid_event(
        _this: &mut Self,
        _grid: Entity<DataGridPanel>,
        event: &DataGridEvent,
        cx: &mut Context<Self>,
    ) {
        match event {
            DataGridEvent::Focused => {
                cx.emit(DocumentEvent::RequestFocus);
            }
            DataGridEvent::RequestSqlPreview {
                context,
                generation_type,
            } => {
                cx.emit(DocumentEvent::RequestSqlPreview {
                    context: context.clone(),
                    generation_type: *generation_type,
                });
            }
            DataGridEvent::OpenInspector { title, content } => {
                cx.emit(DocumentEvent::OpenInspector {
                    title: title.clone(),
                    content: content.clone(),
                });
            }
            DataGridEvent::ChartThisQuery {
                query,
                connection_id,
            } => {
                cx.emit(DocumentEvent::ChartThisQuery {
                    query: query.clone(),
                    connection_id: *connection_id,
                });
            }
            DataGridEvent::RefreshPolicyReset(_policy) => {
                // The refresh dropdown is owned by DataGridPanel itself and
                // reset internally — no forwarding to ResultPanel needed.
            }
            _ => {}
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

    /// Short summary of pending edits for the dirty-dot tooltip.
    pub fn change_summary(&self, cx: &App) -> Option<String> {
        self.data_grid.read(cx).change_summary(cx)
    }

    pub fn focus(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
    }

    pub fn connection_id(&self, cx: &App) -> Option<Uuid> {
        match self.data_grid.read(cx).source() {
            DataSource::Table { profile_id, .. } => Some(*profile_id),
            DataSource::Collection { profile_id, .. } => Some(*profile_id),
            DataSource::QueryResult { profile_id, .. } => *profile_id,
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

    /// Returns the synthesized query text that produced the current result, if available.
    pub fn synthesized_query(&self, cx: &App) -> Option<String> {
        match self.data_grid.read(cx).source() {
            DataSource::QueryResult { original_query, .. } => {
                if original_query.is_empty() {
                    None
                } else {
                    Some(original_query.clone())
                }
            }
            DataSource::Table { .. } | DataSource::Collection { .. } => None,
        }
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

impl EventEmitter<DocumentEvent> for DataDocument {}

impl Render for DataDocument {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        // Render through the ResultPanel, which provides the chrome row
        // (mode bar + filter bar segment + refresh dropdown) and hosts the
        // DataGridPanel as its inner child view via ViewHandle.
        div()
            .size_full()
            .track_focus(&self.focus_handle)
            .child(self.result_panel.clone())
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time structural assertion: `DataDocument` owns a `result_panel`
    /// field of the correct type.
    #[allow(dead_code)]
    fn _assert_result_panel_field_type(doc: &DataDocument) -> &Entity<ResultPanel> {
        &doc.result_panel
    }
}
