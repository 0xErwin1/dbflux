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
    /// the inner `data_grid` entity.
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

    /// Shared construction logic: wraps a `DataGridPanel` in a `ResultPanel`
    /// and wires up all subscriptions.
    fn new_with_grid(
        title: String,
        source_kind: DataSourceKind,
        data_grid: Entity<DataGridPanel>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        // Auto-refresh is only available for Table and Collection sources.
        let supports_auto = matches!(
            source_kind,
            DataSourceKind::Table | DataSourceKind::Collection
        );

        // Read the default refresh policy the grid already resolved from settings
        // so ResultPanel's dropdown shows the correct initial selection.
        let default_policy = data_grid.read(cx).refresh_policy();

        // Create ResultPanel with a refresh dropdown. The dropdown entity will be
        // passed back to the grid so the chart toolbar can render it.
        let grid_view = AnyView::from(data_grid.clone());
        let result_panel = cx.new(|cx| {
            ResultPanel::new_with_refresh(grid_view, vec![], default_policy, supports_auto, cx)
        });

        // Wire the ResultPanel's dropdown into the grid so it appears in the
        // chart toolbar and the grid can reset it when set_query_result fires.
        {
            let dropdown_entity = result_panel.read(cx).refresh_dropdown_entity().cloned();
            if let Some(dd) = dropdown_entity {
                data_grid.update(cx, |grid, cx| {
                    grid.set_refresh_dropdown(dd, cx);
                });
            }
        }

        let grid_sub = cx.subscribe(&data_grid, Self::on_grid_event);

        // Forward ResultPanel events to the underlying DataGridPanel.
        let panel_sub = cx.subscribe(&result_panel, {
            let data_grid = data_grid.clone();
            move |this: &mut DataDocument, _panel, event: &ResultPanelEvent, cx| match event {
                ResultPanelEvent::ModeChanged(mode) => {
                    data_grid.update(cx, |grid, cx| {
                        grid.set_result_view_mode(*mode, cx);
                    });
                }
                ResultPanelEvent::RefreshPolicyChanged(policy) => {
                    if policy.is_auto() && !data_grid.read(cx).supports_auto_refresh() {
                        // Reject auto-refresh for QueryResult sources and reset
                        // the dropdown back to Manual without triggering another
                        // event (use sync so no round-trip).
                        this.result_panel.update(cx, |panel, cx| {
                            panel.sync_refresh_policy(RefreshPolicy::Manual, cx);
                        });
                    } else {
                        data_grid.update(cx, |grid, cx| {
                            grid.set_refresh_policy(*policy, cx);
                        });
                    }
                }
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

    /// Forwards `DataGridEvent` emissions to `DocumentEvent` and syncs
    /// `ResultPanel`'s available modes after any grid state change.
    fn on_grid_event(
        this: &mut Self,
        grid: Entity<DataGridPanel>,
        event: &DataGridEvent,
        cx: &mut Context<Self>,
    ) {
        // Sync result-view modes into the ResultPanel so the mode bar
        // reflects the current result shape.
        let modes = grid.read(cx).available_result_view_modes(cx);
        let current = grid.read(cx).current_result_view_mode();

        this.result_panel.update(cx, |panel, cx| {
            panel.set_available_modes(modes, cx);
            panel.set_current_mode(current, cx);
        });

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
            DataGridEvent::RefreshPolicyReset(policy) => {
                // Sync ResultPanel's mode bar dropdown when the grid resets
                // its policy (e.g. when a new QueryResult arrives).
                this.result_panel.update(cx, |panel, cx| {
                    panel.sync_refresh_policy(*policy, cx);
                });
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
    ///
    /// For `QueryResult` sources the original query string is returned. For `Table`
    /// and `Collection` sources the grid builds the query internally and does not
    /// expose it as a user-readable string — `None` is returned in those cases.
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
        // Render through the ResultPanel, which provides the mode bar chrome
        // and hosts the DataGridPanel as its inner child view.
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
    ///
    /// This function is never called — it exists only to verify that the field
    /// types compile. Runtime behavior tests require a live GPUI TestAppContext
    /// and a running driver connection, which are not available in unit tests.
    #[allow(dead_code)]
    fn _assert_result_panel_field_type(doc: &DataDocument) -> &Entity<ResultPanel> {
        &doc.result_panel
    }
}
